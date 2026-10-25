//! Multi-account service/stream import/export translation and the
//! `_RIMPORT` reply-mapping table.
//!
//! A service import rewrites a local publish into the exporting account
//! and replaces its reply subject with a synthetic `_RIMPORT.<seq>`
//! subject; replies to that subject are routed back to the importer's
//! original reply subject. Entries expire after 60 s (Phase 28C; Go's
//! `_R_.` mappings expire by TTL) and are pruned opportunistically on
//! insert and lookup, so a stale mapping can never route a reply.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;

use super::delivery::{deliver, deliver_stream_copy};
use super::{Message, Origin, Shared};

/// Lifetime of a service-import reply mapping (Phase 28C; Go's
/// `_R_.` mappings expire by TTL).
const IMPORT_REPLY_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// Drop expired import-reply mappings. Called opportunistically on
/// insert and lookup; there is no background sweep task, so expiry
/// is enforced at every use and the map cannot grow without bound
/// on a live server (each new import also prunes the old ones).
pub(crate) fn prune_import_replies(shared: &Arc<Shared>) {
    let mut map = shared.import_replies.lock().unwrap();
    prune_import_map(&mut map, std::time::Instant::now());
}

/// The TTL rule itself, factored out so tests can exercise it with
/// an explicit `now` instead of waiting 60 s (see the unit tests).
pub(crate) fn prune_import_map(
    map: &mut HashMap<String, (String, String, std::time::Instant)>,
    now: std::time::Instant,
) {
    map.retain(|_, (_, _, created)| now.duration_since(*created) < IMPORT_REPLY_TTL);
}

/// Look up a service-import reply mapping, honouring the TTL: an
/// expired entry is removed and treated as unmapped, so a reply
/// arriving after 60 s falls through to the normal publish path
/// instead of being routed to a stale inbox.
pub(crate) fn lookup_import_reply(shared: &Arc<Shared>, subject: &str) -> Option<(String, String)> {
    prune_import_replies(shared);
    shared
        .import_replies
        .lock()
        .unwrap()
        .get(subject)
        .map(|(account, orig, _)| (account.clone(), orig.clone()))
}

/// Add (or replace) a `Nats-Request-Info` header on a translated
/// service-import request. Go v2.15.1 shape, probed: the responder
/// sees `Nats-Request-Info: {"acc":"<importing account>","rtt":<ns>}`
/// where `rtt` is the importing client's measured RTT (omitted by
/// Go when 0). rust-natsd does not measure client RTT, so it emits
/// `{"acc":...}` only — the account identity, which is what
/// responders use, is exact; the RTT is absent, as Go's is when
/// unmeasured.
pub(crate) fn with_request_info(headers: Option<Bytes>, importing_account: &str) -> Option<Bytes> {
    let info = serde_json::json!({"acc": importing_account}).to_string();
    let line = format!("Nats-Request-Info: {info}");
    match headers {
        None => Some(Bytes::from(format!("NATS/1.0\r\n{line}\r\n\r\n"))),
        Some(h) => {
            let text = String::from_utf8_lossy(&h);
            let mut body = text.trim_end_matches(['\r', '\n']).to_string();
            // Drop any pre-existing (possibly forged) Request-Info.
            let mut kept: Vec<&str> = Vec::new();
            for l in body.lines() {
                if l.split(':')
                    .next()
                    .is_some_and(|k| k.eq_ignore_ascii_case("Nats-Request-Info"))
                {
                    continue;
                }
                kept.push(l);
            }
            body = kept.join("\r\n");
            if !body.starts_with("NATS/") {
                body = format!("NATS/1.0\r\n{body}");
            }
            Some(Bytes::from(format!("{body}\r\n{line}\r\n\r\n")))
        }
    }
}

/// Service-import translation, moved verbatim out of `deliver` in
/// Phase 29 (only the guard became an early return and the
/// `return delivered` / `return false` became `Some(..)`).
// Service import (local publishes only, same node): a publish on
// the importing account's local subject is rewritten into the
// exporting account and never delivered locally — the import
// shadows the local subject, as on Go. An import the exporting
// account does not authorize (account not listed on the export, or
// remote pattern not covered by the export) is dropped outright:
// it must never leak across accounts. Go refuses to start on an
// unauthorized service import; rust-natsd keeps the server up and
// drops the message instead (see Config::import_authorized).
pub(crate) fn try_service_import(
    shared: &Arc<Shared>,
    msg: &Message,
    origin: Origin,
) -> Option<bool> {
    if !(matches!(origin, Origin::Local(_, _)) && !msg.service_translated && !msg.import_copy) {
        return None;
    }
    if let Some(publisher_account) = shared.config.account(&msg.account).cloned() {
        if let Some((import, remote)) = publisher_account.service_import_for(&msg.subject) {
            if !shared.config.import_authorized(&msg.account, import) {
                eprintln!(
                    "rust-natsd: dropping unauthorized service import {} -> {}:{}",
                    msg.subject, import.source_account, remote
                );
                return Some(false);
            }
            let mut translated = Message::new(
                &remote,
                None,
                with_request_info(msg.headers.clone(), &msg.account),
                msg.payload.clone(),
            )
            .with_account(&import.source_account)
            .with_no_responders(msg.no_responders);
            translated.service_translated = true;
            if let Some(orig_reply) = &msg.reply_to {
                let seq = shared.import_seq.fetch_add(1, Ordering::SeqCst);
                let synthetic = format!("_RIMPORT.{seq}");
                prune_import_replies(shared);
                shared.import_replies.lock().unwrap().insert(
                    synthetic.clone(),
                    (
                        msg.account.clone(),
                        orig_reply.clone(),
                        std::time::Instant::now(),
                    ),
                );
                translated.reply_to = Some(synthetic);
            }
            let delivered = deliver(shared, &translated, origin, None);
            // No-responders for the translated request: the 503 goes
            // to the importer's original reply subject, not to the
            // synthetic one (nothing subscribes to that in A).
            if msg.no_responders && !delivered {
                if let Some(orig_reply) = &msg.reply_to {
                    let headers = Bytes::from(format!(
                        "NATS/1.0 503\r\nNats-Subject: {}\r\n\r\n",
                        msg.subject
                    ));
                    deliver(
                        shared,
                        &Message::new(orig_reply, None, Some(headers), Bytes::new())
                            .with_account(&msg.account),
                        Origin::Internal,
                        None,
                    );
                }
            }
            return Some(delivered);
        }
    }
    None
}

/// Stream-import copies, moved verbatim out of `deliver` in
/// Phase 29 (only `has_delivery = true` became the local
/// `delivered_any` flag).
// Stream imports (local/internal publishes only, same node):
// copy the message, subject rewritten to the importer's local
// subject, to that account's local subscribers. The importer's
// subscribe permission was checked against the local subject at
// SUB time, and the export must cover this concrete subject and
// name the importer — otherwise no copy is made. Copies are
// marked import_copy, so they never translate again.
pub(crate) fn deliver_stream_imports(shared: &Arc<Shared>, msg: &Message) -> bool {
    let mut delivered_any = false;
    if !msg.import_copy {
        let importers: Vec<(String, String)> = shared
            .config
            .all_accounts()
            .filter(|a| a.name != msg.account)
            .filter_map(|a| {
                a.stream_import_for(&msg.account, &msg.subject)
                    .map(|(_, local)| (a.name.clone(), local))
            })
            .collect();
        for (importer, local) in importers {
            if !shared
                .config
                .stream_export_allows(&msg.account, &importer, &msg.subject)
            {
                continue;
            }
            let mut copy = Message::new(
                &local,
                msg.reply_to.clone(),
                msg.headers.clone(),
                msg.payload.clone(),
            )
            .with_account(&importer);
            copy.import_copy = true;
            if deliver_stream_copy(shared, &copy) {
                delivered_any = true;
            }
        }
    }
    delivered_any
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_reply_prune_drops_expired_only() {
        // Phase 28C: entries older than 60 s expire; fresh ones stay.
        // Exercised with an explicit `now` so the test never waits.
        let now = std::time::Instant::now();
        let mut map = HashMap::new();
        map.insert(
            "_RIMPORT.1".to_string(),
            (
                "B".to_string(),
                "_INBOX.x".to_string(),
                now - std::time::Duration::from_secs(61),
            ),
        );
        map.insert(
            "_RIMPORT.2".to_string(),
            ("B".to_string(), "_INBOX.y".to_string(), now),
        );
        prune_import_map(&mut map, now);
        assert!(!map.contains_key("_RIMPORT.1"), "expired entry must go");
        assert!(map.contains_key("_RIMPORT.2"), "fresh entry must stay");
    }

    #[test]
    fn request_info_injected_and_forged_replaced() {
        // No prior headers: a fresh header block is created.
        let h = with_request_info(None, "B").unwrap();
        let text = String::from_utf8_lossy(&h);
        assert_eq!(
            text, "NATS/1.0\r\nNats-Request-Info: {\"acc\":\"B\"}\r\n\r\n",
            "fresh header block: {text}"
        );
        // Existing headers are preserved, a forged Request-Info is
        // replaced (exactly one survives, with the real account).
        let prior = Bytes::from(
            "NATS/1.0\r\nNats-Msg-Id: abc\r\nNats-Request-Info: {\"acc\":\"E\"}\r\n\r\n",
        );
        let h = with_request_info(Some(prior), "B").unwrap();
        let text = String::from_utf8_lossy(&h);
        assert!(
            text.contains("Nats-Msg-Id: abc"),
            "other header kept: {text}"
        );
        assert!(
            text.contains("Nats-Request-Info: {\"acc\":\"B\"}"),
            "real account wins: {text}"
        );
        assert!(
            !text.contains("\"acc\":\"E\""),
            "forged account must not survive: {text}"
        );
        assert_eq!(
            text.matches("Nats-Request-Info").count(),
            1,
            "exactly one Request-Info line: {text}"
        );
    }
}
