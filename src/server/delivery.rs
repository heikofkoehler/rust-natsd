//! Message delivery: `deliver` and its fan-out, global queue-group
//! selection across local clients, routes, and leaf nodes, subscription
//! interest propagation, and inbound route/leaf frame processing
//! (`route_read_loop` / `process_route_op`, `leaf_read_loop` /
//! `process_leaf_op`), which feeds `deliver` and interest state.
//!
//! Invariants preserved from the pre-split `server.rs` (Phase 29 moves
//! code only): the sublist `RwLock` is never held across socket or
//! channel operations — `deliver` takes the lock only to compute
//! matches / note deliveries, then releases it before sending frames.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::sync::mpsc;

use super::conn::{try_send, ConnHandle};
use super::{InterestMap, InterestTargets, Shared};
use crate::leaf::{
    encode_leaf_hmsg, encode_lmsg, encode_ls_minus, encode_ls_plus, LeafOp, LeafParser,
};
use crate::protocol::{encode_err, encode_hmsg, encode_msg};
use crate::route::{
    encode_rmsg, encode_route_hmsg, encode_rs_minus, encode_rs_plus, RouteOp, RouteParser,
};

/// A message in flight: subject, optional reply, optional raw header block
/// (starts with `NATS/1.0\r\n`, ends with `\r\n\r\n`), payload.
#[derive(Debug, Clone)]
pub struct Message {
    pub subject: String,
    pub reply_to: Option<String>,
    pub headers: Option<Bytes>,
    pub payload: Bytes,
    /// Account the message belongs to; delivery is scoped to it.
    /// Defaults to `$G` (Message::new); JetStream and route/leaf paths
    /// set it explicitly.
    pub account: String,
    /// The publishing connection set `no_responders:true` in CONNECT.
    /// Only meaningful for Local origin; route/leaf/internal messages
    /// never carry it.
    pub no_responders: bool,
    /// True for a stream-import copy (already translated once): it is
    /// delivered to local subscribers only and is never translated
    /// again, so import chains cannot loop.
    pub import_copy: bool,
    /// True for a service-import translated request (already rewritten
    /// into the exporting account): it must not be service-translated
    /// a second time, but it may still fan out to stream importers.
    pub service_translated: bool,
    /// Phase 31: when set, sublist matching routes on this subject
    /// while frames still carry `subject`. JetStream consumer
    /// deliveries need it: the frame subject must be the original
    /// stream message subject (clients read it — async-nats KV
    /// Watch derives the key from it), but the only subscription
    /// that must receive it is the consumer's deliver subject /
    /// pull reply inbox. Without the override the server used to
    /// send the inbox as the frame subject, which broke Watch.
    pub route_via: Option<String>,
}

impl Message {
    pub fn new(
        subject: &str,
        reply_to: Option<String>,
        headers: Option<Bytes>,
        payload: Bytes,
    ) -> Self {
        Message {
            subject: subject.to_string(),
            reply_to,
            headers,
            payload,
            account: "$G".to_string(),
            no_responders: false,
            import_copy: false,
            service_translated: false,
            route_via: None,
        }
    }

    pub fn with_account(mut self, account: &str) -> Self {
        self.account = account.to_string();
        self
    }

    pub fn with_route_via(mut self, via: &str) -> Self {
        self.route_via = Some(via.to_string());
        self
    }

    pub fn with_no_responders(mut self, no_responders: bool) -> Self {
        self.no_responders = no_responders;
        self
    }

    /// Parse the raw header block into (name, value) pairs. Malformed lines
    /// are skipped; the `NATS/1.0` status line is skipped.
    pub fn header_map(&self) -> HashMap<String, String> {
        let mut out = HashMap::new();
        if let Some(h) = &self.headers {
            let text = String::from_utf8_lossy(h);
            for line in text.lines() {
                if line.starts_with("NATS/") || line.is_empty() {
                    continue;
                }
                if let Some((k, v)) = line.split_once(':') {
                    out.insert(k.trim().to_string(), v.trim().to_string());
                }
            }
        }
        out
    }
}

/// Where a message came from. Local carries (conn_id, echo flag).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Origin {
    Local(u64, bool),
    Route(u64),
    Leaf(u64),
    Internal,
}

/// Deliver a message: to local subscribers, and (unless it arrived on a
/// route/leaf) forward it to routes and leafs with matching interest.
///
/// Account imports/exports (same node only): a local publish whose
/// subject matches a service import is rewritten into the exporting
/// account instead of being delivered locally, with the reply subject
/// replaced by a synthetic `_RIMPORT.<seq>` subject that routes
/// replies back to the importer's original reply subject; a local
/// publish in an exporting account is additionally copied to local
/// subscribers of every authorized stream import of that subject.
/// Returns true when the message reached at least one subscriber (or
/// was consumed by JetStream), which the no-responders logic uses.
///
/// `queue_filter` restricts queue-group delivery for messages arriving
/// on a route/leaf: only the listed groups are delivered (the sender
/// already ran the global queue selection). `None` means all groups
/// (local publishes and header route frames, which carry no list).
pub fn deliver(
    shared: &Arc<Shared>,
    msg: &Message,
    origin: Origin,
    queue_filter: Option<&[String]>,
) -> bool {
    // JetStream capture / API handling happens before core delivery so a
    // stream message is stored even when nobody is subscribed, and API
    // requests are answered by the hosting node.
    if let Some(js) = &shared.jetstream {
        if crate::jetstream::handle_message(shared, js, msg, origin) {
            // API requests and acks are consumed by JetStream; they are not
            // also delivered as ordinary messages.
            return true;
        }
    }

    // Phase 31 consumer deliveries (see `route_via`): route on the
    // deliver subject / pull reply inbox, but frame with the
    // original subject. Local-only, like `deliver_stream_copy`:
    // never service-translated, never forwarded to routes/leafs
    // (a JS delivery to a local inbox is not a publish on the
    // original subject), and never a no-responders case.
    if let Some(via) = msg.route_via.clone() {
        let (deliveries, expired) = {
            let mut sl = shared.sublist.write().unwrap();
            let gm = sl.match_groups(&via, u64::MAX, true, Some(&msg.account));
            let mut deliveries = gm.plain;
            for members in gm.groups.values() {
                if !members.is_empty() {
                    let cursor = shared.queue_cursor.fetch_add(1, Ordering::SeqCst);
                    deliveries.push(members[(cursor as usize) % members.len()].clone());
                }
            }
            let expired = sl.note_deliveries(&deliveries);
            (deliveries, expired)
        };
        send_local_frames(shared, msg, &deliveries);
        cleanup_expired(shared, &expired);
        shared
            .stats
            .out_msgs
            .fetch_add(deliveries.len() as u64, Ordering::Relaxed);
        return !deliveries.is_empty();
    }

    let (publisher_conn, publisher_echo) = match origin {
        Origin::Local(id, echo) => (id, echo),
        _ => (u64::MAX, true),
    };

    // Messages arriving on a route/leaf are delivered locally only
    // (full mesh / hub-and-spoke loop prevention).
    if matches!(origin, Origin::Route(_) | Origin::Leaf(_)) {
        let result = {
            let mut sl = shared.sublist.write().unwrap();
            match origin {
                Origin::Route(_) | Origin::Leaf(_) => {
                    // Filtered queue selection for remote-origin messages.
                    let gm = sl.match_groups(
                        &msg.subject,
                        publisher_conn,
                        publisher_echo,
                        Some(&msg.account),
                    );
                    let mut deliveries = gm.plain;
                    for (q, members) in &gm.groups {
                        let wanted = match queue_filter {
                            Some(list) => list.iter().any(|g| g == q),
                            None => true,
                        };
                        if wanted && !members.is_empty() {
                            let cursor = shared.queue_cursor.fetch_add(1, Ordering::SeqCst);
                            deliveries.push(members[(cursor as usize) % members.len()].clone());
                        }
                    }
                    let expired = sl.note_deliveries(&deliveries);
                    crate::sublist::MatchResult {
                        deliveries,
                        expired,
                    }
                }
                _ => unreachable!(),
            }
        };
        send_local_frames(shared, msg, &result.deliveries);
        cleanup_expired(shared, &result.expired);
        shared
            .stats
            .out_msgs
            .fetch_add(result.deliveries.len() as u64, Ordering::Relaxed);
        return !result.deliveries.is_empty();
    }

    // Service import (local publishes only, same node): translation,
    // synthetic `_RIMPORT.<seq>` reply mapping, and no-responders 503
    // live in `imports::try_service_import` (moved verbatim here in Phase 29).
    if let Some(delivered) = super::imports::try_service_import(shared, msg, origin) {
        return delivered;
    }

    // Local / internal origin: global queue selection across local
    // members, routes, and leafs (weighted by member count), so a queue
    // group spanning nodes still delivers exactly once per message.
    let gm = {
        let sl = shared.sublist.read().unwrap();
        sl.match_groups(
            &msg.subject,
            publisher_conn,
            publisher_echo,
            Some(&msg.account),
        )
    };
    let route_full = route_interest_full(shared, &msg.subject, &msg.account);
    let leaf_full = leaf_interest_full(shared, &msg.subject, &msg.account);

    enum Dest {
        Local(crate::sublist::Delivery),
        Route(u64),
        Leaf(u64),
    }
    let mut local_deliveries: Vec<crate::sublist::Delivery> = gm.plain;
    let mut route_selected: HashMap<u64, Vec<String>> = HashMap::new();
    let mut leaf_selected: HashMap<u64, Vec<String>> = HashMap::new();
    for (group, members) in &gm.groups {
        let mut dests: Vec<Dest> = members.iter().map(|m| Dest::Local(m.clone())).collect();
        for (rid, _, _, counts) in &route_full {
            for _ in 0..counts.get(group).copied().unwrap_or(0) {
                dests.push(Dest::Route(*rid));
            }
        }
        for (lid, _, _, counts) in &leaf_full {
            for _ in 0..counts.get(group).copied().unwrap_or(0) {
                dests.push(Dest::Leaf(*lid));
            }
        }
        if dests.is_empty() {
            continue;
        }
        let cursor = shared.queue_cursor.fetch_add(1, Ordering::SeqCst);
        match &dests[(cursor as usize) % dests.len()] {
            Dest::Local(d) => local_deliveries.push(d.clone()),
            Dest::Route(rid) => route_selected.entry(*rid).or_default().push(group.clone()),
            Dest::Leaf(lid) => leaf_selected.entry(*lid).or_default().push(group.clone()),
        }
    }
    let expired = {
        let mut sl = shared.sublist.write().unwrap();
        sl.note_deliveries(&local_deliveries)
    };
    send_local_frames(shared, msg, &local_deliveries);
    cleanup_expired(shared, &expired);
    shared
        .stats
        .out_msgs
        .fetch_add(local_deliveries.len() as u64, Ordering::Relaxed);

    // No-responders accounting is computed before the route/leaf loops
    // consume route_full/leaf_full: any local delivery, plain route/leaf
    // interest, or selected queue group counts as a responder.
    let mut has_delivery = !local_deliveries.is_empty()
        || route_selected.values().any(|v| !v.is_empty())
        || leaf_selected.values().any(|v| !v.is_empty())
        || route_full.iter().any(|(_, _, plain, _)| *plain)
        || leaf_full.iter().any(|(_, _, plain, _)| *plain);

    // Routes: one frame per route with plain interest and/or a selected
    // queue group. The queues list on the frame is exactly the groups
    // this route was selected for, so the receiver delivers to plain
    // subscribers plus one member per listed group — never more.
    for (rid, handle, plain, _counts) in route_full {
        let selected = route_selected.get(&rid).cloned().unwrap_or_default();
        if !plain && selected.is_empty() {
            continue;
        }
        let frame = match &msg.headers {
            Some(h) => encode_route_hmsg(
                &msg.account,
                &msg.subject,
                msg.reply_to.as_deref(),
                h,
                &msg.payload,
            ),
            None => encode_rmsg(
                &msg.account,
                &msg.subject,
                msg.reply_to.as_deref(),
                &selected,
                &msg.payload,
            ),
        };
        if !try_send(&handle, frame) {
            eprintln!("rust-natsd: slow route {rid}, closing");
            close_route(shared, rid);
        }
    }
    // Leafs: same shape, via LMSG (which carries queues in both the
    // plain and header forms).
    for (lid, handle, plain, _counts) in leaf_full {
        let selected = leaf_selected.get(&lid).cloned().unwrap_or_default();
        if !plain && selected.is_empty() {
            continue;
        }
        let frame = match &msg.headers {
            Some(h) => encode_leaf_hmsg(&msg.subject, msg.reply_to.as_deref(), h, &msg.payload),
            None => encode_lmsg(
                &msg.subject,
                msg.reply_to.as_deref(),
                &selected,
                None,
                &msg.payload,
            ),
        };
        if !try_send(&handle, frame) {
            eprintln!("rust-natsd: slow leaf {lid}, closing");
            close_leaf(shared, lid);
        }
    }

    // Stream imports (local/internal publishes only, same node):
    // copies to authorized stream importers; see
    // `imports::deliver_stream_imports` (moved verbatim here in Phase 29).
    if super::imports::deliver_stream_imports(shared, msg) {
        has_delivery = true;
    }

    // No-responders (Go behaviour, verified against v2.15.1): a local
    // publish with a reply subject and no deliveries, from a connection
    // that set no_responders:true, generates an HMSG to the reply subject
    // with a 503 header block and an empty payload. JetStream-consumed
    // messages return early above, so they never generate one.
    if msg.no_responders && !has_delivery {
        if let Some(reply_to) = &msg.reply_to {
            let headers = Bytes::from(format!(
                "NATS/1.0 503\r\nNats-Subject: {}\r\n\r\n",
                msg.subject
            ));
            deliver(
                shared,
                &Message::new(reply_to, None, Some(headers), Bytes::new())
                    .with_account(&msg.account),
                Origin::Internal,
                None,
            );
        }
    }
    has_delivery
}

/// Deliver a stream-import copy to local subscribers of its (already
/// rewritten) subject/account only — no route/leaf forwarding, no
/// further import translation. Queue groups get exactly one member,
/// like the route/leaf-origin path. Returns true on any delivery.
pub(crate) fn deliver_stream_copy(shared: &Arc<Shared>, msg: &Message) -> bool {
    let (deliveries, expired) = {
        let mut sl = shared.sublist.write().unwrap();
        let gm = sl.match_groups(&msg.subject, u64::MAX, true, Some(&msg.account));
        let mut deliveries = gm.plain;
        for members in gm.groups.values() {
            if !members.is_empty() {
                let cursor = shared.queue_cursor.fetch_add(1, Ordering::SeqCst);
                deliveries.push(members[(cursor as usize) % members.len()].clone());
            }
        }
        let expired = sl.note_deliveries(&deliveries);
        (deliveries, expired)
    };
    send_local_frames(shared, msg, &deliveries);
    cleanup_expired(shared, &expired);
    shared
        .stats
        .out_msgs
        .fetch_add(deliveries.len() as u64, Ordering::Relaxed);
    !deliveries.is_empty()
}

/// Send MSG/HMSG frames for local deliveries, dropping slow consumers.
/// The connection registry is locked once for all targets, not once
/// per message: at burst rates a per-message lock was measurable
/// contention on the publisher thread.
pub(crate) fn send_local_frames(
    shared: &Arc<Shared>,
    msg: &Message,
    deliveries: &[crate::sublist::Delivery],
) {
    let targets: Vec<(crate::sublist::Delivery, Arc<ConnHandle>)> = {
        let conns = shared.conns.lock().unwrap();
        deliveries
            .iter()
            .filter_map(|d| conns.get(&d.conn_id).cloned().map(|h| (d.clone(), h)))
            .collect()
    };
    for (d, target) in targets {
        let frame = match &msg.headers {
            Some(h) if target.headers.load(Ordering::SeqCst) => encode_hmsg(
                &msg.subject,
                &d.sid,
                msg.reply_to.as_deref(),
                h,
                &msg.payload,
            ),
            _ => encode_msg(&msg.subject, &d.sid, msg.reply_to.as_deref(), &msg.payload),
        };
        if !try_send(&target, frame) {
            shared.stats.slow_consumers.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "rust-natsd: slow consumer, dropping connection {}",
                d.conn_id
            );
            shared.sublist.write().unwrap().remove_conn(d.conn_id);
            if let Some(h) = shared.conns.lock().unwrap().remove(&d.conn_id) {
                h.mark_closed();
            }
            shared.conn_info.lock().unwrap().remove(&d.conn_id);
        }
    }
}

/// Remove auto-unsubscribed subscriptions and propagate RS-/LS- for
/// patterns that lost their last local subscriber.
pub(crate) fn cleanup_expired(shared: &Arc<Shared>, expired: &[(u64, String)]) {
    if expired.is_empty() {
        return;
    }
    let patterns: Vec<(String, String, Option<String>)> = {
        let sl = shared.sublist.read().unwrap();
        expired
            .iter()
            .filter_map(|(cid, sid)| sl.pattern_of(*cid, sid))
            .collect()
    };
    {
        let mut sl = shared.sublist.write().unwrap();
        for (cid, sid) in expired {
            sl.remove(*cid, sid);
        }
    }
    for (account, subject, queue) in patterns {
        if !local_pattern_in_use(shared, &account, &subject, queue.as_deref()) {
            propagate_local_sub(shared, &account, &subject, queue.as_deref(), false);
        }
    }
}

/// Snapshot of one connection's interest: (plain_match, queue -> count).
pub(crate) fn interest_of(
    subs: &InterestMap,
    subject: &str,
    account: &str,
) -> (bool, HashMap<String, u32>) {
    let mut plain = false;
    let mut counts: HashMap<String, u32> = HashMap::new();
    for ((acct, pattern, queue), count) in subs {
        if acct != account {
            continue;
        }
        if *count == 0 {
            continue;
        }
        if !crate::subject::subject_matches(pattern, subject) {
            continue;
        }
        match queue {
            None => plain = true,
            Some(q) => {
                *counts.entry(q.clone()).or_insert(0) += *count;
            }
        }
    }
    (plain, counts)
}

/// Whether any local subscription, route, or leaf has interest in
/// `subject` for `account`. Used by the JetStream tick to defer a push
/// redelivery while nobody is listening on the deliver subject, so it
/// does not burn a delivery cycle on a dropped message. Read-only: it
/// uses `match_groups`, which does not update delivery counters.
pub(crate) fn has_interest(shared: &Arc<Shared>, account: &str, subject: &str) -> bool {
    {
        let sl = shared.sublist.read().unwrap();
        let gm = sl.match_groups(subject, u64::MAX, true, Some(account));
        if !gm.plain.is_empty() || !gm.groups.is_empty() {
            return true;
        }
    }
    for subs in shared.route_subs.lock().unwrap().values() {
        let (plain, counts) = interest_of(subs, subject, account);
        if plain || !counts.is_empty() {
            return true;
        }
    }
    for subs in shared.leaf_subs.lock().unwrap().values() {
        let (plain, counts) = interest_of(subs, subject, account);
        if plain || !counts.is_empty() {
            return true;
        }
    }
    false
}

pub(crate) fn route_interest_full(
    shared: &Arc<Shared>,
    subject: &str,
    account: &str,
) -> InterestTargets {
    let subs = shared.route_subs.lock().unwrap();
    shared
        .routes
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, r)| !r.handle.is_closed())
        .map(|(id, r)| {
            let (plain, counts) = subs
                .get(id)
                .map(|m| interest_of(m, subject, account))
                .unwrap_or((false, HashMap::new()));
            (*id, r.handle.clone(), plain, counts)
        })
        .collect()
}

pub(crate) fn leaf_interest_full(
    shared: &Arc<Shared>,
    subject: &str,
    account: &str,
) -> InterestTargets {
    let subs = shared.leaf_subs.lock().unwrap();
    shared
        .leafs
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, l)| !l.handle.is_closed() && l.account == account)
        .map(|(id, l)| {
            let (plain, counts) = subs
                .get(id)
                .map(|m| interest_of(m, subject, account))
                .unwrap_or((false, HashMap::new()));
            (*id, l.handle.clone(), plain, counts)
        })
        .collect()
}

/// Broadcast a local subscription change to all routes and leafs.
pub(crate) fn propagate_local_sub(
    shared: &Arc<Shared>,
    account: &str,
    subject: &str,
    queue: Option<&str>,
    added: bool,
) {
    let rframe = if added {
        encode_rs_plus(account, subject, queue)
    } else {
        encode_rs_minus(account, subject, queue)
    };
    // Collect handles first and send after the locks drop, mirroring
    // send_local_frames: the registry locks are never held across the
    // (non-blocking channel) sends.
    let route_handles: Vec<Arc<ConnHandle>> = shared
        .routes
        .lock()
        .unwrap()
        .values()
        .filter(|r| !r.handle.is_closed())
        .map(|r| r.handle.clone())
        .collect();
    for handle in route_handles {
        let _ = try_send(&handle, rframe.clone());
    }
    // Leaf frames carry no account token: a leaf connection only ever
    // carries its own account, so propagate only to matching leafs.
    let lframe = if added {
        encode_ls_plus(subject, queue)
    } else {
        encode_ls_minus(subject, queue)
    };
    let leaf_handles: Vec<Arc<ConnHandle>> = shared
        .leafs
        .lock()
        .unwrap()
        .values()
        .filter(|l| !l.handle.is_closed() && l.account == account)
        .map(|l| l.handle.clone())
        .collect();
    for handle in leaf_handles {
        let _ = try_send(&handle, lframe.clone());
    }
}

/// Does any local subscription (or JetStream internal pattern) still use
/// this pattern+queue in this account?
pub(crate) fn local_pattern_in_use(
    shared: &Arc<Shared>,
    account: &str,
    subject: &str,
    queue: Option<&str>,
) -> bool {
    if shared.jetstream.is_some() && subject == "$JS.API.>" && queue.is_none() {
        return true;
    }
    shared
        .sublist
        .read()
        .unwrap()
        .has_pattern(subject, queue, account)
}

/// Send every local subscription (plus JetStream internal patterns) to one
/// route or leaf connection as RS+/LS+ frames.
pub(crate) fn send_local_interest(
    shared: &Arc<Shared>,
    handle: &ConnHandle,
    is_route: bool,
    leaf_account: Option<&str>,
) {
    let subs: Vec<(String, String, Option<String>)> = {
        let sl = shared.sublist.read().unwrap();
        sl.patterns()
    };
    for (account, subject, queue) in subs {
        if is_route {
            let _ = try_send(handle, encode_rs_plus(&account, &subject, queue.as_deref()));
        } else if leaf_account == Some(account.as_str()) {
            let _ = try_send(handle, encode_ls_plus(&subject, queue.as_deref()));
        }
    }
    if shared.jetstream.is_some() {
        if is_route {
            // Advertise JetStream API interest for every account so
            // remote nodes can forward $JS.API requests per account.
            let mut accounts: Vec<String> = shared
                .config
                .accounts
                .iter()
                .map(|a| a.name.clone())
                .collect();
            if !accounts.contains(&"$G".to_string()) {
                accounts.push("$G".to_string());
            }
            for account in accounts {
                let _ = try_send(handle, encode_rs_plus(&account, "$JS.API.>", None));
            }
        } else if let Some(account) = leaf_account {
            let _ = try_send(handle, encode_ls_plus("$JS.API.>", None));
            let _ = account;
        }
    }
}

pub(crate) fn close_route(shared: &Arc<Shared>, id: u64) {
    let removed = shared.routes.lock().unwrap().remove(&id);
    shared.route_subs.lock().unwrap().remove(&id);
    shared.conn_info.lock().unwrap().remove(&id);
    if let Some(r) = removed {
        r.handle.mark_closed();
    }
}

pub(crate) fn close_leaf(shared: &Arc<Shared>, id: u64) {
    let removed = shared.leafs.lock().unwrap().remove(&id);
    shared.leaf_subs.lock().unwrap().remove(&id);
    shared.conn_info.lock().unwrap().remove(&id);
    if let Some(l) = removed {
        l.handle.mark_closed();
    }
}

/// Bump a counted interest entry by `delta` (+1 / -1), removing at zero.
pub(crate) fn bump_interest(
    map: &mut InterestMap,
    account: &str,
    subject: &str,
    queue: Option<&str>,
    delta: i32,
) {
    let key = (
        account.to_string(),
        subject.to_string(),
        queue.map(|q| q.to_string()),
    );
    let entry = map.entry(key).or_insert(0);
    if delta > 0 {
        *entry = entry.saturating_add(delta as u32);
    } else {
        *entry = entry.saturating_sub((-delta) as u32);
        if *entry == 0 {
            map.remove(&(
                account.to_string(),
                subject.to_string(),
                queue.map(|q| q.to_string()),
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Client connections
// ---------------------------------------------------------------------------

pub(crate) fn json_str(json: &str, field: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()?
        .get(field)?
        .as_str()
        .map(|s| s.to_string())
}

pub(crate) fn json_u64(json: &str, field: &str) -> Option<u64> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()?
        .get(field)?
        .as_u64()
}

pub(crate) fn json_bool(json: &str, field: &str) -> Option<bool> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()?
        .get(field)?
        .as_bool()
}

/// Read one CRLF-terminated line from a buffered reader. Generic over
/// the read half so plain TCP and TLS streams share the route/leaf
/// handshake and read loops.
pub(crate) async fn read_line<R: AsyncReadExt + Unpin>(
    reader: &mut BufReader<R>,
) -> io::Result<String> {
    let mut buf = Vec::new();
    let n = reader.read_until(b'\n', &mut buf).await?;
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
    }
    Ok(String::from_utf8_lossy(&buf).trim_end().to_string())
}

pub(crate) async fn route_read_loop<R: AsyncReadExt + Unpin>(
    reader: &mut BufReader<R>,
    shared: &Arc<Shared>,
    id: u64,
    handle: &Arc<ConnHandle>,
    tx: &mpsc::Sender<Bytes>,
) {
    let mut parser = RouteParser::new(shared.config.max_payload);
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        if handle.is_closed() {
            break;
        }
        let n = tokio::select! {
            _ = handle.close.notified() => break,
            _ = shared.shutdown.notified() => break,
            r = reader.read(&mut buf) => match r {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            },
        };
        parser.feed(&buf[..n]);
        let mut close = false;
        loop {
            match parser.next_op() {
                Ok(Some(op)) => {
                    if !process_route_op(op, shared, id, handle, tx).await {
                        close = true;
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = tx.send(encode_err(&e.to_string())).await;
                    close = true;
                    break;
                }
            }
        }
        if close {
            break;
        }
    }
}

/// Process one route operation. Returns false when the connection must close.
pub(crate) async fn process_route_op(
    op: RouteOp,
    shared: &Arc<Shared>,
    id: u64,
    handle: &Arc<ConnHandle>,
    tx: &mpsc::Sender<Bytes>,
) -> bool {
    match op {
        RouteOp::Info(_) | RouteOp::Connect(_) | RouteOp::Pong => true,
        RouteOp::Ping => {
            let _ = tx.send(Bytes::from("PONG\r\n")).await;
            true
        }
        RouteOp::RsPlus {
            account,
            subject,
            queue,
            ..
        } => {
            let mut subs = shared.route_subs.lock().unwrap();
            if let Some(m) = subs.get_mut(&id) {
                bump_interest(m, &account, &subject, queue.as_deref(), 1);
            }
            true
        }
        RouteOp::RsMinus {
            account,
            subject,
            queue,
            ..
        } => {
            let mut subs = shared.route_subs.lock().unwrap();
            if let Some(m) = subs.get_mut(&id) {
                bump_interest(m, &account, &subject, queue.as_deref(), -1);
            }
            true
        }
        RouteOp::RMsg {
            account,
            subject,
            reply_to,
            queues,
            headers,
            payload,
            ..
        } => {
            handle.in_msgs.fetch_add(1, Ordering::Relaxed);
            handle.in_bytes.fetch_add(
                (headers.as_ref().map(|h| h.len()).unwrap_or(0) + payload.len()) as u64,
                Ordering::Relaxed,
            );
            shared.stats.in_msgs.fetch_add(1, Ordering::Relaxed);
            shared.stats.in_bytes.fetch_add(
                (headers.as_ref().map(|h| h.len()).unwrap_or(0) + payload.len()) as u64,
                Ordering::Relaxed,
            );
            // HMSG frames carry no queue list: deliver to all matching
            // groups. RMSG frames restrict to the listed groups.
            let filter = if headers.is_some() {
                None
            } else {
                Some(queues.as_slice())
            };
            deliver(
                shared,
                &Message::new(&subject, reply_to, headers, payload).with_account(&account),
                Origin::Route(id),
                filter,
            );
            true
        }
        RouteOp::Err(e) => {
            eprintln!("rust-natsd: route {id} error: {e}");
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Leaf nodes
// ---------------------------------------------------------------------------

pub(crate) async fn leaf_read_loop<R: AsyncReadExt + Unpin>(
    reader: &mut BufReader<R>,
    shared: &Arc<Shared>,
    id: u64,
    handle: &Arc<ConnHandle>,
    tx: &mpsc::Sender<Bytes>,
) {
    let mut parser = LeafParser::new(shared.config.max_payload);
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        if handle.is_closed() {
            break;
        }
        let n = tokio::select! {
            _ = handle.close.notified() => break,
            _ = shared.shutdown.notified() => break,
            r = reader.read(&mut buf) => match r {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            },
        };
        parser.feed(&buf[..n]);
        let mut close = false;
        loop {
            match parser.next_op() {
                Ok(Some(op)) => {
                    if !process_leaf_op(op, shared, id, handle, tx).await {
                        close = true;
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = tx.send(encode_err(&e.to_string())).await;
                    close = true;
                    break;
                }
            }
        }
        if close {
            break;
        }
    }
}

pub(crate) async fn process_leaf_op(
    op: LeafOp,
    shared: &Arc<Shared>,
    id: u64,
    handle: &Arc<ConnHandle>,
    tx: &mpsc::Sender<Bytes>,
) -> bool {
    match op {
        LeafOp::Info(_) | LeafOp::Connect(_) | LeafOp::Pong => true,
        LeafOp::Ping => {
            let _ = tx.send(Bytes::from("PONG\r\n")).await;
            true
        }
        LeafOp::LsPlus { subject, queue } => {
            let leaf_account = shared
                .leafs
                .lock()
                .unwrap()
                .get(&id)
                .map(|l| l.account.clone())
                .unwrap_or_else(|| "$G".to_string());
            let mut subs = shared.leaf_subs.lock().unwrap();
            if let Some(m) = subs.get_mut(&id) {
                bump_interest(m, &leaf_account, &subject, queue.as_deref(), 1);
            }
            true
        }
        LeafOp::LsMinus { subject, queue } => {
            let leaf_account = shared
                .leafs
                .lock()
                .unwrap()
                .get(&id)
                .map(|l| l.account.clone())
                .unwrap_or_else(|| "$G".to_string());
            let mut subs = shared.leaf_subs.lock().unwrap();
            if let Some(m) = subs.get_mut(&id) {
                bump_interest(m, &leaf_account, &subject, queue.as_deref(), -1);
            }
            true
        }
        LeafOp::LMsg {
            subject,
            reply_to,
            queues,
            headers,
            payload,
            ..
        } => {
            handle.in_msgs.fetch_add(1, Ordering::Relaxed);
            handle.in_bytes.fetch_add(
                (headers.as_ref().map(|h| h.len()).unwrap_or(0) + payload.len()) as u64,
                Ordering::Relaxed,
            );
            shared.stats.in_msgs.fetch_add(1, Ordering::Relaxed);
            shared.stats.in_bytes.fetch_add(
                (headers.as_ref().map(|h| h.len()).unwrap_or(0) + payload.len()) as u64,
                Ordering::Relaxed,
            );
            let leaf_account = shared
                .leafs
                .lock()
                .unwrap()
                .get(&id)
                .map(|l| l.account.clone())
                .unwrap_or_else(|| "$G".to_string());
            deliver(
                shared,
                &Message::new(&subject, reply_to, headers, payload).with_account(&leaf_account),
                Origin::Leaf(id),
                queues.as_deref(),
            );
            true
        }
        LeafOp::Err(e) => {
            eprintln!("rust-natsd: leaf {id} error: {e}");
            false
        }
    }
}
