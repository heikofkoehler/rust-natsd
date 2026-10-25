//! `$JS.API.>` request router, JSON schemas, and error codes, plus the
//! stream API handlers and the top-level `handle_message` dispatcher
//! (API / ack / capture routing, lock-then-dispatch actions pattern).

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use serde_json::{json, Value};

use super::{
    base64_encode, headers_json, make_source_token, now_ns, parse_source_cfg, rfc3339, Action,
    Engine, JetStream, SourceCfg, Stream,
};
use crate::server::{deliver, Message, Origin, Shared};
use crate::subject::subject_matches;

/// API responses are flat, matching nats-server: the `type` marker plus
/// the operation's fields at top level (no "response" envelope).
pub(crate) fn api_response(
    account: &str,
    reply_to: &str,
    type_name: &str,
    response: Value,
    actions: &mut Vec<Action>,
) {
    let mut body = match response {
        Value::Object(map) => map,
        other => {
            let mut map = serde_json::Map::new();
            map.insert("value".to_string(), other);
            map
        }
    };
    body.insert("type".to_string(), Value::String(type_name.to_string()));
    actions.push(Action::Deliver(
        Message::new(
            reply_to,
            None,
            None,
            Bytes::from(Value::Object(body).to_string()),
        )
        .with_account(account),
    ));
}

pub(crate) fn api_error_action(
    account: &str,
    reply_to: &str,
    type_name: &str,
    code: u16,
    err_code: u32,
    desc: &str,
    actions: &mut Vec<Action>,
) {
    let body = json!({
        "type": type_name,
        "error": { "code": code, "err_code": err_code, "description": desc },
    });
    actions.push(Action::Deliver(
        Message::new(reply_to, None, None, Bytes::from(body.to_string())).with_account(account),
    ));
}

/// nats-server error codes (nats.go maps these to typed errors, so they
/// must match: 10059 stream, 10014 consumer, 10037 message).
pub(crate) fn err_not_found(what: &str) -> (u16, u32, String) {
    match what {
        "consumer" => (404, 10014, "consumer not found".to_string()),
        "message" => (404, 10037, "message not found".to_string()),
        _ => (404, 10059, format!("{what} not found")),
    }
}

pub(crate) fn parse_u64(s: &str) -> Option<u64> {
    s.parse().ok()
}

pub(crate) fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('.')
        && !name.contains('$')
        && !name.contains(' ')
        && !name.contains('*')
        && !name.contains('>')
}

impl Engine {
    pub(crate) fn process(
        &mut self,
        msg: &Message,
        origin: Origin,
        js_enabled_for_account: bool,
    ) -> (bool, Vec<Action>) {
        let mut actions = Vec::new();
        if matches!(origin, Origin::Internal) {
            return (false, actions);
        }
        let subject = msg.subject.as_str();
        if let Some(rest) = subject.strip_prefix("$JS.ACK.") {
            self.process_ack(rest, msg, &mut actions);
            return (true, actions);
        }
        if let Some(rest) = subject.strip_prefix("$JS.API.") {
            self.process_api(rest, msg, js_enabled_for_account, &mut actions);
            return (true, actions);
        }
        self.capture(msg, &mut actions);
        (false, actions)
    }

    pub(crate) fn process_api(
        &mut self,
        rest: &str,
        msg: &Message,
        js_enabled_for_account: bool,
        actions: &mut Vec<Action>,
    ) {
        let Some(reply) = msg.reply_to.clone() else {
            return;
        };
        let body: Value = serde_json::from_slice(&msg.payload).unwrap_or(Value::Null);
        let parts: Vec<&str> = rest.split('.').collect();
        // The type marker names the operation only, never the stream or
        // consumer (real nats-server: "...consumer_create_response",
        // not "...consumer_create_<stream>_<consumer>_response").
        let op_name = match parts.as_slice() {
            // $JS.API.INFO has no rust-natsd implementation (it falls
            // through to the unknown-operation error), but its type
            // marker is the account_info one so the not-enabled error
            // below matches Go's shape for that subject.
            ["INFO"] => "account_info",
            ["STREAM", "CREATE", ..] => "stream_create",
            ["STREAM", "INFO", ..] => "stream_info",
            ["STREAM", "LIST"] => "stream_list",
            ["STREAM", "NAMES"] => "stream_names",
            ["STREAM", "DELETE", ..] => "stream_delete",
            ["STREAM", "PURGE", ..] => "stream_purge",
            ["STREAM", "MSG", "GET", ..] => "stream_msg_get",
            ["CONSUMER", "CREATE", ..] => "consumer_create",
            ["CONSUMER", "DURABLE", "CREATE", ..] => "consumer_create",
            ["CONSUMER", "INFO", ..] => "consumer_info",
            ["CONSUMER", "LIST", ..] => "consumer_list",
            ["CONSUMER", "DELETE", ..] => "consumer_delete",
            ["CONSUMER", "MSG", "NEXT", ..] => "consumer_msg_next",
            _ => "unknown",
        };
        let type_name = format!("io.nats.jetstream.api.v1.{op_name}_response");
        // Per-account JetStream gate (Phase 28C, Go 503/10039): a
        // multi-account account without `jetstream: enabled` gets
        // the not-enabled error for every $JS.API operation,
        // including $JS.API.INFO (Go's account_info_response shape
        // is approximated by the op-specific type marker, as with
        // all other rust-natsd API errors).
        if !js_enabled_for_account {
            api_error_action(
                &msg.account,
                &reply,
                &type_name,
                503,
                10039,
                "JetStream not enabled for account",
                actions,
            );
            return;
        }
        match parts.as_slice() {
            ["STREAM", "CREATE", name] => {
                self.api_stream_create(&msg.account, name, &body, &reply, &type_name, actions);
            }
            ["STREAM", "CREATE"] => {
                let name = body
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                self.api_stream_create(&msg.account, &name, &body, &reply, &type_name, actions);
            }
            ["STREAM", "INFO", name] => {
                self.api_stream_info(&msg.account, name, &reply, &type_name, actions)
            }
            ["STREAM", "LIST"] => {
                self.api_stream_list(&msg.account, &body, &reply, &type_name, actions)
            }
            ["STREAM", "NAMES"] => {
                self.api_stream_names(&msg.account, &body, &reply, &type_name, actions)
            }
            ["STREAM", "DELETE", name] => {
                self.api_stream_delete(&msg.account, name, &reply, &type_name, actions)
            }
            ["STREAM", "PURGE", name] => {
                self.api_stream_purge(&msg.account, name, &body, &reply, &type_name, actions)
            }
            ["STREAM", "MSG", "GET", name] => {
                self.api_stream_msg_get(&msg.account, name, &body, &reply, &type_name, actions)
            }
            // The consumer name is parts[3] when present; clients
            // append the filter subject as further tokens
            // (CREATE.<stream>.<consumer>.<filter...>), which are
            // ignored here because the body carries filter_subject.
            ["CONSUMER", "CREATE", stream, ..] => {
                let from_subject = parts.get(3).map(|s| s.to_string());
                self.api_consumer_create(
                    &msg.account,
                    stream,
                    from_subject,
                    &body,
                    &reply,
                    &type_name,
                    actions,
                );
            }
            // Legacy durable-create endpoint, used by clients (nats.go)
            // when the server reports a version below 2.9.0.
            ["CONSUMER", "DURABLE", "CREATE", stream, consumer] => {
                self.api_consumer_create(
                    &msg.account,
                    stream,
                    Some(consumer.to_string()),
                    &body,
                    &reply,
                    &type_name,
                    actions,
                );
            }
            ["CONSUMER", "INFO", stream, consumer] => {
                self.api_consumer_info(&msg.account, stream, consumer, &reply, &type_name, actions)
            }
            ["CONSUMER", "LIST", stream] => {
                self.api_consumer_list(&msg.account, stream, &body, &reply, &type_name, actions)
            }
            ["CONSUMER", "DELETE", stream, consumer] => self.api_consumer_delete(
                &msg.account,
                stream,
                consumer,
                &reply,
                &type_name,
                actions,
            ),
            ["CONSUMER", "MSG", "NEXT", stream, consumer] => {
                self.api_consumer_msg_next(&msg.account, stream, consumer, &body, &reply, actions)
            }
            _ => api_error_action(
                &msg.account,
                &reply,
                &type_name,
                400,
                10002,
                "unknown JetStream API operation",
                actions,
            ),
        }
    }

    pub(crate) fn api_stream_create(
        &mut self,
        account: &str,
        name: &str,
        body: &Value,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        if !valid_name(name) {
            api_error_action(
                account,
                reply,
                type_name,
                400,
                10002,
                "invalid stream name",
                actions,
            );
            return;
        }
        let subjects: Vec<String> = body
            .get("subjects")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        // Mirror/source configs (same node only). A mirror must not
        // carry subjects or other sources (Go rejects both); a
        // sourced stream may carry subjects (it then also captures
        // direct publishes, as Go does). `external` (cross-account /
        // cross-node) and `subject_transforms` are rejected: same-node
        // only in this implementation.
        let mirror_raw = body.get("mirror").filter(|v| !v.is_null());
        let sources_raw = body
            .get("sources")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut reject = |desc: &str| {
            api_error_action(account, reply, type_name, 400, 10002, desc, actions);
        };
        if mirror_raw.is_some() && !subjects.is_empty() {
            reject("stream mirrors can not contain subjects");
            return;
        }
        if mirror_raw.is_some() && !sources_raw.is_empty() {
            reject("stream mirrors can not also contain other sources");
            return;
        }
        let mut mirror: Option<SourceCfg> = None;
        if let Some(raw) = mirror_raw {
            if raw.get("external").is_some_and(|v| !v.is_null()) {
                reject("cross-node/cross-account mirroring (external) is not supported");
                return;
            }
            if raw
                .get("subject_transforms")
                .and_then(|v| v.as_array())
                .is_some_and(|a| !a.is_empty())
            {
                reject("subject transforms on mirrors are not supported");
                return;
            }
            let Some(cfg) = parse_source_cfg(raw) else {
                reject("mirror name is required");
                return;
            };
            mirror = Some(cfg);
        }
        let mut sources: Vec<SourceCfg> = Vec::new();
        for raw in &sources_raw {
            if raw.get("external").is_some_and(|v| !v.is_null()) {
                reject("cross-node/cross-account sourcing (external) is not supported");
                return;
            }
            if raw
                .get("subject_transforms")
                .and_then(|v| v.as_array())
                .is_some_and(|a| !a.is_empty())
            {
                reject("subject transforms on sources are not supported");
                return;
            }
            let Some(cfg) = parse_source_cfg(raw) else {
                reject("source name is required");
                return;
            };
            if sources.iter().any(|c: &SourceCfg| c.name == cfg.name) {
                reject("duplicate source stream");
                return;
            }
            sources.push(cfg);
        }
        // Every origin must exist in this account. Go accepts a
        // mirror/source of a nonexistent stream and leaves it empty;
        // this implementation rejects it instead (404/10059), so a
        // typo cannot silently create an empty stream.
        let mut origins: Vec<String> = Vec::new();
        if let Some(cfg) = &mirror {
            origins.push(cfg.name.clone());
        }
        origins.extend(sources.iter().map(|c| c.name.clone()));
        for origin in &origins {
            match self.stream_for(origin, account) {
                Some(_) => {}
                None => {
                    let (code, err_code, desc) = err_not_found("stream");
                    api_error_action(account, reply, type_name, code, err_code, &desc, actions);
                    return;
                }
            }
        }
        if self.would_cycle(name, &origins) {
            reject("detected cycle");
            return;
        }
        if mirror.is_none() && sources.is_empty() && subjects.is_empty() {
            api_error_action(
                account,
                reply,
                type_name,
                400,
                10002,
                "stream subjects are required",
                actions,
            );
            return;
        }
        if self.streams.contains_key(name) {
            api_error_action(
                account,
                reply,
                type_name,
                400,
                10058,
                "stream name already in use",
                actions,
            );
            return;
        }
        let retention = body
            .get("retention")
            .and_then(|v| v.as_str())
            .unwrap_or("limits")
            .to_string();
        if retention != "limits" && retention != "workqueue" {
            api_error_action(
                account,
                reply,
                type_name,
                400,
                10002,
                "only limits and workqueue retention are supported",
                actions,
            );
            return;
        }
        let storage = body
            .get("storage")
            .and_then(|v| v.as_str())
            .unwrap_or("memory")
            .to_string();
        let created_ns = now_ns();
        let stream = Stream {
            name: name.to_string(),
            account: account.to_string(),
            created_ns,
            subjects,
            retention,
            max_msgs: body.get("max_msgs").and_then(|v| v.as_i64()).unwrap_or(0),
            max_bytes: body.get("max_bytes").and_then(|v| v.as_i64()).unwrap_or(0),
            max_age_ns: body.get("max_age").and_then(|v| v.as_i64()).unwrap_or(0),
            max_msgs_per_subject: body
                .get("max_msgs_per_subject")
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
            deleted: std::collections::HashSet::new(),
            storage,
            messages: VecDeque::new(),
            base_seq: 1,
            msg_ids: HashMap::new(),
            consumers: HashMap::new(),
            mirror: mirror.clone(),
            sources: sources.clone(),
            source_token: make_source_token(name, created_ns),
        };
        self.streams.insert(name.to_string(), stream);
        // Backfill (synchronous, under the engine lock, before the
        // create response: backfill + link registration are atomic
        // with respect to publishes, so nothing is lost or doubled.
        // Go backfills asynchronously; the final state matches.)
        if let Some(cfg) = &mirror {
            self.backfill(name, account, cfg, true);
        }
        for cfg in &sources {
            self.backfill(name, account, cfg, false);
        }
        let info = self.streams.get(name).unwrap().info_json();
        api_response(account, reply, type_name, info, actions);
    }

    pub(crate) fn api_stream_info(
        &mut self,
        account: &str,
        name: &str,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        match self.stream_for(name, account) {
            Some(stream) => {
                let info = stream.info_json();
                api_response(account, reply, type_name, info, actions);
            }
            None => {
                let (code, err_code, desc) = err_not_found("stream");
                api_error_action(account, reply, type_name, code, err_code, &desc, actions);
            }
        }
    }

    pub(crate) fn api_stream_list(
        &mut self,
        account: &str,
        body: &Value,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        let offset = body.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let mut names: Vec<String> = self
            .streams
            .values()
            .filter(|s| s.account == account)
            .map(|s| s.name.clone())
            .collect();
        names.sort();
        let total = names.len();
        let streams: Vec<Value> = names
            .iter()
            .skip(offset)
            .filter_map(|n| self.streams.get(n).map(|s| s.info_json()))
            .collect();
        api_response(
            account,
            reply,
            type_name,
            json!({ "streams": streams, "total": total, "offset": offset, "limit": 1024 }),
            actions,
        );
    }

    pub(crate) fn api_stream_names(
        &mut self,
        account: &str,
        body: &Value,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        let offset = body.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let subject = body.get("subject").and_then(|v| v.as_str());
        let mut names: Vec<String> = self
            .streams
            .values()
            .filter(|s| s.account == account)
            .filter(|s| match subject {
                Some(subj) => s.subjects.iter().any(|p| subject_matches(p, subj)),
                None => true,
            })
            .map(|s| s.name.clone())
            .collect();
        names.sort();
        let total = names.len();
        let streams: Vec<Value> = names
            .iter()
            .skip(offset)
            .map(|n| Value::String(n.clone()))
            .collect();
        api_response(
            account,
            reply,
            type_name,
            json!({ "streams": streams, "total": total, "offset": offset, "limit": 1024 }),
            actions,
        );
    }

    pub(crate) fn api_stream_delete(
        &mut self,
        account: &str,
        name: &str,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        if self.stream_for(name, account).is_some() && self.streams.remove(name).is_some() {
            // Deleting an origin kills live links to it permanently:
            // dependents keep their config and their copies, but the
            // link never revives, even if a same-named origin is
            // created later (Go behaviour, probed against v2.15.1).
            for s in self.streams.values_mut() {
                if let Some(cfg) = &mut s.mirror {
                    if cfg.name == name {
                        cfg.live = false;
                    }
                }
                for cfg in &mut s.sources {
                    if cfg.name == name {
                        cfg.live = false;
                    }
                }
            }
            api_response(
                account,
                reply,
                type_name,
                json!({ "success": true }),
                actions,
            );
        } else {
            let (code, err_code, desc) = err_not_found("stream");
            api_error_action(account, reply, type_name, code, err_code, &desc, actions);
        }
    }

    pub(crate) fn api_stream_purge(
        &mut self,
        account: &str,
        name: &str,
        body: &Value,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        let Some(stream) = self.stream_for_mut(name, account) else {
            let (code, err_code, desc) = err_not_found("stream");
            api_error_action(account, reply, type_name, code, err_code, &desc, actions);
            return;
        };
        let filter = body
            .get("filter")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let purged = match filter {
            None => {
                let n = stream.messages.len() as u64;
                stream.messages.clear();
                stream.base_seq += n;
                // Consumers restart after the purge point.
                for c in stream.consumers.values_mut() {
                    c.next_stream_seq = stream.base_seq + stream.messages.len() as u64;
                    c.pending.clear();
                    c.redeliver.clear();
                }
                n
            }
            Some(f) => {
                // Filtered purge removes matching messages and compacts
                // the remaining ones to contiguous sequences. Consumer
                // positions reset to the new base (documented gap:
                // filtered purge is destructive to consumer state).
                let before = stream.messages.len();
                stream.messages.retain(|m| !subject_matches(&f, &m.subject));
                let removed = before - stream.messages.len();
                let base = stream
                    .messages
                    .front()
                    .map(|m| m.seq)
                    .unwrap_or(stream.base_seq + before as u64);
                stream.base_seq = base;
                for (i, m) in stream.messages.iter_mut().enumerate() {
                    m.seq = base + i as u64;
                }
                for c in stream.consumers.values_mut() {
                    c.pending.clear();
                    c.redeliver.clear();
                    c.next_stream_seq = base;
                    c.ack_floor_stream_seq = 0;
                    c.ack_floor_consumer_seq = 0;
                }
                removed as u64
            }
        };
        api_response(
            account,
            reply,
            type_name,
            json!({ "success": true, "purged": purged }),
            actions,
        );
    }

    pub(crate) fn api_stream_msg_get(
        &mut self,
        account: &str,
        name: &str,
        body: &Value,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        let Some(stream) = self.stream_for(name, account) else {
            let (code, err_code, desc) = err_not_found("stream");
            api_error_action(account, reply, type_name, code, err_code, &desc, actions);
            return;
        };
        let found = if let Some(seq) = body.get("seq").and_then(|v| v.as_u64()) {
            stream.get(seq)
        } else if let Some(subj) = body.get("last_by_subj").and_then(|v| v.as_str()) {
            if subj.contains('*') || subj.contains('>') {
                stream
                    .messages
                    .iter()
                    .rev()
                    .find(|m| subject_matches(subj, &m.subject))
            } else {
                stream.messages.iter().rev().find(|m| m.subject == subj)
            }
        } else {
            None
        };
        match found {
            Some(m) => {
                let mut message = json!({
                    "seq": m.seq,
                    "subject": m.subject,
                    "data": base64_encode(&m.payload),
                    "time": rfc3339(m.time_ns),
                });
                if let Some(h) = &m.headers {
                    // async-nats/nats.go read the raw header block from
                    // base64 `hdrs`; the parsed map is a convenience extra.
                    message["hdrs"] = Value::String(base64_encode(h));
                    message["headers"] = headers_json(h);
                }
                api_response(
                    account,
                    reply,
                    type_name,
                    json!({ "message": message }),
                    actions,
                );
            }
            None => {
                let (code, err_code, desc) = err_not_found("message");
                api_error_action(account, reply, type_name, code, err_code, &desc, actions);
            }
        }
    }
}

/// Called for every message before core delivery. Returns true when
/// JetStream consumed the message (API request or ack) so it must not
/// also be delivered as an ordinary message.
pub fn handle_message(
    shared: &std::sync::Arc<Shared>,
    js: &std::sync::Arc<JetStream>,
    msg: &Message,
    origin: Origin,
) -> bool {
    let js_enabled_for_account = shared.config.jetstream_enabled_for_account(&msg.account);
    let (consumed, actions) = {
        let mut engine = js.engine.lock().unwrap();
        engine.process(msg, origin, js_enabled_for_account)
    };
    // Persist after every mutating operation (capture, API, ack) so
    // a restart restores the engine. Cheap at this scale (whole-engine
    // JSON); a no-op without a store dir.
    js.persist();
    for action in actions {
        match action {
            Action::Deliver(m) => {
                deliver(shared, &m, Origin::Internal, None);
            }
        }
    }
    consumed
}
