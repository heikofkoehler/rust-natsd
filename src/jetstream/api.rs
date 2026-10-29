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

/// Stream-config boolean: present JSON bool, else `default`.
/// async-nats serializes `allow_rollup` as `allow_rollup_hdrs`;
/// Go/nats.go use the same five names.
pub(crate) fn cfg_bool(body: &Value, key: &str, default: bool) -> bool {
    body.get(key).and_then(|v| v.as_bool()).unwrap_or(default)
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
        has_peers: bool,
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
            self.process_api(rest, msg, js_enabled_for_account, has_peers, &mut actions);
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
        has_peers: bool,
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
            ["STREAM", "UPDATE", ..] => "stream_update",
            ["STREAM", "INFO", ..] => "stream_info",
            ["STREAM", "LIST"] => "stream_list",
            ["STREAM", "NAMES"] => "stream_names",
            ["STREAM", "DELETE", ..] => "stream_delete",
            ["STREAM", "PURGE", ..] => "stream_purge",
            ["STREAM", "MSG", "GET", ..] => "stream_msg_get",
            ["STREAM", "MSG", "DELETE", ..] => "stream_msg_delete",
            ["DIRECT", "GET", ..] => "direct_get",
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
                self.api_stream_create(
                    &msg.account,
                    name,
                    &body,
                    &reply,
                    &type_name,
                    has_peers,
                    actions,
                );
            }
            ["STREAM", "CREATE"] => {
                let name = body
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                self.api_stream_create(
                    &msg.account,
                    &name,
                    &body,
                    &reply,
                    &type_name,
                    has_peers,
                    actions,
                );
            }
            ["STREAM", "UPDATE", name] => {
                self.api_stream_update(&msg.account, name, &body, &reply, &type_name, actions);
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
            ["STREAM", "MSG", "DELETE", name] => {
                self.api_stream_msg_delete(&msg.account, name, &body, &reply, &type_name, actions)
            }
            // $JS.API.DIRECT.GET.<stream> (JSON body: seq /
            // next_by_subj / last_by_subj) and
            // $JS.API.DIRECT.GET.<stream>.<last_by_subj subject...>
            // (empty body). The subject tokens are joined back with
            // '.' because KV subjects themselves contain dots.
            ["DIRECT", "GET", name, rest @ ..] => {
                let last = if rest.is_empty() {
                    None
                } else {
                    Some(rest.join("."))
                };
                self.api_direct_get(&msg.account, name, last.as_deref(), &body, &reply, actions);
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn api_stream_create(
        &mut self,
        account: &str,
        name: &str,
        body: &Value,
        reply: &str,
        type_name: &str,
        has_peers: bool,
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
        // Phase 34: replicated streams are refused, never silently
        // stored as R1. The Raft layer (raft.rs / js_cluster.rs)
        // exists and is tested at the group layer, but it is NOT
        // wired into the JetStream publish path yet, so an acked
        // publish could not be quorum-committed. Refusing here —
        // with or without cluster peers — is the honest behaviour
        // until the wiring phase lands; advertising R3 while
        // storing R1 is a forbidden shortcut.
        if body
            .get("num_replicas")
            .and_then(|v| v.as_u64())
            .is_some_and(|n| n > 1)
        {
            api_error_action(
                account,
                reply,
                type_name,
                400,
                10002,
                "replicated streams (num_replicas > 1) are not yet supported: the Phase 34 Raft layer is experimental and not wired into the JetStream publish path, so this request is refused rather than stored as a single replica",
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
        // typo cannot silently create an empty stream. Phase 32
        // exception: when the node has route/leaf peers, an origin
        // absent locally is treated as remote — the creating node
        // cannot synchronously verify a remote stream's existence
        // (STREAM.CREATE is answered under the engine lock, with no
        // network round trip), so the link is created in remote mode
        // and the mirror loop's consumer creation on the origin node
        // is the existence check: a truly nonexistent remote origin
        // leaves the mirror empty (Go's behaviour) and the loop
        // retries, rather than failing creation. Without peers the
        // 404 deviation is preserved exactly.
        let mut origins: Vec<String> = Vec::new();
        if let Some(cfg) = &mirror {
            origins.push(cfg.name.clone());
        }
        origins.extend(sources.iter().map(|c| c.name.clone()));
        for origin in &origins {
            match self.stream_for(origin, account) {
                Some(_) => {}
                None => {
                    if has_peers {
                        if let Some(cfg) = &mut mirror {
                            if cfg.name == *origin {
                                cfg.remote = true;
                            }
                        }
                        for cfg in &mut sources {
                            if cfg.name == *origin {
                                cfg.remote = true;
                            }
                        }
                    } else {
                        let (code, err_code, desc) = err_not_found("stream");
                        api_error_action(account, reply, type_name, code, err_code, &desc, actions);
                        return;
                    }
                }
            }
        }
        if self.would_cycle(account, name, &origins) {
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
        if self
            .streams
            .contains_key(&(account.to_string(), name.to_string()))
        {
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
            allow_direct: cfg_bool(body, "allow_direct", false),
            mirror_direct: cfg_bool(body, "mirror_direct", false),
            deny_delete: cfg_bool(body, "deny_delete", false),
            deny_purge: cfg_bool(body, "deny_purge", false),
            allow_rollup_hdrs: cfg_bool(body, "allow_rollup_hdrs", false),
            remote_progress: HashMap::new(),
            wal_rewrite: false,
        };
        self.streams
            .insert((account.to_string(), name.to_string()), stream);
        // Backfill (synchronous, under the engine lock, before the
        // create response: backfill + link registration are atomic
        // with respect to publishes, so nothing is lost or doubled.
        // Go backfills asynchronously; the final state matches.)
        // Remote links are not backfilled here: the origin's messages
        // are not on this node, and the mirror loop pulls them (its
        // consumer's deliver policy replays the same backfill range).
        if let Some(cfg) = &mirror {
            if !cfg.remote {
                self.backfill(name, account, cfg, true);
            }
        }
        for cfg in &sources {
            if !cfg.remote {
                self.backfill(name, account, cfg, false);
            }
        }
        let info = self.stream_for(name, account).unwrap().info_json();
        api_response(account, reply, type_name, info, actions);
    }

    /// STREAM.UPDATE: replace mutable config on an existing stream,
    /// preserving messages, consumers, creation time, and the
    /// mirror/source links. Applied: subjects, retention (limits /
    /// workqueue only), max_msgs, max_bytes, max_age,
    /// max_msgs_per_subject, and the five Phase 31 flags.
    /// Deliberately not applied (Go restricts them too, in various
    /// ways): storage changes are ignored, and a mirror/sources
    /// config naming a different origin than the current one is
    /// rejected — relinking a live stream is not implemented.
    pub(crate) fn api_stream_update(
        &mut self,
        account: &str,
        name: &str,
        body: &Value,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        // Phase 34: same refusal as STREAM.CREATE — an update
        // must not raise a stream's replica count into unwired
        // territory either.
        if body
            .get("num_replicas")
            .and_then(|v| v.as_u64())
            .is_some_and(|n| n > 1)
        {
            api_error_action(
                account,
                reply,
                type_name,
                400,
                10002,
                "replicated streams (num_replicas > 1) are not yet supported: the Phase 34 Raft layer is experimental and not wired into the JetStream publish path, so this request is refused rather than stored as a single replica",
                actions,
            );
            return;
        }
        if let Some(body_name) = body.get("name").and_then(|v| v.as_str()) {
            if body_name != name {
                api_error_action(
                    account,
                    reply,
                    type_name,
                    400,
                    10002,
                    "stream name in request does not match update subject",
                    actions,
                );
                return;
            }
        }
        let Some(stream) = self.stream_for_mut(name, account) else {
            let (code, err_code, desc) = err_not_found("stream");
            api_error_action(account, reply, type_name, code, err_code, &desc, actions);
            return;
        };
        // Mirror/source relink rejection (name comparison only; a
        // same-named link keeps its existing filter/start config).
        let check_link =
            |raw: Option<&Value>, current: &Option<SourceCfg>| -> Result<(), &'static str> {
                let Some(raw) = raw.filter(|v| !v.is_null()) else {
                    return Ok(());
                };
                let Some(new_name) = raw.get("name").and_then(|v| v.as_str()) else {
                    return Err("mirror/source name is required");
                };
                match current {
                    Some(cfg) if cfg.name == new_name => Ok(()),
                    Some(_) => Err("mirror/source changes are not supported"),
                    None => Err("mirror/source changes are not supported"),
                }
            };
        if let Err(desc) = check_link(body.get("mirror"), &stream.mirror) {
            api_error_action(account, reply, type_name, 400, 10002, desc, actions);
            return;
        }
        if let Some(arr) = body.get("sources").and_then(|v| v.as_array()) {
            if !arr.is_empty() {
                if arr.len() != stream.sources.len() {
                    api_error_action(
                        account,
                        reply,
                        type_name,
                        400,
                        10002,
                        "mirror/source changes are not supported",
                        actions,
                    );
                    return;
                }
                for (raw, cur) in arr.iter().zip(stream.sources.iter()) {
                    if raw.get("name").and_then(|v| v.as_str()) != Some(cur.name.as_str()) {
                        api_error_action(
                            account,
                            reply,
                            type_name,
                            400,
                            10002,
                            "mirror/source changes are not supported",
                            actions,
                        );
                        return;
                    }
                }
            }
        }
        if let Some(subjects) = body.get("subjects").and_then(|v| v.as_array()) {
            let subjects: Vec<String> = subjects
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
            if subjects.is_empty() && stream.mirror.is_none() && stream.sources.is_empty() {
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
            if !subjects.is_empty() && stream.mirror.is_some() {
                api_error_action(
                    account,
                    reply,
                    type_name,
                    400,
                    10002,
                    "stream mirrors can not contain subjects",
                    actions,
                );
                return;
            }
            if !subjects.is_empty() {
                stream.subjects = subjects;
            }
        }
        if let Some(retention) = body.get("retention").and_then(|v| v.as_str()) {
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
            stream.retention = retention.to_string();
        }
        if let Some(v) = body.get("max_msgs").and_then(|v| v.as_i64()) {
            stream.max_msgs = v;
        }
        if let Some(v) = body.get("max_bytes").and_then(|v| v.as_i64()) {
            stream.max_bytes = v;
        }
        if let Some(v) = body.get("max_age").and_then(|v| v.as_i64()) {
            stream.max_age_ns = v;
        }
        if let Some(v) = body.get("max_msgs_per_subject").and_then(|v| v.as_i64()) {
            stream.max_msgs_per_subject = v;
        }
        stream.allow_direct = cfg_bool(body, "allow_direct", stream.allow_direct);
        stream.mirror_direct = cfg_bool(body, "mirror_direct", stream.mirror_direct);
        stream.deny_delete = cfg_bool(body, "deny_delete", stream.deny_delete);
        stream.deny_purge = cfg_bool(body, "deny_purge", stream.deny_purge);
        stream.allow_rollup_hdrs = cfg_bool(body, "allow_rollup_hdrs", stream.allow_rollup_hdrs);
        let info = stream.info_json();
        api_response(account, reply, type_name, info, actions);
    }

    /// STREAM.MSG.DELETE: mark one sequence deleted in place (the
    /// sequence gap stays, as Go does: first_seq jumps past a
    /// deleted prefix). Error shapes probed against Go v2.15.1:
    /// missing stream 404/10059; deny_delete 500/10057 "message
    /// delete not permitted"; seq past the end 500/10057 "stream
    /// store EOF"; deleted / pre-base seq 500/10057 "no message
    /// found". `no_erase` is accepted and ignored: the message is
    /// unretrievable either way, which is the client-visible part.
    pub(crate) fn api_stream_msg_delete(
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
        if stream.deny_delete {
            api_error_action(
                account,
                reply,
                type_name,
                500,
                10057,
                "message delete not permitted",
                actions,
            );
            return;
        }
        let seq = body.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
        if seq == 0 || seq > stream.last_seq() {
            api_error_action(
                account,
                reply,
                type_name,
                500,
                10057,
                "stream store EOF",
                actions,
            );
            return;
        }
        if seq < stream.base_seq || stream.deleted.contains(&seq) {
            api_error_action(
                account,
                reply,
                type_name,
                500,
                10057,
                "no message found",
                actions,
            );
            return;
        }
        stream.deleted.insert(seq);
        api_response(
            account,
            reply,
            type_name,
            json!({ "success": true }),
            actions,
        );
    }

    /// DIRECT.GET: the raw-message endpoint modern KV
    /// getters/watchers use when the stream has allow_direct (nats.go
    /// `DirectGet`, async-nats `direct_get*`). Unlike MSG.GET it
    /// returns the payload itself, with `Nats-Stream`,
    /// `Nats-Sequence`, `Nats-Subject`, `Nats-Time-Stamp` headers
    /// plus the stored message's own headers, exactly as Go
    /// v2.15.1 does (probed). Error shapes are HMSG status lines,
    /// because both clients parse the status from the version line:
    /// - not servable (stream missing, or allow_direct off and not
    ///   a mirror_direct mirror): `NATS/1.0 503` with no payload —
    ///   Go answers this way too (no direct-get responder), so
    ///   nats.go reports "no responders" and async-nats reports
    ///   NoResponders on both servers.
    /// - message not found: `NATS/1.0 404 Message Not Found`.
    /// - empty request (no seq / next_by_subj / last_by_subj):
    ///   `NATS/1.0 408 Empty Request`.
    ///
    /// Go difference, documented: Go lets a mirror answer direct
    /// gets addressed to its origin (the `Nats-Stream` header then
    /// names the mirror, which nats.go explicitly tolerates), and
    /// answers nothing for a mirror's own name. Rust answers the
    /// requested stream directly when it is servable, including a
    /// mirror_direct mirror's own copies — the client-visible
    /// outcome (message + valid headers) is the same, and the
    /// mirror-name case is a permissive superset, not a gap.
    pub(crate) fn api_direct_get(
        &mut self,
        account: &str,
        name: &str,
        last_subject: Option<&str>,
        body: &Value,
        reply: &str,
        actions: &mut Vec<Action>,
    ) {
        let no_responders = |actions: &mut Vec<Action>| {
            actions.push(Action::Deliver(
                Message::new(
                    reply,
                    None,
                    Some(Bytes::from("NATS/1.0 503\r\n\r\n")),
                    Bytes::new(),
                )
                .with_account(account),
            ));
        };
        let status_error = |status_line: &str, actions: &mut Vec<Action>| {
            actions.push(Action::Deliver(
                Message::new(
                    reply,
                    None,
                    Some(Bytes::from(format!("{status_line}\r\n\r\n"))),
                    Bytes::new(),
                )
                .with_account(account),
            ));
        };
        let Some(stream) = self.stream_for(name, account) else {
            no_responders(actions);
            return;
        };
        let servable = stream.allow_direct || (stream.mirror.is_some() && stream.mirror_direct);
        if !servable {
            no_responders(actions);
            return;
        }
        let found = if let Some(subj) = last_subject {
            stream.last_for_subject(subj).map(|m| {
                (
                    m.seq,
                    m.subject.clone(),
                    m.headers.clone(),
                    m.payload.clone(),
                    m.time_ns,
                )
            })
        } else if let Some(subj) = body.get("last_by_subj").and_then(|v| v.as_str()) {
            stream.last_for_subject(subj).map(|m| {
                (
                    m.seq,
                    m.subject.clone(),
                    m.headers.clone(),
                    m.payload.clone(),
                    m.time_ns,
                )
            })
        } else if let Some(next) = body.get("next_by_subj").and_then(|v| v.as_str()) {
            let from = body.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
            stream
                .messages
                .iter()
                .filter(|m| !stream.deleted.contains(&m.seq))
                .filter(|m| m.seq >= from || from == 0)
                .find(|m| subject_matches(next, &m.subject))
                .map(|m| {
                    (
                        m.seq,
                        m.subject.clone(),
                        m.headers.clone(),
                        m.payload.clone(),
                        m.time_ns,
                    )
                })
        } else if let Some(seq) = body.get("seq").and_then(|v| v.as_u64()) {
            if seq == 0 {
                status_error("NATS/1.0 408 Empty Request", actions);
                return;
            }
            stream.get(seq).map(|m| {
                (
                    m.seq,
                    m.subject.clone(),
                    m.headers.clone(),
                    m.payload.clone(),
                    m.time_ns,
                )
            })
        } else {
            status_error("NATS/1.0 408 Empty Request", actions);
            return;
        };
        match found {
            Some((seq, subject, stored_headers, payload, time_ns)) => {
                let mut block = format!(
                    "NATS/1.0\r\nNats-Stream: {name}\r\nNats-Sequence: {seq}\r\nNats-Subject: {subject}\r\nNats-Time-Stamp: {}\r\n",
                    rfc3339(time_ns),
                );
                if let Some(h) = &stored_headers {
                    let text = String::from_utf8_lossy(h);
                    for line in text.lines() {
                        if line.starts_with("NATS/") || line.is_empty() {
                            continue;
                        }
                        // The four direct-get headers above win over
                        // any same-named stored header.
                        let key = line.split(':').next().unwrap_or("").trim();
                        if key.eq_ignore_ascii_case("Nats-Stream")
                            || key.eq_ignore_ascii_case("Nats-Sequence")
                            || key.eq_ignore_ascii_case("Nats-Subject")
                            || key.eq_ignore_ascii_case("Nats-Time-Stamp")
                        {
                            continue;
                        }
                        block.push_str(line);
                        block.push_str("\r\n");
                    }
                }
                block.push_str("\r\n");
                actions.push(Action::Deliver(
                    Message::new(reply, None, Some(Bytes::from(block)), payload)
                        .with_account(account),
                ));
            }
            None => status_error("NATS/1.0 404 Message Not Found", actions),
        }
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
            .filter_map(|n| self.stream_for(n, account).map(|s| s.info_json()))
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
        if self
            .streams
            .remove(&(account.to_string(), name.to_string()))
            .is_some()
        {
            // Deleting an origin kills live links to it permanently:
            // dependents keep their config and their copies, but the
            // link never revives, even if a same-named origin is
            // created later (Go behaviour, probed against v2.15.1).
            for s in self.streams.values_mut().filter(|s| s.account == account) {
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
        // Go's purge-denied shape, probed: the error plus
        // `"purged": 0` in the same body (not a bare error).
        if stream.deny_purge {
            let body = json!({
                "type": type_name,
                "error": { "code": 500, "err_code": 10110, "description": "stream purge not permitted" },
                "purged": 0,
            });
            actions.push(Action::Deliver(
                Message::new(reply, None, None, Bytes::from(body.to_string()))
                    .with_account(account),
            ));
            return;
        }
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
                // Renumbered in place: the WAL diff cannot track
                // this append-only, so force a msgs.wal rewrite.
                stream.wal_rewrite = true;
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
            stream.last_for_subject(subj)
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

/// Whether a `$JS.API` / `$JS.ACK` message for a stream this node
/// does not host should be forwarded to another node instead of
/// being answered locally with a 404. Phase 32: previously
/// `handle_message` consumed every API request locally, so the
/// `$JS.API.>` interest the server advertises to routes/leafs never
/// actually carried a request to the hosting node (the README's
/// "routes cluster-wide" claim was aspirational). Now a
/// stream-referencing request/ack from a local client, for a stream
/// absent from this node's engine, is not consumed when the node has
/// route/leaf peers — `deliver` then forwards it via the peers'
/// `$JS.API.>` / `$JS.ACK.>` interest, and the hosting node answers
/// (its origin is Route/Leaf, so it always processes locally and
/// cannot bounce the request back). `STREAM.CREATE`, `STREAM.LIST`,
/// `STREAM.NAMES` and account `INFO` are never forwarded: creation
/// is always local (a remote mirror is created locally in remote
/// mode, see `api_stream_create`), and list/names are per-node.
/// Only Local origin forwards; Route/Leaf/Internal messages are
/// always processed locally, which is also the loop guard.
fn should_forward_api(shared: &Shared, js: &JetStream, msg: &Message, origin: Origin) -> bool {
    if !matches!(origin, Origin::Local(_, _)) {
        return false;
    }
    let has_peers =
        !shared.routes.lock().unwrap().is_empty() || !shared.leafs.lock().unwrap().is_empty();
    if !has_peers {
        return false;
    }
    let stream_name: Option<&str> = if let Some(rest) = msg.subject.strip_prefix("$JS.ACK.") {
        rest.split('.').next()
    } else if let Some(rest) = msg.subject.strip_prefix("$JS.API.") {
        let parts: Vec<&str> = rest.split('.').collect();
        match parts.as_slice() {
            ["STREAM", "INFO", name]
            | ["STREAM", "UPDATE", name]
            | ["STREAM", "DELETE", name]
            | ["STREAM", "PURGE", name]
            | ["STREAM", "MSG", "GET", name]
            | ["STREAM", "MSG", "DELETE", name]
            | ["DIRECT", "GET", name, ..]
            | ["CONSUMER", "CREATE", name, ..]
            | ["CONSUMER", "DURABLE", "CREATE", name, ..]
            | ["CONSUMER", "INFO", name, ..]
            | ["CONSUMER", "LIST", name]
            | ["CONSUMER", "DELETE", name, ..]
            | ["CONSUMER", "MSG", "NEXT", name, ..] => Some(name),
            _ => None,
        }
    } else {
        None
    };
    let Some(name) = stream_name else {
        return false;
    };
    let engine = js.engine.lock().unwrap();
    engine.stream_for(name, &msg.account).is_none()
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
    // Phase 32 forwarding (see `should_forward_api`): not consumed,
    // so `deliver` forwards the request to the hosting node.
    if should_forward_api(shared, js, msg, origin) {
        return false;
    }
    let js_enabled_for_account = shared.config.jetstream_enabled_for_account(&msg.account);
    let has_peers =
        !shared.routes.lock().unwrap().is_empty() || !shared.leafs.lock().unwrap().is_empty();
    let (consumed, actions) = {
        let mut engine = js.engine.lock().unwrap();
        engine.process(msg, origin, js_enabled_for_account, has_peers)
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
