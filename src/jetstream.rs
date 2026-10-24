//! JetStream engine (Phase 9): in-memory streams, consumers (push and
//! pull), acknowledgements with redelivery, and the `$JS.API` surface.
//!
//! Scope notes (also in the README):
//! - Storage is in-memory only. A stream created with file storage is
//!   accepted but its data lives in memory and is lost on restart.
//! - JetStream is per-node: streams capture messages published on the
//!   hosting node, but `$JS.API` requests and `$JS.ACK` messages route
//!   cluster-wide via the `$JS.API.>` interest the server advertises.
//! - No replication, no mirroring/sourcing, no workqueue retention.
//! - KV is not a separate implementation: a KV bucket is a plain stream
//!   named `KV_<bucket>` with subjects `$KV.<bucket>.>`; the primitives
//!   KV needs (headers on stored messages, `MSG.GET` with `last_by_subj`)
//!   are implemented, but the nats.go/async-nats KV client helpers are
//!   not verified against this server.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde_json::{json, Value};

use crate::server::{deliver, Message, Origin, Shared};
use crate::subject::subject_matches;

/// Default dedup window for `Nats-Msg-Id` (matches nats-server's 2m).
const DEDUP_WINDOW: Duration = Duration::from_secs(120);
/// Default ack wait when a consumer does not set one.
const DEFAULT_ACK_WAIT: Duration = Duration::from_secs(30);
/// Redelivery / pull-expiry tick interval.
const TICK: Duration = Duration::from_millis(100);

/// In-memory JetStream engine handle.
pub struct JetStream {
    engine: Mutex<Engine>,
}

impl JetStream {
    /// Totals for /varz's jetstream block.
    pub(crate) fn snapshot_totals(&self) -> Value {
        let engine = self.engine.lock().unwrap();
        let mut messages = 0u64;
        let mut bytes = 0u64;
        let mut consumers = 0usize;
        for s in engine.streams.values() {
            messages += s.messages.len() as u64;
            bytes += s.bytes();
            consumers += s.consumers.len();
        }
        json!({
            "memory": 0,
            "storage": 0,
            "reserved_memory": 0,
            "reserved_storage": 0,
            "accounts": 1,
            "ha_assets": 0,
            "api": {"total": 0, "errors": 0},
            "streams": engine.streams.len(),
            "consumers": consumers,
            "messages": messages,
            "bytes": bytes,
        })
    }

    /// Full snapshot for /jsz.
    pub(crate) fn snapshot(&self) -> Value {
        let engine = self.engine.lock().unwrap();
        let mut streams = Vec::new();
        let mut total_messages = 0u64;
        let mut total_bytes = 0u64;
        let mut total_consumers = 0usize;
        for s in engine.streams.values() {
            total_messages += s.messages.len() as u64;
            total_bytes += s.bytes();
            total_consumers += s.consumers.len();
            streams.push(json!({
                "name": s.name,
                "messages": s.messages.len(),
                "bytes": s.bytes(),
                "first_seq": s.first_seq(),
                "last_seq": s.last_seq(),
                "consumer_count": s.consumers.len(),
                "consumers": s.consumers.keys().cloned().collect::<Vec<_>>(),
            }));
        }
        json!({
            "config": {"max_memory": 0, "max_file": 0, "store_dir": ""},
            "streams": streams,
            "total_streams": engine.streams.len(),
            "total_messages": total_messages,
            "total_bytes": total_bytes,
            "total_consumers": total_consumers,
        })
    }

    pub fn new() -> Self {
        JetStream {
            engine: Mutex::new(Engine {
                streams: HashMap::new(),
            }),
        }
    }
}

impl Default for JetStream {
    fn default() -> Self {
        Self::new()
    }
}

/// Something the engine wants the server core to send after the engine
/// lock is released (delivering while holding the lock would deadlock:
/// `deliver` re-enters `handle_message`).
enum Action {
    Deliver(Message),
}

struct Engine {
    streams: HashMap<String, Stream>,
}

struct StoredMsg {
    seq: u64,
    subject: String,
    headers: Option<Bytes>,
    payload: Bytes,
    /// Unix nanos, for max-age eviction.
    time_ns: u64,
}

struct Stream {
    name: String,
    /// Account that owns this stream; capture and API lookups are
    /// scoped to it. Stream names are node-global in this
    /// implementation (a second account creating a same-named stream
    /// gets "stream name already in use").
    account: String,
    created_ns: u64,
    subjects: Vec<String>,
    retention: String,
    max_msgs: i64,
    max_bytes: i64,
    max_age_ns: i64,
    storage: String,
    messages: VecDeque<StoredMsg>,
    /// Sequence number of messages[0]; next sequence is base_seq + len.
    base_seq: u64,
    msg_ids: HashMap<String, (u64, Instant)>,
    consumers: HashMap<String, Consumer>,
}

#[derive(Clone, Copy, PartialEq)]
enum AckPolicy {
    Explicit,
    None,
    All,
}

struct Pending {
    stream_seq: u64,
    delivered: u64,
    deadline: Instant,
}

struct WaitingPull {
    reply: String,
    remaining: usize,
    delivered: usize,
    deadline: Instant,
}

struct Consumer {
    name: String,
    created_ns: u64,
    deliver_subject: Option<String>,
    filter_subject: Option<String>,
    ack_policy: AckPolicy,
    ack_wait: Duration,
    max_deliver: i64,
    max_ack_pending: i64,
    deliver_policy: String,
    next_stream_seq: u64,
    consumer_seq: u64,
    pending: HashMap<u64, Pending>,
    redeliver: VecDeque<u64>,
    waiting_pulls: VecDeque<WaitingPull>,
    ack_floor_stream_seq: u64,
    ack_floor_consumer_seq: u64,
    num_redelivered: u64,
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn base64_encode(data: &[u8]) -> String {
    const ALPH: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPH[(n >> 18 & 63) as usize] as char);
        out.push(ALPH[(n >> 12 & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPH[(n >> 6 & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPH[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// Format unix nanos as RFC3339 (UTC), e.g. `2026-10-24T01:23:45.123456789Z`.
/// Implemented by hand (civil-from-days) so MSG.GET responses carry the
/// `time` field async-nats requires, without a date dependency.
pub(crate) fn rfc3339(time_ns: u64) -> String {
    let secs = (time_ns / 1_000_000_000) as i64;
    let nanos = time_ns % 1_000_000_000;
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    format!(
        "{year:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{nanos:09}Z",
        secs_of_day / 3600,
        secs_of_day / 60 % 60,
        secs_of_day % 60,
    )
}

/// Case-insensitive header lookup on a message's raw header block.
fn header_get(msg: &Message, name: &str) -> Option<String> {
    msg.header_map()
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
}

/// Parse a raw header block into a JSON object of name -> [values].
fn headers_json(headers: &Bytes) -> Value {
    let mut map = serde_json::Map::new();
    let text = String::from_utf8_lossy(headers);
    for line in text.lines() {
        if line.starts_with("NATS/") || line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            let entry = map
                .entry(k.trim().to_string())
                .or_insert_with(|| Value::Array(Vec::new()));
            if let Value::Array(arr) = entry {
                arr.push(Value::String(v.trim().to_string()));
            }
        }
    }
    Value::Object(map)
}

impl Stream {
    fn last_seq(&self) -> u64 {
        if self.messages.is_empty() {
            0
        } else {
            self.base_seq + self.messages.len() as u64 - 1
        }
    }

    fn first_seq(&self) -> u64 {
        if self.messages.is_empty() {
            self.base_seq + self.messages.len() as u64
        } else {
            self.base_seq
        }
    }

    fn bytes(&self) -> u64 {
        self.messages
            .iter()
            .map(|m| {
                m.payload.len() as u64 + m.headers.as_ref().map(|h| h.len() as u64).unwrap_or(0)
            })
            .sum()
    }

    fn get(&self, seq: u64) -> Option<&StoredMsg> {
        if seq < self.base_seq {
            return None;
        }
        self.messages.get((seq - self.base_seq) as usize)
    }

    fn last_seq_for_subject(&self, subject: &str) -> Option<u64> {
        self.messages
            .iter()
            .rev()
            .find(|m| m.subject == subject)
            .map(|m| m.seq)
    }

    fn info_json(&self) -> Value {
        let first_ts = self
            .messages
            .front()
            .map(|m| rfc3339(m.time_ns))
            .unwrap_or_else(|| "0001-01-01T00:00:00Z".to_string());
        let last_ts = self
            .messages
            .back()
            .map(|m| rfc3339(m.time_ns))
            .unwrap_or_else(|| "0001-01-01T00:00:00Z".to_string());
        json!({
            "config": {
                "name": self.name,
                "subjects": self.subjects,
                "retention": self.retention,
                "max_consumers": -1,
                "max_msgs": if self.max_msgs <= 0 { -1 } else { self.max_msgs },
                "max_bytes": if self.max_bytes <= 0 { -1 } else { self.max_bytes },
                "max_age": self.max_age_ns,
                "max_msgs_per_subject": -1,
                "max_msg_size": -1,
                "discard": "old",
                "storage": self.storage,
                "num_replicas": 1,
                "duplicate_window": 120_000_000_000i64,
                "compression": "none",
                "allow_direct": false,
                "mirror_direct": false,
                "sealed": false,
                "deny_delete": false,
                "deny_purge": false,
                "allow_rollup_hdrs": false,
                "consumer_limits": {},
                "allow_msg_ttl": false,
            },
            "created": rfc3339(self.created_ns),
            "state": {
                "messages": self.messages.len(),
                "bytes": self.bytes(),
                "first_seq": self.first_seq(),
                "first_ts": first_ts,
                "last_seq": self.last_seq(),
                "last_ts": last_ts,
                "consumer_count": self.consumers.len(),
            },
        })
    }
}

impl Consumer {
    fn info_json(&self, stream_name: &str) -> Value {
        let ack_policy = match self.ack_policy {
            AckPolicy::Explicit => "explicit",
            AckPolicy::None => "none",
            AckPolicy::All => "all",
        };
        json!({
            "stream_name": stream_name,
            "name": self.name,
            "created": rfc3339(self.created_ns),
            "config": {
                "name": self.name,
                "durable_name": self.name,
                "deliver_subject": self.deliver_subject,
                // async-nats parses filter_subject as a plain string
                // (null is a deserialization error); unset means "".
                "filter_subject": self.filter_subject.clone().unwrap_or_default(),
                "deliver_policy": self.deliver_policy,
                "ack_policy": ack_policy,
                "ack_wait": self.ack_wait.as_nanos() as u64,
                "max_deliver": self.max_deliver,
                "replay_policy": "instant",
                "max_waiting": 512,
                "max_ack_pending": self.max_ack_pending,
                "num_replicas": 0,
            },
            "delivered": {
                "consumer_seq": self.consumer_seq,
                "stream_seq": self.next_stream_seq.saturating_sub(1),
            },
            "ack_floor": {
                "consumer_seq": self.ack_floor_consumer_seq,
                "stream_seq": self.ack_floor_stream_seq,
            },
            "num_ack_pending": self.pending.len(),
            "num_redelivered": self.num_redelivered,
            "num_waiting": self.waiting_pulls.len(),
            "num_pending": 0,
        })
    }

    fn ack_subject(
        &self,
        stream: &str,
        delivered: u64,
        stream_seq: u64,
        consumer_seq: u64,
        pending: usize,
    ) -> String {
        format!(
            "$JS.ACK.{stream}.{}.{delivered}.{stream_seq}.{consumer_seq}.{}.{pending}",
            self.name,
            now_ns()
        )
    }
}

/// API responses are flat, matching nats-server: the `type` marker plus
/// the operation's fields at top level (no "response" envelope).
fn api_response(
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

fn api_error_action(
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
fn err_not_found(what: &str) -> (u16, u32, String) {
    match what {
        "consumer" => (404, 10014, "consumer not found".to_string()),
        "message" => (404, 10037, "message not found".to_string()),
        _ => (404, 10059, format!("{what} not found")),
    }
}

/// Deliver one stored message to a consumer. Records the pending entry
/// (unless ack policy is none).
fn deliver_to_consumer(
    stream_name: &str,
    account: &str,
    consumer: &mut Consumer,
    msg: &StoredMsg,
    to_subject: &str,
    actions: &mut Vec<Action>,
) {
    if consumer.ack_policy == AckPolicy::None {
        actions.push(Action::Deliver(
            Message::new(to_subject, None, msg.headers.clone(), msg.payload.clone())
                .with_account(account),
        ));
        return;
    }
    consumer.consumer_seq += 1;
    let cseq = consumer.consumer_seq;
    let pending_count = consumer.pending.len() + 1;
    let reply = consumer.ack_subject(stream_name, 1, msg.seq, cseq, pending_count);
    actions.push(Action::Deliver(
        Message::new(
            to_subject,
            Some(reply),
            msg.headers.clone(),
            msg.payload.clone(),
        )
        .with_account(account),
    ));
    consumer.pending.insert(
        cseq,
        Pending {
            stream_seq: msg.seq,
            delivered: 1,
            deadline: Instant::now() + consumer.ack_wait,
        },
    );
}

/// Deliver the next available messages to a push consumer, honouring
/// max_ack_pending and the filter. Advances next_stream_seq past every
/// sequence considered (matching or not).
fn drain_push(
    stream_name: &str,
    stream: &mut Stream,
    consumer_name: &str,
    actions: &mut Vec<Action>,
) {
    loop {
        let (deliver_subject, filter, max_pending, pending_len, seq) = {
            let Some(c) = stream.consumers.get(consumer_name) else {
                return;
            };
            let Some(ds) = c.deliver_subject.clone() else {
                return;
            };
            (
                ds,
                c.filter_subject.clone(),
                c.max_ack_pending,
                c.pending.len(),
                c.next_stream_seq,
            )
        };
        if max_pending > 0 && pending_len as i64 >= max_pending {
            break;
        }
        let Some(msg) = stream.get(seq).map(|m| StoredMsg {
            seq: m.seq,
            subject: m.subject.clone(),
            headers: m.headers.clone(),
            payload: m.payload.clone(),
            time_ns: m.time_ns,
        }) else {
            break;
        };
        let matches = match &filter {
            Some(f) => subject_matches(f, &msg.subject),
            None => true,
        };
        let consumer = stream.consumers.get_mut(consumer_name).unwrap();
        consumer.next_stream_seq = seq + 1;
        if !matches {
            continue;
        }
        let account = stream.account.clone();
        deliver_to_consumer(
            stream_name,
            &account,
            consumer,
            &msg,
            &deliver_subject,
            actions,
        );
    }
}

/// Try to satisfy waiting pulls of one consumer from newly available
/// messages (redelivery queue first, then fresh sequences).
fn fulfill_pulls(
    stream_name: &str,
    stream: &mut Stream,
    consumer_name: &str,
    actions: &mut Vec<Action>,
) {
    loop {
        // Drop finished pulls.
        {
            let Some(c) = stream.consumers.get_mut(consumer_name) else {
                return;
            };
            if c.waiting_pulls
                .front()
                .map(|p| p.remaining == 0)
                .unwrap_or(false)
            {
                c.waiting_pulls.pop_front();
                continue;
            }
        }
        let Some((reply, filter)) = ({
            let Some(c) = stream.consumers.get(consumer_name) else {
                return;
            };
            c.waiting_pulls
                .front()
                .map(|p| (p.reply.clone(), c.filter_subject.clone()))
        }) else {
            return;
        };
        // Pick the next sequence: redelivery first, else fresh.
        let seq = {
            let next = stream.consumers.get(consumer_name).unwrap().next_stream_seq;
            if let Some(s) = stream
                .consumers
                .get_mut(consumer_name)
                .unwrap()
                .redeliver
                .pop_front()
            {
                Some(s)
            } else if stream.get(next).is_some() {
                stream
                    .consumers
                    .get_mut(consumer_name)
                    .unwrap()
                    .next_stream_seq = next + 1;
                Some(next)
            } else {
                None
            }
        };
        let Some(seq) = seq else {
            return;
        };
        let Some(msg) = stream.get(seq).map(|m| StoredMsg {
            seq: m.seq,
            subject: m.subject.clone(),
            headers: m.headers.clone(),
            payload: m.payload.clone(),
            time_ns: m.time_ns,
        }) else {
            continue;
        };
        let matches = match &filter {
            Some(f) => subject_matches(f, &msg.subject),
            None => true,
        };
        if !matches {
            continue;
        }
        {
            let c = stream.consumers.get_mut(consumer_name).unwrap();
            let pull = c.waiting_pulls.front_mut().unwrap();
            pull.remaining -= 1;
            pull.delivered += 1;
        }
        let consumer = stream.consumers.get_mut(consumer_name).unwrap();
        let account = stream.account.clone();
        deliver_to_consumer(stream_name, &account, consumer, &msg, &reply, actions);
    }
}

fn parse_u64(s: &str) -> Option<u64> {
    s.parse().ok()
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('.')
        && !name.contains('$')
        && !name.contains(' ')
        && !name.contains('*')
        && !name.contains('>')
}

impl Engine {
    /// Process one message. Returns (consumed, actions). API requests and
    /// acks are consumed; stream capture stores and still allows normal
    /// delivery, so it returns consumed=false.
    /// Stream by name, only when it belongs to `account`.
    fn stream_for(&self, name: &str, account: &str) -> Option<&Stream> {
        self.streams.get(name).filter(|s| s.account == account)
    }

    fn stream_for_mut(&mut self, name: &str, account: &str) -> Option<&mut Stream> {
        self.streams.get_mut(name).filter(|s| s.account == account)
    }

    fn process(&mut self, msg: &Message, origin: Origin) -> (bool, Vec<Action>) {
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
            self.process_api(rest, msg, &mut actions);
            return (true, actions);
        }
        self.capture(msg, &mut actions);
        (false, actions)
    }

    fn capture(&mut self, msg: &Message, actions: &mut Vec<Action>) {
        // Which streams match this subject?
        let matching: Vec<String> = self
            .streams
            .values()
            .filter(|s| {
                s.account == msg.account
                    && s.subjects.iter().any(|p| subject_matches(p, &msg.subject))
            })
            .map(|s| s.name.clone())
            .collect();
        if matching.is_empty() {
            return;
        }
        let mut stored: Vec<(String, u64, bool)> = Vec::new();
        let mut first_error: Option<(u16, u32, String)> = None;
        for name in &matching {
            let stream = self.stream_for_mut(name, &msg.account).unwrap();
            // Dedup by Nats-Msg-Id.
            if let Some(id) = header_get(msg, "Nats-Msg-Id") {
                if let Some((seq, _)) = stream.msg_ids.get(&id) {
                    let seq = *seq;
                    stored.push((name.clone(), seq, true));
                    continue;
                }
            }
            // Expected-sequence checks.
            if let Some(exp) = header_get(msg, "Nats-Expected-Last-Sequence") {
                let want = parse_u64(&exp).unwrap_or(u64::MAX);
                let last = stream.last_seq();
                let last = if stream.messages.is_empty() { 0 } else { last };
                if want != last {
                    if first_error.is_none() {
                        first_error =
                            Some((400, 10071, "expected last sequence mismatch".to_string()));
                    }
                    continue;
                }
            }
            if let Some(exp) = header_get(msg, "Nats-Expected-Last-Subject-Sequence") {
                let want = parse_u64(&exp).unwrap_or(u64::MAX);
                let last = stream.last_seq_for_subject(&msg.subject).unwrap_or(0);
                if want != last {
                    if first_error.is_none() {
                        first_error = Some((
                            400,
                            10071,
                            "expected last subject sequence mismatch".to_string(),
                        ));
                    }
                    continue;
                }
            }
            if let Some(exp_stream) = header_get(msg, "Nats-Expected-Stream") {
                if exp_stream != stream.name {
                    if first_error.is_none() {
                        first_error = Some((400, 10071, "expected stream mismatch".to_string()));
                    }
                    continue;
                }
            }
            let seq = stream.base_seq + stream.messages.len() as u64;
            let stored_msg = StoredMsg {
                seq,
                subject: msg.subject.clone(),
                headers: msg.headers.clone(),
                payload: msg.payload.clone(),
                time_ns: now_ns(),
            };
            stream.messages.push_back(stored_msg);
            if let Some(id) = header_get(msg, "Nats-Msg-Id") {
                stream.msg_ids.insert(id, (seq, Instant::now()));
            }
            // Enforce limits (discard-old policy).
            if stream.max_msgs > 0 {
                while stream.messages.len() as i64 > stream.max_msgs {
                    stream.messages.pop_front();
                    stream.base_seq += 1;
                }
            }
            if stream.max_bytes > 0 {
                while stream.bytes() > stream.max_bytes as u64 {
                    if stream.messages.pop_front().is_none() {
                        break;
                    }
                    stream.base_seq += 1;
                }
            }
            stored.push((name.clone(), seq, false));
        }
        // Publish acknowledgement (to the publisher's reply subject).
        if let Some(reply) = &msg.reply_to {
            if let Some((name, seq, duplicate)) = stored.first() {
                let body = json!({
                    "stream": name,
                    "seq": seq,
                    "duplicate": duplicate,
                });
                actions.push(Action::Deliver(Message::new(
                    reply,
                    None,
                    None,
                    Bytes::from(body.to_string()),
                )));
            } else if let Some((code, err_code, desc)) = first_error {
                let body = json!({
                    "error": { "code": code, "err_code": err_code, "description": desc },
                });
                actions.push(Action::Deliver(Message::new(
                    reply,
                    None,
                    None,
                    Bytes::from(body.to_string()),
                )));
            }
        }
        // Fan out to push consumers and waiting pulls of every stream
        // that stored (or deduplicated) this message.
        for (name, _, _) in &stored {
            let push_names: Vec<String> = self
                .streams
                .get(name)
                .map(|s| {
                    s.consumers
                        .values()
                        .filter(|c| c.deliver_subject.is_some())
                        .map(|c| c.name.clone())
                        .collect()
                })
                .unwrap_or_default();
            for cname in push_names {
                let stream = self.stream_for_mut(name, &msg.account).unwrap();
                drain_push(name, stream, &cname, actions);
            }
            let pull_names: Vec<String> = self
                .streams
                .get(name)
                .map(|s| {
                    s.consumers
                        .values()
                        .filter(|c| c.deliver_subject.is_none() && !c.waiting_pulls.is_empty())
                        .map(|c| c.name.clone())
                        .collect()
                })
                .unwrap_or_default();
            for cname in pull_names {
                let stream = self.stream_for_mut(name, &msg.account).unwrap();
                fulfill_pulls(name, stream, &cname, actions);
            }
        }
    }

    fn process_ack(&mut self, rest: &str, msg: &Message, actions: &mut Vec<Action>) {
        // $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>
        // (7 tokens after the "$JS.ACK." prefix.)
        let parts: Vec<&str> = rest.split('.').collect();
        if parts.len() != 7 {
            return;
        }
        let (stream_name, consumer_name) = (parts[0], parts[1]);
        let sseq = match parse_u64(parts[3]) {
            Some(v) => v,
            None => return,
        };
        let cseq = match parse_u64(parts[4]) {
            Some(v) => v,
            None => return,
        };
        let Some(stream) = self.stream_for_mut(stream_name, &msg.account) else {
            return;
        };
        // Clone the target message before borrowing the consumer.
        let staged = stream.get(sseq).map(|m| StoredMsg {
            seq: m.seq,
            subject: m.subject.clone(),
            headers: m.headers.clone(),
            payload: m.payload.clone(),
            time_ns: m.time_ns,
        });
        let Some(consumer) = stream.consumers.get_mut(consumer_name) else {
            return;
        };
        let Some(pending) = consumer.pending.remove(&cseq) else {
            return;
        };
        if pending.stream_seq != sseq {
            // Stale ack for a different message; restore the entry.
            consumer.pending.insert(cseq, pending);
            return;
        }
        if cseq > consumer.ack_floor_consumer_seq {
            consumer.ack_floor_consumer_seq = cseq;
            consumer.ack_floor_stream_seq = sseq;
        }
        let payload = String::from_utf8_lossy(&msg.payload);
        match payload.as_ref() {
            // The wire format for negative ack is `-NAK` (async-nats
            // and nats.go both send that); `+NAK` is accepted too.
            "-NAK" | "+NAK" => {
                // Immediate redelivery.
                if consumer.deliver_subject.is_some() {
                    if let Some(m) = staged {
                        let to = consumer.deliver_subject.clone().unwrap();
                        let delivered = pending.delivered + 1;
                        if consumer.max_deliver <= 0 || delivered <= consumer.max_deliver as u64 {
                            consumer.consumer_seq += 1;
                            let ncseq = consumer.consumer_seq;
                            let reply = consumer.ack_subject(
                                stream_name,
                                delivered,
                                sseq,
                                ncseq,
                                consumer.pending.len() + 1,
                            );
                            actions.push(Action::Deliver(Message::new(
                                &to,
                                Some(reply),
                                m.headers.clone(),
                                m.payload.clone(),
                            )));
                            consumer.pending.insert(
                                ncseq,
                                Pending {
                                    stream_seq: sseq,
                                    delivered,
                                    deadline: Instant::now() + consumer.ack_wait,
                                },
                            );
                            consumer.num_redelivered += 1;
                        }
                    }
                } else {
                    consumer.redeliver.push_back(sseq);
                    consumer.num_redelivered += 1;
                }
            }
            "+TERM" => {
                // Terminate: drop, do not redeliver.
            }
            "+WPI" => {
                consumer.pending.insert(
                    cseq,
                    Pending {
                        stream_seq: sseq,
                        delivered: pending.delivered,
                        deadline: Instant::now() + consumer.ack_wait,
                    },
                );
            }
            _ => {
                // Empty payload (or anything else) counts as +ACK.
            }
        }
        // An ack may free max_ack_pending room for push consumers.
        if consumer.deliver_subject.is_some() {
            let cname = consumer.name.clone();
            drain_push(stream_name, stream, &cname, actions);
        }
    }
}

impl Engine {
    fn process_api(&mut self, rest: &str, msg: &Message, actions: &mut Vec<Action>) {
        let Some(reply) = msg.reply_to.clone() else {
            return;
        };
        let body: Value = serde_json::from_slice(&msg.payload).unwrap_or(Value::Null);
        let parts: Vec<&str> = rest.split('.').collect();
        // The type marker names the operation only, never the stream or
        // consumer (real nats-server: "...consumer_create_response",
        // not "...consumer_create_<stream>_<consumer>_response").
        let op_name = match parts.as_slice() {
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
            ["CONSUMER", "CREATE", stream] | ["CONSUMER", "CREATE", stream, _] => {
                let from_subject = if parts.len() == 4 {
                    Some(parts[3].to_string())
                } else {
                    None
                };
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

    fn api_stream_create(
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
        if subjects.is_empty() {
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
        if retention != "limits" {
            api_error_action(
                account,
                reply,
                type_name,
                400,
                10002,
                "only limits retention is supported",
                actions,
            );
            return;
        }
        let storage = body
            .get("storage")
            .and_then(|v| v.as_str())
            .unwrap_or("memory")
            .to_string();
        let stream = Stream {
            name: name.to_string(),
            account: account.to_string(),
            created_ns: now_ns(),
            subjects,
            retention,
            max_msgs: body.get("max_msgs").and_then(|v| v.as_i64()).unwrap_or(0),
            max_bytes: body.get("max_bytes").and_then(|v| v.as_i64()).unwrap_or(0),
            max_age_ns: body.get("max_age").and_then(|v| v.as_i64()).unwrap_or(0),
            storage,
            messages: VecDeque::new(),
            base_seq: 1,
            msg_ids: HashMap::new(),
            consumers: HashMap::new(),
        };
        let info = stream.info_json();
        self.streams.insert(name.to_string(), stream);
        api_response(account, reply, type_name, info, actions);
    }

    fn api_stream_info(
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

    fn api_stream_list(
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

    fn api_stream_names(
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

    fn api_stream_delete(
        &mut self,
        account: &str,
        name: &str,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        if self.stream_for(name, account).is_some() && self.streams.remove(name).is_some() {
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

    fn api_stream_purge(
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

    fn api_stream_msg_get(
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

impl Engine {
    // Account scoping pushed this over clippy's 7-argument default.
    #[allow(clippy::too_many_arguments)]
    fn api_consumer_create(
        &mut self,
        account: &str,
        stream_name: &str,
        from_subject: Option<String>,
        body: &Value,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        let Some(stream) = self.stream_for_mut(stream_name, account) else {
            let (code, err_code, desc) = err_not_found("stream");
            api_error_action(account, reply, type_name, code, err_code, &desc, actions);
            return;
        };
        let config = body.get("config").unwrap_or(&Value::Null);
        let name = from_subject
            .or_else(|| {
                config
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .or_else(|| {
                config
                    .get("durable_name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_default();
        if !valid_name(&name) {
            api_error_action(
                account,
                reply,
                type_name,
                400,
                10002,
                "consumer name is required and must not contain dots",
                actions,
            );
            return;
        }
        if stream.consumers.contains_key(&name) {
            api_error_action(
                account,
                reply,
                type_name,
                400,
                10013,
                "consumer name already in use",
                actions,
            );
            return;
        }
        let deliver_subject = config
            .get("deliver_subject")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty());
        let filter_subject = config
            .get("filter_subject")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty());
        let ack_policy = match config.get("ack_policy").and_then(|v| v.as_str()) {
            Some("none") => AckPolicy::None,
            Some("all") => AckPolicy::All,
            _ => AckPolicy::Explicit,
        };
        let ack_wait_ns = config.get("ack_wait").and_then(|v| v.as_u64()).unwrap_or(0);
        let ack_wait = if ack_wait_ns == 0 {
            DEFAULT_ACK_WAIT
        } else {
            Duration::from_nanos(ack_wait_ns)
        };
        let deliver_policy = config
            .get("deliver_policy")
            .and_then(|v| v.as_str())
            .unwrap_or("all")
            .to_string();
        let start_seq = match deliver_policy.as_str() {
            "new" => stream.last_seq() + 1,
            "last" => {
                if stream.messages.is_empty() {
                    stream.base_seq + stream.messages.len() as u64
                } else {
                    stream.last_seq()
                }
            }
            _ => stream.first_seq(),
        };
        let consumer = Consumer {
            name: name.clone(),
            created_ns: now_ns(),
            deliver_subject,
            filter_subject,
            ack_policy,
            ack_wait,
            max_deliver: config
                .get("max_deliver")
                .and_then(|v| v.as_i64())
                .unwrap_or(-1),
            max_ack_pending: match config.get("max_ack_pending").and_then(|v| v.as_i64()) {
                Some(v) if v > 0 => v,
                _ => 1000,
            },
            deliver_policy,
            next_stream_seq: start_seq,
            consumer_seq: 0,
            pending: HashMap::new(),
            redeliver: VecDeque::new(),
            waiting_pulls: VecDeque::new(),
            ack_floor_stream_seq: 0,
            ack_floor_consumer_seq: 0,
            num_redelivered: 0,
        };
        let info = consumer.info_json(stream_name);
        stream.consumers.insert(name.clone(), consumer);
        // Push consumers with backlog deliver immediately.
        if stream
            .consumers
            .get(&name)
            .unwrap()
            .deliver_subject
            .is_some()
        {
            let stream = self.stream_for_mut(stream_name, account).unwrap();
            drain_push(stream_name, stream, &name, actions);
        }
        api_response(account, reply, type_name, info, actions);
    }

    fn api_consumer_info(
        &mut self,
        account: &str,
        stream_name: &str,
        consumer_name: &str,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        match self
            .streams
            .get(stream_name)
            .and_then(|s| s.consumers.get(consumer_name))
        {
            Some(consumer) => {
                let info = consumer.info_json(stream_name);
                api_response(account, reply, type_name, info, actions);
            }
            None => {
                let (code, err_code, desc) = err_not_found("consumer");
                api_error_action(account, reply, type_name, code, err_code, &desc, actions);
            }
        }
    }

    fn api_consumer_list(
        &mut self,
        account: &str,
        stream_name: &str,
        body: &Value,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        let Some(stream) = self.stream_for(stream_name, account) else {
            let (code, err_code, desc) = err_not_found("stream");
            api_error_action(account, reply, type_name, code, err_code, &desc, actions);
            return;
        };
        let offset = body.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let mut names: Vec<String> = stream.consumers.keys().cloned().collect();
        names.sort();
        let total = names.len();
        let consumers: Vec<Value> = names
            .iter()
            .skip(offset)
            .filter_map(|n| stream.consumers.get(n).map(|c| c.info_json(stream_name)))
            .collect();
        api_response(
            account,
            reply,
            type_name,
            json!({ "consumers": consumers, "total": total, "offset": offset, "limit": 1024 }),
            actions,
        );
    }

    fn api_consumer_delete(
        &mut self,
        account: &str,
        stream_name: &str,
        consumer_name: &str,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        let removed = self
            .streams
            .get_mut(stream_name)
            .map(|s| s.consumers.remove(consumer_name).is_some())
            .unwrap_or(false);
        if removed {
            api_response(
                account,
                reply,
                type_name,
                json!({ "success": true }),
                actions,
            );
        } else {
            let (code, err_code, desc) = err_not_found("consumer");
            api_error_action(account, reply, type_name, code, err_code, &desc, actions);
        }
    }

    fn api_consumer_msg_next(
        &mut self,
        account: &str,
        stream_name: &str,
        consumer_name: &str,
        body: &Value,
        reply: &str,
        actions: &mut Vec<Action>,
    ) {
        let Some(stream) = self.stream_for_mut(stream_name, account) else {
            api_error_action(
                account,
                reply,
                "io.nats.jetstream.api.v1.consumer_msg_next_response",
                404,
                10059,
                "stream not found",
                actions,
            );
            return;
        };
        if !stream.consumers.contains_key(consumer_name) {
            api_error_action(
                account,
                reply,
                "io.nats.jetstream.api.v1.consumer_msg_next_response",
                404,
                10059,
                "consumer not found",
                actions,
            );
            return;
        }
        if stream
            .consumers
            .get(consumer_name)
            .unwrap()
            .deliver_subject
            .is_some()
        {
            api_error_action(
                account,
                reply,
                "io.nats.jetstream.api.v1.consumer_msg_next_response",
                400,
                10002,
                "consumer is push based",
                actions,
            );
            return;
        }
        let batch = body
            .get("batch")
            .and_then(|v| v.as_u64())
            .unwrap_or(1)
            .max(1) as usize;
        let no_wait = body
            .get("no_wait")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let expires_ns = body.get("expires").and_then(|v| v.as_u64()).unwrap_or(0);
        // Deliver what is available right now.
        let mut delivered = 0usize;
        while delivered < batch {
            let seq = {
                let next = stream.consumers.get(consumer_name).unwrap().next_stream_seq;
                if let Some(s) = stream
                    .consumers
                    .get_mut(consumer_name)
                    .unwrap()
                    .redeliver
                    .pop_front()
                {
                    Some(s)
                } else if stream.get(next).is_some() {
                    stream
                        .consumers
                        .get_mut(consumer_name)
                        .unwrap()
                        .next_stream_seq = next + 1;
                    Some(next)
                } else {
                    None
                }
            };
            let Some(seq) = seq else {
                break;
            };
            let Some(m) = stream.get(seq).map(|m| StoredMsg {
                seq: m.seq,
                subject: m.subject.clone(),
                headers: m.headers.clone(),
                payload: m.payload.clone(),
                time_ns: m.time_ns,
            }) else {
                continue;
            };
            let matches = match &stream.consumers.get(consumer_name).unwrap().filter_subject {
                Some(f) => subject_matches(f, &m.subject),
                None => true,
            };
            if !matches {
                continue;
            }
            let consumer = stream.consumers.get_mut(consumer_name).unwrap();
            let account = stream.account.clone();
            deliver_to_consumer(stream_name, &account, consumer, &m, reply, actions);
            delivered += 1;
        }
        if delivered < batch && !no_wait {
            let wait = if expires_ns == 0 {
                Duration::from_secs(30)
            } else {
                Duration::from_nanos(expires_ns)
            };
            stream
                .consumers
                .get_mut(consumer_name)
                .unwrap()
                .waiting_pulls
                .push_back(WaitingPull {
                    reply: reply.to_string(),
                    remaining: batch - delivered,
                    delivered,
                    deadline: Instant::now() + wait,
                });
        } else if delivered == 0 && no_wait {
            // Immediate "no messages" status message, as nats-server does.
            actions.push(Action::Deliver(Message::new(
                reply,
                None,
                Some(Bytes::from(
                    "NATS/1.0\r\nStatus: 404\r\nDescription: No Messages\r\n\r\n",
                )),
                Bytes::new(),
            )));
        }
    }

    /// Background maintenance: max-age eviction, redelivery of expired
    /// pending messages, expiry of waiting pulls, push backlog drains,
    /// dedup-window pruning. Returns actions to send.
    fn tick(&mut self) -> Vec<Action> {
        let mut actions = Vec::new();
        let now = Instant::now();
        let now_ns = now_ns();
        let stream_names: Vec<String> = self.streams.keys().cloned().collect();
        for name in stream_names {
            let account = self
                .streams
                .get(&name)
                .map(|s| s.account.clone())
                .unwrap_or_default();
            let stream = self.stream_for_mut(&name, &account).unwrap();
            // Max-age eviction.
            if stream.max_age_ns > 0 {
                let cutoff = now_ns.saturating_sub(stream.max_age_ns as u64);
                while let Some(front) = stream.messages.front() {
                    if front.time_ns < cutoff {
                        stream.messages.pop_front();
                        stream.base_seq += 1;
                    } else {
                        break;
                    }
                }
            }
            // Dedup pruning.
            stream
                .msg_ids
                .retain(|_, (_, t)| now.duration_since(*t) < DEDUP_WINDOW);
            let consumer_names: Vec<String> = stream.consumers.keys().cloned().collect();
            for cname in consumer_names {
                // Expire waiting pulls.
                let expired: Vec<WaitingPull> = {
                    let c = stream.consumers.get_mut(&cname).unwrap();
                    let mut out = Vec::new();
                    while let Some(p) = c.waiting_pulls.front() {
                        if p.deadline <= now {
                            out.push(c.waiting_pulls.pop_front().unwrap());
                        } else {
                            break;
                        }
                    }
                    out
                };
                for pull in expired {
                    if pull.delivered == 0 {
                        actions.push(Action::Deliver(Message::new(
                            &pull.reply,
                            None,
                            Some(Bytes::from(
                                "NATS/1.0\r\nStatus: 404\r\nDescription: No Messages\r\n\r\n",
                            )),
                            Bytes::new(),
                        )));
                    }
                }
                // Redeliver expired pending messages.
                let due: Vec<(u64, u64, u64)> = {
                    let c = stream.consumers.get_mut(&cname).unwrap();
                    c.pending
                        .iter()
                        .filter(|(_, p)| p.deadline <= now)
                        .map(|(cseq, p)| (*cseq, p.stream_seq, p.delivered))
                        .collect()
                };
                for (cseq, sseq, delivered) in due {
                    let staged = stream.get(sseq).map(|m| StoredMsg {
                        seq: m.seq,
                        subject: m.subject.clone(),
                        headers: m.headers.clone(),
                        payload: m.payload.clone(),
                        time_ns: m.time_ns,
                    });
                    let c = stream.consumers.get_mut(&cname).unwrap();
                    let Some(pending) = c.pending.remove(&cseq) else {
                        continue;
                    };
                    let _ = pending;
                    if c.max_deliver > 0 && delivered >= c.max_deliver as u64 {
                        continue;
                    }
                    if c.deliver_subject.is_some() {
                        if let Some(m) = staged {
                            let to = c.deliver_subject.clone().unwrap();
                            let nd = delivered + 1;
                            c.consumer_seq += 1;
                            let ncseq = c.consumer_seq;
                            let reply = c.ack_subject(&name, nd, sseq, ncseq, c.pending.len() + 1);
                            actions.push(Action::Deliver(Message::new(
                                &to,
                                Some(reply),
                                m.headers.clone(),
                                m.payload.clone(),
                            )));
                            c.pending.insert(
                                ncseq,
                                Pending {
                                    stream_seq: sseq,
                                    delivered: nd,
                                    deadline: now + c.ack_wait,
                                },
                            );
                            c.num_redelivered += 1;
                        }
                    } else {
                        c.redeliver.push_back(sseq);
                        c.num_redelivered += 1;
                    }
                }
                // Push backlog drain (also after acks free room).
                drain_push(&name, stream, &cname, &mut actions);
                // Fulfill waiting pulls from fresh messages.
                fulfill_pulls(&name, stream, &cname, &mut actions);
            }
        }
        actions
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
    let (consumed, actions) = {
        let mut engine = js.engine.lock().unwrap();
        engine.process(msg, origin)
    };
    for action in actions {
        match action {
            Action::Deliver(m) => {
                deliver(shared, &m, Origin::Internal, None);
            }
        }
    }
    consumed
}

/// Background task: redelivers consumer messages whose ack deadline
/// passed, expires waiting pulls, evicts over-age messages.
pub async fn redelivery_loop(js: std::sync::Arc<JetStream>, shared: std::sync::Arc<Shared>) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(TICK) => {
                let actions = {
                    let mut engine = js.engine.lock().unwrap();
                    engine.tick()
                };
                for action in actions {
                    match action {
                        Action::Deliver(m) => {
                            deliver(&shared, &m, Origin::Internal, None);
                        }
                    }
                }
            }
            _ = shared.shutdown.notified() => break,
        }
    }
}
