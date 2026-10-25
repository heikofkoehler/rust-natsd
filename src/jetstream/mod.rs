//! JetStream engine (Phase 9, upgraded in Phase 15): streams, consumers
//! (push and pull), acknowledgements with redelivery, and the
//! `$JS.API` surface.
//!
//! Scope notes (also in the README):
//! - Persistence (Phase 30 WAL): when a store dir is configured
//!   (`--js-store-dir` / `jetstream.store_dir`), every mutation is
//!   appended to a per-account/per-stream WAL (`meta.json` +
//!   `msgs.wal` + `consumers/*.wal`, CRC-checked frames) and
//!   replayed on startup; a legacy whole-engine JSON snapshot is
//!   migrated once when no WAL layout exists yet. Without a store
//!   dir the engine is purely in-memory and data is lost on
//!   restart. A stream created with file storage but no store dir
//!   is accepted but stays in-memory.
//! - Pending ack state IS persisted (snapshot version 2): per-consumer
//!   delivered-but-unacked entries (consumer seq, stream seq, delivery
//!   count, wall-clock delivery time), the ack floor, the redelivery
//!   count, and the pull redelivery queue. On reload, each pending
//!   entry's deadline is recomputed as delivery_time + ack_wait, so a
//!   restart preserves the remaining ack window (verified against Go
//!   v2.15.1: unacked messages are NOT redelivered immediately after a
//!   restart inside the window, and ARE redelivered — with delivery
//!   count incremented and a fresh consumer seq — once the window
//!   expires; acked messages are never redelivered). Pull redelivery
//!   entries already past their deadline at load are queued at once.
//!   Old (version 1) snapshots load with empty ack state, as before.
//!   Push redelivery defers while the deliver subject has no interest
//!   (local, route, or leaf) instead of burning a delivery cycle, so a
//!   late subscriber still sees delivery count 2, as Go does.
//!   Waiting pulls are NOT persisted: they belong to a dead
//!   connection's reply inbox, and the client must re-issue Fetch.
//!   Dedup ids are persisted (window restarts).
//! - JetStream is per-node: streams capture messages published on the
//!   hosting node, but `$JS.API` requests and `$JS.ACK` messages route
//!   cluster-wide via the `$JS.API.>` interest the server advertises.
//!   Streams are account-scoped (Phase 14); stream names are
//!   node-global.
//! - Retention: limits and workqueue (deleted after first ack; ack
//!   policy All deletes only the acked message, not the whole floor).
//! - Consumer deliver_policy: all, last (filter-aware), new, and
//!   by_start_sequence. No replication.
//! - Mirroring/sourcing (same node only): a stream whose config
//!   carries `mirror` or `sources` copies messages from origin
//!   stream(s) on the same node — backfill at creation (from
//!   `opt_start_seq` / `opt_start_time` when given, honouring
//!   `filter_subject`), then live copies on every origin publish
//!   (hooked into capture, under the engine lock, so backfill +
//!   link registration are atomic: no loss, no duplicates).
//!   Mirror copies are exact (origin subject, new mirror sequence,
//!   original headers and timestamp preserved); source copies add a
//!   `Nats-Stream-Source: <origin> <origin_seq> <filter|>> >
//!   <subject> <origin_token>` header (Go v2.15.1 shape, probed;
//!   Go's trailing token is a per-origin-stream id, replicated here
//!   as a per-stream token generated at creation and persisted).
//!   Mirror streams must not carry subjects or other sources (Go
//!   rejects both); a mirror/source of a nonexistent stream is
//!   rejected with 404/10059 (stricter than Go, which accepts it
//!   and leaves the stream empty — see PLAN Phase 20); cycles in
//!   the config graph are rejected. Deleting an origin keeps the
//!   mirror's copies but kills the live link permanently, even if a
//!   same-named origin is recreated later (Go behaviour, probed);
//!   purging an origin does not touch the mirror. Cross-node
//!   mirroring/sourcing (routes/leafs, `external`) is NOT
//!   implemented and is rejected at creation.
//! - KV is not a separate implementation: a KV bucket is a plain stream
//!   named `KV_<bucket>` with subjects `$KV.<bucket>.>`; the primitives
//!   KV needs (headers on stored messages, `MSG.GET` with `last_by_subj`)
//!   are implemented, but the nats.go/async-nats KV client helpers are
//!   not verified against this server.
//!
//! Phase 29 layout: this module is the public facade (`JetStream` handle
//! and lock coordinator) plus the shared engine types (`Engine`, `Stream`,
//! `Consumer`, …) and small helpers. The `$JS.API` request router and
//! stream/consumer API handlers live in `api`, stream state, capture,
//! dedup and retention in `stream`, consumer state machines in
//! `consumer`, and persistence in `storage`. Phase 30 replaced the
//! atomic JSON snapshot write path with an append-only WAL there
//! (the snapshot-v2 format survives as the migration source and
//! as the WAL-load assembly format, so pending ack state, ack
//! floors, redelivery queues, and dedup ids reload identically).
//!
//! Actions pattern (unchanged): engine methods never deliver while
//! holding `engine.lock()` — they return `Vec<Action>` and the caller
//! (`handle_message`, `redelivery_loop`) dispatches after releasing
//! the lock, because `deliver` re-enters `handle_message`.

mod api;
mod consumer;
mod storage;
mod stream;

pub use self::api::handle_message;
pub use self::consumer::redelivery_loop;

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde_json::{json, Value};

use crate::server::Message;
use crate::subject::subject_matches;

/// Default dedup window for `Nats-Msg-Id` (matches nats-server's 2m).
pub(crate) const DEDUP_WINDOW: Duration = Duration::from_secs(120);
/// Default ack wait when a consumer does not set one.
pub(crate) const DEFAULT_ACK_WAIT: Duration = Duration::from_secs(30);
/// Redelivery / pull-expiry tick interval.
pub(crate) const TICK: Duration = Duration::from_millis(100);

/// JetStream engine handle. When `store_dir` is set, mutations
/// are appended to the WAL in that directory (see `storage.rs`)
/// after every mutating operation, and replayed on startup.
/// Without a store dir the engine is purely in-memory.
pub struct JetStream {
    pub(crate) engine: Mutex<Engine>,
    pub(crate) store_dir: Option<std::path::PathBuf>,
    pub(crate) wal: Mutex<Option<storage::FileWalStore>>,
    pub(crate) shadow: Mutex<storage::Shadow>,
}

impl JetStream {
    /// Totals for /varz's jetstream block.
    pub(crate) fn snapshot_totals(&self) -> Value {
        let engine = self.engine.lock().unwrap();
        let mut messages = 0u64;
        let mut bytes = 0u64;
        let mut consumers = 0usize;
        for s in engine.streams.values() {
            messages += s.live_count() as u64;
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
            total_messages += s.live_count() as u64;
            total_bytes += s.bytes();
            total_consumers += s.consumers.len();
            streams.push(json!({
                "name": s.name,
                "messages": s.live_count(),
                "bytes": s.bytes(),
                "first_seq": s.first_seq(),
                "last_seq": s.last_seq(),
                "consumer_count": s.consumers.len(),
                "consumers": s.consumers.keys().cloned().collect::<Vec<_>>(),
            }));
        }
        json!({
            "config": {"max_memory": 0, "max_file": 0, "store_dir": self.store_dir.as_ref().map(|d| d.to_string_lossy().to_string()).unwrap_or_default()},
            "streams": streams,
            "total_streams": engine.streams.len(),
            "total_messages": total_messages,
            "total_bytes": total_bytes,
            "total_consumers": total_consumers,
        })
    }

    pub fn new() -> Self {
        Self::with_store_dir(None)
    }

    /// Create an engine, restoring from the WAL layout in
    /// `store_dir` when one exists there (else migrating a legacy
    /// snapshot once, else starting empty).
    pub fn with_store_dir(store_dir: Option<std::path::PathBuf>) -> Self {
        match &store_dir {
            Some(dir) => {
                let (engine, wal, shadow) = Self::load_engine_and_store(dir);
                JetStream {
                    engine: Mutex::new(engine),
                    store_dir,
                    wal: Mutex::new(Some(wal)),
                    shadow: Mutex::new(shadow),
                }
            }
            None => JetStream {
                engine: Mutex::new(Engine {
                    streams: HashMap::new(),
                }),
                store_dir,
                wal: Mutex::new(None),
                shadow: Mutex::new(storage::Shadow::default()),
            },
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
pub(crate) enum Action {
    Deliver(Message),
}

pub(crate) struct Engine {
    pub(crate) streams: HashMap<String, Stream>,
}

pub(crate) struct StoredMsg {
    pub(crate) seq: u64,
    pub(crate) subject: String,
    pub(crate) headers: Option<Bytes>,
    pub(crate) payload: Bytes,
    /// Unix nanos, for max-age eviction.
    pub(crate) time_ns: u64,
}

/// One mirror/source link in a stream's config. `live` is the
/// runtime link state: true while the origin exists and the link
/// copies, false after the origin was deleted (the config is kept,
/// the copies are kept, but the link never revives — even if a
/// same-named origin is created later, matching Go). `live` is
/// persisted so a restart does not revive a dead link.
#[derive(Clone)]
pub(crate) struct SourceCfg {
    pub(crate) name: String,
    pub(crate) filter_subject: Option<String>,
    pub(crate) start_seq: u64,
    pub(crate) start_time_ns: u64,
    pub(crate) live: bool,
}

pub(crate) struct Stream {
    pub(crate) name: String,
    /// Account that owns this stream; capture and API lookups are
    /// scoped to it. Stream names are node-global in this
    /// implementation (a second account creating a same-named stream
    /// gets "stream name already in use").
    pub(crate) account: String,
    pub(crate) created_ns: u64,
    pub(crate) subjects: Vec<String>,
    pub(crate) retention: String,
    pub(crate) max_msgs: i64,
    pub(crate) max_bytes: i64,
    pub(crate) max_age_ns: i64,
    pub(crate) max_msgs_per_subject: i64,
    /// Sequences deleted in place (workqueue acks, per-subject eviction
    /// of interior messages). The VecDeque keeps its indexing; get()
    /// returns None for these, so consumers skip them.
    pub(crate) deleted: std::collections::HashSet<u64>,
    pub(crate) storage: String,
    pub(crate) messages: VecDeque<StoredMsg>,
    /// Sequence number of messages[0]; next sequence is base_seq + len.
    pub(crate) base_seq: u64,
    pub(crate) msg_ids: HashMap<String, (u64, Instant)>,
    pub(crate) consumers: HashMap<String, Consumer>,
    /// Mirror config (at most one; never combined with sources or
    /// with subjects — validated at creation).
    pub(crate) mirror: Option<SourceCfg>,
    /// Source configs, backfilled and live-copied in config order.
    pub(crate) sources: Vec<SourceCfg>,
    /// Opaque per-stream id used as the trailing token of the
    /// `Nats-Stream-Source` header on source copies (Go's token is a
    /// per-origin-stream id; this one is generated at creation and
    /// persisted).
    pub(crate) source_token: String,
    /// Phase 30: set when messages were renumbered in place
    /// (filtered purge), forcing the WAL writer to rewrite this
    /// stream's `msgs.wal` instead of trusting the append-only
    /// diff. Cleared by the sync that performs the rewrite.
    pub(crate) wal_rewrite: bool,
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum AckPolicy {
    Explicit,
    None,
    All,
}

pub(crate) struct Pending {
    pub(crate) stream_seq: u64,
    pub(crate) delivered: u64,
    pub(crate) deadline: Instant,
    /// Wall-clock delivery time (unix nanos), so the remaining ack
    /// window survives a restart: on load, deadline is recomputed as
    /// delivered_at_ns + ack_wait. `Instant` cannot be persisted.
    pub(crate) delivered_at_ns: u64,
}

/// One pull redelivery queued for the next Fetch/MSG.NEXT: the stream
/// sequence plus the delivery count the next delivery must carry
/// (Go reports delivered=2 on first redelivery, not 1).
pub(crate) struct Redeliver {
    pub(crate) stream_seq: u64,
    pub(crate) delivered: u64,
}

pub(crate) struct WaitingPull {
    pub(crate) reply: String,
    pub(crate) remaining: usize,
    pub(crate) delivered: usize,
    pub(crate) deadline: Instant,
}

pub(crate) struct Consumer {
    pub(crate) name: String,
    pub(crate) created_ns: u64,
    pub(crate) deliver_subject: Option<String>,
    pub(crate) filter_subject: Option<String>,
    /// Multi-subject filter (`filter_subjects` in CONSUMER.CREATE);
    /// when non-empty it takes precedence over `filter_subject`,
    /// matching Go's consumer filter resolution.
    pub(crate) filter_subjects: Vec<String>,
    pub(crate) ack_policy: AckPolicy,
    pub(crate) ack_wait: Duration,
    pub(crate) max_deliver: i64,
    pub(crate) max_ack_pending: i64,
    pub(crate) deliver_policy: String,
    pub(crate) next_stream_seq: u64,
    pub(crate) consumer_seq: u64,
    pub(crate) pending: HashMap<u64, Pending>,
    pub(crate) redeliver: VecDeque<Redeliver>,
    pub(crate) waiting_pulls: VecDeque<WaitingPull>,
    pub(crate) ack_floor_stream_seq: u64,
    pub(crate) ack_floor_consumer_seq: u64,
    pub(crate) num_redelivered: u64,
}

pub(crate) fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

pub(crate) fn base64_encode(data: &[u8]) -> String {
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

/// Parse an RFC3339 UTC timestamp (`2026-10-24T01:23:45.123Z`) back
/// to unix nanos. Inverse of `rfc3339`; only the `Z` form (which is
/// what Go and async-nats emit for `opt_start_time`) is supported.
pub(crate) fn parse_rfc3339_ns(s: &str) -> Option<u64> {
    let t = s.strip_suffix('Z')?;
    let main = t;
    let frac = "";
    let (date, time) = main.split_once('T')?;
    let mut d = date.split('-');
    let year: i64 = d.next()?.parse().ok()?;
    let month: i64 = d.next()?.parse().ok()?;
    let day: i64 = d.next()?.parse().ok()?;
    let (time, frac) = match time.split_once('.') {
        Some((t, f)) => (t, f),
        None => (time, frac),
    };
    let mut t = time.split(':');
    let hour: i64 = t.next()?.parse().ok()?;
    let min: i64 = t.next()?.parse().ok()?;
    let sec: i64 = t.next()?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || min > 59 || sec > 60 {
        return None;
    }
    // days_from_civil (Howard Hinnant's algorithm).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let mut nanos: u64 =
        ((days * 86_400 + hour * 3600 + min * 60 + sec) as u64).checked_mul(1_000_000_000)?;
    if !frac.is_empty() {
        let mut f = frac.to_string();
        while f.len() < 9 {
            f.push('0');
        }
        f.truncate(9);
        nanos += f.parse::<u64>().ok()?;
    }
    Some(nanos)
}

/// Opaque per-stream token (8 chars) for the `Nats-Stream-Source`
/// header. Derived from the stream name and creation time; it only
/// needs to be stable for the stream's lifetime (it is persisted).
pub(crate) fn make_source_token(name: &str, now: u64) -> String {
    const ALPH: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.as_bytes().iter().chain(&now.to_le_bytes()) {
        h ^= *b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    let mut out = String::with_capacity(8);
    let mut v = h;
    for _ in 0..8 {
        out.push(ALPH[(v % 62) as usize] as char);
        v /= 62;
    }
    out
}

/// Parse one mirror/source config object from a STREAM.CREATE body:
/// `{"name", "filter_subject"?, "opt_start_seq"?, "opt_start_time"?}`.
/// Returns None when `name` is missing/empty. `opt_start_time` may be
/// an RFC3339 string (Go/async-nats) or a unix-nanos number (tolerant
/// raw clients). `external` and `subject_transforms` are rejected by
/// the caller, not here.
pub(crate) fn parse_source_cfg(v: &Value) -> Option<SourceCfg> {
    let name = v.get("name")?.as_str()?;
    if name.is_empty() {
        return None;
    }
    let start_time_ns = match v.get("opt_start_time") {
        None => 0,
        Some(Value::String(s)) => parse_rfc3339_ns(s)?,
        Some(Value::Number(n)) => n.as_u64().unwrap_or(0),
        Some(_) => return None,
    };
    Some(SourceCfg {
        name: name.to_string(),
        filter_subject: v
            .get("filter_subject")
            .and_then(|f| f.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty()),
        start_seq: v.get("opt_start_seq").and_then(|n| n.as_u64()).unwrap_or(0),
        start_time_ns,
        live: true,
    })
}

pub(crate) fn source_cfg_json(cfg: &SourceCfg) -> Value {
    json!({
        "name": cfg.name,
        "filter_subject": cfg.filter_subject,
        "opt_start_seq": cfg.start_seq,
        "opt_start_time": if cfg.start_time_ns == 0 { Value::Null } else { Value::String(rfc3339(cfg.start_time_ns)) },
    })
}

/// Append a `Nats-Stream-Source` header line to a stored message's
/// header block (creating the block when the message had none).
/// Value shape, probed against Go v2.15.1:
/// `<origin> <origin_seq> <filter|">"> <subject> <origin_token>`.
pub(crate) fn append_source_header(
    headers: Option<Bytes>,
    origin: &str,
    origin_seq: u64,
    filter: Option<&str>,
    subject: &str,
    token: &str,
) -> Bytes {
    let line = format!(
        "Nats-Stream-Source: {origin} {origin_seq} {} > {subject} {token}",
        filter.unwrap_or(">")
    );
    match headers {
        None => Bytes::from(format!("NATS/1.0\r\n{line}\r\n\r\n")),
        Some(h) => {
            let text = String::from_utf8_lossy(&h);
            let body = text.trim_end_matches(['\r', '\n']);
            let body = if body.starts_with("NATS/") {
                body.to_string()
            } else {
                format!("NATS/1.0\r\n{body}")
            };
            Bytes::from(format!("{body}\r\n{line}\r\n\r\n"))
        }
    }
}
pub(crate) fn header_get(msg: &Message, name: &str) -> Option<String> {
    msg.header_map()
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
}

/// Parse a raw header block into a JSON object of name -> [values].
pub(crate) fn headers_json(headers: &Bytes) -> Value {
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
    pub(crate) fn last_seq(&self) -> u64 {
        if self.messages.is_empty() {
            0
        } else {
            self.base_seq + self.messages.len() as u64 - 1
        }
    }

    pub(crate) fn first_seq(&self) -> u64 {
        // Go reports FirstSeq 0 for a fresh, empty stream (not 1).
        if self.messages.is_empty() {
            return 0;
        }
        // Skip a deleted prefix (workqueue acks from the front).
        let mut seq = self.base_seq;
        while seq < self.base_seq + self.messages.len() as u64 && self.deleted.contains(&seq) {
            seq += 1;
        }
        seq
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.messages
            .iter()
            .filter(|m| !self.deleted.contains(&m.seq))
            .map(|m| {
                m.payload.len() as u64 + m.headers.as_ref().map(|h| h.len() as u64).unwrap_or(0)
            })
            .sum()
    }

    pub(crate) fn get(&self, seq: u64) -> Option<&StoredMsg> {
        if seq < self.base_seq || self.deleted.contains(&seq) {
            return None;
        }
        self.messages.get((seq - self.base_seq) as usize)
    }

    /// Live (non-deleted) message count.
    pub(crate) fn live_count(&self) -> usize {
        self.messages.len().saturating_sub(self.deleted.len())
    }

    pub(crate) fn last_seq_for_subject(&self, subject: &str) -> Option<u64> {
        self.messages
            .iter()
            .rev()
            .filter(|m| !self.deleted.contains(&m.seq))
            .find(|m| m.subject == subject)
            .map(|m| m.seq)
    }

    pub(crate) fn info_json(&self) -> Value {
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
                "max_msgs_per_subject": if self.max_msgs_per_subject <= 0 { -1 } else { self.max_msgs_per_subject },
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
                "mirror": self.mirror.as_ref().map(source_cfg_json),
                "sources": self.sources.iter().map(source_cfg_json).collect::<Vec<_>>(),
            },
            "created": rfc3339(self.created_ns),
            "state": {
                "messages": self.live_count(),
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
    /// True when `subject` matches this consumer's filter: any of
    /// `filter_subjects` when set, else `filter_subject`, else all.
    pub(crate) fn matches_filter(&self, subject: &str) -> bool {
        if !self.filter_subjects.is_empty() {
            return self
                .filter_subjects
                .iter()
                .any(|f| subject_matches(f, subject));
        }
        match &self.filter_subject {
            Some(f) => subject_matches(f, subject),
            None => true,
        }
    }

    /// Go's CONSUMER.INFO `num_pending`: non-deleted stream messages
    /// with sequence greater than the last delivered stream sequence
    /// (`next_stream_seq - 1`) whose subject matches the consumer's
    /// filter. Already-delivered-but-unacked messages (in
    /// `ack_pending`) and redelivery-queue messages have sequences
    /// below `next_stream_seq`, so they are correctly excluded.
    pub(crate) fn num_pending(&self, stream: &Stream) -> u64 {
        let floor = self.next_stream_seq.saturating_sub(1);
        stream
            .messages
            .iter()
            .filter(|m| {
                m.seq > floor && !stream.deleted.contains(&m.seq) && self.matches_filter(&m.subject)
            })
            .count() as u64
    }

    pub(crate) fn info_json(&self, stream_name: &str, stream: &Stream) -> Value {
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
                "filter_subjects": self.filter_subjects,
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
            "num_pending": self.num_pending(stream),
        })
    }

    pub(crate) fn ack_subject(
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
