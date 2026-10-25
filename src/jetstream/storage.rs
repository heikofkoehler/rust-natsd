//! JetStream persistence: the existing atomic JSON snapshot
//! implementation (snapshot v2, including pending ack state).
//!
//! Unchanged in Phase 29: whole-engine JSON, payloads base64-encoded,
//! written via temp file + rename after every mutation, reloaded on
//! startup. The WAL design is a later phase and is not introduced here.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde_json::{json, Value};

use super::{
    make_source_token, now_ns, AckPolicy, Consumer, Engine, JetStream, Pending, Redeliver,
    SourceCfg, StoredMsg, Stream, DEFAULT_ACK_WAIT,
};

impl JetStream {
    /// Snapshot file name inside the store dir.
    pub(crate) fn snapshot_path(dir: &std::path::Path) -> std::path::PathBuf {
        dir.join("rust-natsd-js.json")
    }

    /// Write the current engine state to the store dir, if configured.
    /// Best-effort: errors are logged, never fatal (the in-memory
    /// state remains authoritative until the next successful write).
    pub(crate) fn persist(&self) {
        let Some(dir) = &self.store_dir else {
            return;
        };
        if let Err(e) = std::fs::create_dir_all(dir) {
            eprintln!("rust-natsd: jetstream store dir {dir:?}: {e}");
            return;
        }
        let snapshot = {
            let engine = self.engine.lock().unwrap();
            engine.snapshot_json()
        };
        let tmp = dir.join("rust-natsd-js.json.tmp");
        let path = Self::snapshot_path(dir);
        match std::fs::write(&tmp, serde_json::to_string_pretty(&snapshot).unwrap()) {
            Ok(()) => {
                if let Err(e) = std::fs::rename(&tmp, &path) {
                    eprintln!("rust-natsd: jetstream snapshot rename: {e}");
                }
            }
            Err(e) => eprintln!("rust-natsd: jetstream snapshot write: {e}"),
        }
    }

    /// Load a snapshot from the store dir, if one exists. Returns
    /// None when there is no snapshot (fresh start) or it is
    /// unreadable (in which case the error is logged and the engine
    /// starts empty rather than failing startup).
    pub(crate) fn load_snapshot(dir: &std::path::Path) -> Option<Engine> {
        let path = Self::snapshot_path(dir);
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(_) => return None,
        };
        match Engine::from_snapshot_json(&text) {
            Ok(engine) => Some(engine),
            Err(e) => {
                eprintln!(
                    "rust-natsd: jetstream snapshot {path:?} unreadable ({e}); starting empty"
                );
                None
            }
        }
    }
}

impl Engine {
    /// Process one message. Returns (consumed, actions). API requests and
    /// acks are consumed; stream capture stores and still allows normal
    /// delivery, so it returns consumed=false.
    /// Serialize the whole engine: streams, messages (payloads and
    /// headers base64-encoded via the `base64` crate), dedup ids, and
    /// consumer definitions with their positions, pending ack state
    /// (consumer seq, stream seq, delivery count, wall-clock delivery
    /// time), ack floors, redelivery counts, and pull redelivery
    /// queues (snapshot version 2; version 1 snapshots, which lack
    /// these fields, still load with empty ack state).
    pub(crate) fn snapshot_json(&self) -> Value {
        use base64::Engine as _;
        let enc = base64::engine::general_purpose::STANDARD;
        let streams: Vec<Value> = self
            .streams
            .values()
            .map(|s| {
                let messages: Vec<Value> = s
                    .messages
                    .iter()
                    .filter(|m| !s.deleted.contains(&m.seq))
                    .map(|m| {
                        json!({
                            "seq": m.seq,
                            "subject": m.subject,
                            "headers": m.headers.as_ref().map(|h| enc.encode(h.as_ref())),
                            "payload": enc.encode(m.payload.as_ref()),
                            "time_ns": m.time_ns,
                        })
                    })
                    .collect();
                let consumers: Vec<Value> = s
                    .consumers
                    .values()
                    .map(|c| {
                        let mut pending: Vec<(&u64, &Pending)> = c.pending.iter().collect();
                        pending.sort_by_key(|(cseq, _)| **cseq);
                        let pending_json: Vec<Value> = pending
                            .iter()
                            .map(|(cseq, p)| {
                                json!({
                                    "consumer_seq": **cseq,
                                    "stream_seq": p.stream_seq,
                                    "delivered": p.delivered,
                                    "delivered_at_ns": p.delivered_at_ns,
                                })
                            })
                            .collect();
                        let redeliver_json: Vec<Value> = c
                            .redeliver
                            .iter()
                            .map(|r| {
                                json!({
                                    "stream_seq": r.stream_seq,
                                    "delivered": r.delivered,
                                })
                            })
                            .collect();
                        json!({
                            "name": c.name,
                            "deliver_subject": c.deliver_subject,
                            "filter_subject": c.filter_subject,
                            "filter_subjects": c.filter_subjects,
                            "ack_policy": match c.ack_policy {
                                AckPolicy::Explicit => "explicit",
                                AckPolicy::None => "none",
                                AckPolicy::All => "all",
                            },
                            "ack_wait_ns": c.ack_wait.as_nanos() as u64,
                            "max_deliver": c.max_deliver,
                            "max_ack_pending": c.max_ack_pending,
                            "deliver_policy": c.deliver_policy,
                            "next_stream_seq": c.next_stream_seq,
                            "consumer_seq": c.consumer_seq,
                            "ack_floor_stream_seq": c.ack_floor_stream_seq,
                            "ack_floor_consumer_seq": c.ack_floor_consumer_seq,
                            "num_redelivered": c.num_redelivered,
                            "pending": pending_json,
                            "redeliver": redeliver_json,
                        })
                    })
                    .collect();
                let msg_ids: Value = s
                    .msg_ids
                    .iter()
                    .map(|(id, (seq, _))| (id.clone(), json!(seq)))
                    .collect();
                json!({
                    "name": s.name,
                    "account": s.account,
                    "created_ns": s.created_ns,
                    "subjects": s.subjects,
                    "retention": s.retention,
                    "max_msgs": s.max_msgs,
                    "max_bytes": s.max_bytes,
                    "max_age_ns": s.max_age_ns,
                    "max_msgs_per_subject": s.max_msgs_per_subject,
                    "storage": s.storage,
                    "base_seq": s.base_seq,
                    "messages": messages,
                    "msg_ids": msg_ids,
                    "consumers": consumers,
                    "mirror": s.mirror.as_ref().map(|c| json!({
                        "name": c.name,
                        "filter_subject": c.filter_subject,
                        "opt_start_seq": c.start_seq,
                        "opt_start_time_ns": c.start_time_ns,
                        "live": c.live,
                    })),
                    "sources": s.sources.iter().map(|c| json!({
                        "name": c.name,
                        "filter_subject": c.filter_subject,
                        "opt_start_seq": c.start_seq,
                        "opt_start_time_ns": c.start_time_ns,
                        "live": c.live,
                    })).collect::<Vec<_>>(),
                    "source_token": s.source_token,
                })
            })
            .collect();
        json!({ "version": 2, "streams": streams })
    }

    /// Restore an engine from `snapshot_json` output. Deleted
    /// sequences are not stored (deleted messages are simply absent,
    /// so a restored stream's first_seq may jump past a deleted
    /// prefix — equivalent, since those messages are gone).
    pub(crate) fn from_snapshot_json(text: &str) -> Result<Engine, String> {
        use base64::Engine as _;
        let dec = base64::engine::general_purpose::STANDARD;
        let root: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let mut engine = Engine {
            streams: HashMap::new(),
        };
        let Some(streams) = root.get("streams").and_then(|v| v.as_array()) else {
            return Err("snapshot has no streams array".to_string());
        };
        for sv in streams {
            let get_s = |k: &str| sv.get(k).and_then(|v| v.as_str()).unwrap_or("");
            let get_i = |k: &str| sv.get(k).and_then(|v| v.as_i64()).unwrap_or(0);
            let get_u = |k: &str| sv.get(k).and_then(|v| v.as_u64()).unwrap_or(0);
            let parse_snap_cfg = |v: &Value| -> SourceCfg {
                SourceCfg {
                    name: v
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("")
                        .to_string(),
                    filter_subject: v
                        .get("filter_subject")
                        .and_then(|f| f.as_str())
                        .map(|s| s.to_string()),
                    start_seq: v.get("opt_start_seq").and_then(|n| n.as_u64()).unwrap_or(0),
                    start_time_ns: v
                        .get("opt_start_time_ns")
                        .and_then(|n| n.as_u64())
                        .unwrap_or(0),
                    live: v.get("live").and_then(|n| n.as_bool()).unwrap_or(true),
                }
            };
            let mirror = sv.get("mirror").and_then(|v| {
                if v.is_null() {
                    None
                } else {
                    Some(parse_snap_cfg(v))
                }
            });
            let sources: Vec<SourceCfg> = sv
                .get("sources")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().map(&parse_snap_cfg).collect())
                .unwrap_or_default();
            let created_ns = get_u("created_ns");
            let mut stream = Stream {
                name: get_s("name").to_string(),
                account: {
                    let a = get_s("account");
                    if a.is_empty() {
                        "$G".to_string()
                    } else {
                        a.to_string()
                    }
                },
                created_ns,
                subjects: sv
                    .get("subjects")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default(),
                retention: get_s("retention").to_string(),
                max_msgs: get_i("max_msgs"),
                max_bytes: get_i("max_bytes"),
                max_age_ns: get_i("max_age_ns"),
                max_msgs_per_subject: get_i("max_msgs_per_subject"),
                deleted: std::collections::HashSet::new(),
                storage: get_s("storage").to_string(),
                messages: VecDeque::new(),
                base_seq: get_u("base_seq"),
                msg_ids: HashMap::new(),
                consumers: HashMap::new(),
                mirror,
                sources,
                source_token: sv
                    .get("source_token")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| make_source_token(get_s("name"), created_ns)),
            };
            if stream.base_seq == 0 {
                stream.base_seq = 1;
            }
            // Load stored messages into a map, then rebuild the deque
            // over base_seq..=last_seq, inserting placeholders for
            // missing sequences (deleted messages are not stored).
            // Placeholders keep VecDeque indexing exact and are
            // marked deleted, so get()/bytes()/first_seq() skip them
            // exactly as before the restart.
            let mut by_seq: HashMap<u64, StoredMsg> = HashMap::new();
            if let Some(msgs) = sv.get("messages").and_then(|v| v.as_array()) {
                for mv in msgs {
                    let payload = mv
                        .get("payload")
                        .and_then(|v| v.as_str())
                        .and_then(|b| dec.decode(b).ok())
                        .unwrap_or_default();
                    let headers = mv
                        .get("headers")
                        .and_then(|v| v.as_str())
                        .and_then(|b| dec.decode(b).ok())
                        .map(Bytes::from);
                    let stored = StoredMsg {
                        seq: mv.get("seq").and_then(|v| v.as_u64()).unwrap_or(0),
                        subject: mv
                            .get("subject")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        headers,
                        payload: Bytes::from(payload),
                        time_ns: mv.get("time_ns").and_then(|v| v.as_u64()).unwrap_or(0),
                    };
                    by_seq.insert(stored.seq, stored);
                }
            }
            if let Some(last) = by_seq.keys().max().copied() {
                let base = stream.base_seq;
                for seq in base..=last {
                    match by_seq.remove(&seq) {
                        Some(m) => stream.messages.push_back(m),
                        None => {
                            stream.deleted.insert(seq);
                            stream.messages.push_back(StoredMsg {
                                seq,
                                subject: String::new(),
                                headers: None,
                                payload: Bytes::new(),
                                time_ns: 0,
                            });
                        }
                    }
                }
            }
            if let Some(ids) = sv.get("msg_ids").and_then(|v| v.as_object()) {
                for (id, seq) in ids {
                    if let Some(seq) = seq.as_u64() {
                        stream.msg_ids.insert(id.clone(), (seq, Instant::now()));
                    }
                }
            }
            if let Some(consumers) = sv.get("consumers").and_then(|v| v.as_array()) {
                for cv in consumers {
                    let ack_policy = match cv
                        .get("ack_policy")
                        .and_then(|v| v.as_str())
                        .unwrap_or("explicit")
                    {
                        "none" => AckPolicy::None,
                        "all" => AckPolicy::All,
                        _ => AckPolicy::Explicit,
                    };
                    let name = cv
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    // Ack wait of 0 in an old snapshot means "default"
                    // (creation maps 0 to DEFAULT_ACK_WAIT); keep that
                    // mapping here so restored deadlines are sane.
                    let ack_wait_ns = cv.get("ack_wait_ns").and_then(|v| v.as_u64()).unwrap_or(0);
                    let ack_wait = if ack_wait_ns == 0 {
                        DEFAULT_ACK_WAIT
                    } else {
                        Duration::from_nanos(ack_wait_ns)
                    };
                    let is_push = cv.get("deliver_subject").and_then(|v| v.as_str()).is_some();
                    let max_deliver = cv.get("max_deliver").and_then(|v| v.as_i64()).unwrap_or(-1);
                    // Restore pending ack state. Each entry's deadline
                    // is recomputed from its wall-clock delivery time
                    // plus ack_wait, preserving the remaining window
                    // (Go behaviour, probed against v2.15.1). Entries
                    // already past their deadline for a pull consumer
                    // are queued for immediate redelivery (with the
                    // incremented delivery count); push entries keep a
                    // now deadline so the next tick redelivers them.
                    // Max-deliver-exhausted entries are dropped.
                    // Missing fields (version 1 snapshots) mean empty
                    // ack state, exactly as before.
                    let now = Instant::now();
                    let now_wall = now_ns();
                    let mut pending: HashMap<u64, Pending> = HashMap::new();
                    // Entries already past their deadline are queued
                    // after the persisted redelivery queue (which was
                    // queued first, before the restart).
                    let mut expired: Vec<Redeliver> = Vec::new();
                    let mut num_redelivered = cv
                        .get("num_redelivered")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    if let Some(entries) = cv.get("pending").and_then(|v| v.as_array()) {
                        let mut restored: Vec<(u64, u64, u64, u64)> = entries
                            .iter()
                            .filter_map(|pv| {
                                Some((
                                    pv.get("consumer_seq")?.as_u64()?,
                                    pv.get("stream_seq")?.as_u64()?,
                                    pv.get("delivered")?.as_u64()?,
                                    pv.get("delivered_at_ns")?.as_u64()?,
                                ))
                            })
                            .collect();
                        // Redelivery order is stream order (Go).
                        restored.sort_by_key(|(_, sseq, _, _)| *sseq);
                        for (cseq, sseq, delivered, delivered_at_ns) in restored {
                            if max_deliver > 0 && delivered >= max_deliver as u64 {
                                continue;
                            }
                            let deadline_wall =
                                delivered_at_ns.saturating_add(ack_wait.as_nanos() as u64);
                            if deadline_wall <= now_wall {
                                if is_push {
                                    pending.insert(
                                        cseq,
                                        Pending {
                                            stream_seq: sseq,
                                            delivered,
                                            deadline: now,
                                            delivered_at_ns,
                                        },
                                    );
                                } else {
                                    expired.push(Redeliver {
                                        stream_seq: sseq,
                                        delivered: delivered + 1,
                                    });
                                    num_redelivered += 1;
                                }
                            } else {
                                let remaining = Duration::from_nanos(deadline_wall - now_wall);
                                pending.insert(
                                    cseq,
                                    Pending {
                                        stream_seq: sseq,
                                        delivered,
                                        deadline: now + remaining,
                                        delivered_at_ns,
                                    },
                                );
                            }
                        }
                    }
                    // Persisted pull redelivery queue (from NAKs or a
                    // tick that queued before the restart) survives
                    // verbatim, ahead of freshly expired pending.
                    let mut redeliver: VecDeque<Redeliver> = VecDeque::new();
                    if let Some(entries) = cv.get("redeliver").and_then(|v| v.as_array()) {
                        for rv in entries {
                            if let (Some(sseq), Some(delivered)) = (
                                rv.get("stream_seq").and_then(|v| v.as_u64()),
                                rv.get("delivered").and_then(|v| v.as_u64()),
                            ) {
                                // A queued count past the cap is
                                // already exhausted (see
                                // fulfill_pulls guard); drop it
                                // rather than resurrecting it.
                                if max_deliver > 0 && delivered > max_deliver as u64 {
                                    continue;
                                }
                                redeliver.push_back(Redeliver {
                                    stream_seq: sseq,
                                    delivered,
                                });
                            }
                        }
                    }
                    redeliver.extend(expired);
                    stream.consumers.insert(
                        name.clone(),
                        Consumer {
                            name,
                            created_ns: now_ns(),
                            deliver_subject: cv
                                .get("deliver_subject")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            filter_subject: cv
                                .get("filter_subject")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            filter_subjects: cv
                                .get("filter_subjects")
                                .and_then(|v| v.as_array())
                                .map(|a| {
                                    a.iter()
                                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                        .collect()
                                })
                                .unwrap_or_default(),
                            ack_policy,
                            ack_wait,
                            max_deliver,
                            max_ack_pending: cv
                                .get("max_ack_pending")
                                .and_then(|v| v.as_i64())
                                .unwrap_or(1000),
                            deliver_policy: cv
                                .get("deliver_policy")
                                .and_then(|v| v.as_str())
                                .unwrap_or("all")
                                .to_string(),
                            next_stream_seq: cv
                                .get("next_stream_seq")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(1),
                            consumer_seq: cv
                                .get("consumer_seq")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0),
                            pending,
                            redeliver,
                            waiting_pulls: VecDeque::new(),
                            ack_floor_stream_seq: cv
                                .get("ack_floor_stream_seq")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0),
                            ack_floor_consumer_seq: cv
                                .get("ack_floor_consumer_seq")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0),
                            num_redelivered,
                        },
                    );
                }
            }
            engine.streams.insert(stream.name.clone(), stream);
        }
        Ok(engine)
    }
}
