//! Push/pull consumer state machines: deliver_policy, ack_policy,
//! max_deliver, ack_wait timers, `MSG.NEXT` handling, the consumer API
//! handlers, the maintenance `tick`, and the `redelivery_loop` driver.
//!
//! Redelivery defers while the deliver subject has no interest, and
//! actions are dispatched only after the engine lock is released.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde_json::{json, Value};

use super::api::{api_error_action, api_response, err_not_found, valid_name};
use super::{
    now_ns, rfc3339, AckPolicy, Action, Consumer, Engine, JetStream, Pending, Redeliver, StoredMsg,
    Stream, WaitingPull, DEDUP_WINDOW, DEFAULT_ACK_WAIT, TICK,
};
use crate::server::{deliver, Message, Origin, Shared};
use crate::subject::subject_matches;

/// Deliver one stored message to a consumer. Records the pending entry
/// (unless ack policy is none). `delivered` is 1 for a fresh delivery
/// and the incremented count for a redelivery; it is carried in the
/// ack subject, which is how clients report `NumDelivered`.
///
/// Phase 31: the `$JS.ACK` reply subject is attached even for ack
/// policy none (Go v2.15.1, probed: an ack-none push delivery still
/// carries `$JS.ACK.<stream>.<consumer>...` as its reply). Clients
/// need it: async-nats KV watch parses every delivered message's
/// reply to recover stream/consumer sequences, and fails the watch
/// ("did not found reply subject") without it. Only the pending
/// tracking is skipped for ack none, so no redelivery ever happens.
pub(crate) fn deliver_to_consumer(
    stream_name: &str,
    account: &str,
    consumer: &mut Consumer,
    msg: &StoredMsg,
    to_subject: &str,
    delivered: u64,
    actions: &mut Vec<Action>,
) {
    consumer.consumer_seq += 1;
    let cseq = consumer.consumer_seq;
    let pending_count = consumer.pending.len() + 1;
    let reply = consumer.ack_subject(stream_name, delivered, msg.seq, cseq, pending_count);
    // Frame subject is the original stream message subject; routing
    // goes to the deliver subject / pull reply inbox (see route_via).
    actions.push(Action::Deliver(
        Message::new(
            &msg.subject,
            Some(reply),
            msg.headers.clone(),
            msg.payload.clone(),
        )
        .with_account(account)
        .with_route_via(to_subject),
    ));
    if consumer.ack_policy == AckPolicy::None {
        return;
    }
    consumer.pending.insert(
        cseq,
        Pending {
            stream_seq: msg.seq,
            delivered,
            deadline: Instant::now() + consumer.ack_wait,
            delivered_at_ns: now_ns(),
        },
    );
}

/// Publish a max-deliveries advisory when a consumer exhausts its
/// `max_deliver` cap for one message. Go v2.15.1 shape, probed:
/// subject `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>`,
/// body `{"type":"io.nats.jetstream.advisory.v1.max_deliver",
/// "stream":..., "consumer":..., "stream_seq":..., "deliveries":...}`.
/// Go also carries an `id` (nuid) and a `timestamp`; rust-natsd emits
/// a timestamp and a deterministic id derived from stream/consumer/
/// sequence/time (no nuid dependency). The advisory is emitted exactly
/// once per exhaustion: the pending entry and any redelivery queue
/// entry are removed at the same site, so no later path re-emits it.
/// Workqueue retention does NOT delete the message on exhaustion
/// (probed: Go leaves the message in the stream, `num_pending` 0,
/// `num_ack_pending` 0, advisory published); only plain +ACK deletes
/// a workqueue message, as before.
pub(crate) fn max_deliver_advisory_action(
    stream_name: &str,
    account: &str,
    consumer_name: &str,
    stream_seq: u64,
    deliveries: u64,
) -> Action {
    let body = json!({
        "type": "io.nats.jetstream.advisory.v1.max_deliver",
        "id": format!("{stream_name}-{consumer_name}-{stream_seq}-{}", now_ns()),
        "timestamp": rfc3339(now_ns()),
        "stream": stream_name,
        "consumer": consumer_name,
        "stream_seq": stream_seq,
        "deliveries": deliveries,
    });
    Action::Deliver(
        Message::new(
            &format!("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.{stream_name}.{consumer_name}"),
            None,
            None,
            Bytes::from(body.to_string()),
        )
        .with_account(account),
    )
}

/// Deliver the next available messages to a push consumer, honouring
/// max_ack_pending and the filter. Advances next_stream_seq past every
/// sequence considered (matching or not).
pub(crate) fn drain_push(
    stream_name: &str,
    stream: &mut Stream,
    consumer_name: &str,
    actions: &mut Vec<Action>,
) {
    loop {
        let (deliver_subject, matches_filter, max_pending, pending_len, seq) = {
            let Some(c) = stream.consumers.get(consumer_name) else {
                return;
            };
            let Some(ds) = c.deliver_subject.clone() else {
                return;
            };
            (
                ds,
                (c.filter_subject.clone(), c.filter_subjects.clone()),
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
            // A deleted gap (workqueue ack, per-subject eviction) is
            // skipped, not the end of the stream; only a sequence past
            // the last stored message ends the drain.
            if seq <= stream.last_seq() {
                let cname = stream.consumers.get(consumer_name).unwrap().name.clone();
                stream.consumers.get_mut(&cname).unwrap().next_stream_seq = seq + 1;
                continue;
            }
            break;
        };
        let (filter_subject, filter_subjects) = matches_filter;
        let matches = if !filter_subjects.is_empty() {
            filter_subjects
                .iter()
                .any(|f| subject_matches(f, &msg.subject))
        } else {
            match &filter_subject {
                Some(f) => subject_matches(f, &msg.subject),
                None => true,
            }
        };
        let consumer = stream.consumers.get_mut(consumer_name).unwrap();
        consumer.next_stream_seq = seq + 1;
        if !matches {
            continue;
        }
        let account = stream.account.clone();
        let ack_none = consumer.ack_policy == AckPolicy::None;
        let workqueue = stream.retention == "workqueue";
        deliver_to_consumer(
            stream_name,
            &account,
            consumer,
            &msg,
            &deliver_subject,
            1,
            actions,
        );
        // Workqueue with ack policy none: no ack will ever come, so
        // delivery itself deletes the message (as in nats-server).
        if ack_none && workqueue {
            stream.deleted.insert(msg.seq);
        }
    }
}

/// Try to satisfy waiting pulls of one consumer from newly available
/// messages (redelivery queue first, then fresh sequences).
pub(crate) fn fulfill_pulls(
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
        let Some((reply, filters)) = ({
            let Some(c) = stream.consumers.get(consumer_name) else {
                return;
            };
            c.waiting_pulls.front().map(|p| {
                (
                    p.reply.clone(),
                    (c.filter_subject.clone(), c.filter_subjects.clone()),
                )
            })
        }) else {
            return;
        };
        // Pick the next sequence: redelivery first, else fresh.
        // A redelivery carries its incremented delivery count; a fresh
        // sequence carries 1.
        let (seq, delivered) = {
            let next = stream.consumers.get(consumer_name).unwrap().next_stream_seq;
            if let Some(r) = stream
                .consumers
                .get_mut(consumer_name)
                .unwrap()
                .redeliver
                .pop_front()
            {
                (Some(r.stream_seq), r.delivered)
            } else if stream.get(next).is_some() {
                stream
                    .consumers
                    .get_mut(consumer_name)
                    .unwrap()
                    .next_stream_seq = next + 1;
                (Some(next), 1)
            } else if next <= stream.last_seq() {
                // Deleted gap: skip it and retry for a live message.
                stream
                    .consumers
                    .get_mut(consumer_name)
                    .unwrap()
                    .next_stream_seq = next + 1;
                continue;
            } else {
                (None, 1)
            }
        };
        let Some(seq) = seq else {
            return;
        };
        // Defensive exhaustion guard for redelivery queue entries
        // that predate the NAK/tick enforcement (e.g. restored from
        // an old snapshot): a queued delivery count past the cap is
        // dropped with an advisory, never delivered. Fresh deliveries
        // carry delivered=1, so this only fires on redeliveries.
        {
            let (max_deliver, account) = {
                let c = stream.consumers.get(consumer_name).unwrap();
                (c.max_deliver, stream.account.clone())
            };
            if max_deliver > 0 && delivered > max_deliver as u64 {
                actions.push(max_deliver_advisory_action(
                    stream_name,
                    &account,
                    consumer_name,
                    seq,
                    delivered - 1,
                ));
                continue;
            }
        }
        let Some(msg) = stream.get(seq).map(|m| StoredMsg {
            seq: m.seq,
            subject: m.subject.clone(),
            headers: m.headers.clone(),
            payload: m.payload.clone(),
            time_ns: m.time_ns,
        }) else {
            continue;
        };
        let (filter_subject, filter_subjects) = filters;
        let matches = if !filter_subjects.is_empty() {
            filter_subjects
                .iter()
                .any(|f| subject_matches(f, &msg.subject))
        } else {
            match &filter_subject {
                Some(f) => subject_matches(f, &msg.subject),
                None => true,
            }
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
        deliver_to_consumer(
            stream_name,
            &account,
            consumer,
            &msg,
            &reply,
            delivered,
            actions,
        );
    }
}

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn api_consumer_create(
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
        let mut name = from_subject
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
        // Phase 31: a create with no name at all is an ephemeral
        // consumer (Go assigns a server-side name; async-nats KV
        // watch and ordered consumers create exactly this way).
        // A present-but-invalid name is still rejected below.
        if name.is_empty() {
            let mut salt = crate::jetstream::now_ns();
            loop {
                let candidate = format!("eph-{salt:x}");
                if !stream.consumers.contains_key(&candidate) {
                    name = candidate;
                    break;
                }
                salt += 1;
            }
        }
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
        let filter_subjects: Vec<String> = config
            .get("filter_subjects")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();
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
                // With a filter, "last" means the last message matching
                // the filter, not the stream's last message (which the
                // drain would otherwise skip past).
                let filter_matches = |subject: &str| -> bool {
                    if !filter_subjects.is_empty() {
                        filter_subjects.iter().any(|f| subject_matches(f, subject))
                    } else if let Some(filter) = &filter_subject {
                        subject_matches(filter, subject)
                    } else {
                        true
                    }
                };
                if !filter_subjects.is_empty() || filter_subject.is_some() {
                    stream
                        .messages
                        .iter()
                        .rev()
                        .find(|m| !stream.deleted.contains(&m.seq) && filter_matches(&m.subject))
                        .map(|m| m.seq)
                        .unwrap_or_else(|| stream.last_seq() + 1)
                } else if stream.messages.is_empty() {
                    stream.base_seq + stream.messages.len() as u64
                } else {
                    stream.last_seq()
                }
            }
            "by_start_sequence" => {
                let wanted = config
                    .get("opt_start_seq")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                wanted.max(stream.first_seq())
            }
            // Phase 32: needed by the cross-node mirror pull loop,
            // which translates a link's opt_start_time into a
            // by_start_time consumer. The first message at or after
            // the timestamp, else just past the last message.
            "by_start_time" => {
                let wanted = match config.get("opt_start_time") {
                    Some(Value::Number(n)) => n.as_u64().unwrap_or(0),
                    Some(Value::String(s)) => crate::jetstream::parse_rfc3339_ns(s).unwrap_or(0),
                    _ => 0,
                };
                stream
                    .messages
                    .iter()
                    .filter(|m| !stream.deleted.contains(&m.seq) && m.time_ns >= wanted)
                    .map(|m| m.seq)
                    .next()
                    .unwrap_or_else(|| stream.last_seq() + 1)
            }
            _ => stream.first_seq(),
        };
        let consumer = Consumer {
            name: name.clone(),
            created_ns: now_ns(),
            deliver_subject,
            filter_subject,
            filter_subjects,
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
        let info = consumer.info_json(stream_name, stream);
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

    pub(crate) fn api_consumer_info(
        &mut self,
        account: &str,
        stream_name: &str,
        consumer_name: &str,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        match self.stream_for(stream_name, account) {
            Some(stream) => match stream.consumers.get(consumer_name) {
                Some(consumer) => {
                    let info = consumer.info_json(stream_name, stream);
                    api_response(account, reply, type_name, info, actions);
                }
                None => {
                    let (code, err_code, desc) = err_not_found("consumer");
                    api_error_action(account, reply, type_name, code, err_code, &desc, actions);
                }
            },
            None => {
                let (code, err_code, desc) = err_not_found("consumer");
                api_error_action(account, reply, type_name, code, err_code, &desc, actions);
            }
        }
    }

    pub(crate) fn api_consumer_list(
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
            .filter_map(|n| {
                stream
                    .consumers
                    .get(n)
                    .map(|c| c.info_json(stream_name, stream))
            })
            .collect();
        api_response(
            account,
            reply,
            type_name,
            json!({ "consumers": consumers, "total": total, "offset": offset, "limit": 1024 }),
            actions,
        );
    }

    pub(crate) fn api_consumer_delete(
        &mut self,
        account: &str,
        stream_name: &str,
        consumer_name: &str,
        reply: &str,
        type_name: &str,
        actions: &mut Vec<Action>,
    ) {
        let removed = self
            .stream_for_mut(stream_name, account)
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

    pub(crate) fn api_consumer_msg_next(
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
            let (seq, delivered_count) = {
                let next = stream.consumers.get(consumer_name).unwrap().next_stream_seq;
                if let Some(r) = stream
                    .consumers
                    .get_mut(consumer_name)
                    .unwrap()
                    .redeliver
                    .pop_front()
                {
                    (Some(r.stream_seq), r.delivered)
                } else if stream.get(next).is_some() {
                    stream
                        .consumers
                        .get_mut(consumer_name)
                        .unwrap()
                        .next_stream_seq = next + 1;
                    (Some(next), 1)
                } else {
                    (None, 1)
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
            // Defensive exhaustion guard (see fulfill_pulls): a
            // redelivery queue entry past the cap is dropped with an
            // advisory, never delivered.
            {
                let (max_deliver, account) = {
                    let c = stream.consumers.get(consumer_name).unwrap();
                    (c.max_deliver, stream.account.clone())
                };
                if max_deliver > 0 && delivered_count > max_deliver as u64 {
                    actions.push(max_deliver_advisory_action(
                        stream_name,
                        &account,
                        consumer_name,
                        seq,
                        delivered_count - 1,
                    ));
                    continue;
                }
            }
            let matches = stream
                .consumers
                .get(consumer_name)
                .unwrap()
                .matches_filter(&m.subject);
            if !matches {
                continue;
            }
            let consumer = stream.consumers.get_mut(consumer_name).unwrap();
            let account = stream.account.clone();
            deliver_to_consumer(
                stream_name,
                &account,
                consumer,
                &m,
                reply,
                delivered_count,
                actions,
            );
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
            actions.push(Action::Deliver(
                Message::new(
                    reply,
                    None,
                    Some(Bytes::from(
                        "NATS/1.0\r\nStatus: 404\r\nDescription: No Messages\r\n\r\n",
                    )),
                    Bytes::new(),
                )
                .with_account(account),
            ));
        }
    }

    /// Background maintenance: max-age eviction, redelivery of expired
    /// pending messages, expiry of waiting pulls, push backlog drains,
    /// dedup-window pruning. Returns actions to send.
    pub(crate) fn tick(&mut self, has_interest: &dyn Fn(&str, &str) -> bool) -> Vec<Action> {
        let mut actions = Vec::new();
        let now = Instant::now();
        let now_ns = now_ns();
        let stream_keys: Vec<(String, String)> = self.streams.keys().cloned().collect();
        for (account, name) in stream_keys {
            let stream = self.stream_for_mut(&name, &account).unwrap();
            // Max-age eviction.
            if stream.max_age_ns > 0 {
                let cutoff = now_ns.saturating_sub(stream.max_age_ns as u64);
                while let Some(front) = stream.messages.front() {
                    if front.time_ns < cutoff {
                        let seq = front.seq;
                        stream.messages.pop_front();
                        stream.deleted.remove(&seq);
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
                        actions.push(Action::Deliver(
                            Message::new(
                                &pull.reply,
                                None,
                                Some(Bytes::from(
                                    "NATS/1.0\r\nStatus: 404\r\nDescription: No Messages\r\n\r\n",
                                )),
                                Bytes::new(),
                            )
                            .with_account(&account),
                        ));
                    }
                }
                // Redeliver expired pending messages, in stream
                // order (Go serves redeliveries oldest-first).
                let mut due: Vec<(u64, u64, u64)> = {
                    let c = stream.consumers.get_mut(&cname).unwrap();
                    c.pending
                        .iter()
                        .filter(|(_, p)| p.deadline <= now)
                        .map(|(cseq, p)| (*cseq, p.stream_seq, p.delivered))
                        .collect()
                };
                due.sort_by_key(|(_, sseq, _)| *sseq);
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
                    if c.max_deliver > 0 && delivered >= c.max_deliver as u64 {
                        // Ack-wait exhaustion: drop with advisory
                        // and advance the ack floor (Go's
                        // hasMaxDeliveries), no redelivery, no
                        // deletion (workqueue messages stay).
                        if cseq > c.ack_floor_consumer_seq {
                            c.ack_floor_consumer_seq = cseq;
                            c.ack_floor_stream_seq = sseq;
                        }
                        actions.push(max_deliver_advisory_action(
                            &name, &account, &cname, sseq, delivered,
                        ));
                        continue;
                    }
                    if c.deliver_subject.is_some() {
                        // No interest on the deliver subject yet (e.g.
                        // the client has not reconnected after a
                        // restart): defer without burning a delivery
                        // cycle, as Go does — the next tick retries.
                        // A delivery to a subject nobody reads would
                        // otherwise inflate the delivery count. The
                        // original delivery time is kept so a restart
                        // during deferral does not stretch the window.
                        let to = c.deliver_subject.clone().unwrap();
                        if !has_interest(&account, &to) {
                            c.pending.insert(
                                cseq,
                                Pending {
                                    stream_seq: sseq,
                                    delivered,
                                    deadline: now + TICK,
                                    delivered_at_ns: pending.delivered_at_ns,
                                },
                            );
                            continue;
                        }
                        if let Some(m) = staged {
                            let nd = delivered + 1;
                            c.consumer_seq += 1;
                            let ncseq = c.consumer_seq;
                            let reply = c.ack_subject(&name, nd, sseq, ncseq, c.pending.len() + 1);
                            actions.push(Action::Deliver(
                                Message::new(
                                    &m.subject,
                                    Some(reply),
                                    m.headers.clone(),
                                    m.payload.clone(),
                                )
                                .with_account(&account)
                                .with_route_via(&to),
                            ));
                            c.pending.insert(
                                ncseq,
                                Pending {
                                    stream_seq: sseq,
                                    delivered: nd,
                                    deadline: now + c.ack_wait,
                                    delivered_at_ns: now_ns,
                                },
                            );
                            c.num_redelivered += 1;
                        }
                    } else {
                        c.redeliver.push_back(Redeliver {
                            stream_seq: sseq,
                            delivered: delivered + 1,
                        });
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

/// Background task: redelivers consumer messages whose ack deadline
/// passed, expires waiting pulls, evicts over-age messages.
pub async fn redelivery_loop(js: std::sync::Arc<JetStream>, shared: std::sync::Arc<Shared>) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(TICK) => {
                let actions = {
                    let mut engine = js.engine.lock().unwrap();
                    engine.tick(&|account, subject| {
                        crate::server::has_interest(&shared, account, subject)
                    })
                };
                // Tick can evict messages, so it is a mutation too.
                js.persist();
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
