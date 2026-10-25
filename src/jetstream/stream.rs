//! Stream state: lookup, message capture, `Nats-Msg-Id` dedup,
//! retention policies (Limits, Interest, WorkQueue), purge helpers,
//! and same-node mirroring/sourcing (backfill + live copies).
//!
//! Engine methods here never deliver while holding the engine lock;
//! fan-out produces `Action`s for the caller to dispatch.

use std::time::Instant;

use bytes::Bytes;
use serde_json::json;

use super::api::parse_u64;
use super::consumer::{drain_push, fulfill_pulls, max_deliver_advisory_action};
use super::{
    append_source_header, header_get, now_ns, Action, Engine, Pending, Redeliver, SourceCfg,
    StoredMsg, Stream,
};
use crate::server::Message;
use crate::subject::subject_matches;

impl Engine {
    /// Stream by name, only when it belongs to `account`.
    pub(crate) fn stream_for(&self, name: &str, account: &str) -> Option<&Stream> {
        self.streams.get(name).filter(|s| s.account == account)
    }

    pub(crate) fn stream_for_mut(&mut self, name: &str, account: &str) -> Option<&mut Stream> {
        self.streams.get_mut(name).filter(|s| s.account == account)
    }

    /// Append one message to a stream, enforcing its limits
    /// (discard-old). Bypasses dedup/expected-sequence checks: it is
    /// used for direct captures (after those checks) and for
    /// mirror/source copies (which are internal, not publishes).
    /// Returns the new sequence.
    pub(crate) fn store_message(
        stream: &mut Stream,
        subject: &str,
        headers: Option<Bytes>,
        payload: Bytes,
        time_ns: u64,
    ) -> u64 {
        let seq = stream.base_seq + stream.messages.len() as u64;
        stream.messages.push_back(StoredMsg {
            seq,
            subject: subject.to_string(),
            headers,
            payload,
            time_ns,
        });
        if stream.max_msgs > 0 {
            while stream.live_count() as i64 > stream.max_msgs {
                if let Some(front) = stream.messages.pop_front() {
                    stream.deleted.remove(&front.seq);
                }
                stream.base_seq += 1;
            }
        }
        if stream.max_bytes > 0 {
            while stream.bytes() > stream.max_bytes as u64 {
                if let Some(front) = stream.messages.pop_front() {
                    stream.deleted.remove(&front.seq);
                } else {
                    break;
                }
                stream.base_seq += 1;
            }
        }
        if stream.max_msgs_per_subject > 0 {
            let limit = stream.max_msgs_per_subject as usize;
            loop {
                let count = stream
                    .messages
                    .iter()
                    .filter(|m| m.subject == subject && !stream.deleted.contains(&m.seq))
                    .count();
                if count <= limit {
                    break;
                }
                let front_is_match = stream
                    .messages
                    .front()
                    .map(|m| m.subject == subject && !stream.deleted.contains(&m.seq))
                    .unwrap_or(false);
                if front_is_match {
                    if let Some(front) = stream.messages.pop_front() {
                        stream.deleted.remove(&front.seq);
                    }
                    stream.base_seq += 1;
                } else if let Some(oldest) = stream
                    .messages
                    .iter()
                    .find(|m| m.subject == subject && !stream.deleted.contains(&m.seq))
                    .map(|m| m.seq)
                {
                    stream.deleted.insert(oldest);
                } else {
                    break;
                }
            }
        }
        seq
    }

    /// Fan out one stored message to a stream's push consumers and
    /// waiting pulls.
    pub(crate) fn fan_out(&mut self, stream_name: &str, account: &str, actions: &mut Vec<Action>) {
        let push_names: Vec<String> = self
            .streams
            .get(stream_name)
            .map(|s| {
                s.consumers
                    .values()
                    .filter(|c| c.deliver_subject.is_some())
                    .map(|c| c.name.clone())
                    .collect()
            })
            .unwrap_or_default();
        for cname in push_names {
            let stream = self.stream_for_mut(stream_name, account).unwrap();
            drain_push(stream_name, stream, &cname, actions);
        }
        let pull_names: Vec<String> = self
            .streams
            .get(stream_name)
            .map(|s| {
                s.consumers
                    .values()
                    .filter(|c| c.deliver_subject.is_none() && !c.waiting_pulls.is_empty())
                    .map(|c| c.name.clone())
                    .collect()
            })
            .unwrap_or_default();
        for cname in pull_names {
            let stream = self.stream_for_mut(stream_name, account).unwrap();
            fulfill_pulls(stream_name, stream, &cname, actions);
        }
    }

    /// Live-copy one origin message into every stream with a live
    /// mirror/source link from this origin, then recurse so chains
    /// (mirror of a mirror, source of a source) propagate. `visited`
    /// guards against pathological cycles (creation-time cycle
    /// rejection makes them unreachable, but a cheap guard beats a
    /// stack overflow). Mirror copies are exact; source copies add
    /// the `Nats-Stream-Source` header. Copies fan out to the
    /// dependent's consumers, but never produce publish acks.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn copy_to_dependents(
        &mut self,
        origin_name: &str,
        account: &str,
        subject: &str,
        headers: &Option<Bytes>,
        payload: &Bytes,
        time_ns: u64,
        origin_seq: u64,
        actions: &mut Vec<Action>,
        visited: &mut std::collections::HashSet<String>,
    ) {
        if !visited.insert(origin_name.to_string()) {
            return;
        }
        let origin_token = self
            .streams
            .get(origin_name)
            .map(|s| s.source_token.clone())
            .unwrap_or_default();
        // Plan first (immutable scan), then mutate: dependents in
        // name order for deterministic action order.
        let mut plan: Vec<(String, bool, SourceCfg)> = Vec::new();
        let mut dep_names: Vec<String> = self
            .streams
            .values()
            .filter(|s| s.account == account)
            .map(|s| s.name.clone())
            .collect();
        dep_names.sort();
        for dep in dep_names {
            let Some(s) = self.streams.get(&dep) else {
                continue;
            };
            // Phase 32: remote links are served by the mirror loop,
            // never by the local capture hook (a same-named local
            // stream must not also feed a remote link).
            if let Some(cfg) = &s.mirror {
                if cfg.live && !cfg.remote && cfg.name == origin_name {
                    plan.push((dep.clone(), true, cfg.clone()));
                    continue;
                }
            }
            for cfg in &s.sources {
                if cfg.live && !cfg.remote && cfg.name == origin_name {
                    plan.push((dep.clone(), false, cfg.clone()));
                }
            }
        }
        for (dep, is_mirror, cfg) in plan {
            if let Some(filter) = &cfg.filter_subject {
                if !subject_matches(filter, subject) {
                    continue;
                }
            }
            let headers2 = if is_mirror {
                headers.clone()
            } else {
                Some(append_source_header(
                    headers.clone(),
                    origin_name,
                    origin_seq,
                    cfg.filter_subject.as_deref(),
                    subject,
                    &origin_token,
                ))
            };
            let dep_seq = {
                let stream = self.stream_for_mut(&dep, account).unwrap();
                Self::store_message(stream, subject, headers2.clone(), payload.clone(), time_ns)
            };
            self.fan_out(&dep, account, actions);
            self.copy_to_dependents(
                &dep, account, subject, &headers2, payload, time_ns, dep_seq, actions, visited,
            );
        }
    }

    /// Backfill one mirror/source link at stream creation: copy the
    /// origin's existing messages (from `opt_start_seq` /
    /// `opt_start_time` when given, honouring `filter_subject`) into
    /// the target, in origin order. No consumer fan-out: the target
    /// is brand new, so no live link to it can exist yet.
    pub(crate) fn backfill(
        &mut self,
        target: &str,
        account: &str,
        cfg: &SourceCfg,
        is_mirror: bool,
    ) {
        let Some(origin) = self.stream_for(cfg.name.as_str(), account) else {
            return;
        };
        let token = origin.source_token.clone();
        let copied: Vec<StoredMsg> = origin
            .messages
            .iter()
            .filter(|m| !origin.deleted.contains(&m.seq))
            .filter(|m| cfg.start_seq == 0 || m.seq >= cfg.start_seq)
            .filter(|m| cfg.start_time_ns == 0 || m.time_ns >= cfg.start_time_ns)
            .filter(|m| match &cfg.filter_subject {
                Some(f) => subject_matches(f, &m.subject),
                None => true,
            })
            .map(|m| StoredMsg {
                seq: m.seq,
                subject: m.subject.clone(),
                headers: m.headers.clone(),
                payload: m.payload.clone(),
                time_ns: m.time_ns,
            })
            .collect();
        for m in copied {
            let headers = if is_mirror {
                m.headers.clone()
            } else {
                Some(append_source_header(
                    m.headers.clone(),
                    &cfg.name,
                    m.seq,
                    cfg.filter_subject.as_deref(),
                    &m.subject,
                    &token,
                ))
            };
            let Some(stream) = self.stream_for_mut(target, account) else {
                return;
            };
            Self::store_message(stream, &m.subject, headers, m.payload, m.time_ns);
        }
    }

    /// Phase 32: store one message pulled from a remote origin into a
    /// remote mirror/source link's target stream. Returns the new
    /// target sequence, or None when the message is a duplicate of
    /// one already stored for this link (the pull loop tracks origin
    /// sequences per link and skips re-deliveries, but this is the
    /// second line of defence: `last_origin_seq` is kept per link
    /// on the target stream). Mirror copies are exact; source copies
    /// add the `Nats-Stream-Source` header with the target's own
    /// token (the origin's token is not visible across the cluster;
    /// the header shape is otherwise Go's). Timestamps are the
    /// mirror-arrival time: a pull delivery carries no timestamp,
    /// unlike the same-node capture hook which preserves the origin
    /// timestamp. Fan-out actions are returned for the caller to
    /// dispatch after releasing the engine lock, as usual.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn append_remote_copy(
        &mut self,
        target: &str,
        account: &str,
        origin_name: &str,
        origin_seq: u64,
        is_mirror: bool,
        filter_subject: Option<&str>,
        subject: &str,
        headers: &Option<Bytes>,
        payload: &Bytes,
        actions: &mut Vec<Action>,
    ) -> Option<u64> {
        let (token, last) = {
            let stream = self.stream_for(target, account)?;
            let link = if is_mirror {
                stream.mirror.as_ref()
            } else {
                stream.sources.iter().find(|c| c.name == origin_name)
            }?;
            if !link.live || !link.remote {
                return None;
            }
            (
                stream.source_token.clone(),
                stream
                    .remote_progress
                    .get(origin_name)
                    .copied()
                    .unwrap_or(0),
            )
        };
        if origin_seq <= last {
            return None;
        }
        let headers2 = if is_mirror {
            headers.clone()
        } else {
            Some(append_source_header(
                headers.clone(),
                origin_name,
                origin_seq,
                filter_subject,
                subject,
                &token,
            ))
        };
        let time_ns = now_ns();
        let seq = {
            let stream = self.stream_for_mut(target, account).unwrap();
            Self::store_message(stream, subject, headers2, payload.clone(), time_ns)
        };
        {
            let stream = self.stream_for_mut(target, account).unwrap();
            stream
                .remote_progress
                .insert(origin_name.to_string(), origin_seq);
        }
        self.fan_out(target, account, actions);
        Some(seq)
    }

    /// Would adding `origins` to a new stream named `new_name`
    /// close a cycle in the config graph? Follows mirror/sources
    /// configs transitively by name (Go rejects the cycle even when
    /// the intermediate stream was deleted, because its config still
    /// names the stream — so dead links count here).
    pub(crate) fn would_cycle(&self, new_name: &str, origins: &[String]) -> bool {
        let mut stack: Vec<String> = origins.to_vec();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        while let Some(name) = stack.pop() {
            if name == new_name {
                return true;
            }
            if !seen.insert(name.clone()) {
                continue;
            }
            if let Some(s) = self.streams.get(&name) {
                if let Some(cfg) = &s.mirror {
                    stack.push(cfg.name.clone());
                }
                for cfg in &s.sources {
                    stack.push(cfg.name.clone());
                }
            }
        }
        false
    }

    pub(crate) fn capture(&mut self, msg: &Message, actions: &mut Vec<Action>) {
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
        // One timestamp for the whole publish, shared by the origin
        // stores and their mirror/source copies (Go preserves the
        // origin timestamp in copies).
        let time_ns = now_ns();
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
            // Phase 31 rollup (Go v2.15.1, probed): a publish
            // carrying `Nats-Rollup` replaces history before the new
            // message is stored — `sub` deletes all prior live
            // messages for this subject, `all` deletes the whole
            // stream's live messages — and the new message is then
            // stored at the next sequence (no renumbering, so
            // earlier sequences become gaps). A stream without
            // allow_rollup_hdrs, or a value other than sub/all,
            // fails the publish: 500/10111 "rollup not permitted"
            // / `rollup value invalid: "<value>"`. Rollup runs
            // only for a message that is actually stored (after
            // dedup and expected-sequence checks), and only on
            // direct publishes — mirror/source copies use
            // store_message directly and never re-apply it.
            if let Some(rollup) = header_get(msg, "Nats-Rollup") {
                let stream = self.stream_for_mut(name, &msg.account).unwrap();
                match rollup.as_str() {
                    "sub" | "all" if stream.allow_rollup_hdrs => {
                        let is_all = rollup == "all";
                        let subject = msg.subject.clone();
                        for m in stream.messages.iter() {
                            if !stream.deleted.contains(&m.seq) && (is_all || m.subject == subject)
                            {
                                stream.deleted.insert(m.seq);
                            }
                        }
                    }
                    "sub" | "all" => {
                        if first_error.is_none() {
                            first_error = Some((500, 10111, "rollup not permitted".to_string()));
                        }
                        continue;
                    }
                    other => {
                        if first_error.is_none() {
                            first_error =
                                Some((500, 10111, format!("rollup value invalid: \"{other}\"")));
                        }
                        continue;
                    }
                }
            }
            let seq = {
                let stream = self.stream_for_mut(name, &msg.account).unwrap();
                Self::store_message(
                    stream,
                    &msg.subject,
                    msg.headers.clone(),
                    msg.payload.clone(),
                    time_ns,
                )
            };
            let stream = self.stream_for_mut(name, &msg.account).unwrap();
            if let Some(id) = header_get(msg, "Nats-Msg-Id") {
                stream.msg_ids.insert(id, (seq, Instant::now()));
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
                actions.push(Action::Deliver(
                    Message::new(reply, None, None, Bytes::from(body.to_string()))
                        .with_account(&msg.account),
                ));
            } else if let Some((code, err_code, desc)) = first_error {
                let body = json!({
                    "error": { "code": code, "err_code": err_code, "description": desc },
                });
                actions.push(Action::Deliver(
                    Message::new(reply, None, None, Bytes::from(body.to_string()))
                        .with_account(&msg.account),
                ));
            }
        }
        // Fan out to push consumers and waiting pulls of every stream
        // that stored (or deduplicated) this message.
        for (name, _, _) in &stored {
            self.fan_out(name, &msg.account, actions);
        }
        // Live-copy every freshly stored (non-duplicate) message into
        // its mirrors/sources. Duplicates are not re-copied: the
        // origin did not store a new message, and the mirror already
        // holds the first copy.
        for (name, seq, duplicate) in &stored {
            if *duplicate {
                continue;
            }
            let mut visited = std::collections::HashSet::new();
            self.copy_to_dependents(
                name,
                &msg.account,
                &msg.subject,
                &msg.headers,
                &msg.payload,
                time_ns,
                *seq,
                actions,
                &mut visited,
            );
        }
    }

    pub(crate) fn process_ack(&mut self, rest: &str, msg: &Message, actions: &mut Vec<Action>) {
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
                // Immediate redelivery, subject to max_deliver.
                // Exhaustion (deliveries already made >= cap) drops
                // the message from ack_pending with no re-queue and
                // publishes the MAX_DELIVERIES advisory; the ack
                // floor was already advanced above. The message
                // itself is NOT deleted, whatever the retention
                // (Go workqueue behaviour, probed): only a plain
                // +ACK deletes a workqueue message.
                if consumer.deliver_subject.is_some() {
                    if consumer.max_deliver > 0 && pending.delivered >= consumer.max_deliver as u64
                    {
                        actions.push(max_deliver_advisory_action(
                            stream_name,
                            &msg.account,
                            consumer_name,
                            sseq,
                            pending.delivered,
                        ));
                    } else if let Some(m) = staged {
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
                            actions.push(Action::Deliver(
                                Message::new(
                                    &m.subject,
                                    Some(reply),
                                    m.headers.clone(),
                                    m.payload.clone(),
                                )
                                .with_account(&msg.account)
                                .with_route_via(&to),
                            ));
                            consumer.pending.insert(
                                ncseq,
                                Pending {
                                    stream_seq: sseq,
                                    delivered,
                                    deadline: Instant::now() + consumer.ack_wait,
                                    delivered_at_ns: now_ns(),
                                },
                            );
                            consumer.num_redelivered += 1;
                        }
                    }
                } else if consumer.max_deliver > 0
                    && pending.delivered >= consumer.max_deliver as u64
                {
                    // Exhausted: drop, do not re-queue, advise.
                    // num_redelivered is not incremented (Go reports
                    // only actual redeliveries, probed).
                    actions.push(max_deliver_advisory_action(
                        stream_name,
                        &msg.account,
                        consumer_name,
                        sseq,
                        pending.delivered,
                    ));
                } else {
                    consumer.redeliver.push_back(Redeliver {
                        stream_seq: sseq,
                        delivered: pending.delivered + 1,
                    });
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
                        delivered_at_ns: now_ns(),
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
        // Workqueue retention: a plain ack deletes the message (after
        // its first ack). NAK/TERM/WPI do not delete. Note: ack policy
        // All is not bulk-aware here — only the acked message itself
        // is deleted; other messages covered by an All ack are deleted
        // when they are individually acked.
        let plain_ack = !matches!(payload.as_ref(), "-NAK" | "+NAK" | "+TERM" | "+WPI");
        if plain_ack && stream.retention == "workqueue" {
            stream.deleted.insert(sseq);
        }
    }
}
