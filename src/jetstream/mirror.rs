//! Phase 32 cross-node mirror/source loop.
//!
//! A remote mirror/source link (a stream whose `mirror`/`sources`
//! config names a stream that is not on this node) is served by a
//! pull loop, not by the same-node capture hook:
//!
//! * The loop runs inside the server process and talks to its own
//!   node over a loopback client connection (raw TCP, no client
//!   library dependency — the server crate cannot depend on
//!   async-nats, which is a dev-dependency only). It authenticates
//!   with the first password user of the mirror stream's account
//!   when the config has one, else anonymously; a JWT-only or
//!   TLS-required client port cannot be used by the loopback client
//!   and is a documented remainder.
//! * It creates one durable push-less consumer on the origin stream
//!   via `$JS.API.CONSUMER.CREATE.<origin>` (forwarded cluster-wide
//!   by `jetstream::handle_message` when the origin is not local),
//!   with `deliver_policy` replaying the link's backfill range
//!   (`all`, `by_start_sequence`, or `by_start_time`) and the link's
//!   `filter_subject`, then repeatedly issues
//!   `$JS.API.CONSUMER.MSG.NEXT` (`no_wait`, batch 32) and stores
//!   each delivered message into the mirror stream via
//!   `Engine::append_remote_copy` (mirror copies exact, source
//!   copies gain the `Nats-Stream-Source` header). The consumer is
//!   `ack_policy: none`: with no acks there is no ack-routing to
//!   lose, and the consumer's `next_stream_seq` on the origin node
//!   is the durable progress cursor — recreating the loop after a
//!   mirror-node restart reuses the existing consumer (create
//!   returns 10013) and resumes where it left off, with no
//!   duplicates. `append_remote_copy`'s per-link `remote_progress`
//!   (persisted in stream meta) additionally drops any
//!   re-delivery within or across restarts.
//! * Honest limits (see PLAN Phase 32): if the origin node restarts
//!   without a store dir, its consumer is lost and the loop
//!   recreates it from the link's start range, so already-copied
//!   messages can be duplicated into the mirror (the persisted
//!   `remote_progress` bounds this: only messages after the last
//!   stored origin sequence are stored, but a recreated consumer
//!   starting at `all` re-delivers from sequence 1 and the progress
//!   check drops everything up to the stored watermark — so in
//!   fact duplicates are dropped too, at the cost of re-pulling the
//!   whole backfill range. The remaining loss window is a mirror
//!   node crashing between receiving a pull message and storing it:
//!   with ack-none the origin already advanced past it, so that one
//!   message is lost. Timestamps on cross-node copies are the
//!   mirror-arrival time, not the origin timestamp (a pull delivery
//!   carries no timestamp, unlike the same-node capture hook).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use super::{now_ns, Action, JetStream, SourceCfg};
use crate::server::{deliver, Origin, Shared};

/// One frame read from the loopback client connection.
enum Frame {
    Msg {
        subject: String,
        reply: Option<String>,
        headers: Option<Bytes>,
        payload: Bytes,
    },
    /// A `Status: 404` header frame (MSG.NEXT with no messages).
    NoMessages,
    /// A server PONG (returned, not skipped, so a PING/PONG
    /// handshake wait ends as soon as the PONG arrives instead of
    /// running out the read timeout).
    Pong,
}

/// Minimal raw NATS client over one TCP connection to this node.
struct LoopClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
    buf: Vec<u8>,
    next_sid: u64,
}

impl LoopClient {
    async fn connect(
        addr: std::net::SocketAddr,
        account: &str,
        shared: &Arc<Shared>,
    ) -> Option<Self> {
        let stream = TcpStream::connect(addr).await.ok()?;
        let _ = stream.set_nodelay(true);
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        // Read INFO.
        let mut line = String::new();
        if reader.read_line(&mut line).await.is_err() || !line.starts_with("INFO ") {
            return None;
        }
        // Credentials: first password user of this account, else the
        // server's global user/pass, else anonymous.
        let mut user: Option<String> = None;
        let mut pass: Option<String> = None;
        if let Some(acct) = shared.config.account(account) {
            for u in &acct.users {
                if let Some(p) = &u.pass {
                    user = Some(u.name.clone());
                    pass = Some(p.clone());
                    break;
                }
            }
        }
        if user.is_none() {
            user = shared.config.user.clone();
            pass = shared.config.pass.clone();
        }
        let connect = match (&user, &pass) {
            (Some(u), Some(p)) => json!({"user": u, "pass": p, "verbose": false, "headers": true}),
            _ => json!({"verbose": false, "headers": true}),
        };
        if writer
            .write_all(format!("CONNECT {connect}\r\n").as_bytes())
            .await
            .is_err()
        {
            return None;
        }
        Some(LoopClient {
            reader,
            writer,
            buf: Vec::new(),
            next_sid: 2,
        })
    }

    async fn send(&mut self, data: &str) -> bool {
        self.writer.write_all(data.as_bytes()).await.is_ok()
    }

    async fn read_line_inner(&mut self) -> Option<String> {
        loop {
            if let Some(pos) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let line = self.buf.drain(..pos + 2).collect::<Vec<_>>();
                return Some(String::from_utf8_lossy(&line[..line.len() - 2]).into_owned());
            }
            let mut chunk = [0u8; 8192];
            match self.reader.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
            }
        }
    }

    async fn read_exact_n(&mut self, n: usize) -> Option<Vec<u8>> {
        while self.buf.len() < n {
            let mut chunk = [0u8; 8192];
            match self.reader.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(m) => self.buf.extend_from_slice(&chunk[..m]),
            }
        }
        Some(self.buf.drain(..n).collect())
    }

    /// Read one message frame, answering server PINGs and skipping
    /// +OK/-ERR control lines. Returns None on timeout/EOF.
    async fn read_frame(&mut self, timeout_ms: u64) -> Option<Frame> {
        let fut = async {
            loop {
                let line = self.read_line_inner().await?;
                let parts: Vec<&str> = line.split(' ').collect();
                match parts.first().copied().unwrap_or("") {
                    "PING" => {
                        let _ = self.send("PONG\r\n").await;
                    }
                    "PONG" => {
                        return Some(Frame::Pong);
                    }
                    "+OK" => {}
                    "-ERR" => {}
                    "MSG" => {
                        let (subject, reply, size) = if parts.len() == 4 {
                            (parts[1].to_string(), None, parts[3].parse().ok()?)
                        } else if parts.len() == 5 {
                            (
                                parts[1].to_string(),
                                Some(parts[3].to_string()),
                                parts[4].parse().ok()?,
                            )
                        } else {
                            return None;
                        };
                        let data = self.read_exact_n(size + 2).await?;
                        return Some(Frame::Msg {
                            subject,
                            reply,
                            headers: None,
                            payload: Bytes::from(data[..size].to_vec()),
                        });
                    }
                    "HMSG" => {
                        let (subject, reply, hdr_len, total_len) = if parts.len() == 5 {
                            (
                                parts[1].to_string(),
                                None,
                                parts[3].parse().ok()?,
                                parts[4].parse().ok()?,
                            )
                        } else if parts.len() == 6 {
                            (
                                parts[1].to_string(),
                                Some(parts[3].to_string()),
                                parts[4].parse().ok()?,
                                parts[5].parse().ok()?,
                            )
                        } else {
                            return None;
                        };
                        let data = self.read_exact_n(total_len + 2).await?;
                        let headers = Bytes::from(data[..hdr_len].to_vec());
                        let text = String::from_utf8_lossy(&headers);
                        if text.contains("Status: 404") {
                            return Some(Frame::NoMessages);
                        }
                        return Some(Frame::Msg {
                            subject,
                            reply,
                            headers: Some(headers),
                            payload: Bytes::from(data[hdr_len..total_len].to_vec()),
                        });
                    }
                    _ => {}
                }
            }
        };
        tokio::time::timeout(Duration::from_millis(timeout_ms), fut)
            .await
            .ok()
            .flatten()
    }
}

type LinkKey = (String, String, String, bool); // (account, target, origin, is_mirror)

struct LinkState {
    client: Option<LoopClient>,
    inbox: String,
    consumer_ready: bool,
    /// Origin subjects subscribed (so pull deliveries, framed with
    /// the original subject, match on this node). Set after the
    /// STREAM.INFO + SUB step; pulls start only after it, because a
    /// pull delivery made before the origin node learns the interest
    /// would advance the consumer past a message nobody received.
    subjects_subscribed: bool,
    last_origin_seq: u64,
    next_try: Instant,
    unsupported: bool,
}

fn consumer_name(target: &str, origin: &str) -> String {
    format!("mirror-{target}-{origin}")
}

/// Scan the engine for remote live links: (key, account, target, cfg, is_mirror).
fn remote_links(js: &JetStream) -> Vec<(LinkKey, SourceCfg, bool)> {
    let engine = js.engine.lock().unwrap();
    let mut out = Vec::new();
    for s in engine.streams.values() {
        if let Some(cfg) = &s.mirror {
            if cfg.live && cfg.remote {
                out.push((
                    (s.account.clone(), s.name.clone(), cfg.name.clone(), true),
                    cfg.clone(),
                    true,
                ));
            }
        }
        for cfg in &s.sources {
            if cfg.live && cfg.remote {
                out.push((
                    (s.account.clone(), s.name.clone(), cfg.name.clone(), false),
                    cfg.clone(),
                    false,
                ));
            }
        }
    }
    out
}

/// Background task: serve every remote mirror/source link on this node.
pub(crate) async fn mirror_loop(js: Arc<JetStream>, shared: Arc<Shared>) {
    let mut states: HashMap<LinkKey, LinkState> = HashMap::new();
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            _ = shared.shutdown.notified() => break,
        }
        for (key, cfg, is_mirror) in remote_links(&js) {
            let (account, target, origin, _) = key.clone();
            let state = states.entry(key.clone()).or_insert_with(|| LinkState {
                client: None,
                inbox: format!(
                    "_MIRROR.{target}.{origin}.{:x}",
                    now_ns() ^ (std::process::id() as u64) << 32
                ),
                consumer_ready: false,
                subjects_subscribed: false,
                last_origin_seq: 0,
                next_try: Instant::now(),
                unsupported: false,
            });
            if state.unsupported || Instant::now() < state.next_try {
                continue;
            }
            // (Re)connect the loopback client when needed. A TLS
            // client port or a JWT-only account cannot be used here:
            // mark unsupported once instead of retrying forever.
            if state.client.is_none() {
                if shared.config.tls.is_some() {
                    eprintln!(
                        "rust-natsd: cross-node mirror {target} <- {origin}: loopback client cannot use a TLS client port (remainder)"
                    );
                    state.unsupported = true;
                    continue;
                }
                match LoopClient::connect(shared.listen_addr, &account, &shared).await {
                    Some(mut client) => {
                        if !client.send(&format!("SUB {} 1\r\n", state.inbox)).await {
                            continue;
                        }
                        // PING/PONG confirms the subscription is
                        // registered locally; route interest for the
                        // inbox still needs a moment to reach the
                        // origin node, so API responses to the first
                        // requests may be lost and are retried.
                        if !client.send("PING\r\n").await {
                            continue;
                        }
                        // Wait for the PONG (read_frame returns it
                        // immediately now); the inbox RS+/LS+ interest
                        // still needs a moment to reach the origin node.
                        while let Some(f) = client.read_frame(2000).await {
                            if matches!(f, Frame::Pong) {
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        state.subjects_subscribed = false;
                        state.client = Some(client);
                    }
                    None => {
                        state.next_try = Instant::now() + Duration::from_secs(1);
                        continue;
                    }
                }
            };
            let Some(client) = state.client.as_mut() else {
                continue;
            };
            if !state.consumer_ready {
                let (deliver_policy, start_seq, start_time) = if cfg.start_seq > 0 {
                    ("by_start_sequence", cfg.start_seq, 0u64)
                } else if cfg.start_time_ns > 0 {
                    ("by_start_time", 0u64, cfg.start_time_ns)
                } else {
                    ("all", 0u64, 0u64)
                };
                let body = json!({
                    "config": {
                        "name": consumer_name(&target, &origin),
                        "deliver_policy": deliver_policy,
                        "opt_start_seq": start_seq,
                        "opt_start_time": start_time,
                        "filter_subject": cfg.filter_subject,
                        "ack_policy": "none",
                        "max_deliver": -1,
                    }
                });
                if !client
                    .send(&format!(
                        "PUB $JS.API.CONSUMER.CREATE.{origin} {} {}\r\n{}\r\n",
                        state.inbox,
                        body.to_string().len(),
                        body
                    ))
                    .await
                {
                    state.client = None;
                    continue;
                }
                match client.read_frame(3000).await {
                    Some(Frame::Msg { payload, .. }) => {
                        let v: Value = serde_json::from_slice(&payload).unwrap_or(Value::Null);
                        if let Some(err) = v.get("error") {
                            let code = err.get("err_code").and_then(|c| c.as_u64()).unwrap_or(0);
                            if code == 10013 {
                                // Consumer already exists (mirror
                                // restart, or a previous create that
                                // succeeded but whose response was
                                // lost): reuse it, resume pulling.
                                state.consumer_ready = true;
                            } else {
                                // 10059 (origin stream not found on
                                // any reachable node) or anything
                                // else: retry — the origin may not
                                // exist yet, or the route may still
                                // be forming.
                                state.next_try = Instant::now() + Duration::from_secs(2);
                            }
                        } else {
                            state.consumer_ready = true;
                        }
                    }
                    _ => {
                        state.next_try = Instant::now() + Duration::from_secs(1);
                    }
                }
                continue;
            }
            // Subscribe to the origin stream's subjects (or the
            // link's filter) before the first pull, so pull
            // deliveries — framed with the original subject by the
            // route_via path — match a subscription on this node.
            // Subjects come from a forwarded STREAM.INFO on the
            // origin node; a filtered link needs only its filter.
            if !state.subjects_subscribed {
                let subjects: Vec<String> = if let Some(f) = &cfg.filter_subject {
                    vec![f.clone()]
                } else {
                    let body = json!({});
                    if !client
                        .send(&format!(
                            "PUB $JS.API.STREAM.INFO.{origin} {} {}\r\n{}\r\n",
                            state.inbox,
                            body.to_string().len(),
                            body
                        ))
                        .await
                    {
                        state.client = None;
                        continue;
                    }
                    match client.read_frame(3000).await {
                        Some(Frame::Msg { payload, .. }) => {
                            let v: Value = serde_json::from_slice(&payload).unwrap_or(Value::Null);
                            v.get("config")
                                .and_then(|c| c.get("subjects"))
                                .and_then(|s| s.as_array())
                                .map(|a| {
                                    a.iter()
                                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                                        .collect()
                                })
                                .unwrap_or_default()
                        }
                        _ => {
                            state.next_try = Instant::now() + Duration::from_secs(1);
                            continue;
                        }
                    }
                };
                let subjects = if subjects.is_empty() {
                    vec![">".to_string()]
                } else {
                    subjects
                };
                let mut ok = true;
                for subj in &subjects {
                    let sid = client.next_sid;
                    client.next_sid += 1;
                    if !client.send(&format!("SUB {subj} {sid}\r\n")).await {
                        ok = false;
                        break;
                    }
                }
                if !ok {
                    state.client = None;
                    continue;
                }
                // PING/PONG confirms local registration; the
                // RS+/LS+ interest still needs a moment to reach the
                // origin node, and a pull before it arrives would
                // lose messages (see the field comment).
                if !client.send("PING\r\n").await {
                    state.client = None;
                    continue;
                }
                while let Some(f) = client.read_frame(2000).await {
                    if matches!(f, Frame::Pong) {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                state.subjects_subscribed = true;
                continue;
            }
            // Pull one batch.
            let body = json!({ "batch": 32, "no_wait": true });
            if !client
                .send(&format!(
                    "PUB $JS.API.CONSUMER.MSG.NEXT.{origin}.{} {} {}\r\n{}\r\n",
                    consumer_name(&target, &origin),
                    state.inbox,
                    body.to_string().len(),
                    body
                ))
                .await
            {
                state.client = None;
                state.consumer_ready = false;
                continue;
            }
            let mut stored_any = false;
            for _ in 0..32 {
                match client.read_frame(500).await {
                    Some(Frame::Msg {
                        reply,
                        headers,
                        payload,
                        subject,
                    }) => {
                        // An API error frame on the inbox (no ack
                        // reply, JSON error body — e.g. the consumer
                        // was lost when the origin restarted without
                        // persistence) means the consumer is gone:
                        // drop back to consumer creation next tick
                        // instead of pulling against a dead consumer
                        // forever.
                        if reply.is_none()
                            && payload.starts_with(b"{")
                            && serde_json::from_slice::<Value>(&payload)
                                .ok()
                                .and_then(|v| v.get("error").cloned())
                                .is_some()
                        {
                            state.consumer_ready = false;
                            break;
                        }
                        // Origin sequence from the ack reply subject:
                        // $JS.ACK.<origin>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>
                        let sseq = reply
                            .as_deref()
                            .and_then(|r| r.split('.').nth(5))
                            .and_then(|s| s.parse::<u64>().ok());
                        let Some(sseq) = sseq else {
                            continue;
                        };
                        if sseq <= state.last_origin_seq {
                            continue;
                        }
                        let mut actions: Vec<Action> = Vec::new();
                        let stored = {
                            let mut engine = js.engine.lock().unwrap();
                            engine.append_remote_copy(
                                &target,
                                &account,
                                &origin,
                                sseq,
                                is_mirror,
                                cfg.filter_subject.as_deref(),
                                &subject,
                                &headers,
                                &payload,
                                &mut actions,
                            )
                        };
                        if stored.is_some() {
                            state.last_origin_seq = sseq;
                            stored_any = true;
                            for action in actions {
                                let Action::Deliver(m) = action;
                                deliver(&shared, &m, Origin::Internal, None);
                            }
                        }
                    }
                    _ => break, // NoMessages status, timeout, or EOF: next tick.
                }
            }
            if stored_any {
                js.persist();
            }
        }
        // Drop state for links that no longer exist (stream deleted).
        let live: std::collections::HashSet<LinkKey> =
            remote_links(&js).into_iter().map(|(k, _, _)| k).collect();
        states.retain(|k, _| live.contains(k));
    }
}
