//! Client connections: the writer task and slow-consumer byte queue,
//! `ConnHandle`/`ConnInfo`/`Stats`/route/leaf connection types, the
//! per-connection state machine (`handle_conn`, `process_op`), and the
//! shared helpers it uses (`client_info`, JSON field readers).

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};

use crate::leaf::leaf_info_json;
use crate::protocol::{
    encode_err, encode_info, encode_ok, encode_ping, encode_pong, ClientOp, Parser, ServerInfo,
};
use crate::route::route_info_json;
use crate::sublist::Subscription;

use super::delivery::{deliver, json_str, local_pattern_in_use, propagate_local_sub};
use super::imports::lookup_import_reply;
use super::{Message, Origin, Shared};

/// Bytes the writer coalesces into a single socket write. The old code
/// did one `write_all` syscall per frame, so the writer drained at
/// ~100-200k small frames/s while a burst publisher enqueued far faster,
/// and the 1024-frame queue filled after only ~100 deliveries to a fast
/// reader. Batching is what lets the writer keep up with a burst (Go
/// batches through its outbound buffer the same way).
const WRITE_BATCH_BYTES: usize = 64 * 1024;

/// The channel itself is effectively unbounded (capacity 2^30 frames):
/// the real bound is the per-connection *byte* limit enforced in
/// `try_send` (`Config::max_pending`, default 64 MiB, Go's `max_pending`
/// default). A frame-count cap would bind first for small frames — a
/// 1024-frame cap is only ~280 KiB at 256 B payloads — which is exactly
/// how fast readers were dropped as "slow consumers" under bursts.
const OUTBOUND_CHANNEL_CAPACITY: usize = 1 << 30;

/// Outbound channel + close signalling, shared by client, route and leaf
/// connections. The writer task holds no sender, so dropping all senders
/// flushes pending frames (e.g. a final -ERR) before the socket closes.
pub(crate) struct ConnHandle {
    tx: mpsc::Sender<Bytes>,
    pub(crate) headers: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    pub(crate) close: Arc<Notify>,
    pub(crate) in_msgs: AtomicU64,
    pub(crate) in_bytes: AtomicU64,
    pub(crate) out_msgs: AtomicU64,
    pub(crate) out_bytes: AtomicU64,
    /// PINGs sent by the server ping task without a PONG reply.
    pub(crate) ping_missed: AtomicU64,
    /// Queued outbound bytes, reserved in `try_send` and released by
    /// the writer after a successful write. Kept as a separate Arc so
    /// the writer task can own it without owning the handle (which
    /// holds a channel sender; the writer holding one would keep the
    /// channel open forever). Control frames sent directly on `tx`
    /// (a connection's own +OK/PONG/-ERR) are not reserved, so this
    /// slightly undercounts; they are a handful of bytes per round
    /// trip and the decrement below saturates rather than underflowing.
    pub(crate) pending_bytes: Arc<AtomicUsize>,
    /// Byte limit for `pending_bytes`; see `Config::max_pending`.
    pub(crate) max_pending: usize,
}

impl ConnHandle {
    pub(crate) fn new(tx: mpsc::Sender<Bytes>, max_pending: usize) -> Arc<Self> {
        Arc::new(ConnHandle {
            tx,
            headers: Arc::new(AtomicBool::new(false)),
            closed: Arc::new(AtomicBool::new(false)),
            close: Arc::new(Notify::new()),
            in_msgs: AtomicU64::new(0),
            in_bytes: AtomicU64::new(0),
            out_msgs: AtomicU64::new(0),
            out_bytes: AtomicU64::new(0),
            ping_missed: AtomicU64::new(0),
            pending_bytes: Arc::new(AtomicUsize::new(0)),
            max_pending,
        })
    }
    pub(crate) fn mark_closed(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.close.notify_waiters();
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

/// A route peer connection.
pub(crate) struct RouteConn {
    pub(crate) peer_id: Mutex<Option<String>>,
    /// The peer's cluster listen address (host:port from its INFO), used to
    /// avoid dialing a peer we already have a connection to.
    pub(crate) peer_listen_addr: Mutex<Option<String>>,
    /// Server id of the connection's initiator (for duplicate resolution:
    /// the connection initiated by the smaller server id wins).
    pub(crate) initiator: String,
    pub(crate) handle: Arc<ConnHandle>,
}

/// A leaf connection (either side of it: hub-side accepted, or leaf-side
/// solicited — the protocol is symmetric after the handshake).
/// A leaf connection is account-scoped: it only carries messages for
/// this account (default `$G`; hub-side, the account of the leaf user
/// when that user belongs to a configured account).
pub(crate) struct LeafConn {
    pub(crate) handle: Arc<ConnHandle>,
    pub(crate) account: String,
}

/// A gateway connection (either side: accepter-side inbound, or
/// solicited outbound — one connection per direction, not one shared
/// socket, as Go's supercluster forms them). Unlike leafs, a gateway
/// connection is multi-account: every RS+/RMSG frame carries its
/// account token, and accounts are matched by name across gateways.
/// `configured` mirrors Go's `/gatewayz`: true only for an explicitly
/// configured solicitation; inbound connections and implicit
/// solicitations report false.
pub(crate) struct GatewayConn {
    pub(crate) handle: Arc<ConnHandle>,
    /// The peer gateway's name (from its CONNECT/INFO `gateway`).
    pub(crate) peer_gateway: String,
    /// The peer server's id (from its INFO `server_id`).
    pub(crate) peer_id: Mutex<Option<String>>,
    /// True for solicited (outbound) connections, false for inbound.
    pub(crate) outbound: bool,
    pub(crate) configured: bool,
    /// Accounts the peer has announced via cmd 2 (informational;
    /// forwarding is interest-driven regardless).
    pub(crate) announced_accounts: Mutex<Vec<String>>,
    /// Accounts the peer has marked interest-only via cmd 3
    /// (informational; see gateway.rs on the inference).
    pub(crate) interest_only_accounts: Mutex<Vec<String>>,
}

/// Connection metadata for the monitoring endpoint.
#[derive(Debug, Clone)]
pub struct ConnInfo {
    pub id: u64,
    pub kind: &'static str,
    pub addr: String,
    pub peer_id: Option<String>,
    pub started: Instant,
    pub account: String,
    pub user: Option<String>,
    pub tls: bool,
}

impl ConnInfo {
    pub(crate) fn client(id: u64, addr: String, tls: bool) -> Self {
        ConnInfo {
            id,
            kind: "client",
            addr,
            peer_id: None,
            started: Instant::now(),
            account: "$G".to_string(),
            user: None,
            tls,
        }
    }
}

/// Server-wide counters for the monitoring endpoint.
#[derive(Default)]
pub struct Stats {
    pub in_msgs: AtomicU64,
    pub out_msgs: AtomicU64,
    pub in_bytes: AtomicU64,
    pub out_bytes: AtomicU64,
    pub total_connections: AtomicU64,
    /// Connections dropped as slow consumers (queue-full on delivery).
    pub slow_consumers: AtomicU64,
}

/// Create a connection handle and spawn its writer task. Returns
/// (handle, sender). The writer holds no sender itself, so dropping all
/// senders flushes pending frames before the socket closes.
/// Generic over the write half so plain TCP and TLS streams share it.
pub(crate) fn spawn_writer<W>(
    mut writer: W,
    shared: Arc<Shared>,
) -> (Arc<ConnHandle>, mpsc::Sender<Bytes>)
where
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Bytes>(OUTBOUND_CHANNEL_CAPACITY);
    let handle = ConnHandle::new(tx.clone(), shared.config.max_pending);
    let closed = handle.closed.clone();
    let close = handle.close.clone();
    let pending = handle.pending_bytes.clone();
    let shutdown = shared.shutdown.clone();
    tokio::spawn(async move {
        let shutdown = shutdown.notified();
        tokio::pin!(shutdown);
        loop {
            if closed.load(Ordering::SeqCst) {
                break;
            }
            tokio::select! {
                msg = rx.recv() => match msg {
                    Some(first) => {
                        if write_batch(&mut writer, &mut rx, &pending, Some(first))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    None => break,
                },
                // On close/shutdown, drain any queued frames first so a
                // final -ERR (e.g. 'Stale Connection') is not lost.
                _ = close.notified() => {
                    while let Ok(first) = rx.try_recv() {
                        if write_batch(&mut writer, &mut rx, &pending, Some(first))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    break;
                }
                _ = &mut shutdown => {
                    while let Ok(first) = rx.try_recv() {
                        if write_batch(&mut writer, &mut rx, &pending, Some(first))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    break;
                }
            }
        }
        let _ = writer.shutdown().await;
    });
    (handle, tx)
}

/// Write `first` plus whatever else is already queued, coalesced into
/// as few socket writes as possible (one batch of up to
/// WRITE_BATCH_BYTES). Returns the bytes written; the writer releases
/// that many pending bytes. Saturating so unreserved control frames
/// cannot underflow the counter.
async fn write_batch<W>(
    writer: &mut W,
    rx: &mut mpsc::Receiver<Bytes>,
    pending: &AtomicUsize,
    first: Option<Bytes>,
) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let Some(first) = first else {
        return Ok(());
    };
    let mut buf = Vec::with_capacity(first.len().min(WRITE_BATCH_BYTES));
    buf.extend_from_slice(&first);
    let mut total = first.len();
    while total < WRITE_BATCH_BYTES {
        match rx.try_recv() {
            Ok(frame) => {
                total += frame.len();
                buf.extend_from_slice(&frame);
            }
            Err(_) => break,
        }
    }
    writer.write_all(&buf).await?;
    // Release pending bytes (saturating: see ConnHandle::pending_bytes).
    let mut cur = pending.load(Ordering::Relaxed);
    loop {
        let next = cur.saturating_sub(total);
        match pending.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => cur = actual,
        }
    }
    Ok(())
}

/// Send a frame to a connection's queue. Returns false when the queued
/// byte limit (`max_pending`) would be exceeded (slow consumer) or the
/// channel is closed. The byte reservation is released by the writer
/// after a successful write, or here if the enqueue itself fails.
pub(crate) fn try_send(handle: &ConnHandle, frame: Bytes) -> bool {
    let n = frame.len();
    let mut cur = handle.pending_bytes.load(Ordering::Relaxed);
    loop {
        if cur + n > handle.max_pending {
            return false;
        }
        match handle.pending_bytes.compare_exchange_weak(
            cur,
            cur + n,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(actual) => cur = actual,
        }
    }
    match handle.tx.try_send(frame) {
        Ok(()) => {
            handle.out_msgs.fetch_add(1, Ordering::Relaxed);
            handle.out_bytes.fetch_add(n as u64, Ordering::Relaxed);
            true
        }
        Err(_) => {
            let mut cur = handle.pending_bytes.load(Ordering::Relaxed);
            loop {
                let next = cur.saturating_sub(n);
                match handle.pending_bytes.compare_exchange_weak(
                    cur,
                    next,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => cur = actual,
                }
            }
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

/// Per-connection INFO nonce: 16 random bytes as hex. Read from
/// /dev/urandom on Unix; on other platforms fall back to a
/// time/pid/counter mix (documented as weak in protocol.rs).
pub(crate) fn gen_nonce() -> String {
    #[cfg(unix)]
    {
        use std::io::Read;
        if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
            let mut buf = [0u8; 16];
            if f.read_exact(&mut buf).is_ok() {
                return buf.iter().map(|b| format!("{b:02x}")).collect();
            }
        }
    }
    let mut x = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E37_79B9_7F4A_7C15)
        ^ (std::process::id() as u64).wrapping_mul(0x2545_F491_4F6C_DD1D);
    let mut s = String::with_capacity(32);
    for _ in 0..16 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.push_str(&format!("{:02x}", (x & 0xff) as u8));
    }
    s
}

/// SNI / server name for a TLS route/leaf dial: IP literals use the
/// IP-address form, hostnames the DNS form; loopback hosts use
/// `localhost` (test certificates carry DNS:localhost / IP:127.0.0.1
/// SANs, and Go interop certificates do the same).
pub(crate) fn tls_server_name(host: &str) -> Option<rustls::pki_types::ServerName<'static>> {
    use rustls::pki_types::ServerName;
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Some(ServerName::IpAddress(ip.into()));
    }
    let name = if host == "localhost" || host == "127.0.0.1" || host == "::1" {
        "localhost"
    } else {
        host
    };
    ServerName::try_from(name.to_string()).ok()
}

pub(crate) fn route_info_text(shared: &Arc<Shared>) -> String {
    let (host, port) = match shared.cluster_addr {
        Some(a) => (a.ip().to_string(), a.port()),
        None => (
            shared.config.cluster_host.clone(),
            shared.config.cluster_port,
        ),
    };
    route_info_json(
        &shared.server_id,
        &shared.config.name,
        &shared.config.effective_cluster_name(),
        &host,
        port,
        shared.config.max_payload,
        shared.listen_addr,
        shared.config.cluster_tls.is_some(),
    )
}

pub(crate) fn peer_listen_addr(shared: &Arc<Shared>, id: u64) -> String {
    shared
        .routes
        .lock()
        .unwrap()
        .get(&id)
        .and_then(|r| r.peer_listen_addr.lock().unwrap().clone())
        .unwrap_or_default()
}

pub(crate) fn leaf_info_text(shared: &Arc<Shared>, client_id: u64, connect_info: bool) -> String {
    let (host, port) = match shared.leaf_addr {
        Some(a) => (a.ip().to_string(), a.port()),
        None => (shared.config.host.clone(), shared.config.leaf_port),
    };
    // Go's soliciting leaf requires client_id AND a leaf advertisement
    // (leafnode_urls) in the hub INFO, else it closes with
    // "attempted to connect to wrong port".
    let urls = shared.leaf_addr.map(|a| format!("{}:{}", a.ip(), a.port()));
    let (tls_required, tls_verify) = match &shared.config.leaf_tls {
        Some(t) => (true, t.verify || t.verify_and_map),
        None => (false, false),
    };
    leaf_info_json(
        &shared.server_id,
        &shared.config.name,
        &host,
        port,
        shared.config.max_payload,
        shared.config.jetstream,
        &shared.config.effective_cluster_name(),
        client_id,
        urls.as_deref(),
        tls_required,
        tls_verify,
        connect_info,
    )
}

/// Account for an accepted leaf connection: the account of the leaf
/// user when that user belongs to a configured account, else `$G`.
/// Matches on user+pass or on token, mirroring `authenticate` minus
/// nkey (leaf CONNECTs do not carry nkey signatures).
pub(crate) fn leaf_account_for(shared: &Arc<Shared>, connect_json: &str) -> String {
    let user = json_str(connect_json, "user");
    let pass = json_str(connect_json, "pass");
    let token = json_str(connect_json, "auth_token");
    for account in &shared.config.accounts {
        for u in &account.users {
            let pass_ok = !u.name.is_empty()
                && user.as_deref() == Some(u.name.as_str())
                && pass.as_deref() == u.pass.as_deref();
            let token_ok = u.token.is_some() && token.as_deref() == u.token.as_deref();
            if pass_ok || token_ok {
                return account.name.clone();
            }
        }
    }
    "$G".to_string()
}

/// Hub-side leaf auth: explicit --leaf-user/--leaf-pass win; otherwise the
/// server's client auth (if any) applies; otherwise open.
pub(crate) fn leaf_connect_authorized(shared: &Arc<Shared>, connect_json: &str) -> bool {
    if let Some(user) = &shared.config.leaf_user {
        let ok_user = json_str(connect_json, "user").as_deref() == Some(user.as_str());
        let ok_pass =
            json_str(connect_json, "pass").as_deref() == shared.config.leaf_pass.as_deref();
        return ok_user && ok_pass;
    }
    if shared.config.user.is_some() {
        let ok_user = json_str(connect_json, "user").as_deref() == shared.config.user.as_deref();
        let ok_pass = json_str(connect_json, "pass").as_deref() == shared.config.pass.as_deref();
        return ok_user && ok_pass;
    }
    true
}

pub(crate) struct ConnState {
    verbose: bool,
    echo: bool,
    /// Pedantic mode from CONNECT: invalid publish subjects get -ERR but
    /// are still delivered (Go behaviour).
    pedantic: bool,
    /// No-responders flag from CONNECT (see deliver()).
    no_responders: bool,
    /// Account this connection authenticated as ($G by default).
    account: String,
    /// JWT account limits for this connection's account, if it is a
    /// JWT account carrying a `limits` object (Phase 28C). `None`
    /// means static accounts or limits-less JWT accounts: unlimited.
    jwt_limits: Option<crate::jwt::AccountLimits>,
    /// Effective per-message payload limit for this connection:
    /// min(server `max_payload`, account `payload` limit) when the
    /// account limit is >= 0, else the server value. Synced into the
    /// connection's parser after each CONNECT (so a PUB in the same
    /// TCP chunk as CONNECT is still checked).
    effective_max_payload: usize,
    /// Effective subject permissions for this connection.
    permissions: crate::config::Permissions,
    /// Per-connection INFO nonce (nkey clients sign this).
    nonce: String,
    /// Peer certificate identity candidates (SAN/CN/DN) when the
    /// connection used TLS; empty otherwise. Used only when
    /// `verify_and_map` is configured.
    cert_candidates: Vec<String>,
}

/// Build the client INFO for one connection. The nonce is a per-connection
/// challenge string (what an nkey client signs in CONNECT); it is only
/// advertised when authentication is configured (Go omitempty).
pub(crate) fn client_info(
    shared: &Shared,
    client_id: u64,
    peer_ip: &str,
    nonce: &str,
) -> ServerInfo {
    let auth_required = shared.config.auth_required();
    ServerInfo {
        server_id: shared.server_id.clone(),
        server_name: shared.config.name.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        proto: 1,
        headers: true,
        max_payload: shared.config.max_payload,
        host: shared.listen_addr.ip().to_string(),
        port: shared.listen_addr.port(),
        client_id,
        nonce: if auth_required {
            nonce.to_string()
        } else {
            String::new()
        },
        auth_required,
        tls_required: shared.config.tls_required(),
        jetstream: shared.config.jetstream,
        client_ip: peer_ip.to_string(),
        cluster: shared.config.cluster_name.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_conn<R, W>(
    mut reader: R,
    writer: W,
    shared: Arc<Shared>,
    client_id: u64,
    peer_ip: String,
    nonce: String,
    info_sent: bool,
    cert_candidates: Vec<String>,
) where
    R: AsyncReadExt + Unpin + Send,
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    let (handle, tx) = spawn_writer(writer, shared.clone());
    shared
        .conns
        .lock()
        .unwrap()
        .insert(client_id, handle.clone());

    // Send INFO first, per protocol, unless the accept loop already sent
    // it in plaintext for a STARTTLS upgrade (see client_accept_loop).
    if !info_sent {
        let info = client_info(&shared, client_id, &peer_ip, &nonce);
        let _ = tx.send(encode_info(&info)).await;
    }

    // Server-initiated PING (Go behaviour): every ping_interval, send
    // PING; after ping_max missed PONGs, close with -ERR 'Stale
    // Connection'. With max 2 and interval T the close lands at ~3T
    // (PING at T, PING at 2T, close at 3T when still unanswered), matching
    // Go v2.15.1's observed timing. The task holds only a Weak handle,
    // so it cannot keep the connection alive, and it stops when
    // handle_conn notifies it at cleanup.
    let ping_stop = Arc::new(Notify::new());
    if !shared.config.ping_interval.is_zero() && shared.config.ping_max > 0 {
        let weak = Arc::downgrade(&handle);
        let stop = ping_stop.clone();
        let interval = shared.config.ping_interval;
        let max = shared.config.ping_max;
        let shutdown = shared.shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop.notified() => break,
                    _ = shutdown.notified() => break,
                    _ = tokio::time::sleep(interval) => {}
                }
                let Some(h) = weak.upgrade() else { break };
                if h.is_closed() {
                    break;
                }
                if h.ping_missed.load(Ordering::SeqCst) >= max as u64 {
                    let _ = try_send(&h, encode_err("Stale Connection"));
                    h.mark_closed();
                    break;
                }
                if !try_send(&h, encode_ping()) {
                    break;
                }
                h.ping_missed.fetch_add(1, Ordering::SeqCst);
            }
        });
    }

    let mut state = ConnState {
        verbose: false,
        echo: true,
        pedantic: false,
        no_responders: false,
        account: "$G".to_string(),
        jwt_limits: None,
        effective_max_payload: shared.config.max_payload,
        permissions: crate::config::Permissions::default(),
        nonce: nonce.clone(),
        cert_candidates,
    };
    let mut parser = Parser::new(shared.config.max_payload);
    let mut buf = vec![0u8; 16 * 1024];

    'read_loop: loop {
        if handle.is_closed() {
            break;
        }
        let n = tokio::select! {
            _ = handle.close.notified() => break 'read_loop,
            _ = shared.shutdown.notified() => break 'read_loop,
            r = reader.read(&mut buf) => match r {
                Ok(0) | Err(_) => break 'read_loop,
                Ok(n) => n,
            },
        };
        parser.feed(&buf[..n]);
        loop {
            match parser.next_op() {
                Ok(Some(op)) => {
                    if !process_op(op, client_id, &shared, &handle, &tx, &mut state).await {
                        break 'read_loop;
                    }
                    // Re-sync the parser's payload ceiling after a
                    // CONNECT that set a per-connection limit (Phase
                    // 28C): a PUB arriving in the same TCP chunk as
                    // CONNECT is parsed after this sync, so it is
                    // checked against the account limit too.
                    parser.set_max_payload(state.effective_max_payload);
                }
                Ok(None) => break,
                Err(e) => {
                    // Go closes silently on malformed CONNECT: no -ERR,
                    // and the serde detail must never reach the wire.
                    if !matches!(e, crate::protocol::ParseError::InvalidConnect(_)) {
                        let _ = tx.send(encode_err(&e.to_string())).await;
                    }
                    break 'read_loop;
                }
            }
        }
    }

    ping_stop.notify_waiters();
    // Cleanup: subscriptions and connection registry. Dropping the last
    // channel senders closes the writer queue, so the writer flushes any
    // pending frames (e.g. a final -ERR) and then exits on its own.
    let removed = {
        let mut sl = shared.sublist.write().unwrap();
        let patterns = sl.patterns_for_conn(client_id);
        sl.remove_conn(client_id);
        patterns
    };
    for (account, subject, queue) in removed {
        if !local_pattern_in_use(&shared, &account, &subject, queue.as_deref()) {
            propagate_local_sub(&shared, &account, &subject, queue.as_deref(), false);
        }
    }
    shared.conns.lock().unwrap().remove(&client_id);
    shared.conn_info.lock().unwrap().remove(&client_id);
    drop(tx);
    drop(handle);
}

/// JWT account publish limits (Phase 28C). Returns `Some(err)` when
/// the publish must not be delivered, with `close` saying whether
/// the connection closes too (Go: payload violations close, data
/// budget exhaustion drops the message and keeps the connection).
/// A data-limit check that passes charges the payload bytes to the
/// account's cumulative budget before returning `None`.
fn publish_limit_violation(
    state: &ConnState,
    shared: &Arc<Shared>,
    payload_len: usize,
) -> Option<(&'static str, bool)> {
    let limits = state.jwt_limits.as_ref()?;
    if limits.payload >= 0 && payload_len > limits.payload as usize {
        return Some(("Maximum Payload Violation", true));
    }
    if limits.data >= 0 {
        let mut usage = shared.account_data_bytes.lock().unwrap();
        let used = *usage.get(&state.account).unwrap_or(&0);
        if used.saturating_add(payload_len as u64) > limits.data as u64 {
            return Some(("Maximum Data Exceeded", false));
        }
        usage.insert(state.account.clone(), used + payload_len as u64);
    }
    None
}

/// Send the limit error for a publish that `publish_limit_violation`
/// rejected. Returns false when the connection must close.
async fn send_limit_error(tx: &mpsc::Sender<Bytes>, err: &'static str, close: bool) -> bool {
    let _ = tx.send(encode_err(err)).await;
    !close
}

/// Process one client operation. Returns false when the connection must close.
pub(crate) async fn process_op(
    op: ClientOp,
    client_id: u64,
    shared: &Arc<Shared>,
    handle: &Arc<ConnHandle>,
    tx: &mpsc::Sender<Bytes>,
    state: &mut ConnState,
) -> bool {
    match op {
        ClientOp::Connect(info) => {
            let nonce = state.nonce.as_bytes();
            let Some(outcome) =
                shared
                    .config
                    .authenticate_with_cert(&info, Some(nonce), &state.cert_candidates)
            else {
                let _ = tx.send(encode_err("Authorization Violation")).await;
                return false;
            };
            state.verbose = info.verbose;
            state.echo = info.echo;
            state.pedantic = info.pedantic;
            state.no_responders = info.no_responders;
            // JWT account limits (Phase 28C). The connection limit
            // is account-wide: count live connections already on
            // this account (the current entry still says `$G` until
            // the block below, so it is not counted; exclude it
            // anyway). At the limit, Go's text and a close.
            let jwt_limits = shared.config.jwt_account_limits(&outcome.account);
            if let Some(limits) = &jwt_limits {
                if limits.conn >= 0 {
                    let live = {
                        let infos = shared.conn_info.lock().unwrap();
                        infos
                            .values()
                            .filter(|ci| ci.account == outcome.account && ci.id != client_id)
                            .count()
                    };
                    if live >= limits.conn as usize {
                        let _ = tx
                            .send(encode_err("maximum account active connections exceeded"))
                            .await;
                        return false;
                    }
                }
                state.effective_max_payload = if limits.payload >= 0 {
                    shared.config.max_payload.min(limits.payload as usize)
                } else {
                    shared.config.max_payload
                };
            }
            state.jwt_limits = jwt_limits;
            state.account = outcome.account.clone();
            state.permissions = outcome.permissions.clone();
            handle.headers.store(info.headers, Ordering::SeqCst);
            // Record account/user for monitoring (connz/accounz).
            {
                let mut infos = shared.conn_info.lock().unwrap();
                if let Some(ci) = infos.get_mut(&client_id) {
                    ci.account = outcome.account;
                    ci.user = outcome.user;
                }
            }
            if state.verbose {
                let _ = tx.send(encode_ok()).await;
            }
            true
        }
        ClientOp::Ping => {
            let _ = tx.send(encode_pong()).await;
            true
        }
        ClientOp::Pong => {
            // A PONG answers every outstanding server PING.
            handle.ping_missed.store(0, Ordering::SeqCst);
            true
        }
        ClientOp::Pub {
            subject,
            reply_to,
            payload,
        } => {
            handle.in_msgs.fetch_add(1, Ordering::Relaxed);
            handle
                .in_bytes
                .fetch_add(payload.len() as u64, Ordering::Relaxed);
            shared.stats.in_msgs.fetch_add(1, Ordering::Relaxed);
            shared
                .stats
                .in_bytes
                .fetch_add(payload.len() as u64, Ordering::Relaxed);
            // Service-import reply: a responder in the exporting
            // account publishes to the synthetic `_RIMPORT.<seq>`
            // subject it received as its reply subject. That publish
            // is routed back to the importer's original reply subject
            // before any permission check — the responder's publish
            // allow list is about its own subjects, not the import
            // machinery's reply channel (Go's `_R_.` replies work the
            // same way). An unmapped synthetic subject falls through
            // to the normal permission-checked publish path.
            if subject.starts_with("_RIMPORT.") {
                let reply_map = lookup_import_reply(shared, &subject);
                if let Some((account, orig_reply)) = reply_map {
                    if let Some((err, close)) =
                        publish_limit_violation(state, shared, payload.len())
                    {
                        return send_limit_error(tx, err, close).await;
                    }
                    if state.verbose {
                        let _ = tx.send(encode_ok()).await;
                    }
                    deliver(
                        shared,
                        &Message::new(&orig_reply, None, None, payload).with_account(&account),
                        Origin::Internal,
                        None,
                    );
                    return true;
                }
                // Phase 32: a reply minted by another node (its node
                // id differs from ours) is forwarded to every
                // route/leaf, bypassing the publish-permission check
                // exactly as the same-node interception above does:
                // only the minting node claims it on arrival. A
                // same-node subject whose mapping expired falls
                // through to the normal permission-checked path, as
                // before.
                if let Some((node, _)) = super::imports::parse_import_reply_subject(&subject) {
                    if node != shared.server_id {
                        if state.verbose {
                            let _ = tx.send(encode_ok()).await;
                        }
                        super::delivery::forward_remote_import_reply(
                            shared,
                            &Message::new(&subject, reply_to, None, payload)
                                .with_account(&state.account),
                        );
                        return true;
                    }
                }
            }
            if !state.permissions.can_publish(&subject) {
                // Go's exact error text; the connection stays alive and
                // the verbose +OK is replaced by the error (verified
                // against nats-server v2.15.1).
                let _ = tx
                    .send(encode_err(&format!(
                        "Permissions Violation for Publish to \"{subject}\""
                    )))
                    .await;
                return true;
            }
            // JWT account publish limits (Phase 28C), checked after
            // permissions and before the verbose +OK, so a rejected
            // publish is never acknowledged as delivered.
            if let Some((err, close)) = publish_limit_violation(state, shared, payload.len()) {
                return send_limit_error(tx, err, close).await;
            }
            // Go ordering (verified): pedantic -ERR first, then the
            // verbose +OK, then delivery to the publisher itself. An
            // invalid publish subject is still delivered literally,
            // except empty-token subjects, which Go routes nowhere.
            if state.pedantic && !crate::subject::is_valid_publish_subject(&subject) {
                let _ = tx.send(encode_err("Invalid Publish Subject")).await;
            }
            if state.verbose {
                let _ = tx.send(encode_ok()).await;
            }
            if !crate::subject::has_empty_token(&subject) {
                deliver(
                    shared,
                    &Message::new(&subject, reply_to, None, payload)
                        .with_account(&state.account)
                        .with_no_responders(state.no_responders),
                    Origin::Local(client_id, state.echo),
                    None,
                );
            }
            true
        }
        ClientOp::HPub {
            subject,
            reply_to,
            headers,
            payload,
        } => {
            handle.in_msgs.fetch_add(1, Ordering::Relaxed);
            handle
                .in_bytes
                .fetch_add((headers.len() + payload.len()) as u64, Ordering::Relaxed);
            shared.stats.in_msgs.fetch_add(1, Ordering::Relaxed);
            shared
                .stats
                .in_bytes
                .fetch_add((headers.len() + payload.len()) as u64, Ordering::Relaxed);
            // Same synthetic-reply interception as PUB, preserving
            // headers on the return leg.
            if subject.starts_with("_RIMPORT.") {
                let reply_map = lookup_import_reply(shared, &subject);
                if let Some((account, orig_reply)) = reply_map {
                    if let Some((err, close)) =
                        publish_limit_violation(state, shared, payload.len())
                    {
                        return send_limit_error(tx, err, close).await;
                    }
                    if state.verbose {
                        let _ = tx.send(encode_ok()).await;
                    }
                    deliver(
                        shared,
                        &Message::new(&orig_reply, None, Some(headers), payload)
                            .with_account(&account),
                        Origin::Internal,
                        None,
                    );
                    return true;
                }
                // Phase 32: remote-node reply, as for PUB above.
                if let Some((node, _)) = super::imports::parse_import_reply_subject(&subject) {
                    if node != shared.server_id {
                        if state.verbose {
                            let _ = tx.send(encode_ok()).await;
                        }
                        super::delivery::forward_remote_import_reply(
                            shared,
                            &Message::new(&subject, reply_to, Some(headers), payload)
                                .with_account(&state.account),
                        );
                        return true;
                    }
                }
            }
            if !state.permissions.can_publish(&subject) {
                let _ = tx
                    .send(encode_err(&format!(
                        "Permissions Violation for Publish to \"{subject}\""
                    )))
                    .await;
                return true;
            }
            // JWT account publish limits (Phase 28C), as for PUB.
            if let Some((err, close)) = publish_limit_violation(state, shared, payload.len()) {
                return send_limit_error(tx, err, close).await;
            }
            if state.pedantic && !crate::subject::is_valid_publish_subject(&subject) {
                let _ = tx.send(encode_err("Invalid Publish Subject")).await;
            }
            if state.verbose {
                let _ = tx.send(encode_ok()).await;
            }
            if !crate::subject::has_empty_token(&subject) {
                deliver(
                    shared,
                    &Message::new(&subject, reply_to, Some(headers), payload)
                        .with_account(&state.account)
                        .with_no_responders(state.no_responders),
                    Origin::Local(client_id, state.echo),
                    None,
                );
            }
            true
        }
        ClientOp::Sub {
            subject,
            queue,
            sid,
        } => {
            if !state.permissions.can_subscribe(&subject) {
                // Go's exact error text; the connection stays alive and
                // no +OK is sent (verified against nats-server v2.15.1).
                let _ = tx
                    .send(encode_err(&format!(
                        "Permissions Violation for Subscription to \"{subject}\""
                    )))
                    .await;
                return true;
            }
            // Go: an invalid SUB subject gets -ERR, no subscription is
            // created, no +OK is sent, and the connection stays alive.
            if !crate::subject::is_valid_subscribe_subject(&subject) {
                let _ = tx.send(encode_err("Invalid Subject")).await;
                return true;
            }
            // Go keeps the first subscription for a sid: a duplicate SUB
            // must not replace it and must not propagate its (different)
            // pattern to routes/leafs. It still gets the verbose +OK.
            let already = {
                let sl = shared.sublist.read().unwrap();
                sl.pattern_of(client_id, &sid).is_some()
            };
            // JWT account subscription limit (Phase 28C, Go:
            // per-connection, connection stays alive, subscription
            // is not created). Duplicate sids are exempt — Go keeps
            // the first subscription and its count is unchanged.
            if !already {
                if let Some(limits) = &state.jwt_limits {
                    if limits.subs >= 0 {
                        let count = {
                            let sl = shared.sublist.read().unwrap();
                            sl.patterns_for_conn(client_id).len()
                        };
                        if count >= limits.subs as usize {
                            let _ = tx.send(encode_err("maximum subscriptions exceeded")).await;
                            return true;
                        }
                    }
                }
            }
            if !already {
                let is_new = {
                    let mut sl = shared.sublist.write().unwrap();
                    let is_new = !sl.has_pattern(&subject, queue.as_deref(), &state.account);
                    sl.add(Subscription::new(
                        client_id,
                        sid,
                        subject.clone(),
                        queue.clone(),
                        state.account.clone(),
                    ));
                    is_new
                };
                if is_new {
                    propagate_local_sub(shared, &state.account, &subject, queue.as_deref(), true);
                }
            }
            if state.verbose {
                let _ = tx.send(encode_ok()).await;
            }
            true
        }
        ClientOp::Unsub { sid, max } => {
            match max {
                None => {
                    let pattern = {
                        let mut sl = shared.sublist.write().unwrap();
                        let p = sl.pattern_of(client_id, &sid);
                        sl.remove(client_id, &sid);
                        p
                    };
                    if let Some((account, subject, queue)) = pattern {
                        if !local_pattern_in_use(shared, &account, &subject, queue.as_deref()) {
                            propagate_local_sub(
                                shared,
                                &account,
                                &subject,
                                queue.as_deref(),
                                false,
                            );
                        }
                    }
                }
                Some(max) => {
                    shared
                        .sublist
                        .write()
                        .unwrap()
                        .set_max(client_id, &sid, max);
                }
            }
            if state.verbose {
                let _ = tx.send(encode_ok()).await;
            }
            true
        }
    }
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------
