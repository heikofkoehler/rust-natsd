//! Tokio server core: client listener, cluster routes, leaf nodes, and
//! message routing between them.
//!
//! Topology rules (full mesh for routes, hub-and-spoke for leafs):
//! - A message published by a local client is delivered to local subscribers
//!   and forwarded to every route/leaf with matching interest.
//! - A message arriving on a route is delivered locally only (full mesh: the
//!   origin already forwarded to every other node directly).
//! - A message arriving on a leaf is delivered locally only (the leaf's hub
//!   never forwards it back down the same leaf, and vice versa).
//! - Subscription interest is propagated as RS+/RS- (routes) and LS+/LS-
//!   (leafs) with a per-(pattern, queue) count, mirroring Go's weight
//!   accounting closely enough for interop.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Notify};
use tokio::task::JoinSet;

use crate::config::{parse_route_url, Config};
use crate::leaf::{
    encode_leaf_hmsg, encode_lmsg, encode_ls_minus, encode_ls_plus, leaf_connect_json,
    leaf_info_json, LeafOp, LeafParser,
};
use crate::protocol::{
    encode_err, encode_hmsg, encode_info, encode_msg, encode_ok, encode_pong, ClientOp, Parser,
    ServerInfo,
};
use crate::route::{
    encode_rmsg, encode_route_hmsg, encode_rs_minus, encode_rs_plus, route_connect_json,
    route_info_json, RouteOp, RouteParser,
};
use crate::sublist::{Sublist, Subscription};

/// Bounded per-connection outbound queue. A subscriber that cannot keep up
/// (queue full) is disconnected as a slow consumer, like `nats-server`.
const OUTBOUND_CHANNEL_CAPACITY: usize = 1024;

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
        }
    }

    pub fn with_account(mut self, account: &str) -> Self {
        self.account = account.to_string();
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

/// Outbound channel + close signalling, shared by client, route and leaf
/// connections. The writer task holds no sender, so dropping all senders
/// flushes pending frames (e.g. a final -ERR) before the socket closes.
pub(crate) struct ConnHandle {
    tx: mpsc::Sender<Bytes>,
    headers: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    close: Arc<Notify>,
    pub(crate) in_msgs: AtomicU64,
    pub(crate) in_bytes: AtomicU64,
    pub(crate) out_msgs: AtomicU64,
    pub(crate) out_bytes: AtomicU64,
}

impl ConnHandle {
    fn new(tx: mpsc::Sender<Bytes>) -> Arc<Self> {
        Arc::new(ConnHandle {
            tx,
            headers: Arc::new(AtomicBool::new(false)),
            closed: Arc::new(AtomicBool::new(false)),
            close: Arc::new(Notify::new()),
            in_msgs: AtomicU64::new(0),
            in_bytes: AtomicU64::new(0),
            out_msgs: AtomicU64::new(0),
            out_bytes: AtomicU64::new(0),
        })
    }
    fn mark_closed(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.close.notify_waiters();
    }
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

/// One inter-server connection's interest: (account, pattern, queue)
/// -> count. Routes are multi-account; leaf interest is stored the
/// same way but a leaf connection only ever carries its own account.
type InterestMap = HashMap<(String, String, Option<String>), u32>;

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
    fn client(id: u64, addr: String, tls: bool) -> Self {
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
}

pub struct Shared {
    pub(crate) config: Config,
    pub(crate) listen_addr: SocketAddr,
    pub(crate) cluster_addr: Option<SocketAddr>,
    pub(crate) leaf_addr: Option<SocketAddr>,
    // Consumed by the monitoring endpoint (Phase 5: /varz http_port).
    #[allow(dead_code)]
    pub(crate) http_addr: Option<SocketAddr>,
    /// TLS acceptor for client connections; None means plain text.
    pub(crate) tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
    pub(crate) server_id: String,
    pub(crate) started: Instant,
    /// Wall-clock start time, for the monitoring endpoint's `start` field.
    pub(crate) started_wall: std::time::SystemTime,
    pub(crate) sublist: Mutex<Sublist>,
    pub(crate) conns: Mutex<HashMap<u64, Arc<ConnHandle>>>,
    pub(crate) conn_info: Mutex<HashMap<u64, ConnInfo>>,
    pub(crate) routes: Mutex<HashMap<u64, Arc<RouteConn>>>,
    pub(crate) route_subs: Mutex<HashMap<u64, InterestMap>>,
    pub(crate) leafs: Mutex<HashMap<u64, Arc<LeafConn>>>,
    pub(crate) leaf_subs: Mutex<HashMap<u64, InterestMap>>,
    pub(crate) next_conn_id: AtomicU64,
    /// Rotating cursor for global queue-group selection (local members,
    /// routes, leafs are weighted by member count).
    pub(crate) queue_cursor: AtomicU64,
    pub(crate) stats: Stats,
    pub(crate) shutdown: Arc<Notify>,
    pub(crate) shutdown_flag: AtomicBool,
    /// JetStream engine, present when `--jetstream` is set (Phase 9).
    pub(crate) jetstream: Option<Arc<crate::jetstream::JetStream>>,
}

/// Targets for one cross-node fan-out: (conn id, writer, has plain
/// interest, queue-group member counts).
type InterestTargets = Vec<(u64, Arc<ConnHandle>, bool, HashMap<String, u32>)>;

/// A running server instance; used by `main` and by tests.
pub struct RunningServer {
    pub addr: SocketAddr,
    pub cluster_addr: Option<SocketAddr>,
    pub leaf_addr: Option<SocketAddr>,
    pub http_addr: Option<SocketAddr>,
    shutdown_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<io::Result<()>>,
}

impl RunningServer {
    /// Signal shutdown and wait for listeners and connections to drain.
    pub async fn shutdown(self) -> io::Result<()> {
        let _ = self.shutdown_tx.send(true);
        match self.task.await {
            Ok(r) => r,
            Err(e) => Err(io::Error::other(e.to_string())),
        }
    }
}

/// Bind listeners and start serving in a background supervisor task.
pub async fn start(config: Config) -> io::Result<RunningServer> {
    let listener = TcpListener::bind((config.host.as_str(), config.port)).await?;
    let addr = listener.local_addr()?;
    let cluster_listener = if config.cluster_port != 0 {
        Some(TcpListener::bind((config.cluster_host.as_str(), config.cluster_port)).await?)
    } else {
        None
    };
    let cluster_addr = cluster_listener
        .as_ref()
        .map(|l| l.local_addr())
        .transpose()?;
    let leaf_listener = if config.leaf_port != 0 {
        Some(TcpListener::bind((config.host.as_str(), config.leaf_port)).await?)
    } else {
        None
    };
    let leaf_addr = leaf_listener.as_ref().map(|l| l.local_addr()).transpose()?;
    let http_listener = if config.http_port != 0 {
        Some(TcpListener::bind((config.host.as_str(), config.http_port)).await?)
    } else {
        None
    };
    let http_addr = http_listener.as_ref().map(|l| l.local_addr()).transpose()?;
    let jetstream = if config.jetstream {
        Some(Arc::new(crate::jetstream::JetStream::new()))
    } else {
        None
    };
    // Build the TLS acceptor up front so a bad cert/key is a startup
    // error, not a per-connection failure.
    let tls_acceptor = match &config.tls {
        Some(settings) => Some(Arc::new(tokio_rustls::TlsAcceptor::from(
            crate::tls::build_server_config(settings).map_err(io::Error::other)?,
        ))),
        None => None,
    };
    let shared = Arc::new(Shared {
        config,
        listen_addr: addr,
        cluster_addr,
        leaf_addr,
        http_addr,
        tls_acceptor,
        server_id: gen_server_id(),
        started: Instant::now(),
        started_wall: std::time::SystemTime::now(),
        sublist: Mutex::new(Sublist::new()),
        conns: Mutex::new(HashMap::new()),
        conn_info: Mutex::new(HashMap::new()),
        routes: Mutex::new(HashMap::new()),
        route_subs: Mutex::new(HashMap::new()),
        leafs: Mutex::new(HashMap::new()),
        leaf_subs: Mutex::new(HashMap::new()),
        next_conn_id: AtomicU64::new(1),
        queue_cursor: AtomicU64::new(0),
        stats: Stats::default(),
        shutdown: Arc::new(Notify::new()),
        shutdown_flag: AtomicBool::new(false),
        jetstream,
    });
    // JetStream nodes advertise $JS.API.> interest to routes/leafs so API
    // requests published on other nodes are forwarded to the node that
    // hosts the stream (see jetstream.rs).
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(supervisor(
        listener,
        cluster_listener,
        leaf_listener,
        http_listener,
        shared,
        shutdown_rx,
    ));
    Ok(RunningServer {
        addr,
        cluster_addr,
        leaf_addr,
        http_addr,
        shutdown_tx,
        task,
    })
}

async fn supervisor(
    listener: TcpListener,
    cluster_listener: Option<TcpListener>,
    leaf_listener: Option<TcpListener>,
    http_listener: Option<TcpListener>,
    shared: Arc<Shared>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut tasks = JoinSet::new();
    tasks.spawn(client_accept_loop(listener, shared.clone()));
    if let Some(l) = cluster_listener {
        tasks.spawn(route_accept_loop(l, shared.clone()));
    }
    if let Some(l) = leaf_listener {
        tasks.spawn(leaf_accept_loop(l, shared.clone()));
    }
    if let Some(l) = http_listener {
        tasks.spawn(crate::monitor::accept_loop(l, shared.clone()));
    }
    // Solicited routes and leafs, each with its own retry loop.
    for url in shared.config.routes.clone() {
        tasks.spawn(solicit_route_loop(url, shared.clone()));
    }
    for url in shared.config.leaf_urls.clone() {
        tasks.spawn(solicit_leaf_loop(url, shared.clone()));
    }
    if let Some(js) = &shared.jetstream {
        tasks.spawn(crate::jetstream::redelivery_loop(
            js.clone(),
            shared.clone(),
        ));
    }
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() { break; }
            }
            _ = tasks.join_next(), if !tasks.is_empty() => {}
        }
    }
    shared.shutdown_flag.store(true, Ordering::SeqCst);
    shared.shutdown.notify_waiters();
    let drain = async { while tasks.join_next().await.is_some() {} };
    let _ = tokio::time::timeout(Duration::from_secs(2), drain).await;
    tasks.abort_all();
    Ok(())
}

async fn client_accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    loop {
        tokio::select! {
            _ = shared.shutdown.notified() => break,
            res = listener.accept() => match res {
                Ok((stream, peer)) => {
                    let _ = stream.set_nodelay(true);
                    let id = shared.next_conn_id.fetch_add(1, Ordering::SeqCst);
                    shared.stats.total_connections.fetch_add(1, Ordering::Relaxed);
                    let tls = shared.tls_acceptor.is_some();
                    shared.conn_info.lock().unwrap().insert(id, ConnInfo::client(id, peer.to_string(), tls));
                    let shared = shared.clone();
                    tokio::spawn(async move {
                        match &shared.tls_acceptor {
                            Some(acceptor) => {
                                // Handshake first, INFO only inside TLS:
                                // a plain-text client fails here and
                                // never sees protocol bytes.
                                match acceptor.accept(stream).await {
                                    Ok(tls_stream) => {
                                        let (reader, writer) = tokio::io::split(tls_stream);
                                        handle_conn(reader, writer, shared, id).await;
                                    }
                                    Err(e) => {
                                        eprintln!("rust-natsd: TLS handshake failed: {e}");
                                        shared.conn_info.lock().unwrap().remove(&id);
                                    }
                                }
                            }
                            None => {
                                let (reader, writer) = stream.into_split();
                                handle_conn(reader, writer, shared, id).await;
                            }
                        }
                    });
                }
                Err(e) => eprintln!("rust-natsd: accept error: {e}"),
            },
        }
    }
}

/// Per-connection INFO nonce: 16 random bytes as hex. Read from
/// /dev/urandom on Unix; on other platforms fall back to a
/// time/pid/counter mix (documented as weak in protocol.rs).
fn gen_nonce() -> String {
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

async fn route_accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    loop {
        tokio::select! {
            _ = shared.shutdown.notified() => break,
            res = listener.accept() => match res {
                Ok((stream, _peer)) => {
                    let _ = stream.set_nodelay(true);
                    tokio::spawn(handle_route_accepted(stream, shared.clone()));
                }
                Err(e) => eprintln!("rust-natsd: route accept error: {e}"),
            },
        }
    }
}

async fn leaf_accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    loop {
        tokio::select! {
            _ = shared.shutdown.notified() => break,
            res = listener.accept() => match res {
                Ok((stream, _peer)) => {
                    let _ = stream.set_nodelay(true);
                    tokio::spawn(handle_leaf_accepted(stream, shared.clone()));
                }
                Err(e) => eprintln!("rust-natsd: leaf accept error: {e}"),
            },
        }
    }
}

/// Create a connection handle and spawn its writer task. Returns
/// (handle, sender). The writer holds no sender itself, so dropping all
/// senders flushes pending frames before the socket closes.
/// Generic over the write half so plain TCP and TLS streams share it.
fn spawn_writer<W>(mut writer: W, shared: Arc<Shared>) -> (Arc<ConnHandle>, mpsc::Sender<Bytes>)
where
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Bytes>(OUTBOUND_CHANNEL_CAPACITY);
    let handle = ConnHandle::new(tx.clone());
    let closed = handle.closed.clone();
    let close = handle.close.clone();
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
                    Some(bytes) => {
                        if writer.write_all(&bytes).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                },
                _ = close.notified() => break,
                _ = &mut shutdown => break,
            }
        }
        let _ = writer.shutdown().await;
    });
    (handle, tx)
}

/// Send a frame to a connection's queue. Returns false when the queue is
/// full (slow consumer) or closed.
fn try_send(handle: &ConnHandle, frame: Bytes) -> bool {
    let n = frame.len() as u64;
    match handle.tx.try_send(frame) {
        Ok(()) => {
            handle.out_msgs.fetch_add(1, Ordering::Relaxed);
            handle.out_bytes.fetch_add(n, Ordering::Relaxed);
            true
        }
        Err(_) => false,
    }
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

/// Deliver a message: to local subscribers, and (unless it arrived on a
/// route/leaf) forward it to routes and leafs with matching interest.
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
) {
    // JetStream capture / API handling happens before core delivery so a
    // stream message is stored even when nobody is subscribed, and API
    // requests are answered by the hosting node.
    if let Some(js) = &shared.jetstream {
        if crate::jetstream::handle_message(shared, js, msg, origin) {
            // API requests and acks are consumed by JetStream; they are not
            // also delivered as ordinary messages.
            return;
        }
    }

    let (publisher_conn, publisher_echo) = match origin {
        Origin::Local(id, echo) => (id, echo),
        _ => (u64::MAX, true),
    };

    // Messages arriving on a route/leaf are delivered locally only
    // (full mesh / hub-and-spoke loop prevention).
    if matches!(origin, Origin::Route(_) | Origin::Leaf(_)) {
        let result = {
            let mut sl = shared.sublist.lock().unwrap();
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
        return;
    }

    // Local / internal origin: global queue selection across local
    // members, routes, and leafs (weighted by member count), so a queue
    // group spanning nodes still delivers exactly once per message.
    let gm = {
        let sl = shared.sublist.lock().unwrap();
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
        let mut sl = shared.sublist.lock().unwrap();
        sl.note_deliveries(&local_deliveries)
    };
    send_local_frames(shared, msg, &local_deliveries);
    cleanup_expired(shared, &expired);
    shared
        .stats
        .out_msgs
        .fetch_add(local_deliveries.len() as u64, Ordering::Relaxed);

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
}

/// Send MSG/HMSG frames for local deliveries, dropping slow consumers.
fn send_local_frames(shared: &Arc<Shared>, msg: &Message, deliveries: &[crate::sublist::Delivery]) {
    for d in deliveries {
        let target = shared.conns.lock().unwrap().get(&d.conn_id).cloned();
        let Some(target) = target else { continue };
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
            eprintln!(
                "rust-natsd: slow consumer, dropping connection {}",
                d.conn_id
            );
            shared.sublist.lock().unwrap().remove_conn(d.conn_id);
            if let Some(h) = shared.conns.lock().unwrap().remove(&d.conn_id) {
                h.mark_closed();
            }
            shared.conn_info.lock().unwrap().remove(&d.conn_id);
        }
    }
}

/// Remove auto-unsubscribed subscriptions and propagate RS-/LS- for
/// patterns that lost their last local subscriber.
fn cleanup_expired(shared: &Arc<Shared>, expired: &[(u64, String)]) {
    if expired.is_empty() {
        return;
    }
    let patterns: Vec<(String, String, Option<String>)> = {
        let sl = shared.sublist.lock().unwrap();
        expired
            .iter()
            .filter_map(|(cid, sid)| sl.pattern_of(*cid, sid))
            .collect()
    };
    {
        let mut sl = shared.sublist.lock().unwrap();
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
fn interest_of(subs: &InterestMap, subject: &str, account: &str) -> (bool, HashMap<String, u32>) {
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

fn route_interest_full(shared: &Arc<Shared>, subject: &str, account: &str) -> InterestTargets {
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

fn leaf_interest_full(shared: &Arc<Shared>, subject: &str, account: &str) -> InterestTargets {
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
fn propagate_local_sub(
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
    for r in shared.routes.lock().unwrap().values() {
        if !r.handle.is_closed() {
            let _ = try_send(&r.handle, rframe.clone());
        }
    }
    // Leaf frames carry no account token: a leaf connection only ever
    // carries its own account, so propagate only to matching leafs.
    let lframe = if added {
        encode_ls_plus(subject, queue)
    } else {
        encode_ls_minus(subject, queue)
    };
    for l in shared.leafs.lock().unwrap().values() {
        if !l.handle.is_closed() && l.account == account {
            let _ = try_send(&l.handle, lframe.clone());
        }
    }
}

/// Does any local subscription (or JetStream internal pattern) still use
/// this pattern+queue in this account?
fn local_pattern_in_use(
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
        .lock()
        .unwrap()
        .has_pattern(subject, queue, account)
}

/// Send every local subscription (plus JetStream internal patterns) to one
/// route or leaf connection as RS+/LS+ frames.
fn send_local_interest(
    shared: &Arc<Shared>,
    handle: &ConnHandle,
    is_route: bool,
    leaf_account: Option<&str>,
) {
    let subs: Vec<(String, String, Option<String>)> = {
        let sl = shared.sublist.lock().unwrap();
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

fn close_route(shared: &Arc<Shared>, id: u64) {
    let removed = shared.routes.lock().unwrap().remove(&id);
    shared.route_subs.lock().unwrap().remove(&id);
    shared.conn_info.lock().unwrap().remove(&id);
    if let Some(r) = removed {
        r.handle.mark_closed();
    }
}

fn close_leaf(shared: &Arc<Shared>, id: u64) {
    let removed = shared.leafs.lock().unwrap().remove(&id);
    shared.leaf_subs.lock().unwrap().remove(&id);
    shared.conn_info.lock().unwrap().remove(&id);
    if let Some(l) = removed {
        l.handle.mark_closed();
    }
}

/// Bump a counted interest entry by `delta` (+1 / -1), removing at zero.
fn bump_interest(
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

struct ConnState {
    verbose: bool,
    echo: bool,
    /// Account this connection authenticated as ($G by default).
    account: String,
    /// Effective subject permissions for this connection.
    permissions: crate::config::Permissions,
    /// Per-connection INFO nonce (nkey clients sign this).
    nonce: String,
}

async fn handle_conn<R, W>(mut reader: R, writer: W, shared: Arc<Shared>, client_id: u64)
where
    R: AsyncReadExt + Unpin + Send,
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    let (handle, tx) = spawn_writer(writer, shared.clone());
    shared
        .conns
        .lock()
        .unwrap()
        .insert(client_id, handle.clone());

    // Send INFO first, per protocol. The nonce is generated per
    // connection and is what an nkey client signs in CONNECT.
    let nonce = gen_nonce();
    let info = ServerInfo {
        server_id: shared.server_id.clone(),
        server_name: shared.config.name.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        proto: 1,
        headers: true,
        max_payload: shared.config.max_payload,
        host: shared.listen_addr.ip().to_string(),
        port: shared.listen_addr.port(),
        client_id,
        nonce: nonce.clone(),
        auth_required: shared.config.auth_required(),
        tls_required: shared.config.tls_required(),
        jetstream: shared.config.jetstream,
    };
    let _ = tx.send(encode_info(&info)).await;

    let mut state = ConnState {
        verbose: false,
        echo: true,
        account: "$G".to_string(),
        permissions: crate::config::Permissions::default(),
        nonce: nonce.clone(),
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
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = tx.send(encode_err(&e.to_string())).await;
                    break 'read_loop;
                }
            }
        }
    }

    // Cleanup: subscriptions and connection registry. Dropping the last
    // channel senders closes the writer queue, so the writer flushes any
    // pending frames (e.g. a final -ERR) and then exits on its own.
    let removed = {
        let mut sl = shared.sublist.lock().unwrap();
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

/// Process one client operation. Returns false when the connection must close.
async fn process_op(
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
            let Some(outcome) = shared.config.authenticate(&info, Some(nonce)) else {
                let _ = tx.send(encode_err("Authorization Violation")).await;
                return false;
            };
            state.verbose = info.verbose;
            state.echo = info.echo;
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
        ClientOp::Pong => true,
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
            deliver(
                shared,
                &Message::new(&subject, reply_to, None, payload).with_account(&state.account),
                Origin::Local(client_id, state.echo),
                None,
            );
            if state.verbose {
                let _ = tx.send(encode_ok()).await;
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
            if !state.permissions.can_publish(&subject) {
                let _ = tx
                    .send(encode_err(&format!(
                        "Permissions Violation for Publish to \"{subject}\""
                    )))
                    .await;
                return true;
            }
            deliver(
                shared,
                &Message::new(&subject, reply_to, Some(headers), payload)
                    .with_account(&state.account),
                Origin::Local(client_id, state.echo),
                None,
            );
            if state.verbose {
                let _ = tx.send(encode_ok()).await;
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
            let is_new = {
                let mut sl = shared.sublist.lock().unwrap();
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
            if state.verbose {
                let _ = tx.send(encode_ok()).await;
            }
            true
        }
        ClientOp::Unsub { sid, max } => {
            match max {
                None => {
                    let pattern = {
                        let mut sl = shared.sublist.lock().unwrap();
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
                    shared.sublist.lock().unwrap().set_max(client_id, &sid, max);
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

fn json_str(json: &str, field: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()?
        .get(field)?
        .as_str()
        .map(|s| s.to_string())
}

fn json_u64(json: &str, field: &str) -> Option<u64> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()?
        .get(field)?
        .as_u64()
}

/// Read one CRLF-terminated line from a buffered reader.
async fn read_line(reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> io::Result<String> {
    let mut buf = Vec::new();
    let n = reader.read_until(b'\n', &mut buf).await?;
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
    }
    Ok(String::from_utf8_lossy(&buf).trim_end().to_string())
}

fn route_info_text(shared: &Arc<Shared>) -> String {
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
    )
}

async fn handle_route_accepted(stream: TcpStream, shared: Arc<Shared>) {
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let (handle, tx) = spawn_writer(writer, shared.clone());
    // Accepter sends INFO first.
    if tx
        .send(Bytes::from(format!(
            "INFO {}\r\n",
            route_info_text(&shared)
        )))
        .await
        .is_err()
    {
        return;
    }
    // Then expects CONNECT, then INFO from the peer.
    let connect_line = match read_line(&mut reader).await {
        Ok(l) => l,
        Err(_) => return,
    };
    let Some(connect_json) = connect_line.strip_prefix("CONNECT ") else {
        return;
    };
    if let Some(cluster) = json_str(connect_json, "cluster") {
        if cluster != shared.config.effective_cluster_name() {
            let _ = tx.send(encode_err("Cluster Name Mismatch")).await;
            return;
        }
    }
    let info_line = match read_line(&mut reader).await {
        Ok(l) => l,
        Err(_) => return,
    };
    let Some(info_json) = info_line.strip_prefix("INFO ") else {
        return;
    };
    let Some(peer_id) = json_str(info_json, "server_id") else {
        return;
    };
    if let Some(cluster) = json_str(info_json, "cluster") {
        if cluster != shared.config.effective_cluster_name() {
            let _ = tx.send(encode_err("Cluster Name Mismatch")).await;
            return;
        }
    }
    let peer_listen = match (json_str(info_json, "host"), json_u64(info_json, "port")) {
        (Some(h), Some(p)) => Some(format!("{h}:{p}")),
        _ => None,
    };
    let id = shared.next_conn_id.fetch_add(1, Ordering::SeqCst);
    shared
        .stats
        .total_connections
        .fetch_add(1, Ordering::Relaxed);
    // Inbound connection: the initiator is the peer.
    if !register_route(
        &shared,
        id,
        peer_id.clone(),
        peer_listen,
        peer_id.clone(),
        handle.clone(),
    ) {
        handle.mark_closed();
        return;
    }
    shared.conn_info.lock().unwrap().insert(
        id,
        ConnInfo {
            id,
            kind: "route",
            addr: peer_listen_addr(&shared, id),
            peer_id: Some(peer_id),
            started: Instant::now(),
            account: "$G".to_string(),
            user: None,
            tls: false,
        },
    );
    send_local_interest(&shared, &handle, true, None);
    route_read_loop(&mut reader, &shared, id, &handle, &tx).await;
    close_route(&shared, id);
}

fn peer_listen_addr(shared: &Arc<Shared>, id: u64) -> String {
    shared
        .routes
        .lock()
        .unwrap()
        .get(&id)
        .and_then(|r| r.peer_listen_addr.lock().unwrap().clone())
        .unwrap_or_default()
}

/// Register a route connection, resolving duplicates by keeping the
/// connection initiated by the smaller server id. Returns false when this
/// connection lost the duplicate race and must be closed.
fn register_route(
    shared: &Arc<Shared>,
    id: u64,
    peer_id: String,
    peer_listen: Option<String>,
    initiator: String,
    handle: Arc<ConnHandle>,
) -> bool {
    let mut routes = shared.routes.lock().unwrap();
    let mut losers: Vec<u64> = Vec::new();
    for (eid, r) in routes.iter() {
        let same = r.peer_id.lock().unwrap().as_deref() == Some(peer_id.as_str());
        if same {
            if initiator < r.initiator {
                losers.push(*eid);
            } else {
                return false;
            }
        }
    }
    for eid in losers {
        if let Some(r) = routes.remove(&eid) {
            r.handle.mark_closed();
        }
        shared.route_subs.lock().unwrap().remove(&eid);
        shared.conn_info.lock().unwrap().remove(&eid);
    }
    routes.insert(
        id,
        Arc::new(RouteConn {
            peer_id: Mutex::new(Some(peer_id)),
            peer_listen_addr: Mutex::new(peer_listen),
            initiator,
            handle,
        }),
    );
    shared.route_subs.lock().unwrap().insert(id, HashMap::new());
    true
}

async fn solicit_route_loop(url: String, shared: Arc<Shared>) {
    let Ok((host, port)) = parse_route_url(&url) else {
        eprintln!("rust-natsd: invalid route url: {url}");
        return;
    };
    let target = format!("{host}:{port}");
    let mut backoff = Duration::from_secs(1);
    loop {
        if shared.shutdown_flag.load(Ordering::SeqCst) {
            break;
        }
        // Skip dialing when we already have a route to this peer.
        let already = {
            let routes = shared.routes.lock().unwrap();
            routes
                .values()
                .any(|r| r.peer_listen_addr.lock().unwrap().as_deref() == Some(target.as_str()))
        };
        if already {
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        match TcpStream::connect((host.as_str(), port)).await {
            Ok(stream) => {
                backoff = Duration::from_secs(1);
                let _ = stream.set_nodelay(true);
                handle_route_solicited(stream, shared.clone(), target.clone()).await;
            }
            Err(_) => {
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, Duration::from_secs(5));
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn handle_route_solicited(stream: TcpStream, shared: Arc<Shared>, target: String) {
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let (handle, tx) = spawn_writer(writer, shared.clone());
    // Dialer sends CONNECT first, then reads peer INFO, then sends INFO.
    let connect = route_connect_json(&shared.server_id, &shared.config.effective_cluster_name());
    if tx
        .send(Bytes::from(format!("CONNECT {connect}\r\n")))
        .await
        .is_err()
    {
        return;
    }
    let info_line = match read_line(&mut reader).await {
        Ok(l) => l,
        Err(_) => return,
    };
    let Some(info_json) = info_line.strip_prefix("INFO ") else {
        return;
    };
    let Some(peer_id) = json_str(info_json, "server_id") else {
        return;
    };
    if let Some(cluster) = json_str(info_json, "cluster") {
        if cluster != shared.config.effective_cluster_name() {
            let _ = tx.send(encode_err("Cluster Name Mismatch")).await;
            return;
        }
    }
    if tx
        .send(Bytes::from(format!(
            "INFO {}\r\n",
            route_info_text(&shared)
        )))
        .await
        .is_err()
    {
        return;
    }
    let id = shared.next_conn_id.fetch_add(1, Ordering::SeqCst);
    shared
        .stats
        .total_connections
        .fetch_add(1, Ordering::Relaxed);
    // Solicited connection: the initiator is us.
    if !register_route(
        &shared,
        id,
        peer_id.clone(),
        Some(target),
        shared.server_id.clone(),
        handle.clone(),
    ) {
        handle.mark_closed();
        return;
    }
    shared.conn_info.lock().unwrap().insert(
        id,
        ConnInfo {
            id,
            kind: "route",
            addr: peer_listen_addr(&shared, id),
            peer_id: Some(peer_id),
            started: Instant::now(),
            account: "$G".to_string(),
            user: None,
            tls: false,
        },
    );
    send_local_interest(&shared, &handle, true, None);
    route_read_loop(&mut reader, &shared, id, &handle, &tx).await;
    close_route(&shared, id);
}

async fn route_read_loop(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
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
async fn process_route_op(
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

fn leaf_info_text(shared: &Arc<Shared>, client_id: u64) -> String {
    let (host, port) = match shared.leaf_addr {
        Some(a) => (a.ip().to_string(), a.port()),
        None => (shared.config.host.clone(), shared.config.leaf_port),
    };
    // Go's soliciting leaf requires client_id AND a leaf advertisement
    // (leafnode_urls) in the hub INFO, else it closes with
    // "attempted to connect to wrong port".
    let urls = shared.leaf_addr.map(|a| format!("{}:{}", a.ip(), a.port()));
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
    )
}

async fn handle_leaf_accepted(stream: TcpStream, shared: Arc<Shared>) {
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let (handle, tx) = spawn_writer(writer, shared.clone());
    // Hub sends INFO first.
    let client_id = shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
    if tx
        .send(Bytes::from(format!(
            "INFO {}\r\n",
            leaf_info_text(&shared, client_id)
        )))
        .await
        .is_err()
    {
        return;
    }
    // Then expects leaf CONNECT, optionally preceded by leaf INFO (Go
    // sends its INFO only when the hub INFO sets info_on_connect; our
    // INFO does not, so a Go leaf sends CONNECT straight away).
    let first_line = match read_line(&mut reader).await {
        Ok(l) => l,
        Err(_) => return,
    };
    let (peer_id, connect_json) = if let Some(info_json) = first_line.strip_prefix("INFO ") {
        let peer_id = json_str(info_json, "server_id").unwrap_or_default();
        let connect_line = match read_line(&mut reader).await {
            Ok(l) => l,
            Err(_) => return,
        };
        let Some(connect_json) = connect_line.strip_prefix("CONNECT ") else {
            return;
        };
        (peer_id, connect_json.to_string())
    } else if let Some(connect_json) = first_line.strip_prefix("CONNECT ") {
        let peer_id = json_str(connect_json, "server_id").unwrap_or_default();
        (peer_id, connect_json.to_string())
    } else {
        return;
    };
    if !leaf_connect_authorized(&shared, &connect_json) {
        let _ = tx.send(encode_err("Authorization Violation")).await;
        return;
    }
    let id = shared.next_conn_id.fetch_add(1, Ordering::SeqCst);
    shared
        .stats
        .total_connections
        .fetch_add(1, Ordering::Relaxed);
    let leaf_account = leaf_account_for(&shared, &connect_json);
    shared.leafs.lock().unwrap().insert(
        id,
        Arc::new(LeafConn {
            handle: handle.clone(),
            account: leaf_account.clone(),
        }),
    );
    shared.leaf_subs.lock().unwrap().insert(id, HashMap::new());
    shared.conn_info.lock().unwrap().insert(
        id,
        ConnInfo {
            id,
            kind: "leaf",
            addr: String::new(),
            peer_id: Some(peer_id),
            started: Instant::now(),
            account: leaf_account.clone(),
            user: None,
            tls: false,
        },
    );
    send_local_interest(&shared, &handle, false, Some(&leaf_account));
    leaf_read_loop(&mut reader, &shared, id, &handle, &tx).await;
    close_leaf(&shared, id);
}

/// Account for an accepted leaf connection: the account of the leaf
/// user when that user belongs to a configured account, else `$G`.
/// Matches on user+pass or on token, mirroring `authenticate` minus
/// nkey (leaf CONNECTs do not carry nkey signatures).
fn leaf_account_for(shared: &Arc<Shared>, connect_json: &str) -> String {
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
fn leaf_connect_authorized(shared: &Arc<Shared>, connect_json: &str) -> bool {
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

async fn solicit_leaf_loop(url: String, shared: Arc<Shared>) {
    let Ok((host, port)) = parse_route_url(&url) else {
        eprintln!("rust-natsd: invalid leaf url: {url}");
        return;
    };
    let mut backoff = Duration::from_secs(1);
    loop {
        if shared.shutdown_flag.load(Ordering::SeqCst) {
            break;
        }
        match TcpStream::connect((host.as_str(), port)).await {
            Ok(stream) => {
                backoff = Duration::from_secs(1);
                let _ = stream.set_nodelay(true);
                handle_leaf_solicited(stream, shared.clone()).await;
            }
            Err(_) => {
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, Duration::from_secs(5));
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn handle_leaf_solicited(stream: TcpStream, shared: Arc<Shared>) {
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let (handle, tx) = spawn_writer(writer, shared.clone());
    // Hub INFO first, then leaf sends INFO + CONNECT.
    let info_line = match read_line(&mut reader).await {
        Ok(l) => l,
        Err(_) => return,
    };
    let Some(_hub_info) = info_line.strip_prefix("INFO ") else {
        return;
    };
    let client_id = shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
    if tx
        .send(Bytes::from(format!(
            "INFO {}\r\n",
            leaf_info_text(&shared, client_id)
        )))
        .await
        .is_err()
    {
        return;
    }
    let connect = leaf_connect_json(
        &shared.server_id,
        &shared.config.name,
        &shared.config.effective_cluster_name(),
        shared.config.jetstream,
        shared.config.leaf_user.as_deref(),
        shared.config.leaf_pass.as_deref(),
    );
    if tx
        .send(Bytes::from(format!("CONNECT {connect}\r\n")))
        .await
        .is_err()
    {
        return;
    }
    // The hub identifies us by our INFO server_id; we don't learn a peer id
    // until the hub sends something addressed to us, so track it as the hub
    // address. (Peer id is informational only for leafs.)
    let id = shared.next_conn_id.fetch_add(1, Ordering::SeqCst);
    shared
        .stats
        .total_connections
        .fetch_add(1, Ordering::Relaxed);
    let leaf_account = shared
        .config
        .leaf_user
        .as_ref()
        .and_then(|u| {
            shared
                .config
                .accounts
                .iter()
                .find(|a| a.users.iter().any(|usr| &usr.name == u))
                .map(|a| a.name.clone())
        })
        .unwrap_or_else(|| "$G".to_string());
    shared.leafs.lock().unwrap().insert(
        id,
        Arc::new(LeafConn {
            handle: handle.clone(),
            account: leaf_account.clone(),
        }),
    );
    shared.leaf_subs.lock().unwrap().insert(id, HashMap::new());
    shared.conn_info.lock().unwrap().insert(
        id,
        ConnInfo {
            id,
            kind: "leaf",
            addr: String::new(),
            peer_id: None,
            started: Instant::now(),
            account: leaf_account.clone(),
            user: None,
            tls: false,
        },
    );
    send_local_interest(&shared, &handle, false, Some(&leaf_account));
    leaf_read_loop(&mut reader, &shared, id, &handle, &tx).await;
    close_leaf(&shared, id);
}

async fn leaf_read_loop(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
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

async fn process_leaf_op(
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

/// NATS-style server id: `N` followed by 21 chars from a 32-char alphabet,
/// derived from time and pid via xorshift (uniqueness, not security).
fn gen_server_id() -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut x = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E37_79B9_7F4A_7C15)
        ^ (std::process::id() as u64).wrapping_mul(0x2545_F491_4F6C_DD1D);
    let mut s = String::with_capacity(22);
    s.push('N');
    for _ in 0..21 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.push(ALPHABET[(x % 32) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_id_shape() {
        let id = gen_server_id();
        assert_eq!(id.len(), 22);
        assert!(id.starts_with('N'));
        assert!(id
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()));
    }
}
