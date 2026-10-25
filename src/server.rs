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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
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
    encode_err, encode_hmsg, encode_info, encode_msg, encode_ok, encode_ping, encode_pong,
    ClientOp, Parser, ServerInfo,
};
use crate::route::{
    encode_rmsg, encode_route_hmsg, encode_rs_minus, encode_rs_plus, route_connect_json,
    route_info_json, RouteOp, RouteParser,
};
use crate::sublist::{Sublist, Subscription};

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
    /// The publishing connection set `no_responders:true` in CONNECT.
    /// Only meaningful for Local origin; route/leaf/internal messages
    /// never carry it.
    pub no_responders: bool,
    /// True for a stream-import copy (already translated once): it is
    /// delivered to local subscribers only and is never translated
    /// again, so import chains cannot loop.
    pub import_copy: bool,
    /// True for a service-import translated request (already rewritten
    /// into the exporting account): it must not be service-translated
    /// a second time, but it may still fan out to stream importers.
    pub service_translated: bool,
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
            no_responders: false,
            import_copy: false,
            service_translated: false,
        }
    }

    pub fn with_account(mut self, account: &str) -> Self {
        self.account = account.to_string();
        self
    }

    pub fn with_no_responders(mut self, no_responders: bool) -> Self {
        self.no_responders = no_responders;
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
    fn new(tx: mpsc::Sender<Bytes>, max_pending: usize) -> Arc<Self> {
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
    /// Connections dropped as slow consumers (queue-full on delivery).
    pub slow_consumers: AtomicU64,
}

pub struct Shared {
    pub(crate) config: Config,
    pub(crate) listen_addr: SocketAddr,
    pub(crate) cluster_addr: Option<SocketAddr>,
    pub(crate) leaf_addr: Option<SocketAddr>,
    pub(crate) http_addr: Option<SocketAddr>,
    /// TLS acceptor for client connections; None means plain text.
    pub(crate) tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
    /// TLS acceptor for route connections (implicit TLS); None means
    /// plain text on the route port.
    pub(crate) route_tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
    /// TLS connector for solicited routes (from `cluster_tls`).
    pub(crate) route_tls_connector: Option<std::sync::Arc<tokio_rustls::TlsConnector>>,
    /// TLS acceptor for the leafnode listener (INFO first, then
    /// upgrade); None means plain text on the leaf port.
    pub(crate) leaf_tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
    /// TLS connectors for solicited leaf remotes, keyed by remote URL.
    pub(crate) leaf_remote_connectors:
        std::collections::HashMap<String, std::sync::Arc<tokio_rustls::TlsConnector>>,
    pub(crate) server_id: String,
    pub(crate) started: Instant,
    /// Wall-clock start time, for the monitoring endpoint's `start` field.
    pub(crate) started_wall: std::time::SystemTime,
    pub(crate) sublist: RwLock<Sublist>,
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
    /// Service-import reply mappings: synthetic reply subject
    /// (`_RIMPORT.<seq>`, handed to the exporting account's responder)
    /// -> (importing account, original reply subject, creation
    /// instant). Replies published to a synthetic subject are
    /// delivered back into the importing account (see process_op).
    /// Entries expire after 60 s (Phase 28C; Go's `_R_.` mappings
    /// expire by TTL, and unbounded retention was a known
    /// limitation) and are pruned opportunistically on insert and
    /// lookup, so a stale mapping can never route a reply.
    pub(crate) import_replies: Mutex<HashMap<String, (String, String, std::time::Instant)>>,
    /// Counter for synthetic service-import reply subjects.
    pub(crate) import_seq: AtomicU64,
    /// Account-wide cumulative inbound payload bytes, for JWT
    /// account `limits.data` enforcement (Phase 28C). Only accounts
    /// with a data limit are tracked here; see `ConnState`.
    pub(crate) account_data_bytes: Mutex<HashMap<String, u64>>,
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
        Some(Arc::new(crate::jetstream::JetStream::with_store_dir(
            config.js_store_dir.as_ref().map(std::path::PathBuf::from),
        )))
    } else {
        None
    };
    // Unauthorized imports are warned about at startup (Go refuses to
    // start instead); at delivery time they are dropped, never leaked.
    for account in config.all_accounts() {
        for import in &account.imports {
            if !config.import_authorized(&account.name, import) {
                eprintln!(
                    "rust-natsd: unauthorized import in account {}: {}:{} -> {} (no matching export; messages through it will be dropped)",
                    account.name, import.source_account, import.remote_subject, import.local_subject
                );
            }
        }
    }
    // Build the TLS acceptor up front so a bad cert/key is a startup
    // error, not a per-connection failure.
    let tls_acceptor = match &config.tls {
        Some(settings) => Some(Arc::new(tokio_rustls::TlsAcceptor::from(
            crate::tls::build_server_config(settings).map_err(io::Error::other)?,
        ))),
        None => None,
    };
    let route_tls_acceptor = match &config.cluster_tls {
        Some(settings) => Some(Arc::new(tokio_rustls::TlsAcceptor::from(
            crate::tls::build_server_config(settings).map_err(io::Error::other)?,
        ))),
        None => None,
    };
    let route_tls_connector = match &config.cluster_tls {
        Some(settings) => Some(Arc::new(tokio_rustls::TlsConnector::from(
            crate::tls::build_client_config(settings).map_err(io::Error::other)?,
        ))),
        None => None,
    };
    let leaf_tls_acceptor = match &config.leaf_tls {
        Some(settings) => Some(Arc::new(tokio_rustls::TlsAcceptor::from(
            crate::tls::build_server_config(settings).map_err(io::Error::other)?,
        ))),
        None => None,
    };
    let mut leaf_remote_connectors = std::collections::HashMap::new();
    for (url, settings) in &config.leaf_remote_tls {
        leaf_remote_connectors.insert(
            url.clone(),
            Arc::new(tokio_rustls::TlsConnector::from(
                crate::tls::build_client_config(settings).map_err(io::Error::other)?,
            )),
        );
    }
    let shared = Arc::new(Shared {
        config,
        listen_addr: addr,
        cluster_addr,
        leaf_addr,
        http_addr,
        tls_acceptor,
        route_tls_acceptor,
        route_tls_connector,
        leaf_tls_acceptor,
        leaf_remote_connectors,
        server_id: gen_server_id(),
        started: Instant::now(),
        started_wall: std::time::SystemTime::now(),
        sublist: RwLock::new(Sublist::new()),
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
        import_replies: Mutex::new(HashMap::new()),
        import_seq: AtomicU64::new(1),
        account_data_bytes: Mutex::new(HashMap::new()),
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
                                // Go's STARTTLS order (verified against
                                // nats-server v2.15.1): INFO (with
                                // tls_required:true) goes out in plaintext
                                // first; the client then upgrades the same
                                // socket to TLS and sends CONNECT inside
                                // it. A direct/implicit-TLS client fails
                                // on its own side (it receives plaintext
                                // INFO where it expected ServerHello),
                                // matching Go. A plain client that sends
                                // non-TLS bytes after INFO is closed
                                // silently, also matching Go. Peek does
                                // not consume, so the handshake still
                                // sees the full ClientHello.
                                use tokio::io::AsyncWriteExt;
                                let nonce = gen_nonce();
                                let info =
                                    client_info(&shared, id, &peer.ip().to_string(), &nonce);
                                let mut stream = stream;
                                if stream
                                    .write_all(&encode_info(&info))
                                    .await
                                    .is_err()
                                {
                                    shared.conn_info.lock().unwrap().remove(&id);
                                    return;
                                }
                                let mut first = [0u8; 1];
                                match stream.peek(&mut first).await {
                                    Ok(0) | Err(_) => {
                                        shared.conn_info.lock().unwrap().remove(&id);
                                        return;
                                    }
                                    Ok(_) => {}
                                }
                                if first[0] != 0x16 {
                                    // Plain client after a TLS INFO:
                                    // Go closes silently, no -ERR.
                                    shared.conn_info.lock().unwrap().remove(&id);
                                    return;
                                }
                                match acceptor.accept(stream).await {
                                    Ok(tls_stream) => {
                                        // Peer certificate identity for
                                        // verify_and_map: the first
                                        // peer certificate's SAN/CN/DN
                                        // candidates, in Go's lookup
                                        // order. No certificate (or an
                                        // unparsable one) means no
                                        // candidates — mapping then
                                        // rejects the connection.
                                        let cert_candidates = tls_stream
                                            .get_ref()
                                            .1
                                            .peer_certificates()
                                            .and_then(|certs| certs.first())
                                            .and_then(|cert| {
                                                crate::tls::parse_cert_identity(cert.as_ref())
                                            })
                                            .map(|id| id.candidates())
                                            .unwrap_or_default();
                                        let (reader, writer) = tokio::io::split(tls_stream);
                                        handle_conn(
                                            reader,
                                            writer,
                                            shared,
                                            id,
                                            peer.ip().to_string(),
                                            nonce,
                                            true,
                                            cert_candidates,
                                        )
                                        .await;
                                    }
                                    Err(e) => {
                                        eprintln!("rust-natsd: TLS handshake failed: {e}");
                                        shared.conn_info.lock().unwrap().remove(&id);
                                    }
                                }
                            }
                            None => {
                                let (reader, writer) = stream.into_split();
                                handle_conn(
                                    reader,
                                    writer,
                                    shared,
                                    id,
                                    peer.ip().to_string(),
                                    gen_nonce(),
                                    false,
                                    Vec::new(),
                                )
                                .await;
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

/// SNI / server name for a TLS route/leaf dial: IP literals use the
/// IP-address form, hostnames the DNS form; loopback hosts use
/// `localhost` (test certificates carry DNS:localhost / IP:127.0.0.1
/// SANs, and Go interop certificates do the same).
fn tls_server_name(host: &str) -> Option<rustls::pki_types::ServerName<'static>> {
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

async fn route_accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    loop {
        tokio::select! {
            _ = shared.shutdown.notified() => break,
            res = listener.accept() => match res {
                Ok((stream, _peer)) => {
                    let _ = stream.set_nodelay(true);
                    let shared = shared.clone();
                    tokio::spawn(async move {
                        match &shared.route_tls_acceptor {
                            // Implicit TLS (Go's probed route order):
                            // the peer's ClientHello comes first; no
                            // plaintext is ever sent on this port.
                            Some(acceptor) => {
                                match acceptor.accept(stream).await {
                                    Ok(tls_stream) => {
                                        let (reader, writer) = tokio::io::split(tls_stream);
                                        handle_route_accepted(reader, writer, shared, true).await;
                                    }
                                    Err(e) => {
                                        eprintln!("rust-natsd: route TLS handshake failed: {e}");
                                    }
                                }
                            }
                            None => {
                                let (reader, writer) = stream.into_split();
                                handle_route_accepted(reader, writer, shared, false).await;
                            }
                        }
                    });
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
                    let shared = shared.clone();
                    tokio::spawn(async move {
                        match &shared.leaf_tls_acceptor {
                            // STARTTLS-like leaf order (Go, probed):
                            // INFO goes out in plaintext first; the
                            // leaf then upgrades the same socket to
                            // TLS and sends CONNECT inside it. A
                            // plain CONNECT after INFO is closed
                            // silently, like Go.
                            Some(acceptor) => {
                                use tokio::io::AsyncWriteExt;
                                let client_id =
                                    shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
                                let mut stream = stream;
                                if stream
                                    .write_all(
                                        format!(
                                            "INFO {}\r\n",
                                            leaf_info_text(&shared, client_id, false)
                                        )
                                        .as_bytes(),
                                    )
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                let mut first = [0u8; 1];
                                match stream.peek(&mut first).await {
                                    Ok(0) | Err(_) => return,
                                    Ok(_) => {}
                                }
                                if first[0] != 0x16 {
                                    return;
                                }
                                match acceptor.accept(stream).await {
                                    Ok(tls_stream) => {
                                        let (reader, writer) = tokio::io::split(tls_stream);
                                        handle_leaf_accepted(
                                            reader, writer, shared, client_id, true, true,
                                        )
                                        .await;
                                    }
                                    Err(e) => {
                                        eprintln!("rust-natsd: leaf TLS handshake failed: {e}");
                                    }
                                }
                            }
                            None => {
                                let (reader, writer) = stream.into_split();
                                let client_id =
                                    shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
                                handle_leaf_accepted(
                                    reader, writer, shared, client_id, false, false,
                                )
                                .await;
                            }
                        }
                    });
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
fn try_send(handle: &ConnHandle, frame: Bytes) -> bool {
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

/// Lifetime of a service-import reply mapping (Phase 28C; Go's
/// `_R_.` mappings expire by TTL).
const IMPORT_REPLY_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// Drop expired import-reply mappings. Called opportunistically on
/// insert and lookup; there is no background sweep task, so expiry
/// is enforced at every use and the map cannot grow without bound
/// on a live server (each new import also prunes the old ones).
fn prune_import_replies(shared: &Arc<Shared>) {
    let mut map = shared.import_replies.lock().unwrap();
    prune_import_map(&mut map, std::time::Instant::now());
}

/// The TTL rule itself, factored out so tests can exercise it with
/// an explicit `now` instead of waiting 60 s (see the unit tests).
fn prune_import_map(
    map: &mut HashMap<String, (String, String, std::time::Instant)>,
    now: std::time::Instant,
) {
    map.retain(|_, (_, _, created)| now.duration_since(*created) < IMPORT_REPLY_TTL);
}

/// Look up a service-import reply mapping, honouring the TTL: an
/// expired entry is removed and treated as unmapped, so a reply
/// arriving after 60 s falls through to the normal publish path
/// instead of being routed to a stale inbox.
fn lookup_import_reply(shared: &Arc<Shared>, subject: &str) -> Option<(String, String)> {
    prune_import_replies(shared);
    shared
        .import_replies
        .lock()
        .unwrap()
        .get(subject)
        .map(|(account, orig, _)| (account.clone(), orig.clone()))
}

/// Add (or replace) a `Nats-Request-Info` header on a translated
/// service-import request. Go v2.15.1 shape, probed: the responder
/// sees `Nats-Request-Info: {"acc":"<importing account>","rtt":<ns>}`
/// where `rtt` is the importing client's measured RTT (omitted by
/// Go when 0). rust-natsd does not measure client RTT, so it emits
/// `{"acc":...}` only — the account identity, which is what
/// responders use, is exact; the RTT is absent, as Go's is when
/// unmeasured.
fn with_request_info(headers: Option<Bytes>, importing_account: &str) -> Option<Bytes> {
    let info = serde_json::json!({"acc": importing_account}).to_string();
    let line = format!("Nats-Request-Info: {info}");
    match headers {
        None => Some(Bytes::from(format!("NATS/1.0\r\n{line}\r\n\r\n"))),
        Some(h) => {
            let text = String::from_utf8_lossy(&h);
            let mut body = text.trim_end_matches(['\r', '\n']).to_string();
            // Drop any pre-existing (possibly forged) Request-Info.
            let mut kept: Vec<&str> = Vec::new();
            for l in body.lines() {
                if l.split(':')
                    .next()
                    .is_some_and(|k| k.eq_ignore_ascii_case("Nats-Request-Info"))
                {
                    continue;
                }
                kept.push(l);
            }
            body = kept.join("\r\n");
            if !body.starts_with("NATS/") {
                body = format!("NATS/1.0\r\n{body}");
            }
            Some(Bytes::from(format!("{body}\r\n{line}\r\n\r\n")))
        }
    }
}

/// Deliver a message: to local subscribers, and (unless it arrived on a
/// route/leaf) forward it to routes and leafs with matching interest.
///
/// Account imports/exports (same node only): a local publish whose
/// subject matches a service import is rewritten into the exporting
/// account instead of being delivered locally, with the reply subject
/// replaced by a synthetic `_RIMPORT.<seq>` subject that routes
/// replies back to the importer's original reply subject; a local
/// publish in an exporting account is additionally copied to local
/// subscribers of every authorized stream import of that subject.
/// Returns true when the message reached at least one subscriber (or
/// was consumed by JetStream), which the no-responders logic uses.
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
) -> bool {
    // JetStream capture / API handling happens before core delivery so a
    // stream message is stored even when nobody is subscribed, and API
    // requests are answered by the hosting node.
    if let Some(js) = &shared.jetstream {
        if crate::jetstream::handle_message(shared, js, msg, origin) {
            // API requests and acks are consumed by JetStream; they are not
            // also delivered as ordinary messages.
            return true;
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
            let mut sl = shared.sublist.write().unwrap();
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
        return !result.deliveries.is_empty();
    }

    // Service import (local publishes only, same node): a publish on
    // the importing account's local subject is rewritten into the
    // exporting account and never delivered locally — the import
    // shadows the local subject, as on Go. An import the exporting
    // account does not authorize (account not listed on the export, or
    // remote pattern not covered by the export) is dropped outright:
    // it must never leak across accounts. Go refuses to start on an
    // unauthorized service import; rust-natsd keeps the server up and
    // drops the message instead (see Config::import_authorized).
    if matches!(origin, Origin::Local(_, _)) && !msg.service_translated && !msg.import_copy {
        if let Some(publisher_account) = shared.config.account(&msg.account).cloned() {
            if let Some((import, remote)) = publisher_account.service_import_for(&msg.subject) {
                if !shared.config.import_authorized(&msg.account, import) {
                    eprintln!(
                        "rust-natsd: dropping unauthorized service import {} -> {}:{}",
                        msg.subject, import.source_account, remote
                    );
                    return false;
                }
                let mut translated = Message::new(
                    &remote,
                    None,
                    with_request_info(msg.headers.clone(), &msg.account),
                    msg.payload.clone(),
                )
                .with_account(&import.source_account)
                .with_no_responders(msg.no_responders);
                translated.service_translated = true;
                if let Some(orig_reply) = &msg.reply_to {
                    let seq = shared.import_seq.fetch_add(1, Ordering::SeqCst);
                    let synthetic = format!("_RIMPORT.{seq}");
                    prune_import_replies(shared);
                    shared.import_replies.lock().unwrap().insert(
                        synthetic.clone(),
                        (
                            msg.account.clone(),
                            orig_reply.clone(),
                            std::time::Instant::now(),
                        ),
                    );
                    translated.reply_to = Some(synthetic);
                }
                let delivered = deliver(shared, &translated, origin, None);
                // No-responders for the translated request: the 503 goes
                // to the importer's original reply subject, not to the
                // synthetic one (nothing subscribes to that in A).
                if msg.no_responders && !delivered {
                    if let Some(orig_reply) = &msg.reply_to {
                        let headers = Bytes::from(format!(
                            "NATS/1.0 503\r\nNats-Subject: {}\r\n\r\n",
                            msg.subject
                        ));
                        deliver(
                            shared,
                            &Message::new(orig_reply, None, Some(headers), Bytes::new())
                                .with_account(&msg.account),
                            Origin::Internal,
                            None,
                        );
                    }
                }
                return delivered;
            }
        }
    }

    // Local / internal origin: global queue selection across local
    // members, routes, and leafs (weighted by member count), so a queue
    // group spanning nodes still delivers exactly once per message.
    let gm = {
        let sl = shared.sublist.read().unwrap();
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
        let mut sl = shared.sublist.write().unwrap();
        sl.note_deliveries(&local_deliveries)
    };
    send_local_frames(shared, msg, &local_deliveries);
    cleanup_expired(shared, &expired);
    shared
        .stats
        .out_msgs
        .fetch_add(local_deliveries.len() as u64, Ordering::Relaxed);

    // No-responders accounting is computed before the route/leaf loops
    // consume route_full/leaf_full: any local delivery, plain route/leaf
    // interest, or selected queue group counts as a responder.
    let mut has_delivery = !local_deliveries.is_empty()
        || route_selected.values().any(|v| !v.is_empty())
        || leaf_selected.values().any(|v| !v.is_empty())
        || route_full.iter().any(|(_, _, plain, _)| *plain)
        || leaf_full.iter().any(|(_, _, plain, _)| *plain);

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

    // Stream imports (local/internal publishes only, same node):
    // copy the message, subject rewritten to the importer's local
    // subject, to that account's local subscribers. The importer's
    // subscribe permission was checked against the local subject at
    // SUB time, and the export must cover this concrete subject and
    // name the importer — otherwise no copy is made. Copies are
    // marked import_copy, so they never translate again.
    if !msg.import_copy {
        let importers: Vec<(String, String)> = shared
            .config
            .all_accounts()
            .filter(|a| a.name != msg.account)
            .filter_map(|a| {
                a.stream_import_for(&msg.account, &msg.subject)
                    .map(|(_, local)| (a.name.clone(), local))
            })
            .collect();
        for (importer, local) in importers {
            if !shared
                .config
                .stream_export_allows(&msg.account, &importer, &msg.subject)
            {
                continue;
            }
            let mut copy = Message::new(
                &local,
                msg.reply_to.clone(),
                msg.headers.clone(),
                msg.payload.clone(),
            )
            .with_account(&importer);
            copy.import_copy = true;
            if deliver_stream_copy(shared, &copy) {
                has_delivery = true;
            }
        }
    }

    // No-responders (Go behaviour, verified against v2.15.1): a local
    // publish with a reply subject and no deliveries, from a connection
    // that set no_responders:true, generates an HMSG to the reply subject
    // with a 503 header block and an empty payload. JetStream-consumed
    // messages return early above, so they never generate one.
    if msg.no_responders && !has_delivery {
        if let Some(reply_to) = &msg.reply_to {
            let headers = Bytes::from(format!(
                "NATS/1.0 503\r\nNats-Subject: {}\r\n\r\n",
                msg.subject
            ));
            deliver(
                shared,
                &Message::new(reply_to, None, Some(headers), Bytes::new())
                    .with_account(&msg.account),
                Origin::Internal,
                None,
            );
        }
    }
    has_delivery
}

/// Deliver a stream-import copy to local subscribers of its (already
/// rewritten) subject/account only — no route/leaf forwarding, no
/// further import translation. Queue groups get exactly one member,
/// like the route/leaf-origin path. Returns true on any delivery.
fn deliver_stream_copy(shared: &Arc<Shared>, msg: &Message) -> bool {
    let (deliveries, expired) = {
        let mut sl = shared.sublist.write().unwrap();
        let gm = sl.match_groups(&msg.subject, u64::MAX, true, Some(&msg.account));
        let mut deliveries = gm.plain;
        for members in gm.groups.values() {
            if !members.is_empty() {
                let cursor = shared.queue_cursor.fetch_add(1, Ordering::SeqCst);
                deliveries.push(members[(cursor as usize) % members.len()].clone());
            }
        }
        let expired = sl.note_deliveries(&deliveries);
        (deliveries, expired)
    };
    send_local_frames(shared, msg, &deliveries);
    cleanup_expired(shared, &expired);
    shared
        .stats
        .out_msgs
        .fetch_add(deliveries.len() as u64, Ordering::Relaxed);
    !deliveries.is_empty()
}

/// Send MSG/HMSG frames for local deliveries, dropping slow consumers.
/// The connection registry is locked once for all targets, not once
/// per message: at burst rates a per-message lock was measurable
/// contention on the publisher thread.
fn send_local_frames(shared: &Arc<Shared>, msg: &Message, deliveries: &[crate::sublist::Delivery]) {
    let targets: Vec<(crate::sublist::Delivery, Arc<ConnHandle>)> = {
        let conns = shared.conns.lock().unwrap();
        deliveries
            .iter()
            .filter_map(|d| conns.get(&d.conn_id).cloned().map(|h| (d.clone(), h)))
            .collect()
    };
    for (d, target) in targets {
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
            shared.stats.slow_consumers.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "rust-natsd: slow consumer, dropping connection {}",
                d.conn_id
            );
            shared.sublist.write().unwrap().remove_conn(d.conn_id);
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
        let sl = shared.sublist.read().unwrap();
        expired
            .iter()
            .filter_map(|(cid, sid)| sl.pattern_of(*cid, sid))
            .collect()
    };
    {
        let mut sl = shared.sublist.write().unwrap();
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

/// Whether any local subscription, route, or leaf has interest in
/// `subject` for `account`. Used by the JetStream tick to defer a push
/// redelivery while nobody is listening on the deliver subject, so it
/// does not burn a delivery cycle on a dropped message. Read-only: it
/// uses `match_groups`, which does not update delivery counters.
pub(crate) fn has_interest(shared: &Arc<Shared>, account: &str, subject: &str) -> bool {
    {
        let sl = shared.sublist.read().unwrap();
        let gm = sl.match_groups(subject, u64::MAX, true, Some(account));
        if !gm.plain.is_empty() || !gm.groups.is_empty() {
            return true;
        }
    }
    for subs in shared.route_subs.lock().unwrap().values() {
        let (plain, counts) = interest_of(subs, subject, account);
        if plain || !counts.is_empty() {
            return true;
        }
    }
    for subs in shared.leaf_subs.lock().unwrap().values() {
        let (plain, counts) = interest_of(subs, subject, account);
        if plain || !counts.is_empty() {
            return true;
        }
    }
    false
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
    // Collect handles first and send after the locks drop, mirroring
    // send_local_frames: the registry locks are never held across the
    // (non-blocking channel) sends.
    let route_handles: Vec<Arc<ConnHandle>> = shared
        .routes
        .lock()
        .unwrap()
        .values()
        .filter(|r| !r.handle.is_closed())
        .map(|r| r.handle.clone())
        .collect();
    for handle in route_handles {
        let _ = try_send(&handle, rframe.clone());
    }
    // Leaf frames carry no account token: a leaf connection only ever
    // carries its own account, so propagate only to matching leafs.
    let lframe = if added {
        encode_ls_plus(subject, queue)
    } else {
        encode_ls_minus(subject, queue)
    };
    let leaf_handles: Vec<Arc<ConnHandle>> = shared
        .leafs
        .lock()
        .unwrap()
        .values()
        .filter(|l| !l.handle.is_closed() && l.account == account)
        .map(|l| l.handle.clone())
        .collect();
    for handle in leaf_handles {
        let _ = try_send(&handle, lframe.clone());
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
        .read()
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
        let sl = shared.sublist.read().unwrap();
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
fn client_info(shared: &Shared, client_id: u64, peer_ip: &str, nonce: &str) -> ServerInfo {
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
async fn handle_conn<R, W>(
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

fn json_bool(json: &str, field: &str) -> Option<bool> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()?
        .get(field)?
        .as_bool()
}

/// Read one CRLF-terminated line from a buffered reader. Generic over
/// the read half so plain TCP and TLS streams share the route/leaf
/// handshake and read loops.
async fn read_line<R: AsyncReadExt + Unpin>(reader: &mut BufReader<R>) -> io::Result<String> {
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
        shared.config.cluster_tls.is_some(),
    )
}

async fn handle_route_accepted<R, W>(reader: R, writer: W, shared: Arc<Shared>, tls: bool)
where
    R: AsyncReadExt + Unpin + Send,
    W: AsyncWriteExt + Unpin + Send + 'static,
{
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
            tls,
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
                match &shared.route_tls_connector {
                    // Implicit TLS (Go's probed route order): the
                    // ClientHello goes first, inside which CONNECT
                    // and INFO then flow.
                    Some(connector) => {
                        let Some(name) = tls_server_name(&host) else {
                            eprintln!("rust-natsd: invalid route TLS server name: {host}");
                            continue;
                        };
                        match connector.connect(name, stream).await {
                            Ok(tls_stream) => {
                                let (reader, writer) = tokio::io::split(tls_stream);
                                handle_route_solicited(
                                    reader,
                                    writer,
                                    shared.clone(),
                                    target.clone(),
                                    true,
                                )
                                .await;
                            }
                            Err(e) => {
                                eprintln!("rust-natsd: route TLS handshake failed: {e}");
                            }
                        }
                    }
                    None => {
                        let (reader, writer) = stream.into_split();
                        handle_route_solicited(
                            reader,
                            writer,
                            shared.clone(),
                            target.clone(),
                            false,
                        )
                        .await;
                    }
                }
            }
            Err(_) => {
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, Duration::from_secs(5));
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn handle_route_solicited<R, W>(
    reader: R,
    writer: W,
    shared: Arc<Shared>,
    target: String,
    tls: bool,
) where
    R: AsyncReadExt + Unpin + Send,
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    let mut reader = BufReader::new(reader);
    let (handle, tx) = spawn_writer(writer, shared.clone());
    // Dialer sends CONNECT first, then reads peer INFO, then sends INFO.
    let connect = route_connect_json(
        &shared.server_id,
        &shared.config.effective_cluster_name(),
        tls,
    );
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
            tls,
        },
    );
    send_local_interest(&shared, &handle, true, None);
    route_read_loop(&mut reader, &shared, id, &handle, &tx).await;
    close_route(&shared, id);
}

async fn route_read_loop<R: AsyncReadExt + Unpin>(
    reader: &mut BufReader<R>,
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

fn leaf_info_text(shared: &Arc<Shared>, client_id: u64, connect_info: bool) -> String {
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

async fn handle_leaf_accepted<R, W>(
    reader: R,
    writer: W,
    shared: Arc<Shared>,
    client_id: u64,
    info_sent: bool,
    tls: bool,
) where
    R: AsyncReadExt + Unpin + Send,
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    let mut reader = BufReader::new(reader);
    let (handle, tx) = spawn_writer(writer, shared.clone());
    // Hub sends INFO first — unless the accept loop already sent it
    // in plaintext for a TLS upgrade (see leaf_accept_loop).
    if !info_sent
        && tx
            .send(Bytes::from(format!(
                "INFO {}\r\n",
                leaf_info_text(&shared, client_id, false)
            )))
            .await
            .is_err()
    {
        return;
    }
    // Then expects leaf CONNECT, optionally preceded by leaf INFO (Go
    // sends its INFO when the hub INFO sets info_on_connect, which
    // ours does for TLS hubs; plain hubs get CONNECT straight away).
    // When the leaf does send INFO, the hub replies with its own INFO
    // (with connect_info: true) on the same connection before
    // reading CONNECT: a Go hub does this (captured via a
    // TLS-intercepting proxy against Go nats-server v2.15.1). A Go
    // leaf arms a 2s timer when it sends CONNECT and only cancels
    // it on a hub INFO with connect_info: true; without one it
    // closes the connection with
    // `-ERR 'Stale Leaf Node Connection - Closing'`. The reply is
    // sent on the same stream the leaf INFO arrived on, so for TLS
    // hubs it is inside TLS.
    let first_line = match read_line(&mut reader).await {
        Ok(l) => l,
        Err(_) => return,
    };
    let (peer_id, connect_json, sent_connect_info) =
        if let Some(info_json) = first_line.strip_prefix("INFO ") {
            let peer_id = json_str(info_json, "server_id").unwrap_or_default();
            if tx
                .send(Bytes::from(format!(
                    "INFO {}\r\n",
                    leaf_info_text(&shared, client_id, true)
                )))
                .await
                .is_err()
            {
                return;
            }
            let connect_line = match read_line(&mut reader).await {
                Ok(l) => l,
                Err(_) => return,
            };
            let Some(connect_json) = connect_line.strip_prefix("CONNECT ") else {
                return;
            };
            (peer_id, connect_json.to_string(), true)
        } else if let Some(connect_json) = first_line.strip_prefix("CONNECT ") {
            let peer_id = json_str(connect_json, "server_id").unwrap_or_default();
            (peer_id, connect_json.to_string(), false)
        } else {
            return;
        };
    // A Go leaf that sent CONNECT straight away (it does this when
    // the hub's pre-TLS INFO differs from Go's in the field that
    // triggers leaf-INFO-first) still waits for a hub INFO with
    // connect_info: true to cancel its 2s connect timer — but only
    // when the first INFO set info_on_connect, which ours does for
    // TLS hubs (info_sent). Plain hubs set no info_on_connect, so
    // the leaf already finished on the first INFO and gets nothing
    // extra here.
    if info_sent
        && !sent_connect_info
        && tx
            .send(Bytes::from(format!(
                "INFO {}\r\n",
                leaf_info_text(&shared, client_id, true)
            )))
            .await
            .is_err()
    {
        return;
    }
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
            tls,
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
                // Hub INFO first (plaintext, even for TLS hubs).
                // Read it on the unsplit socket so a TLS upgrade
                // afterwards cannot lose buffered bytes: after its
                // INFO the hub sends nothing until the leaf upgrades
                // (TLS) or sends INFO+CONNECT (plain), so the read
                // buffer must be empty here.
                let mut pre = BufReader::new(stream);
                let info_line = match read_line(&mut pre).await {
                    Ok(l) => l,
                    Err(_) => continue,
                };
                let Some(hub_info) = info_line.strip_prefix("INFO ") else {
                    continue;
                };
                let hub_tls = json_bool(hub_info, "tls_required").unwrap_or(false);
                if hub_tls {
                    let Some(connector) = shared.leaf_remote_connectors.get(&url).cloned() else {
                        eprintln!(
                            "rust-natsd: leaf hub {url} requires TLS but no tls block is configured for this remote"
                        );
                        continue;
                    };
                    if !pre.buffer().is_empty() {
                        continue;
                    }
                    let tcp = pre.into_inner();
                    let Some(name) = tls_server_name(&host) else {
                        eprintln!("rust-natsd: invalid leaf TLS server name: {host}");
                        continue;
                    };
                    match connector.connect(name, tcp).await {
                        Ok(tls_stream) => {
                            let (reader, writer) = tokio::io::split(tls_stream);
                            handle_leaf_solicited_after_info(reader, writer, shared.clone(), true)
                                .await;
                        }
                        Err(e) => {
                            eprintln!("rust-natsd: leaf TLS handshake failed: {e}");
                        }
                    }
                } else {
                    let tcp = pre.into_inner();
                    let (reader, writer) = tcp.into_split();
                    handle_leaf_solicited_after_info(reader, writer, shared.clone(), false).await;
                }
            }
            Err(_) => {
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, Duration::from_secs(5));
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Solicited leaf after the hub INFO has been consumed: the leaf sends
/// its INFO + CONNECT (inside TLS when `tls` is true), then runs the
/// leaf read loop.
async fn handle_leaf_solicited_after_info<R, W>(
    reader: R,
    writer: W,
    shared: Arc<Shared>,
    tls: bool,
) where
    R: AsyncReadExt + Unpin + Send,
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    let mut reader = BufReader::new(reader);
    let (handle, tx) = spawn_writer(writer, shared.clone());
    let client_id = shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
    if tx
        .send(Bytes::from(format!(
            "INFO {}\r\n",
            leaf_info_text(&shared, client_id, false)
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
            tls,
        },
    );
    send_local_interest(&shared, &handle, false, Some(&leaf_account));
    leaf_read_loop(&mut reader, &shared, id, &handle, &tx).await;
    close_leaf(&shared, id);
}

async fn leaf_read_loop<R: AsyncReadExt + Unpin>(
    reader: &mut BufReader<R>,
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

    #[test]
    fn import_reply_prune_drops_expired_only() {
        // Phase 28C: entries older than 60 s expire; fresh ones stay.
        // Exercised with an explicit `now` so the test never waits.
        let now = std::time::Instant::now();
        let mut map = HashMap::new();
        map.insert(
            "_RIMPORT.1".to_string(),
            (
                "B".to_string(),
                "_INBOX.x".to_string(),
                now - std::time::Duration::from_secs(61),
            ),
        );
        map.insert(
            "_RIMPORT.2".to_string(),
            ("B".to_string(), "_INBOX.y".to_string(), now),
        );
        prune_import_map(&mut map, now);
        assert!(!map.contains_key("_RIMPORT.1"), "expired entry must go");
        assert!(map.contains_key("_RIMPORT.2"), "fresh entry must stay");
    }

    #[test]
    fn request_info_injected_and_forged_replaced() {
        // No prior headers: a fresh header block is created.
        let h = with_request_info(None, "B").unwrap();
        let text = String::from_utf8_lossy(&h);
        assert_eq!(
            text, "NATS/1.0\r\nNats-Request-Info: {\"acc\":\"B\"}\r\n\r\n",
            "fresh header block: {text}"
        );
        // Existing headers are preserved, a forged Request-Info is
        // replaced (exactly one survives, with the real account).
        let prior = Bytes::from(
            "NATS/1.0\r\nNats-Msg-Id: abc\r\nNats-Request-Info: {\"acc\":\"E\"}\r\n\r\n",
        );
        let h = with_request_info(Some(prior), "B").unwrap();
        let text = String::from_utf8_lossy(&h);
        assert!(
            text.contains("Nats-Msg-Id: abc"),
            "other header kept: {text}"
        );
        assert!(
            text.contains("Nats-Request-Info: {\"acc\":\"B\"}"),
            "real account wins: {text}"
        );
        assert!(
            !text.contains("\"acc\":\"E\""),
            "forged account must not survive: {text}"
        );
        assert_eq!(
            text.matches("Nats-Request-Info").count(),
            1,
            "exactly one Request-Info line: {text}"
        );
    }
}
