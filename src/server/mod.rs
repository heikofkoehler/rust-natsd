//! Server core: listener loops, lifecycle/shutdown, and the shared
//! server state (`Shared`). Connection handling lives in `conn`
//! (client connections, writer, CONNECT/PING/PONG state machine, and
//! route/leaf handshake + solicitation), message delivery in
//! `delivery` (including inbound route/leaf frame processing, which
//! feeds `deliver` and interest propagation directly), and account
//! import/export translation in `imports`.
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

mod conn;
mod delivery;
mod imports;

pub use self::conn::{ConnInfo, Stats};
pub(crate) use self::delivery::has_interest;
pub use self::delivery::{deliver, Message, Origin};

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Notify};
use tokio::task::JoinSet;

use crate::config::{parse_route_url, Config};
use crate::leaf::leaf_connect_json;
use crate::protocol::{encode_err, encode_info};
use crate::route::route_connect_json;
use crate::sublist::Sublist;

use self::conn::{
    client_info, gen_nonce, handle_conn, leaf_account_for, leaf_connect_authorized, leaf_info_text,
    peer_listen_addr, route_info_text, spawn_writer, tls_server_name, ConnHandle, GatewayConn,
    LeafConn, RouteConn,
};
use self::delivery::{
    close_gateway, close_leaf, close_route, gateway_read_loop, leaf_read_loop, route_read_loop,
    send_gateway_interest, send_local_interest,
};
use self::delivery::{json_bool, json_str, json_u64, read_line};
/// One inter-server connection's interest: (account, pattern, queue)
/// -> count. Routes are multi-account; leaf interest is stored the
/// same way but a leaf connection only ever carries its own account.
pub(crate) type InterestMap = HashMap<(String, String, Option<String>), u32>;

pub struct Shared {
    pub(crate) config: Config,
    pub(crate) listen_addr: SocketAddr,
    pub(crate) cluster_addr: Option<SocketAddr>,
    pub(crate) leaf_addr: Option<SocketAddr>,
    pub(crate) gateway_addr: Option<SocketAddr>,
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
    pub(crate) gateways: Mutex<HashMap<u64, Arc<GatewayConn>>>,
    pub(crate) gateway_subs: Mutex<HashMap<u64, InterestMap>>,
    /// URLs of implicit solicitation loops already spawned (by
    /// gateway name + url), so one peer INFO cannot spawn duplicate
    /// loops. Explicit solicitations are spawned once at startup and
    /// are not tracked here.
    pub(crate) gateway_implicit: Mutex<std::collections::HashSet<String>>,
    /// TLS acceptor for gateway connections (implicit TLS); None
    /// means plain text on the gateway port.
    pub(crate) gateway_tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
    /// TLS connector from the gateway listener's TLS settings, used
    /// for implicit solicitations and for explicit remotes without
    /// their own tls block (Go's implicit solicitations inherit the
    /// local gateway TLS settings, probed in Phase 33).
    pub(crate) gateway_tls_connector: Option<std::sync::Arc<tokio_rustls::TlsConnector>>,
    /// TLS connectors for solicited gateway remotes, keyed by the
    /// remote URL string as configured.
    pub(crate) gateway_remote_connectors:
        std::collections::HashMap<String, std::sync::Arc<tokio_rustls::TlsConnector>>,
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
    /// (`_RIMPORT.<originating server id>.<seq>` since Phase 32,
    /// handed to the exporting account's responder)
    /// -> (importing account, original reply subject, creation
    /// instant). Replies published to a synthetic subject are
    /// delivered back into the importing account (see process_op).
    /// Entries expire after 60 s (Phase 28C; Go's `_R_.` mappings
    /// expire by TTL, and unbounded retention was a known
    /// limitation) and are pruned opportunistically on insert and
    /// lookup, so a stale mapping can never route a reply.
    pub(crate) import_replies: Mutex<HashMap<String, (String, String, std::time::Instant)>>,
    /// Gateway reply-subject mappings: a reply subject carried by a
    /// message that arrived on a gateway connection -> (that
    /// connection's id, arrival instant). Go gateways do not
    /// propagate `_INBOX` subscriptions as RS+; instead, when a
    /// request crosses a gateway, the receiving side remembers its
    /// reply subject so the reply can flow back over the same
    /// connection without interest (the shadow-subscription
    /// mechanism). Entries expire after 60 s (same TTL shape as the
    /// service-import reply mappings) and are pruned on insert and
    /// lookup, so a stale mapping can never route a reply.
    pub(crate) gateway_replies: Mutex<HashMap<String, (u64, std::time::Instant)>>,
    /// Counter for synthetic service-import reply subjects.
    pub(crate) import_seq: AtomicU64,
    /// Account-wide cumulative inbound payload bytes, for JWT
    /// account `limits.data` enforcement (Phase 28C). Only accounts
    /// with a data limit are tracked here; see `ConnState`.
    pub(crate) account_data_bytes: Mutex<HashMap<String, u64>>,
}

/// Targets for one cross-node fan-out: (conn id, writer, has plain
/// interest, queue-group member counts).
pub(crate) type InterestTargets = Vec<(u64, Arc<ConnHandle>, bool, HashMap<String, u32>)>;

/// A running server instance; used by `main` and by tests.
pub struct RunningServer {
    pub addr: SocketAddr,
    pub cluster_addr: Option<SocketAddr>,
    pub leaf_addr: Option<SocketAddr>,
    pub gateway_addr: Option<SocketAddr>,
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
    // Gateway listener: only when a gateway name and port are
    // configured. A `gateway {}` block with no port binds nothing —
    // it must not silently swallow the section (Phase 24's ignored
    // section is gone once a listener exists; a portless block is
    // still inert, but only because there is nothing to listen on).
    let gateway_listener = match &config.gateway {
        Some(gw) if gw.port != 0 => {
            let host = if gw.host.is_empty() {
                config.host.as_str()
            } else {
                gw.host.as_str()
            };
            Some(TcpListener::bind((host, gw.port)).await?)
        }
        _ => None,
    };
    let gateway_addr = gateway_listener
        .as_ref()
        .map(|l| l.local_addr())
        .transpose()?;
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
    // Gateway TLS (implicit, probed): the listener's TLS settings
    // build both an acceptor and a client connector (for implicit
    // solicitations); each remote's own tls block builds its own
    // connector keyed by the remote URL string.
    let gateway_tls_acceptor = match config.gateway.as_ref().and_then(|g| g.tls.as_ref()) {
        Some(settings) => Some(Arc::new(tokio_rustls::TlsAcceptor::from(
            crate::tls::build_server_config(settings).map_err(io::Error::other)?,
        ))),
        None => None,
    };
    // The listener connector is only built when a CA is configured
    // (client-config verification needs roots); a cert-only listener
    // still accepts TLS connections, it just cannot solicit TLS peers
    // without roots — the solicitation then falls back to plain TCP
    // and fails at the peer's TLS handshake, as a plain dialer would.
    let gateway_tls_connector = match config.gateway.as_ref().and_then(|g| g.tls.as_ref()) {
        Some(settings) if settings.ca_file.is_some() => {
            Some(Arc::new(tokio_rustls::TlsConnector::from(
                crate::tls::build_client_config(settings).map_err(io::Error::other)?,
            )))
        }
        _ => None,
    };
    let mut gateway_remote_connectors = std::collections::HashMap::new();
    if let Some(gw) = &config.gateway {
        for remote in &gw.remotes {
            if let Some(settings) = &remote.tls {
                for url in &remote.urls {
                    gateway_remote_connectors.insert(
                        url.clone(),
                        Arc::new(tokio_rustls::TlsConnector::from(
                            crate::tls::build_client_config(settings).map_err(io::Error::other)?,
                        )),
                    );
                }
            }
        }
    }
    let shared = Arc::new(Shared {
        config,
        listen_addr: addr,
        cluster_addr,
        leaf_addr,
        gateway_addr,
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
        gateways: Mutex::new(HashMap::new()),
        gateway_subs: Mutex::new(HashMap::new()),
        gateway_implicit: Mutex::new(std::collections::HashSet::new()),
        gateway_tls_acceptor,
        gateway_tls_connector,
        gateway_remote_connectors,
        next_conn_id: AtomicU64::new(1),
        queue_cursor: AtomicU64::new(0),
        stats: Stats::default(),
        shutdown: Arc::new(Notify::new()),
        shutdown_flag: AtomicBool::new(false),
        jetstream,
        import_replies: Mutex::new(HashMap::new()),
        gateway_replies: Mutex::new(HashMap::new()),
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
        gateway_listener,
        http_listener,
        shared,
        shutdown_rx,
    ));
    Ok(RunningServer {
        addr,
        cluster_addr,
        leaf_addr,
        gateway_addr,
        http_addr,
        shutdown_tx,
        task,
    })
}

async fn supervisor(
    listener: TcpListener,
    cluster_listener: Option<TcpListener>,
    leaf_listener: Option<TcpListener>,
    gateway_listener: Option<TcpListener>,
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
    if let Some(l) = gateway_listener {
        tasks.spawn(gateway_accept_loop(l, shared.clone()));
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
    // Explicitly configured gateways, each URL with its own retry loop.
    if let Some(gw) = shared.config.gateway.clone() {
        for remote in gw.remotes {
            for url in remote.urls {
                tasks.spawn(solicit_gateway_loop(
                    url,
                    remote.name.clone(),
                    true,
                    shared.clone(),
                ));
            }
        }
    }
    if let Some(js) = &shared.jetstream {
        tasks.spawn(crate::jetstream::redelivery_loop(
            js.clone(),
            shared.clone(),
        ));
        // Phase 32: cross-node mirror/source pull loop (no-op on
        // nodes with no remote links; see jetstream/mirror.rs).
        tasks.spawn(crate::jetstream::mirror_loop(js.clone(), shared.clone()));
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
    send_local_interest(&shared, &handle, id, true, None);
    route_read_loop(&mut reader, &shared, id, &handle, &tx).await;
    close_route(&shared, id);
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
    send_local_interest(&shared, &handle, id, true, None);
    route_read_loop(&mut reader, &shared, id, &handle, &tx).await;
    close_route(&shared, id);
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
    send_local_interest(&shared, &handle, id, false, Some(&leaf_account));
    leaf_read_loop(&mut reader, &shared, id, &handle, &tx).await;
    close_leaf(&shared, id);
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
    send_local_interest(&shared, &handle, id, false, Some(&leaf_account));
    leaf_read_loop(&mut reader, &shared, id, &handle, &tx).await;
    close_leaf(&shared, id);
}

/// Gateway INFO text for this server: the actual gateway listener
/// address, with the advertised host taken from the gateway block's
/// host when it names a real interface (Go advertises its gateway
/// listener address; a 0.0.0.0 bind advertises 127.0.0.1 here, a
/// documented simplification of Go's external-IP detection).
pub(crate) fn gateway_info_text(shared: &Arc<Shared>) -> String {
    let gw = shared.config.gateway.as_ref();
    let (host, port) = match shared.gateway_addr {
        Some(a) => (a.ip().to_string(), a.port()),
        None => (
            gw.map(|g| g.host.clone()).unwrap_or_default(),
            gw.map(|g| g.port).unwrap_or(0),
        ),
    };
    let advertised = match gw {
        Some(g) if !g.host.is_empty() && g.host != "0.0.0.0" => g.host.clone(),
        _ if host != "0.0.0.0" => host.clone(),
        _ => "127.0.0.1".to_string(),
    };
    crate::gateway::gateway_info_json(
        &shared.server_id,
        &shared.config.name,
        &host,
        port,
        shared.config.max_payload,
        gw.map(|g| g.name.as_str()).unwrap_or(""),
        &[format!("{advertised}:{port}")],
    )
}

/// Spawn implicit solicitation loops for a peer's advertised gateway
/// URLs, deduped against explicitly configured gateways (by name and
/// by URL), existing outbound connections (by peer gateway name),
/// and loops already spawned for this peer. One connection per
/// direction is the goal: an inbound connection from the peer never
/// suppresses the outbound back-connect, and vice versa.
pub(crate) fn maybe_solicit_implicit(
    shared: &Arc<Shared>,
    peer_gateway: &str,
    peer_urls: &[String],
) {
    if peer_gateway.is_empty() {
        return;
    }
    let explicit_names: Vec<String> = shared
        .config
        .gateway
        .as_ref()
        .map(|g| g.remotes.iter().map(|r| r.name.clone()).collect())
        .unwrap_or_default();
    if explicit_names.iter().any(|n| n == peer_gateway) {
        return;
    }
    let explicit_urls: Vec<String> = shared
        .config
        .gateway
        .as_ref()
        .map(|g| g.remotes.iter().flat_map(|r| r.urls.clone()).collect())
        .unwrap_or_default();
    let has_outbound = shared
        .gateways
        .lock()
        .unwrap()
        .values()
        .any(|g| g.outbound && g.peer_gateway == peer_gateway);
    if has_outbound {
        return;
    }
    for url in peer_urls {
        let Some((host, port)) = crate::gateway::parse_gateway_url(url) else {
            continue;
        };
        let target = format!("{host}:{port}");
        if explicit_urls.iter().any(|u| {
            crate::gateway::parse_gateway_url(u)
                .map(|(h, p)| format!("{h}:{p}"))
                .as_deref()
                == Some(target.as_str())
        }) {
            continue;
        }
        let key = format!("{peer_gateway}@{target}");
        let fresh = shared.gateway_implicit.lock().unwrap().insert(key);
        if fresh {
            tokio::spawn(solicit_gateway_loop(
                url.clone(),
                peer_gateway.to_string(),
                false,
                shared.clone(),
            ));
        }
    }
}

/// Register a gateway connection, resolving duplicates: a second
/// inbound connection from the same peer server id, or a second
/// outbound connection to the same peer gateway name, loses.
fn register_gateway(
    shared: &Arc<Shared>,
    id: u64,
    peer_gateway: String,
    peer_id: Option<String>,
    outbound: bool,
    configured: bool,
    handle: Arc<ConnHandle>,
) -> bool {
    let mut gateways = shared.gateways.lock().unwrap();
    for g in gateways.values() {
        if outbound {
            if g.outbound && g.peer_gateway == peer_gateway {
                return false;
            }
        } else if !g.outbound {
            let same = peer_id
                .as_ref()
                .is_some_and(|pid| g.peer_id.lock().unwrap().as_deref() == Some(pid.as_str()));
            if same {
                return false;
            }
        }
    }
    gateways.insert(
        id,
        Arc::new(GatewayConn {
            handle,
            peer_gateway,
            peer_id: Mutex::new(peer_id),
            outbound,
            configured,
            announced_accounts: Mutex::new(Vec::new()),
            interest_only_accounts: Mutex::new(Vec::new()),
        }),
    );
    shared
        .gateway_subs
        .lock()
        .unwrap()
        .insert(id, HashMap::new());
    true
}

async fn gateway_accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    loop {
        tokio::select! {
            _ = shared.shutdown.notified() => break,
            res = listener.accept() => match res {
                Ok((stream, peer)) => {
                    let _ = stream.set_nodelay(true);
                    let shared = shared.clone();
                    tokio::spawn(async move {
                        match &shared.gateway_tls_acceptor {
                            // Implicit TLS (Go's probed gateway
                            // order): the peer's ClientHello comes
                            // first; no plaintext is ever sent.
                            Some(acceptor) => {
                                match acceptor.accept(stream).await {
                                    Ok(tls_stream) => {
                                        let (reader, writer) = tokio::io::split(tls_stream);
                                        handle_gateway_accepted(reader, writer, shared, peer.to_string(), true).await;
                                    }
                                    Err(e) => {
                                        eprintln!("rust-natsd: gateway TLS handshake failed: {e}");
                                    }
                                }
                            }
                            None => {
                                let (reader, writer) = stream.into_split();
                                handle_gateway_accepted(reader, writer, shared, peer.to_string(), false).await;
                            }
                        }
                    });
                }
                Err(e) => eprintln!("rust-natsd: gateway accept error: {e}"),
            },
        }
    }
}

async fn handle_gateway_accepted<R, W>(
    reader: R,
    writer: W,
    shared: Arc<Shared>,
    peer_addr: String,
    tls: bool,
) where
    R: AsyncReadExt + Unpin + Send,
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    use self::delivery::{json_str, read_line};
    let mut reader = BufReader::new(reader);
    let (handle, tx) = spawn_writer(writer, shared.clone());
    // Accepter sends INFO first (gateway order, unlike routes).
    if tx
        .send(Bytes::from(format!(
            "INFO {}\r\n",
            gateway_info_text(&shared)
        )))
        .await
        .is_err()
    {
        return;
    }
    // Then expects CONNECT, then INFO from the dialer.
    let connect_line = match read_line(&mut reader).await {
        Ok(l) => l,
        Err(_) => return,
    };
    let Some(connect_json) = connect_line.strip_prefix("CONNECT ") else {
        return;
    };
    let dialer_gateway = json_str(connect_json, "gateway").unwrap_or_default();
    if dialer_gateway.is_empty() {
        let _ = tx.send(encode_err("Authorization Violation")).await;
        return;
    }
    let info_line = match read_line(&mut reader).await {
        Ok(l) => l,
        Err(_) => return,
    };
    let Some(info_json) = info_line.strip_prefix("INFO ") else {
        return;
    };
    let Some(info) = crate::gateway::parse_gateway_info(info_json) else {
        return;
    };
    let peer_id = info.server_id.clone();
    let id = shared.next_conn_id.fetch_add(1, Ordering::SeqCst);
    shared
        .stats
        .total_connections
        .fetch_add(1, Ordering::Relaxed);
    if !register_gateway(
        &shared,
        id,
        dialer_gateway.clone(),
        peer_id.clone(),
        false,
        false,
        handle.clone(),
    ) {
        handle.mark_closed();
        return;
    }
    shared.conn_info.lock().unwrap().insert(
        id,
        ConnInfo {
            id,
            kind: "gateway",
            addr: peer_addr,
            peer_id: peer_id.clone(),
            started: Instant::now(),
            account: "$G".to_string(),
            user: None,
            tls,
        },
    );
    // Accepter's account announcements (cmd 2), then its local
    // interest, then interest-only markers (cmd 3) — the probed
    // Go ordering (cmd 2 per account, RS+ burst, cmd 3 per account).
    let accounts = crate::gateway::gateway_accounts(&shared.config);
    let my_name = shared
        .config
        .gateway
        .as_ref()
        .map(|g| g.name.clone())
        .unwrap_or_default();
    for account in &accounts {
        let _ = tx
            .send(Bytes::from(format!(
                "INFO {}\r\n",
                crate::gateway::gateway_cmd_info_json(&my_name, 2, account)
            )))
            .await;
    }
    send_gateway_interest(&shared, &handle);
    for account in &accounts {
        let _ = tx
            .send(Bytes::from(format!(
                "INFO {}\r\n",
                crate::gateway::gateway_cmd_info_json(&my_name, 3, account)
            )))
            .await;
    }
    // Implicit back-connect from the dialer's advertised URLs.
    maybe_solicit_implicit(&shared, &dialer_gateway, &info.gateway_urls);
    gateway_read_loop(&mut reader, &shared, id, &handle, &tx).await;
    close_gateway(&shared, id);
}

async fn solicit_gateway_loop(
    url: String,
    peer_gateway: String,
    configured: bool,
    shared: Arc<Shared>,
) {
    let Some((host, port)) = crate::gateway::parse_gateway_url(&url) else {
        eprintln!("rust-natsd: invalid gateway url: {url}");
        return;
    };
    let target = format!("{host}:{port}");
    let mut backoff = Duration::from_secs(1);
    loop {
        if shared.shutdown_flag.load(Ordering::SeqCst) {
            break;
        }
        // Skip dialing when we already have an outbound connection
        // to this peer gateway (inbound connections do not count:
        // one connection per direction).
        let already = shared
            .gateways
            .lock()
            .unwrap()
            .values()
            .any(|g| g.outbound && g.peer_gateway == peer_gateway);
        if already {
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        match TcpStream::connect((host.as_str(), port)).await {
            Ok(stream) => {
                backoff = Duration::from_secs(1);
                let _ = stream.set_nodelay(true);
                // Connector precedence: the remote's own tls block,
                // else the local gateway listener's TLS settings
                // (Go's implicit solicitations inherit those).
                let connector = shared
                    .gateway_remote_connectors
                    .get(&url)
                    .cloned()
                    .or_else(|| shared.gateway_tls_connector.clone());
                match connector {
                    Some(connector) => {
                        let Some(name) = tls_server_name(&host) else {
                            eprintln!("rust-natsd: invalid gateway TLS server name: {host}");
                            continue;
                        };
                        match connector.connect(name, stream).await {
                            Ok(tls_stream) => {
                                let (reader, writer) = tokio::io::split(tls_stream);
                                handle_gateway_solicited(
                                    reader,
                                    writer,
                                    shared.clone(),
                                    peer_gateway.clone(),
                                    target.clone(),
                                    configured,
                                    true,
                                )
                                .await;
                            }
                            Err(e) => {
                                eprintln!("rust-natsd: gateway TLS handshake failed: {e}");
                            }
                        }
                    }
                    None => {
                        let (reader, writer) = stream.into_split();
                        handle_gateway_solicited(
                            reader,
                            writer,
                            shared.clone(),
                            peer_gateway.clone(),
                            target.clone(),
                            configured,
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

async fn handle_gateway_solicited<R, W>(
    reader: R,
    writer: W,
    shared: Arc<Shared>,
    peer_gateway: String,
    target: String,
    configured: bool,
    tls: bool,
) where
    R: AsyncReadExt + Unpin + Send,
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    use self::delivery::read_line;
    let mut reader = BufReader::new(reader);
    let (handle, tx) = spawn_writer(writer, shared.clone());
    // Accepter INFO first; it must name the gateway we solicited.
    let info_line = match read_line(&mut reader).await {
        Ok(l) => l,
        Err(_) => return,
    };
    let Some(info_json) = info_line.strip_prefix("INFO ") else {
        return;
    };
    let Some(info) = crate::gateway::parse_gateway_info(info_json) else {
        return;
    };
    if info.gateway.as_deref() != Some(peer_gateway.as_str()) {
        return;
    }
    let peer_id = info.server_id.clone();
    // Dialer sends CONNECT (name = our server id, gateway = our
    // gateway name) followed immediately by its own INFO.
    let my_name = shared
        .config
        .gateway
        .as_ref()
        .map(|g| g.name.clone())
        .unwrap_or_default();
    let connect = crate::gateway::gateway_connect_json(&shared.server_id, &my_name, tls);
    if tx
        .send(Bytes::from(format!("CONNECT {connect}\r\n")))
        .await
        .is_err()
    {
        return;
    }
    if tx
        .send(Bytes::from(format!(
            "INFO {}\r\n",
            gateway_info_text(&shared)
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
    if !register_gateway(
        &shared,
        id,
        peer_gateway.clone(),
        peer_id.clone(),
        true,
        configured,
        handle.clone(),
    ) {
        handle.mark_closed();
        return;
    }
    shared.conn_info.lock().unwrap().insert(
        id,
        ConnInfo {
            id,
            kind: "gateway",
            addr: target,
            peer_id: peer_id.clone(),
            started: Instant::now(),
            account: "$G".to_string(),
            user: None,
            tls,
        },
    );
    // The dialer sends NO interest on this connection: per Go's
    // directional rule (see gateway.rs) the accepter sends RS+ and
    // a dialer RS+ is a protocol violation that closes the
    // connection. This side's subscriptions reach the peer over the
    // reverse (implicit) connection, where this side is the accepter.
    // The accepter's cmd 2/3 frames arriving afterwards are handled
    // by the read loop (account announcement / interest-only marker).
    // Implicit discovery also applies to solicited peers: a peer
    // may advertise further gateway URLs, but dedup (by name)
    // keeps this from duplicating the connection just made.
    maybe_solicit_implicit(&shared, &peer_gateway, &info.gateway_urls);
    gateway_read_loop(&mut reader, &shared, id, &handle, &tx).await;
    close_gateway(&shared, id);
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
