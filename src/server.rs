//! Tokio server core: listener, per-connection tasks, message routing.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Notify};
use tokio::task::JoinSet;

use crate::config::Config;
use crate::protocol::{
    encode_err, encode_hmsg, encode_info, encode_msg, encode_ok, encode_pong, ClientOp, Parser,
    ServerInfo,
};
use crate::sublist::{Sublist, Subscription};

/// Bounded per-connection outbound queue. A subscriber that cannot keep up
/// (queue full) is disconnected as a slow consumer, like `nats-server`.
const OUTBOUND_CHANNEL_CAPACITY: usize = 1024;

struct ConnHandle {
    tx: mpsc::Sender<Bytes>,
    headers: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    close: Arc<Notify>,
}

impl ConnHandle {
    fn mark_closed(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.close.notify_waiters();
    }
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

struct Shared {
    config: Config,
    listen_addr: SocketAddr,
    server_id: String,
    sublist: Mutex<Sublist>,
    conns: Mutex<HashMap<u64, Arc<ConnHandle>>>,
    next_client_id: AtomicU64,
    shutdown: Arc<Notify>,
}

/// A running server instance; used by `main` and by tests.
pub struct RunningServer {
    pub addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<io::Result<()>>,
}

impl RunningServer {
    /// Signal shutdown and wait for the listener and connections to drain.
    pub async fn shutdown(self) -> io::Result<()> {
        let _ = self.shutdown_tx.send(true);
        match self.task.await {
            Ok(r) => r,
            Err(e) => Err(io::Error::other(e.to_string())),
        }
    }
}

/// Bind the listener and start serving in a background task.
pub async fn start(config: Config) -> io::Result<RunningServer> {
    let listener = TcpListener::bind((config.host.as_str(), config.port)).await?;
    let addr = listener.local_addr()?;
    let shared = Arc::new(Shared {
        config,
        listen_addr: addr,
        server_id: gen_server_id(),
        sublist: Mutex::new(Sublist::new()),
        conns: Mutex::new(HashMap::new()),
        next_client_id: AtomicU64::new(1),
        shutdown: Arc::new(Notify::new()),
    });
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(run_loop(listener, shared, shutdown_rx));
    Ok(RunningServer {
        addr,
        shutdown_tx,
        task,
    })
}

async fn run_loop(
    listener: TcpListener,
    shared: Arc<Shared>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut conn_tasks = JoinSet::new();
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() { break; }
            }
            res = listener.accept() => {
                match res {
                    Ok((stream, _peer)) => {
                        let _ = stream.set_nodelay(true);
                        conn_tasks.spawn(handle_conn(stream, shared.clone()));
                    }
                    Err(e) => {
                        eprintln!("rust-natsd: accept error: {e}");
                    }
                }
            }
            _ = conn_tasks.join_next(), if !conn_tasks.is_empty() => {}
        }
    }
    shared.shutdown.notify_waiters();
    let drain = async { while conn_tasks.join_next().await.is_some() {} };
    let _ = tokio::time::timeout(Duration::from_secs(2), drain).await;
    conn_tasks.abort_all();
    Ok(())
}

struct ConnState {
    verbose: bool,
    echo: bool,
}

async fn handle_conn(stream: TcpStream, shared: Arc<Shared>) {
    let client_id = shared.next_client_id.fetch_add(1, Ordering::SeqCst);
    let (mut reader, mut writer) = stream.into_split();
    let (tx, mut rx) = mpsc::channel::<Bytes>(OUTBOUND_CHANNEL_CAPACITY);
    let handle = Arc::new(ConnHandle {
        tx: tx.clone(),
        headers: Arc::new(AtomicBool::new(false)),
        closed: Arc::new(AtomicBool::new(false)),
        close: Arc::new(Notify::new()),
    });
    shared
        .conns
        .lock()
        .unwrap()
        .insert(client_id, handle.clone());

    // Writer task: drain the outbound queue to the socket. It holds no
    // channel sender, so when the reader side drops every sender the queue
    // closes and the writer flushes what remains (e.g. a final -ERR) before
    // exiting.
    let writer_closed = handle.closed.clone();
    let writer_close = handle.close.clone();
    let writer_shutdown = shared.shutdown.clone();
    tokio::spawn(async move {
        let writer_shutdown = writer_shutdown.notified();
        tokio::pin!(writer_shutdown);
        loop {
            if writer_closed.load(Ordering::SeqCst) {
                break;
            }
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(bytes) => {
                            if writer.write_all(&bytes).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                _ = writer_close.notified() => break,
                _ = &mut writer_shutdown => break,
            }
        }
        let _ = writer.shutdown().await;
    });

    // Send INFO first, per protocol.
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
    };
    let _ = tx.send(encode_info(&info)).await;

    let mut state = ConnState {
        verbose: false,
        echo: true,
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
    shared.sublist.lock().unwrap().remove_conn(client_id);
    shared.conns.lock().unwrap().remove(&client_id);
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
            if !shared.config.authorize(&info) {
                let _ = tx.send(encode_err("Authorization Violation")).await;
                return false;
            }
            state.verbose = info.verbose;
            state.echo = info.echo;
            handle.headers.store(info.headers, Ordering::SeqCst);
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
            route_message(
                shared, client_id, state.echo, &subject, reply_to, None, payload,
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
            route_message(
                shared,
                client_id,
                state.echo,
                &subject,
                reply_to,
                Some(headers),
                payload,
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
            shared
                .sublist
                .lock()
                .unwrap()
                .add(Subscription::new(client_id, sid, subject, queue));
            if state.verbose {
                let _ = tx.send(encode_ok()).await;
            }
            true
        }
        ClientOp::Unsub { sid, max } => {
            match max {
                None => {
                    shared.sublist.lock().unwrap().remove(client_id, &sid);
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

/// Resolve recipients and deliver one message. Called without holding any
/// lock while writing to subscriber queues.
fn route_message(
    shared: &Arc<Shared>,
    publisher_id: u64,
    publisher_echo: bool,
    subject: &str,
    reply_to: Option<String>,
    headers: Option<Bytes>,
    payload: Bytes,
) {
    let result = {
        let mut sl = shared.sublist.lock().unwrap();
        sl.match_recipients(subject, publisher_id, publisher_echo)
    };
    for d in &result.deliveries {
        let target = shared.conns.lock().unwrap().get(&d.conn_id).cloned();
        let Some(target) = target else { continue };
        let frame = match &headers {
            Some(h) if target.headers.load(Ordering::SeqCst) => {
                encode_hmsg(subject, &d.sid, reply_to.as_deref(), h, &payload)
            }
            _ => encode_msg(subject, &d.sid, reply_to.as_deref(), &payload),
        };
        match target.tx.try_send(frame) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Slow consumer: drop the connection entirely.
                eprintln!(
                    "rust-natsd: slow consumer, dropping connection {}",
                    d.conn_id
                );
                shared.sublist.lock().unwrap().remove_conn(d.conn_id);
                if let Some(h) = shared.conns.lock().unwrap().remove(&d.conn_id) {
                    h.mark_closed();
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
    if !result.expired.is_empty() {
        let mut sl = shared.sublist.lock().unwrap();
        for (cid, sid) in &result.expired {
            sl.remove(*cid, sid);
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
