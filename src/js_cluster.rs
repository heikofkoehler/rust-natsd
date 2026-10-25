//! JetStream cluster layer — EXPERIMENTAL, Phase 34 (partial).
//!
//! What exists here, and what does not, is stated plainly because a
//! half-wired cluster path that can fork or lose acked data must not
//! ship (see PLAN.md Phase 34):
//!
//! EXISTS: [`ClusterManager`] — one meta Raft group plus one Raft
//! group per replicated stream, each backed by a [`FileRaftStore`]
//! under `<store_dir>/raft/<group>/` (never the single-node stream
//! WAL layout, so R1 data is untouched). [`NatsRaftTransport`]
//! binds Raft RPCs to internal NATS subjects
//! (`$JS.RAFT.<node-id>`) carried over the existing cluster routes:
//! each node holds an internal client connection subscribed to its
//! own subject, and publishes RPCs to peers' subjects; route RS+
//! propagation does the cross-node delivery. This is a Rust-only
//! cluster: Go Raft interop is a separate reverse-engineering
//! effort and is NOT claimed.
//!
//! NOT YET WIRED: the JetStream engine's publish/ack/consumer path
//! does not propose through these groups. A `STREAM.CREATE` with
//! `num_replicas > 1` is therefore refused (see `api.rs`), never
//! silently stored as R1. Until the engine path is wired, an acked
//! publish is still a single-node ack — this module replicates
//! *operations* only for groups driven directly (meta placement and
//! the integration test's stream-op groups), and must not be read
//! as "clustered JetStream works".
//!
//! Design choices, documented so the wiring phase does not have to
//! rediscover them:
//! - Consumer state is replicated per *stream* group, not per
//!   consumer group (Go uses a group per consumer). The stream
//!   group already orders stream and consumer mutations against
//!   each other (a consumer ack and a message delete cannot
//!   reorder across groups), at the cost of a larger group log.
//!   This choice is consistent everywhere in this module.
//! - Reads are served by the group leader only. Followers refuse
//!   (propose returns `NotLeader`); there are no stale-tolerant
//!   follower reads in this phase, so no reader can observe a
//!   forked minority's state.
//! - Raft RPCs are fire-and-forget NATS messages: loss is handled
//!   by heartbeat/appends retries, exactly as Raft expects of an
//!   unreliable transport. Election timeouts are randomised only
//!   here, in the transport's tick loop — never inside `raft.rs`.
//! - Node identity is the configured server name (stable across
//!   restarts). The transport assumes no client authentication on
//!   the local client port; with auth configured the internal
//!   connection cannot authenticate and the transport logs and
//!   retries (a wiring-phase item, not silently worked around).
//! - Membership is static: the peer set is fixed at group
//!   formation. Dynamic membership change (joint consensus) is a
//!   documented remainder.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::raft::{Effect, FileRaftStore, RaftError, RaftNode, RaftStore, Rpc};

/// Operations replicated through a stream Raft group. Every
/// JetStream mutation (publish, ack, consumer create/delete,
/// purge, delete) must become one of these proposals applied only
/// on commit — that wiring is the remainder; the encoding is fixed
/// here so the wiring phase does not change the log format.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupOp {
    /// Append a message to a stream (subject, payload, headers).
    StreamAppend {
        stream: String,
        subject: String,
        payload: Vec<u8>,
        headers: Option<Vec<u8>>,
    },
    StreamDeleteMsg {
        stream: String,
        seq: u64,
    },
    StreamPurge {
        stream: String,
    },
    ConsumerCreate {
        stream: String,
        consumer: String,
    },
    ConsumerDelete {
        stream: String,
        consumer: String,
    },
    ConsumerAck {
        stream: String,
        consumer: String,
        stream_seq: u64,
    },
}

/// Meta-group operations: stream/consumer placement across peers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaOp {
    AssignStream { stream: String, peers: Vec<String> },
    DeleteStream { stream: String },
}

/// Envelope carried on `$JS.RAFT.<node-id>`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RpcEnvelope {
    pub group: String,
    pub from: String,
    pub rpc: Rpc,
}

/// One Raft group managed by a node: its node, its WAL store, and
/// the operations committed (in order) since this manager started.
/// `committed` is the observation point for tests and, in the
/// wiring phase, the apply queue for the JetStream engine — the
/// engine must drain it in order and never apply an op twice.
pub struct ManagedGroup {
    pub node: RaftNode,
    pub store: FileRaftStore,
    pub committed: Vec<GroupOp>,
    pub committed_meta: Vec<MetaOp>,
    pub last_snapshot: Option<Vec<u8>>,
}

impl ManagedGroup {
    fn persist(&mut self) {
        let state = self.node.persistent_state();
        if let Err(e) = self.store.save(&state) {
            eprintln!("[js-cluster] WAL save failed: {e}");
        }
    }

    fn drain_effects(&mut self, effects: Vec<Effect>) -> Vec<(String, Rpc)> {
        let mut sends = Vec::new();
        for eff in effects {
            match eff {
                Effect::Send { to, rpc } => sends.push((to, rpc)),
                Effect::Apply(entry) => {
                    if let Ok(op) = serde_json::from_slice::<GroupOp>(&entry.data) {
                        self.committed.push(op);
                    } else if let Ok(op) = serde_json::from_slice::<MetaOp>(&entry.data) {
                        self.committed_meta.push(op);
                    }
                    // Undecodable entries are ignored here: the
                    // log position still advanced, and the op
                    // format is fixed by this module's proposer.
                }
                Effect::ApplySnapshot(snap) => {
                    self.last_snapshot = Some(snap.data.clone());
                }
            }
        }
        sends
    }
}

/// All Raft groups on one node (meta + per-stream).
pub struct ClusterManager {
    pub node_id: String,
    pub peers: Vec<String>,
    pub store_dir: PathBuf,
    pub election_timeout_ms: u64,
    pub groups: HashMap<String, ManagedGroup>,
}

impl ClusterManager {
    pub fn new(
        node_id: String,
        peers: Vec<String>,
        store_dir: PathBuf,
        election_timeout_ms: u64,
    ) -> Self {
        ClusterManager {
            node_id,
            peers,
            store_dir,
            election_timeout_ms,
            groups: HashMap::new(),
        }
    }

    /// Create (or rejoin) a group: replay its WAL/snapshot before
    /// it participates in any election — crash recovery.
    pub fn create_group(&mut self, name: &str) -> std::io::Result<()> {
        if self.groups.contains_key(name) {
            return Ok(());
        }
        let store = FileRaftStore::new(&self.store_dir, name);
        let node = match store.load()? {
            Some(state) => RaftNode::from_persistent(
                self.node_id.clone(),
                self.peers.clone(),
                self.election_timeout_ms,
                state,
            ),
            None => RaftNode::new(
                self.node_id.clone(),
                self.peers.clone(),
                self.election_timeout_ms,
            ),
        };
        let mut group = ManagedGroup {
            node,
            store,
            committed: Vec::new(),
            committed_meta: Vec::new(),
            last_snapshot: None,
        };
        // Persist the freshly created group dir so a crash before
        // the first RPC still replays an empty-but-present group.
        group.persist();
        self.groups.insert(name.to_string(), group);
        Ok(())
    }

    pub fn is_leader(&self, group: &str) -> bool {
        self.groups
            .get(group)
            .map(|g| g.node.is_leader())
            .unwrap_or(false)
    }

    pub fn leader_id(&self, group: &str) -> Option<String> {
        self.groups
            .get(group)
            .and_then(|g| g.node.leader_id.clone())
    }

    pub fn committed(&self, group: &str) -> Vec<GroupOp> {
        self.groups
            .get(group)
            .map(|g| g.committed.clone())
            .unwrap_or_default()
    }

    /// Timer tick for all groups (fake-clock `now_ms` in tests,
    /// wall-clock ms in the transport). Returns RPCs to send.
    pub fn tick(&mut self, group: &str, now_ms: u64) -> Vec<(String, Rpc)> {
        let Some(g) = self.groups.get_mut(group) else {
            return Vec::new();
        };
        let effects = g.node.tick(now_ms);
        let sends = g.drain_effects(effects);
        g.persist();
        sends
    }

    /// Handle one inbound RPC. Returns RPCs to send (responses and
    /// follow-ups). `from` must be the sender's node id.
    pub fn handle_rpc(
        &mut self,
        group: &str,
        from: &str,
        rpc: Rpc,
        now_ms: u64,
    ) -> Vec<(String, Rpc)> {
        let Some(g) = self.groups.get_mut(group) else {
            return Vec::new();
        };
        let mut sends = Vec::new();
        match &rpc {
            Rpc::RequestVote { .. } => {
                if let Some(resp) = g.node.on_request_vote(from, &rpc, now_ms) {
                    sends.push((from.to_string(), resp));
                }
            }
            Rpc::RequestVoteResp { .. } => {
                let effects = g.node.on_request_vote_resp(from, &rpc, now_ms);
                sends.extend(g.drain_effects(effects));
            }
            Rpc::AppendEntries { .. } => {
                let (resp, effects) = g.node.on_append_entries(&rpc, now_ms);
                sends.extend(g.drain_effects(effects));
                sends.push((from.to_string(), resp));
            }
            Rpc::AppendEntriesResp { .. } => {
                let effects = g.node.on_append_entries_resp(from, &rpc, now_ms);
                sends.extend(g.drain_effects(effects));
            }
            Rpc::InstallSnapshot { .. } => {
                let (resp, effects) = g.node.on_install_snapshot(&rpc, now_ms);
                sends.extend(g.drain_effects(effects));
                sends.push((from.to_string(), resp));
            }
            Rpc::InstallSnapshotResp { .. } => {
                let effects = g.node.on_install_snapshot_resp(from, &rpc);
                sends.extend(g.drain_effects(effects));
            }
        }
        g.persist();
        sends
    }

    /// Propose an operation to a group. Only the leader may call
    /// this; followers/candidates get `NotLeader` and must redirect
    /// the client to the leader (or refuse) — never apply locally.
    /// A successful return means the entry is in the leader's log
    /// and replication was started; it is committed (visible in
    /// `committed()`) only after a quorum acks it. Callers must
    /// NOT ack a client publish until the op appears committed.
    pub fn propose(
        &mut self,
        group: &str,
        op: &GroupOp,
        now_ms: u64,
    ) -> Result<Vec<(String, Rpc)>, RaftError> {
        let Some(g) = self.groups.get_mut(group) else {
            return Err(RaftError::NotFound);
        };
        let data = serde_json::to_vec(op).map_err(|_| RaftError::NotFound)?;
        let effects = g.node.propose(data, now_ms)?;
        let sends = g.drain_effects(effects);
        g.persist();
        Ok(sends)
    }

    pub fn propose_meta(
        &mut self,
        group: &str,
        op: &MetaOp,
        now_ms: u64,
    ) -> Result<Vec<(String, Rpc)>, RaftError> {
        let Some(g) = self.groups.get_mut(group) else {
            return Err(RaftError::NotFound);
        };
        let data = serde_json::to_vec(op).map_err(|_| RaftError::NotFound)?;
        let effects = g.node.propose(data, now_ms)?;
        let sends = g.drain_effects(effects);
        g.persist();
        Ok(sends)
    }
}

pub fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Internal-subject transport over the node's own client port.
///
/// Wire shape: `PUB $JS.RAFT.<peer-id>` with a JSON
/// [`RpcEnvelope`] body; the local internal connection subscribes
/// to `$JS.RAFT.<own-id>`. Cross-node delivery rides the existing
/// route mesh (RS+ propagation of that subscription). This is
/// deliberately a plain client connection, not a new wire protocol:
/// it reuses route interest, queue selection is irrelevant (each
/// subject has exactly one subscriber), and no route-parser changes
/// were needed.
pub struct NatsRaftTransport {
    pub manager: Arc<Mutex<ClusterManager>>,
    pub groups: Vec<String>,
}

impl NatsRaftTransport {
    pub fn new(manager: Arc<Mutex<ClusterManager>>, groups: Vec<String>) -> Self {
        NatsRaftTransport { manager, groups }
    }

    /// Run until cancelled: connect, subscribe, serve RPCs, tick
    /// elections/heartbeats. Reconnects with backoff on failure.
    /// The caller aborts the returned task to simulate a node kill
    /// (SIGKILL equivalent at this layer: no graceful close).
    pub async fn run(&self, node_id: String, local_addr: String) {
        loop {
            match self.serve_once(&node_id, &local_addr).await {
                Ok(()) => return,
                Err(e) => {
                    eprintln!("[js-cluster:{node_id}] transport error: {e}; reconnecting");
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        }
    }

    async fn serve_once(&self, node_id: &str, local_addr: &str) -> std::io::Result<()> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let stream = tokio::net::TcpStream::connect(local_addr).await?;
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        // Read INFO, then CONNECT + PING.
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        if !line.starts_with("INFO ") {
            return Err(std::io::Error::other(format!("expected INFO, got: {line}")));
        }
        writer
            .write_all(
                format!(
                    "CONNECT {{\"verbose\":false,\"pedantic\":false,\"headers\":false,\"name\":\"js-raft-{node_id}\"}}\r\nPING\r\nSUB $JS.RAFT.{node_id} 1\r\n"
                )
                .as_bytes(),
            )
            .await?;
        writer.flush().await?;
        // Wait for the PONG (SUB is processed before it, in order).
        loop {
            line.clear();
            let n = reader.read_line(&mut line).await?;
            if n == 0 {
                return Err(std::io::Error::other("EOF before PONG"));
            }
            if line.trim() == "PONG" {
                break;
            }
            if line.starts_with("-ERR") {
                return Err(std::io::Error::other(format!("server error: {line}")));
            }
        }
        let writer = Arc::new(tokio::sync::Mutex::new(writer));
        // The ticker must die with this connection: it runs as a
        // future selected against the reader loop, NOT as a detached
        // spawn — a detached ticker would survive a transport abort
        // and keep a "killed" leader sending heartbeats forever
        // (found by the leader-kill test: survivors never timed
        // out because the dead leader's orphan ticker lived on).
        let ticker = self.ticker_loop(&writer, node_id);
        let result = tokio::select! {
            res = self.reader_loop(&mut reader, &writer, node_id) => res,
            () = ticker => Err(std::io::Error::other("ticker ended")),
        };
        result
    }

    async fn ticker_loop(
        &self,
        writer: &Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
        node_id: &str,
    ) {
        use tokio::io::AsyncWriteExt;
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(20));
        loop {
            interval.tick().await;
            let now = wall_ms();
            let mut sends: Vec<(String, String, Rpc)> = Vec::new();
            {
                let mut mgr = self.manager.lock().unwrap();
                for group in &self.groups {
                    for (to, rpc) in mgr.tick(group, now) {
                        sends.push((group.clone(), to, rpc));
                    }
                }
            }
            for (group, to, rpc) in sends {
                let env = RpcEnvelope {
                    group,
                    from: node_id.to_string(),
                    rpc,
                };
                let body = match serde_json::to_string(&env) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                let frame = format!("PUB $JS.RAFT.{to} {}\r\n{body}\r\n", body.len());
                let mut w = writer.lock().await;
                if w.write_all(frame.as_bytes()).await.is_err() {
                    return;
                }
                if w.flush().await.is_err() {
                    return;
                }
            }
        }
    }

    async fn reader_loop(
        &self,
        reader: &mut tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>,
        writer: &Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
        node_id: &str,
    ) -> std::io::Result<()> {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
        let mut line = String::new();
        loop {
            line.clear();
            let n = reader.read_line(&mut line).await?;
            if n == 0 {
                return Err(std::io::Error::other("EOF from server"));
            }
            let trimmed = line.trim_end().to_string();
            if trimmed == "PING" {
                let mut w = writer.lock().await;
                w.write_all(b"PONG\r\n").await?;
                w.flush().await?;
                continue;
            }
            if let Some(rest) = trimmed.strip_prefix("MSG ") {
                let parts: Vec<&str> = rest.split_whitespace().collect();
                // MSG <subject> <sid> [reply] <size>
                let Some(size_str) = parts.last() else {
                    continue;
                };
                let Ok(size) = size_str.parse::<usize>() else {
                    continue;
                };
                let mut buf = vec![0u8; size + 2];
                reader.read_exact(&mut buf).await?;
                let body = &buf[..size];
                let Ok(env) = serde_json::from_slice::<RpcEnvelope>(body) else {
                    continue;
                };
                let now = wall_ms();
                let sends = {
                    let mut mgr = self.manager.lock().unwrap();
                    mgr.handle_rpc(&env.group, &env.from, env.rpc, now)
                };
                for (to, rpc) in sends {
                    let env = RpcEnvelope {
                        group: env.group.clone(),
                        from: node_id.to_string(),
                        rpc,
                    };
                    let Ok(body) = serde_json::to_string(&env) else {
                        continue;
                    };
                    let frame = format!("PUB $JS.RAFT.{to} {}\r\n{body}\r\n", body.len());
                    let mut w = writer.lock().await;
                    w.write_all(frame.as_bytes()).await?;
                    w.flush().await?;
                }
            }
            // PONG / +OK / INFO frames are ignored here.
        }
    }
}
