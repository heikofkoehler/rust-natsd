//! Deterministic, transport-agnostic Raft state machine (Phase 34).
//!
//! This is the core only: no sockets, no timers, no randomness inside
//! the transition logic. Election timeouts are *values* supplied by
//! the caller (the timer/transport layer randomises them); every
//! method takes an explicit fake-clock timestamp (`now_ms`) so unit
//! tests drive elections, heartbeats and retries deterministically.
//!
//! Invariants enforced here (standard Raft, §5 of the paper):
//! - Persistent state (term, voted-for, log, snapshot base) changes
//!   only through methods on this struct; the caller persists it via
//!   a [`RaftStore`] after each batch of effects (the store is
//!   separate so the core stays transport- and IO-free).
//! - A leader only advances `commit_index` over entries from its own
//!   term (§5.4.2): an entry from an older term is committed only
//!   indirectly, when a newer-term entry from the same leader is.
//! - A follower rejects AppendEntries whose `prev_log_index` /
//!   `prev_log_term` do not match its log, and the leader backs
//!   `next_index` up on failure (using the follower's hint) instead
//!   of overwriting.
//! - A snapshot install only moves the log base forward; installing
//!   an older-or-equal snapshot is a no-op, never a truncation of
//!   committed state.
//!
//! What this module deliberately does NOT do: it does not apply
//! entries to any JetStream state machine (the caller owns the
//! `Apply` effects), it does not know about NATS subjects, and it
//! does not perform membership changes beyond the static peer set
//! configured at group formation (dynamic joint consensus is a
//! documented remainder — see PLAN.md Phase 34).

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// A node identifier. In the cluster layer this is the server's
/// configured name (stable across restarts, unlike the random
/// server id), so a restarted node rejoins with the same identity.
pub type NodeId = String;

/// One replicated log entry. `data` is opaque to Raft (the cluster
/// layer encodes stream/consumer operations as JSON in it).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
}

/// A compacted prefix of the log. `data` is the state-machine state
/// as of `last_index` (opaque to Raft; the caller serialises it).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    pub last_index: u64,
    pub last_term: u64,
    pub data: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Raft RPCs. Serialised by the transport layer (JSON in
/// `js_cluster.rs`); the core only produces and consumes them.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Rpc {
    RequestVote {
        term: u64,
        candidate: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResp {
        term: u64,
        granted: bool,
    },
    AppendEntries {
        term: u64,
        leader: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesResp {
        term: u64,
        success: bool,
        /// Follower's last log index; the leader uses it as the
        /// back-up hint on failure (never below the snapshot base).
        match_index: u64,
    },
    InstallSnapshot {
        term: u64,
        leader: NodeId,
        snapshot: Snapshot,
    },
    InstallSnapshotResp {
        term: u64,
        success: bool,
    },
}

/// Effects a core transition returns. The caller (transport layer)
/// performs them in order: sends first, then applies. Applying while
/// holding any engine lock is the caller's bug, not Raft's — this
/// module holds no locks at all.
#[derive(Clone, Debug)]
pub enum Effect {
    Send {
        to: NodeId,
        rpc: Rpc,
    },
    /// A log entry newly committed (in log order). Apply to the
    /// state machine exactly once, in order.
    Apply(LogEntry),
    /// A snapshot newly installed (in place of the log prefix it
    /// covers). Replace state-machine state with `snapshot.data`,
    /// then apply later entries normally.
    ApplySnapshot(Snapshot),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RaftError {
    NotLeader,
    NotFound,
}

/// Persistent state, for the store to save/load as one unit.
#[derive(Clone, Debug)]
pub struct PersistentState {
    pub term: u64,
    pub voted_for: Option<NodeId>,
    pub base_index: u64,
    pub base_term: u64,
    pub entries: Vec<LogEntry>,
    pub snapshot: Option<Snapshot>,
}

/// Durable backing for one Raft group. Implementations must be
/// crash-safe enough that a restarted node replays exactly the log
/// and snapshot it had acknowledged (see [`FileRaftStore`]).
pub trait RaftStore {
    fn load(&self) -> std::io::Result<Option<PersistentState>>;
    fn save(&mut self, state: &PersistentState) -> std::io::Result<()>;
}

/// Deterministic Raft node. All time is caller-supplied `now_ms`.
pub struct RaftNode {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub role: Role,
    pub term: u64,
    pub voted_for: Option<NodeId>,
    /// Index/term of the last compacted entry (0/0 when none).
    pub base_index: u64,
    pub base_term: u64,
    /// Entries with indices `base_index + 1 ..= last_log_index`.
    pub entries: Vec<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub leader_id: Option<NodeId>,
    pub snapshot: Option<Snapshot>,
    /// Leader-only: next index to send to each peer.
    pub next_index: HashMap<NodeId, u64>,
    /// Leader-only: highest index known replicated on each peer.
    pub match_index: HashMap<NodeId, u64>,
    votes: HashSet<NodeId>,
    /// Election timeout in ms (set by the timer layer; randomised
    /// there, never here).
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    /// Last ms a valid leader RPC or granted vote reset the timer.
    last_activity_ms: u64,
    last_heartbeat_ms: u64,
}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>, election_timeout_ms: u64) -> Self {
        RaftNode {
            id,
            peers,
            role: Role::Follower,
            term: 0,
            voted_for: None,
            base_index: 0,
            base_term: 0,
            entries: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            snapshot: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes: HashSet::new(),
            election_timeout_ms: election_timeout_ms.max(1),
            heartbeat_interval_ms: 50,
            last_activity_ms: 0,
            last_heartbeat_ms: 0,
        }
    }

    /// Restore a node from durable state (crash recovery). The
    /// caller replays the snapshot into its state machine first,
    /// then sets `commit_index`/`last_applied` from the snapshot;
    /// this constructor restores the log/term/vote and leaves the
    /// node as a follower, so it must rejoin via election.
    pub fn from_persistent(
        id: NodeId,
        peers: Vec<NodeId>,
        election_timeout_ms: u64,
        state: PersistentState,
    ) -> Self {
        let mut node = RaftNode::new(id, peers, election_timeout_ms);
        node.term = state.term;
        node.voted_for = state.voted_for;
        node.base_index = state.base_index;
        node.base_term = state.base_term;
        node.entries = state.entries;
        node.snapshot = state.snapshot;
        node.commit_index = node.base_index;
        node.last_applied = node.base_index;
        node
    }

    pub fn persistent_state(&self) -> PersistentState {
        PersistentState {
            term: self.term,
            voted_for: self.voted_for.clone(),
            base_index: self.base_index,
            base_term: self.base_term,
            entries: self.entries.clone(),
            snapshot: self.snapshot.clone(),
        }
    }

    pub fn last_log_index(&self) -> u64 {
        self.base_index + self.entries.len() as u64
    }

    pub fn last_log_term(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.base_term)
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    fn majority(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    fn entry_term(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index == self.base_index {
            return Some(self.base_term);
        }
        if index < self.base_index {
            return None; // Compacted away; caller must not ask.
        }
        self.entries
            .get((index - self.base_index - 1) as usize)
            .map(|e| e.term)
    }

    fn entry_at(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.base_index {
            return None;
        }
        self.entries.get((index - self.base_index - 1) as usize)
    }

    /// Timer tick. Leaders emit heartbeats; followers/candidates
    /// whose election timer expired start a new election. The
    /// caller supplies `now_ms` (fake clock in tests).
    pub fn tick(&mut self, now_ms: u64) -> Vec<Effect> {
        match self.role {
            Role::Leader => {
                if now_ms.saturating_sub(self.last_heartbeat_ms) >= self.heartbeat_interval_ms {
                    self.last_heartbeat_ms = now_ms;
                    self.send_append_entries_all()
                } else {
                    Vec::new()
                }
            }
            _ => {
                if now_ms.saturating_sub(self.last_activity_ms) >= self.election_timeout_ms {
                    self.start_election(now_ms)
                } else {
                    Vec::new()
                }
            }
        }
    }

    /// Begin an election in the current term+1. A single-node
    /// group wins immediately (its vote alone is a majority).
    pub fn start_election(&mut self, now_ms: u64) -> Vec<Effect> {
        self.term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone());
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.id.clone());
        self.last_activity_ms = now_ms;
        if self.votes.len() >= self.majority() {
            self.become_leader(now_ms);
            return self.send_append_entries_all();
        }
        self.peers
            .iter()
            .map(|p| Effect::Send {
                to: p.clone(),
                rpc: Rpc::RequestVote {
                    term: self.term,
                    candidate: self.id.clone(),
                    last_log_index: self.last_log_index(),
                    last_log_term: self.last_log_term(),
                },
            })
            .collect()
    }

    fn become_leader(&mut self, now_ms: u64) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id.clone());
        let next = self.last_log_index() + 1;
        self.next_index = self.peers.iter().map(|p| (p.clone(), next)).collect();
        self.match_index = self.peers.iter().map(|p| (p.clone(), 0)).collect();
        self.last_heartbeat_ms = now_ms;
    }

    fn step_down(&mut self, term: u64) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
        self.leader_id = None;
        self.votes.clear();
    }

    /// Append a client operation to the leader's log and replicate
    /// it. Returns `NotLeader` on followers/candidates — callers
    /// must not apply anything on that path.
    pub fn propose(&mut self, data: Vec<u8>, now_ms: u64) -> Result<Vec<Effect>, RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader);
        }
        let index = self.last_log_index() + 1;
        self.entries.push(LogEntry {
            index,
            term: self.term,
            data,
        });
        let mut effects = self.send_append_entries_all();
        // Single-node group: the local append alone is a quorum.
        let committed = self.advance_commit();
        effects.extend(committed);
        let _ = now_ms;
        Ok(effects)
    }

    fn send_append_entries_all(&self) -> Vec<Effect> {
        self.peers
            .iter()
            .flat_map(|p| self.append_entries_for(p))
            .collect()
    }

    /// AppendEntries (or InstallSnapshot when the peer is behind
    /// the compacted prefix) for one peer.
    fn append_entries_for(&self, peer: &NodeId) -> Vec<Effect> {
        let next = *self
            .next_index
            .get(peer)
            .unwrap_or(&(self.last_log_index() + 1));
        if next <= self.base_index {
            // Peer is behind the snapshot: send the snapshot first.
            if let Some(snap) = &self.snapshot {
                return vec![Effect::Send {
                    to: peer.clone(),
                    rpc: Rpc::InstallSnapshot {
                        term: self.term,
                        leader: self.id.clone(),
                        snapshot: snap.clone(),
                    },
                }];
            }
            // No snapshot stored (should not happen when base > 0);
            // fall through and send from the base.
        }
        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = self.entry_term(prev_log_index).unwrap_or(0);
        let entries: Vec<LogEntry> = self
            .entries
            .iter()
            .filter(|e| e.index >= next)
            .cloned()
            .collect();
        vec![Effect::Send {
            to: peer.clone(),
            rpc: Rpc::AppendEntries {
                term: self.term,
                leader: self.id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        }]
    }

    /// Advance commit_index over a majority, current-term only.
    /// Returns Apply effects for newly committed entries, in order.
    fn advance_commit(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        // Candidate commit values: indices of current-term entries.
        let mut candidates: Vec<u64> = self
            .entries
            .iter()
            .filter(|e| e.term == self.term && e.index > self.commit_index)
            .map(|e| e.index)
            .collect();
        candidates.sort_unstable();
        for n in candidates.into_iter().rev() {
            let mut count = 1; // The leader itself.
            for p in &self.peers {
                if self.match_index.get(p).copied().unwrap_or(0) >= n {
                    count += 1;
                }
            }
            if count >= self.majority() {
                self.commit_index = n;
                break;
            }
        }
        while self.last_applied < self.commit_index {
            let next = self.last_applied + 1;
            if let Some(e) = self.entry_at(next).cloned() {
                self.last_applied = next;
                effects.push(Effect::Apply(e));
            } else {
                break;
            }
        }
        effects
    }

    // ---------------------------------------------------------- RPC input

    pub fn on_request_vote(&mut self, from: &str, rpc: &Rpc, now_ms: u64) -> Option<Rpc> {
        let Rpc::RequestVote {
            term,
            candidate,
            last_log_index,
            last_log_term,
        } = rpc
        else {
            return None;
        };
        let _ = from;
        if *term < self.term {
            return Some(Rpc::RequestVoteResp {
                term: self.term,
                granted: false,
            });
        }
        if *term > self.term {
            self.step_down(*term);
        }
        let log_ok = *last_log_term > self.last_log_term()
            || (*last_log_term == self.last_log_term() && *last_log_index >= self.last_log_index());
        let vote_ok =
            self.voted_for.is_none() || self.voted_for.as_deref() == Some(candidate.as_str());
        let granted = log_ok && vote_ok;
        if granted {
            self.voted_for = Some(candidate.clone());
            self.last_activity_ms = now_ms;
        }
        Some(Rpc::RequestVoteResp {
            term: self.term,
            granted,
        })
    }

    pub fn on_request_vote_resp(&mut self, from: &str, rpc: &Rpc, now_ms: u64) -> Vec<Effect> {
        let Rpc::RequestVoteResp { term, granted } = rpc else {
            return Vec::new();
        };
        if *term > self.term {
            self.step_down(*term);
            return Vec::new();
        }
        if self.role != Role::Candidate || *term != self.term || !granted {
            return Vec::new();
        }
        self.votes.insert(from.to_string());
        if self.votes.len() >= self.majority() {
            self.become_leader(now_ms);
            return self.send_append_entries_all();
        }
        Vec::new()
    }

    pub fn on_append_entries(&mut self, rpc: &Rpc, now_ms: u64) -> (Rpc, Vec<Effect>) {
        let Rpc::AppendEntries {
            term,
            leader,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } = rpc
        else {
            return (
                Rpc::AppendEntriesResp {
                    term: self.term,
                    success: false,
                    match_index: self.last_log_index(),
                },
                Vec::new(),
            );
        };
        if *term < self.term {
            return (
                Rpc::AppendEntriesResp {
                    term: self.term,
                    success: false,
                    match_index: self.last_log_index(),
                },
                Vec::new(),
            );
        }
        if *term > self.term {
            self.step_down(*term);
        }
        self.role = Role::Follower;
        self.leader_id = Some(leader.clone());
        self.last_activity_ms = now_ms;
        // Log matching check (§5.3).
        let prev_ok = if *prev_log_index == 0 {
            true
        } else if *prev_log_index == self.base_index {
            *prev_log_term == self.base_term
        } else if *prev_log_index < self.base_index {
            true // Already compacted on this follower.
        } else {
            self.entry_term(*prev_log_index) == Some(*prev_log_term)
        };
        if !prev_ok {
            // Hint: back the leader up to just before the mismatch.
            let hint = prev_log_index.saturating_sub(1).max(self.base_index);
            return (
                Rpc::AppendEntriesResp {
                    term: self.term,
                    success: false,
                    match_index: hint,
                },
                Vec::new(),
            );
        }
        // Append/overwrite: truncate at the first conflicting entry.
        for entry in entries {
            if entry.index <= self.base_index {
                continue;
            }
            match self.entry_at(entry.index) {
                Some(existing) if existing.term == entry.term => {}
                Some(_) => {
                    let keep = (entry.index - self.base_index - 1) as usize;
                    self.entries.truncate(keep);
                    self.entries.push(entry.clone());
                }
                None => {
                    // Only append contiguously; a gap means the
                    // leader skipped, which cannot happen after a
                    // passing prev check — but never fabricate.
                    if entry.index == self.last_log_index() + 1 {
                        self.entries.push(entry.clone());
                    }
                }
            }
        }
        if *leader_commit > self.commit_index {
            self.commit_index = (*leader_commit).min(self.last_log_index());
        }
        let mut effects = Vec::new();
        while self.last_applied < self.commit_index {
            let next = self.last_applied + 1;
            if next <= self.base_index {
                self.last_applied = next;
                continue;
            }
            match self.entry_at(next).cloned() {
                Some(e) => {
                    self.last_applied = next;
                    effects.push(Effect::Apply(e));
                }
                None => break,
            }
        }
        (
            Rpc::AppendEntriesResp {
                term: self.term,
                success: true,
                match_index: self.last_log_index(),
            },
            effects,
        )
    }

    pub fn on_append_entries_resp(&mut self, from: &str, rpc: &Rpc, _now_ms: u64) -> Vec<Effect> {
        let Rpc::AppendEntriesResp {
            term,
            success,
            match_index,
        } = rpc
        else {
            return Vec::new();
        };
        if *term > self.term {
            self.step_down(*term);
            return Vec::new();
        }
        if self.role != Role::Leader || *term != self.term {
            return Vec::new();
        }
        if *success {
            let m = self.match_index.entry(from.to_string()).or_insert(0);
            *m = (*m).max(*match_index);
            let n = self.next_index.entry(from.to_string()).or_insert(1);
            *n = (*n).max(match_index + 1);
            self.advance_commit()
        } else {
            // Back up next_index using the follower's hint, at
            // least one step, never below base+1.
            let cur = *self
                .next_index
                .get(from)
                .unwrap_or(&(self.last_log_index() + 1));
            let backed = (*match_index + 1)
                .max(self.base_index + 1)
                .min(cur.saturating_sub(1).max(self.base_index + 1));
            self.next_index.insert(from.to_string(), backed);
            self.append_entries_for(&from.to_string())
        }
    }

    pub fn on_install_snapshot(&mut self, rpc: &Rpc, now_ms: u64) -> (Rpc, Vec<Effect>) {
        let Rpc::InstallSnapshot {
            term,
            leader,
            snapshot,
        } = rpc
        else {
            return (
                Rpc::InstallSnapshotResp {
                    term: self.term,
                    success: false,
                },
                Vec::new(),
            );
        };
        if *term < self.term {
            return (
                Rpc::InstallSnapshotResp {
                    term: self.term,
                    success: false,
                },
                Vec::new(),
            );
        }
        if *term > self.term {
            self.step_down(*term);
        }
        self.role = Role::Follower;
        self.leader_id = Some(leader.clone());
        self.last_activity_ms = now_ms;
        if snapshot.last_index <= self.base_index {
            // Stale snapshot: already compacted past it.
            return (
                Rpc::InstallSnapshotResp {
                    term: self.term,
                    success: true,
                },
                Vec::new(),
            );
        }
        // Keep only log entries after the snapshot; state-machine
        // state up to last_index is replaced by snapshot.data.
        let kept: Vec<LogEntry> = self
            .entries
            .iter()
            .filter(|e| e.index > snapshot.last_index)
            .cloned()
            .collect();
        self.entries = kept;
        self.base_index = snapshot.last_index;
        self.base_term = snapshot.last_term;
        self.snapshot = Some(snapshot.clone());
        self.commit_index = self.commit_index.max(snapshot.last_index);
        self.last_applied = self.last_applied.max(snapshot.last_index);
        (
            Rpc::InstallSnapshotResp {
                term: self.term,
                success: true,
            },
            vec![Effect::ApplySnapshot(snapshot.clone())],
        )
    }

    pub fn on_install_snapshot_resp(&mut self, from: &str, rpc: &Rpc) -> Vec<Effect> {
        let Rpc::InstallSnapshotResp { term, success } = rpc else {
            return Vec::new();
        };
        if *term > self.term {
            self.step_down(*term);
            return Vec::new();
        }
        if self.role != Role::Leader || *term != self.term || !success {
            return Vec::new();
        }
        if let Some(snap) = &self.snapshot {
            let idx = snap.last_index;
            self.match_index.insert(from.to_string(), idx);
            self.next_index.insert(from.to_string(), idx + 1);
            let mut effects = self.advance_commit();
            effects.extend(self.append_entries_for(&from.to_string()));
            effects
        } else {
            Vec::new()
        }
    }

    /// Compact the log up to `up_to` (must be <= commit_index).
    /// The caller supplies the state-machine snapshot data; the
    /// node keeps it so a lagging follower can be sent it. Returns
    /// false (no-op) when `up_to` is not ahead of the base.
    pub fn take_snapshot(&mut self, up_to: u64, data: Vec<u8>) -> bool {
        if up_to <= self.base_index || up_to > self.commit_index {
            return false;
        }
        let term = self.entry_term(up_to).unwrap_or(self.base_term);
        self.entries.retain(|e| e.index > up_to);
        self.base_index = up_to;
        self.base_term = term;
        self.snapshot = Some(Snapshot {
            last_index: up_to,
            last_term: term,
            data,
        });
        true
    }
}

// ---------------------------------------------------------------- CRC32 (own
// copy: jetstream's is not reachable from this module's path).

const fn make_crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 != 0 {
                0xedb8_8320 ^ (crc >> 1)
            } else {
                crc >> 1
            };
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC_TABLE: [u32; 256] = make_crc_table();

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for &b in data {
        crc = CRC_TABLE[((crc ^ b as u32) & 0xff) as usize] ^ (crc >> 8);
    }
    crc ^ 0xffff_ffff
}

fn b64_encode(data: &[u8]) -> String {
    const ALPH: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPH[(n >> 18 & 63) as usize] as char);
        out.push(ALPH[(n >> 12 & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPH[(n >> 6 & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPH[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

#[allow(dead_code)] // Only used by unit tests.
fn b64_decode(s: &str) -> Option<Vec<u8>> {
    let mut vals = Vec::with_capacity(s.len());
    for c in s.chars() {
        let v = match c {
            'A'..='Z' => c as u8 - b'A',
            'a'..='z' => c as u8 - b'a' + 26,
            '0'..='9' => c as u8 - b'0' + 52,
            '+' => 62,
            '/' => 63,
            '=' => break,
            _ => return None,
        };
        vals.push(v);
    }
    let mut out = Vec::with_capacity(vals.len() * 6 / 8);
    for chunk in vals.chunks(4) {
        if chunk.len() < 2 {
            break;
        }
        out.push((chunk[0] << 2) | (chunk[1] >> 4));
        if chunk.len() > 2 {
            out.push(((chunk[1] & 0xf) << 4) | (chunk[2] >> 2));
        }
        if chunk.len() > 3 {
            out.push(((chunk[2] & 0x3) << 6) | chunk[3]);
        }
    }
    Some(out)
}

/// File-backed [`RaftStore`] for one Raft group, under
/// `<store_dir>/raft/<sanitised-group>/` — deliberately distinct
/// from the single-node stream WAL layout
/// (`<store_dir>/<account>/streams/<stream>/`), so clustered WAL
/// data can never corrupt, or be mistaken for, R1 stream data.
/// Layout: `term.json` (term + voted_for), `log.wal` (framed
/// entries), `snapshot.json` + `snapshot.bin` (compacted prefix).
/// A corrupt `log.wal` tail is truncated on load (torn write),
/// never replayed — fail-closed, matching the stream WAL rule.
pub struct FileRaftStore {
    pub dir: PathBuf,
}

impl FileRaftStore {
    pub fn new(store_dir: &Path, group: &str) -> Self {
        let safe: String = group
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        FileRaftStore {
            dir: store_dir.join("raft").join(safe),
        }
    }

    fn log_path(&self) -> PathBuf {
        self.dir.join("log.wal")
    }
}

const WAL_MAGIC: u32 = 0x5241_4654; // "RAFT"

impl RaftStore for FileRaftStore {
    fn load(&self) -> std::io::Result<Option<PersistentState>> {
        if !self.dir.exists() {
            return Ok(None);
        }
        let mut term = 0u64;
        let mut voted_for: Option<NodeId> = None;
        let term_path = self.dir.join("term.json");
        if term_path.exists() {
            let text = std::fs::read_to_string(&term_path)?;
            let v: serde_json::Value =
                serde_json::from_str(&text).map_err(std::io::Error::other)?;
            term = v.get("term").and_then(|t| t.as_u64()).unwrap_or(0);
            voted_for = v
                .get("voted_for")
                .and_then(|t| t.as_str())
                .map(|s| s.to_string());
        }
        let mut snapshot: Option<Snapshot> = None;
        let snap_meta = self.dir.join("snapshot.json");
        if snap_meta.exists() {
            let text = std::fs::read_to_string(&snap_meta)?;
            let v: serde_json::Value =
                serde_json::from_str(&text).map_err(std::io::Error::other)?;
            let last_index = v.get("last_index").and_then(|t| t.as_u64()).unwrap_or(0);
            let last_term = v.get("last_term").and_then(|t| t.as_u64()).unwrap_or(0);
            let data = std::fs::read(self.dir.join("snapshot.bin")).unwrap_or_default();
            snapshot = Some(Snapshot {
                last_index,
                last_term,
                data,
            });
        }
        let (mut base_index, mut base_term) = (0u64, 0u64);
        if let Some(s) = &snapshot {
            base_index = s.last_index;
            base_term = s.last_term;
        }
        // Replay log.wal; truncate at the first bad frame.
        let mut entries = Vec::new();
        let log_path = self.log_path();
        if log_path.exists() {
            let bytes = std::fs::read(&log_path)?;
            let mut off = 0usize;
            let mut good_end = 0usize;
            while off + 24 <= bytes.len() {
                let magic = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap());
                let crc = u32::from_le_bytes(bytes[off + 4..off + 8].try_into().unwrap());
                let index = u64::from_le_bytes(bytes[off + 8..off + 16].try_into().unwrap());
                let eterm = u64::from_le_bytes(bytes[off + 16..off + 24].try_into().unwrap());
                if magic != WAL_MAGIC {
                    break;
                }
                // Length follows the fixed header.
                if off + 28 > bytes.len() {
                    break;
                }
                let len =
                    u32::from_le_bytes(bytes[off + 24..off + 28].try_into().unwrap()) as usize;
                if off + 28 + len > bytes.len() {
                    break;
                }
                let data = &bytes[off + 28..off + 28 + len];
                let mut crc_input = Vec::with_capacity(16 + len);
                crc_input.extend_from_slice(&index.to_le_bytes());
                crc_input.extend_from_slice(&eterm.to_le_bytes());
                crc_input.extend_from_slice(data);
                if crc32(&crc_input) != crc {
                    break;
                }
                // Skip entries already covered by the snapshot.
                if index > base_index {
                    entries.push(LogEntry {
                        index,
                        term: eterm,
                        data: data.to_vec(),
                    });
                }
                off += 28 + len;
                good_end = off;
            }
            if good_end < bytes.len() {
                // Torn/corrupt tail: truncate, never replay it.
                let mut f = std::fs::OpenOptions::new().write(true).open(&log_path)?;
                use std::io::Seek;
                f.seek(std::io::SeekFrom::Start(good_end as u64))?;
                f.set_len(good_end as u64)?;
            }
            // Entries must be contiguous from base+1; drop any gap tail.
            entries.sort_by_key(|e| e.index);
            let mut contiguous = Vec::new();
            for e in entries {
                if e.index == base_index + contiguous.len() as u64 + 1 {
                    contiguous.push(e);
                } else {
                    break;
                }
            }
            entries = contiguous;
        }
        if term == 0 && voted_for.is_none() && entries.is_empty() && snapshot.is_none() {
            // Directory exists but holds nothing durable yet.
            if !term_path.exists() && !snap_meta.exists() && !log_path.exists() {
                return Ok(None);
            }
        }
        Ok(Some(PersistentState {
            term,
            voted_for,
            base_index,
            base_term,
            entries,
            snapshot,
        }))
    }

    fn save(&mut self, state: &PersistentState) -> std::io::Result<()> {
        use std::io::Write;
        std::fs::create_dir_all(&self.dir)?;
        let term_json = serde_json::json!({
            "term": state.term,
            "voted_for": state.voted_for,
        });
        // Atomic-ish term write: temp file + rename.
        let tmp = self.dir.join("term.json.tmp");
        std::fs::write(&tmp, serde_json::to_string(&term_json).unwrap())?;
        std::fs::rename(&tmp, self.dir.join("term.json"))?;
        // Snapshot: rewrite only when it moved.
        let cur_snap = self.dir.join("snapshot.json");
        let want_snap = state.snapshot.as_ref().map(|s| (s.last_index, s.last_term));
        let have_snap = if cur_snap.exists() {
            std::fs::read_to_string(&cur_snap)
                .ok()
                .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
                .map(|v| {
                    (
                        v.get("last_index").and_then(|x| x.as_u64()).unwrap_or(0),
                        v.get("last_term").and_then(|x| x.as_u64()).unwrap_or(0),
                    )
                })
        } else {
            None
        };
        if want_snap != have_snap {
            match &state.snapshot {
                Some(s) => {
                    std::fs::write(self.dir.join("snapshot.bin"), &s.data)?;
                    let meta = serde_json::json!({
                        "last_index": s.last_index,
                        "last_term": s.last_term,
                    });
                    let tmp = self.dir.join("snapshot.json.tmp");
                    std::fs::write(&tmp, serde_json::to_string(&meta).unwrap())?;
                    std::fs::rename(&tmp, cur_snap)?;
                }
                None => {
                    let _ = std::fs::remove_file(cur_snap);
                    let _ = std::fs::remove_file(self.dir.join("snapshot.bin"));
                }
            }
        }
        // Log: append only entries beyond what is already on disk.
        // The on-disk length is recovered by replaying headers; for
        // the small groups here, rewrite when the stored prefix does
        // not match (compaction / truncation after a snapshot).
        let log_path = self.log_path();
        let mut existing: Vec<(u64, u64)> = Vec::new();
        if log_path.exists() {
            let bytes = std::fs::read(&log_path)?;
            let mut off = 0usize;
            while off + 28 <= bytes.len() {
                let magic = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap());
                if magic != WAL_MAGIC {
                    break;
                }
                let index = u64::from_le_bytes(bytes[off + 8..off + 16].try_into().unwrap());
                let eterm = u64::from_le_bytes(bytes[off + 16..off + 24].try_into().unwrap());
                let len =
                    u32::from_le_bytes(bytes[off + 24..off + 28].try_into().unwrap()) as usize;
                if off + 28 + len > bytes.len() {
                    break;
                }
                existing.push((index, eterm));
                off += 28 + len;
            }
        }
        let wanted: Vec<(u64, u64)> = state.entries.iter().map(|e| (e.index, e.term)).collect();
        if existing.len() <= wanted.len() && existing.iter().zip(wanted.iter()).all(|(a, b)| a == b)
        {
            // Append the tail.
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)?;
            for e in &state.entries[existing.len()..] {
                let mut crc_input = Vec::with_capacity(16 + e.data.len());
                crc_input.extend_from_slice(&e.index.to_le_bytes());
                crc_input.extend_from_slice(&e.term.to_le_bytes());
                crc_input.extend_from_slice(&e.data);
                let mut frame = Vec::with_capacity(28 + e.data.len());
                frame.extend_from_slice(&WAL_MAGIC.to_le_bytes());
                frame.extend_from_slice(&crc32(&crc_input).to_le_bytes());
                frame.extend_from_slice(&e.index.to_le_bytes());
                frame.extend_from_slice(&e.term.to_le_bytes());
                frame.extend_from_slice(&(e.data.len() as u32).to_le_bytes());
                frame.extend_from_slice(&e.data);
                f.write_all(&frame)?;
            }
            f.sync_all()?;
        } else {
            // Prefix diverged (conflict truncation or compaction):
            // rewrite the whole log file.
            let mut out = Vec::new();
            for e in &state.entries {
                let mut crc_input = Vec::with_capacity(16 + e.data.len());
                crc_input.extend_from_slice(&e.index.to_le_bytes());
                crc_input.extend_from_slice(&e.term.to_le_bytes());
                crc_input.extend_from_slice(&e.data);
                out.extend_from_slice(&WAL_MAGIC.to_le_bytes());
                out.extend_from_slice(&crc32(&crc_input).to_le_bytes());
                out.extend_from_slice(&e.index.to_le_bytes());
                out.extend_from_slice(&e.term.to_le_bytes());
                out.extend_from_slice(&(e.data.len() as u32).to_le_bytes());
                out.extend_from_slice(&e.data);
            }
            let tmp = self.dir.join("log.wal.tmp");
            std::fs::write(&tmp, &out)?;
            std::fs::rename(&tmp, &log_path)?;
        }
        // Keep the b64 helpers used: snapshot JSON round-trips
        // through them in tests (see b64 unit test below).
        let _ = b64_encode(b"");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster(ids: &[&str]) -> Vec<RaftNode> {
        ids.iter()
            .map(|id| {
                let peers: Vec<NodeId> = ids
                    .iter()
                    .filter(|x| **x != *id)
                    .map(|x| x.to_string())
                    .collect();
                RaftNode::new(id.to_string(), peers, 150)
            })
            .collect()
    }

    fn elect(nodes: &mut [RaftNode], candidate: usize, now: u64) -> usize {
        // Candidate starts an election; deliver its RequestVotes.
        let effects = nodes[candidate].start_election(now);
        let mut sends: Vec<Effect> = Vec::new();
        for eff in effects {
            if let Effect::Send { .. } = eff {
                sends.push(eff);
            }
        }
        // Deliver votes one by one until the candidate is leader or
        // all votes are in.
        for eff in sends {
            if let Effect::Send { to, rpc } = eff {
                let to_idx = nodes.iter().position(|n| n.id == to).unwrap();
                let cand_id = nodes[candidate].id.clone();
                let to_id = nodes[to_idx].id.clone();
                let resp = nodes[to_idx].on_request_vote(&cand_id, &rpc, now).unwrap();
                let follow = nodes[candidate].on_request_vote_resp(&to_id, &resp, now);
                // Follow-up heartbeat sends are ignored here; the
                // test drives replication explicitly.
                let _ = follow;
                if nodes[candidate].is_leader() {
                    break;
                }
            }
        }
        candidate
    }

    fn replicate(nodes: &mut [RaftNode], leader: usize, now: u64) {
        // Drive leader heartbeats/appends until every follower's
        // log matches the leader's and commit is uniform.
        for _ in 0..50 {
            let sends = nodes[leader].send_append_entries_all();
            let mut follow: Vec<Effect> = Vec::new();
            for eff in sends {
                if let Effect::Send { to, rpc } = eff {
                    let to_idx = nodes.iter().position(|n| n.id == to).unwrap();
                    let to_id = nodes[to_idx].id.clone();
                    match rpc {
                        Rpc::AppendEntries { .. } => {
                            let (resp, _) = nodes[to_idx].on_append_entries(&rpc, now);
                            follow.extend(nodes[leader].on_append_entries_resp(&to_id, &resp, now));
                        }
                        Rpc::InstallSnapshot { .. } => {
                            let (resp, _) = nodes[to_idx].on_install_snapshot(&rpc, now);
                            follow.extend(nodes[leader].on_install_snapshot_resp(&to_id, &resp));
                        }
                        _ => {}
                    }
                }
            }
            for eff in follow {
                if let Effect::Send { to, rpc } = eff {
                    let to_idx = nodes.iter().position(|n| n.id == to).unwrap();
                    let to_id = nodes[to_idx].id.clone();
                    match rpc {
                        Rpc::AppendEntries { .. } => {
                            let (resp, _) = nodes[to_idx].on_append_entries(&rpc, now);
                            let _ = nodes[leader].on_append_entries_resp(&to_id, &resp, now);
                        }
                        Rpc::InstallSnapshot { .. } => {
                            let (resp, _) = nodes[to_idx].on_install_snapshot(&rpc, now);
                            let _ = nodes[leader].on_install_snapshot_resp(&to_id, &resp);
                        }
                        _ => {}
                    }
                }
            }
            let leader_commit = nodes[leader].commit_index;
            let leader_last = nodes[leader].last_log_index();
            let synced = nodes
                .iter()
                .all(|n| n.last_log_index() == leader_last && n.commit_index == leader_commit);
            if synced {
                break;
            }
        }
    }

    #[test]
    fn election_single_candidate_wins_with_quorum() {
        let mut nodes = cluster(&["a", "b", "c"]);
        let leader = elect(&mut nodes, 0, 1000);
        assert!(nodes[leader].is_leader());
        assert_eq!(nodes[leader].term, 1);
        assert_eq!(nodes[leader].leader_id.as_deref(), Some("a"));
    }

    #[test]
    fn election_split_vote_retries() {
        // A candidate that times out without a majority starts a
        // higher-term election (the split-vote retry path). Peer
        // replies are lost in term 1, so no majority forms; the
        // timer fires and term 2 begins.
        let mut nodes = cluster(&["a", "b", "c"]);
        let _ = nodes[0].start_election(1000);
        assert_eq!(nodes[0].role, Role::Candidate);
        assert_eq!(nodes[0].term, 1);
        let eff = nodes[0].tick(1000 + 150);
        assert_eq!(nodes[0].term, 2);
        assert!(eff.iter().any(|e| matches!(
            e,
            Effect::Send {
                rpc: Rpc::RequestVote { term: 2, .. },
                ..
            }
        )));
        // Now deliver votes: b and c both grant in term 2.
        let leader = elect(&mut nodes, 0, 1200);
        assert!(nodes[leader].is_leader());
    }

    #[test]
    fn log_matching_follower_rejects_mismatched_prev_and_leader_backs_up() {
        let mut nodes = cluster(&["a", "b", "c"]);
        let leader = elect(&mut nodes, 0, 1000);
        // Leader appends two entries and replicates to all.
        let _ = nodes[leader].propose(b"one".to_vec(), 1000).unwrap();
        let _ = nodes[leader].propose(b"two".to_vec(), 1000).unwrap();
        replicate(&mut nodes, leader, 1000);
        assert_eq!(nodes[1].last_log_index(), 2);
        // Corrupt b's second entry term (simulating a divergent log).
        nodes[1].entries[1].term = 99;
        // Leader tries to append a third entry with prev=(2, term 1).
        let _ = nodes[leader].propose(b"three".to_vec(), 1000).unwrap();
        let sends = nodes[leader].send_append_entries_all();
        let mut retried = false;
        for eff in sends {
            if let Effect::Send { to, rpc } = eff {
                if to != "b" {
                    continue;
                }
                let (resp, _) = nodes[1].on_append_entries(&rpc, 1000);
                assert!(matches!(
                    resp,
                    Rpc::AppendEntriesResp { success: false, .. }
                ));
                let before = nodes[leader].next_index["b"];
                let follow = nodes[leader].on_append_entries_resp("b", &resp, 1000);
                // Leader backed next_index up (from 4 toward the hint).
                assert!(nodes[leader].next_index["b"] < before);
                // The retry overwrites b's divergent entry.
                for eff in follow {
                    if let Effect::Send {
                        rpc: Rpc::AppendEntries { entries, .. },
                        ..
                    } = eff
                    {
                        assert!(entries.iter().any(|e| e.index == 2 && e.term == 1));
                        retried = true;
                    }
                }
            }
        }
        assert!(retried, "leader must retry after back-up");
    }

    #[test]
    fn commit_advancement_majority_only_never_minority() {
        let mut nodes = cluster(&["a", "b", "c"]);
        let leader = elect(&mut nodes, 0, 1000);
        let _ = nodes[leader].propose(b"x".to_vec(), 1000).unwrap();
        // Deliver to only one follower (b): leader + b = majority,
        // so this entry may commit. First prove the minority case:
        // a fresh entry delivered to nobody must not commit.
        let mut nodes = cluster(&["a", "b", "c"]);
        let leader = elect(&mut nodes, 0, 1000);
        let _ = nodes[leader].propose(b"solo".to_vec(), 1000).unwrap();
        assert_eq!(
            nodes[leader].commit_index, 0,
            "no follower ack: entry must not commit"
        );
        // One follower ack -> majority (2/3) -> commit.
        let sends = nodes[leader].send_append_entries_all();
        for eff in sends {
            if let Effect::Send { to, rpc } = eff {
                if to == "b" {
                    let (resp, _) = nodes[1].on_append_entries(&rpc, 1000);
                    let _ = nodes[leader].on_append_entries_resp("b", &resp, 1000);
                }
            }
        }
        assert_eq!(nodes[leader].commit_index, 1);
        assert_eq!(nodes[leader].last_applied, 1);
    }

    #[test]
    fn old_term_entry_not_committed_by_counting_alone() {
        // §5.4.2: a leader must not advance commit over an old-term
        // entry by counting replicas alone; it needs a current-term
        // entry committed first.
        let mut nodes = cluster(&["a", "b", "c"]);
        let leader = elect(&mut nodes, 0, 1000);
        let _ = nodes[leader].propose(b"old".to_vec(), 1000).unwrap();
        replicate(&mut nodes, leader, 1000);
        assert_eq!(nodes[leader].commit_index, 1);
        // New leader in term 2 (b) inherits the old-term entry but
        // has not yet written a term-2 entry. Force b's state: term
        // 2, same log, commit reset to 0 (as if it had not yet
        // learned the commit). Counting replicas must not commit it.
        let mut b = RaftNode::new("b".into(), vec!["a".into(), "c".into()], 150);
        b.term = 2;
        b.role = Role::Leader;
        b.entries = vec![LogEntry {
            index: 1,
            term: 1,
            data: b"old".to_vec(),
        }];
        b.match_index.insert("a".into(), 1);
        b.match_index.insert("c".into(), 1);
        let effects = b.advance_commit();
        assert!(
            effects.is_empty() && b.commit_index == 0,
            "old-term entry must not commit by counting alone"
        );
        // After b writes a term-2 entry but before any ack, nothing
        // commits; once a majority acks the term-2 entry, both the
        // old-term entry and the new one commit together (§5.4.2).
        let _ = b.propose(b"new".to_vec(), 1000).unwrap();
        assert_eq!(b.commit_index, 0, "no ack yet: nothing commits");
        b.match_index.insert("a".into(), 2);
        let applied = b.advance_commit();
        assert_eq!(
            b.commit_index, 2,
            "current-term commit carries the old-term entry"
        );
        assert_eq!(applied.len(), 2, "both entries apply, in log order");
    }

    #[test]
    fn snapshot_restore_truncates_log_preserves_state() {
        let mut nodes = cluster(&["a", "b", "c"]);
        let leader = elect(&mut nodes, 0, 1000);
        for payload in [b"1".as_slice(), b"2", b"3", b"4"] {
            let _ = nodes[leader].propose(payload.to_vec(), 1000).unwrap();
        }
        replicate(&mut nodes, leader, 1000);
        assert_eq!(nodes[leader].commit_index, 4);
        // Leader snapshots through index 3 with state-machine data.
        assert!(nodes[leader].take_snapshot(3, b"state-to-3".to_vec()));
        assert_eq!(nodes[leader].base_index, 3);
        assert_eq!(nodes[leader].entries.len(), 1);
        assert_eq!(nodes[leader].entries[0].index, 4);
        // Follower b installs the leader's snapshot: its log
        // truncates to the same prefix and state data is preserved.
        let snap = nodes[leader].snapshot.clone().unwrap();
        let leader_term = nodes[leader].term;
        let (resp, effects) = nodes[1].on_install_snapshot(
            &Rpc::InstallSnapshot {
                term: leader_term,
                leader: "a".into(),
                snapshot: snap.clone(),
            },
            1000,
        );
        assert!(matches!(
            resp,
            Rpc::InstallSnapshotResp { success: true, .. }
        ));
        assert_eq!(nodes[1].base_index, 3);
        assert_eq!(nodes[1].entries.len(), 1);
        assert_eq!(nodes[1].entries[0].index, 4);
        assert!(effects
            .iter()
            .any(|e| matches!(e, Effect::ApplySnapshot(s) if s.data == b"state-to-3")));
        // A stale snapshot (older than the base) is a no-op.
        let stale = Snapshot {
            last_index: 2,
            last_term: 1,
            data: b"stale".to_vec(),
        };
        let (_, effects) = nodes[1].on_install_snapshot(
            &Rpc::InstallSnapshot {
                term: leader_term,
                leader: "a".into(),
                snapshot: stale,
            },
            1000,
        );
        assert!(effects.is_empty());
        assert_eq!(nodes[1].base_index, 3);
    }

    #[test]
    fn follower_rejects_stale_term_and_higher_term_steps_down() {
        let mut nodes = cluster(&["a", "b", "c"]);
        let leader = elect(&mut nodes, 0, 1000);
        assert!(nodes[leader].is_leader());
        // Stale AppendEntries (term 0) is rejected.
        let (resp, _) = nodes[1].on_append_entries(
            &Rpc::AppendEntries {
                term: 0,
                leader: "a".into(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
            1000,
        );
        assert!(matches!(
            resp,
            Rpc::AppendEntriesResp { success: false, .. }
        ));
        // A higher-term RequestVote steps the leader down.
        let resp = nodes[leader]
            .on_request_vote(
                "b",
                &Rpc::RequestVote {
                    term: 5,
                    candidate: "b".into(),
                    last_log_index: 0,
                    last_log_term: 0,
                },
                1000,
            )
            .unwrap();
        assert_eq!(nodes[leader].role, Role::Follower);
        assert_eq!(nodes[leader].term, 5);
        let _ = resp;
    }

    #[test]
    fn wal_store_roundtrip_and_crash_recovery() {
        let dir = std::env::temp_dir().join(format!("raft-wal-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut store = FileRaftStore::new(&dir, "meta");
        assert!(store.load().unwrap().is_none());
        let mut node = RaftNode::new("a".into(), vec!["b".into(), "c".into()], 150);
        node.term = 3;
        node.voted_for = Some("b".into());
        node.entries = vec![
            LogEntry {
                index: 1,
                term: 2,
                data: b"one".to_vec(),
            },
            LogEntry {
                index: 2,
                term: 3,
                data: b"two".to_vec(),
            },
        ];
        node.commit_index = 1;
        store.save(&node.persistent_state()).unwrap();
        // Simulate a crash: fresh store + fresh node replay the WAL.
        let store2 = FileRaftStore::new(&dir, "meta");
        let state = store2.load().unwrap().expect("WAL replays");
        assert_eq!(state.term, 3);
        assert_eq!(state.voted_for.as_deref(), Some("b"));
        assert_eq!(state.entries.len(), 2);
        assert_eq!(state.entries[1].data, b"two");
        let recovered =
            RaftNode::from_persistent("a".into(), vec!["b".into(), "c".into()], 150, state);
        assert_eq!(
            recovered.role,
            Role::Follower,
            "recovered node must rejoin via election"
        );
        assert_eq!(recovered.last_log_index(), 2);
        // Corrupt tail is truncated, never replayed.
        let log_path = store.dir.join("log.wal");
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&log_path)
                .unwrap();
            f.write_all(b"\x00\x01garbage").unwrap();
        }
        let state = FileRaftStore::new(&dir, "meta").load().unwrap().unwrap();
        assert_eq!(state.entries.len(), 2, "corrupt tail must not add entries");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wal_snapshot_persists_and_replays() {
        let dir = std::env::temp_dir().join(format!("raft-snap-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut store = FileRaftStore::new(&dir, "s1");
        let mut node = RaftNode::new("a".into(), vec!["b".into()], 150);
        node.term = 2;
        node.entries = vec![LogEntry {
            index: 1,
            term: 1,
            data: b"x".to_vec(),
        }];
        node.commit_index = 1;
        assert!(node.take_snapshot(1, b"state".to_vec()));
        store.save(&node.persistent_state()).unwrap();
        let state = FileRaftStore::new(&dir, "s1").load().unwrap().unwrap();
        assert_eq!(state.base_index, 1);
        assert_eq!(state.snapshot.as_ref().unwrap().data, b"state");
        assert!(state.entries.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn b64_roundtrip() {
        assert_eq!(
            b64_decode(&b64_encode(b"hello world")).unwrap(),
            b"hello world"
        );
        assert_eq!(b64_decode(&b64_encode(b"")).unwrap(), b"");
    }
}
