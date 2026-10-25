//! JetStream persistence (Phase 30): an append-only WAL.
//!
//! Layout under `<store_dir>/`:
//! ```text
//! <account>/streams/<stream>/meta.json       stream config + base_seq + dedup ids
//! <account>/streams/<stream>/msgs.wal        framed message appends + tombstones
//! <account>/streams/<stream>/consumers/<consumer>.wal   consumer-state append log
//! ```
//! Account/stream/consumer names are sanitised for path components
//! (anything outside `[A-Za-z0-9._-]` becomes `_`); `meta.json`
//! carries the authoritative names, so a sanitised collision would
//! merge two names — account and stream names in practice come from
//! validated configs, and this is documented rather than silently
//! worked around.
//!
//! Frame format (all integers little-endian):
//! `[magic u32][crc32 u32][record_type u8][seq u64][timestamp i64]
//!  [subject_len u16][headers_len u32][payload_len u32][bytes...]`
//! where `bytes` is `subject ++ headers ++ payload` in that order.
//! Record types: AppendMsg `0x01`, TombstoneSeq `0x02` (msgs.wal
//! only; lengths are zero, `seq` is the deleted sequence),
//! ConsumerState `0x03` (consumers/*.wal only; `bytes` is the
//! consumer snapshot JSON, `seq` is the consumer sequence).
//! The CRC32 (IEEE, hand-rolled — no new dependency) covers
//! everything after the CRC field. A record that fails its magic,
//! length, or CRC check ends replay: the tail from that point on is
//! corrupt or a torn partial write, so it is **truncated on load**
//! (logged) and never enters the engine — fail-closed, never
//! silently accepted. A corrupt `meta.json` skips that one stream
//! (logged); this matches the legacy snapshot behaviour of starting
//! without unreadable data rather than refusing the whole server.
//!
//! The write path is a diff, not a rewrite: `JetStream::persist`
//! (called after every `handle_message` and every redelivery tick,
//! never while holding `engine.lock()` for delivery — the actions
//! pattern is unchanged) compares the live engine against a shadow
//! of the last persisted state and appends only what changed:
//! new tail messages, tombstones for newly deleted / evicted
//! sequences, a fresh `meta.json` only when config, base sequence,
//! mirror/source liveness, or dedup ids changed, and one consumer
//! record per changed consumer. Appends are O(1) single `write`
//! syscalls with no fsync (a graceful restart, the case the tests
//! exercise, sees every completed write; only a machine crash
//! could lose the OS-buffered tail — the same trade Go's file
//! store makes by default). The one operation the diff cannot
//! track O(1) — filtered purge renumbering messages in place —
//! sets `Stream::wal_rewrite`, which forces a rewrite of that
//! stream's `msgs.wal`; a stream replaced wholesale (new
//! `created_ns`) and a base sequence moving backwards take the
//! same rewrite path defensively.
//!
//! Dedup ids live in `meta.json`, not in a third record type:
//! they change only on publishes carrying `Nats-Msg-Id`, and a
//! small atomic meta rewrite on those publishes keeps the frame
//! format exactly as specified. The dedup window still restarts
//! on reload (snapshot v2 behaviour, preserved).
//!
//! Compaction: when a stream's tombstoned/evicted bytes exceed
//! 50% of its `msgs.wal` size and the file is larger than 1 MiB
//! (default), the next persist rewrites `msgs.wal` with only the
//! live messages. Consumer logs checkpoint the same way: after
//! 128 appended state records the file is rewritten to just the
//! latest record (last record always wins on replay).
//!
//! Migration: if a legacy whole-engine `rust-natsd-js.json`
//! exists and no per-account WAL layout exists yet, the legacy
//! snapshot (v2, including pending ack state) is loaded once and
//! written out through the WAL. The legacy file is left in place;
//! a second start sees the WAL layout and never re-migrates.
//! Re-migration would still be idempotent — replay keys messages
//! by sequence and consumer state is last-record-wins — but the
//! layout check makes it unnecessary.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use serde_json::{json, Value};

use super::{
    make_source_token, now_ns, AckPolicy, Consumer, Engine, JetStream, Pending, Redeliver,
    SourceCfg, StoredMsg, Stream, DEFAULT_ACK_WAIT,
};

// ---------------------------------------------------------------- CRC32

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

pub(crate) fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for &b in data {
        crc = CRC_TABLE[((crc ^ b as u32) & 0xff) as usize] ^ (crc >> 8);
    }
    crc ^ 0xffff_ffff
}

// ---------------------------------------------------------------- Frames

const WAL_MAGIC: u32 = 0x5741_4c31; // "WAL1"
const REC_APPEND: u8 = 0x01;
const REC_TOMBSTONE: u8 = 0x02;
const REC_CONSUMER: u8 = 0x03;
const FRAME_HEADER_LEN: usize = 4 + 4 + 1 + 8 + 8 + 2 + 4 + 4; // 35
const MAX_FRAME_BODY: usize = 64 * 1024 * 1024;

fn encode_frame(
    rtype: u8,
    seq: u64,
    timestamp: i64,
    subject: &[u8],
    headers: &[u8],
    payload: &[u8],
) -> Vec<u8> {
    let mut body = Vec::with_capacity(27 + subject.len() + headers.len() + payload.len());
    body.push(rtype);
    body.extend_from_slice(&seq.to_le_bytes());
    body.extend_from_slice(&timestamp.to_le_bytes());
    body.extend_from_slice(&(subject.len() as u16).to_le_bytes());
    body.extend_from_slice(&(headers.len() as u32).to_le_bytes());
    body.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    body.extend_from_slice(subject);
    body.extend_from_slice(headers);
    body.extend_from_slice(payload);
    let crc = crc32(&body);
    let mut out = Vec::with_capacity(FRAME_HEADER_LEN + body.len() - 27);
    out.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&body);
    out
}

fn encode_append(msg: &StoredMsg) -> Vec<u8> {
    encode_frame(
        REC_APPEND,
        msg.seq,
        msg.time_ns as i64,
        msg.subject.as_bytes(),
        msg.headers.as_deref().unwrap_or(&[]),
        msg.payload.as_ref(),
    )
}

fn encode_tombstone(seq: u64) -> Vec<u8> {
    encode_frame(REC_TOMBSTONE, seq, 0, &[], &[], &[])
}

fn encode_consumer(consumer_seq: u64, json_text: &str) -> Vec<u8> {
    encode_frame(
        REC_CONSUMER,
        consumer_seq,
        now_ns() as i64,
        &[],
        &[],
        json_text.as_bytes(),
    )
}

struct Frame {
    rtype: u8,
    seq: u64,
    timestamp: i64,
    subject: Vec<u8>,
    headers: Vec<u8>,
    payload: Vec<u8>,
}

/// Parse frames from the start of `data`. Stops at the first
/// incomplete, malformed, or CRC-bad record and reports the good
/// prefix length, so the caller can truncate the corrupt tail.
fn parse_frames(data: &[u8]) -> (Vec<Frame>, usize, bool) {
    let mut frames = Vec::new();
    let mut off = 0usize;
    loop {
        if off == data.len() {
            return (frames, off, false);
        }
        if data.len() - off < FRAME_HEADER_LEN {
            return (frames, off, true); // torn partial header
        }
        let magic = u32::from_le_bytes(data[off..off + 4].try_into().unwrap());
        if magic != WAL_MAGIC {
            return (frames, off, true);
        }
        let crc = u32::from_le_bytes(data[off + 4..off + 8].try_into().unwrap());
        let body_start = off + 8;
        let rtype = data[body_start];
        if rtype != REC_APPEND && rtype != REC_TOMBSTONE && rtype != REC_CONSUMER {
            return (frames, off, true);
        }
        let seq = u64::from_le_bytes(data[body_start + 1..body_start + 9].try_into().unwrap());
        let timestamp =
            i64::from_le_bytes(data[body_start + 9..body_start + 17].try_into().unwrap());
        let subject_len =
            u16::from_le_bytes(data[body_start + 17..body_start + 19].try_into().unwrap()) as usize;
        let headers_len =
            u32::from_le_bytes(data[body_start + 19..body_start + 23].try_into().unwrap()) as usize;
        let payload_len =
            u32::from_le_bytes(data[body_start + 23..body_start + 27].try_into().unwrap()) as usize;
        let body_len = 27 + subject_len + headers_len + payload_len;
        if body_len > MAX_FRAME_BODY || data.len() - body_start < body_len {
            return (frames, off, true);
        }
        let body = &data[body_start..body_start + body_len];
        if crc32(body) != crc {
            return (frames, off, true);
        }
        let mut p = body_start + 27;
        let subject = data[p..p + subject_len].to_vec();
        p += subject_len;
        let headers = data[p..p + headers_len].to_vec();
        p += headers_len;
        let payload = data[p..p + payload_len].to_vec();
        frames.push(Frame {
            rtype,
            seq,
            timestamp,
            subject,
            headers,
            payload,
        });
        off = body_start + body_len;
    }
}

// A frame on disk is exactly the 35-byte header plus
// subject/headers/payload bytes.
fn frame_disk_len(subject_len: usize, headers_len: usize, payload_len: usize) -> usize {
    FRAME_HEADER_LEN + subject_len + headers_len + payload_len
}

struct MsgReplay {
    /// Live messages by sequence (tombstones removed, later appends
    /// of the same sequence overwrite — replay is idempotent).
    by_seq: HashMap<u64, StoredMsg>,
    tombstoned: HashSet<u64>,
    frame_lens: HashMap<u64, usize>,
    good_len: usize,
    truncated: bool,
}

fn replay_msgs(data: &[u8]) -> MsgReplay {
    let (frames, good_len, truncated) = parse_frames(data);
    let mut by_seq: HashMap<u64, StoredMsg> = HashMap::new();
    let mut tombstoned: HashSet<u64> = HashSet::new();
    let mut frame_lens: HashMap<u64, usize> = HashMap::new();
    for f in &frames {
        match f.rtype {
            REC_APPEND => {
                frame_lens.insert(
                    f.seq,
                    frame_disk_len(f.subject.len(), f.headers.len(), f.payload.len()),
                );
                by_seq.insert(
                    f.seq,
                    StoredMsg {
                        seq: f.seq,
                        subject: String::from_utf8_lossy(&f.subject).into_owned(),
                        headers: if f.headers.is_empty() {
                            None
                        } else {
                            Some(Bytes::from(f.headers.clone()))
                        },
                        payload: Bytes::from(f.payload.clone()),
                        time_ns: f.timestamp as u64,
                    },
                );
            }
            REC_TOMBSTONE => {
                tombstoned.insert(f.seq);
                by_seq.remove(&f.seq);
            }
            _ => {}
        }
    }
    MsgReplay {
        by_seq,
        tombstoned,
        frame_lens,
        good_len,
        truncated,
    }
}

struct ConsumerReplay {
    /// Last consumer-state JSON record, if any.
    last_json: Option<String>,
    count: usize,
    good_len: usize,
    truncated: bool,
}

fn replay_consumers(data: &[u8]) -> ConsumerReplay {
    let (frames, good_len, truncated) = parse_frames(data);
    let mut last_json = None;
    let mut count = 0;
    for f in &frames {
        if f.rtype == REC_CONSUMER {
            last_json = Some(String::from_utf8_lossy(&f.payload).into_owned());
            count += 1;
        }
    }
    ConsumerReplay {
        last_json,
        count,
        good_len,
        truncated,
    }
}

fn truncate_to(path: &Path, len: usize) {
    if let Ok(f) = OpenOptions::new().write(true).open(path) {
        if let Err(e) = f.set_len(len as u64) {
            eprintln!("rust-natsd: wal truncate {path:?}: {e}");
        }
    }
}

// ---------------------------------------------------------------- Trait

/// JetStream storage backend. `FileWalStore` is the only
/// implementation; the trait exists so the diff-sync in
/// `JetStream::persist` is written against an abstraction (and so
/// a future backend can slot in without touching the engine).
pub(crate) trait JsStore {
    fn save_stream_meta(&mut self, stream: &Stream) -> io::Result<()>;
    fn delete_stream(&mut self, account: &str, name: &str) -> io::Result<()>;
    fn append_msg(&mut self, account: &str, stream: &str, msg: &StoredMsg) -> io::Result<()>;
    fn delete_msg(&mut self, account: &str, stream: &str, seq: u64) -> io::Result<()>;
    fn save_consumer_state(
        &mut self,
        account: &str,
        stream: &str,
        consumer: &Consumer,
    ) -> io::Result<()>;
    fn delete_consumer(&mut self, account: &str, stream: &str, name: &str) -> io::Result<()>;
    /// True when this stream's WAL is past the compaction trigger.
    fn needs_compaction(&self, account: &str, stream: &str) -> bool;
    /// Rewrite `msgs.wal` with exactly `messages` (live messages,
    /// in sequence order). Used for compaction and for the
    /// renumber/replace rewrite paths.
    fn compact_msgs(
        &mut self,
        account: &str,
        stream: &str,
        messages: &[StoredMsg],
    ) -> io::Result<()>;
}

// ---------------------------------------------------------------- Store

fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[derive(Default)]
struct StreamStats {
    wal_bytes: u64,
    dead_bytes: u64,
    frame_lens: HashMap<u64, usize>,
    dead: HashSet<u64>,
}

pub(crate) struct FileWalStore {
    root: PathBuf,
    msg_files: HashMap<String, File>,
    cons_files: HashMap<String, File>,
    stats: HashMap<String, StreamStats>,
    cons_counts: HashMap<String, usize>,
    compact_threshold_bytes: u64,
}

const DEFAULT_COMPACT_THRESHOLD: u64 = 1024 * 1024;
const CONSUMER_CHECKPOINT_RECORDS: usize = 128;

impl FileWalStore {
    pub(crate) fn new(root: PathBuf) -> Self {
        FileWalStore {
            root,
            msg_files: HashMap::new(),
            cons_files: HashMap::new(),
            stats: HashMap::new(),
            cons_counts: HashMap::new(),
            compact_threshold_bytes: DEFAULT_COMPACT_THRESHOLD,
        }
    }

    /// Test-only threshold override (unit tests drive compaction
    /// through the store directly; production config has no knob).
    #[cfg(test)]
    pub(crate) fn set_compact_threshold_bytes(&mut self, n: u64) {
        self.compact_threshold_bytes = n;
    }

    pub(crate) fn stream_dir(&self, account: &str, name: &str) -> PathBuf {
        self.root
            .join(sanitize(account))
            .join("streams")
            .join(sanitize(name))
    }

    fn msgs_path(&self, account: &str, name: &str) -> PathBuf {
        self.stream_dir(account, name).join("msgs.wal")
    }

    fn consumer_path(&self, account: &str, stream: &str, name: &str) -> PathBuf {
        self.stream_dir(account, stream)
            .join("consumers")
            .join(format!("{}.wal", sanitize(name)))
    }

    fn stream_key(account: &str, name: &str) -> String {
        format!("{account}\0{name}")
    }

    fn consumer_key(account: &str, stream: &str, name: &str) -> String {
        format!("{account}\0{stream}\0{name}")
    }

    fn msg_file(&mut self, account: &str, name: &str) -> io::Result<&mut File> {
        let key = Self::stream_key(account, name);
        if !self.msg_files.contains_key(&key) {
            let dir = self.stream_dir(account, name);
            fs::create_dir_all(&dir)?;
            let path = self.msgs_path(account, name);
            let f = OpenOptions::new().create(true).append(true).open(&path)?;
            self.msg_files.insert(key.clone(), f);
        }
        Ok(self.msg_files.get_mut(&key).unwrap())
    }

    fn cons_file(&mut self, account: &str, stream: &str, name: &str) -> io::Result<&mut File> {
        let key = Self::consumer_key(account, stream, name);
        if !self.cons_files.contains_key(&key) {
            let dir = self.stream_dir(account, stream).join("consumers");
            fs::create_dir_all(&dir)?;
            let path = self.consumer_path(account, stream, name);
            let f = OpenOptions::new().create(true).append(true).open(&path)?;
            self.cons_files.insert(key.clone(), f);
        }
        Ok(self.cons_files.get_mut(&key).unwrap())
    }

    fn write_msg_frame(&mut self, account: &str, name: &str, frame: &[u8]) -> io::Result<()> {
        let f = self.msg_file(account, name)?;
        f.write_all(frame)?;
        let key = Self::stream_key(account, name);
        self.stats.entry(key).or_default().wal_bytes += frame.len() as u64;
        Ok(())
    }

    /// Stats seeded by WAL load (replay), so compaction decisions
    /// after a restart see the true file state.
    fn seed_stats(&mut self, account: &str, name: &str, replay: &MsgReplay, base_seq: u64) {
        let key = Self::stream_key(account, name);
        let st = self.stats.entry(key).or_default();
        st.wal_bytes = replay.good_len as u64;
        st.frame_lens = replay.frame_lens.clone();
        for seq in replay
            .tombstoned
            .iter()
            .chain(replay.frame_lens.keys().filter(|seq| **seq < base_seq))
        {
            if st.dead.insert(*seq) {
                st.dead_bytes += replay.frame_lens.get(seq).copied().unwrap_or(0) as u64;
            }
        }
    }
}

impl JsStore for FileWalStore {
    fn save_stream_meta(&mut self, stream: &Stream) -> io::Result<()> {
        let dir = self.stream_dir(&stream.account, &stream.name);
        fs::create_dir_all(&dir)?;
        let meta = stream_meta_json(stream);
        let tmp = dir.join("meta.json.tmp");
        let path = dir.join("meta.json");
        fs::write(&tmp, serde_json::to_string_pretty(&meta).unwrap())?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    fn delete_stream(&mut self, account: &str, name: &str) -> io::Result<()> {
        let skey = Self::stream_key(account, name);
        self.msg_files.remove(&skey);
        self.stats.remove(&skey);
        let prefix = format!("{skey}\0");
        self.cons_files.retain(|k, _| !k.starts_with(&prefix));
        self.cons_counts.retain(|k, _| !k.starts_with(&prefix));
        let dir = self.stream_dir(account, name);
        if dir.exists() {
            fs::remove_dir_all(&dir)?;
        }
        Ok(())
    }

    fn append_msg(&mut self, account: &str, stream: &str, msg: &StoredMsg) -> io::Result<()> {
        let frame = encode_append(msg);
        let len = frame.len();
        self.write_msg_frame(account, stream, &frame)?;
        let key = Self::stream_key(account, stream);
        let st = self.stats.entry(key).or_default();
        st.frame_lens.insert(msg.seq, len);
        Ok(())
    }

    fn delete_msg(&mut self, account: &str, stream: &str, seq: u64) -> io::Result<()> {
        let key = Self::stream_key(account, stream);
        let known = self
            .stats
            .get(&key)
            .and_then(|st| st.frame_lens.get(&seq))
            .copied();
        let Some(len) = known else {
            return Ok(()); // never appended (or already compacted away)
        };
        let frame = encode_tombstone(seq);
        self.write_msg_frame(account, stream, &frame)?;
        let st = self.stats.entry(key).or_default();
        if st.dead.insert(seq) {
            st.dead_bytes += len as u64;
        }
        Ok(())
    }

    fn save_consumer_state(
        &mut self,
        account: &str,
        stream: &str,
        consumer: &Consumer,
    ) -> io::Result<()> {
        let text = serde_json::to_string(&consumer_snapshot_json(consumer)).unwrap();
        let frame = encode_consumer(consumer.consumer_seq, &text);
        let f = self.cons_file(account, stream, &consumer.name)?;
        f.write_all(&frame)?;
        let key = Self::consumer_key(account, stream, &consumer.name);
        let count = {
            let c = self.cons_counts.entry(key.clone()).or_insert(0);
            *c += 1;
            *c
        };
        if count > CONSUMER_CHECKPOINT_RECORDS {
            // Checkpoint: rewrite to just the latest record.
            self.cons_files.remove(&key);
            let path = self.consumer_path(account, stream, &consumer.name);
            fs::write(&path, &frame)?;
            self.cons_counts.insert(key, 1);
        }
        Ok(())
    }

    fn delete_consumer(&mut self, account: &str, stream: &str, name: &str) -> io::Result<()> {
        let key = Self::consumer_key(account, stream, name);
        self.cons_files.remove(&key);
        self.cons_counts.remove(&key);
        let path = self.consumer_path(account, stream, name);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    fn needs_compaction(&self, account: &str, stream: &str) -> bool {
        let key = Self::stream_key(account, stream);
        match self.stats.get(&key) {
            Some(st) => {
                st.wal_bytes > self.compact_threshold_bytes && st.dead_bytes * 2 > st.wal_bytes
            }
            None => false,
        }
    }

    fn compact_msgs(
        &mut self,
        account: &str,
        stream: &str,
        messages: &[StoredMsg],
    ) -> io::Result<()> {
        let key = Self::stream_key(account, stream);
        self.msg_files.remove(&key);
        let dir = self.stream_dir(account, stream);
        fs::create_dir_all(&dir)?;
        let path = self.msgs_path(account, stream);
        let mut buf = Vec::new();
        let mut lens = HashMap::new();
        for m in messages {
            let frame = encode_append(m);
            lens.insert(m.seq, frame.len());
            buf.extend_from_slice(&frame);
        }
        fs::write(&path, &buf)?;
        let st = self.stats.entry(key).or_default();
        st.wal_bytes = buf.len() as u64;
        st.dead_bytes = 0;
        st.frame_lens = lens;
        st.dead.clear();
        Ok(())
    }
}

// ---------------------------------------------------------------- JSON shapes

/// One consumer, in exactly the snapshot-v2 fragment shape, so
/// consumer WAL records and legacy snapshots interconvert.
pub(crate) fn consumer_snapshot_json(c: &Consumer) -> Value {
    let mut pending: Vec<(&u64, &Pending)> = c.pending.iter().collect();
    pending.sort_by_key(|(cseq, _)| **cseq);
    let pending_json: Vec<Value> = pending
        .iter()
        .map(|(cseq, p)| {
            json!({
                "consumer_seq": **cseq,
                "stream_seq": p.stream_seq,
                "delivered": p.delivered,
                "delivered_at_ns": p.delivered_at_ns,
            })
        })
        .collect();
    let redeliver_json: Vec<Value> = c
        .redeliver
        .iter()
        .map(|r| {
            json!({
                "stream_seq": r.stream_seq,
                "delivered": r.delivered,
            })
        })
        .collect();
    json!({
        "name": c.name,
        "deliver_subject": c.deliver_subject,
        "filter_subject": c.filter_subject,
        "filter_subjects": c.filter_subjects,
        "ack_policy": match c.ack_policy {
            AckPolicy::Explicit => "explicit",
            AckPolicy::None => "none",
            AckPolicy::All => "all",
        },
        "ack_wait_ns": c.ack_wait.as_nanos() as u64,
        "max_deliver": c.max_deliver,
        "max_ack_pending": c.max_ack_pending,
        "deliver_policy": c.deliver_policy,
        "next_stream_seq": c.next_stream_seq,
        "consumer_seq": c.consumer_seq,
        "ack_floor_stream_seq": c.ack_floor_stream_seq,
        "ack_floor_consumer_seq": c.ack_floor_consumer_seq,
        "num_redelivered": c.num_redelivered,
        "pending": pending_json,
        "redeliver": redeliver_json,
    })
}

fn source_cfg_json(v: &SourceCfg) -> Value {
    json!({
        "name": v.name,
        "filter_subject": v.filter_subject,
        "opt_start_seq": v.start_seq,
        "opt_start_time_ns": v.start_time_ns,
        "live": v.live,
    })
}

/// `meta.json`: snapshot stream fields minus messages/consumers,
/// plus `base_seq` and the dedup map.
pub(crate) fn stream_meta_json(s: &Stream) -> Value {
    let msg_ids: Value = s
        .msg_ids
        .iter()
        .map(|(id, (seq, _))| (id.clone(), json!(seq)))
        .collect();
    json!({
        "name": s.name,
        "account": s.account,
        "created_ns": s.created_ns,
        "subjects": s.subjects,
        "retention": s.retention,
        "max_msgs": s.max_msgs,
        "max_bytes": s.max_bytes,
        "max_age_ns": s.max_age_ns,
        "max_msgs_per_subject": s.max_msgs_per_subject,
        "storage": s.storage,
        "base_seq": s.base_seq,
        "msg_ids": msg_ids,
        "mirror": s.mirror.as_ref().map(source_cfg_json),
        "sources": s.sources.iter().map(source_cfg_json).collect::<Vec<_>>(),
        "source_token": s.source_token,
        "allow_direct": s.allow_direct,
        "mirror_direct": s.mirror_direct,
        "deny_delete": s.deny_delete,
        "deny_purge": s.deny_purge,
        "allow_rollup_hdrs": s.allow_rollup_hdrs,
    })
}

// ---------------------------------------------------------------- Shadow + sync

#[derive(Default)]
pub(crate) struct Shadow {
    streams: HashMap<String, StreamShadow>,
}

struct StreamShadow {
    account: String,
    name: String,
    meta: String,
    created_ns: u64,
    base_seq: u64,
    len: usize,
    last_seq: u64,
    appended: HashSet<u64>,
    deleted: HashSet<u64>,
    consumers: HashMap<String, String>,
}

impl Shadow {
    pub(crate) fn from_engine(engine: &Engine) -> Shadow {
        let mut shadow = Shadow::default();
        for s in engine.streams.values() {
            let mut appended: HashSet<u64> = HashSet::new();
            for m in &s.messages {
                appended.insert(m.seq);
            }
            shadow.streams.insert(
                format!("{}\0{}", s.account, s.name),
                StreamShadow {
                    account: s.account.clone(),
                    name: s.name.clone(),
                    meta: serde_json::to_string(&stream_meta_json(s)).unwrap(),
                    created_ns: s.created_ns,
                    base_seq: s.base_seq,
                    len: s.messages.len(),
                    last_seq: s.last_seq(),
                    appended,
                    deleted: s.deleted.clone(),
                    consumers: s
                        .consumers
                        .iter()
                        .map(|(n, c)| {
                            (
                                n.clone(),
                                serde_json::to_string(&consumer_snapshot_json(c)).unwrap(),
                            )
                        })
                        .collect(),
                },
            );
        }
        shadow
    }
}

fn live_messages(s: &Stream) -> Vec<StoredMsg> {
    s.messages
        .iter()
        .filter(|m| !s.deleted.contains(&m.seq))
        .map(|m| StoredMsg {
            seq: m.seq,
            subject: m.subject.clone(),
            headers: m.headers.clone(),
            payload: m.payload.clone(),
            time_ns: m.time_ns,
        })
        .collect()
}

/// Append-only diff-sync of the whole engine into `store`.
/// Best-effort like the old snapshot path: errors are logged,
/// never fatal (in-memory state stays authoritative).
pub(crate) fn sync_engine(store: &mut impl JsStore, engine: &mut Engine, shadow: &mut Shadow) {
    let mut live_keys: HashSet<String> = HashSet::new();
    for stream in engine.streams.values_mut() {
        let key = format!("{}\0{}", stream.account, stream.name);
        live_keys.insert(key.clone());
        let meta = serde_json::to_string(&stream_meta_json(stream)).unwrap();
        let replaced = match shadow.streams.get(&key) {
            Some(sh) => sh.created_ns != 0 && sh.created_ns != stream.created_ns,
            None => false,
        };
        if replaced {
            // Wholesale replace (STREAM.CREATE over an existing
            // name): drop the old WAL tree, then full-write below.
            if let Err(e) = store.delete_stream(&stream.account, &stream.name) {
                eprintln!(
                    "rust-natsd: wal delete replaced stream {}: {e}",
                    stream.name
                );
            }
            shadow.streams.remove(&key);
        }
        let is_new = !shadow.streams.contains_key(&key);
        if is_new {
            if let Err(e) = store.save_stream_meta(stream) {
                eprintln!("rust-natsd: wal meta {}: {e}", stream.name);
            }
            for m in live_messages(stream) {
                if let Err(e) = store.append_msg(&stream.account, &stream.name, &m) {
                    eprintln!("rust-natsd: wal append {}: {e}", stream.name);
                }
            }
            for c in stream.consumers.values() {
                if let Err(e) = store.save_consumer_state(&stream.account, &stream.name, c) {
                    eprintln!("rust-natsd: wal consumer {}: {e}", stream.name);
                }
            }
            let mut sh = StreamShadow {
                account: stream.account.clone(),
                name: stream.name.clone(),
                meta,
                created_ns: stream.created_ns,
                base_seq: stream.base_seq,
                len: stream.messages.len(),
                last_seq: stream.last_seq(),
                appended: live_messages(stream).iter().map(|m| m.seq).collect(),
                deleted: stream.deleted.clone(),
                consumers: stream
                    .consumers
                    .iter()
                    .map(|(n, c)| {
                        (
                            n.clone(),
                            serde_json::to_string(&consumer_snapshot_json(c)).unwrap(),
                        )
                    })
                    .collect(),
            };
            // A brand-new stream has no deleted placeholders yet;
            // appended covers exactly the live messages.
            sh.appended = live_messages(stream).iter().map(|m| m.seq).collect();
            shadow.streams.insert(key, sh);
            continue;
        }
        // Existing stream: meta, then messages, then consumers.
        let sh = shadow.streams.get_mut(&key).unwrap();
        if meta != sh.meta {
            if let Err(e) = store.save_stream_meta(stream) {
                eprintln!("rust-natsd: wal meta {}: {e}", stream.name);
            }
            sh.meta = meta;
        }
        let rewrite = stream.wal_rewrite || stream.base_seq < sh.base_seq;
        if rewrite {
            let live = live_messages(stream);
            if let Err(e) = store.compact_msgs(&stream.account, &stream.name, &live) {
                eprintln!("rust-natsd: wal rewrite {}: {e}", stream.name);
            }
            sh.appended = live.iter().map(|m| m.seq).collect();
            sh.deleted = stream
                .deleted
                .iter()
                .copied()
                .filter(|s| *s >= stream.base_seq)
                .collect();
            stream.wal_rewrite = false;
        } else {
            // Tombstones for sequences evicted from the front
            // (max_msgs / max_bytes / max_age / unfiltered purge).
            if stream.base_seq > sh.base_seq {
                for seq in sh.base_seq..stream.base_seq {
                    if sh.appended.contains(&seq) {
                        if let Err(e) = store.delete_msg(&stream.account, &stream.name, seq) {
                            eprintln!("rust-natsd: wal tombstone {}: {e}", stream.name);
                        }
                        sh.appended.remove(&seq);
                    }
                }
            }
            // Tail appends: messages after the persisted prefix,
            // adjusted for front pops. Anything else (a tail that
            // does not continue the persisted sequence) falls
            // back to a full rewrite — correctness over speed.
            let popped = (stream.base_seq - sh.base_seq) as usize;
            let prefix_len = sh.len.saturating_sub(popped);
            let mut diverged = prefix_len > stream.messages.len();
            if !diverged && prefix_len < stream.messages.len() {
                let first_new = stream.messages.get(prefix_len).map(|m| m.seq).unwrap_or(0);
                if !stream.messages.is_empty()
                    && sh.last_seq != 0
                    && first_new != 0
                    && first_new <= sh.last_seq
                    && !sh.appended.contains(&first_new)
                {
                    diverged = true;
                }
            }
            if diverged {
                let live = live_messages(stream);
                if let Err(e) = store.compact_msgs(&stream.account, &stream.name, &live) {
                    eprintln!("rust-natsd: wal rewrite {}: {e}", stream.name);
                }
                sh.appended = live.iter().map(|m| m.seq).collect();
                sh.deleted = stream
                    .deleted
                    .iter()
                    .copied()
                    .filter(|s| *s >= stream.base_seq)
                    .collect();
            } else {
                for m in stream.messages.iter().skip(prefix_len) {
                    if sh.appended.contains(&m.seq) {
                        continue;
                    }
                    let owned = StoredMsg {
                        seq: m.seq,
                        subject: m.subject.clone(),
                        headers: m.headers.clone(),
                        payload: m.payload.clone(),
                        time_ns: m.time_ns,
                    };
                    if let Err(e) = store.append_msg(&stream.account, &stream.name, &owned) {
                        eprintln!("rust-natsd: wal append {}: {e}", stream.name);
                    }
                    sh.appended.insert(m.seq);
                }
                // Interior deletions (workqueue acks, per-subject
                // eviction) become tombstones.
                for seq in stream.deleted.iter() {
                    if sh.appended.contains(seq) && !sh.deleted.contains(seq) {
                        if let Err(e) = store.delete_msg(&stream.account, &stream.name, *seq) {
                            eprintln!("rust-natsd: wal tombstone {}: {e}", stream.name);
                        }
                    }
                }
                sh.deleted = stream
                    .deleted
                    .iter()
                    .copied()
                    .filter(|s| *s >= stream.base_seq)
                    .collect();
            }
        }
        // Consumers: append on change, delete on removal.
        let mut cur: HashMap<String, String> = HashMap::new();
        for (cname, c) in stream.consumers.iter() {
            let text = serde_json::to_string(&consumer_snapshot_json(c)).unwrap();
            if sh.consumers.get(cname).map(|t| t.as_str()) != Some(text.as_str()) {
                if let Err(e) = store.save_consumer_state(&stream.account, &stream.name, c) {
                    eprintln!("rust-natsd: wal consumer {}: {e}", stream.name);
                }
            }
            cur.insert(cname.clone(), text);
        }
        let gone: Vec<String> = sh
            .consumers
            .keys()
            .filter(|n| !cur.contains_key(*n))
            .cloned()
            .collect();
        for cname in gone {
            if let Err(e) = store.delete_consumer(&stream.account, &stream.name, &cname) {
                eprintln!("rust-natsd: wal consumer delete {}: {e}", stream.name);
            }
        }
        sh.consumers = cur;
        sh.base_seq = stream.base_seq;
        sh.len = stream.messages.len();
        sh.last_seq = stream.last_seq();
        // Compaction trigger (checked after this stream's sync so
        // the rewrite sees the just-persisted state).
        if store.needs_compaction(&stream.account, &stream.name) {
            let live = live_messages(stream);
            if let Err(e) = store.compact_msgs(&stream.account, &stream.name, &live) {
                eprintln!("rust-natsd: wal compact {}: {e}", stream.name);
            }
            sh.appended = live.iter().map(|m| m.seq).collect();
            sh.deleted = stream
                .deleted
                .iter()
                .copied()
                .filter(|s| *s >= stream.base_seq)
                .collect();
        }
    }
    // Deleted streams drop their whole WAL tree.
    let gone: Vec<(String, String)> = shadow
        .streams
        .values()
        .filter(|sh| !live_keys.contains(&format!("{}\0{}", sh.account, sh.name)))
        .map(|sh| (sh.account.clone(), sh.name.clone()))
        .collect();
    for (account, name) in gone {
        if let Err(e) = store.delete_stream(&account, &name) {
            eprintln!("rust-natsd: wal delete stream {name}: {e}");
        }
        shadow.streams.remove(&format!("{account}\0{name}"));
    }
}

// ---------------------------------------------------------------- Load / migrate

impl JetStream {
    /// Snapshot file name inside the store dir (legacy layout,
    /// kept as the migration source only).
    pub(crate) fn snapshot_path(dir: &Path) -> PathBuf {
        dir.join("rust-natsd-js.json")
    }

    /// Write path: append-only WAL diff-sync. A no-op without a
    /// store dir. Called after `handle_message` and after each
    /// redelivery tick, with the engine lock released for delivery
    /// (the actions pattern); this re-locks the engine briefly
    /// to diff, which cannot deadlock because no caller holds
    /// the engine lock across `persist`.
    pub(crate) fn persist(&self) {
        if self.store_dir.is_none() {
            return;
        }
        let mut wal = match self.wal.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let Some(store) = wal.as_mut() else {
            return;
        };
        let mut shadow = match self.shadow.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let mut engine = match self.engine.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        sync_engine(store, &mut engine, &mut shadow);
    }

    /// Load a legacy snapshot from the store dir, if one exists.
    /// Returns None when there is no snapshot or it is unreadable
    /// (logged; the engine starts empty, as before).
    pub(crate) fn load_snapshot(dir: &Path) -> Option<Engine> {
        let path = Self::snapshot_path(dir);
        let text = match fs::read_to_string(&path) {
            Ok(t) => t,
            Err(_) => return None,
        };
        match Engine::from_snapshot_json(&text) {
            Ok(engine) => Some(engine),
            Err(e) => {
                eprintln!(
                    "rust-natsd: jetstream snapshot {path:?} unreadable ({e}); starting empty"
                );
                None
            }
        }
    }

    /// True when any per-account WAL layout exists under `dir`.
    fn has_wal_layout(dir: &Path) -> bool {
        let Ok(rd) = fs::read_dir(dir) else {
            return false;
        };
        rd.flatten().any(|e| e.path().join("streams").is_dir())
    }

    /// Load the engine + store from the WAL layout, or return None
    /// when no WAL layout exists (fresh dir or legacy-only dir).
    /// Corrupt tails are truncated (logged) during replay.
    pub(crate) fn load_wal(dir: &Path) -> Option<(Engine, FileWalStore)> {
        if !Self::has_wal_layout(dir) {
            return None;
        }
        let mut store = FileWalStore::new(dir.to_path_buf());
        let mut stream_jsons: Vec<Value> = Vec::new();
        let Ok(accounts) = fs::read_dir(dir) else {
            return Some((
                Engine {
                    streams: HashMap::new(),
                },
                store,
            ));
        };
        let mut account_dirs: Vec<PathBuf> = accounts
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.join("streams").is_dir())
            .collect();
        account_dirs.sort();
        for adir in account_dirs {
            let Ok(streams_rd) = fs::read_dir(adir.join("streams")) else {
                continue;
            };
            let mut sdirs: Vec<PathBuf> = streams_rd
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.is_dir())
                .collect();
            sdirs.sort();
            for sdir in sdirs {
                let meta_text = match fs::read_to_string(sdir.join("meta.json")) {
                    Ok(t) => t,
                    Err(_) => {
                        eprintln!("rust-natsd: wal stream {sdir:?} has no meta.json; skipping");
                        continue;
                    }
                };
                let Ok(mut meta) = serde_json::from_str::<Value>(&meta_text) else {
                    eprintln!("rust-natsd: wal meta {sdir:?} unreadable; skipping stream");
                    continue;
                };
                let account = meta
                    .get("account")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let name = meta
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let base_seq = meta.get("base_seq").and_then(|v| v.as_u64()).unwrap_or(1);
                // Messages.
                let mut messages_json: Vec<Value> = Vec::new();
                let msgs_path = sdir.join("msgs.wal");
                if let Ok(data) = fs::read(&msgs_path) {
                    let replay = replay_msgs(&data);
                    if replay.truncated {
                        eprintln!(
                            "rust-natsd: wal {msgs_path:?} corrupt/truncated tail at byte {}; truncating to last good record",
                            replay.good_len
                        );
                        truncate_to(&msgs_path, replay.good_len);
                    }
                    store.seed_stats(&account, &name, &replay, base_seq);
                    let mut seqs: Vec<u64> = replay.by_seq.keys().copied().collect();
                    seqs.sort_unstable();
                    use base64::Engine as _;
                    let enc = base64::engine::general_purpose::STANDARD;
                    for seq in seqs {
                        let m = &replay.by_seq[&seq];
                        messages_json.push(json!({
                            "seq": m.seq,
                            "subject": m.subject,
                            "headers": m.headers.as_ref().map(|h| enc.encode(h.as_ref())),
                            "payload": enc.encode(m.payload.as_ref()),
                            "time_ns": m.time_ns,
                        }));
                    }
                }
                // Consumers: last record per file wins.
                let mut consumers_json: Vec<Value> = Vec::new();
                let cdir = sdir.join("consumers");
                if let Ok(rd) = fs::read_dir(&cdir) {
                    let mut files: Vec<PathBuf> = rd
                        .flatten()
                        .map(|e| e.path())
                        .filter(|p| p.is_file())
                        .collect();
                    files.sort();
                    for fpath in files {
                        if let Ok(data) = fs::read(&fpath) {
                            let replay = replay_consumers(&data);
                            if replay.truncated {
                                eprintln!(
                                    "rust-natsd: wal {fpath:?} corrupt/truncated tail at byte {}; truncating",
                                    replay.good_len
                                );
                                truncate_to(&fpath, replay.good_len);
                            }
                            if let Some(text) = replay.last_json {
                                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                    consumers_json.push(v);
                                }
                                let cname = fpath
                                    .file_stem()
                                    .map(|s| s.to_string_lossy().into_owned())
                                    .unwrap_or_default();
                                store.cons_counts.insert(
                                    FileWalStore::consumer_key(&account, &name, &cname),
                                    replay.count,
                                );
                            }
                        }
                    }
                }
                if let Some(obj) = meta.as_object_mut() {
                    obj.insert("messages".to_string(), Value::Array(messages_json));
                    obj.insert("consumers".to_string(), Value::Array(consumers_json));
                }
                stream_jsons.push(meta);
            }
        }
        let text =
            serde_json::to_string(&json!({ "version": 2, "streams": stream_jsons })).unwrap();
        match Engine::from_snapshot_json(&text) {
            Ok(engine) => Some((engine, store)),
            Err(e) => {
                eprintln!("rust-natsd: wal replay failed ({e}); starting empty");
                Some((
                    Engine {
                        streams: HashMap::new(),
                    },
                    store,
                ))
            }
        }
    }

    /// Startup load: WAL layout first; else a one-time legacy
    /// migration; else a fresh store. Returns engine, store
    /// (always present when a dir is configured), and shadow.
    pub(crate) fn load_engine_and_store(dir: &Path) -> (Engine, FileWalStore, Shadow) {
        if let Some((engine, store)) = Self::load_wal(dir) {
            let shadow = Shadow::from_engine(&engine);
            return (engine, store, shadow);
        }
        if let Some(mut engine) = Self::load_snapshot(dir) {
            eprintln!("rust-natsd: migrating legacy jetstream snapshot to WAL layout in {dir:?}");
            let mut store = FileWalStore::new(dir.to_path_buf());
            let mut shadow = Shadow::default();
            sync_engine(&mut store, &mut engine, &mut shadow);
            return (engine, store, shadow);
        }
        (
            Engine {
                streams: HashMap::new(),
            },
            FileWalStore::new(dir.to_path_buf()),
            Shadow::default(),
        )
    }
}

impl Engine {
    /// Serialize the whole engine (legacy snapshot v2 format, kept
    /// as the migration source; the WAL write path no longer uses
    /// it). Streams, messages (payloads and headers base64-encoded),
    /// dedup ids, and consumer definitions with positions, pending
    /// ack state, ack floors, redelivery counts, and pull redelivery
    /// queues.
    /// Only used by tests (round-trip proof for the WAL-load
    /// assembly format) in production builds, hence the allow.
    #[allow(dead_code)]
    pub(crate) fn snapshot_json(&self) -> Value {
        use base64::Engine as _;
        let enc = base64::engine::general_purpose::STANDARD;
        let streams: Vec<Value> = self
            .streams
            .values()
            .map(|s| {
                let messages: Vec<Value> = s
                    .messages
                    .iter()
                    .filter(|m| !s.deleted.contains(&m.seq))
                    .map(|m| {
                        json!({
                            "seq": m.seq,
                            "subject": m.subject,
                            "headers": m.headers.as_ref().map(|h| enc.encode(h.as_ref())),
                            "payload": enc.encode(m.payload.as_ref()),
                            "time_ns": m.time_ns,
                        })
                    })
                    .collect();
                let consumers: Vec<Value> =
                    s.consumers.values().map(consumer_snapshot_json).collect();
                let mut stream_json = stream_meta_json(s);
                if let Some(obj) = stream_json.as_object_mut() {
                    obj.insert("messages".to_string(), Value::Array(messages));
                    obj.insert("consumers".to_string(), Value::Array(consumers));
                }
                stream_json
            })
            .collect();
        json!({ "version": 2, "streams": streams })
    }
}

impl Engine {
    /// Restore an engine from `snapshot_json` output. Deleted
    /// sequences are not stored (deleted messages are simply absent,
    /// so a restored stream's first_seq may jump past a deleted
    /// prefix — equivalent, since those messages are gone).
    /// Used by legacy migration and by WAL load, which assembles
    /// a snapshot-v2 document from meta.json + WAL replay.
    pub(crate) fn from_snapshot_json(text: &str) -> Result<Engine, String> {
        use base64::Engine as _;
        use std::time::Duration;
        let dec = base64::engine::general_purpose::STANDARD;
        let root: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let mut engine = Engine {
            streams: HashMap::new(),
        };
        let Some(streams) = root.get("streams").and_then(|v| v.as_array()) else {
            return Err("snapshot has no streams array".to_string());
        };
        for sv in streams {
            let get_s = |k: &str| sv.get(k).and_then(|v| v.as_str()).unwrap_or("");
            let get_i = |k: &str| sv.get(k).and_then(|v| v.as_i64()).unwrap_or(0);
            let get_u = |k: &str| sv.get(k).and_then(|v| v.as_u64()).unwrap_or(0);
            let get_b = |k: &str| sv.get(k).and_then(|v| v.as_bool()).unwrap_or(false);
            let parse_snap_cfg = |v: &Value| -> SourceCfg {
                SourceCfg {
                    name: v
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("")
                        .to_string(),
                    filter_subject: v
                        .get("filter_subject")
                        .and_then(|f| f.as_str())
                        .map(|s| s.to_string()),
                    start_seq: v.get("opt_start_seq").and_then(|n| n.as_u64()).unwrap_or(0),
                    start_time_ns: v
                        .get("opt_start_time_ns")
                        .and_then(|n| n.as_u64())
                        .unwrap_or(0),
                    live: v.get("live").and_then(|n| n.as_bool()).unwrap_or(true),
                }
            };
            let mirror = sv.get("mirror").and_then(|v| {
                if v.is_null() {
                    None
                } else {
                    Some(parse_snap_cfg(v))
                }
            });
            let sources: Vec<SourceCfg> = sv
                .get("sources")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().map(&parse_snap_cfg).collect())
                .unwrap_or_default();
            let created_ns = get_u("created_ns");
            let mut stream = Stream {
                name: get_s("name").to_string(),
                account: {
                    let a = get_s("account");
                    if a.is_empty() {
                        "$G".to_string()
                    } else {
                        a.to_string()
                    }
                },
                created_ns,
                subjects: sv
                    .get("subjects")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default(),
                retention: get_s("retention").to_string(),
                max_msgs: get_i("max_msgs"),
                max_bytes: get_i("max_bytes"),
                max_age_ns: get_i("max_age_ns"),
                max_msgs_per_subject: get_i("max_msgs_per_subject"),
                deleted: std::collections::HashSet::new(),
                storage: get_s("storage").to_string(),
                messages: std::collections::VecDeque::new(),
                base_seq: get_u("base_seq"),
                msg_ids: HashMap::new(),
                consumers: HashMap::new(),
                mirror,
                sources,
                source_token: sv
                    .get("source_token")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| make_source_token(get_s("name"), created_ns)),
                // Pre-Phase-31 snapshots lack these keys and load
                // as false, matching the old hardcoded info_json.
                allow_direct: get_b("allow_direct"),
                mirror_direct: get_b("mirror_direct"),
                deny_delete: get_b("deny_delete"),
                deny_purge: get_b("deny_purge"),
                allow_rollup_hdrs: get_b("allow_rollup_hdrs"),
                wal_rewrite: false,
            };
            if stream.base_seq == 0 {
                stream.base_seq = 1;
            }
            let mut by_seq: HashMap<u64, StoredMsg> = HashMap::new();
            if let Some(msgs) = sv.get("messages").and_then(|v| v.as_array()) {
                for mv in msgs {
                    let payload = mv
                        .get("payload")
                        .and_then(|v| v.as_str())
                        .and_then(|b| dec.decode(b).ok())
                        .unwrap_or_default();
                    let headers = mv
                        .get("headers")
                        .and_then(|v| v.as_str())
                        .and_then(|b| dec.decode(b).ok())
                        .map(Bytes::from);
                    let stored = StoredMsg {
                        seq: mv.get("seq").and_then(|v| v.as_u64()).unwrap_or(0),
                        subject: mv
                            .get("subject")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        headers,
                        payload: Bytes::from(payload),
                        time_ns: mv.get("time_ns").and_then(|v| v.as_u64()).unwrap_or(0),
                    };
                    by_seq.insert(stored.seq, stored);
                }
            }
            if let Some(last) = by_seq.keys().max().copied() {
                let base = stream.base_seq;
                for seq in base..=last {
                    match by_seq.remove(&seq) {
                        Some(m) => stream.messages.push_back(m),
                        None => {
                            stream.deleted.insert(seq);
                            stream.messages.push_back(StoredMsg {
                                seq,
                                subject: String::new(),
                                headers: None,
                                payload: Bytes::new(),
                                time_ns: 0,
                            });
                        }
                    }
                }
            }
            if let Some(ids) = sv.get("msg_ids").and_then(|v| v.as_object()) {
                for (id, seq) in ids {
                    if let Some(seq) = seq.as_u64() {
                        stream
                            .msg_ids
                            .insert(id.clone(), (seq, std::time::Instant::now()));
                    }
                }
            }
            if let Some(consumers) = sv.get("consumers").and_then(|v| v.as_array()) {
                for cv in consumers {
                    let ack_policy = match cv
                        .get("ack_policy")
                        .and_then(|v| v.as_str())
                        .unwrap_or("explicit")
                    {
                        "none" => AckPolicy::None,
                        "all" => AckPolicy::All,
                        _ => AckPolicy::Explicit,
                    };
                    let name = cv
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let ack_wait_ns = cv.get("ack_wait_ns").and_then(|v| v.as_u64()).unwrap_or(0);
                    let ack_wait = if ack_wait_ns == 0 {
                        DEFAULT_ACK_WAIT
                    } else {
                        Duration::from_nanos(ack_wait_ns)
                    };
                    let is_push = cv.get("deliver_subject").and_then(|v| v.as_str()).is_some();
                    let max_deliver = cv.get("max_deliver").and_then(|v| v.as_i64()).unwrap_or(-1);
                    let now = std::time::Instant::now();
                    let now_wall = now_ns();
                    let mut pending: HashMap<u64, Pending> = HashMap::new();
                    let mut expired: Vec<Redeliver> = Vec::new();
                    let mut num_redelivered = cv
                        .get("num_redelivered")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    if let Some(entries) = cv.get("pending").and_then(|v| v.as_array()) {
                        let mut restored: Vec<(u64, u64, u64, u64)> = entries
                            .iter()
                            .filter_map(|pv| {
                                Some((
                                    pv.get("consumer_seq")?.as_u64()?,
                                    pv.get("stream_seq")?.as_u64()?,
                                    pv.get("delivered")?.as_u64()?,
                                    pv.get("delivered_at_ns")?.as_u64()?,
                                ))
                            })
                            .collect();
                        restored.sort_by_key(|(_, sseq, _, _)| *sseq);
                        for (cseq, sseq, delivered, delivered_at_ns) in restored {
                            if max_deliver > 0 && delivered >= max_deliver as u64 {
                                continue;
                            }
                            let deadline_wall =
                                delivered_at_ns.saturating_add(ack_wait.as_nanos() as u64);
                            if deadline_wall <= now_wall {
                                if is_push {
                                    pending.insert(
                                        cseq,
                                        Pending {
                                            stream_seq: sseq,
                                            delivered,
                                            deadline: now,
                                            delivered_at_ns,
                                        },
                                    );
                                } else {
                                    expired.push(Redeliver {
                                        stream_seq: sseq,
                                        delivered: delivered + 1,
                                    });
                                    num_redelivered += 1;
                                }
                            } else {
                                let remaining = Duration::from_nanos(deadline_wall - now_wall);
                                pending.insert(
                                    cseq,
                                    Pending {
                                        stream_seq: sseq,
                                        delivered,
                                        deadline: now + remaining,
                                        delivered_at_ns,
                                    },
                                );
                            }
                        }
                    }
                    let mut redeliver: std::collections::VecDeque<Redeliver> =
                        std::collections::VecDeque::new();
                    if let Some(entries) = cv.get("redeliver").and_then(|v| v.as_array()) {
                        for rv in entries {
                            if let (Some(sseq), Some(delivered)) = (
                                rv.get("stream_seq").and_then(|v| v.as_u64()),
                                rv.get("delivered").and_then(|v| v.as_u64()),
                            ) {
                                if max_deliver > 0 && delivered > max_deliver as u64 {
                                    continue;
                                }
                                redeliver.push_back(Redeliver {
                                    stream_seq: sseq,
                                    delivered,
                                });
                            }
                        }
                    }
                    redeliver.extend(expired);
                    stream.consumers.insert(
                        name.clone(),
                        Consumer {
                            name,
                            created_ns: now_ns(),
                            deliver_subject: cv
                                .get("deliver_subject")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            filter_subject: cv
                                .get("filter_subject")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            filter_subjects: cv
                                .get("filter_subjects")
                                .and_then(|v| v.as_array())
                                .map(|a| {
                                    a.iter()
                                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                        .collect()
                                })
                                .unwrap_or_default(),
                            ack_policy,
                            ack_wait,
                            max_deliver,
                            max_ack_pending: cv
                                .get("max_ack_pending")
                                .and_then(|v| v.as_i64())
                                .unwrap_or(1000),
                            deliver_policy: cv
                                .get("deliver_policy")
                                .and_then(|v| v.as_str())
                                .unwrap_or("all")
                                .to_string(),
                            next_stream_seq: cv
                                .get("next_stream_seq")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(1),
                            consumer_seq: cv
                                .get("consumer_seq")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0),
                            pending,
                            redeliver,
                            waiting_pulls: std::collections::VecDeque::new(),
                            ack_floor_stream_seq: cv
                                .get("ack_floor_stream_seq")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0),
                            ack_floor_consumer_seq: cv
                                .get("ack_floor_consumer_seq")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0),
                            num_redelivered,
                        },
                    );
                }
            }
            engine.streams.insert(stream.name.clone(), stream);
        }
        Ok(engine)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(seq: u64, subject: &str, payload: &str) -> StoredMsg {
        StoredMsg {
            seq,
            subject: subject.to_string(),
            headers: None,
            payload: Bytes::from(payload.to_string()),
            time_ns: 1_000_000 + seq,
        }
    }

    #[test]
    fn frame_roundtrip_and_crc_rejection() {
        let msg = stored(7, "orders.1", "hello");
        let frame = encode_append(&msg);
        let (frames, good, truncated) = parse_frames(&frame);
        assert_eq!(frames.len(), 1);
        assert!(!truncated);
        assert_eq!(good, frame.len());
        assert_eq!(frames[0].seq, 7);
        assert_eq!(frames[0].payload, b"hello");
        // Flip one payload byte: CRC must reject the record.
        let mut bad = frame.clone();
        let last = bad.len() - 1;
        bad[last] ^= 0xff;
        let (frames, good, truncated) = parse_frames(&bad);
        assert_eq!(frames.len(), 0);
        assert!(truncated);
        assert_eq!(good, 0);
        // A good record followed by a corrupt tail keeps the prefix.
        let mut two = frame.clone();
        two.extend_from_slice(&bad);
        let (frames, good, truncated) = parse_frames(&two);
        assert_eq!(frames.len(), 1);
        assert!(truncated);
        assert_eq!(good, frame.len());
        // A torn partial record is a truncated tail, not data.
        let partial = &frame[..frame.len() - 3];
        let (frames, _, truncated) = parse_frames(partial);
        assert_eq!(frames.len(), 0);
        assert!(truncated);
    }

    #[test]
    fn snapshot_json_roundtrips_through_from_snapshot_json() {
        // snapshot_json is the migration-source format and the
        // shape WAL load assembles, so it must round-trip even
        // though the WAL write path no longer calls it.
        let mut engine = Engine {
            streams: HashMap::new(),
        };
        let mut stream = Stream {
            name: "S".to_string(),
            account: "$G".to_string(),
            created_ns: 42,
            subjects: vec!["s.>".to_string()],
            retention: "limits".to_string(),
            max_msgs: 0,
            max_bytes: 0,
            max_age_ns: 0,
            max_msgs_per_subject: 0,
            deleted: HashSet::new(),
            storage: "file".to_string(),
            messages: std::collections::VecDeque::new(),
            base_seq: 1,
            msg_ids: HashMap::new(),
            consumers: HashMap::new(),
            mirror: None,
            sources: Vec::new(),
            source_token: "tok".to_string(),
            allow_direct: false,
            mirror_direct: false,
            deny_delete: false,
            deny_purge: false,
            allow_rollup_hdrs: false,
            wal_rewrite: false,
        };
        stream.messages.push_back(stored(1, "s.a", "hello"));
        stream.consumers.insert(
            "c".to_string(),
            Consumer {
                name: "c".to_string(),
                created_ns: 43,
                deliver_subject: None,
                filter_subject: None,
                filter_subjects: Vec::new(),
                ack_policy: AckPolicy::Explicit,
                ack_wait: std::time::Duration::from_secs(30),
                max_deliver: -1,
                max_ack_pending: 1000,
                deliver_policy: "all".to_string(),
                next_stream_seq: 2,
                consumer_seq: 1,
                pending: HashMap::new(),
                redeliver: std::collections::VecDeque::new(),
                waiting_pulls: std::collections::VecDeque::new(),
                ack_floor_stream_seq: 0,
                ack_floor_consumer_seq: 0,
                num_redelivered: 0,
            },
        );
        engine.streams.insert("S".to_string(), stream);
        let text = serde_json::to_string(&engine.snapshot_json()).unwrap();
        let restored = Engine::from_snapshot_json(&text).unwrap();
        let s = &restored.streams["S"];
        assert_eq!(s.messages.len(), 1);
        assert_eq!(s.messages[0].payload.as_ref(), b"hello");
        assert!(s.consumers.contains_key("c"));
    }

    #[test]
    fn crc32_matches_known_vector() {
        // CRC-32/ISO-HDLC check value for "123456789".
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
    }

    #[test]
    fn store_compaction_trigger_and_rewrite() {
        let dir = std::env::temp_dir().join(format!("wal-unit-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut store = FileWalStore::new(dir.clone());
        store.set_compact_threshold_bytes(100);
        let mut stream = Stream {
            name: "S".to_string(),
            account: "$G".to_string(),
            created_ns: 42,
            subjects: vec!["s.>".to_string()],
            retention: "limits".to_string(),
            max_msgs: 0,
            max_bytes: 0,
            max_age_ns: 0,
            max_msgs_per_subject: 0,
            deleted: HashSet::new(),
            storage: "file".to_string(),
            messages: std::collections::VecDeque::new(),
            base_seq: 1,
            msg_ids: HashMap::new(),
            consumers: HashMap::new(),
            mirror: None,
            sources: Vec::new(),
            source_token: "tok".to_string(),
            allow_direct: false,
            mirror_direct: false,
            deny_delete: false,
            deny_purge: false,
            allow_rollup_hdrs: false,
            wal_rewrite: false,
        };
        store.save_stream_meta(&stream).unwrap();
        for i in 1..=10u64 {
            store
                .append_msg("$G", "S", &stored(i, "s.a", "payload-0123456789"))
                .unwrap();
        }
        // Tombstone 8 of 10: dead bytes exceed 50% of the file.
        for i in 1..=8u64 {
            store.delete_msg("$G", "S", i).unwrap();
        }
        assert!(store.needs_compaction("$G", "S"));
        let live: Vec<StoredMsg> = (9..=10)
            .map(|i| stored(i, "s.a", "payload-0123456789"))
            .collect();
        store.compact_msgs("$G", "S", &live).unwrap();
        assert!(!store.needs_compaction("$G", "S"));
        let data = fs::read(store.msgs_path("$G", "S")).unwrap();
        let replay = replay_msgs(&data);
        assert_eq!(replay.by_seq.len(), 2);
        assert_eq!(replay.by_seq[&9].payload.as_ref(), b"payload-0123456789");
        assert_eq!(replay.by_seq[&10].payload.as_ref(), b"payload-0123456789");
        // Stream struct is unused beyond meta here; keep clippy quiet.
        stream.base_seq = 1;
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_10k_records_wall_time() {
        // Direct-store throughput (no client RTT): 10,000 small
        // appends must be comfortably under the 1 s plan gate.
        let dir = std::env::temp_dir().join(format!("wal-bench-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut store = FileWalStore::new(dir.clone());
        let start = std::time::Instant::now();
        for i in 1..=10_000u64 {
            store
                .append_msg("$G", "S", &stored(i, "s.a", "0123456789abcdef"))
                .unwrap();
        }
        let elapsed = start.elapsed();
        eprintln!("wal direct-append 10k records: {elapsed:?}");
        assert!(
            elapsed.as_secs_f64() < 1.0,
            "direct WAL append of 10k records took {elapsed:?}, over the 1s gate"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
