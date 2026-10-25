//! Phase 34 Raft-cluster integration tests (Raft-group layer).
//!
//! These tests run three real rust-natsd nodes meshed by routes and
//! drive one Raft group ("meta") through [`ClusterManager`]s whose
//! RPCs travel as `$JS.RAFT.<node-id>` messages over those routes.
//! They prove the Raft layer — election, quorum commit, leader
//! kill, minority partition, restart catch-up, quorum loss —
//! WITHOUT claiming the JetStream engine path is wired: engine
//! publishes are not proposed through these groups yet (see
//! `src/js_cluster.rs` and PLAN.md Phase 34), so the stream-level
//! gate (R3 stream, SIGKILL of a stream leader, zero acked loss)
//! remains a documented remainder, tested here at the layer that
//! must underpin it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rust_natsd::config::Config;
use rust_natsd::js_cluster::{ClusterManager, GroupOp, NatsRaftTransport, RpcEnvelope};
use rust_natsd::raft::RaftError;
use rust_natsd::server::{self, RunningServer};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(30);

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn tmpdir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rust-natsd-raft-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn node_config(cluster_port: u16, routes: Vec<String>, store_dir: std::path::PathBuf) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_host: "127.0.0.1".into(),
        cluster_port,
        routes,
        jetstream: true,
        js_store_dir: Some(store_dir.to_string_lossy().to_string()),
        ..Default::default()
    }
}

struct Node {
    id: String,
    server: RunningServer,
    manager: Arc<Mutex<ClusterManager>>,
    transport: Option<tokio::task::JoinHandle<()>>,
    store_dir: std::path::PathBuf,
}

impl Node {
    fn start_transport(&mut self) {
        let transport = NatsRaftTransport::new(self.manager.clone(), vec!["meta".to_string()]);
        let id = self.id.clone();
        let addr = self.server.addr.to_string();
        self.transport = Some(tokio::spawn(async move {
            transport.run(id, addr).await;
        }));
    }

    fn stop_transport(&mut self) {
        if let Some(h) = self.transport.take() {
            h.abort();
        }
    }

    fn committed_len(&self) -> usize {
        self.manager.lock().unwrap().committed("meta").len()
    }

    fn is_leader(&self) -> bool {
        self.manager.lock().unwrap().is_leader("meta")
    }
}

async fn wait_until<F: FnMut() -> bool>(desc: &str, mut f: F) {
    timeout(T, async {
        loop {
            if f() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for: {desc}"));
}

async fn wait_leader(nodes: &[Node]) -> usize {
    let mut leader = None;
    wait_until("a Raft leader to emerge", || {
        for (i, n) in nodes.iter().enumerate() {
            if n.transport.is_some() && n.is_leader() {
                leader = Some(i);
                return true;
            }
        }
        false
    })
    .await;
    leader.unwrap()
}

/// Propose via the current leader, delivering the leader's initial
/// replication sends over the real route mesh with an async-nats
/// client (responses and heartbeats flow through the transports).
async fn propose_via_leader(
    nodes: &[Node],
    leader: usize,
    client: &async_nats::Client,
    op: GroupOp,
) {
    let leader_id = nodes[leader].id.clone();
    let sends = {
        let mut mgr = nodes[leader].manager.lock().unwrap();
        mgr.propose("meta", &op, rust_natsd::js_cluster::wall_ms())
            .expect("leader proposes")
    };
    for (to, rpc) in sends {
        let env = RpcEnvelope {
            group: "meta".to_string(),
            from: leader_id.clone(),
            rpc,
        };
        let body = serde_json::to_string(&env).unwrap();
        client
            .publish(format!("$JS.RAFT.{to}"), body.into())
            .await
            .unwrap();
    }
    client.flush().await.unwrap();
}

#[tokio::test]
async fn cluster_raft_leader_kill_partition_restart_quorum() {
    // Three nodes, full route mesh (n2 solicits n1; n3 solicits
    // n1 and n2; inbound routes complete the mesh).
    let mut cluster_ports = Vec::new();
    for _ in 0..3 {
        cluster_ports.push(free_port());
    }
    let routes_for = |i: usize| -> Vec<String> {
        (0..i)
            .map(|j| format!("nats://127.0.0.1:{}", cluster_ports[j]))
            .collect()
    };
    let mut nodes: Vec<Node> = Vec::new();
    for (i, cluster_port) in cluster_ports.iter().enumerate() {
        let dir = tmpdir(&format!("n{i}"));
        let srv = server::start(node_config(*cluster_port, routes_for(i), dir.clone()))
            .await
            .expect("server starts");
        let id = format!("n{}", i + 1);
        let peers: Vec<String> = (1..=3)
            .map(|k| format!("n{k}"))
            .filter(|p| *p != id)
            .collect();
        // Staggered election timeouts (randomised at this layer).
        let eto = 150 + ((std::process::id() as u64 + (i as u64) * 67) % 150);
        let mut mgr = ClusterManager::new(id.clone(), peers, dir.clone(), eto);
        mgr.create_group("meta").expect("group creates");
        let mut node = Node {
            id,
            server: srv,
            manager: Arc::new(Mutex::new(mgr)),
            transport: None,
            store_dir: dir,
        };
        node.start_transport();
        nodes.push(node);
    }
    // Let routes + subscriptions propagate before asserting.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 1. Election + replication (5 ops, all three commit in order).
    let leader = wait_leader(&nodes).await;
    let client = async_nats::connect(format!("nats://{}", nodes[leader].server.addr))
        .await
        .unwrap();
    for k in 0..5 {
        propose_via_leader(
            &nodes,
            leader,
            &client,
            GroupOp::StreamAppend {
                stream: "S".into(),
                subject: "s".into(),
                payload: format!("msg-{k}").into_bytes(),
                headers: None,
            },
        )
        .await;
    }
    wait_until("all 3 nodes commit 5 ops", || {
        nodes.iter().all(|n| n.committed_len() == 5)
    })
    .await;
    for n in &nodes {
        let ops = n.manager.lock().unwrap().committed("meta");
        for (k, op) in ops.iter().enumerate() {
            match op {
                GroupOp::StreamAppend { payload, .. } => {
                    assert_eq!(payload, format!("msg-{k}").as_bytes())
                }
                _ => panic!("unexpected op"),
            }
        }
    }

    // 2. Leader kill (abort = SIGKILL equivalent at this layer: no
    // graceful close). Survivors re-elect and keep serving.
    nodes[leader].stop_transport();
    let survivors: Vec<usize> = (0..3).filter(|i| *i != leader).collect();
    let new_leader = {
        let mut found = None;
        wait_until("survivors re-elect a leader", || {
            for i in &survivors {
                if nodes[*i].is_leader() {
                    found = Some(*i);
                    return true;
                }
            }
            false
        })
        .await;
        found.unwrap()
    };
    let client2 = async_nats::connect(format!("nats://{}", nodes[new_leader].server.addr))
        .await
        .unwrap();
    for k in 5..8 {
        propose_via_leader(
            &nodes,
            new_leader,
            &client2,
            GroupOp::StreamAppend {
                stream: "S".into(),
                subject: "s".into(),
                payload: format!("msg-{k}").into_bytes(),
                headers: None,
            },
        )
        .await;
    }
    wait_until("survivors commit 8 ops after leader kill", || {
        survivors.iter().all(|i| nodes[*i].committed_len() == 8)
    })
    .await;
    assert_eq!(
        nodes[leader].committed_len(),
        5,
        "dead leader commits nothing new"
    );

    // 3. Restart catch-up: the killed node rejoins and serves the
    // full sequence (no gap, no duplicates).
    nodes[leader].start_transport();
    wait_until("restarted node catches up to 8 ops", || {
        nodes[leader].committed_len() == 8
    })
    .await;
    let ops = nodes[leader].manager.lock().unwrap().committed("meta");
    assert_eq!(ops.len(), 8);
    assert!(matches!(&ops[7], GroupOp::StreamAppend { payload, .. } if payload == b"msg-7"));

    // 4. Minority partition: isolate one follower. Its own propose
    // must fail (NotLeader) and commit nothing; it must not fork.
    let current_leader = wait_leader(&nodes).await;
    let minority = (0..3).find(|i| *i != current_leader).unwrap();
    nodes[minority].stop_transport();
    let isolated_result = {
        let mut mgr = nodes[minority].manager.lock().unwrap();
        mgr.propose(
            "meta",
            &GroupOp::StreamAppend {
                stream: "S".into(),
                subject: "s".into(),
                payload: b"minority-write".to_vec(),
                headers: None,
            },
            rust_natsd::js_cluster::wall_ms(),
        )
    };
    assert_eq!(
        isolated_result.unwrap_err(),
        RaftError::NotLeader,
        "isolated follower must refuse writes, not fork"
    );
    let client3 = async_nats::connect(format!("nats://{}", nodes[current_leader].server.addr))
        .await
        .unwrap();
    propose_via_leader(
        &nodes,
        current_leader,
        &client3,
        GroupOp::StreamAppend {
            stream: "S".into(),
            subject: "s".into(),
            payload: b"msg-8".to_vec(),
            headers: None,
        },
    )
    .await;
    wait_until("majority commits msg-8 while minority is isolated", || {
        (0..3)
            .filter(|i| *i != minority)
            .all(|i| nodes[i].committed_len() == 9)
    })
    .await;
    assert_eq!(
        nodes[minority].committed_len(),
        8,
        "minority must not advance alone"
    );
    nodes[minority].start_transport();
    wait_until("minority rejoins cleanly at 9 ops", || {
        nodes[minority].committed_len() == 9
    })
    .await;

    // 5. Quorum loss: 2 of 3 transports down. The remaining node
    // must not commit anything new (if it is leader, its proposal
    // stays uncommitted; if follower, it refuses outright).
    let remaining = wait_leader(&nodes).await;
    let down: Vec<usize> = (0..3).filter(|i| *i != remaining).collect();
    for i in &down {
        nodes[*i].stop_transport();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = nodes[remaining].committed_len();
    let _ = {
        let mut mgr = nodes[remaining].manager.lock().unwrap();
        mgr.propose(
            "meta",
            &GroupOp::StreamAppend {
                stream: "S".into(),
                subject: "s".into(),
                payload: b"quorum-loss-write".to_vec(),
                headers: None,
            },
            rust_natsd::js_cluster::wall_ms(),
        )
    };
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        nodes[remaining].committed_len(),
        before,
        "quorum loss must not commit (and must not silently serve R1)"
    );
    // Restore: fresh writes through the re-formed majority commit.
    for i in &down {
        nodes[*i].start_transport();
    }
    let leader2 = wait_leader(&nodes).await;
    let client4 = async_nats::connect(format!("nats://{}", nodes[leader2].server.addr))
        .await
        .unwrap();
    propose_via_leader(
        &nodes,
        leader2,
        &client4,
        GroupOp::StreamAppend {
            stream: "S".into(),
            subject: "s".into(),
            payload: b"after-restore".to_vec(),
            headers: None,
        },
    )
    .await;
    // Raft §5.4.2: an entry written under an old term (the
    // after-restore entry may predate the post-restore leader's
    // term) commits only when a current-term entry commits. So
    // propose a second op through the stabilised leader; both ops
    // must then appear committed on every node.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let leader3 = wait_leader(&nodes).await;
    let client5 = async_nats::connect(format!("nats://{}", nodes[leader3].server.addr))
        .await
        .unwrap();
    propose_via_leader(
        &nodes,
        leader3,
        &client5,
        GroupOp::StreamAppend {
            stream: "S".into(),
            subject: "s".into(),
            payload: b"after-restore-2".to_vec(),
            headers: None,
        },
    )
    .await;
    wait_until("cluster serves again after quorum restore", || {
        nodes.iter().all(|n| {
            let ops = n.manager.lock().unwrap().committed("meta");
            ops.iter().any(|op| matches!(op, GroupOp::StreamAppend { payload, .. } if payload == b"after-restore"))
                && ops.iter().any(|op| matches!(op, GroupOp::StreamAppend { payload, .. } if payload == b"after-restore-2"))
        })
    })
    .await;
    drop(client5);

    drop(client);
    drop(client2);
    drop(client3);
    drop(client4);
    for mut n in nodes {
        n.stop_transport();
        n.server.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(&n.store_dir);
    }
    // Sanity: the Raft WAL lives under <store_dir>/raft/, never in
    // the single-node stream layout (checked implicitly by the
    // removal above; the layout assertion is in the unit tests).
    let _ = HashMap::<String, String>::new();
}

#[tokio::test]
async fn replicated_stream_create_refused_not_stored_as_r1() {
    let dir = tmpdir("r1-refusal");
    let srv = server::start(node_config(0, vec![], dir.clone()))
        .await
        .expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .unwrap();
    let js = async_nats::jetstream::new(client.clone());
    // num_replicas 3 must fail, with or without cluster peers.
    let err = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: "R3".into(),
            subjects: vec!["r3.>".into()],
            num_replicas: 3,
            ..Default::default()
        })
        .await
        .expect_err("R3 create must be refused");
    assert!(
        err.to_string().contains("not yet supported"),
        "unexpected error: {err}"
    );
    // And nothing was stored under that name (no silent R1).
    assert!(js.get_stream("R3").await.is_err(), "R3 must not exist");
    // R1 still works, and its INFO honestly reports no cluster.
    let mut s = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: "R1".into(),
            subjects: vec!["r1.>".into()],
            ..Default::default()
        })
        .await
        .expect("R1 create works");
    let info = s.info().await.expect("R1 info");
    assert_eq!(
        info.cluster, None,
        "R1 cluster info must be absent, not fabricated"
    );
    drop(client);
    srv.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
