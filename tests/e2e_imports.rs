//! Account imports/exports integration tests (gap area 6):
//! service imports with local/remote mapping and reply return,
//! stream imports, denied imports, and local allow/deny preservation.
//! All through real raw clients, not mapping-table unit tests.

use rust_natsd::config::{Account, AccountUser, Config, Export, Import, ImportKind, Permissions};
use rust_natsd::server;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Raw {
    stream: tokio::net::TcpStream,
    buf: Vec<u8>,
}

impl Raw {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        Raw {
            stream: tokio::net::TcpStream::connect(addr).await.unwrap(),
            buf: Vec::new(),
        }
    }

    async fn read_line(&mut self) -> String {
        loop {
            if let Some(pos) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let line = self.buf.drain(..pos + 2).collect::<Vec<_>>();
                return String::from_utf8_lossy(&line[..line.len() - 2]).into_owned();
            }
            let mut chunk = [0u8; 4096];
            let n = self.stream.read(&mut chunk).await.expect("read");
            assert!(n > 0, "server closed connection");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    async fn read_line_timeout(&mut self, ms: u64) -> Option<String> {
        tokio::time::timeout(std::time::Duration::from_millis(ms), self.read_line())
            .await
            .ok()
    }

    async fn read_exact_n(&mut self, n: usize) -> Vec<u8> {
        while self.buf.len() < n {
            let mut chunk = [0u8; 4096];
            let m = self.stream.read(&mut chunk).await.expect("read");
            assert!(m > 0, "server closed connection");
            self.buf.extend_from_slice(&chunk[..m]);
        }
        self.buf.drain(..n).collect()
    }

    /// Read one MSG frame, returning (subject, reply, payload).
    async fn read_msg(&mut self) -> (String, Option<String>, Vec<u8>) {
        let line = self.read_line().await;
        let parts: Vec<&str> = line.split(' ').collect();
        assert_eq!(parts[0], "MSG", "expected MSG, got: {line}");
        let (subject, reply, size) = if parts.len() == 4 {
            (
                parts[1].to_string(),
                None,
                parts[3].parse::<usize>().unwrap(),
            )
        } else {
            assert_eq!(parts.len(), 5, "MSG shape: {line}");
            (
                parts[1].to_string(),
                Some(parts[3].to_string()),
                parts[4].parse::<usize>().unwrap(),
            )
        };
        let data = self.read_exact_n(size + 2).await;
        (subject, reply, data[..size].to_vec())
    }

    /// Read one HMSG frame, returning (subject, reply, headers, payload).
    async fn read_hmsg(&mut self) -> (String, Option<String>, String, Vec<u8>) {
        let line = self.read_line().await;
        let parts: Vec<&str> = line.split(' ').collect();
        assert_eq!(parts[0], "HMSG", "expected HMSG, got: {line}");
        let (subject, reply, hdr_len, total_len) = if parts.len() == 5 {
            (
                parts[1].to_string(),
                None,
                parts[3].parse::<usize>().unwrap(),
                parts[4].parse::<usize>().unwrap(),
            )
        } else {
            assert_eq!(parts.len(), 6, "HMSG shape: {line}");
            (
                parts[1].to_string(),
                Some(parts[3].to_string()),
                parts[4].parse::<usize>().unwrap(),
                parts[5].parse::<usize>().unwrap(),
            )
        };
        let data = self.read_exact_n(total_len + 2).await;
        let headers = String::from_utf8_lossy(&data[..hdr_len]).into_owned();
        let payload = data[hdr_len..total_len].to_vec();
        (subject, reply, headers, payload)
    }

    async fn send(&mut self, data: &str) {
        self.stream.write_all(data.as_bytes()).await.unwrap();
    }

    async fn login(&mut self, user: &str, pass: &str) {
        let line = self.read_line().await;
        assert!(line.starts_with("INFO "), "first line is INFO: {line}");
        self.send(&format!(
            "CONNECT {{\"user\":\"{user}\",\"pass\":\"{pass}\",\"verbose\":false,\"headers\":true}}\r\n"
        ))
        .await;
    }
}

fn svc_export(subject: &str, accounts: &[&str]) -> Export {
    Export {
        kind: Some(ImportKind::Service),
        subject: subject.into(),
        accounts: accounts.iter().map(|s| s.to_string()).collect(),
    }
}

fn stream_export(subject: &str, accounts: &[&str]) -> Export {
    Export {
        kind: Some(ImportKind::Stream),
        subject: subject.into(),
        accounts: accounts.iter().map(|s| s.to_string()).collect(),
    }
}

fn svc_import(source: &str, remote: &str, local: &str) -> Import {
    Import {
        kind: Some(ImportKind::Service),
        source_account: source.into(),
        remote_subject: remote.into(),
        local_subject: local.into(),
    }
}

fn stream_import(source: &str, remote: &str, local: &str) -> Import {
    Import {
        kind: Some(ImportKind::Stream),
        source_account: source.into(),
        remote_subject: remote.into(),
        local_subject: local.into(),
    }
}

/// A exports svc.> (service) and events.> (stream) to B only; B
/// imports both; C imports the service but is NOT listed on the
/// export, so C's import is unauthorized.
fn imports_config() -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        accounts: vec![
            Account {
                name: "A".into(),
                users: vec![AccountUser {
                    name: "alice".into(),
                    pass: Some("alicepw".into()),
                    ..Default::default()
                }],
                exports: vec![
                    svc_export("svc.>", &["B"]),
                    stream_export("events.>", &["B"]),
                ],
                ..Default::default()
            },
            Account {
                name: "B".into(),
                users: vec![AccountUser {
                    name: "bob".into(),
                    pass: Some("bobpw".into()),
                    ..Default::default()
                }],
                imports: vec![
                    svc_import("A", "svc.>", "local.>"),
                    stream_import("A", "events.>", "feed.>"),
                ],
                ..Default::default()
            },
            Account {
                name: "C".into(),
                users: vec![AccountUser {
                    name: "carol".into(),
                    pass: Some("carolpw".into()),
                    ..Default::default()
                }],
                imports: vec![svc_import("A", "svc.>", "local.>")],
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

#[tokio::test]
async fn service_request_reply_through_import() {
    let srv = server::start(imports_config())
        .await
        .expect("server starts");
    let mut alice = Raw::connect(srv.addr).await;
    alice.login("alice", "alicepw").await;
    let mut bob = Raw::connect(srv.addr).await;
    bob.login("bob", "bobpw").await;

    alice.send("SUB svc.echo 1\r\n").await;
    bob.send("SUB _INBOX.bob1 2\r\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Bob requests on B's local subject; alice must see the request
    // on A's exported subject, with a synthetic reply subject.
    // Phase 28C: the translated request is an HMSG carrying
    // Nats-Request-Info naming the importing account (B).
    bob.send("PUB local.echo _INBOX.bob1 5\r\nhello\r\n").await;
    let (subject, reply, headers, payload) = alice.read_hmsg().await;
    assert_eq!(subject, "svc.echo");
    assert_eq!(payload, b"hello");
    assert!(
        headers.contains("Nats-Request-Info: {\"acc\":\"B\"}"),
        "translated request must carry Nats-Request-Info for B: {headers}"
    );
    let reply = reply.expect("service request carries a reply subject");
    assert!(
        reply.starts_with("_RIMPORT."),
        "reply must be synthetic, got: {reply}"
    );

    // Alice replies to the synthetic subject; bob gets it on his inbox.
    alice.send(&format!("PUB {reply} 5\r\nworld\r\n")).await;
    let (subject, _reply, payload) = bob.read_msg().await;
    assert_eq!(subject, "_INBOX.bob1");
    assert_eq!(payload, b"world");
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn stream_import_copies_to_local_subject() {
    let srv = server::start(imports_config())
        .await
        .expect("server starts");
    let mut alice = Raw::connect(srv.addr).await;
    alice.login("alice", "alicepw").await;
    let mut bob = Raw::connect(srv.addr).await;
    bob.login("bob", "bobpw").await;

    bob.send("SUB feed.news 1\r\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    alice.send("PUB events.news 4\r\nnews\r\n").await;
    let (subject, _reply, payload) = bob.read_msg().await;
    assert_eq!(subject, "feed.news");
    assert_eq!(payload, b"news");

    // A subject bob did not subscribe to locally must not arrive.
    alice.send("PUB events.other 3\r\nxyz\r\n").await;
    assert_eq!(bob.read_line_timeout(300).await, None);
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn denied_import_is_dropped_not_delivered() {
    let srv = server::start(imports_config())
        .await
        .expect("server starts");
    let mut alice = Raw::connect(srv.addr).await;
    alice.login("alice", "alicepw").await;
    let mut carol = Raw::connect(srv.addr).await;
    carol.login("carol", "carolpw").await;

    alice.send("SUB svc.echo 1\r\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    // C imports svc.> but A exports only to B: carol's request must
    // never reach alice, and must not leak into C's own local space
    // either (the import shadows the local subject and is dropped).
    carol
        .send("PUB local.echo _INBOX.carol1 5\r\nhello\r\n")
        .await;
    assert_eq!(alice.read_line_timeout(300).await, None);
    assert_eq!(carol.read_line_timeout(300).await, None);
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn import_subject_outside_export_is_dropped() {
    // A exports only svc.echo, but B imports the broader svc.>: the
    // import is not covered by the export, so requests are dropped.
    let mut cfg = imports_config();
    cfg.accounts[0].exports = vec![svc_export("svc.echo", &["B"])];
    let srv = server::start(cfg).await.expect("server starts");
    let mut alice = Raw::connect(srv.addr).await;
    alice.login("alice", "alicepw").await;
    let mut bob = Raw::connect(srv.addr).await;
    bob.login("bob", "bobpw").await;
    alice.send("SUB svc.echo 1\r\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    bob.send("PUB local.echo _INBOX.bob1 5\r\nhello\r\n").await;
    assert_eq!(alice.read_line_timeout(300).await, None);
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn local_allow_deny_still_enforced_around_import() {
    // B's user may only publish other.>: a publish to the imported
    // local subject is a permission violation, not a delivery.
    let mut cfg = imports_config();
    cfg.accounts[1].users = vec![AccountUser {
        name: "bob".into(),
        pass: Some("bobpw".into()),
        permissions: Permissions {
            publish_allow: Some(vec!["other.>".into()]),
            subscribe_allow: Some(vec!["other.>".into(), "_INBOX.>".into()]),
            ..Default::default()
        },
        ..Default::default()
    }];
    let srv = server::start(cfg).await.expect("server starts");
    let mut alice = Raw::connect(srv.addr).await;
    alice.login("alice", "alicepw").await;
    let mut bob = Raw::connect(srv.addr).await;
    bob.login("bob", "bobpw").await;
    alice.send("SUB svc.echo 1\r\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    bob.send("PUB local.echo _INBOX.bob1 5\r\nhello\r\n").await;
    let line = bob.read_line().await;
    assert_eq!(
        line, "-ERR 'Permissions Violation for Publish to \"local.echo\"'",
        "local publish deny must fire on the local subject: {line}"
    );
    assert_eq!(alice.read_line_timeout(300).await, None);
    // Connection stays alive after the violation.
    bob.send("PING\r\n").await;
    assert_eq!(bob.read_line().await, "PONG");
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn conf_imports_exports_shape_parses() {
    let cfg = rust_natsd::conf::apply_to(
        &Config::default(),
        "accounts {\n  A {\n    users: [ { user: alice, password: alicepw } ]\n    exports: [ { service: \"svc.>\", accounts: [\"B\"] }, { stream: \"events.>\", accounts: [\"B\"] } ]\n  }\n  B {\n    users: [ { user: bob, password: bobpw } ]\n    imports: [ { service: { account: A, subject: \"svc.>\" }, to: \"local.>\" }, { stream: { account: A, subject: \"events.>\" }, local_subject: \"feed.>\" } ]\n  }\n}\n",
    )
    .expect("parse imports conf");
    let a = cfg.accounts.iter().find(|a| a.name == "A").expect("A");
    assert_eq!(a.exports.len(), 2);
    assert_eq!(a.exports[0].kind, Some(ImportKind::Service));
    assert_eq!(a.exports[0].subject, "svc.>");
    assert_eq!(a.exports[0].accounts, vec!["B".to_string()]);
    assert_eq!(a.exports[1].kind, Some(ImportKind::Stream));
    let b = cfg.accounts.iter().find(|a| a.name == "B").expect("B");
    assert_eq!(b.imports.len(), 2);
    assert_eq!(b.imports[0].kind, Some(ImportKind::Service));
    assert_eq!(b.imports[0].source_account, "A");
    assert_eq!(b.imports[0].local_subject, "local.>");
    // local_subject alias parses to the same field.
    assert_eq!(b.imports[1].kind, Some(ImportKind::Stream));
    assert_eq!(b.imports[1].local_subject, "feed.>");
    assert!(
        cfg.import_authorized("B", &b.imports[0]),
        "B's service import is authorized by A's export"
    );
    assert!(
        !cfg.import_authorized("C", &b.imports[0]),
        "an unlisted account is not authorized"
    );
}

// ---------------------------------------------------------------------------
// Phase 32: cross-node imports (routes and leafs)
// ---------------------------------------------------------------------------

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn two_route_nodes() -> (
    rust_natsd::server::RunningServer,
    rust_natsd::server::RunningServer,
) {
    // Both nodes carry the same account/import config (cluster nodes
    // share it); what differs is where each client connects.
    let mut cfg1 = imports_config();
    cfg1.cluster_host = "127.0.0.1".into();
    cfg1.cluster_port = free_port();
    let n1 = server::start(cfg1).await.expect("node1 starts");
    let n1_cluster = n1.cluster_addr.unwrap();
    let mut cfg2 = imports_config();
    cfg2.cluster_host = "127.0.0.1".into();
    cfg2.cluster_port = free_port();
    cfg2.routes = vec![format!("nats://{n1_cluster}")];
    let n2 = server::start(cfg2).await.expect("node2 starts");
    // Route establishment + RS+ propagation (import interest included).
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    (n1, n2)
}

#[tokio::test]
async fn cross_node_service_request_reply_through_import() {
    let (n1, n2) = two_route_nodes().await;
    // Responder on node 1 (exporter side), requester on node 2.
    let mut alice = Raw::connect(n1.addr).await;
    alice.login("alice", "alicepw").await;
    let mut bob = Raw::connect(n2.addr).await;
    bob.login("bob", "bobpw").await;
    alice.send("SUB svc.echo 1\r\n").await;
    bob.send("SUB _INBOX.bob1 2\r\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    bob.send("PUB local.echo _INBOX.bob1 5\r\nhello\r\n").await;
    let (subject, reply, headers, payload) = alice.read_hmsg().await;
    assert_eq!(subject, "svc.echo");
    assert_eq!(payload, b"hello");
    assert!(
        headers.contains("Nats-Request-Info: {\"acc\":\"B\"}"),
        "translated request must carry Nats-Request-Info for B: {headers}"
    );
    let reply = reply.expect("service request carries a reply subject");
    // Phase 32: the synthetic subject now carries the minting node's
    // id, so the responder's node can tell it is remote.
    assert!(
        reply.starts_with("_RIMPORT."),
        "reply must be synthetic, got: {reply}"
    );
    assert_eq!(
        reply.split('.').count(),
        3,
        "cross-node reply subject carries a node id: {reply}"
    );

    alice.send(&format!("PUB {reply} 5\r\nworld\r\n")).await;
    let (subject, _reply, payload) = bob.read_msg().await;
    assert_eq!(subject, "_INBOX.bob1");
    assert_eq!(payload, b"world");
    n2.shutdown().await.expect("shutdown");
    n1.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn cross_node_service_request_reply_reverse_direction() {
    let (n1, n2) = two_route_nodes().await;
    // Requester on node 1, responder on node 2: the reply must travel
    // back across the route to the minting node.
    let mut bob = Raw::connect(n1.addr).await;
    bob.login("bob", "bobpw").await;
    let mut alice = Raw::connect(n2.addr).await;
    alice.login("alice", "alicepw").await;
    alice.send("SUB svc.echo 1\r\n").await;
    bob.send("SUB _INBOX.bob1 2\r\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    bob.send("PUB local.echo _INBOX.bob1 5\r\nhello\r\n").await;
    let (subject, reply, _headers, payload) = alice.read_hmsg().await;
    assert_eq!(subject, "svc.echo");
    assert_eq!(payload, b"hello");
    let reply = reply.expect("reply subject");
    alice.send(&format!("PUB {reply} 5\r\nworld\r\n")).await;
    let (subject, _reply, payload) = bob.read_msg().await;
    assert_eq!(subject, "_INBOX.bob1");
    assert_eq!(payload, b"world");
    n2.shutdown().await.expect("shutdown");
    n1.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn cross_node_stream_import_copies() {
    let (n1, n2) = two_route_nodes().await;
    let mut alice = Raw::connect(n1.addr).await;
    alice.login("alice", "alicepw").await;
    let mut bob = Raw::connect(n2.addr).await;
    bob.login("bob", "bobpw").await;
    bob.send("SUB feed.news 1\r\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    alice.send("PUB events.news 4\r\nnews\r\n").await;
    let (subject, _reply, payload) = bob.read_msg().await;
    assert_eq!(subject, "feed.news");
    assert_eq!(payload, b"news");
    alice.send("PUB events.other 3\r\nxyz\r\n").await;
    assert_eq!(bob.read_line_timeout(300).await, None);
    n2.shutdown().await.expect("shutdown");
    n1.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn cross_node_denied_import_dropped() {
    let (n1, n2) = two_route_nodes().await;
    let mut alice = Raw::connect(n1.addr).await;
    alice.login("alice", "alicepw").await;
    let mut carol = Raw::connect(n2.addr).await;
    carol.login("carol", "carolpw").await;
    alice.send("SUB svc.echo 1\r\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    // C is not listed on A's export: the cross-node request must be
    // dropped on the importing node, never forwarded to the exporter.
    carol
        .send("PUB local.echo _INBOX.carol1 5\r\nhello\r\n")
        .await;
    assert_eq!(alice.read_line_timeout(300).await, None);
    assert_eq!(carol.read_line_timeout(300).await, None);
    n2.shutdown().await.expect("shutdown");
    n1.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn leaf_service_and_stream_imports_through_exporter_account_leaf() {
    // Leaf connection bound to the exporting account (leaf user
    // alice in A): the leaf node translates B->A locally and the
    // translated traffic rides the leaf connection in account A;
    // stream-import interest (A events.>) is advertised as LS+ on
    // that connection, and replies return via the node-id synthetic
    // subject claimed on the leaf node.
    let mut hub_cfg = imports_config();
    hub_cfg.leaf_port = free_port();
    let hub = server::start(hub_cfg).await.expect("hub starts");
    let hub_leaf = hub.leaf_addr.unwrap();
    let mut leaf_cfg = imports_config();
    leaf_cfg.leaf_urls = vec![format!("nats://{hub_leaf}")];
    leaf_cfg.leaf_user = Some("alice".into());
    leaf_cfg.leaf_pass = Some("alicepw".into());
    let leaf = server::start(leaf_cfg).await.expect("leaf starts");
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let mut alice = Raw::connect(hub.addr).await;
    alice.login("alice", "alicepw").await;
    let mut bob = Raw::connect(leaf.addr).await;
    bob.login("bob", "bobpw").await;
    alice.send("SUB svc.echo 1\r\n").await;
    bob.send("SUB _INBOX.bob1 2\r\n").await;
    bob.send("SUB feed.news 3\r\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    bob.send("PUB local.echo _INBOX.bob1 5\r\nhello\r\n").await;
    let (subject, reply, _headers, payload) = alice.read_hmsg().await;
    assert_eq!(subject, "svc.echo");
    assert_eq!(payload, b"hello");
    let reply = reply.expect("reply subject");
    alice.send(&format!("PUB {reply} 5\r\nworld\r\n")).await;
    let (subject, _reply, payload) = bob.read_msg().await;
    assert_eq!(subject, "_INBOX.bob1");
    assert_eq!(payload, b"world");

    alice.send("PUB events.news 4\r\nnews\r\n").await;
    let (subject, _reply, payload) = bob.read_msg().await;
    assert_eq!(subject, "feed.news");
    assert_eq!(payload, b"news");
    leaf.shutdown().await.expect("shutdown");
    hub.shutdown().await.expect("shutdown");
}
