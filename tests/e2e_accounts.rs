//! Multi-account integration tests: subscriptions and deliveries are
//! scoped per account, and publish/subscribe permission violations get
//! Go's exact `-ERR` text with the connection staying alive.

use rust_natsd::config::{Account, AccountUser, Config, Permissions};
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

    /// Read one line, or None if nothing arrives within the timeout
    /// (used to assert non-delivery).
    async fn read_line_timeout(&mut self, ms: u64) -> Option<String> {
        tokio::time::timeout(std::time::Duration::from_millis(ms), self.read_line())
            .await
            .ok()
    }

    async fn info(&mut self) -> serde_json::Value {
        let line = self.read_line().await;
        assert!(line.starts_with("INFO "), "first line is INFO: {line}");
        serde_json::from_str(&line[5..]).expect("INFO json")
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

    async fn send(&mut self, data: &str) {
        self.stream.write_all(data.as_bytes()).await.unwrap();
    }

    async fn login(&mut self, user: &str, pass: &str) {
        self.info().await;
        self.send(&format!(
            "CONNECT {{\"user\":\"{user}\",\"pass\":\"{pass}\",\"verbose\":false,\"headers\":true}}\r\n"
        ))
        .await;
    }
}

fn accounts_config() -> Config {
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
                    permissions: Permissions {
                        publish_allow: Some(vec!["allowed.>".into()]),
                        subscribe_allow: Some(vec!["allowed.>".into(), "_INBOX.>".into()]),
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            },
            Account {
                name: "B".into(),
                users: vec![AccountUser {
                    name: "bob".into(),
                    pass: Some("bobpw".into()),
                    ..Default::default()
                }],
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

#[tokio::test]
async fn cross_account_delivery_is_impossible() {
    let srv = server::start(accounts_config())
        .await
        .expect("server starts");
    let mut alice = Raw::connect(srv.addr).await;
    alice.login("alice", "alicepw").await;
    let mut bob = Raw::connect(srv.addr).await;
    bob.login("bob", "bobpw").await;

    // Same subject, both accounts subscribed: each message stays in
    // its own account, in both directions.
    alice.send("SUB allowed.shared 1\r\n").await;
    bob.send("SUB allowed.shared 1\r\n").await;
    alice.send("PUB allowed.shared 1\r\na\r\n").await;
    let line = alice.read_line().await;
    assert!(
        line.starts_with("MSG allowed.shared 1 1"),
        "alice self-delivery: {line}"
    );
    alice.read_exact_n(3).await; // payload "a" + CRLF
    let got = bob.read_line_timeout(300).await;
    assert_eq!(got, None, "bob must not see alice's account's message");
    bob.send("PUB allowed.shared 1\r\nb\r\n").await;
    let line = bob.read_line().await;
    assert!(
        line.starts_with("MSG allowed.shared 1 1"),
        "bob self-delivery: {line}"
    );
    bob.read_exact_n(3).await; // payload "b" + CRLF
    assert_eq!(
        alice.read_line_timeout(300).await,
        None,
        "alice must not see bob's account's message"
    );
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn permission_violations_use_go_error_text_and_keep_connection_alive() {
    let srv = server::start(accounts_config())
        .await
        .expect("server starts");
    let mut alice = Raw::connect(srv.addr).await;
    alice.login("alice", "alicepw").await;

    // Denied publish: exact Go error text, no delivery, connection alive.
    alice.send("PUB denied.y 1\r\nx\r\n").await;
    let line = alice.read_line().await;
    assert_eq!(
        line, "-ERR 'Permissions Violation for Publish to \"denied.y\"'",
        "publish violation: {line}"
    );
    alice.send("PING\r\n").await;
    assert_eq!(
        alice.read_line().await,
        "PONG",
        "connection must stay alive"
    );

    // Denied subscribe: exact Go error text, no +OK, connection alive.
    alice.send("SUB denied.x 2\r\n").await;
    let line = alice.read_line().await;
    assert_eq!(
        line, "-ERR 'Permissions Violation for Subscription to \"denied.x\"'",
        "subscribe violation: {line}"
    );
    alice.send("PING\r\n").await;
    assert_eq!(
        alice.read_line().await,
        "PONG",
        "connection must stay alive"
    );

    // Allowed publish/subscribe still work after violations.
    alice.send("SUB allowed.z 3\r\n").await;
    alice.send("PUB allowed.z 1\r\ny\r\n").await;
    let line = alice.read_line().await;
    assert!(
        line.starts_with("MSG allowed.z 3 1"),
        "allowed delivery: {line}"
    );
    alice.read_exact_n(3).await; // payload "y" + CRLF
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn conf_accounts_shape_parses() {
    let cfg = rust_natsd::conf::apply_to(
        &Config::default(),
        "port: 0\naccounts {\n  A {\n    users: [\n      { user: alice, password: alicepw, permissions: { publish: { allow: [\"allowed.>\"] }, subscribe: { allow: [\"allowed.>\", \"_INBOX.>\"] } } }\n    ]\n  }\n  B {\n    users: [ { user: bob, password: bobpw } ]\n    authorization { publish { allow: [\"b.>\"] } subscribe { allow: [\"b.>\"] } }\n  }\n}\nsystem_account: B\nno_auth_user: alice\n",
    )
    .expect("parse accounts conf");
    assert_eq!(cfg.accounts.len(), 2);
    let a = cfg
        .accounts
        .iter()
        .find(|a| a.name == "A")
        .expect("account A");
    assert_eq!(a.users.len(), 1);
    assert_eq!(a.users[0].name, "alice");
    assert_eq!(
        a.users[0].permissions.publish_allow.as_deref(),
        Some(&["allowed.>".to_string()][..])
    );
    let b = cfg
        .accounts
        .iter()
        .find(|a| a.name == "B")
        .expect("account B");
    assert_eq!(
        b.permissions.publish_allow.as_deref(),
        Some(&["b.>".to_string()][..]),
        "account-level authorization is the account default"
    );
    assert_eq!(cfg.system_account.as_deref(), Some("B"));
    assert_eq!(cfg.no_auth_user.as_deref(), Some("alice"));
}

#[tokio::test]
async fn jetstream_streams_and_consumers_isolated_across_accounts() {
    let cfg = Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        jetstream: true,
        accounts: vec![
            Account {
                name: "A".into(),
                users: vec![AccountUser {
                    name: "alice".into(),
                    pass: Some("alicepw".into()),
                    ..Default::default()
                }],
                jetstream_enabled: true,
                ..Default::default()
            },
            Account {
                name: "B".into(),
                users: vec![AccountUser {
                    name: "bob".into(),
                    pass: Some("bobpw".into()),
                    ..Default::default()
                }],
                jetstream_enabled: true,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let srv = server::start(cfg).await.expect("server starts");

    let ca = async_nats::ConnectOptions::with_user_and_password(
        "alice".to_string(),
        "alicepw".to_string(),
    )
    .connect(format!("nats://{}", srv.addr))
    .await
    .unwrap();
    let cb =
        async_nats::ConnectOptions::with_user_and_password("bob".to_string(), "bobpw".to_string())
            .connect(format!("nats://{}", srv.addr))
            .await
            .unwrap();

    let jsa = async_nats::jetstream::new(ca.clone());
    let jsb = async_nats::jetstream::new(cb.clone());

    // Account A creates stream "SHARED" and durable consumer "c1".
    let sa = jsa
        .create_stream(async_nats::jetstream::stream::Config {
            name: "SHARED".into(),
            subjects: vec!["orders.>".into()],
            ..Default::default()
        })
        .await
        .expect("account A creates SHARED");
    sa.create_consumer(async_nats::jetstream::consumer::pull::Config {
        durable_name: Some("c1".into()),
        ..Default::default()
    })
    .await
    .expect("account A creates consumer c1");

    // Account B cannot read or delete Account A's consumer "c1" before B
    // even has a stream named "SHARED".
    let info_resp: serde_json::Value = serde_json::from_slice(
        &cb.request("$JS.API.CONSUMER.INFO.SHARED.c1", "".into())
            .await
            .unwrap()
            .payload,
    )
    .unwrap();
    assert!(
        info_resp.get("error").is_some(),
        "Account B must not read Account A's consumer info: {info_resp}"
    );
    let del_resp: serde_json::Value = serde_json::from_slice(
        &cb.request("$JS.API.CONSUMER.DELETE.SHARED.c1", "".into())
            .await
            .unwrap()
            .payload,
    )
    .unwrap();
    assert!(
        del_resp.get("error").is_some(),
        "Account B must not delete Account A's consumer: {del_resp}"
    );

    // Account A's consumer "c1" is still intact.
    let info_a: serde_json::Value = serde_json::from_slice(
        &ca.request("$JS.API.CONSUMER.INFO.SHARED.c1", "".into())
            .await
            .unwrap()
            .payload,
    )
    .unwrap();
    assert_eq!(info_a["name"], "c1");

    // Account B can also create a stream with the same name "SHARED".
    jsb.create_stream(async_nats::jetstream::stream::Config {
        name: "SHARED".into(),
        subjects: vec!["orders.>".into()],
        ..Default::default()
    })
    .await
    .expect("account B creates its own SHARED stream");

    srv.shutdown().await.expect("shutdown");
}
