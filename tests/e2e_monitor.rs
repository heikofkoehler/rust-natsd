//! Monitoring endpoint tests: /varz, /connz, /subz, /routez, /leafz,
//! /jsz and /healthz over plain HTTP.

use bytes::Bytes;
use rust_natsd::config::Config;
use rust_natsd::server::{self, RunningServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn get(srv: &RunningServer, path: &str) -> (u16, serde_json::Value) {
    let addr = srv.http_addr.expect("http enabled");
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect http");
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .expect("write request");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8_lossy(&raw);
    let (head, body) = text.split_once("\r\n\r\n").expect("http response");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .expect("status line");
    (status, serde_json::from_str(body).expect("json body"))
}

async fn start(jetstream: bool) -> (RunningServer, async_nats::Client) {
    let srv = server::start(Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        http_port: free_port(),
        jetstream,
        ..Default::default()
    })
    .await
    .expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("async-nats connects");
    (srv, client)
}

#[tokio::test]
async fn monitor_endpoints() {
    let (srv, client) = start(false).await;
    let _sub = client
        .subscribe("mon.topic".to_string())
        .await
        .expect("subscribe");
    client
        .publish("mon.topic", Bytes::from("hi"))
        .await
        .expect("publish");
    // Let the server count the message.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let (status, health) = get(&srv, "/healthz").await;
    assert_eq!(status, 200);
    assert_eq!(health["status"], "ok");

    let (status, varz) = get(&srv, "/varz").await;
    assert_eq!(status, 200);
    assert_eq!(varz["server_name"], "rust-natsd");
    assert!(varz["connections"].as_u64().unwrap() >= 1);
    assert!(varz["in_msgs"].as_u64().unwrap() >= 1);
    assert!(varz["subscriptions"].as_u64().unwrap() >= 1);

    let (status, connz) = get(&srv, "/connz").await;
    assert_eq!(status, 200);
    assert!(connz["num_connections"].as_u64().unwrap() >= 1);
    let has_client = connz["connections"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["kind"] == "client");
    assert!(has_client, "connz lists the client");

    let (status, subz) = get(&srv, "/subz").await;
    assert_eq!(status, 200);
    let has_sub = subz["subscriptions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["subject"] == "mon.topic");
    assert!(has_sub, "subz lists mon.topic");

    let (status, routez) = get(&srv, "/routez").await;
    assert_eq!(status, 200);
    assert_eq!(routez["num_routes"], 0);

    let (status, leafz) = get(&srv, "/leafz").await;
    assert_eq!(status, 200);
    assert_eq!(leafz["num_leafnodes"], 0);

    let (status, _) = get(&srv, "/jsz").await;
    assert_eq!(status, 404, "jsz without JetStream is 404");

    let (status, _) = get(&srv, "/nope").await;
    assert_eq!(status, 404);
}

#[tokio::test]
async fn monitor_jsz_with_stream() {
    let (srv, client) = start(true).await;
    let js = async_nats::jetstream::new(client);
    js.create_stream(async_nats::jetstream::stream::Config {
        name: "MON".into(),
        subjects: vec!["mon.>".into()],
        ..Default::default()
    })
    .await
    .expect("create stream");
    let (status, jsz) = get(&srv, "/jsz").await;
    assert_eq!(status, 200);
    assert_eq!(jsz["total_streams"], 1);
    assert_eq!(jsz["streams"][0]["name"], "MON");
}

#[tokio::test]
async fn monitor_phase16_varz_accounz_gatewayz_connz_filters() {
    let srv = server::start(Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        http_port: free_port(),
        accounts: vec![
            rust_natsd::config::Account {
                name: "A".into(),
                users: vec![rust_natsd::config::AccountUser {
                    name: "alice".into(),
                    pass: Some("alicepw".into()),
                    ..Default::default()
                }],
                ..Default::default()
            },
            rust_natsd::config::Account {
                name: "B".into(),
                users: vec![rust_natsd::config::AccountUser {
                    name: "bob".into(),
                    pass: Some("bobpw".into()),
                    ..Default::default()
                }],
                ..Default::default()
            },
        ],
        ..Default::default()
    })
    .await
    .expect("server starts");
    let alice = async_nats::ConnectOptions::with_user_and_password(
        "alice".to_string(),
        "alicepw".to_string(),
    )
    .connect(format!("nats://{}", srv.addr))
    .await
    .expect("alice connects");
    let bob =
        async_nats::ConnectOptions::with_user_and_password("bob".to_string(), "bobpw".to_string())
            .connect(format!("nats://{}", srv.addr))
            .await
            .expect("bob connects");

    // varz: http_port is the HTTP port (not the client port),
    // auth_required reflects the accounts, mem is real on Linux.
    let (status, varz) = get(&srv, "/varz").await;
    assert_eq!(status, 200);
    assert_eq!(
        varz["http_port"].as_u64().unwrap() as u16,
        srv.http_addr.unwrap().port()
    );
    assert_ne!(
        varz["http_port"], varz["port"],
        "http_port must not echo the client port"
    );
    assert_eq!(varz["auth_required"], true);
    assert_eq!(varz["tls_required"], false);
    assert!(
        varz["mem"].as_u64().unwrap() > 0,
        "mem from /proc/self/statm"
    );
    assert_eq!(varz["slow_consumers"], 0);

    // accounz: per-account connection counts.
    let (status, accounz) = get(&srv, "/accounz").await;
    assert_eq!(status, 200);
    let by_name = |name: &str| {
        accounz["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["name"] == name)
            .unwrap_or_else(|| panic!("account {name} in accounz"))
            .clone()
    };
    assert_eq!(by_name("A")["connections"], 1);
    assert_eq!(by_name("B")["connections"], 1);
    assert_eq!(by_name("$G")["connections"], 0);

    // gatewayz: no gateways, empty Go-shape maps (Phase 33).
    let (status, gatewayz) = get(&srv, "/gatewayz").await;
    assert_eq!(status, 200);
    assert_eq!(gatewayz["outbound_gateways"], serde_json::json!({}));
    assert_eq!(gatewayz["inbound_gateways"], serde_json::json!({}));

    // connz filters: acc, limit/offset, state, and per-connection fields.
    let (status, connz) = get(&srv, "/connz?acc=A").await;
    assert_eq!(status, 200);
    assert_eq!(connz["total"], 1);
    assert_eq!(connz["connections"][0]["account"], "A");
    assert_eq!(connz["connections"][0]["user"], "alice");
    assert_eq!(connz["connections"][0]["tls"], false);
    let (status, connz) = get(&srv, "/connz?acc=B").await;
    assert_eq!(status, 200);
    assert_eq!(connz["connections"][0]["user"], "bob");
    let (status, connz) = get(&srv, "/connz?limit=1&offset=0").await;
    assert_eq!(status, 200);
    assert_eq!(connz["total"], 2);
    assert_eq!(connz["num_connections"], 1);
    let (status, connz) = get(&srv, "/connz?state=closed").await;
    assert_eq!(status, 200);
    assert_eq!(connz["total"], 0);
    assert_eq!(connz["connections"], serde_json::json!([]));

    drop(alice);
    drop(bob);
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn monitor_subz_routez_leafz_gatewayz_shapes() {
    let (srv, client) = start(false).await;
    let _sub = client
        .subscribe("mon.subz".to_string())
        .await
        .expect("subscribe");
    client.flush().await.expect("flush sub");
    let (status, subz) = get(&srv, "/subz").await;
    assert_eq!(status, 200);
    let subs = subz["subscriptions"].as_array().expect("subz array");
    assert!(
        subs.iter().any(|s| s["subject"] == "mon.subz"),
        "subz must list the subscription: {subz}"
    );
    // No routes or leafs connected: counts are zero, shapes stable.
    let (status, routez) = get(&srv, "/routez").await;
    assert_eq!(status, 200);
    assert_eq!(routez["num_routes"], 0);
    assert_eq!(routez["routes"], serde_json::json!([]));
    let (status, leafz) = get(&srv, "/leafz").await;
    assert_eq!(status, 200);
    assert_eq!(leafz["num_leafnodes"], 0);
    assert_eq!(leafz["leafnodes"], serde_json::json!([]));
    // No gateways connected: the empty Go shape, never 404.
    let (status, gatewayz) = get(&srv, "/gatewayz").await;
    assert_eq!(status, 200);
    assert_eq!(gatewayz["outbound_gateways"], serde_json::json!({}));
    assert_eq!(gatewayz["inbound_gateways"], serde_json::json!({}));
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn monitor_varz_reports_configured_limits() {
    let srv = server::start(Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        http_port: free_port(),
        max_pending: 1024 * 1024,
        ping_interval: std::time::Duration::from_millis(200),
        ping_max: 3,
        ..Default::default()
    })
    .await
    .expect("server starts");
    let (status, varz) = get(&srv, "/varz").await;
    assert_eq!(status, 200);
    assert_eq!(varz["max_pending"], 1024 * 1024);
    assert_eq!(varz["ping_interval"], 200_000_000);
    assert_eq!(varz["ping_max"], 3);
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn monitor_jsz_disabled_and_unknown_path_404() {
    let (srv, _client) = start(false).await;
    let (status, jsz) = get(&srv, "/jsz").await;
    assert_eq!(status, 404);
    assert!(jsz.get("error").is_some(), "jsz disabled must error: {jsz}");
    let (status, body) = get(&srv, "/bogus").await;
    assert_eq!(status, 404);
    assert_eq!(body["error"], "unknown endpoint");
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn monitor_connz_state_open_and_offset_pagination() {
    let (srv, c1) = start(false).await;
    let c2 = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("second client connects");
    let (status, connz) = get(&srv, "/connz?state=open").await;
    assert_eq!(status, 200);
    assert_eq!(connz["total"], 2);
    let (status, connz) = get(&srv, "/connz?offset=1").await;
    assert_eq!(status, 200);
    assert_eq!(connz["total"], 2, "total is pre-pagination");
    assert_eq!(connz["connections"].as_array().unwrap().len(), 1);
    let (status, connz) = get(&srv, "/connz?offset=99").await;
    assert_eq!(status, 200);
    assert_eq!(connz["connections"], serde_json::json!([]));
    drop(c1);
    drop(c2);
    srv.shutdown().await.expect("shutdown");
}
