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
