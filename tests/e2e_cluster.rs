//! Cluster (route) end-to-end tests: multiple rust-natsd nodes connected
//! via the route protocol, exercised with the official async-nats client.

use std::time::Duration;

use futures::StreamExt;
use rust_natsd::config::Config;
use rust_natsd::server::{self, RunningServer};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn node_config(cluster_port: u16, routes: Vec<String>) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_host: "127.0.0.1".into(),
        cluster_port,
        routes,
        ..Default::default()
    }
}

async fn start(config: Config) -> RunningServer {
    server::start(config).await.expect("server starts")
}

async fn connect(srv: &RunningServer) -> async_nats::Client {
    async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("async-nats connects")
}

/// Publish on `pub_client` until a subscriber on another node receives it
/// (covers route establishment + RS+ propagation delay), or time out.
async fn publish_until_received(
    pub_client: &async_nats::Client,
    sub: &mut async_nats::Subscriber,
    subject: &str,
    payload: &[u8],
) {
    let subject = subject.to_string();
    let deadline = tokio::time::Instant::now() + T;
    loop {
        pub_client
            .publish(subject.clone(), payload.to_vec().into())
            .await
            .unwrap();
        match timeout(Duration::from_millis(200), sub.next()).await {
            Ok(Some(msg)) => {
                assert_eq!(msg.payload.as_ref(), payload);
                return;
            }
            _ => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "cross-node delivery timed out for {subject}"
                );
            }
        }
    }
}

#[tokio::test]
async fn cluster_pub_sub_both_directions() {
    let b = start(node_config(free_port(), vec![])).await;
    let b_cluster = b.cluster_addr.unwrap();
    let a = start(node_config(
        free_port(),
        vec![format!("nats://{b_cluster}")],
    ))
    .await;

    let sub_b = connect(&b).await;
    let pub_a = connect(&a).await;
    let mut sub = sub_b.subscribe("hello").await.unwrap();
    sub_b.flush().await.unwrap();
    publish_until_received(&pub_a, &mut sub, "hello", b"from-a").await;

    // Reverse direction over the same route connection.
    let sub_a = connect(&a).await;
    let pub_b = connect(&b).await;
    let mut sub2 = sub_a.subscribe("back").await.unwrap();
    sub_a.flush().await.unwrap();
    publish_until_received(&pub_b, &mut sub2, "back", b"from-b").await;

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn cluster_request_reply_across_nodes() {
    let b = start(node_config(free_port(), vec![])).await;
    let b_cluster = b.cluster_addr.unwrap();
    let a = start(node_config(
        free_port(),
        vec![format!("nats://{b_cluster}")],
    ))
    .await;

    let responder = connect(&b).await;
    let mut sub = responder.subscribe("svc.echo").await.unwrap();
    responder.flush().await.unwrap();
    let task = tokio::spawn(async move {
        let msg = timeout(T, sub.next()).await.unwrap().unwrap();
        responder
            .publish(msg.reply.unwrap(), msg.payload)
            .await
            .unwrap();
    });
    let requester = connect(&a).await;
    let resp = timeout(T, requester.request("svc.echo", "ping".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resp.payload.as_ref(), b"ping");
    task.await.unwrap();

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn cluster_queue_group_across_nodes() {
    let b = start(node_config(free_port(), vec![])).await;
    let b_cluster = b.cluster_addr.unwrap();
    let a = start(node_config(
        free_port(),
        vec![format!("nats://{b_cluster}")],
    ))
    .await;

    let ca = connect(&a).await;
    let cb = connect(&b).await;
    let mut qa = ca
        .queue_subscribe("jobs", "workers".to_string())
        .await
        .unwrap();
    let mut qb = cb
        .queue_subscribe("jobs", "workers".to_string())
        .await
        .unwrap();
    ca.flush().await.unwrap();
    cb.flush().await.unwrap();
    // Wait for queue interest to propagate both ways.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let pub_client = connect(&a).await;
    for _ in 0..20 {
        pub_client.publish("jobs", "x".into()).await.unwrap();
    }
    pub_client.flush().await.unwrap();
    let mut total = 0;
    let deadline = tokio::time::Instant::now() + T;
    while total < 20 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "only got {total}/20"
        );
        tokio::select! {
            m = qa.next() => { assert!(m.is_some()); total += 1; }
            m = qb.next() => { assert!(m.is_some()); total += 1; }
        }
    }

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn cluster_headers_across_nodes() {
    let b = start(node_config(free_port(), vec![])).await;
    let b_cluster = b.cluster_addr.unwrap();
    let a = start(node_config(
        free_port(),
        vec![format!("nats://{b_cluster}")],
    ))
    .await;

    let sub_client = connect(&b).await;
    let pub_client = connect(&a).await;
    let mut sub = sub_client.subscribe("with.headers").await.unwrap();
    sub_client.flush().await.unwrap();
    let mut headers = async_nats::header::HeaderMap::new();
    headers.insert("X-Node", "a");
    let deadline = tokio::time::Instant::now() + T;
    loop {
        pub_client
            .publish_with_headers("with.headers", headers.clone(), "body".into())
            .await
            .unwrap();
        match timeout(Duration::from_millis(200), sub.next()).await {
            Ok(Some(msg)) => {
                assert_eq!(msg.payload.as_ref(), b"body");
                assert_eq!(msg.headers.unwrap().get("X-Node").unwrap().as_str(), "a");
                break;
            }
            _ => assert!(tokio::time::Instant::now() < deadline),
        }
    }

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn cluster_mutual_solicit_delivers_exactly_once() {
    // Both nodes solicit each other (A starts first, retrying its dial to
    // B's not-yet-listening cluster port; B then solicits A). The
    // duplicate-resolution rule must leave exactly one route connection,
    // so messages are not duplicated.
    let b_cluster_port = free_port();
    let a = start(node_config(
        free_port(),
        vec![format!("nats://127.0.0.1:{b_cluster_port}")],
    ))
    .await;
    let a_cluster = a.cluster_addr.unwrap();
    let b = start(node_config(
        b_cluster_port,
        vec![format!("nats://{a_cluster}")],
    ))
    .await;

    let sub_client = connect(&b).await;
    let pub_client = connect(&a).await;
    let mut sub = sub_client.subscribe("counted").await.unwrap();
    sub_client.flush().await.unwrap();
    publish_until_received(&pub_client, &mut sub, "counted", b"1").await;
    for i in 2..=10u8 {
        pub_client.publish("counted", vec![i].into()).await.unwrap();
    }
    pub_client.flush().await.unwrap();
    let mut got = 1;
    while got < 10 {
        let msg = timeout(T, sub.next()).await.unwrap().unwrap();
        assert_eq!(msg.payload.len(), 1);
        got += 1;
    }
    // No duplicate 11th message.
    assert!(timeout(Duration::from_millis(300), sub.next())
        .await
        .is_err());

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn cluster_three_nodes_full_mesh() {
    let c = start(node_config(free_port(), vec![])).await;
    let c_cluster = c.cluster_addr.unwrap();
    let b = start(node_config(
        free_port(),
        vec![format!("nats://{c_cluster}")],
    ))
    .await;
    let b_cluster = b.cluster_addr.unwrap();
    let a = start(node_config(
        free_port(),
        vec![format!("nats://{b_cluster}"), format!("nats://{c_cluster}")],
    ))
    .await;

    let sub_c = connect(&c).await;
    let pub_a = connect(&a).await;
    let mut sub = sub_c.subscribe("mesh").await.unwrap();
    sub_c.flush().await.unwrap();
    publish_until_received(&pub_a, &mut sub, "mesh", b"a-to-c").await;

    let sub_a = connect(&a).await;
    let pub_b = connect(&b).await;
    let mut sub2 = sub_a.subscribe("mesh2").await.unwrap();
    sub_a.flush().await.unwrap();
    publish_until_received(&pub_b, &mut sub2, "mesh2", b"b-to-a").await;

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
}
