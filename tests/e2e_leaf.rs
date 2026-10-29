//! Leaf node end-to-end tests: a leaf rust-natsd soliciting a hub
//! rust-natsd, exercised with the official async-nats client.

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

fn base_config() -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
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

/// Publish until a subscriber on the other side of the leaf receives it
/// (covers leaf establishment + LS+ propagation delay).
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
            _ => assert!(
                tokio::time::Instant::now() < deadline,
                "leaf delivery timed out for {subject}"
            ),
        }
    }
}

async fn hub_and_leaf() -> (RunningServer, RunningServer) {
    let hub = start(Config {
        leaf_port: free_port(),
        ..base_config()
    })
    .await;
    let hub_leaf = hub.leaf_addr.unwrap();
    let leaf = start(Config {
        leaf_urls: vec![format!("nats://{hub_leaf}")],
        ..base_config()
    })
    .await;
    (hub, leaf)
}

#[tokio::test]
async fn leaf_pub_sub_both_directions() {
    let (hub, leaf) = hub_and_leaf().await;

    let sub_leaf = connect(&leaf).await;
    let pub_hub = connect(&hub).await;
    let mut sub = sub_leaf.subscribe("hello").await.unwrap();
    sub_leaf.flush().await.unwrap();
    publish_until_received(&pub_hub, &mut sub, "hello", b"hub-to-leaf").await;

    let sub_hub = connect(&hub).await;
    let pub_leaf = connect(&leaf).await;
    let mut sub2 = sub_hub.subscribe("back").await.unwrap();
    sub_hub.flush().await.unwrap();
    publish_until_received(&pub_leaf, &mut sub2, "back", b"leaf-to-hub").await;

    leaf.shutdown().await.unwrap();
    hub.shutdown().await.unwrap();
}

#[tokio::test]
async fn leaf_request_reply_across() {
    let (hub, leaf) = hub_and_leaf().await;

    let responder = connect(&hub).await;
    let mut sub = responder.subscribe("svc.echo").await.unwrap();
    responder.flush().await.unwrap();
    let task = tokio::spawn(async move {
        let msg = timeout(T, sub.next()).await.unwrap().unwrap();
        responder
            .publish(msg.reply.unwrap(), msg.payload)
            .await
            .unwrap();
    });
    let requester = connect(&leaf).await;
    let resp = timeout(T, requester.request("svc.echo", "ping".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resp.payload.as_ref(), b"ping");
    task.await.unwrap();

    leaf.shutdown().await.unwrap();
    hub.shutdown().await.unwrap();
}

#[tokio::test]
async fn leaf_queue_group_exactly_once() {
    let (hub, leaf) = hub_and_leaf().await;

    let ch = connect(&hub).await;
    let cl = connect(&leaf).await;
    let mut qh = ch
        .queue_subscribe("jobs", "workers".to_string())
        .await
        .unwrap();
    let mut ql = cl
        .queue_subscribe("jobs", "workers".to_string())
        .await
        .unwrap();
    ch.flush().await.unwrap();
    cl.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let pub_client = connect(&hub).await;
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
            m = qh.next() => { assert!(m.is_some()); total += 1; }
            m = ql.next() => { assert!(m.is_some()); total += 1; }
        }
    }
    // Exactly once globally: no 21st message.
    tokio::select! {
        m = qh.next() => panic!("extra delivery on hub: {:?}", m.map(|m| m.payload)),
        m = ql.next() => panic!("extra delivery on leaf: {:?}", m.map(|m| m.payload)),
        _ = tokio::time::sleep(Duration::from_millis(300)) => {}
    }

    leaf.shutdown().await.unwrap();
    hub.shutdown().await.unwrap();
}

#[tokio::test]
async fn leaf_headers_across() {
    let (hub, leaf) = hub_and_leaf().await;

    let sub_client = connect(&leaf).await;
    let pub_client = connect(&hub).await;
    let mut sub = sub_client.subscribe("with.headers").await.unwrap();
    sub_client.flush().await.unwrap();
    let mut headers = async_nats::header::HeaderMap::new();
    headers.insert("X-Leaf", "yes");
    let deadline = tokio::time::Instant::now() + T;
    loop {
        pub_client
            .publish_with_headers("with.headers", headers.clone(), "body".into())
            .await
            .unwrap();
        match timeout(Duration::from_millis(200), sub.next()).await {
            Ok(Some(msg)) => {
                assert_eq!(msg.payload.as_ref(), b"body");
                assert_eq!(msg.headers.unwrap().get("X-Leaf").unwrap().as_str(), "yes");
                break;
            }
            _ => assert!(tokio::time::Instant::now() < deadline),
        }
    }

    leaf.shutdown().await.unwrap();
    hub.shutdown().await.unwrap();
}

#[tokio::test]
async fn leaf_late_subscription_after_connect() {
    let (hub, leaf) = hub_and_leaf().await;

    // Establish the leaf connection first with an unrelated subscription.
    let warm = connect(&leaf).await;
    let mut warm_sub = warm.subscribe("warmup").await.unwrap();
    warm.flush().await.unwrap();
    let pub_hub = connect(&hub).await;
    publish_until_received(&pub_hub, &mut warm_sub, "warmup", b"w").await;

    // Now subscribe late on the leaf; LS+ must propagate to the hub.
    let sub_leaf = connect(&leaf).await;
    let mut sub = sub_leaf.subscribe("late.subj").await.unwrap();
    sub_leaf.flush().await.unwrap();
    publish_until_received(&pub_hub, &mut sub, "late.subj", b"late").await;

    leaf.shutdown().await.unwrap();
    hub.shutdown().await.unwrap();
}

#[tokio::test]
async fn leaf_reconnects_after_restart() {
    let hub = start(Config {
        leaf_port: free_port(),
        ..base_config()
    })
    .await;
    let hub_leaf = hub.leaf_addr.unwrap();
    let mk_leaf = || {
        start(Config {
            leaf_urls: vec![format!("nats://{hub_leaf}")],
            ..base_config()
        })
    };
    let leaf = mk_leaf().await;
    let sub_leaf = connect(&leaf).await;
    let pub_hub = connect(&hub).await;
    let mut sub = sub_leaf.subscribe("flap").await.unwrap();
    sub_leaf.flush().await.unwrap();
    publish_until_received(&pub_hub, &mut sub, "flap", b"one").await;
    leaf.shutdown().await.unwrap();

    // Restart the leaf node; its solicitation loop reconnects and LS+
    // interest resyncs, so delivery resumes.
    let leaf2 = mk_leaf().await;
    let sub_leaf2 = connect(&leaf2).await;
    let mut sub2 = sub_leaf2.subscribe("flap").await.unwrap();
    sub_leaf2.flush().await.unwrap();
    publish_until_received(&pub_hub, &mut sub2, "flap", b"two").await;

    leaf2.shutdown().await.unwrap();
    hub.shutdown().await.unwrap();
}

#[tokio::test]
async fn leaf_to_leaf_via_hub() {
    let hub = start(Config {
        leaf_port: free_port(),
        ..base_config()
    })
    .await;
    let hub_leaf = hub.leaf_addr.unwrap();
    let leaf1 = start(Config {
        leaf_urls: vec![format!("nats://{hub_leaf}")],
        ..base_config()
    })
    .await;
    let leaf2 = start(Config {
        leaf_urls: vec![format!("nats://{hub_leaf}")],
        ..base_config()
    })
    .await;

    let c1 = connect(&leaf1).await;
    let c2 = connect(&leaf2).await;
    let mut sub2 = c2.subscribe("spoke.to.spoke").await.unwrap();
    c2.flush().await.unwrap();
    publish_until_received(&c1, &mut sub2, "spoke.to.spoke", b"l1-to-l2").await;

    let mut sub1 = c1.subscribe("spoke.back").await.unwrap();
    c1.flush().await.unwrap();
    publish_until_received(&c2, &mut sub1, "spoke.back", b"l2-to-l1").await;

    leaf1.shutdown().await.unwrap();
    leaf2.shutdown().await.unwrap();
    hub.shutdown().await.unwrap();
}

#[tokio::test]
async fn leaf_to_route_and_route_to_leaf() {
    let node_b_cluster_port = free_port();
    let node_b = start(Config {
        cluster_host: "127.0.0.1".into(),
        cluster_port: node_b_cluster_port,
        cluster_name: Some("c1".into()),
        ..base_config()
    })
    .await;
    let hub_a = start(Config {
        cluster_host: "127.0.0.1".into(),
        cluster_port: free_port(),
        cluster_name: Some("c1".into()),
        routes: vec![format!("nats://127.0.0.1:{node_b_cluster_port}")],
        leaf_port: free_port(),
        ..base_config()
    })
    .await;
    let hub_a_leaf = hub_a.leaf_addr.unwrap();
    let leaf = start(Config {
        leaf_urls: vec![format!("nats://{hub_a_leaf}")],
        ..base_config()
    })
    .await;

    let c_leaf = connect(&leaf).await;
    let c_b = connect(&node_b).await;

    // Subscriber on cluster node B, publisher on leaf (via hub A).
    let mut sub_b = c_b.subscribe("from.leaf").await.unwrap();
    c_b.flush().await.unwrap();
    publish_until_received(&c_leaf, &mut sub_b, "from.leaf", b"leaf-to-route").await;

    // Subscriber on leaf, publisher on cluster node B (via hub A).
    let mut sub_leaf = c_leaf.subscribe("from.route").await.unwrap();
    c_leaf.flush().await.unwrap();
    publish_until_received(&c_b, &mut sub_leaf, "from.route", b"route-to-leaf").await;

    leaf.shutdown().await.unwrap();
    hub_a.shutdown().await.unwrap();
    node_b.shutdown().await.unwrap();
}

#[tokio::test]
async fn leaf_queue_group_with_headers_exactly_once() {
    let (hub, leaf) = hub_and_leaf().await;

    let ch = connect(&hub).await;
    let cl = connect(&leaf).await;
    // Also add a plain subscriber on the leaf so HMSG frames go over the
    // leaf connection even when the hub's queue worker wins selection.
    let mut plain_leaf = cl.subscribe("hjobs").await.unwrap();
    let mut qh = ch
        .queue_subscribe("hjobs", "workers".to_string())
        .await
        .unwrap();
    let mut ql = cl
        .queue_subscribe("hjobs", "workers".to_string())
        .await
        .unwrap();
    ch.flush().await.unwrap();
    cl.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let pub_client = connect(&hub).await;
    let mut hdrs = async_nats::header::HeaderMap::new();
    hdrs.insert("X-Test", "1");
    for _ in 0..20 {
        pub_client
            .publish_with_headers("hjobs", hdrs.clone(), "x".into())
            .await
            .unwrap();
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
            m = qh.next() => { assert!(m.is_some()); total += 1; }
            m = ql.next() => { assert!(m.is_some()); total += 1; }
        }
    }
    // Plain subscriber receives all 20, while the queue group receives
    // each message once globally (never duplicated across hub + leaf).
    for _ in 0..20 {
        assert!(timeout(T, plain_leaf.next()).await.unwrap().is_some());
    }
    tokio::select! {
        m = qh.next() => panic!("extra queue delivery on hub: {:?}", m.map(|m| m.payload)),
        m = ql.next() => panic!("extra queue delivery on leaf: {:?}", m.map(|m| m.payload)),
        _ = tokio::time::sleep(Duration::from_millis(300)) => {}
    }

    leaf.shutdown().await.unwrap();
    hub.shutdown().await.unwrap();
}
