//! End-to-end tests using the official Rust client (`async-nats`) against
//! rust-natsd — client-compatibility verification, not just protocol shape.

use std::time::Duration;

use futures::StreamExt;
use rust_natsd::config::Config;
use rust_natsd::server::{self, RunningServer};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(5);

async fn start_server(config: Config) -> RunningServer {
    server::start(config).await.expect("server starts")
}

fn test_config() -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        ..Default::default()
    }
}

async fn connect(srv: &RunningServer) -> async_nats::Client {
    async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("async-nats connects")
}

#[tokio::test]
async fn rust_client_pub_sub() {
    let srv = start_server(test_config()).await;
    let sub_client = connect(&srv).await;
    let pub_client = connect(&srv).await;
    let mut subscriber = sub_client.subscribe("greetings").await.unwrap();
    sub_client.flush().await.unwrap();
    pub_client
        .publish("greetings", "hello rust".into())
        .await
        .unwrap();
    let msg = timeout(T, subscriber.next()).await.unwrap().unwrap();
    assert_eq!(msg.payload.as_ref(), b"hello rust");
    assert_eq!(msg.subject.as_str(), "greetings");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn rust_client_wildcard() {
    let srv = start_server(test_config()).await;
    let client = connect(&srv).await;
    let mut sub = client.subscribe("events.*").await.unwrap();
    client.flush().await.unwrap();
    client.publish("events.click", "1".into()).await.unwrap();
    client.publish("events.user.click", "2".into()).await.unwrap();
    let msg = timeout(T, sub.next()).await.unwrap().unwrap();
    assert_eq!(msg.payload.as_ref(), b"1");
    // Deeper subject must not match `events.*`: nothing more arrives.
    let none = timeout(Duration::from_millis(300), sub.next()).await;
    assert!(none.is_err(), "wildcard * must match exactly one token");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn rust_client_queue_group() {
    let srv = start_server(test_config()).await;
    let c1 = connect(&srv).await;
    let c2 = connect(&srv).await;
    let publisher = connect(&srv).await;
    let mut q1 = c1
        .queue_subscribe("jobs", "workers".to_string())
        .await
        .unwrap();
    let mut q2 = c2
        .queue_subscribe("jobs", "workers".to_string())
        .await
        .unwrap();
    c1.flush().await.unwrap();
    c2.flush().await.unwrap();
    for i in 0..20 {
        publisher
            .publish("jobs", format!("job-{i}").into())
            .await
            .unwrap();
    }
    publisher.flush().await.unwrap();
    let mut total = 0;
    let mut per_worker = [0usize; 2];
    while total < 20 {
        tokio::select! {
            // Poll each subscriber in its own task-free loop is complex;
            // alternate short-timeout polls instead.
            _ = tokio::time::sleep(Duration::from_millis(1)) => {}
        }
        for (i, q) in [&mut q1, &mut q2].into_iter().enumerate() {
            while let Ok(Some(_)) = timeout(Duration::from_millis(50), q.next()).await {
                total += 1;
                per_worker[i] += 1;
            }
        }
    }
    assert_eq!(total, 20);
    assert!(
        per_worker[0] > 0 && per_worker[1] > 0,
        "queue load should spread across workers: {per_worker:?}"
    );
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn rust_client_request_reply() {
    let srv = start_server(test_config()).await;
    let responder = connect(&srv).await;
    let requester = connect(&srv).await;
    let mut svc = responder.subscribe("svc.upper").await.unwrap();
    responder.flush().await.unwrap();
    let responder_pub = responder.clone();
    let task = tokio::spawn(async move {
        if let Some(msg) = svc.next().await {
            let upper = String::from_utf8_lossy(&msg.payload).to_uppercase();
            let reply = msg.reply.expect("request has a reply subject");
            responder_pub.publish(reply, upper.into()).await.unwrap();
        }
    });
    let resp = timeout(T, requester.request("svc.upper", "hello".into()))
        .await
        .unwrap()
        .expect("request succeeds");
    assert_eq!(resp.payload.as_ref(), b"HELLO");
    task.await.unwrap();
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn rust_client_headers() {
    let srv = start_server(test_config()).await;
    let client = connect(&srv).await;
    let mut sub = client.subscribe("with.headers").await.unwrap();
    client.flush().await.unwrap();
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("X-Request-Id", "abc-123");
    headers.insert("X-Other", "z");
    client
        .publish_with_headers("with.headers", headers, "body".into())
        .await
        .unwrap();
    let msg = timeout(T, sub.next()).await.unwrap().unwrap();
    assert_eq!(msg.payload.as_ref(), b"body");
    let hdrs = msg.headers.expect("headers delivered");
    let v = hdrs.get("X-Request-Id").expect("X-Request-Id present");
    assert_eq!(v.as_str(), "abc-123");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn rust_client_auth() {
    let mut cfg = test_config();
    cfg.user = Some("derek".into());
    cfg.pass = Some("s3cret".into());
    let srv = start_server(cfg).await;
    let url = format!("nats://{}", srv.addr);

    let bad = async_nats::ConnectOptions::with_user_and_password(
        "derek".into(),
        "wrong".into(),
    )
    .connect(&url)
    .await;
    assert!(bad.is_err(), "bad password must be rejected");

    let good = async_nats::ConnectOptions::with_user_and_password(
        "derek".into(),
        "s3cret".into(),
    )
    .connect(&url)
    .await
    .expect("good credentials connect");
    let mut sub = good.subscribe("secure").await.unwrap();
    good.publish("secure", "ok".into()).await.unwrap();
    let msg = timeout(T, sub.next()).await.unwrap().unwrap();
    assert_eq!(msg.payload.as_ref(), b"ok");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn rust_client_many_messages_in_order() {
    let srv = start_server(test_config()).await;
    let client = connect(&srv).await;
    let mut sub = client.subscribe("seq").await.unwrap();
    client.flush().await.unwrap();
    for i in 0..100u32 {
        client.publish("seq", i.to_be_bytes().to_vec().into()).await.unwrap();
    }
    client.flush().await.unwrap();
    for i in 0..100u32 {
        let msg = timeout(T, sub.next()).await.unwrap().unwrap();
        assert_eq!(msg.payload.as_ref(), i.to_be_bytes().as_slice());
    }
    srv.shutdown().await.unwrap();
}
