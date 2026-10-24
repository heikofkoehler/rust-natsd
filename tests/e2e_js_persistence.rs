//! JetStream Phase 15 tests: file persistence across restarts,
//! workqueue retention, max_msgs_per_subject, and consumer
//! deliver_policy last/new (filter-aware last).

use std::time::Duration;

use async_nats::jetstream::{self, consumer::DeliverPolicy};
use bytes::Bytes;
use futures::StreamExt;
use rust_natsd::config::Config;
use rust_natsd::server;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);

fn tmpdir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rust-natsd-js-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn js_config(store_dir: Option<std::path::PathBuf>) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        jetstream: true,
        js_store_dir: store_dir.map(|d| d.to_string_lossy().to_string()),
        ..Default::default()
    }
}

async fn publish(js: &jetstream::Context, subject: &str, body: &str) {
    js.publish(subject.to_string(), Bytes::from(body.to_string()))
        .await
        .expect("send")
        .await
        .expect("ack");
}

#[tokio::test]
async fn persistence_survives_restart() {
    let dir = tmpdir("persist");
    let srv = server::start(js_config(Some(dir.clone())))
        .await
        .expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    let _stream = js
        .create_stream(jetstream::stream::Config {
            name: "ORDERS".into(),
            subjects: vec!["orders.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    publish(&js, "orders.1", "hello").await;
    publish(&js, "orders.2", "world").await;
    drop(client);
    srv.shutdown().await.expect("shutdown");

    // Restart with the same store dir: stream, messages, MSG.GET and
    // consumer delivery must all still work.
    let srv = server::start(js_config(Some(dir.clone())))
        .await
        .expect("server restarts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    let stream = js.get_stream("ORDERS").await.expect("stream survives");
    let msg = stream.get_raw_message(2).await.expect("MSG.GET survives");
    assert_eq!(msg.payload.as_ref(), b"world");
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("c-persist".into()),
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .expect("consumer after restart");
    let mut messages = consumer.messages().await.expect("messages stream");
    let first = timeout(T, messages.next())
        .await
        .expect("timeout")
        .expect("message")
        .expect("ok");
    assert_eq!(first.payload.as_ref(), b"hello");
    srv.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn workqueue_message_deleted_after_first_ack() {
    let (_srv, client) = {
        let srv = server::start(js_config(None)).await.expect("server starts");
        let client = async_nats::connect(format!("nats://{}", srv.addr))
            .await
            .expect("connects");
        (srv, client)
    };
    let js = jetstream::new(client.clone());
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "WORK".into(),
            subjects: vec!["work.>".into()],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            ..Default::default()
        })
        .await
        .expect("create workqueue stream");
    publish(&js, "work.1", "job1").await;
    publish(&js, "work.2", "job2").await;
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("c-work".into()),
            ..Default::default()
        })
        .await
        .expect("consumer");
    let mut messages = consumer.messages().await.expect("messages stream");
    let first = timeout(T, messages.next())
        .await
        .expect("timeout")
        .expect("message")
        .expect("ok");
    assert_eq!(first.payload.as_ref(), b"job1");
    first.ack().await.expect("ack");
    // The acked message is gone: MSG.GET for seq 1 fails, seq 2 works.
    assert!(
        stream.get_raw_message(1).await.is_err(),
        "acked message deleted"
    );
    let second = stream.get_raw_message(2).await.expect("seq 2 survives");
    assert_eq!(second.payload.as_ref(), b"job2");
}

#[tokio::test]
async fn max_msgs_per_subject_evicts_oldest_for_subject() {
    let (_srv, client) = {
        let srv = server::start(js_config(None)).await.expect("server starts");
        let client = async_nats::connect(format!("nats://{}", srv.addr))
            .await
            .expect("connects");
        (srv, client)
    };
    let js = jetstream::new(client.clone());
    let mut stream = js
        .create_stream(jetstream::stream::Config {
            name: "PER-SUBJ".into(),
            subjects: vec!["s.>".into()],
            max_messages_per_subject: 2,
            ..Default::default()
        })
        .await
        .expect("create stream");
    publish(&js, "s.a", "a1").await;
    publish(&js, "s.b", "b1").await;
    publish(&js, "s.a", "a2").await;
    publish(&js, "s.a", "a3").await;
    // s.a keeps only its last 2 (seqs 3, 4); s.b's message survives.
    let last_a = stream
        .get_last_raw_message_by_subject("s.a")
        .await
        .expect("last s.a");
    assert_eq!(last_a.payload.as_ref(), b"a3");
    let info = stream.info().await.expect("info");
    assert_eq!(info.state.messages, 3, "a1 evicted, b1 + a2 + a3 remain");
    let b = stream.get_raw_message(2).await.expect("s.b survives");
    assert_eq!(b.payload.as_ref(), b"b1");
}

#[tokio::test]
async fn deliver_policy_last_and_new() {
    let (_srv, client) = {
        let srv = server::start(js_config(None)).await.expect("server starts");
        let client = async_nats::connect(format!("nats://{}", srv.addr))
            .await
            .expect("connects");
        (srv, client)
    };
    let js = jetstream::new(client.clone());
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "POL".into(),
            subjects: vec!["p.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    publish(&js, "p.a", "old-a").await;
    publish(&js, "p.b", "old-b").await;
    publish(&js, "p.a", "new-a").await;

    // last with a filter delivers the last matching message only.
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("c-last".into()),
            deliver_policy: DeliverPolicy::Last,
            filter_subject: "p.a".into(),
            ..Default::default()
        })
        .await
        .expect("last consumer");
    let mut messages = consumer.messages().await.expect("messages stream");
    let first = timeout(T, messages.next())
        .await
        .expect("timeout")
        .expect("message")
        .expect("ok");
    assert_eq!(first.payload.as_ref(), b"new-a");

    // new delivers only messages published after creation.
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("c-new".into()),
            deliver_policy: DeliverPolicy::New,
            ..Default::default()
        })
        .await
        .expect("new consumer");
    let mut messages = consumer.messages().await.expect("messages stream");
    publish(&js, "p.c", "fresh").await;
    let first = timeout(T, messages.next())
        .await
        .expect("timeout")
        .expect("message")
        .expect("ok");
    assert_eq!(first.payload.as_ref(), b"fresh");
}
