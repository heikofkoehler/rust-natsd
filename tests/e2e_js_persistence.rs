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

// --- Area 4: pending ack state survives restart (Go v2.15.1 semantics,
// probed first: unacked messages are NOT redelivered immediately after
// a restart inside the ack window, and ARE redelivered — with the
// delivery count incremented and a fresh consumer seq — once the
// window expires; acked messages are never redelivered). ---

async fn fetch_one(
    consumer: &jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
    n: usize,
) -> Vec<(Vec<u8>, i64)> {
    let mut batch = consumer
        .fetch()
        .max_messages(n)
        .messages()
        .await
        .expect("fetch");
    let mut out = Vec::new();
    for _ in 0..n {
        let msg = timeout(T, batch.next())
            .await
            .expect("fetch timeout")
            .expect("message")
            .expect("ok");
        let delivered = msg.info().expect("info").delivered;
        out.push((msg.payload.to_vec(), delivered));
        // Deliberately not acked by the caller unless it acks explicitly.
        // (The message is dropped here; the server-side pending entry
        // is what matters, and acking is done by the caller when wanted.)
        // NOTE: dropping without ack is the point of these tests.
        // To keep the message handle alive for an explicit ack, callers
        // use their own fetch loops instead of this helper where an
        // ack is needed.
        let _ = msg;
    }
    out
}

#[tokio::test]
async fn pending_ack_state_survives_restart_pull() {
    let dir = tmpdir("pending-pull");
    let srv = server::start(js_config(Some(dir.clone())))
        .await
        .expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "PEND".into(),
            subjects: vec!["pend.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    publish(&js, "pend.1", "m1").await;
    publish(&js, "pend.1", "m2").await;
    publish(&js, "pend.1", "m3").await;
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("c-pend".into()),
            ack_wait: Duration::from_secs(3),
            ..Default::default()
        })
        .await
        .expect("consumer");
    // Deliver m1, m2 without acking.
    let got = fetch_one(&consumer, 2).await;
    assert_eq!(got[0].0, b"m1");
    assert_eq!(got[1].0, b"m2");
    assert_eq!(got[0].1, 1);
    assert_eq!(got[1].1, 1);
    drop(client);
    srv.shutdown().await.expect("shutdown");

    // Restart: pending state must survive, with the ack window
    // preserved (not reset, not dropped).
    let srv = server::start(js_config(Some(dir.clone())))
        .await
        .expect("server restarts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    let stream = js.get_stream("PEND").await.expect("stream survives");
    let info = stream
        .consumer_info("c-pend")
        .await
        .expect("consumer survives");
    assert_eq!(info.num_ack_pending, 2, "unacked m1, m2 still pending");
    assert_eq!(info.delivered.stream_sequence, 2);
    let consumer = stream
        .get_consumer::<jetstream::consumer::pull::Config>("c-pend")
        .await
        .expect("get consumer");
    // Inside the original ack window, only the fresh message is
    // available — m1/m2 are NOT redelivered yet (Go behaviour).
    let got = fetch_one(&consumer, 1).await;
    assert_eq!(got[0].0, b"m3", "fresh message first, no early redelivery");
    assert_eq!(got[0].1, 1);
    // Ack m3 at once so it cannot redeliver later and pollute the
    // redelivery assertion below — not possible from here (fetch_one
    // dropped its handle), so instead the assertion below fetches up
    // to 3 and expects m3 last, as its own window also expires.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let got = fetch_one(&consumer, 3).await;
    // m1, m2 redelivered first (stream order), each with delivered=2;
    // m3 follows as its own window also expired (it was never acked).
    assert_eq!(got[0].0, b"m1");
    assert_eq!(got[1].0, b"m2");
    assert_eq!(got[0].1, 2, "first redelivery carries delivered=2");
    assert_eq!(got[1].1, 2);
    assert_eq!(got[2].0, b"m3");
    assert_eq!(got[2].1, 2);
    srv.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn acked_messages_not_redelivered_after_restart() {
    let dir = tmpdir("pending-acked");
    let srv = server::start(js_config(Some(dir.clone())))
        .await
        .expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "ACKD".into(),
            subjects: vec!["ackd.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    publish(&js, "ackd.1", "a1").await;
    publish(&js, "ackd.1", "a2").await;
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("c-ackd".into()),
            ack_wait: Duration::from_secs(2),
            ..Default::default()
        })
        .await
        .expect("consumer");
    let mut batch = consumer
        .fetch()
        .max_messages(2)
        .messages()
        .await
        .expect("fetch");
    let first = timeout(T, batch.next())
        .await
        .expect("timeout")
        .expect("message")
        .expect("ok");
    assert_eq!(first.payload.as_ref(), b"a1");
    first.ack().await.expect("ack a1");
    // The ack is fire-and-forget on the wire: wait until the server
    // has processed it before shutting down, or the restart would
    // legitimately see a1 still pending.
    for _ in 0..50 {
        let info = stream.consumer_info("c-ackd").await.expect("consumer info");
        if info.num_ack_pending == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let second = timeout(T, batch.next())
        .await
        .expect("timeout")
        .expect("message")
        .expect("ok");
    assert_eq!(second.payload.as_ref(), b"a2");
    // a2 deliberately unacked.
    drop(client);
    srv.shutdown().await.expect("shutdown");

    let srv = server::start(js_config(Some(dir.clone())))
        .await
        .expect("server restarts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    let stream = js.get_stream("ACKD").await.expect("stream survives");
    let info = stream
        .consumer_info("c-ackd")
        .await
        .expect("consumer survives");
    assert_eq!(info.num_ack_pending, 1, "only a2 still pending");
    assert_eq!(
        info.ack_floor.stream_sequence, 1,
        "ack floor survived the restart"
    );
    let consumer = stream
        .get_consumer::<jetstream::consumer::pull::Config>("c-ackd")
        .await
        .expect("get consumer");
    tokio::time::sleep(Duration::from_secs(3)).await;
    // Exactly one redelivery: a2, delivered=2. a1 never comes back.
    let got = fetch_one(&consumer, 1).await;
    assert_eq!(got[0].0, b"a2");
    assert_eq!(got[0].1, 2);
    // No further messages: a second fetch must not yield a real
    // message. (The fetch stream may surface the server's 404
    // No-Messages status as an empty-payload message; that is not a
    // redelivery.)
    let mut batch = consumer
        .fetch()
        .max_messages(1)
        .expires(Duration::from_millis(500))
        .messages()
        .await
        .expect("fetch");
    if let Ok(Some(Ok(extra))) = timeout(Duration::from_secs(3), batch.next()).await {
        let is_status = extra
            .headers
            .as_ref()
            .and_then(|h| h.get("Status"))
            .map(|v| v.as_str() == "404")
            .unwrap_or(false);
        if !is_status {
            panic!(
                "unexpected extra redelivery after a2: {:?}",
                String::from_utf8_lossy(&extra.payload)
            );
        }
    }
    srv.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn pending_ack_state_survives_restart_push() {
    let dir = tmpdir("pending-push");
    let srv = server::start(js_config(Some(dir.clone())))
        .await
        .expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "PPUSH".into(),
            subjects: vec!["ppush.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    let consumer = stream
        .create_consumer(jetstream::consumer::push::Config {
            deliver_subject: "deliver.ppush".into(),
            durable_name: Some("c-ppush".into()),
            ack_wait: Duration::from_secs(3),
            ..Default::default()
        })
        .await
        .expect("consumer");
    let mut messages = consumer.messages().await.expect("push messages");
    publish(&js, "ppush.1", "p1").await;
    publish(&js, "ppush.1", "p2").await;
    for want in ["p1", "p2"] {
        let msg = timeout(T, messages.next())
            .await
            .expect("timeout")
            .expect("message")
            .expect("ok");
        assert_eq!(msg.payload.as_ref(), want.as_bytes());
        // Deliberately not acked.
    }
    drop(client);
    srv.shutdown().await.expect("shutdown");

    // Restart and resubscribe promptly (inside the ack window): the
    // unacked push messages must redeliver once the window expires,
    // with delivered=2 — not immediately, and not lost.
    let srv = server::start(js_config(Some(dir.clone())))
        .await
        .expect("server restarts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    let stream = js.get_stream("PPUSH").await.expect("stream survives");
    let consumer = stream
        .get_consumer::<jetstream::consumer::push::Config>("c-ppush")
        .await
        .expect("get consumer");
    let mut messages = consumer.messages().await.expect("push messages");
    for want in ["p1", "p2"] {
        let msg = timeout(T, messages.next())
            .await
            .expect("timeout")
            .expect("message")
            .expect("ok");
        assert_eq!(msg.payload.as_ref(), want.as_bytes());
        assert_eq!(
            msg.info().expect("info").delivered,
            2,
            "push redelivery carries delivered=2"
        );
    }
    srv.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn dedup_window_survives_restart() {
    let dir = tmpdir("pending-dedup");
    let srv = server::start(js_config(Some(dir.clone())))
        .await
        .expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    let _stream = js
        .create_stream(jetstream::stream::Config {
            name: "DEDUP".into(),
            subjects: vec!["dedup.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", "msg-1");
    let ack = js
        .publish_with_headers("dedup.1", headers.clone(), Bytes::from("once"))
        .await
        .expect("publish")
        .await
        .expect("ack");
    assert!(!ack.duplicate);
    drop(client);
    srv.shutdown().await.expect("shutdown");

    let srv = server::start(js_config(Some(dir.clone())))
        .await
        .expect("server restarts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    let ack = js
        .publish_with_headers("dedup.1", headers, Bytes::from("once"))
        .await
        .expect("publish")
        .await
        .expect("ack");
    assert!(ack.duplicate, "same msg id after restart is a duplicate");
    assert_eq!(ack.sequence, 1);
    let mut stream = js.get_stream("DEDUP").await.expect("stream survives");
    let info = stream.info().await.expect("info");
    assert_eq!(info.state.messages, 1, "no duplicate message stored");
    srv.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}
