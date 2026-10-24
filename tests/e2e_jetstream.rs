//! JetStream end-to-end tests: streams, publish acks, dedup, pull and
//! push consumers, ack/nak/redelivery, purge and delete, exercised with
//! the official async-nats client plus raw $JS.API requests.

use std::time::Duration;

use async_nats::jetstream::consumer::{pull, push};
use async_nats::jetstream::{self, consumer::DeliverPolicy};
use bytes::Bytes;
use futures::StreamExt;
use rust_natsd::config::Config;
use rust_natsd::server::{self, RunningServer};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);

fn base_config() -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        jetstream: true,
        ..Default::default()
    }
}

async fn start_js() -> (RunningServer, async_nats::Client) {
    let srv = server::start(base_config()).await.expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("async-nats connects");
    (srv, client)
}

fn js(client: &async_nats::Client) -> jetstream::Context {
    jetstream::new(client.clone())
}

/// Raw $JS.API request; returns the parsed JSON response body.
async fn api(
    client: &async_nats::Client,
    subject: &str,
    payload: serde_json::Value,
) -> serde_json::Value {
    let msg = client
        .request(subject.to_string(), Bytes::from(payload.to_string()))
        .await
        .expect("api request answers");
    serde_json::from_slice(&msg.payload).expect("api response is json")
}

#[tokio::test]
async fn stream_create_publish_get() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "ORDERS".into(),
            subjects: vec!["orders.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    for (i, body) in ["m1", "m2", "m3"].iter().enumerate() {
        let ack = js
            .publish(format!("orders.{}", i + 1), Bytes::from(*body))
            .await
            .expect("send")
            .await
            .expect("ack");
        assert_eq!(ack.stream, "ORDERS");
        assert_eq!(ack.sequence, (i + 1) as u64);
        assert!(!ack.duplicate);
    }
    let msg = stream.get_raw_message(2).await.expect("get message 2");
    assert_eq!(msg.payload.as_ref(), b"m2");
    assert_eq!(msg.subject.as_str(), "orders.2");
    let last = stream
        .get_last_raw_message_by_subject("orders.2")
        .await
        .expect("last by subject");
    assert_eq!(last.sequence, 2);
}

#[tokio::test]
async fn publish_dedup_and_expected_sequence() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    js.create_stream(jetstream::stream::Config {
        name: "DEDUP".into(),
        subjects: vec!["dedup.>".into()],
        ..Default::default()
    })
    .await
    .expect("create stream");
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", "id-1");
    let ack1 = js
        .publish_with_headers("dedup.a", headers.clone(), Bytes::from("first"))
        .await
        .expect("send")
        .await
        .expect("first publish");
    let ack2 = js
        .publish_with_headers("dedup.a", headers, Bytes::from("first"))
        .await
        .expect("send")
        .await
        .expect("duplicate publish");
    assert_eq!(ack1.sequence, 1);
    assert!(ack2.duplicate);
    assert_eq!(ack2.sequence, 1);
    // Wrong expected last sequence is rejected.
    let mut bad = async_nats::HeaderMap::new();
    bad.insert("Nats-Expected-Last-Sequence", "99");
    let fut = js
        .publish_with_headers("dedup.a", bad, Bytes::from("bad"))
        .await
        .expect("send");
    assert!(fut.await.is_err(), "expected-sequence mismatch must fail");
}

#[tokio::test]
async fn pull_consumer_fetch_and_ack() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "PULL".into(),
            subjects: vec!["pull.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    for i in 0..3 {
        js.publish(format!("pull.{i}"), Bytes::from(format!("p{i}")))
            .await
            .expect("send")
            .await
            .expect("ack");
    }
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("c1".into()),
            ..Default::default()
        })
        .await
        .expect("create pull consumer");
    let mut batch = consumer
        .fetch()
        .max_messages(2)
        .messages()
        .await
        .expect("fetch");
    let mut got = Vec::new();
    while let Ok(Some(msg)) = timeout(T, batch.next()).await {
        let msg = msg.expect("fetch message");
        got.push(msg.payload.to_vec());
        msg.ack().await.expect("ack");
    }
    assert_eq!(got, vec![b"p0".to_vec(), b"p1".to_vec()]);
    // Consumer info shows no pending acks after both acks.
    let info = api(
        &client,
        "$JS.API.CONSUMER.INFO.PULL.c1",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(info["num_ack_pending"], 0);
}

#[tokio::test]
async fn pull_consumer_nak_redelivers() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "NAK".into(),
            subjects: vec!["nak.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    js.publish("nak.1", Bytes::from("only"))
        .await
        .expect("publish");
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("c1".into()),
            ..Default::default()
        })
        .await
        .expect("create pull consumer");
    let mut batch = consumer
        .fetch()
        .max_messages(1)
        .messages()
        .await
        .expect("fetch 1");
    let msg = timeout(T, batch.next())
        .await
        .expect("first fetch yields")
        .expect("some")
        .expect("message");
    assert_eq!(msg.payload.as_ref(), b"only");
    msg.ack_with(jetstream::message::AckKind::Nak(None))
        .await
        .expect("nak");
    let mut batch2 = consumer
        .fetch()
        .max_messages(1)
        .messages()
        .await
        .expect("fetch 2");
    let msg2 = timeout(T, batch2.next())
        .await
        .expect("redelivery yields")
        .expect("some")
        .expect("message");
    assert_eq!(msg2.payload.as_ref(), b"only");
    msg2.ack().await.expect("ack redelivery");
}

#[tokio::test]
async fn pull_consumer_redelivery_on_ack_wait() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "RED".into(),
            subjects: vec!["red.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    js.publish("red.1", Bytes::from("slow"))
        .await
        .expect("publish");
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("c1".into()),
            ack_wait: Duration::from_millis(300),
            ..Default::default()
        })
        .await
        .expect("create pull consumer");
    let mut batch = consumer
        .fetch()
        .max_messages(1)
        .messages()
        .await
        .expect("fetch 1");
    let msg = timeout(T, batch.next())
        .await
        .expect("first fetch yields")
        .expect("some")
        .expect("message");
    assert_eq!(msg.payload.as_ref(), b"slow");
    // Do not ack: after ack_wait + tick the message must redeliver.
    tokio::time::sleep(Duration::from_millis(900)).await;
    let mut batch2 = consumer
        .fetch()
        .max_messages(1)
        .messages()
        .await
        .expect("fetch 2");
    let msg2 = timeout(T, batch2.next())
        .await
        .expect("redelivery yields")
        .expect("some")
        .expect("message");
    assert_eq!(msg2.payload.as_ref(), b"slow");
    msg2.ack().await.expect("ack");
}

#[tokio::test]
async fn push_consumer_delivery() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "PUSH".into(),
            subjects: vec!["push.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    let consumer = stream
        .create_consumer(push::Config {
            deliver_subject: "deliver.push".into(),
            durable_name: Some("p1".into()),
            ..Default::default()
        })
        .await
        .expect("create push consumer");
    // Subscribe before publishing: a push delivery with no subscriber
    // on the deliver subject is lost (same as nats-server).
    let mut messages = consumer.messages().await.expect("push messages");
    js.publish("push.1", Bytes::from("hello"))
        .await
        .expect("publish");
    let msg = timeout(T, messages.next())
        .await
        .expect("push delivery yields")
        .expect("some")
        .expect("message");
    assert_eq!(msg.payload.as_ref(), b"hello");
    msg.ack().await.expect("ack");
}

#[tokio::test]
async fn push_consumer_backlog_on_create() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "BACKLOG".into(),
            subjects: vec!["back.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    js.publish("back.1", Bytes::from("old1"))
        .await
        .expect("publish 1");
    js.publish("back.2", Bytes::from("old2"))
        .await
        .expect("publish 2");
    // Subscribe to the deliver subject BEFORE creating the consumer:
    // the backlog drain happens at creation time.
    let mut sub = client
        .subscribe("deliver.back".to_string())
        .await
        .expect("subscribe deliver subject");
    let _consumer = stream
        .create_consumer(push::Config {
            deliver_subject: "deliver.back".into(),
            durable_name: Some("p1".into()),
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .expect("create push consumer");
    let mut got = Vec::new();
    for _ in 0..2 {
        let msg = timeout(T, sub.next())
            .await
            .expect("backlog yields")
            .expect("message");
        got.push(msg.payload.to_vec());
        // Ack on the wire: publish empty to the ack reply subject.
        let reply = msg.reply.expect("ack reply subject");
        client.publish(reply, Bytes::new()).await.expect("ack");
    }
    assert_eq!(got, vec![b"old1".to_vec(), b"old2".to_vec()]);
}

#[tokio::test]
async fn raw_api_list_purge_delete() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    js.create_stream(jetstream::stream::Config {
        name: "RAW".into(),
        subjects: vec!["raw.>".into()],
        ..Default::default()
    })
    .await
    .expect("create stream");
    for i in 0..2 {
        js.publish(format!("raw.{i}"), Bytes::from("x"))
            .await
            .expect("publish");
    }
    let list = api(&client, "$JS.API.STREAM.LIST", serde_json::json!({})).await;
    assert_eq!(list["total"], 1);
    let msg_get = api(
        &client,
        "$JS.API.STREAM.MSG.GET.RAW",
        serde_json::json!({ "seq": 2 }),
    )
    .await;
    assert_eq!(msg_get["message"]["seq"], 2);
    let last = api(
        &client,
        "$JS.API.STREAM.MSG.GET.RAW",
        serde_json::json!({ "last_by_subj": "raw.1" }),
    )
    .await;
    assert_eq!(last["message"]["subject"], "raw.1");
    let purge = api(&client, "$JS.API.STREAM.PURGE.RAW", serde_json::json!({})).await;
    assert_eq!(purge["purged"], 2);
    let info = api(&client, "$JS.API.STREAM.INFO.RAW", serde_json::json!({})).await;
    assert_eq!(info["state"]["messages"], 0);
    let del = api(&client, "$JS.API.STREAM.DELETE.RAW", serde_json::json!({})).await;
    assert_eq!(del["success"], true);
    let gone = api(&client, "$JS.API.STREAM.INFO.RAW", serde_json::json!({})).await;
    assert!(gone.get("error").is_some(), "deleted stream must error");
}
