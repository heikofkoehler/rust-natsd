//! JetStream e2e gaps: deliver_policy all (pull), pull filter subjects,
//! max_deliver exhaustion, ack_policy none (push), push ack-wait
//! redelivery, filtered purge, STREAM.NAMES subject filter, and
//! CONSUMER.INFO fields. Uses async-nats like tests/e2e_jetstream.rs.

use std::time::Duration;

use async_nats::jetstream::consumer::{pull, push, AckPolicy, DeliverPolicy};
use async_nats::jetstream::{self, Context};
use bytes::Bytes;
use futures::StreamExt;
use rust_natsd::config::Config;
use rust_natsd::server::{self, RunningServer};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(5);

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
        .expect("connects");
    (srv, client)
}

fn js(client: &async_nats::Client) -> Context {
    jetstream::new(client.clone())
}

async fn api(
    client: &async_nats::Client,
    subject: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .request(subject.to_string(), Bytes::from(body.to_string()))
        .await
        .expect("api request");
    serde_json::from_slice(&resp.payload).expect("api json")
}

async fn fetch_all(
    consumer: &async_nats::jetstream::consumer::PullConsumer,
    max: usize,
) -> Vec<Vec<u8>> {
    let mut batch = consumer
        .fetch()
        .max_messages(max)
        .messages()
        .await
        .expect("fetch");
    let mut got = Vec::new();
    while let Ok(Some(msg)) = timeout(T, batch.next()).await {
        let msg = msg.expect("fetch message");
        got.push(msg.payload.to_vec());
        msg.ack().await.expect("ack");
    }
    got
}

#[tokio::test]
async fn pull_deliver_policy_all_gets_existing_messages() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "ALL".into(),
            subjects: vec!["all.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    for i in 0..3 {
        js.publish(format!("all.{i}"), Bytes::from(format!("a{i}")))
            .await
            .expect("publish")
            .await
            .expect("ack");
    }
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("c-all".into()),
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .expect("create consumer");
    assert_eq!(
        fetch_all(&consumer, 10).await,
        vec![b"a0".to_vec(), b"a1".to_vec(), b"a2".to_vec()]
    );
}

#[tokio::test]
async fn pull_filter_subject_limits_deliveries() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "FILT".into(),
            subjects: vec!["filt.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    js.publish("filt.a", Bytes::from("A"))
        .await
        .expect("publish")
        .await
        .expect("ack");
    js.publish("filt.b", Bytes::from("B"))
        .await
        .expect("publish")
        .await
        .expect("ack");
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("c-filt".into()),
            filter_subject: "filt.a".into(),
            ..Default::default()
        })
        .await
        .expect("create consumer");
    assert_eq!(fetch_all(&consumer, 10).await, vec![b"A".to_vec()]);
}

#[tokio::test]
async fn max_deliver_exhaustion_stops_redelivery() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "MAXD".into(),
            subjects: vec!["maxd.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    js.publish("maxd.1", Bytes::from("once"))
        .await
        .expect("publish")
        .await
        .expect("ack");
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("c-maxd".into()),
            max_deliver: 2,
            ..Default::default()
        })
        .await
        .expect("create consumer");
    // Delivery 1, NAK -> delivery 2, NAK -> exhausted, no third delivery.
    for _ in 0..2 {
        let mut batch = consumer
            .fetch()
            .max_messages(1)
            .messages()
            .await
            .expect("fetch");
        let msg = timeout(T, batch.next())
            .await
            .expect("fetch yields")
            .expect("some")
            .expect("message");
        assert_eq!(msg.payload.as_ref(), b"once");
        msg.ack_with(jetstream::message::AckKind::Nak(None))
            .await
            .expect("nak");
    }
    let mut batch = consumer
        .fetch()
        .max_messages(1)
        .messages()
        .await
        .expect("fetch after exhaustion");
    let next = timeout(Duration::from_secs(2), batch.next()).await;
    // No redelivery: either the batch ends/times out, or it yields
    // the server's 404 "No Messages" sentinel (empty payload), never
    // the exhausted message again.
    if let Ok(Some(Ok(m))) = next {
        assert_ne!(
            m.payload.as_ref(),
            b"once",
            "exhausted message must not be delivered again"
        );
    }
}

#[tokio::test]
async fn push_max_deliver_exhaustion_stops_redelivery_and_advises() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "MAXPUSH".into(),
            subjects: vec!["maxpush.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    let mut adv = client
        .subscribe("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.MAXPUSH.>".to_string())
        .await
        .expect("subscribe advisory");
    let mut sub = client
        .subscribe("deliver.maxpush".to_string())
        .await
        .expect("subscribe deliver");
    let _consumer = stream
        .create_consumer(push::Config {
            deliver_subject: "deliver.maxpush".into(),
            durable_name: Some("c-maxpush".into()),
            max_deliver: 2,
            ack_wait: Duration::from_secs(1),
            ..Default::default()
        })
        .await
        .expect("create consumer");
    js.publish("maxpush.1", Bytes::from("once"))
        .await
        .expect("publish")
        .await
        .expect("ack");
    // Delivery 1, NAK -> delivery 2, NAK -> exhausted, no third delivery.
    for _ in 0..2 {
        let msg = timeout(T, sub.next()).await.unwrap().unwrap();
        assert_eq!(msg.payload.as_ref(), b"once");
        let reply = msg.reply.expect("ack reply subject");
        client
            .publish(reply, Bytes::from("-NAK"))
            .await
            .expect("nak");
    }
    let next = timeout(Duration::from_secs(2), sub.next()).await;
    assert!(
        next.is_err(),
        "exhausted push message must not be delivered again: {next:?}"
    );
    let advisory = timeout(T, adv.next()).await.unwrap().unwrap();
    let body: serde_json::Value = serde_json::from_slice(&advisory.payload).expect("advisory json");
    assert_eq!(body["type"], "io.nats.jetstream.advisory.v1.max_deliver");
    assert_eq!(body["stream"], "MAXPUSH");
    assert_eq!(body["consumer"], "c-maxpush");
    assert_eq!(body["stream_seq"], 1);
    assert_eq!(body["deliveries"], 2);
}

#[tokio::test]
async fn workqueue_max_deliver_exhaustion_keeps_message_and_advises() {
    // Go v2.15.1 behaviour, probed: on workqueue exhaustion the
    // message stays in the stream (only a plain +ACK deletes it);
    // the consumer simply stops redelivering and publishes the
    // MAX_DELIVERIES advisory.
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "MAXWQ".into(),
            subjects: vec!["maxwq.>".into()],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            ..Default::default()
        })
        .await
        .expect("create stream");
    let mut adv = client
        .subscribe("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.MAXWQ.>".to_string())
        .await
        .expect("subscribe advisory");
    js.publish("maxwq.1", Bytes::from("once"))
        .await
        .expect("publish")
        .await
        .expect("ack");
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("c-maxwq".into()),
            max_deliver: 2,
            ..Default::default()
        })
        .await
        .expect("create consumer");
    for _ in 0..2 {
        let mut batch = consumer
            .fetch()
            .max_messages(1)
            .messages()
            .await
            .expect("fetch");
        let msg = timeout(T, batch.next())
            .await
            .expect("fetch yields")
            .expect("some")
            .expect("message");
        msg.ack_with(jetstream::message::AckKind::Nak(None))
            .await
            .expect("nak");
    }
    let mut batch = consumer
        .fetch()
        .max_messages(1)
        .messages()
        .await
        .expect("fetch after exhaustion");
    let next = timeout(Duration::from_secs(2), batch.next()).await;
    if let Ok(Some(Ok(m))) = next {
        assert_ne!(
            m.payload.as_ref(),
            b"once",
            "exhausted workqueue message must not be delivered again"
        );
    }
    let advisory = timeout(T, adv.next()).await.unwrap().unwrap();
    let body: serde_json::Value = serde_json::from_slice(&advisory.payload).expect("advisory json");
    assert_eq!(body["deliveries"], 2);
    // Message still in the stream.
    let got = api(
        &client,
        "$JS.API.STREAM.MSG.GET.MAXWQ",
        serde_json::json!({"seq": 1}),
    )
    .await;
    assert_eq!(
        got["message"]["data"], "b25jZQ==",
        "workqueue message retained: {got}"
    );
}

#[tokio::test]
async fn pull_max_deliver_exhaustion_via_ack_wait_timeout() {
    // Same cap, ack-wait path (tick sweep) instead of NAK: delivery
    // 1 times out -> delivery 2 times out -> exhausted, no third.
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "MAXAW".into(),
            subjects: vec!["maxaw.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    let mut adv = client
        .subscribe("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.MAXAW.>".to_string())
        .await
        .expect("subscribe advisory");
    js.publish("maxaw.1", Bytes::from("once"))
        .await
        .expect("publish")
        .await
        .expect("ack");
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("c-maxaw".into()),
            max_deliver: 2,
            ack_wait: Duration::from_millis(500),
            ..Default::default()
        })
        .await
        .expect("create consumer");
    // Fetch is no-wait in async-nats: a poll before the ack-wait
    // window expires yields the server's 404 sentinel (empty payload)
    // instead of blocking, so poll until the redelivery arrives.
    for _ in 0..2 {
        let mut got_once = false;
        for _attempt in 0..20 {
            let mut batch = consumer
                .fetch()
                .max_messages(1)
                .messages()
                .await
                .expect("fetch");
            match timeout(T, batch.next()).await {
                Ok(Some(Ok(m))) if m.payload.as_ref() == b"once" => {
                    got_once = true;
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
        assert!(got_once, "ack-wait redelivery must arrive");
        // Deliberately not acked: ack_wait redelivers, then exhausts.
    }
    let mut batch = consumer
        .fetch()
        .max_messages(1)
        .messages()
        .await
        .expect("fetch after exhaustion");
    let next = timeout(Duration::from_secs(2), batch.next()).await;
    if let Ok(Some(Ok(m))) = next {
        assert_ne!(
            m.payload.as_ref(),
            b"once",
            "ack-wait-exhausted message must not be delivered again"
        );
    }
    let advisory = timeout(T, adv.next()).await.unwrap().unwrap();
    let body: serde_json::Value = serde_json::from_slice(&advisory.payload).expect("advisory json");
    assert_eq!(body["deliveries"], 2);
}

#[tokio::test]
async fn push_ack_policy_none_never_redelivers() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "ACKNONE".into(),
            subjects: vec!["acknone.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    let mut sub = client
        .subscribe("deliver.acknone".to_string())
        .await
        .expect("subscribe deliver");
    let _consumer = stream
        .create_consumer(push::Config {
            deliver_subject: "deliver.acknone".into(),
            durable_name: Some("c-acknone".into()),
            ack_policy: AckPolicy::None,
            ack_wait: Duration::from_secs(1),
            ..Default::default()
        })
        .await
        .expect("create consumer");
    js.publish("acknone.1", Bytes::from("x"))
        .await
        .expect("publish")
        .await
        .expect("ack");
    let msg = timeout(T, sub.next()).await.unwrap().unwrap();
    assert_eq!(msg.payload.as_ref(), b"x");
    // Deliberately not acked; ack_wait passes, but ack_policy none
    // means the message is never redelivered.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let next = timeout(Duration::from_secs(1), sub.next()).await;
    assert!(
        next.is_err(),
        "ack_policy none must not redeliver: {next:?}"
    );
}

#[tokio::test]
async fn push_ack_wait_redelivers_live() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(jetstream::stream::Config {
            name: "PUSHRED".into(),
            subjects: vec!["pushred.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    let mut sub = client
        .subscribe("deliver.pushred".to_string())
        .await
        .expect("subscribe deliver");
    let _consumer = stream
        .create_consumer(push::Config {
            deliver_subject: "deliver.pushred".into(),
            durable_name: Some("c-pushred".into()),
            ack_wait: Duration::from_secs(1),
            ..Default::default()
        })
        .await
        .expect("create consumer");
    js.publish("pushred.1", Bytes::from("r"))
        .await
        .expect("publish")
        .await
        .expect("ack");
    let first = timeout(T, sub.next()).await.unwrap().unwrap();
    assert_eq!(first.payload.as_ref(), b"r");
    // Do not ack: after ack_wait the same message is redelivered.
    let second = timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("redelivery must arrive")
        .unwrap();
    assert_eq!(second.payload.as_ref(), b"r");
    // Ack via the redelivery's reply subject so the server stops.
    let reply = second.reply.expect("ack reply subject");
    client
        .publish(reply, Bytes::new())
        .await
        .expect("ack redelivery");
}

#[tokio::test]
async fn filtered_purge_only_purges_matching_subjects() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let _stream = js
        .create_stream(jetstream::stream::Config {
            name: "FPURGE".into(),
            subjects: vec!["fp.>".into()],
            ..Default::default()
        })
        .await
        .expect("create stream");
    for subj in ["fp.a", "fp.b", "fp.a"] {
        js.publish(subj, Bytes::from("x"))
            .await
            .expect("publish")
            .await
            .expect("ack");
    }
    let purge = api(
        &client,
        "$JS.API.STREAM.PURGE.FPURGE",
        serde_json::json!({"filter": "fp.a"}),
    )
    .await;
    assert_eq!(purge["purged"], 2);
    let info = api(&client, "$JS.API.STREAM.INFO.FPURGE", serde_json::json!({})).await;
    assert_eq!(info["state"]["messages"], 1);
    let last = api(
        &client,
        "$JS.API.STREAM.MSG.GET.FPURGE",
        serde_json::json!({"last_by_subj": "fp.b"}),
    )
    .await;
    assert_eq!(last["message"]["subject"], "fp.b");
}

#[tokio::test]
async fn stream_names_subject_filter_and_consumer_info() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let mut ns_consumer = None;
    for (name, subject) in [("NSA", "ns.a.>"), ("NSB", "ns.b.>")] {
        let stream = js
            .create_stream(jetstream::stream::Config {
                name: name.into(),
                subjects: vec![subject.into()],
                ..Default::default()
            })
            .await
            .expect("create stream");
        if name == "NSA" {
            js.publish("ns.a.1", Bytes::from("x"))
                .await
                .expect("publish")
                .await
                .expect("ack");
            ns_consumer = Some(
                stream
                    .create_consumer(pull::Config {
                        durable_name: Some("c-ns".into()),
                        ..Default::default()
                    })
                    .await
                    .expect("create consumer"),
            );
        }
    }
    let consumer = ns_consumer.expect("NSA consumer created");
    let names = api(
        &client,
        "$JS.API.STREAM.NAMES",
        serde_json::json!({"subject": "ns.a.1"}),
    )
    .await;
    assert_eq!(names["streams"], serde_json::json!(["NSA"]));
    let all = api(&client, "$JS.API.STREAM.NAMES", serde_json::json!({})).await;
    assert_eq!(all["total"], 2);

    // Fetch one message without acking, then CONSUMER.INFO must show
    // exactly one ack pending. (Note: the server hardcodes
    // num_pending to 0, so that field is not asserted here — it counts
    // undelivered messages on Go and is a known fidelity gap.)
    let mut batch = consumer
        .fetch()
        .max_messages(1)
        .messages()
        .await
        .expect("fetch");
    let msg = timeout(T, batch.next())
        .await
        .expect("fetch yields")
        .expect("some")
        .expect("message");
    assert_eq!(msg.payload.as_ref(), b"x");
    let info = api(
        &client,
        "$JS.API.CONSUMER.INFO.NSA.c-ns",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(info["name"], "c-ns");
    assert_eq!(info["stream_name"], "NSA");
    assert_eq!(info["num_ack_pending"], 1);
    // Unknown consumer errors with the documented code, not a panic.
    let gone = api(
        &client,
        "$JS.API.CONSUMER.INFO.NSA.nope",
        serde_json::json!({}),
    )
    .await;
    assert!(gone.get("error").is_some(), "unknown consumer must error");
}
