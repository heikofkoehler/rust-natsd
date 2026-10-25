//! Phase 31: KeyValue via the official async-nats client (which, unlike
//! nats.go, applies no server-version gate — so KV helpers genuinely
//! run against rust-natsd), plus raw-wire coverage for DIRECT.GET,
//! STREAM.MSG.DELETE, STREAM.UPDATE, deny_delete / deny_purge, and
//! Nats-Rollup publishes. Every assertion here is a behaviour Go
//! v2.15.1 was probed to have; the nats.go version gate itself
//! (server 0.1.0 < 2.6.2) is documented in parity/KNOWN_DIFFS.md and
//! is deliberately not papered over by these tests.

use std::time::Duration;

use async_nats::jetstream::kv::{Config as KvConfig, Operation};
use async_nats::jetstream::stream::{Config as StreamConfig, DirectGetErrorKind};
use async_nats::jetstream::{self};
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

/// Raw STREAM.INFO for one stream (avoids async-nats' private `info` field).
async fn stream_info(client: &async_nats::Client, name: &str) -> serde_json::Value {
    api(
        client,
        &format!("$JS.API.STREAM.INFO.{name}"),
        serde_json::json!({}),
    )
    .await
}

fn err_code(resp: &serde_json::Value) -> u64 {
    resp["error"]["err_code"].as_u64().unwrap_or(0)
}

#[tokio::test]
async fn kv_put_get_delete_purge_via_async_nats() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let kv = js
        .create_key_value(KvConfig {
            bucket: "TESTKV".into(),
            ..Default::default()
        })
        .await
        .expect("create_key_value");
    // async-nats create_key_value sets allow_direct + deny_delete +
    // allow_rollup on the KV_<bucket> stream; the stream info must
    // echo them back (the KV getter keys its direct path off this).
    let info = stream_info(&client, "KV_TESTKV").await;
    assert_eq!(
        info["config"]["allow_direct"], true,
        "KV stream must allow_direct"
    );
    assert_eq!(
        info["config"]["deny_delete"], true,
        "KV stream must deny_delete"
    );
    assert_eq!(
        info["config"]["allow_rollup_hdrs"], true,
        "KV stream must allow_rollup"
    );

    let rev1 = kv.put("k1", Bytes::from("v1")).await.expect("put k1");
    assert_eq!(rev1, 1);
    assert_eq!(
        kv.get("k1").await.expect("get k1").as_deref(),
        Some(b"v1".as_slice())
    );
    let rev2 = kv.put("k1", Bytes::from("v2")).await.expect("put k1 again");
    assert_eq!(rev2, 2);
    let entry = kv.entry("k1").await.expect("entry").expect("entry present");
    assert_eq!(entry.value.as_ref(), b"v2");
    assert_eq!(entry.revision, 2);
    assert_eq!(entry.operation, Operation::Put);

    // Delete is a DEL marker publish, not STREAM.MSG.DELETE, so the
    // KV bucket's own deny_delete must not block it.
    kv.delete("k1").await.expect("delete k1");
    assert_eq!(kv.get("k1").await.expect("get after delete"), None);
    let entry = kv.entry("k1").await.expect("entry").expect("entry present");
    assert_eq!(entry.operation, Operation::Delete);

    // Purge is a rollup-sub publish of a PURGE marker: it must roll
    // the key's history up to the single purge entry.
    kv.put("k2", Bytes::from("a")).await.expect("put k2");
    kv.put("k2", Bytes::from("b")).await.expect("put k2 again");
    kv.purge("k2").await.expect("purge k2");
    assert_eq!(kv.get("k2").await.expect("get after purge"), None);
    let entry = kv.entry("k2").await.expect("entry").expect("entry present");
    assert_eq!(entry.operation, Operation::Purge);
    // Only the purge marker survives for k2; the whole bucket holds
    // the k1 DEL marker plus it.
    let info = stream_info(&client, "KV_TESTKV").await;
    assert_eq!(info["state"]["messages"], 2);
}

#[tokio::test]
async fn kv_revisions_and_watch_via_async_nats() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let kv = js
        .create_key_value(KvConfig {
            bucket: "HIST".into(),
            history: 5,
            ..Default::default()
        })
        .await
        .expect("create_key_value");
    kv.put("k", Bytes::from("one")).await.expect("put one");
    kv.put("k", Bytes::from("two")).await.expect("put two");
    let rev3 = kv.put("k", Bytes::from("three")).await.expect("put three");
    assert_eq!(rev3, 3);
    // Revision lookup goes through direct_get (allow_direct) when
    // the stream reports it, and through MSG.GET otherwise; both
    // paths must agree here.
    let old = kv
        .entry_for_revision("k", 1)
        .await
        .expect("entry_for_revision")
        .expect("revision 1 present");
    assert_eq!(old.value.as_ref(), b"one");
    assert_eq!(old.revision, 1);

    let mut watch = kv.watch("k").await.expect("watch");
    kv.put("k", Bytes::from("four")).await.expect("put four");
    let got = timeout(T, watch.next())
        .await
        .expect("watch entry arrives")
        .expect("watch stream open")
        .expect("watch entry ok");
    assert_eq!(got.value.as_ref(), b"four");
    assert_eq!(got.operation, Operation::Put);
}

#[tokio::test]
async fn direct_get_forms_and_errors() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    let stream = js
        .create_stream(StreamConfig {
            name: "DG".into(),
            subjects: vec!["dg.>".into()],
            allow_direct: true,
            ..Default::default()
        })
        .await
        .expect("create DG");
    let ack1 = js
        .publish("dg.a", Bytes::from("v1"))
        .await
        .expect("publish ack future")
        .await
        .expect("publish dg.a v1");
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("X-Custom", "yes");
    let ack2 = js
        .publish_with_headers("dg.a", headers, Bytes::from("v2"))
        .await
        .expect("publish ack future")
        .await
        .expect("publish dg.a v2");
    js.publish("dg.b", Bytes::from("vb"))
        .await
        .expect("publish ack future")
        .await
        .expect("publish dg.b");

    // By sequence: payload + headers, including passthrough of the
    // stored message's own headers.
    let msg = stream
        .direct_get(ack2.sequence)
        .await
        .expect("direct_get seq");
    assert_eq!(msg.payload.as_ref(), b"v2");
    assert_eq!(msg.sequence, ack2.sequence);
    assert_eq!(msg.subject.as_str(), "dg.a");
    assert_eq!(msg.headers.get("X-Custom").map(|v| v.as_str()), Some("yes"));
    // Last for subject (subject-form request under the hood).
    let msg = stream
        .direct_get_last_for_subject("dg.a")
        .await
        .expect("direct_get_last");
    assert_eq!(msg.payload.as_ref(), b"v2");
    assert_eq!(msg.sequence, ack2.sequence);
    // First for subject (next_by_subj without a start sequence).
    // direct_get_first_for_subject returns the jetstream Message
    // wrapper, not a StreamMessage: the response message's own
    // subject is the request inbox, so only the payload (and the
    // Nats-Subject header, which StreamMessage parses) identifies
    // the stream message.
    let msg = stream
        .direct_get_first_for_subject("dg.a")
        .await
        .expect("direct_get_first");
    assert_eq!(msg.message.payload.as_ref(), b"v1".as_slice());
    // (The Nats-Subject header itself is proven by the direct_get /
    // direct_get_last StreamMessage paths above, which fail without
    // all four direct-get headers; the first/next wrappers do not
    // surface parsed headers.)
    // Next for subject from a start sequence (inclusive, wildcard).
    let msg = stream
        .direct_get_next_for_subject("dg.*", Some(ack1.sequence))
        .await
        .expect("direct_get_next");
    assert_eq!(msg.message.payload.as_ref(), b"v1".as_slice());
    // Not found: missing sequence and missing subject both map to
    // DirectGetErrorKind::NotFound (Go's 404 status line).
    let err = stream.direct_get(99).await.expect_err("direct_get missing");
    assert_eq!(err.kind(), DirectGetErrorKind::NotFound);
    let err = stream
        .direct_get_last_for_subject("dg.nope")
        .await
        .expect_err("direct_get_last missing");
    assert_eq!(err.kind(), DirectGetErrorKind::NotFound);

    // A stream without allow_direct has no direct-get responder:
    // async-nats surfaces Go's 503 as NoResponders -> Other.
    let plain = js
        .create_stream(StreamConfig {
            name: "ND".into(),
            subjects: vec!["nd.>".into()],
            ..Default::default()
        })
        .await
        .expect("create ND");
    js.publish("nd.a", Bytes::from("x"))
        .await
        .expect("publish ack future")
        .await
        .expect("publish nd.a");
    let err = plain.direct_get(1).await.expect_err("direct_get refused");
    assert_eq!(err.kind(), DirectGetErrorKind::Other);

    // STREAM.UPDATE can flip allow_direct on, after which the same
    // direct get succeeds (Go behaviour, probed).
    js.update_stream(StreamConfig {
        name: "ND".into(),
        subjects: vec!["nd.>".into()],
        allow_direct: true,
        ..Default::default()
    })
    .await
    .expect("update ND allow_direct");
    let msg = plain.direct_get(1).await.expect("direct_get after update");
    assert_eq!(msg.payload.as_ref(), b"x");
}

#[tokio::test]
async fn deny_delete_deny_purge_and_msg_delete() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    js.create_stream(StreamConfig {
        name: "DD".into(),
        subjects: vec!["dd.>".into()],
        deny_delete: true,
        ..Default::default()
    })
    .await
    .expect("create DD");
    js.publish("dd.a", Bytes::from("1"))
        .await
        .expect("publish ack future")
        .await
        .expect("publish dd.a");
    // MSG.DELETE is rejected with Go's 500/10057 and the message stays.
    let resp = api(
        &client,
        "$JS.API.STREAM.MSG.DELETE.DD",
        serde_json::json!({ "seq": 1 }),
    )
    .await;
    assert_eq!(err_code(&resp), 10057);
    assert_eq!(resp["error"]["description"], "message delete not permitted");
    let info = stream_info(&client, "DD").await;
    assert_eq!(info["state"]["messages"], 1);

    js.create_stream(StreamConfig {
        name: "DP".into(),
        subjects: vec!["dp.>".into()],
        deny_purge: true,
        ..Default::default()
    })
    .await
    .expect("create DP");
    js.publish("dp.a", Bytes::from("1"))
        .await
        .expect("publish ack future")
        .await
        .expect("publish dp.a");
    // STREAM.PURGE is rejected with Go's 500/10110, purged 0, and
    // the message stays.
    let resp = api(&client, "$JS.API.STREAM.PURGE.DP", serde_json::json!({})).await;
    assert_eq!(err_code(&resp), 10110);
    assert_eq!(resp["purged"], 0);
    let info = stream_info(&client, "DP").await;
    assert_eq!(info["state"]["messages"], 1);

    // Control: MSG.DELETE on an ordinary stream succeeds and leaves
    // a sequence gap (first_seq jumps, as Go does).
    js.create_stream(StreamConfig {
        name: "OK".into(),
        subjects: vec!["ok.>".into()],
        ..Default::default()
    })
    .await
    .expect("create OK");
    js.publish("ok.a", Bytes::from("1"))
        .await
        .expect("publish ack future")
        .await
        .expect("publish ok.a");
    let resp = api(
        &client,
        "$JS.API.STREAM.MSG.DELETE.OK",
        serde_json::json!({ "seq": 1 }),
    )
    .await;
    assert_eq!(resp["success"], true);
    let info = stream_info(&client, "OK").await;
    assert_eq!(info["state"]["messages"], 0);
    assert_eq!(info["state"]["first_seq"], 2);
    // Deleting the same sequence again is "no message found".
    let resp = api(
        &client,
        "$JS.API.STREAM.MSG.DELETE.OK",
        serde_json::json!({ "seq": 1 }),
    )
    .await;
    assert_eq!(err_code(&resp), 10057);
    assert_eq!(resp["error"]["description"], "no message found");
    // Deleting past the end is Go's "stream store EOF".
    let resp = api(
        &client,
        "$JS.API.STREAM.MSG.DELETE.OK",
        serde_json::json!({ "seq": 99 }),
    )
    .await;
    assert_eq!(resp["error"]["description"], "stream store EOF");
}

#[tokio::test]
async fn rollup_sub_all_and_denied() {
    let (_srv, client) = start_js().await;
    let js = js(&client);
    js.create_stream(StreamConfig {
        name: "RU".into(),
        subjects: vec!["ru.>".into()],
        allow_rollup: true,
        ..Default::default()
    })
    .await
    .expect("create RU");
    for (subj, data) in [("ru.a", "a1"), ("ru.a", "a2"), ("ru.b", "b1")] {
        js.publish(subj, Bytes::from(data))
            .await
            .expect("publish ack future")
            .await
            .expect("publish");
    }
    // Rollup sub: prior ru.a messages go away, the new one stays,
    // and unrelated subjects are untouched (no renumbering).
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Rollup", "sub");
    js.publish_with_headers("ru.a", headers, Bytes::from("a3"))
        .await
        .expect("publish ack future")
        .await
        .expect("rollup sub publish");
    let info = stream_info(&client, "RU").await;
    assert_eq!(info["state"]["messages"], 2);
    assert_eq!(info["state"]["last_seq"], 4);
    let stream = js.get_stream("RU").await.expect("RU stream");
    assert!(stream.get_raw_message(1).await.is_err(), "seq 1 rolled up");
    assert!(stream.get_raw_message(2).await.is_err(), "seq 2 rolled up");
    let msg = stream.get_raw_message(3).await.expect("seq 3 survives");
    assert_eq!(msg.payload.as_ref(), b"b1");

    // Rollup all: everything prior goes away, the new message is
    // the stream's only message at the next sequence.
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Rollup", "all");
    js.publish_with_headers("ru.c", headers, Bytes::from("c-all"))
        .await
        .expect("publish ack future")
        .await
        .expect("rollup all publish");
    let info = stream_info(&client, "RU").await;
    assert_eq!(info["state"]["messages"], 1);
    assert_eq!(info["state"]["first_seq"], 5);
    assert_eq!(info["state"]["last_seq"], 5);

    // Rollup on a stream without allow_rollup fails the publish
    // (Go: 500/10111 in the ack), and the message is not stored.
    js.create_stream(StreamConfig {
        name: "NR".into(),
        subjects: vec!["nr.>".into()],
        ..Default::default()
    })
    .await
    .expect("create NR");
    js.publish("nr.a", Bytes::from("n1"))
        .await
        .expect("publish ack future")
        .await
        .expect("publish nr.a");
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Rollup", "sub");
    let err = js
        .publish_with_headers("nr.a", headers, Bytes::from("n2"))
        .await
        .expect("publish ack future")
        .await
        .expect_err("rollup denied publish must fail");
    assert!(
        err.to_string().contains("10111") || err.to_string().contains("rollup"),
        "unexpected rollup error: {err}"
    );
    let info = stream_info(&client, "NR").await;
    assert_eq!(info["state"]["messages"], 1);

    // A bogus rollup value fails the same way.
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Rollup", "bogus");
    let err = js
        .publish_with_headers("ru.c", headers, Bytes::from("x"))
        .await
        .expect("publish ack future")
        .await
        .expect_err("bogus rollup publish must fail");
    assert!(
        err.to_string().contains("rollup"),
        "unexpected bogus rollup error: {err}"
    );
}
