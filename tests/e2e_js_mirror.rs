//! JetStream mirroring/sourcing (same node) tests: backfill, live
//! copy, multi-origin sources, filter_subject, start_seq, rejection
//! of nonexistent origins, origin-delete semantics, and MSG.GET /
//! consumer reads on mirror streams. Go v2.15.1 semantics were probed
//! first (see PLAN Phase 20); one deliberate deviation: Go accepts a
//! mirror/source of a nonexistent stream and leaves it empty, while
//! this implementation rejects it with 404/10059.

use std::time::Duration;

use async_nats::jetstream::{self, consumer::pull::Config as PullConfig, stream::Source};
use bytes::Bytes;
use futures::StreamExt;
use rust_natsd::config::Config;
use rust_natsd::server;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);

fn tmpdir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rust-natsd-js-{name}-{}", std::process::id()));
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

async fn wait_msgs(stream: &mut jetstream::stream::Stream, want: u64) -> jetstream::stream::Info {
    for _ in 0..50 {
        let info = stream.info().await.expect("info");
        if info.state.messages == want {
            return info.clone();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "stream {} did not reach {want} messages",
        stream.info().await.expect("info").config.name
    );
}

#[tokio::test]
async fn mirror_backfill_and_live_copy() {
    let srv = server::start(js_config(None)).await.expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    let _origin = js
        .create_stream(jetstream::stream::Config {
            name: "ORIG".into(),
            subjects: vec!["orig.>".into()],
            ..Default::default()
        })
        .await
        .expect("create origin");
    publish(&js, "orig.a", "o1").await;
    publish(&js, "orig.b", "o2").await;
    publish(&js, "orig.a", "o3").await;

    // Backfill: the mirror holds the origin's existing messages at
    // creation (Go backfills asynchronously; this implementation
    // backfills synchronously, same final state).
    let mut mirror = js
        .create_stream(jetstream::stream::Config {
            name: "MIRROR".into(),
            mirror: Some(Source {
                name: "ORIG".into(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect("create mirror");
    wait_msgs(&mut mirror, 3).await;
    // Mirror copies preserve the origin subject and get fresh mirror
    // sequences; mirror copies carry no added headers (Go behaviour).
    for (seq, subject, data) in [
        (1, "orig.a", "o1"),
        (2, "orig.b", "o2"),
        (3, "orig.a", "o3"),
    ] {
        let msg = mirror
            .get_raw_message(seq)
            .await
            .expect("MSG.GET on mirror");
        assert_eq!(msg.subject.as_str(), subject);
        assert_eq!(msg.payload.as_ref(), data.as_bytes());
        assert!(
            msg.headers.get("Nats-Stream-Source").is_none(),
            "mirror copies carry no source header"
        );
    }

    // Live copy: a new origin publish lands in the mirror.
    publish(&js, "orig.b", "o4").await;
    wait_msgs(&mut mirror, 4).await;
    let msg = mirror.get_raw_message(4).await.expect("live copy");
    assert_eq!(msg.subject.as_str(), "orig.b");
    assert_eq!(msg.payload.as_ref(), b"o4");

    // A mirror stream is a normal stream: consumers work on it.
    let consumer = mirror
        .create_consumer(PullConfig {
            durable_name: Some("c-mirror".into()),
            ..Default::default()
        })
        .await
        .expect("consumer on mirror");
    let mut messages = consumer.messages().await.expect("messages stream");
    for want in ["o1", "o2", "o3", "o4"] {
        let msg = timeout(T, messages.next())
            .await
            .expect("timeout")
            .expect("message")
            .expect("ok");
        assert_eq!(msg.payload.as_ref(), want.as_bytes());
    }
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn source_two_origins_filter_and_start_seq() {
    let srv = server::start(js_config(None)).await.expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    for (name, subject) in [("S1", "s1.>"), ("S2", "s2.>")] {
        js.create_stream(jetstream::stream::Config {
            name: name.into(),
            subjects: vec![subject.into()],
            ..Default::default()
        })
        .await
        .expect("create origin");
    }
    publish(&js, "s1.x", "x1").await;
    publish(&js, "s2.y", "y1").await;
    publish(&js, "s1.x", "x2").await;

    // Two origins, backfilled in config order (S1's messages first).
    let mut src = js
        .create_stream(jetstream::stream::Config {
            name: "SRC2".into(),
            sources: Some(vec![
                Source {
                    name: "S1".into(),
                    ..Default::default()
                },
                Source {
                    name: "S2".into(),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        })
        .await
        .expect("create source");
    wait_msgs(&mut src, 3).await;
    for (seq, subject, data) in [(1, "s1.x", "x1"), (2, "s1.x", "x2"), (3, "s2.y", "y1")] {
        let msg = src.get_raw_message(seq).await.expect("MSG.GET on source");
        assert_eq!(msg.subject.as_str(), subject);
        assert_eq!(msg.payload.as_ref(), data.as_bytes());
    }
    // Source copies carry the origin metadata header (Go shape).
    let msg = src.get_raw_message(1).await.expect("source header");
    let header = msg
        .headers
        .get("Nats-Stream-Source")
        .expect("Nats-Stream-Source header")
        .as_str();
    assert!(
        header.starts_with("S1 1 "),
        "source header starts with origin + origin seq, got {header}"
    );

    // Live copy from the second origin, too.
    publish(&js, "s2.y", "y2").await;
    wait_msgs(&mut src, 4).await;
    let msg = src.get_raw_message(4).await.expect("live source copy");
    assert_eq!(msg.subject.as_str(), "s2.y");
    assert_eq!(msg.payload.as_ref(), b"y2");

    // Filter: only orig.a-equivalent messages are copied, backfill
    // and live alike.
    js.create_stream(jetstream::stream::Config {
        name: "ORIGF".into(),
        subjects: vec!["of.>".into()],
        ..Default::default()
    })
    .await
    .expect("create filter origin");
    publish(&js, "of.a", "fa1").await;
    publish(&js, "of.b", "fb1").await;
    publish(&js, "of.a", "fa2").await;
    let mut srcf = js
        .create_stream(jetstream::stream::Config {
            name: "SRCF".into(),
            sources: Some(vec![Source {
                name: "ORIGF".into(),
                filter_subject: Some("of.a".into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("create filtered source");
    wait_msgs(&mut srcf, 2).await;
    publish(&js, "of.b", "fb2").await;
    publish(&js, "of.a", "fa3").await;
    wait_msgs(&mut srcf, 3).await;
    for (seq, data) in [(1, "fa1"), (2, "fa2"), (3, "fa3")] {
        let msg = srcf.get_raw_message(seq).await.expect("filtered copy");
        assert_eq!(msg.payload.as_ref(), data.as_bytes());
    }

    // Start sequence: backfill starts at the given origin sequence.
    let mut srcs = js
        .create_stream(jetstream::stream::Config {
            name: "SRCS".into(),
            sources: Some(vec![Source {
                name: "ORIGF".into(),
                start_sequence: Some(3),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("create start_seq source");
    // ORIGF sequences: 1=fa1, 2=fb1, 3=fa2, 4=fb2, 5=fa3.
    wait_msgs(&mut srcs, 3).await;
    for (seq, data) in [(1, "fa2"), (2, "fb2"), (3, "fa3")] {
        let msg = srcs.get_raw_message(seq).await.expect("start_seq copy");
        assert_eq!(msg.payload.as_ref(), data.as_bytes());
    }
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn mirror_source_of_nonexistent_stream_rejected() {
    let srv = server::start(js_config(None)).await.expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    // Deliberate deviation from Go (which accepts and leaves the
    // stream empty): a typo must not silently create an empty stream.
    let err = js
        .create_stream(jetstream::stream::Config {
            name: "BADM".into(),
            mirror: Some(Source {
                name: "NOPE".into(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect_err("mirror of nonexistent stream must be rejected");
    assert!(
        format!("{err:?}").contains("not found"),
        "error names the missing stream: {err:?}"
    );
    let err = js
        .create_stream(jetstream::stream::Config {
            name: "BADS".into(),
            sources: Some(vec![Source {
                name: "NOPE".into(),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect_err("source of nonexistent stream must be rejected");
    assert!(
        format!("{err:?}").contains("not found"),
        "error names the missing stream: {err:?}"
    );
    // And the rejected streams were not created.
    assert!(js.get_stream("BADM").await.is_err());
    assert!(js.get_stream("BADS").await.is_err());
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn mirror_config_validation_rejected() {
    let srv = server::start(js_config(None)).await.expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    js.create_stream(jetstream::stream::Config {
        name: "ORIG".into(),
        subjects: vec!["orig.>".into()],
        ..Default::default()
    })
    .await
    .expect("create origin");
    // Mirror with subjects (Go: "stream mirrors can not contain subjects").
    assert!(js
        .create_stream(jetstream::stream::Config {
            name: "MWS".into(),
            subjects: vec!["orig.>".into()],
            mirror: Some(Source {
                name: "ORIG".into(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .is_err());
    // Mirror with sources (Go: "stream mirrors can not also contain
    // other sources").
    assert!(js
        .create_stream(jetstream::stream::Config {
            name: "MBOTH".into(),
            mirror: Some(Source {
                name: "ORIG".into(),
                ..Default::default()
            }),
            sources: Some(vec![Source {
                name: "ORIG".into(),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .is_err());
    // Self-mirror is a cycle (Go: "detected cycle").
    assert!(js
        .create_stream(jetstream::stream::Config {
            name: "SELF".into(),
            mirror: Some(Source {
                name: "SELF".into(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .is_err());
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn mirror_keeps_copies_after_origin_delete_no_resume() {
    let srv = server::start(js_config(None)).await.expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    js.create_stream(jetstream::stream::Config {
        name: "ORIG".into(),
        subjects: vec!["orig.>".into()],
        ..Default::default()
    })
    .await
    .expect("create origin");
    publish(&js, "orig.a", "k1").await;
    publish(&js, "orig.a", "k2").await;
    let mut mirror = js
        .create_stream(jetstream::stream::Config {
            name: "MIRROR".into(),
            mirror: Some(Source {
                name: "ORIG".into(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect("create mirror");
    wait_msgs(&mut mirror, 2).await;

    // Deleting the origin does not corrupt the mirror: copies stay
    // readable, and the link is dead.
    js.delete_stream("ORIG").await.expect("delete origin");
    wait_msgs(&mut mirror, 2).await;
    let msg = mirror
        .get_raw_message(2)
        .await
        .expect("copy survives delete");
    assert_eq!(msg.payload.as_ref(), b"k2");

    // Recreating a same-named origin does NOT revive the link (Go).
    js.create_stream(jetstream::stream::Config {
        name: "ORIG".into(),
        subjects: vec!["orig.>".into()],
        ..Default::default()
    })
    .await
    .expect("recreate origin");
    publish(&js, "orig.a", "k3").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let info = mirror.info().await.expect("mirror info");
    assert_eq!(info.state.messages, 2, "no resume after recreate");
    // Purging the (new) origin also leaves the mirror untouched.
    let origin = js.get_stream("ORIG").await.expect("origin stream");
    origin.purge().await.expect("purge origin");
    let info = mirror.info().await.expect("mirror info after purge");
    assert_eq!(info.state.messages, 2);
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn mirror_survives_restart_and_resumes_live_copy() {
    let dir = tmpdir("mirror-restart");
    let srv = server::start(js_config(Some(dir.clone())))
        .await
        .expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    js.create_stream(jetstream::stream::Config {
        name: "ORIG".into(),
        subjects: vec!["orig.>".into()],
        ..Default::default()
    })
    .await
    .expect("create origin");
    publish(&js, "orig.a", "r1").await;
    let mut mirror = js
        .create_stream(jetstream::stream::Config {
            name: "MIRROR".into(),
            mirror: Some(Source {
                name: "ORIG".into(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect("create mirror");
    wait_msgs(&mut mirror, 1).await;
    drop(client);
    srv.shutdown().await.expect("shutdown");

    let srv = server::start(js_config(Some(dir.clone())))
        .await
        .expect("server restarts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    let js = jetstream::new(client.clone());
    let mut mirror = js.get_stream("MIRROR").await.expect("mirror survives");
    wait_msgs(&mut mirror, 1).await;
    let msg = mirror
        .get_raw_message(1)
        .await
        .expect("copy survives restart");
    assert_eq!(msg.payload.as_ref(), b"r1");
    // The mirror config survives, and live copy resumes.
    let info = mirror.info().await.expect("mirror info");
    assert_eq!(
        info.config.mirror.as_ref().map(|m| m.name.as_str()),
        Some("ORIG")
    );
    publish(&js, "orig.a", "r2").await;
    wait_msgs(&mut mirror, 2).await;
    let msg = mirror
        .get_raw_message(2)
        .await
        .expect("live copy after restart");
    assert_eq!(msg.payload.as_ref(), b"r2");
    srv.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}
