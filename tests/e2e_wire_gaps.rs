//! Raw-wire e2e tests for behaviour previously covered only by the
//! parity probes (parity/probes.py), not by any cargo test: pedantic
//! publish, verbose ordering, duplicate sid, echo:false, no-responders,
//! server PING/stale, token-auth failure modes, malformed CONNECT,
//! unknown-op text, second CONNECT, queue exactly-once with three
//! members, HPUB max-payload, and graceful shutdown.
//!
//! RawClient is a local copy of the tests/e2e_raw.rs helper (same
//! conventions: 5s deadlines on every read, PONG synchronisation).

use std::net::SocketAddr;
use std::time::Duration;

use rust_natsd::config::Config;
use rust_natsd::server::{self, RunningServer};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(5);

async fn start_server(config: Config) -> RunningServer {
    server::start(config).await.expect("server starts")
}

fn test_config() -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        ..Default::default()
    }
}

struct RawClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl RawClient {
    async fn connect(addr: SocketAddr, connect_json: &str) -> RawClient {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read, mut writer) = stream.into_split();
        let mut reader = BufReader::new(read);
        let mut info = String::new();
        timeout(T, reader.read_line(&mut info))
            .await
            .unwrap()
            .unwrap();
        assert!(info.starts_with("INFO "), "unexpected first line: {info}");
        writer
            .write_all(format!("CONNECT {connect_json}\r\nPING\r\n").as_bytes())
            .await
            .unwrap();
        let mut c = RawClient { reader, writer };
        assert_eq!(c.read_line().await, "PONG");
        c
    }

    async fn send(&mut self, s: &str) {
        self.writer.write_all(s.as_bytes()).await.unwrap();
    }

    async fn send_sync(&mut self, s: &str) {
        self.send(&format!("{s}PING\r\n")).await;
        assert_eq!(self.read_line().await, "PONG");
    }

    async fn read_line(&mut self) -> String {
        let mut line = String::new();
        let n = timeout(T, self.reader.read_line(&mut line))
            .await
            .expect("read_line timed out")
            .expect("read_line io error");
        assert!(n > 0, "connection closed while expecting a line");
        line.trim_end_matches(['\r', '\n']).to_string()
    }

    async fn read_msg(&mut self) -> (String, Vec<u8>) {
        let line = self.read_line().await;
        let parts: Vec<&str> = line.split_whitespace().collect();
        let (total, hdr_len) = match parts[0] {
            "MSG" => (parts[parts.len() - 1].parse::<usize>().unwrap(), 0),
            "HMSG" => (
                parts[parts.len() - 1].parse::<usize>().unwrap(),
                parts[parts.len() - 2].parse::<usize>().unwrap(),
            ),
            other => panic!("expected MSG/HMSG, got line: {line} ({other})"),
        };
        let mut raw = vec![0u8; total + 2];
        timeout(T, self.reader.read_exact(&mut raw))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&raw[total..], b"\r\n");
        (line, raw[hdr_len..total].to_vec())
    }

    /// Read one HMSG and return its raw header block as a string.
    async fn read_hmsg_headers(&mut self) -> String {
        let line = self.read_line().await;
        let parts: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(parts[0], "HMSG", "expected HMSG, got: {line}");
        let total: usize = parts[parts.len() - 1].parse().unwrap();
        let hdr_len: usize = parts[parts.len() - 2].parse().unwrap();
        let mut raw = vec![0u8; total + 2];
        timeout(T, self.reader.read_exact(&mut raw))
            .await
            .unwrap()
            .unwrap();
        String::from_utf8_lossy(&raw[..hdr_len]).to_string()
    }

    async fn expect_silence(&mut self) {
        let mut line = String::new();
        let r = timeout(Duration::from_millis(300), self.reader.read_line(&mut line)).await;
        assert!(r.is_err(), "expected no data, got: {line}");
    }
}

#[tokio::test]
async fn pedantic_publish_errors_but_still_delivers() {
    let srv = start_server(test_config()).await;
    let mut sub = RawClient::connect(srv.addr, "{\"pedantic\":true}").await;
    // Subscribe to the literal subject and to `>`; a pedantic publish
    // of foo.* is delivered literally to both.
    sub.send_sync("SUB foo.* 1\r\n").await;
    sub.send_sync("SUB > 2\r\n").await;
    let mut pubc = RawClient::connect(srv.addr, "{\"pedantic\":true}").await;
    pubc.send("PUB foo.* 1\r\nx\r\n").await;
    let err = pubc.read_line().await;
    assert_eq!(err, "-ERR 'Invalid Publish Subject'", "got: {err}");
    // Connection stays alive after the pedantic error.
    pubc.send_sync("").await;
    // Both literal subscribers receive the message.
    let (line, payload) = sub.read_msg().await;
    assert!(line.starts_with("MSG foo.* 1 "), "{line}");
    assert_eq!(payload, b"x");
    let (line, payload) = sub.read_msg().await;
    assert!(line.starts_with("MSG foo.* 2 "), "{line}");
    assert_eq!(payload, b"x");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn verbose_ok_precedes_self_delivery() {
    let srv = start_server(test_config()).await;
    // Verbose CONNECT yields +OK before the PONG, so connect manually
    // instead of via RawClient::connect (which expects PONG first).
    let stream = TcpStream::connect(srv.addr).await.unwrap();
    let (read, mut writer) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut info = String::new();
    timeout(T, reader.read_line(&mut info))
        .await
        .unwrap()
        .unwrap();
    writer
        .write_all(b"CONNECT {\"verbose\":true}\r\nPING\r\n")
        .await
        .unwrap();
    let mut c = RawClient { reader, writer };
    assert_eq!(c.read_line().await, "+OK");
    assert_eq!(c.read_line().await, "PONG");
    // Verbose acks SUB too, so synchronise manually (+OK, then PONG).
    c.send("SUB self 1\r\nPING\r\n").await;
    assert_eq!(c.read_line().await, "+OK");
    assert_eq!(c.read_line().await, "PONG");
    c.send("PUB self 2\r\nhi\r\n").await;
    // Go order: +OK first, then the self-delivery.
    assert_eq!(c.read_line().await, "+OK");
    let (line, payload) = c.read_msg().await;
    assert!(line.starts_with("MSG self 1 "), "{line}");
    assert_eq!(payload, b"hi");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn duplicate_sid_keeps_first_subscription() {
    let srv = start_server(test_config()).await;
    let mut sub = RawClient::connect(srv.addr, "{}").await;
    sub.send_sync("SUB foo 1\r\n").await;
    sub.send_sync("SUB bar 1\r\n").await; // same sid: ignored, not replaced
    let mut pubc = RawClient::connect(srv.addr, "{}").await;
    pubc.send("PUB foo 1\r\nx\r\n").await;
    let (line, _) = sub.read_msg().await;
    assert!(line.starts_with("MSG foo 1 "), "{line}");
    pubc.send("PUB bar 1\r\nx\r\n").await;
    sub.expect_silence().await;
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn echo_false_suppresses_only_self_delivery() {
    let srv = start_server(test_config()).await;
    let mut quiet = RawClient::connect(srv.addr, "{\"echo\":false}").await;
    let mut other = RawClient::connect(srv.addr, "{}").await;
    quiet.send_sync("SUB echo.me 1\r\n").await;
    other.send_sync("SUB echo.me 2\r\n").await;
    quiet.send("PUB echo.me 2\r\nhi\r\n").await;
    quiet.expect_silence().await;
    let (line, payload) = other.read_msg().await;
    assert!(line.starts_with("MSG echo.me 2 "), "{line}");
    assert_eq!(payload, b"hi");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn no_responders_true_sends_503_and_false_sends_nothing() {
    let srv = start_server(test_config()).await;
    // headers:true so the 503 arrives as HMSG, not stripped to MSG.
    let mut c = RawClient::connect(srv.addr, "{\"headers\":true,\"no_responders\":true}").await;
    c.send_sync("SUB _INBOX.test 1\r\n").await;
    c.send("PUB req.nowhere _INBOX.test 4\r\nping\r\n").await;
    let headers = c.read_hmsg_headers().await;
    assert!(headers.starts_with("NATS/1.0 503"), "headers: {headers:?}");
    assert!(
        headers.contains("Nats-Subject: req.nowhere"),
        "headers: {headers:?}"
    );
    // Without headers, the same 503 is stripped to an empty MSG.
    let mut plain = RawClient::connect(srv.addr, "{\"no_responders\":true}").await;
    plain.send_sync("SUB _INBOX.plain 1\r\n").await;
    plain
        .send("PUB req.nowhere _INBOX.plain 4\r\nping\r\n")
        .await;
    let (line, payload) = plain.read_msg().await;
    assert!(line.starts_with("MSG _INBOX.plain 1 "), "{line}");
    assert_eq!(payload, b"");

    let mut c2 = RawClient::connect(srv.addr, "{\"no_responders\":false}").await;
    c2.send_sync("SUB _INBOX.test2 1\r\n").await;
    c2.send("PUB req.nowhere _INBOX.test2 4\r\nping\r\n").await;
    c2.expect_silence().await;
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn server_ping_stale_connection_closes() {
    let mut cfg = test_config();
    cfg.ping_interval = Duration::from_millis(100);
    cfg.ping_max = 2;
    let srv = start_server(cfg).await;
    // Raw socket, never answering PING.
    let stream = TcpStream::connect(srv.addr).await.unwrap();
    let (read, mut writer) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut line = String::new();
    timeout(T, reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    assert!(line.starts_with("INFO "));
    writer.write_all(b"CONNECT {}\r\n").await.unwrap();
    let mut saw_ping = false;
    let mut saw_err = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        let mut l = String::new();
        let n = timeout(
            deadline - tokio::time::Instant::now(),
            reader.read_line(&mut l),
        )
        .await
        .expect("stale close timed out")
        .expect("io error");
        if n == 0 {
            break; // EOF: connection closed.
        }
        let l = l.trim_end_matches(['\r', '\n']).to_string();
        if l == "PING" {
            saw_ping = true;
        }
        if l.starts_with("-ERR 'Stale Connection'") {
            saw_err = true;
        }
    }
    assert!(saw_ping, "server never sent PING");
    assert!(saw_err, "no Stale Connection -ERR before close");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn token_auth_modes() {
    let mut cfg = test_config();
    cfg.auth_token = Some("tok123".into());
    let srv = start_server(cfg).await;
    // Correct token via auth_token field.
    let _ok = RawClient::connect(srv.addr, "{\"auth_token\":\"tok123\"}").await;
    // Token presented as pass is rejected (Go behaviour).
    for bad in [
        "{\"pass\":\"tok123\"}",
        "{\"auth_token\":\"wrong\"}",
        "{}",
        "{\"user\":\"nobody\"}",
    ] {
        let stream = TcpStream::connect(srv.addr).await.unwrap();
        let (read, mut writer) = stream.into_split();
        let mut reader = BufReader::new(read);
        let mut info = String::new();
        timeout(T, reader.read_line(&mut info))
            .await
            .unwrap()
            .unwrap();
        writer
            .write_all(format!("CONNECT {bad}\r\n").as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        let n = timeout(T, reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert!(n > 0, "expected -ERR for {bad}, got EOF");
        assert!(
            line.starts_with("-ERR 'Authorization Violation'"),
            "for {bad}: {line}"
        );
        // Connection is closed after the auth error.
        let mut tail = String::new();
        let n = timeout(T, reader.read_line(&mut tail))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, 0, "expected EOF after auth -ERR for {bad}");
    }
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_connect_closes_silently() {
    let srv = start_server(test_config()).await;
    let stream = TcpStream::connect(srv.addr).await.unwrap();
    let (read, mut writer) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut info = String::new();
    timeout(T, reader.read_line(&mut info))
        .await
        .unwrap()
        .unwrap();
    writer.write_all(b"CONNECT notjson\r\n").await.unwrap();
    // No -ERR is sent; the connection just closes.
    let mut line = String::new();
    let n = timeout(T, reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 0, "expected silent close, got: {line:?}");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_op_exact_text_and_close() {
    let srv = start_server(test_config()).await;
    let mut c = RawClient::connect(srv.addr, "{}").await;
    c.send("FOO bar\r\n").await;
    assert_eq!(c.read_line().await, "-ERR 'Unknown Protocol Operation'");
    // Server closes the connection after the protocol error.
    let mut tail = String::new();
    let n = timeout(T, c.reader.read_line(&mut tail))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 0, "expected EOF after unknown op");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn second_connect_reauthenticates() {
    let srv = start_server(test_config()).await;
    let mut c = RawClient::connect(srv.addr, "{}").await;
    c.send("CONNECT {\"verbose\":true}\r\nPING\r\n").await;
    // A second CONNECT is processed as a fresh auth (verbose +OK),
    // then the PING is answered; no error.
    assert_eq!(c.read_line().await, "+OK");
    assert_eq!(c.read_line().await, "PONG");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn queue_exactly_once_three_members() {
    let srv = start_server(test_config()).await;
    let mut workers = Vec::new();
    for _ in 0..3 {
        let mut w = RawClient::connect(srv.addr, "{}").await;
        w.send_sync("SUB jobs workers 1\r\n").await;
        workers.push(w);
    }
    let mut pubc = RawClient::connect(srv.addr, "{}").await;
    for _ in 0..30 {
        pubc.send("PUB jobs 1\r\nx\r\n").await;
    }
    pubc.send_sync("").await;
    // Collect via dedicated tasks (read_msg is not cancel-safe).
    let (tx, mut rx) = tokio::sync::mpsc::channel::<usize>(64);
    for (idx, mut w) in workers.into_iter().enumerate() {
        let tx = tx.clone();
        tokio::spawn(async move {
            loop {
                let (line, _) = w.read_msg().await;
                assert!(line.starts_with("MSG jobs 1"), "{line}");
                if tx.send(idx).await.is_err() {
                    break;
                }
            }
        });
    }
    drop(tx);
    let mut counts = [0usize; 3];
    for _ in 0..30 {
        let idx = timeout(T, rx.recv()).await.unwrap().unwrap();
        counts[idx] += 1;
    }
    assert_eq!(counts.iter().sum::<usize>(), 30);
    // Every member got at least one message (rotation, not stickiness).
    assert!(counts.iter().all(|c| *c > 0), "counts: {counts:?}");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn hpub_over_max_payload_rejected() {
    let mut cfg = test_config();
    cfg.max_payload = 100;
    let srv = start_server(cfg).await;
    let mut c = RawClient::connect(srv.addr, "{}").await;
    // Headers are 12 bytes ("NATS/1.0\r\n\r\n"); payload 101 > 100.
    let hdrs = b"NATS/1.0\r\n\r\n";
    assert_eq!(hdrs.len(), 12);
    c.send("HPUB big 12 113\r\n").await;
    c.writer.write_all(hdrs).await.unwrap();
    c.writer.write_all(&[b'x'; 101]).await.unwrap();
    c.writer.write_all(b"\r\n").await.unwrap();
    assert_eq!(c.read_line().await, "-ERR 'Maximum Payload Violation'");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn graceful_shutdown_refuses_new_connections() {
    let srv = start_server(test_config()).await;
    let addr = srv.addr;
    let _c = RawClient::connect(addr, "{}").await;
    srv.shutdown().await.unwrap();
    let res = TcpStream::connect(addr).await;
    assert!(res.is_err(), "listener must be closed after shutdown");
}
