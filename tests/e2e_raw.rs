//! End-to-end tests over raw TCP using the literal NATS wire protocol.

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
        ..Default::default()
    }
}

struct RawClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
    pub info: String,
}

impl RawClient {
    async fn connect(addr: SocketAddr, connect_json: &str) -> RawClient {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read, mut writer) = stream.into_split();
        let mut reader = BufReader::new(read);
        let mut info = String::new();
        timeout(T, reader.read_line(&mut info)).await.unwrap().unwrap();
        assert!(info.starts_with("INFO "), "unexpected first line: {info}");
        writer
            .write_all(format!("CONNECT {connect_json}\r\nPING\r\n").as_bytes())
            .await
            .unwrap();
        let mut c = RawClient {
            reader,
            writer,
            info,
        };
        assert_eq!(c.read_line().await, "PONG");
        c
    }

    async fn send(&mut self, s: &str) {
        self.writer.write_all(s.as_bytes()).await.unwrap();
    }

    /// Send bytes and synchronize: the server processes ops from one
    /// connection in order, so a PONG guarantees prior ops were applied.
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

    /// Read one MSG/HMSG delivery: (op line, payload bytes without the
    /// header block for HMSG).
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

    async fn expect_silence(&mut self) {
        let mut line = String::new();
        let r = timeout(Duration::from_millis(300), self.reader.read_line(&mut line)).await;
        assert!(r.is_err(), "expected no data, got: {line}");
    }
}

#[tokio::test]
async fn info_advertises_core_features() {
    let srv = start_server(test_config()).await;
    let c = RawClient::connect(srv.addr, "{}").await;
    assert!(c.info.contains("\"headers\":true"));
    assert!(c.info.contains("\"proto\":1"));
    assert!(c.info.contains("\"max_payload\":1048576"));
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn pub_sub_between_two_clients() {
    let srv = start_server(test_config()).await;
    let mut sub = RawClient::connect(srv.addr, "{\"name\":\"sub\"}").await;
    let mut pubc = RawClient::connect(srv.addr, "{\"name\":\"pub\"}").await;
    sub.send_sync("SUB foo 1\r\n").await;
    pubc.send("PUB foo 5\r\nhello\r\n").await;
    let (line, payload) = sub.read_msg().await;
    assert_eq!(line, "MSG foo 1 5");
    assert_eq!(payload, b"hello");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn request_reply_via_inbox() {
    let srv = start_server(test_config()).await;
    let mut responder = RawClient::connect(srv.addr, "{}").await;
    let mut requester = RawClient::connect(srv.addr, "{}").await;
    responder.send_sync("SUB svc.echo 1\r\n").await;
    requester
        .send("PUB svc.echo _INBOX.abc 4\r\nping\r\n")
        .await;
    let (line, payload) = responder.read_msg().await;
    assert_eq!(line, "MSG svc.echo 1 _INBOX.abc 4");
    assert_eq!(payload, b"ping");
    responder.send("PUB _INBOX.abc 4\r\npong\r\n").await;
    // Requester has no subscription on the inbox in this raw test — subscribe first in a second round.
    drop(requester);
    let mut requester2 = RawClient::connect(srv.addr, "{}").await;
    requester2.send_sync("SUB _INBOX.abc 7\r\n").await;
    requester2
        .send("PUB svc.echo _INBOX.abc 4\r\nping\r\n")
        .await;
    let (_, payload) = responder.read_msg().await;
    assert_eq!(payload, b"ping");
    responder.send("PUB _INBOX.abc 4\r\npong\r\n").await;
    let (line, payload) = requester2.read_msg().await;
    assert_eq!(line, "MSG _INBOX.abc 7 4");
    assert_eq!(payload, b"pong");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn wildcards_and_unsub() {
    let srv = start_server(test_config()).await;
    let mut sub = RawClient::connect(srv.addr, "{}").await;
    let mut pubc = RawClient::connect(srv.addr, "{}").await;
    sub.send_sync("SUB events.* 1\r\nSUB events.> 2\r\n").await;
    pubc.send("PUB events.user 1\r\na\r\n").await;
    // Both patterns match events.user.
    let mut sids = Vec::new();
    for _ in 0..2 {
        let (line, _) = sub.read_msg().await;
        sids.push(line.split_whitespace().nth(2).unwrap().to_string());
    }
    sids.sort();
    assert_eq!(sids, vec!["1".to_string(), "2".to_string()]);
    // events.user.created matches only ">".
    pubc.send("PUB events.user.created 1\r\nb\r\n").await;
    let (line, _) = sub.read_msg().await;
    assert!(line.starts_with("MSG events.user.created 2 "), "{line}");
    // UNSUB the ">" sub; further deep events deliver nothing.
    sub.send_sync("UNSUB 2\r\n").await;
    pubc.send("PUB events.user.created 1\r\nc\r\n").await;
    sub.expect_silence().await;
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn queue_group_delivery() {
    let srv = start_server(test_config()).await;
    let mut w1 = RawClient::connect(srv.addr, "{}").await;
    let mut w2 = RawClient::connect(srv.addr, "{}").await;
    let mut pubc = RawClient::connect(srv.addr, "{}").await;
    w1.send_sync("SUB jobs workers 1\r\n").await;
    w2.send_sync("SUB jobs workers 1\r\n").await;
    for _ in 0..10 {
        pubc.send("PUB jobs 1\r\nx\r\n").await;
    }
    pubc.send_sync("").await; // flush all publishes through the server
    // Collect via dedicated tasks (read_msg is not cancel-safe, so no
    // select-ing over two clients): exactly 10 queue deliveries in total,
    // split across both workers.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u8>(32);
    for (idx, mut w) in [w1, w2].into_iter().enumerate() {
        let tx = tx.clone();
        tokio::spawn(async move {
            loop {
                let (line, _) = w.read_msg().await;
                assert!(line.starts_with("MSG jobs 1"), "{line}");
                if tx.send(idx as u8).await.is_err() {
                    break;
                }
            }
        });
    }
    drop(tx);
    let mut counts = [0usize; 2];
    for _ in 0..10 {
        let idx = timeout(T, rx.recv())
            .await
            .expect("timed out waiting for queue deliveries")
            .expect("collector channel closed");
        counts[idx as usize] += 1;
    }
    assert_eq!(counts[0] + counts[1], 10);
    assert!(counts[0] > 0 && counts[1] > 0, "load should spread: {counts:?}");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn headers_hmsg_for_capable_and_stripped_for_others() {
    let srv = start_server(test_config()).await;
    let mut capable = RawClient::connect(srv.addr, "{\"headers\":true}").await;
    let mut plain = RawClient::connect(srv.addr, "{\"headers\":false}").await;
    let mut pubc = RawClient::connect(srv.addr, "{\"headers\":true}").await;
    capable.send_sync("SUB h 1\r\n").await;
    plain.send_sync("SUB h 1\r\n").await;
    let hdrs = b"NATS/1.0\r\nX-One: 1\r\n\r\n";
    let mut wire = format!("HPUB h {} {}\r\n", hdrs.len(), hdrs.len() + 4).into_bytes();
    wire.extend_from_slice(hdrs);
    wire.extend_from_slice(b"data\r\n");
    pubc.writer.write_all(&wire).await.unwrap();

    let (line, payload) = capable.read_msg().await;
    assert!(line.starts_with("HMSG h 1 "), "{line}");
    assert_eq!(payload, b"data");

    let (line, payload) = plain.read_msg().await;
    assert_eq!(line, "MSG h 1 4");
    assert_eq!(payload, b"data");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn verbose_mode_sends_ok() {
    let srv = start_server(test_config()).await;
    let stream = TcpStream::connect(srv.addr).await.unwrap();
    let (read, mut writer) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap(); // INFO
    writer
        .write_all(b"CONNECT {\"verbose\":true}\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line.trim_end(), "+OK");
    writer.write_all(b"SUB foo 1\r\n").await.unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line.trim_end(), "+OK");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn auth_user_pass_enforced() {
    let mut cfg = test_config();
    cfg.user = Some("derek".into());
    cfg.pass = Some("s3cret".into());
    let srv = start_server(cfg).await;

    // Wrong password: -ERR and connection closed.
    let stream = TcpStream::connect(srv.addr).await.unwrap();
    let (read, mut writer) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap(); // INFO
    writer
        .write_all(b"CONNECT {\"user\":\"derek\",\"pass\":\"wrong\"}\r\n")
        .await
        .unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert!(
        line.contains("Authorization Violation"),
        "expected auth error, got: {line}"
    );

    // Correct credentials work.
    let mut good = RawClient::connect(
        srv.addr,
        "{\"user\":\"derek\",\"pass\":\"s3cret\"}",
    )
    .await;
    good.send_sync("SUB ok 1\r\n").await;
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn max_payload_violation_errors() {
    let mut cfg = test_config();
    cfg.max_payload = 8;
    let srv = start_server(cfg).await;
    let mut c = RawClient::connect(srv.addr, "{}").await;
    c.send("PUB big 9\r\n123456789\r\n").await;
    let line = c.read_line().await;
    assert!(
        line.contains("Maximum Payload Violation"),
        "expected payload error, got: {line}"
    );
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn auto_unsubscribe_via_unsub_max() {
    let srv = start_server(test_config()).await;
    let mut sub = RawClient::connect(srv.addr, "{}").await;
    let mut pubc = RawClient::connect(srv.addr, "{}").await;
    sub.send_sync("SUB foo 1\r\nUNSUB 1 2\r\n").await;
    for _ in 0..3 {
        pubc.send("PUB foo 1\r\nx\r\n").await;
    }
    pubc.send_sync("").await;
    // Exactly 2 deliveries, then silence.
    sub.read_msg().await;
    sub.read_msg().await;
    sub.expect_silence().await;
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn protocol_error_closes_connection() {
    let srv = start_server(test_config()).await;
    let mut c = RawClient::connect(srv.addr, "{}").await;
    c.send("BOGUS\r\n").await;
    let line = c.read_line().await;
    assert!(line.starts_with("-ERR"), "expected -ERR, got: {line}");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn many_concurrent_clients_fanout() {
    let srv = start_server(test_config()).await;
    let mut subs = Vec::new();
    for _ in 0..8 {
        let mut c = RawClient::connect(srv.addr, "{}").await;
        c.send_sync("SUB broadcast 1\r\n").await;
        subs.push(c);
    }
    let mut pubc = RawClient::connect(srv.addr, "{}").await;
    pubc.send("PUB broadcast 3\r\nhey\r\n").await;
    for s in &mut subs {
        let (_, payload) = s.read_msg().await;
        assert_eq!(payload, b"hey");
    }
    srv.shutdown().await.unwrap();
}
