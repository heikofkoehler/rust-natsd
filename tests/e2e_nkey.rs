//! NKey integration tests: a client that signs the per-connection INFO
//! nonce with its user key authenticates; a bad signature is rejected.
//!
//! The keypair is generated in-memory per test (throwaway test key,
//! never written to disk, never a real credential).

use rust_natsd::config::{Account, AccountUser, Config};
use rust_natsd::server;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Raw {
    stream: tokio::net::TcpStream,
    buf: Vec<u8>,
}

impl Raw {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        Raw {
            stream: tokio::net::TcpStream::connect(addr).await.unwrap(),
            buf: Vec::new(),
        }
    }

    async fn read_line(&mut self) -> String {
        loop {
            if let Some(pos) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let line = self.buf.drain(..pos + 2).collect::<Vec<_>>();
                return String::from_utf8_lossy(&line[..line.len() - 2]).into_owned();
            }
            let mut chunk = [0u8; 4096];
            let n = self.stream.read(&mut chunk).await.expect("read");
            assert!(n > 0, "server closed connection");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    async fn info(&mut self) -> serde_json::Value {
        let line = self.read_line().await;
        assert!(line.starts_with("INFO "), "first line is INFO: {line}");
        serde_json::from_str(&line[5..]).expect("INFO json")
    }

    async fn send(&mut self, data: &str) {
        self.stream.write_all(data.as_bytes()).await.unwrap();
    }
}

fn nkey_config(pubkey: &str) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        accounts: vec![Account {
            name: "ACCT".into(),
            users: vec![AccountUser {
                nkey: Some(pubkey.to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn nkey_sign_nonce_authenticates_and_publishes() {
    let kp = nkeys::KeyPair::new_user();
    let pubkey = kp.public_key();
    let srv = server::start(nkey_config(&pubkey))
        .await
        .expect("server starts");
    let mut client = Raw::connect(srv.addr).await;
    let info = client.info().await;
    let nonce = info["nonce"].as_str().expect("INFO nonce").to_string();
    assert!(!nonce.is_empty(), "nonce must be non-empty");
    let sig = kp.sign(nonce.as_bytes()).unwrap();
    use base64::Engine;
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig);
    client
        .send(&format!(
            "CONNECT {{\"nkey\":\"{pubkey}\",\"sig\":\"{sig_b64}\",\"verbose\":false,\"headers\":true}}\r\n"
        ))
        .await;
    client.send("SUB nkey.topic 1\r\n").await;
    client.send("PUB nkey.topic 2\r\nhi\r\n").await;
    let line = client.read_line().await;
    assert!(line.starts_with("MSG nkey.topic 1 2"), "delivery: {line}");
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn nkey_bad_signature_is_rejected() {
    let kp = nkeys::KeyPair::new_user();
    let pubkey = kp.public_key();
    let srv = server::start(nkey_config(&pubkey))
        .await
        .expect("server starts");
    let mut client = Raw::connect(srv.addr).await;
    let info = client.info().await;
    let _nonce = info["nonce"].as_str().expect("INFO nonce").to_string();
    // Sign something other than the nonce.
    let sig = kp.sign(b"not the nonce").unwrap();
    use base64::Engine;
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig);
    client
        .send(&format!(
            "CONNECT {{\"nkey\":\"{pubkey}\",\"sig\":\"{sig_b64}\",\"verbose\":false}}\r\n"
        ))
        .await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Authorization Violation'"),
        "rejection: {line}"
    );
    srv.shutdown().await.expect("shutdown");
}
