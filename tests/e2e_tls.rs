//! TLS integration tests: a TLS-required server rejects plain clients
//! and serves TLS clients; with --tlsverify, a client certificate signed
//! by the CA is required.
//!
//! Certificates are generated at test time with the `openssl` CLI into a
//! per-test temp dir (throwaway test certs only, never real credentials).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rust_natsd::config::{Config, TlsSettings};
use rust_natsd::server;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn tmpdir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rust-natsd-tls-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn openssl(args: &[&str]) {
    let out = std::process::Command::new("openssl")
        .args(args)
        .output()
        .expect("openssl runs");
    assert!(
        out.status.success(),
        "openssl {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
}

struct Certs {
    dir: PathBuf,
    ca: PathBuf,
    server_crt: PathBuf,
    server_key: PathBuf,
    client_crt: PathBuf,
    client_key: PathBuf,
}

fn gen_certs(name: &str) -> Certs {
    let dir = tmpdir(name);
    let ca = dir.join("ca.crt");
    let ca_key = dir.join("ca.key");
    openssl(&[
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-keyout",
        ca_key.to_str().unwrap(),
        "-out",
        ca.to_str().unwrap(),
        "-days",
        "2",
        "-nodes",
        "-subj",
        "/CN=TestCA",
    ]);
    let mk = |cn: &str, stem: &str, ext_content: Option<&str>| -> (PathBuf, PathBuf) {
        let key = dir.join(format!("{stem}.key"));
        let csr = dir.join(format!("{stem}.csr"));
        let crt = dir.join(format!("{stem}.crt"));
        openssl(&[
            "req",
            "-newkey",
            "rsa:2048",
            "-keyout",
            key.to_str().unwrap(),
            "-out",
            csr.to_str().unwrap(),
            "-nodes",
            "-subj",
            &format!("/CN={cn}"),
        ]);
        let mut args: Vec<&str> = vec![
            "x509",
            "-req",
            "-in",
            csr.to_str().unwrap(),
            "-CA",
            ca.to_str().unwrap(),
            "-CAkey",
            ca_key.to_str().unwrap(),
            "-CAcreateserial",
            "-out",
            crt.to_str().unwrap(),
            "-days",
            "2",
        ];
        let ext = dir.join(format!("{stem}.ext"));
        if let Some(content) = ext_content {
            std::fs::write(&ext, content).unwrap();
            args.push("-extfile");
            args.push(ext.to_str().unwrap());
        }
        openssl(&args);
        (crt, key)
    };
    let (server_crt, server_key) = mk(
        "localhost",
        "server",
        Some("subjectAltName=DNS:localhost,IP:127.0.0.1"),
    );
    // Client cert needs an extfile too: without extensions openssl emits a
    // v1 certificate, which rustls rejects (UnsupportedCertVersion).
    let (client_crt, client_key) = mk("client", "client", Some("basicConstraints=CA:FALSE"));
    Certs {
        dir,
        ca,
        server_crt,
        server_key,
        client_crt,
        client_key,
    }
}

fn tls_config(certs: &Certs, verify: bool) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        tls: Some(TlsSettings {
            cert_file: certs.server_crt.to_str().unwrap().into(),
            key_file: certs.server_key.to_str().unwrap().into(),
            ca_file: Some(certs.ca.to_str().unwrap().into()),
            verify,
            verify_and_map: false,
        }),
        ..Default::default()
    }
}

fn load_roots(ca: &Path) -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    let file = std::fs::File::open(ca).unwrap();
    for cert in rustls_pemfile::certs(&mut std::io::BufReader::new(file)) {
        roots.add(cert.unwrap()).unwrap();
    }
    roots
}

fn client_config(ca: &Path, client_auth: Option<(&Path, &Path)>) -> Arc<rustls::ClientConfig> {
    rust_natsd::tls::install_default_provider();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .unwrap()
        .with_root_certificates(load_roots(ca));
    let config = match client_auth {
        Some((crt, key)) => {
            let certs: Vec<_> = {
                let file = std::fs::File::open(crt).unwrap();
                rustls_pemfile::certs(&mut std::io::BufReader::new(file))
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()
            };
            let key = {
                let file = std::fs::File::open(key).unwrap();
                rustls_pemfile::private_key(&mut std::io::BufReader::new(file))
                    .unwrap()
                    .unwrap()
            };
            builder.with_client_auth_cert(certs, key).unwrap()
        }
        None => builder.with_no_client_auth(),
    };
    Arc::new(config)
}

/// Minimal raw NATS client over any AsyncRead+AsyncWrite stream.
struct Raw<S> {
    stream: S,
    buf: Vec<u8>,
}

impl<S: AsyncReadExt + AsyncWriteExt + Unpin> Raw<S> {
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

    async fn read_exact_n(&mut self, n: usize) -> Vec<u8> {
        while self.buf.len() < n {
            let mut chunk = [0u8; 4096];
            let m = self.stream.read(&mut chunk).await.expect("read");
            assert!(m > 0, "server closed connection");
            self.buf.extend_from_slice(&chunk[..m]);
        }
        self.buf.drain(..n).collect()
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

/// STARTTLS upgrade (Go's order): read the plaintext INFO first, then
/// wrap the same socket in TLS. Returns the TLS client plus the INFO
/// that was already consumed in plaintext.
async fn tls_connect(
    srv_addr: std::net::SocketAddr,
    certs: &Certs,
    client_auth: bool,
) -> Result<
    (
        Raw<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>,
        serde_json::Value,
    ),
    std::io::Error,
> {
    let config = client_config(
        &certs.ca,
        if client_auth {
            Some((&certs.client_crt, &certs.client_key))
        } else {
            None
        },
    );
    let connector = tokio_rustls::TlsConnector::from(config);
    let tcp = tokio::net::TcpStream::connect(srv_addr).await?;
    let mut plain = Raw {
        stream: tcp,
        buf: Vec::new(),
    };
    let info = plain.info().await;
    // The server sends nothing after INFO before the upgrade, so the
    // read buffer must be empty here; anything else would be lost.
    assert!(plain.buf.is_empty(), "no bytes may follow plaintext INFO");
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let stream = connector.connect(name, plain.stream).await?;
    Ok((
        Raw {
            stream,
            buf: Vec::new(),
        },
        info,
    ))
}

#[tokio::test]
async fn tls_roundtrip_and_plain_client_fails() {
    let certs = gen_certs("roundtrip");
    let srv = server::start(tls_config(&certs, false))
        .await
        .expect("server starts");
    // Plain-text client (Go STARTTLS behaviour): it DOES see the
    // plaintext INFO (tls_required:true), and is then closed silently
    // when it sends non-TLS bytes — no -ERR, no PONG.
    let plain = tokio::net::TcpStream::connect(srv.addr).await.unwrap();
    let mut plain_raw = Raw {
        stream: plain,
        buf: Vec::new(),
    };
    let plain_info = plain_raw.info().await;
    assert_eq!(plain_info["tls_required"], true);
    plain_raw.send("PING\r\n").await;
    let mut buf = [0u8; 256];
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        plain_raw.stream.read(&mut buf),
    )
    .await;
    assert!(
        matches!(read, Ok(Ok(0)) | Ok(Err(_)) | Err(_)),
        "plain connection should close silently, got {read:?}"
    );

    // TLS client: plaintext INFO reports tls_required, then pub/sub
    // works inside TLS.
    let (mut client, info) = tls_connect(srv.addr, &certs, false)
        .await
        .expect("TLS handshake");
    assert_eq!(info["tls_required"], true);
    client
        .send("CONNECT {\"verbose\":false,\"headers\":true}\r\n")
        .await;
    client.send("SUB tls.topic 1\r\n").await;
    client.send("PUB tls.topic 5\r\nhello\r\n").await;
    let line = client.read_line().await;
    assert!(line.starts_with("MSG tls.topic 1 5"), "delivery: {line}");
    let payload = client.read_exact_n(5 + 2).await;
    assert_eq!(&payload[..5], b"hello");
    srv.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&certs.dir);
}

#[tokio::test]
async fn tls_verify_requires_client_certificate() {
    let certs = gen_certs("verify");
    let srv = server::start(tls_config(&certs, true))
        .await
        .expect("server starts");
    // Without a client cert the server must reject the client. Note this
    // surfaces on first read, not on connect(): in TLS 1.3 the client's
    // connect() can return Ok before the server's post-Finished
    // certificate-required alert arrives. The plaintext INFO is expected
    // (STARTTLS); what must never happen is any protocol data after the
    // upgrade — the TLS read must fail or close.
    match tls_connect(srv.addr, &certs, false).await {
        Err(_) => {}
        Ok((mut client, _info)) => {
            let mut buf = [0u8; 256];
            let read = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                client.stream.read(&mut buf),
            )
            .await;
            assert!(
                matches!(read, Ok(Ok(0)) | Ok(Err(_)) | Err(_)),
                "connection should close after rejection, got {read:?}"
            );
        }
    }
    // With a CA-signed client cert it succeeds and can publish.
    let (mut client, info) = tls_connect(srv.addr, &certs, true)
        .await
        .expect("TLS handshake with client cert");
    assert_eq!(info["tls_required"], true);
    client
        .send("CONNECT {\"verbose\":false,\"headers\":true}\r\n")
        .await;
    client.send("SUB v.topic 1\r\n").await;
    client.send("PUB v.topic 2\r\nhi\r\n").await;
    let line = client.read_line().await;
    assert!(line.starts_with("MSG v.topic 1 2"), "delivery: {line}");
    srv.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&certs.dir);
}
