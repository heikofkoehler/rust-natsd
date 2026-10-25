//! Route/leaf TLS + TLS client-certificate-to-user mapping (gap
//! area 7): Rust-to-Rust routes and leafs over TLS, plain-route-peer
//! rejection on a TLS route port, and `verify_and_map` authentication
//! (CN, DN, and SAN forms; unmapped rejection; permissions enforced).
//!
//! Certificates are generated at test time with the `openssl` CLI
//! into a per-test temp dir (throwaway test certs only).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rust_natsd::config::{Account, AccountUser, Config, Permissions, TlsSettings};
use rust_natsd::server::{self, RunningServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);

fn tmpdir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rust-natsd-rtls-{}-{}", std::process::id(), name));
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
}

impl Certs {
    /// Make a client certificate with the given CN and optional
    /// extra extension content (e.g. a SAN line). Returns (crt, key).
    fn client_cert(&self, cn: &str, stem: &str, ext: Option<&str>) -> (PathBuf, PathBuf) {
        let key = self.dir.join(format!("{stem}.key"));
        let csr = self.dir.join(format!("{stem}.csr"));
        let crt = self.dir.join(format!("{stem}.crt"));
        let ca_key = self.dir.join("ca.key");
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
            self.ca.to_str().unwrap(),
            "-CAkey",
            ca_key.to_str().unwrap(),
            "-CAcreateserial",
            "-out",
            crt.to_str().unwrap(),
            "-days",
            "2",
        ];
        let ext_path = self.dir.join(format!("{stem}.ext"));
        if let Some(content) = ext {
            std::fs::write(&ext_path, content).unwrap();
            args.push("-extfile");
            args.push(ext_path.to_str().unwrap());
        } else {
            // A v1 certificate (no extensions) is rejected by rustls;
            // give every client cert a minimal extension block.
            std::fs::write(&ext_path, "basicConstraints=CA:FALSE").unwrap();
            args.push("-extfile");
            args.push(ext_path.to_str().unwrap());
        }
        openssl(&args);
        (crt, key)
    }

    fn tls(&self, verify: bool) -> TlsSettings {
        TlsSettings {
            cert_file: self.server_crt.to_str().unwrap().into(),
            key_file: self.server_key.to_str().unwrap().into(),
            ca_file: Some(self.ca.to_str().unwrap().into()),
            verify,
            verify_and_map: false,
        }
    }
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
    let mk = |cn: &str, stem: &str| -> (PathBuf, PathBuf) {
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
        let ext = dir.join(format!("{stem}.ext"));
        std::fs::write(
            &ext,
            "subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE",
        )
        .unwrap();
        openssl(&[
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
            "-extfile",
            ext.to_str().unwrap(),
        ]);
        (crt, key)
    };
    let (server_crt, server_key) = mk("localhost", "server");
    Certs {
        dir,
        ca,
        server_crt,
        server_key,
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn publish_until_received(
    pub_client: &async_nats::Client,
    sub: &mut async_nats::Subscriber,
    subject: &str,
    payload: &[u8],
) {
    let subject = subject.to_string();
    let deadline = tokio::time::Instant::now() + T;
    loop {
        pub_client
            .publish(subject.clone(), payload.to_vec().into())
            .await
            .unwrap();
        match timeout(Duration::from_millis(200), sub.next()).await {
            Ok(Some(msg)) => {
                assert_eq!(msg.payload.as_ref(), payload);
                return;
            }
            _ => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "delivery timed out for {subject}"
                );
            }
        }
    }
}

async fn connect(srv: &RunningServer) -> async_nats::Client {
    async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("async-nats connects")
}

// ---------------------------------------------------------------------------
// Rust-to-Rust route / leaf over TLS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn route_tls_rust_to_rust_both_directions() {
    let certs = gen_certs("route");
    let cluster_tls = certs.tls(true);
    let seed = server::start(Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_name: Some("test".into()),
        cluster_host: "127.0.0.1".into(),
        cluster_port: free_port(),
        cluster_tls: Some(cluster_tls.clone()),
        ..Default::default()
    })
    .await
    .expect("seed starts");
    let seed_cluster = seed.cluster_addr.unwrap();
    let node = server::start(Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_name: Some("test".into()),
        cluster_host: "127.0.0.1".into(),
        cluster_port: free_port(),
        cluster_tls: Some(cluster_tls),
        routes: vec![format!("nats://{seed_cluster}")],
        ..Default::default()
    })
    .await
    .expect("node starts");

    let sub_seed = connect(&seed).await;
    let pub_node = connect(&node).await;
    let mut sub = sub_seed.subscribe("tls.hello").await.unwrap();
    sub_seed.flush().await.unwrap();
    publish_until_received(&pub_node, &mut sub, "tls.hello", b"via-tls-route").await;

    let sub_node = connect(&node).await;
    let pub_seed = connect(&seed).await;
    let mut sub2 = sub_node.subscribe("tls.back").await.unwrap();
    sub_node.flush().await.unwrap();
    publish_until_received(&pub_seed, &mut sub2, "tls.back", b"back-via-tls").await;

    node.shutdown().await.unwrap();
    seed.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&certs.dir);
}

#[tokio::test]
async fn route_tls_rejects_plain_route_peer() {
    let certs = gen_certs("route-plain");
    let tls_node = server::start(Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_name: Some("test".into()),
        cluster_host: "127.0.0.1".into(),
        cluster_port: free_port(),
        cluster_tls: Some(certs.tls(true)),
        ..Default::default()
    })
    .await
    .expect("tls node starts");
    let tls_cluster = tls_node.cluster_addr.unwrap();
    // Plain node solicits the TLS route port: its CONNECT goes into
    // a TLS handshake and must never establish a route.
    let plain = server::start(Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_name: Some("test".into()),
        cluster_host: "127.0.0.1".into(),
        cluster_port: free_port(),
        routes: vec![format!("nats://{tls_cluster}")],
        ..Default::default()
    })
    .await
    .expect("plain node starts");

    let sub_tls = connect(&tls_node).await;
    let pub_plain = connect(&plain).await;
    let mut sub = sub_tls.subscribe("nope").await.unwrap();
    sub_tls.flush().await.unwrap();
    for _ in 0..5 {
        pub_plain.publish("nope", "x".into()).await.unwrap();
        assert!(
            timeout(Duration::from_millis(300), sub.next())
                .await
                .is_err(),
            "plain peer must never deliver across a TLS route port"
        );
    }

    plain.shutdown().await.unwrap();
    tls_node.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&certs.dir);
}

#[tokio::test]
async fn leaf_tls_rust_to_rust_both_directions() {
    let certs = gen_certs("leaf");
    let hub = server::start(Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        cluster_name: Some("hub".into()),
        leaf_port: free_port(),
        leaf_tls: Some(certs.tls(true)),
        ..Default::default()
    })
    .await
    .expect("hub starts");
    let hub_leaf = hub.leaf_addr.unwrap();
    let leaf_url = format!("nats://{hub_leaf}");
    let mut leaf_remote_tls = std::collections::HashMap::new();
    leaf_remote_tls.insert(leaf_url.clone(), certs.tls(false));
    let leaf = server::start(Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        cluster_name: Some("leaf".into()),
        leaf_urls: vec![leaf_url],
        leaf_remote_tls,
        ..Default::default()
    })
    .await
    .expect("leaf starts");

    // Leaf -> hub.
    let sub_hub = connect(&hub).await;
    let pub_leaf = connect(&leaf).await;
    let mut sub = sub_hub.subscribe("leaf.up").await.unwrap();
    sub_hub.flush().await.unwrap();
    publish_until_received(&pub_leaf, &mut sub, "leaf.up", b"up-via-tls-leaf").await;

    // Hub -> leaf.
    let sub_leaf = connect(&leaf).await;
    let pub_hub = connect(&hub).await;
    let mut sub2 = sub_leaf.subscribe("leaf.down").await.unwrap();
    sub_leaf.flush().await.unwrap();
    publish_until_received(&pub_hub, &mut sub2, "leaf.down", b"down-via-tls-leaf").await;

    leaf.shutdown().await.unwrap();
    hub.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&certs.dir);
}

// ---------------------------------------------------------------------------
// TLS client-certificate-to-user mapping (raw STARTTLS clients)
// ---------------------------------------------------------------------------

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

    async fn send(&mut self, data: &str) {
        self.stream.write_all(data.as_bytes()).await.unwrap();
    }
}

/// STARTTLS upgrade: read the plaintext INFO, then wrap the socket.
async fn tls_connect(
    srv_addr: std::net::SocketAddr,
    certs: &Certs,
    client_auth: Option<(&Path, &Path)>,
) -> Raw<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
    let config = client_config(&certs.ca, client_auth);
    let connector = tokio_rustls::TlsConnector::from(config);
    let tcp = tokio::net::TcpStream::connect(srv_addr).await.unwrap();
    let mut plain = Raw {
        stream: tcp,
        buf: Vec::new(),
    };
    let info = plain.read_line().await;
    assert!(info.starts_with("INFO "), "plaintext INFO: {info}");
    assert!(plain.buf.is_empty(), "no bytes may follow plaintext INFO");
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let stream = connector.connect(name, plain.stream).await.unwrap();
    Raw {
        stream,
        buf: Vec::new(),
    }
}

fn map_config(certs: &Certs) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        tls: Some(TlsSettings {
            cert_file: certs.server_crt.to_str().unwrap().into(),
            key_file: certs.server_key.to_str().unwrap().into(),
            ca_file: Some(certs.ca.to_str().unwrap().into()),
            verify: true,
            verify_and_map: true,
        }),
        accounts: vec![Account {
            name: "A".into(),
            users: vec![
                AccountUser {
                    name: "alice".into(),
                    pass: Some("alicepw".into()),
                    permissions: Permissions {
                        publish_allow: Some(vec!["allowed.>".into()]),
                        subscribe_allow: Some(vec!["allowed.>".into(), "_INBOX.>".into()]),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                // DN-form user (Go's verify_and_map spelling).
                AccountUser {
                    name: "CN=carol".into(),
                    ..Default::default()
                },
                AccountUser {
                    name: "dave".into(),
                    permissions: Permissions {
                        publish_allow: Some(vec!["dave.>".into()]),
                        subscribe_allow: Some(vec!["dave.>".into()]),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn cert_mapped_cn_authenticates_without_password_and_permissions_enforced() {
    let certs = gen_certs("map-cn");
    let (crt, key) = certs.client_cert("alice", "alice", None);
    let srv = server::start(map_config(&certs))
        .await
        .expect("server starts");
    let mut client = tls_connect(srv.addr, &certs, Some((&crt, &key))).await;
    // No credentials in CONNECT: the certificate is the credential.
    client
        .send("CONNECT {\"verbose\":false,\"headers\":true}\r\n")
        .await;
    client.send("SUB allowed.x 1\r\n").await;
    client.send("PUB allowed.x 2\r\nhi\r\n").await;
    let line = client.read_line().await;
    assert!(line.starts_with("MSG allowed.x 1 2"), "delivery: {line}");
    assert_eq!(&client.read_exact_n(4).await[..2], b"hi");
    // Mapped user's allow/deny still enforced.
    client.send("PUB denied.y 2\r\nhi\r\n").await;
    let line = client.read_line().await;
    assert_eq!(
        line, "-ERR 'Permissions Violation for Publish to \"denied.y\"'",
        "mapped user permissions: {line}"
    );
    srv.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&certs.dir);
}

#[tokio::test]
async fn cert_unmapped_cn_rejected() {
    let certs = gen_certs("map-unknown");
    let (crt, key) = certs.client_cert("mallory", "mallory", None);
    let srv = server::start(map_config(&certs))
        .await
        .expect("server starts");
    let mut client = tls_connect(srv.addr, &certs, Some((&crt, &key))).await;
    client
        .send("CONNECT {\"verbose\":false,\"headers\":true}\r\n")
        .await;
    let line = client.read_line().await;
    assert_eq!(line, "-ERR 'Authorization Violation'", "unmapped: {line}");
    srv.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&certs.dir);
}

#[tokio::test]
async fn cert_dn_form_maps_to_dn_user() {
    let certs = gen_certs("map-dn");
    let (crt, key) = certs.client_cert("carol", "carol", None);
    let srv = server::start(map_config(&certs))
        .await
        .expect("server starts");
    let mut client = tls_connect(srv.addr, &certs, Some((&crt, &key))).await;
    client
        .send("CONNECT {\"verbose\":false,\"headers\":true}\r\n")
        .await;
    client.send("SUB carol.x 1\r\n").await;
    client.send("PUB carol.x 2\r\nhi\r\n").await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("MSG carol.x 1 2"),
        "DN-mapped delivery: {line}"
    );
    srv.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&certs.dir);
}

#[tokio::test]
async fn cert_san_takes_priority_over_cn() {
    let certs = gen_certs("map-san");
    // SAN DNS:dave, CN:alice. Both name users; the SAN must win, so
    // this connection is dave (dave.> only), not alice (allowed.>).
    let (crt, key) = certs.client_cert(
        "alice",
        "sandave",
        Some("subjectAltName=DNS:dave\nbasicConstraints=CA:FALSE"),
    );
    let srv = server::start(map_config(&certs))
        .await
        .expect("server starts");
    let mut client = tls_connect(srv.addr, &certs, Some((&crt, &key))).await;
    client
        .send("CONNECT {\"verbose\":false,\"headers\":true}\r\n")
        .await;
    client.send("SUB dave.x 1\r\n").await;
    client.send("PUB dave.x 2\r\nhi\r\n").await;
    let line = client.read_line().await;
    assert!(line.starts_with("MSG dave.x 1 2"), "SAN-mapped: {line}");
    assert_eq!(&client.read_exact_n(4).await[..2], b"hi");
    // alice's subject must be denied for dave.
    client.send("PUB allowed.x 2\r\nhi\r\n").await;
    let line = client.read_line().await;
    assert_eq!(
        line, "-ERR 'Permissions Violation for Publish to \"allowed.x\"'",
        "SAN user, not CN user: {line}"
    );
    srv.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&certs.dir);
}

#[tokio::test]
async fn cert_mapping_wins_over_wrong_password() {
    let certs = gen_certs("map-pass");
    let (crt, key) = certs.client_cert("alice", "alice2", None);
    let srv = server::start(map_config(&certs))
        .await
        .expect("server starts");
    let mut client = tls_connect(srv.addr, &certs, Some((&crt, &key))).await;
    // Wrong password, valid mapped certificate: the certificate is
    // the credential in verify_and_map mode (Go behaviour probed:
    // password auth does not rescue an unmapped cert, and a mapped
    // cert authenticates without credentials).
    client
        .send("CONNECT {\"user\":\"alice\",\"pass\":\"wrong\",\"verbose\":false,\"headers\":true}\r\n")
        .await;
    client.send("SUB allowed.x 1\r\n").await;
    client.send("PUB allowed.x 2\r\nhi\r\n").await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("MSG allowed.x 1 2"),
        "mapping wins: {line}"
    );
    srv.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(&certs.dir);
}

#[tokio::test]
async fn conf_route_leaf_and_map_tls_shape_parses() {
    let cfg = rust_natsd::conf::apply_to(
        &Config::default(),
        "port: 0\ntls { cert_file: \"s.crt\", key_file: \"s.key\", ca_file: \"ca.crt\", verify_and_map: true }\ncluster { name: test, port: 6222, tls { cert_file: \"c.crt\", key_file: \"c.key\", ca_file: \"ca.crt\", verify: true } }\nleafnodes { port: 7422, tls { cert_file: \"l.crt\", key_file: \"l.key\", ca_file: \"ca.crt\", verify: true }, remotes: [ { url: \"nats://127.0.0.1:7422\", tls: { cert_file: \"r.crt\", key_file: \"r.key\", ca_file: \"ca.crt\" } } ] }\n",
    )
    .expect("parse tls conf");
    let tls = cfg.tls.expect("client tls");
    assert!(tls.verify_and_map);
    assert!(tls.verify, "verify_and_map implies verify");
    let cluster_tls = cfg.cluster_tls.expect("cluster tls");
    assert_eq!(cluster_tls.cert_file, "c.crt");
    assert!(cluster_tls.verify);
    let leaf_tls = cfg.leaf_tls.expect("leaf tls");
    assert_eq!(leaf_tls.cert_file, "l.crt");
    assert_eq!(cfg.leaf_urls, vec!["nats://127.0.0.1:7422".to_string()]);
    let remote = cfg
        .leaf_remote_tls
        .get("nats://127.0.0.1:7422")
        .expect("remote tls");
    assert_eq!(remote.cert_file, "r.crt");
}
