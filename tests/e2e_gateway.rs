//! Gateway (supercluster) end-to-end tests: two rust-natsd nodes
//! connected via the gateway protocol, exercised with the official
//! async-nats client, plus the /gatewayz shape and .conf parsing.

use std::time::Duration;

use futures::StreamExt;
use rust_natsd::config::{Account, AccountUser, Config, GatewayConfig, GatewayRemote};
use rust_natsd::server::{self, RunningServer};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn base_config() -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        ..Default::default()
    }
}

fn gateway(name: &str, port: u16, remotes: Vec<GatewayRemote>) -> GatewayConfig {
    GatewayConfig {
        name: name.to_string(),
        host: "127.0.0.1".to_string(),
        port,
        tls: None,
        remotes,
    }
}

fn remote(name: &str, url: String) -> GatewayRemote {
    GatewayRemote {
        name: name.to_string(),
        urls: vec![url],
        tls: None,
    }
}

async fn start(config: Config) -> RunningServer {
    server::start(config).await.expect("server starts")
}

async fn connect(srv: &RunningServer) -> async_nats::Client {
    async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("async-nats connects")
}

/// A seed node (gateway A, no solicitations) and a solicitor node
/// (gateway B, soliciting A). Returns (seed, solicitor).
async fn seed_and_solicitor() -> (RunningServer, RunningServer) {
    let seed_port = free_port();
    let seed = start(Config {
        gateway: Some(gateway("A", seed_port, vec![])),
        http_port: free_port(),
        ..base_config()
    })
    .await;
    let seed_gw = seed.gateway_addr.unwrap();
    let solicitor = start(Config {
        gateway: Some(gateway(
            "B",
            free_port(),
            vec![remote("A", format!("nats://{seed_gw}"))],
        )),
        http_port: free_port(),
        ..base_config()
    })
    .await;
    (seed, solicitor)
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
            _ => assert!(
                tokio::time::Instant::now() < deadline,
                "gateway delivery timed out for {subject}"
            ),
        }
    }
}

async fn http_get(srv: &RunningServer, path: &str) -> serde_json::Value {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let http = srv.http_addr.unwrap();
    let mut stream = tokio::net::TcpStream::connect(http).await.unwrap();
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("");
    serde_json::from_str(body).expect("gatewayz parses")
}

async fn wait_gatewayz(
    srv: &RunningServer,
    want_outbound: &str,
    want_inbound: &str,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + T;
    loop {
        let gz = http_get(srv, "/gatewayz").await;
        let out_ok = want_outbound.is_empty() || gz["outbound_gateways"][want_outbound].is_object();
        let in_ok = want_inbound.is_empty() || gz["inbound_gateways"][want_inbound].is_array();
        if out_ok && in_ok {
            return gz;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "gatewayz never showed outbound={want_outbound} inbound={want_inbound}: {gz}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn gateway_pub_sub_both_directions() {
    let (seed, solicitor) = seed_and_solicitor().await;

    let sub_solicitor = connect(&solicitor).await;
    let pub_seed = connect(&seed).await;
    let mut sub = sub_solicitor.subscribe("hello").await.unwrap();
    sub_solicitor.flush().await.unwrap();
    publish_until_received(&pub_seed, &mut sub, "hello", b"seed-to-solicitor").await;

    let sub_seed = connect(&seed).await;
    let pub_solicitor = connect(&solicitor).await;
    let mut sub2 = sub_seed.subscribe("back").await.unwrap();
    sub_seed.flush().await.unwrap();
    publish_until_received(&pub_solicitor, &mut sub2, "back", b"solicitor-to-seed").await;

    solicitor.shutdown().await.unwrap();
    seed.shutdown().await.unwrap();
}

/// Minimal raw-TCP client: reads the INFO line, sends CONNECT,
/// and speaks SUB/PUB/MSG framing directly (no client library).
async fn raw_client(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert!(line.starts_with("INFO "), "first frame is INFO: {line}");
    let mut stream = reader.into_inner();
    stream
        .write_all(b"CONNECT {\"verbose\":false,\"pedantic\":false}\r\n")
        .await
        .unwrap();
    stream
}

#[tokio::test]
async fn gateway_raw_wire_both_directions() {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    let (seed, solicitor) = seed_and_solicitor().await;

    // Raw subscriber on the solicitor, raw publisher on the seed.
    let mut sub_stream = raw_client(solicitor.addr).await;
    sub_stream.write_all(b"SUB hello 1\r\n").await.unwrap();
    let mut pub_stream = raw_client(seed.addr).await;
    // Retry: the gateway + RS+ propagation may lag the SUB.
    let deadline = tokio::time::Instant::now() + T;
    let mut got = false;
    let mut reader = BufReader::new(sub_stream);
    let mut line = String::new();
    while !got {
        pub_stream
            .write_all(b"PUB hello 4\r\nraw!\r\n")
            .await
            .unwrap();
        line.clear();
        match timeout(Duration::from_millis(200), reader.read_line(&mut line)).await {
            Ok(Ok(_)) if line.starts_with("MSG hello 1 ") => {
                let mut payload = [0u8; 6];
                reader.read_exact(&mut payload).await.unwrap();
                assert_eq!(&payload, b"raw!\r\n");
                got = true;
            }
            _ => assert!(
                tokio::time::Instant::now() < deadline,
                "raw gateway delivery timed out"
            ),
        }
    }
    let sub_stream = reader.into_inner();
    drop(sub_stream);
    drop(pub_stream);

    solicitor.shutdown().await.unwrap();
    seed.shutdown().await.unwrap();
}

#[tokio::test]
async fn gateway_headers_and_request_reply_across() {
    let (seed, solicitor) = seed_and_solicitor().await;

    let sub_client = connect(&solicitor).await;
    let pub_client = connect(&seed).await;
    let mut sub = sub_client.subscribe("with.headers").await.unwrap();
    sub_client.flush().await.unwrap();
    let mut headers = async_nats::header::HeaderMap::new();
    headers.insert("X-Gw", "yes");
    let deadline = tokio::time::Instant::now() + T;
    loop {
        pub_client
            .publish_with_headers("with.headers", headers.clone(), "body".into())
            .await
            .unwrap();
        match timeout(Duration::from_millis(200), sub.next()).await {
            Ok(Some(msg)) => {
                assert_eq!(msg.payload.as_ref(), b"body");
                assert_eq!(msg.headers.unwrap().get("X-Gw").unwrap().as_str(), "yes");
                break;
            }
            _ => assert!(tokio::time::Instant::now() < deadline),
        }
    }

    // Request/reply across the gateway (reply subject routing).
    let responder = connect(&solicitor).await;
    let mut rsub = responder.subscribe("svc.echo").await.unwrap();
    responder.flush().await.unwrap();
    let task = tokio::spawn(async move {
        let msg = timeout(T, rsub.next()).await.unwrap().unwrap();
        responder
            .publish(msg.reply.unwrap(), msg.payload)
            .await
            .unwrap();
    });
    let requester = connect(&seed).await;
    let resp = timeout(T, requester.request("svc.echo", "ping".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resp.payload.as_ref(), b"ping");
    task.await.unwrap();

    solicitor.shutdown().await.unwrap();
    seed.shutdown().await.unwrap();
}

#[tokio::test]
async fn gateway_account_scoped() {
    let accounts = |name: &str, user: &str| Account {
        name: name.to_string(),
        users: vec![AccountUser {
            name: user.to_string(),
            pass: Some(user.to_string()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mk = |gateway_name: &str, port: u16, remotes: Vec<GatewayRemote>| Config {
        gateway: Some(gateway(gateway_name, port, remotes)),
        accounts: vec![accounts("APP", "app"), accounts("OTHER", "oth")],
        ..base_config()
    };
    let seed_port = free_port();
    let seed = start(mk("A", seed_port, vec![])).await;
    let seed_gw = seed.gateway_addr.unwrap();
    let solicitor = start(mk(
        "B",
        free_port(),
        vec![remote("A", format!("nats://{seed_gw}"))],
    ))
    .await;

    let opts = |user: &str| {
        async_nats::ConnectOptions::new().user_and_password(user.to_string(), user.to_string())
    };
    let app_seed = opts("app")
        .connect(format!("nats://{}", seed.addr))
        .await
        .unwrap();
    let oth_seed = opts("oth")
        .connect(format!("nats://{}", seed.addr))
        .await
        .unwrap();
    let mut app_sub = app_seed.subscribe("orders.new").await.unwrap();
    let mut oth_sub = oth_seed.subscribe("orders.new").await.unwrap();
    app_seed.flush().await.unwrap();
    oth_seed.flush().await.unwrap();

    let app_solicitor = opts("app")
        .connect(format!("nats://{}", solicitor.addr))
        .await
        .unwrap();
    publish_until_received(&app_solicitor, &mut app_sub, "orders.new", b"app-msg").await;

    // The OTHER subscriber must receive nothing for the APP publish:
    // publish again and require a timeout on the OTHER subscription.
    for _ in 0..3 {
        app_solicitor
            .publish("orders.new", "app-msg".into())
            .await
            .unwrap();
    }
    app_solicitor.flush().await.unwrap();
    assert!(
        timeout(Duration::from_millis(600), oth_sub.next())
            .await
            .is_err(),
        "OTHER subscriber received an APP message across the gateway"
    );

    solicitor.shutdown().await.unwrap();
    seed.shutdown().await.unwrap();
}

#[tokio::test]
async fn gateway_queue_group_exactly_once() {
    let (seed, solicitor) = seed_and_solicitor().await;

    let ch = connect(&seed).await;
    let cl = connect(&solicitor).await;
    let mut qh = ch
        .queue_subscribe("jobs", "workers".to_string())
        .await
        .unwrap();
    let mut ql = cl
        .queue_subscribe("jobs", "workers".to_string())
        .await
        .unwrap();
    ch.flush().await.unwrap();
    cl.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let pub_client = connect(&seed).await;
    for _ in 0..20 {
        pub_client.publish("jobs", "x".into()).await.unwrap();
    }
    pub_client.flush().await.unwrap();
    let mut total = 0;
    let deadline = tokio::time::Instant::now() + T;
    while total < 20 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "only got {total}/20"
        );
        tokio::select! {
            m = qh.next() => { assert!(m.is_some()); total += 1; }
            m = ql.next() => { assert!(m.is_some()); total += 1; }
        }
    }
    tokio::select! {
        m = qh.next() => panic!("extra delivery on seed: {:?}", m.map(|m| m.payload)),
        m = ql.next() => panic!("extra delivery on solicitor: {:?}", m.map(|m| m.payload)),
        _ = tokio::time::sleep(Duration::from_millis(300)) => {}
    }

    solicitor.shutdown().await.unwrap();
    seed.shutdown().await.unwrap();
}

#[tokio::test]
async fn gateway_implicit_back_connect_and_configured_flags() {
    let (seed, solicitor) = seed_and_solicitor().await;

    // Solicitor: outbound to A (configured: true), inbound from A
    // (the implicit back-connect, configured: false).
    let gz_sol = wait_gatewayz(&solicitor, "A", "A").await;
    assert_eq!(gz_sol["outbound_gateways"]["A"]["configured"], true);
    assert_eq!(gz_sol["inbound_gateways"]["A"][0]["configured"], false);
    // Seed: outbound to B is the implicit back-connect
    // (configured: false); inbound from B is the solicitation.
    let gz_seed = wait_gatewayz(&seed, "B", "B").await;
    assert_eq!(gz_seed["outbound_gateways"]["B"]["configured"], false);
    assert_eq!(gz_seed["inbound_gateways"]["B"][0]["configured"], false);

    // Both directions still carry traffic after the back-connect.
    let sub_sol = connect(&solicitor).await;
    let pub_seed = connect(&seed).await;
    let mut sub = sub_sol.subscribe("impl").await.unwrap();
    sub_sol.flush().await.unwrap();
    publish_until_received(&pub_seed, &mut sub, "impl", b"ok").await;

    solicitor.shutdown().await.unwrap();
    seed.shutdown().await.unwrap();
}

#[tokio::test]
async fn gateway_sys_subject_not_filtered() {
    // `$SYS` as a subject string in the default account must cross
    // like any other subject: the gateway path applies no subject
    // filtering, only the account token (Phase 24: Go's $SYS
    // advisories cross the gateway on the same code path).
    let (seed, solicitor) = seed_and_solicitor().await;
    let sub_client = connect(&solicitor).await;
    let pub_client = connect(&seed).await;
    let mut sub = sub_client.subscribe("$SYS.custom").await.unwrap();
    sub_client.flush().await.unwrap();
    publish_until_received(&pub_client, &mut sub, "$SYS.custom", b"sys-ok").await;
    solicitor.shutdown().await.unwrap();
    seed.shutdown().await.unwrap();
}

#[tokio::test]
async fn gateway_tls_implicit_mtls_both_directions() {
    // Gateway TLS is implicit mTLS (probed against Go 2.15.1):
    // the listener handshake comes first, and the dialer presents
    // a CA-verified client certificate. Certs are generated with
    // the openssl CLI into a throwaway temp dir (same pattern as
    // e2e_tls.rs).
    use rust_natsd::config::TlsSettings;
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
    let dir = std::env::temp_dir().join(format!("rust-natsd-gw-tls-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
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
    let mk = |stem: &str| -> (std::path::PathBuf, std::path::PathBuf) {
        let key = dir.join(format!("{stem}.key"));
        let csr = dir.join(format!("{stem}.csr"));
        let crt = dir.join(format!("{stem}.crt"));
        let ext = dir.join(format!("{stem}.ext"));
        std::fs::write(&ext, "subjectAltName=DNS:localhost,IP:127.0.0.1").unwrap();
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
            "/CN=localhost",
        ]);
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
    let (crt, key) = mk("gw");
    let tls = || TlsSettings {
        cert_file: crt.to_str().unwrap().to_string(),
        key_file: key.to_str().unwrap().to_string(),
        ca_file: Some(ca.to_str().unwrap().to_string()),
        verify: true,
        verify_and_map: false,
    };
    let seed_port = free_port();
    let seed = start(Config {
        gateway: Some(GatewayConfig {
            name: "A".to_string(),
            host: "127.0.0.1".to_string(),
            port: seed_port,
            tls: Some(tls()),
            remotes: vec![],
        }),
        ..base_config()
    })
    .await;
    let seed_gw = seed.gateway_addr.unwrap();
    let solicitor = start(Config {
        gateway: Some(GatewayConfig {
            name: "B".to_string(),
            host: "127.0.0.1".to_string(),
            port: free_port(),
            tls: Some(tls()),
            remotes: vec![GatewayRemote {
                name: "A".to_string(),
                urls: vec![format!("nats://{seed_gw}")],
                tls: Some(tls()),
            }],
        }),
        ..base_config()
    })
    .await;

    let sub_sol = connect(&solicitor).await;
    let pub_seed = connect(&seed).await;
    let mut sub = sub_sol.subscribe("tls.hello").await.unwrap();
    sub_sol.flush().await.unwrap();
    publish_until_received(&pub_seed, &mut sub, "tls.hello", b"tls-ok").await;

    let sub_seed = connect(&seed).await;
    let pub_sol = connect(&solicitor).await;
    let mut sub2 = sub_seed.subscribe("tls.back").await.unwrap();
    sub_seed.flush().await.unwrap();
    publish_until_received(&pub_sol, &mut sub2, "tls.back", b"tls-back").await;

    solicitor.shutdown().await.unwrap();
    seed.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn gateway_conf_parsing() {
    let text = r#"
        port: 14222
        gateway {
          name: EDGE
          host: "127.0.0.1"
          port: 17222
          gateways: [{ name: "CORE", urls: ["nats://127.0.0.1:17221"] }]
          unknown_key: ignored
        }
    "#;
    let cfg = rust_natsd::conf::apply_to(&Config::default(), text).unwrap();
    let gw = cfg.gateway.expect("gateway parsed");
    assert_eq!(gw.name, "EDGE");
    assert_eq!(gw.host, "127.0.0.1");
    assert_eq!(gw.port, 17222);
    assert_eq!(gw.remotes.len(), 1);
    assert_eq!(gw.remotes[0].name, "CORE");
    assert_eq!(gw.remotes[0].urls, vec!["nats://127.0.0.1:17221"]);
}

#[test]
fn gateway_conf_remotes_key_is_ignored_like_go_rejects_it() {
    // Go rejects `remotes` as unknown; rust-natsd ignores unknown
    // keys, so a config using it loads but solicits nothing — it
    // must never be silently treated as a solicitation list.
    let text = r#"
        gateway {
          name: EDGE
          port: 17222
          remotes: [{ name: "CORE", urls: ["nats://127.0.0.1:17221"] }]
        }
    "#;
    let cfg = rust_natsd::conf::apply_to(&Config::default(), text).unwrap();
    let gw = cfg.gateway.expect("gateway parsed");
    assert!(gw.remotes.is_empty());
}
