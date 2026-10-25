//! Config file integration tests: a .conf file loads, CLI flags win
//! over file values, and a server started from file config serves.

use rust_natsd::config::Config;
use rust_natsd::server;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn write_conf(name: &str, body: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "rust-natsd-conf-{}-{}.conf",
        std::process::id(),
        name
    ));
    std::fs::write(&path, body).expect("write conf");
    path
}

#[tokio::test]
async fn file_config_loads_and_cli_wins() {
    let http = free_port();
    let client_port = free_port();
    let path = write_conf("main", &format!(
        "host: 127.0.0.1\nport: 0\nserver_name: conf-name\nhttp_port: {http}\njetstream {{}}\ncluster {{\n  name: conf-cluster\n  port: 0\n  routes: [\"nats://127.0.0.1:6222\"]\n}}\n"
    ));
    let cfg = Config::from_args(&[
        "-c".to_string(),
        path.to_string_lossy().to_string(),
        "-p".to_string(),
        client_port.to_string(),
    ])
    .expect("load conf + cli");
    assert_eq!(cfg.name, "conf-name");
    assert_eq!(cfg.port, client_port, "CLI port wins over file");
    assert_eq!(cfg.http_port, http);
    assert!(cfg.jetstream);
    assert_eq!(cfg.cluster_name.as_deref(), Some("conf-cluster"));
    assert_eq!(cfg.routes, vec!["nats://127.0.0.1:6222"]);

    let srv = server::start(cfg).await.expect("server starts");
    let client = async_nats::connect(format!("nats://{}", srv.addr))
        .await
        .expect("connects");
    drop(client);
    // Monitoring (http_port came from the file) answers /jsz.
    let mut stream = tokio::net::TcpStream::connect(srv.http_addr.unwrap())
        .await
        .expect("connect http");
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream
        .write_all(b"GET /jsz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .expect("write");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read");
    let text = String::from_utf8_lossy(&raw);
    assert!(
        text.starts_with("HTTP/1.1 200"),
        "jsz enabled via file: {text}"
    );
    std::fs::remove_file(&path).ok();
}

#[test]
fn missing_config_file_errors() {
    let res = Config::from_args(&[
        "-c".to_string(),
        "/nonexistent/rust-natsd-test.conf".to_string(),
    ]);
    assert!(res.is_err());
}

#[test]
fn file_auth_and_leafnodes() {
    let path = write_conf(
        "auth",
        "authorization {\n  user: alice\n  password: secret\n}\nleafnodes {\n  port: 7433\n  remotes: [{url: \"nats://127.0.0.1:7432\"}]\n  authorization { user: leafu password: leafp }\n}\n",
    );
    let cfg = Config::from_args(&["-c".to_string(), path.to_string_lossy().to_string()])
        .expect("load conf");
    assert_eq!(cfg.user.as_deref(), Some("alice"));
    assert_eq!(cfg.pass.as_deref(), Some("secret"));
    assert_eq!(cfg.leaf_port, 7433);
    assert_eq!(cfg.leaf_urls, vec!["nats://127.0.0.1:7432"]);
    assert_eq!(cfg.leaf_user.as_deref(), Some("leafu"));
    assert_eq!(cfg.leaf_pass.as_deref(), Some("leafp"));
    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn file_accounts_isolate_clients_end_to_end() {
    // Accounts tests use Config structs directly; this one proves the
    // .conf -> accounts wiring serves real clients in isolation.
    let path = write_conf(
        "accounts",
        "host: 127.0.0.1\nport: 0\ncluster {\n  port: 0\n}\naccounts {\n  A {\n    users: [{user: alice, password: alicepw}]\n  }\n  B {\n    users: [{user: bob, password: bobpw}]\n  }\n}\n",
    );
    let cfg = Config::from_args(&["-c".to_string(), path.to_string_lossy().to_string()])
        .expect("load conf");
    assert_eq!(cfg.accounts.len(), 2);
    let srv = server::start(cfg).await.expect("server starts");
    let alice = async_nats::ConnectOptions::with_user_and_password(
        "alice".to_string(),
        "alicepw".to_string(),
    )
    .connect(format!("nats://{}", srv.addr))
    .await
    .expect("alice connects");
    let bob = async_nats::ConnectOptions::with_user_and_password(
        "bob".to_string(),
        "bobpw".to_string(),
    )
    .connect(format!("nats://{}", srv.addr))
    .await
    .expect("bob connects");
    let mut bob_sub = bob
        .subscribe("shared.topic".to_string())
        .await
        .expect("bob subscribes");
    bob.flush().await.expect("flush");
    alice
        .publish("shared.topic", bytes::Bytes::from("secret"))
        .await
        .expect("alice publishes");
    // Cross-account delivery must not happen.
    let got = tokio::time::timeout(std::time::Duration::from_millis(500), {
        use futures::StreamExt;
        bob_sub.next()
    })
    .await;
    assert!(got.is_err(), "account A publish leaked to account B");
    drop(alice);
    drop(bob);
    srv.shutdown().await.expect("shutdown");
    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn file_jetstream_store_dir_serves_jsz() {
    let dir = std::env::temp_dir().join(format!("rust-natsd-conf-js-{}", std::process::id()));
    let path = write_conf(
        "jsdir",
        &format!(
            "host: 127.0.0.1\nport: 0\ncluster {{\n  port: 0\n}}\nhttp_port: {}\njetstream {{\n  store_dir: \"{}\"\n}}\n",
            free_port(),
            dir.display()
        ),
    );
    let cfg = Config::from_args(&["-c".to_string(), path.to_string_lossy().to_string()])
        .expect("load conf");
    assert!(cfg.jetstream);
    assert_eq!(cfg.js_store_dir.as_deref(), Some(dir.as_path().to_str().unwrap()));
    let srv = server::start(cfg).await.expect("server starts");
    let mut stream = tokio::net::TcpStream::connect(srv.http_addr.unwrap())
        .await
        .expect("connect http");
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream
        .write_all(b"GET /jsz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .expect("write");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read");
    let text = String::from_utf8_lossy(&raw);
    assert!(text.starts_with("HTTP/1.1 200"), "jsz via file: {text}");
    assert!(text.contains("\"store_dir\""), "jsz must report store_dir: {text}");
    srv.shutdown().await.expect("shutdown");
    std::fs::remove_file(&path).ok();
    std::fs::remove_dir_all(&dir).ok();
}
