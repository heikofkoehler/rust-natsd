//! JWT operator mode integration tests: a real operator/account/user
//! JWT chain (real Ed25519 signatures via the `nkeys` crate, encoded
//! with `rust_natsd::jwt::encode_jwt`) authenticates; every broken
//! link rejects. Keypairs are generated in-memory per test (throwaway
//! test keys, never written to disk, never real credentials).
//!
//! Go interop tests at the bottom spawn the real nats-server binary
//! (path from `NATS_GO_BIN` or the repo's tools copy) and present
//! Rust-generated JWTs to it; they skip gracefully if the binary is
//! absent.

use rust_natsd::config::{Account, AccountUser, Config};
use rust_natsd::jwt::{encode_jwt, now_unix, OperatorTrust};
use rust_natsd::server;
use serde_json::{json, Value};
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

    async fn read_line_timeout(&mut self, ms: u64) -> Option<String> {
        tokio::time::timeout(std::time::Duration::from_millis(ms), self.read_line())
            .await
            .ok()
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

    async fn read_msg(&mut self) -> (String, Option<String>, Vec<u8>) {
        let line = self.read_line().await;
        let parts: Vec<&str> = line.split(' ').collect();
        assert_eq!(parts[0], "MSG", "expected MSG, got: {line}");
        let (subject, reply, size) = if parts.len() == 4 {
            (
                parts[1].to_string(),
                None,
                parts[3].parse::<usize>().unwrap(),
            )
        } else {
            assert_eq!(parts.len(), 5, "MSG shape: {line}");
            (
                parts[1].to_string(),
                Some(parts[3].to_string()),
                parts[4].parse::<usize>().unwrap(),
            )
        };
        let data = self.read_exact_n(size + 2).await;
        (subject, reply, data[..size].to_vec())
    }

    /// Read one HMSG frame, returning (subject, reply, headers, payload).
    async fn read_hmsg(&mut self) -> (String, Option<String>, String, Vec<u8>) {
        let line = self.read_line().await;
        let parts: Vec<&str> = line.split(' ').collect();
        assert_eq!(parts[0], "HMSG", "expected HMSG, got: {line}");
        let (subject, reply, hdr_len, total_len) = if parts.len() == 5 {
            (
                parts[1].to_string(),
                None,
                parts[3].parse::<usize>().unwrap(),
                parts[4].parse::<usize>().unwrap(),
            )
        } else {
            assert_eq!(parts.len(), 6, "HMSG shape: {line}");
            (
                parts[1].to_string(),
                Some(parts[3].to_string()),
                parts[4].parse::<usize>().unwrap(),
                parts[5].parse::<usize>().unwrap(),
            )
        };
        let data = self.read_exact_n(total_len + 2).await;
        let headers = String::from_utf8_lossy(&data[..hdr_len]).into_owned();
        let payload = data[hdr_len..total_len].to_vec();
        (subject, reply, headers, payload)
    }

    async fn send(&mut self, data: &str) {
        self.stream.write_all(data.as_bytes()).await.unwrap();
    }

    async fn info(&mut self) -> Value {
        let line = self.read_line().await;
        assert!(line.starts_with("INFO "), "first line is INFO: {line}");
        serde_json::from_str(&line[5..]).expect("INFO json")
    }

    /// Log in with a user JWT + nonce signature. `signer` signs the
    /// INFO nonce; pass a different keypair (or None for no sig) for
    /// negative tests.
    async fn jwt_login(&mut self, jwt: &str, user_pub: &str, signer: Option<&nkeys::KeyPair>) {
        let info = self.info().await;
        let nonce = info["nonce"].as_str().expect("INFO nonce").to_string();
        assert!(!nonce.is_empty(), "nonce must be non-empty");
        let sig_part = match signer {
            Some(kp) => {
                use base64::Engine;
                let sig = kp.sign(nonce.as_bytes()).unwrap();
                format!(
                    ",\"sig\":\"{}\"",
                    base64::engine::general_purpose::STANDARD.encode(sig)
                )
            }
            None => String::new(),
        };
        self.send(&format!(
            "CONNECT {{\"jwt\":\"{jwt}\",\"nkey\":\"{user_pub}\"{sig_part},\"verbose\":false,\"headers\":true}}\r\n"
        ))
        .await;
    }
}

/// Fixture: operator + two accounts + users, with JWTs.
struct Fixture {
    op: nkeys::KeyPair,
    op_pub: String,
    op_jwt: String,
    acct: nkeys::KeyPair,
    acct_pub: String,
    acct_jwt: String,
    acct2: nkeys::KeyPair,
    acct2_pub: String,
    acct2_jwt: String,
    user: nkeys::KeyPair,
    user_pub: String,
    user_jwt: String,
}

fn operator_jwt(op: &nkeys::KeyPair, system_account: &str) -> String {
    let op_pub = op.public_key();
    encode_jwt(
        &json!({
            "iss": op_pub, "sub": op_pub, "iat": now_unix(),
            "nats": {"type": "operator", "version": 2, "name": "OP",
                     "system_account": system_account}
        }),
        op,
    )
    .unwrap()
}

fn account_jwt(op: &nkeys::KeyPair, acct_pub: &str, name: &str, extra: Value) -> String {
    let op_pub = op.public_key();
    let mut claims = json!({
        "iss": op_pub, "sub": acct_pub, "iat": now_unix(), "name": name,
        "nats": {"type": "account", "version": 2, "name": name}
    });
    let nats = claims["nats"].as_object_mut().unwrap();
    for (k, v) in extra.as_object().unwrap() {
        nats.insert(k.clone(), v.clone());
    }
    encode_jwt(&claims, op).unwrap()
}

fn user_jwt(
    issuer: &nkeys::KeyPair,
    issuer_pub: &str,
    user_pub: &str,
    extra_claims: Option<Value>,
    extra_nats: Option<Value>,
) -> String {
    let mut claims = json!({
        "iss": issuer_pub, "sub": user_pub, "iat": now_unix(), "name": "bob",
        "nats": {"type": "user", "version": 2, "name": "bob",
                 "pub": {"allow": ["allowed.>"]},
                 "sub": {"allow": ["allowed.>"]},
                 "payload": 1048576, "subs": -1, "data": -1}
    });
    if let Some(extra) = extra_claims {
        for (k, v) in extra.as_object().unwrap() {
            claims[k] = v.clone();
        }
    }
    if let Some(extra) = extra_nats {
        let nats = claims["nats"].as_object_mut().unwrap();
        for (k, v) in extra.as_object().unwrap() {
            nats.insert(k.clone(), v.clone());
        }
    }
    encode_jwt(&claims, issuer).unwrap()
}

fn fixture() -> Fixture {
    let op = nkeys::KeyPair::new_operator();
    let acct = nkeys::KeyPair::new_account();
    let acct2 = nkeys::KeyPair::new_account();
    let user = nkeys::KeyPair::new_user();
    let op_pub = op.public_key();
    let acct_pub = acct.public_key();
    let acct2_pub = acct2.public_key();
    let user_pub = user.public_key();
    // Dedicated system account (acct2), as in real deployments: the
    // user account is not the system account.
    let op_jwt = operator_jwt(&op, &acct2_pub);
    // Explicit limits on both accounts (rust-natsd ignores limits;
    // they are present so the Go interop server sees a complete claim).
    let acct_jwt = account_jwt(
        &op,
        &acct_pub,
        "ACCT",
        json!({"limits": {"payload": 1048576, "subs": 1000, "data": -1,
                          "conn": 100, "leaf": -1, "imports": -1, "exports": -1,
                          "wildcards": true}}),
    );
    let acct2_jwt = account_jwt(
        &op,
        &acct2_pub,
        "ACCT2",
        json!({"limits": {"payload": 1048576, "subs": 1000, "data": -1,
                          "conn": 100, "leaf": -1, "imports": -1, "exports": -1,
                          "wildcards": true}}),
    );
    let user_jwt = user_jwt(&acct, &acct_pub, &user_pub, None, None);
    Fixture {
        op,
        op_pub,
        op_jwt,
        acct,
        acct_pub,
        acct_jwt,
        acct2,
        acct2_pub,
        acct2_jwt,
        user,
        user_pub,
        user_jwt,
    }
}

fn jwt_config(fx: &Fixture, extra_accounts: Vec<String>) -> Config {
    let mut jwts = vec![fx.acct_jwt.clone()];
    jwts.extend(extra_accounts);
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        operator: Some(OperatorTrust::from_jwts(&fx.op_jwt, &jwts).unwrap()),
        ..Default::default()
    }
}

#[tokio::test]
async fn jwt_valid_chain_authenticates_and_enforces_claims() {
    let fx = fixture();
    let srv = server::start(jwt_config(&fx, vec![]))
        .await
        .expect("server starts");
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    client.send("SUB allowed.news 1\r\n").await;
    client.send("PUB allowed.news 2\r\nhi\r\n").await;
    let (subject, _, payload) = client.read_msg().await;
    assert_eq!(subject, "allowed.news");
    assert_eq!(payload, b"hi");
    // Publish outside the allow list: permissions violation.
    client.send("PUB denied.news 2\r\nhi\r\n").await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Permissions Violation for Publish"),
        "denied publish: {line}"
    );
    // Subscribe outside the allow list: permissions violation.
    client.send("SUB denied.news 2\r\n").await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Permissions Violation for Subscription"),
        "denied subscribe: {line}"
    );
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_account_default_permissions_fill_missing_axis() {
    let fx = fixture();
    // Account default pub allow dflt.>; user JWT has only a sub claim,
    // so its publish axis must fall back to the account default.
    let acct_jwt = account_jwt(
        &fx.op,
        &fx.acct_pub,
        "ACCT",
        json!({"default_permissions": {"pub": {"allow": ["dflt.>"]}}}),
    );
    let claims = json!({
        "iss": fx.acct_pub, "sub": fx.user_pub, "iat": now_unix(), "name": "bob",
        "nats": {"type": "user", "version": 2, "name": "bob",
                 "sub": {"allow": ["allowed.>"]}}
    });
    let user_jwt = encode_jwt(&claims, &fx.acct).unwrap();
    let cfg = Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        operator: Some(OperatorTrust::from_jwts(&fx.op_jwt, &[acct_jwt]).unwrap()),
        ..Default::default()
    };
    let srv = server::start(cfg).await.expect("server starts");
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    client.send("SUB allowed.news 1\r\n").await;
    // Publish axis falls back to the account default: dflt.news is
    // allowed (no error), other.news is denied.
    client.send("PUB dflt.news 2\r\nhi\r\n").await;
    assert_eq!(
        client.read_line_timeout(500).await,
        None,
        "dflt.news publish must not error"
    );
    client.send("PUB other.news 2\r\nhi\r\n").await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Permissions Violation for Publish"),
        "other.news denial: {line}"
    );
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_expired_is_rejected() {
    let fx = fixture();
    let user_jwt = user_jwt(
        &fx.acct,
        &fx.acct_pub,
        &fx.user_pub,
        Some(json!({"exp": now_unix() - 10})),
        None,
    );
    let srv = server::start(jwt_config(&fx, vec![]))
        .await
        .expect("server starts");
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Authorization Violation'"),
        "expired rejection: {line}"
    );
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_tampered_payload_is_rejected() {
    let fx = fixture();
    // Re-encode the payload with a different name, keep the signature.
    let parts: Vec<&str> = fx.user_jwt.split('.').collect();
    let payload_b64 = parts[1];
    let decoded = {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload_b64)
            .unwrap();
        let mut v: Value = serde_json::from_slice(&bytes).unwrap();
        v["nats"]["name"] = json!("mallory");
        let s = v.to_string();
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s.as_bytes())
    };
    let tampered = format!("{}.{}.{}", parts[0], decoded, parts[2]);
    let srv = server::start(jwt_config(&fx, vec![]))
        .await
        .expect("server starts");
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&tampered, &fx.user_pub, Some(&fx.user))
        .await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Authorization Violation'"),
        "tampered rejection: {line}"
    );
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_wrong_issuer_is_rejected() {
    let fx = fixture();
    // User claim signed by the operator itself (iss = operator key).
    let bad = user_jwt(&fx.op, &fx.op_pub, &fx.user_pub, None, None);
    let srv = server::start(jwt_config(&fx, vec![]))
        .await
        .expect("server starts");
    let mut client = Raw::connect(srv.addr).await;
    client.jwt_login(&bad, &fx.user_pub, Some(&fx.user)).await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Authorization Violation'"),
        "wrong-issuer rejection: {line}"
    );
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_for_untrusted_account_is_rejected() {
    let fx = fixture();
    // Server trusts ACCT only; present a user JWT for ACCT2.
    let other_user = nkeys::KeyPair::new_user();
    let other_jwt = user_jwt(
        &fx.acct2,
        &fx.acct2_pub,
        &other_user.public_key(),
        None,
        None,
    );
    let srv = server::start(jwt_config(&fx, vec![]))
        .await
        .expect("server starts");
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&other_jwt, &other_user.public_key(), Some(&other_user))
        .await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Authorization Violation'"),
        "untrusted-account rejection: {line}"
    );
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_revoked_account_is_rejected() {
    let fx = fixture();
    // Operator trust with NO account JWTs: the account was removed
    // (revoked), so its user JWTs no longer authenticate.
    let cfg = Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        operator: Some(OperatorTrust::from_jwts(&fx.op_jwt, &[]).unwrap()),
        ..Default::default()
    };
    let srv = server::start(cfg).await.expect("server starts");
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Authorization Violation'"),
        "revoked-account rejection: {line}"
    );
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_missing_or_bad_nonce_signature_is_rejected() {
    let fx = fixture();
    let srv = server::start(jwt_config(&fx, vec![]))
        .await
        .expect("server starts");
    // JWT alone, no signature at all.
    let mut client = Raw::connect(srv.addr).await;
    client.jwt_login(&fx.user_jwt, &fx.user_pub, None).await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Authorization Violation'"),
        "jwt-alone rejection: {line}"
    );
    // JWT with a signature from the wrong key (not the user key).
    let other = nkeys::KeyPair::new_user();
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&other))
        .await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Authorization Violation'"),
        "bad-sig rejection: {line}"
    );
    // JWT whose CONNECT nkey does not match the JWT sub.
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&fx.user_jwt, &other.public_key(), Some(&other))
        .await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Authorization Violation'"),
        "nkey-mismatch rejection: {line}"
    );
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_signed_by_account_signing_key_authenticates() {
    let fx = fixture();
    let signing = nkeys::KeyPair::new_account();
    let signing_pub = signing.public_key();
    let acct_jwt = account_jwt(
        &fx.op,
        &fx.acct_pub,
        "ACCT",
        json!({"signing_keys": [signing_pub]}),
    );
    let user_jwt = user_jwt(&signing, &signing_pub, &fx.user_pub, None, None);
    let cfg = Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        operator: Some(OperatorTrust::from_jwts(&fx.op_jwt, &[acct_jwt]).unwrap()),
        ..Default::default()
    };
    let srv = server::start(cfg).await.expect("server starts");
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    client.send("SUB allowed.x 1\r\n").await;
    client.send("PUB allowed.x 2\r\nhi\r\n").await;
    let (subject, _, _) = client.read_msg().await;
    assert_eq!(subject, "allowed.x");
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_service_import_uses_area6_machinery() {
    let fx = fixture();
    // A exports svc.> to B; B imports it as local.>.
    let acct_jwt = account_jwt(
        &fx.op,
        &fx.acct_pub,
        "ACCT",
        json!({"exports": [{"subject": "svc.>", "type": "service",
                            "accounts": [fx.acct2_pub]}]}),
    );
    let acct2_jwt = account_jwt(
        &fx.op,
        &fx.acct2_pub,
        "ACCT2",
        json!({"imports": [{"subject": "svc.>", "type": "service",
                            "account": fx.acct_pub, "to": "local.>"}]}),
    );
    let cfg = Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        operator: Some(OperatorTrust::from_jwts(&fx.op_jwt, &[acct_jwt, acct2_jwt]).unwrap()),
        ..Default::default()
    };
    let srv = server::start(cfg).await.expect("server starts");
    let user_b = nkeys::KeyPair::new_user();
    let user_b_jwt = user_jwt(
        &fx.acct2,
        &fx.acct2_pub,
        &user_b.public_key(),
        None,
        Some(json!({
            "pub": {"allow": ["local.>"]},
            "sub": {"allow": ["_INBOX.>"]}
        })),
    );
    // Alice needs claims for the exported subject and the synthetic
    // import reply subject, not just the fixture's allowed.>.
    let alice_jwt = user_jwt(
        &fx.acct,
        &fx.acct_pub,
        &fx.user_pub,
        None,
        Some(json!({
            "pub": {"allow": ["allowed.>", "_RIMPORT.>"]},
            "sub": {"allow": ["allowed.>", "svc.>"]}
        })),
    );
    let mut alice = Raw::connect(srv.addr).await;
    alice
        .jwt_login(&alice_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    let mut bob = Raw::connect(srv.addr).await;
    bob.jwt_login(&user_b_jwt, &user_b.public_key(), Some(&user_b))
        .await;
    alice.send("SUB svc.echo 1\r\n").await;
    bob.send("SUB _INBOX.bob1 2\r\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    bob.send("PUB local.echo _INBOX.bob1 5\r\nhello\r\n").await;
    // Phase 28C: the translated request is an HMSG carrying
    // Nats-Request-Info naming the importing account (ACCT2).
    let (subject, reply, headers, payload) = alice.read_hmsg().await;
    assert_eq!(subject, "svc.echo");
    assert_eq!(payload, b"hello");
    assert!(
        headers.contains("Nats-Request-Info: {\"acc\":\"ACCT2\"}"),
        "translated request must carry Nats-Request-Info for ACCT2: {headers}"
    );
    let reply = reply.expect("service request carries a reply subject");
    assert!(reply.starts_with("_RIMPORT."), "reply: {reply}");
    alice.send(&format!("PUB {reply} 5\r\nworld\r\n")).await;
    let (subject, _, payload) = bob.read_msg().await;
    assert_eq!(subject, "_INBOX.bob1");
    assert_eq!(payload, b"world");
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_standalone_nkey_still_works_alongside_operator() {
    let fx = fixture();
    let static_kp = nkeys::KeyPair::new_user();
    let static_pub = static_kp.public_key();
    let mut cfg = jwt_config(&fx, vec![]);
    cfg.accounts.push(Account {
        name: "STATIC".into(),
        users: vec![AccountUser {
            nkey: Some(static_pub.clone()),
            ..Default::default()
        }],
        ..Default::default()
    });
    let srv = server::start(cfg).await.expect("server starts");
    // Standalone nkey, no jwt field at all.
    let mut client = Raw::connect(srv.addr).await;
    let info = client.info().await;
    let nonce = info["nonce"].as_str().unwrap().to_string();
    use base64::Engine;
    let sig =
        base64::engine::general_purpose::STANDARD.encode(static_kp.sign(nonce.as_bytes()).unwrap());
    client
        .send(&format!(
            "CONNECT {{\"nkey\":\"{static_pub}\",\"sig\":\"{sig}\",\"verbose\":false,\"headers\":true}}\r\n"
        ))
        .await;
    client.send("SUB static.topic 1\r\n").await;
    client.send("PUB static.topic 2\r\nhi\r\n").await;
    let (subject, _, _) = client.read_msg().await;
    assert_eq!(subject, "static.topic");
    // And the JWT path still works on the same server.
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    client.send("SUB allowed.y 1\r\n").await;
    client.send("PUB allowed.y 2\r\nhi\r\n").await;
    let (subject, _, _) = client.read_msg().await;
    assert_eq!(subject, "allowed.y");
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_conf_operator_and_resolver_preload() {
    let fx = fixture();
    // Go-shaped .conf: inline operator JWT + memory resolver preload.
    let text = format!(
        "host: \"127.0.0.1\"\nport: 0\ncluster {{ port: 0 }}\noperator: \"{}\"\nresolver {{ type: memory }}\nresolver_preload: {{ \"{}\": \"{}\" }}\n",
        fx.op_jwt, fx.acct_pub, fx.acct_jwt
    );
    // The resolver.preload convenience shape parses to the same trust.
    let alt = format!(
        "operator: \"{}\"\nresolver {{ type: memory, preload: {{ \"{}\": \"{}\" }} }}\n",
        fx.op_jwt, fx.acct_pub, fx.acct_jwt
    );
    assert!(rust_natsd::conf::apply_to(&Config::default(), &alt)
        .unwrap()
        .operator
        .is_some());
    let cfg = rust_natsd::conf::apply_to(&Config::default(), &text).unwrap();
    assert!(cfg.operator.is_some());
    let srv = server::start(cfg).await.expect("server starts");
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    client.send("SUB allowed.z 1\r\n").await;
    client.send("PUB allowed.z 2\r\nhi\r\n").await;
    let (subject, _, _) = client.read_msg().await;
    assert_eq!(subject, "allowed.z");
    srv.shutdown().await.expect("shutdown");

    // jwt_file variant: operator block with jwt_file + accounts list.
    let dir = std::env::temp_dir();
    let tag = std::process::id();
    let op_path = dir.join(format!("jwt-op-{tag}.jwt"));
    let acct_path = dir.join(format!("jwt-acct-{tag}.jwt"));
    std::fs::write(&op_path, format!("{}\n", fx.op_jwt)).unwrap();
    std::fs::write(&acct_path, &fx.acct_jwt).unwrap();
    let text = format!(
        "host: \"127.0.0.1\"\nport: 0\ncluster {{ port: 0 }}\noperator {{\n  jwt_file: \"{}\"\n  accounts: [{{ jwt_file: \"{}\" }}]\n}}\n",
        op_path.display(),
        acct_path.display()
    );
    let cfg = rust_natsd::conf::apply_to(&Config::default(), &text).unwrap();
    assert!(cfg.operator.is_some());
    let srv = server::start(cfg).await.expect("server starts");
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    client.send("SUB allowed.z 1\r\n").await;
    client.send("PUB allowed.z 2\r\nhi\r\n").await;
    let (subject, _, _) = client.read_msg().await;
    assert_eq!(subject, "allowed.z");
    srv.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_file(&op_path);
    let _ = std::fs::remove_file(&acct_path);

    // Bad operator JWT is a config error, not a half-trusted server.
    let bad = "port: 0\noperator: \"not.a.jwt\"\n".to_string();
    assert!(rust_natsd::conf::apply_to(&Config::default(), &bad).is_err());
}

// ---- Go interop ----

fn go_bin() -> Option<String> {
    if let Ok(p) = std::env::var("NATS_GO_BIN") {
        if std::path::Path::new(&p).exists() {
            return Some(p);
        }
    }
    let home = std::env::var("HOME").ok()?;
    let p = format!("{home}/workspace/tools/nats-server-v2.15.1-linux-amd64/nats-server");
    if std::path::Path::new(&p).exists() {
        Some(p)
    } else {
        None
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct GoServer {
    child: std::process::Child,
    addr: std::net::SocketAddr,
    log: String,
    // Held for the server's lifetime: Go tests run in parallel, and
    // a freed port can otherwise be re-picked by another test before
    // its Go server binds, cross-connecting clients to the wrong
    // fixture's server.
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

async fn go_lock() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

impl Drop for GoServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn start_go(fx: &Fixture, _user_jwt: &str) -> Option<GoServer> {
    let bin = go_bin()?;
    let guard = go_lock().await;
    let port = free_port();
    let dir = std::env::temp_dir();
    let tag = std::process::id();
    let conf_path = dir.join(format!("go-jwt-{tag}.conf"));
    let log_path = dir.join(format!("go-jwt-{tag}.log"));
    let text = format!(
        "port: {port}\nhttp_port: 0\noperator: \"{}\"\nsystem_account: \"{}\"\nresolver {{\n  type: memory\n}}\nresolver_preload: {{\n  \"{}\": \"{}\",\n  \"{}\": \"{}\"\n}}\n",
        fx.op_jwt, fx.acct2_pub, fx.acct_pub, fx.acct_jwt, fx.acct2_pub, fx.acct2_jwt
    );
    std::fs::write(&conf_path, &text).unwrap();
    let log_file = std::fs::File::create(&log_path).unwrap();
    let mut child = std::process::Command::new(&bin)
        .arg("-c")
        .arg(&conf_path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(log_file))
        .spawn()
        .ok()?;
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut up = false;
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            up = true;
            break;
        }
        if let Ok(Some(_)) = child.try_wait() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    if !up {
        let log = std::fs::read_to_string(&log_path).unwrap_or_default();
        let _ = child.kill();
        panic!("Go server failed to start: {log}");
    }
    let log = log_path.display().to_string();
    let _ = std::fs::remove_file(&conf_path);
    Some(GoServer {
        child,
        addr,
        log,
        _guard: guard,
    })
}

#[tokio::test]
async fn go_accepts_rust_generated_jwt() {
    let fx = fixture();
    if go_bin().is_none() {
        eprintln!("SKIP: Go nats-server binary not found");
        return;
    }
    let Some(go) = start_go(&fx, &fx.user_jwt).await else {
        panic!("Go server did not start");
    };
    let mut client = Raw::connect(go.addr).await;
    client
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    client.send("SUB allowed.go 1\r\n").await;
    client.send("PUB allowed.go 2\r\nhi\r\n").await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("MSG allowed.go 1 2"),
        "Go delivery: {line}; Go log: {}",
        std::fs::read_to_string(&go.log).unwrap_or_default()
    );
    drop(go);
}

#[tokio::test]
async fn go_rejects_expired_rust_generated_jwt() {
    let fx = fixture();
    let expired = user_jwt(
        &fx.acct,
        &fx.acct_pub,
        &fx.user_pub,
        Some(json!({"exp": now_unix() - 5})),
        None,
    );
    if go_bin().is_none() {
        eprintln!("SKIP: Go nats-server binary not found");
        return;
    }
    let Some(go) = start_go(&fx, &expired).await else {
        panic!("Go server did not start");
    };
    let mut client = Raw::connect(go.addr).await;
    client
        .jwt_login(&expired, &fx.user_pub, Some(&fx.user))
        .await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR"),
        "Go expired rejection: {line}; Go log: {}",
        std::fs::read_to_string(&go.log).unwrap_or_default()
    );
    drop(go);
}

// Go-client shape (jwt + nonce signature, no nkey field): nats.go's
// UserJWT never sends a CONNECT `nkey` field (UserJWT and Nkey are
// mutually exclusive there); the server derives the user key from
// the JWT `sub` claim, as Go's server does (Phase 28B).
#[tokio::test]
async fn jwt_login_without_nkey_field_authenticates() {
    let fx = fixture();
    let srv = server::start(jwt_config(&fx, vec![]))
        .await
        .expect("server starts");
    let mut client = Raw::connect(srv.addr).await;
    let info = client.info().await;
    let nonce = info["nonce"].as_str().expect("INFO nonce").to_string();
    use base64::Engine;
    let sig = fx.user.sign(nonce.as_bytes()).unwrap();
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig);
    // Deliberately no "nkey" field, exactly as nats.go UserJWT sends.
    client
        .send(&format!(
            "CONNECT {{\"jwt\":\"{}\",\"sig\":\"{sig_b64}\",\"verbose\":false,\"headers\":true}}\r\nPING\r\n",
            fx.user_jwt
        ))
        .await;
    assert_eq!(client.read_line().await, "PONG");
    client.send("SUB allowed.z 1\r\n").await;
    client.send("PUB allowed.z 2\r\nhi\r\n").await;
    let (subject, _, payload) = client.read_msg().await;
    assert_eq!(subject, "allowed.z");
    assert_eq!(payload, b"hi");
    srv.shutdown().await.expect("shutdown");
}

/// Build a server trusting one account JWT with the given extra
/// `nats` fields (limits, revocations, ...).
async fn server_with_account_extra(fx: &Fixture, extra: Value) -> server::RunningServer {
    let acct_jwt = account_jwt(&fx.op, &fx.acct_pub, "ACCT", extra);
    let cfg = Config {
        host: "127.0.0.1".into(),
        port: 0,
        cluster_port: 0,
        operator: Some(OperatorTrust::from_jwts(&fx.op_jwt, &[acct_jwt]).unwrap()),
        ..Default::default()
    };
    server::start(cfg).await.expect("server starts")
}

fn full_limits() -> Value {
    json!({"limits": {"payload": 1048576, "subs": 1000, "data": -1,
                      "conn": 100, "leaf": -1, "imports": -1, "exports": -1,
                      "wildcards": true}})
}

#[tokio::test]
async fn jwt_account_revocation_rejects_and_past_revocation_allows() {
    let fx = fixture();
    // Revocation timestamp in the future: the user JWT (issued now)
    // is revoked.
    let mut extra = full_limits();
    extra.as_object_mut().unwrap().insert(
        "revocations".into(),
        serde_json::json!({fx.user_pub.clone(): now_unix() + 3600}),
    );
    let srv = server_with_account_extra(&fx, extra).await;
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Authorization Violation'"),
        "revoked user rejection: {line}"
    );
    srv.shutdown().await.expect("shutdown");

    // Revocation timestamp in the past: the user JWT was issued
    // after it, so it still authenticates.
    let mut extra = full_limits();
    extra.as_object_mut().unwrap().insert(
        "revocations".into(),
        serde_json::json!({fx.user_pub.clone(): now_unix() - 3600}),
    );
    let srv = server_with_account_extra(&fx, extra).await;
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    client.send("PING\r\n").await;
    assert_eq!(client.read_line().await, "PONG");
    srv.shutdown().await.expect("shutdown");

    // A "*" revocation revokes every user of the account.
    let mut extra = full_limits();
    extra.as_object_mut().unwrap().insert(
        "revocations".into(),
        serde_json::json!({"*": now_unix() + 3600}),
    );
    let srv = server_with_account_extra(&fx, extra).await;
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Authorization Violation'"),
        "star revocation rejection: {line}"
    );
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_bearer_token_authenticates_without_nonce_signature() {
    let fx = fixture();
    // Bearer user JWT, no nonce signature at all: the JWT itself is
    // the credential (Go's IsBearerToken).
    let bearer_jwt = user_jwt(
        &fx.acct,
        &fx.acct_pub,
        &fx.user_pub,
        None,
        Some(json!({"bearer_token": true})),
    );
    let srv = server_with_account_extra(&fx, full_limits()).await;
    let mut client = Raw::connect(srv.addr).await;
    client.jwt_login(&bearer_jwt, &fx.user_pub, None).await;
    client.send("SUB allowed.x 1\r\n").await;
    client.send("PUB allowed.x 2\r\nhi\r\n").await;
    let (subject, _, payload) = client.read_msg().await;
    assert_eq!(subject, "allowed.x");
    assert_eq!(payload, b"hi");
    srv.shutdown().await.expect("shutdown");

    // The same bearer JWT against an account with disallow_bearer
    // is rejected.
    let mut extra = full_limits();
    extra["limits"]["disallow_bearer"] = json!(true);
    let srv = server_with_account_extra(&fx, extra).await;
    let mut client = Raw::connect(srv.addr).await;
    client.jwt_login(&bearer_jwt, &fx.user_pub, None).await;
    let line = client.read_line().await;
    assert!(
        line.starts_with("-ERR 'Authorization Violation'"),
        "disallow_bearer rejection: {line}"
    );
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_account_conn_limit_rejects_second_connection() {
    let fx = fixture();
    let extra = json!({"limits": {"payload": 1048576, "subs": 1000, "data": -1,
                                  "conn": 1}});
    let srv = server_with_account_extra(&fx, extra).await;
    let mut first = Raw::connect(srv.addr).await;
    first
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    first.send("PING\r\n").await;
    assert_eq!(first.read_line().await, "PONG");
    let mut second = Raw::connect(srv.addr).await;
    second
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    let line = second.read_line().await;
    assert!(
        line.contains("maximum account active connections exceeded"),
        "conn limit rejection: {line}"
    );
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_account_subs_limit_rejects_extra_subscription() {
    let fx = fixture();
    let extra = json!({"limits": {"payload": 1048576, "subs": 1, "data": -1,
                                  "conn": 100}});
    let srv = server_with_account_extra(&fx, extra).await;
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    client.send("SUB allowed.a 1\r\n").await;
    client.send("SUB allowed.b 2\r\n").await;
    let line = client.read_line().await;
    assert!(
        line.contains("maximum subscriptions exceeded"),
        "subs limit rejection: {line}"
    );
    // Connection stays alive after the rejection (Go behaviour).
    client.send("PING\r\n").await;
    assert_eq!(client.read_line().await, "PONG");
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_account_payload_limit_rejects_oversized_publish() {
    let fx = fixture();
    let extra = json!({"limits": {"payload": 5, "subs": 1000, "data": -1,
                                  "conn": 100}});
    let srv = server_with_account_extra(&fx, extra).await;
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    client.send("SUB allowed.x 1\r\n").await;
    client.send("PUB allowed.x 2\r\nhi\r\n").await;
    let (subject, _, payload) = client.read_msg().await;
    assert_eq!(subject, "allowed.x");
    assert_eq!(payload, b"hi");
    // 7 bytes over a 5-byte limit: Go's exact error, connection closes.
    client.send("PUB allowed.x 7\r\ntoolong\r\n").await;
    let line = client.read_line().await;
    assert!(
        line.contains("Maximum Payload Violation"),
        "payload limit rejection: {line}"
    );
    srv.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn jwt_account_data_limit_drops_over_budget_publish() {
    let fx = fixture();
    // Cumulative inbound budget of 10 bytes for the account.
    let extra = json!({"limits": {"payload": 1048576, "subs": 1000, "data": 10,
                                  "conn": 100}});
    let srv = server_with_account_extra(&fx, extra).await;
    let mut client = Raw::connect(srv.addr).await;
    client
        .jwt_login(&fx.user_jwt, &fx.user_pub, Some(&fx.user))
        .await;
    client.send("SUB allowed.x 1\r\n").await;
    for payload in ["12345", "12345"] {
        client
            .send(&format!("PUB allowed.x 5\r\n{payload}\r\n"))
            .await;
        let (subject, _, got) = client.read_msg().await;
        assert_eq!(subject, "allowed.x");
        assert_eq!(got, payload.as_bytes());
    }
    // Budget exhausted (10/10): the next byte is dropped with an
    // error, and the connection stays alive.
    client.send("PUB allowed.x 1\r\nx\r\n").await;
    let line = client.read_line().await;
    assert!(
        line.contains("Maximum Data Exceeded"),
        "data limit rejection: {line}"
    );
    client.send("PING\r\n").await;
    assert_eq!(client.read_line().await, "PONG");
    srv.shutdown().await.expect("shutdown");
}
