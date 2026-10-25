//! Unit-style tests for pure logic that previously had no coverage:
//! subject-matching edge cases, parser details, config helpers,
//! .conf parsing branches, JWT claim-validation branches, and
//! TLS cert-identity candidate ordering. These use only public APIs
//! (no server behaviour is exercised here).

use rust_natsd::config::{parse_duration, parse_route_url, parse_size, split_list, Config, Permissions};
use rust_natsd::conf;
use rust_natsd::jwt::{encode_jwt, permissions_from_nats, verify_jwt};
use rust_natsd::protocol::{ClientOp, Parser};
use rust_natsd::subject::{has_empty_token, is_valid_publish_subject, is_valid_subscribe_subject, pattern_covers, subject_matches, translate_subject};
use rust_natsd::tls::CertIdentity;
use serde_json::json;

// --- subject matching edges (beyond the existing truth table) ---

#[test]
fn subject_matches_literal_partial_wildcards() {
    // Partial wildcards are literal tokens, not wildcards.
    assert!(subject_matches("foo*", "foo*"));
    assert!(!subject_matches("foo*", "foobar"));
    assert!(!subject_matches("foo*", "foo.bar"));
    assert!(subject_matches("f>o", "f>o"));
    assert!(!subject_matches("f>o", "foo"));
    assert!(!subject_matches("f>o", "f.bar.o"));
}

#[test]
fn subject_matches_empty_and_root_cases() {
    // Empty subjects never reach the matcher: publish validation
    // rejects them, and the routing layer drops empty-token subjects
    // before matching. Assert that boundary, not matcher behaviour
    // on inputs that cannot arrive.
    assert!(!is_valid_publish_subject(""));
    assert!(has_empty_token(""));
    assert!(has_empty_token("foo..bar"));
    assert!(!subject_matches("foo", ""));
    assert!(subject_matches(">", "a.b.c"));
}

#[test]
fn subject_validation_rejects_wildcard_in_wrong_place() {
    assert!(!is_valid_publish_subject("foo.>"));
    assert!(!is_valid_publish_subject("foo.*"));
    assert!(is_valid_subscribe_subject("foo.>"));
    assert!(!is_valid_subscribe_subject(">.foo"));
    assert!(!is_valid_subscribe_subject("foo.*.>.bar"));
    assert!(is_valid_subscribe_subject("foo.*.>"));
}

#[test]
fn pattern_covers_partial_and_full_wildcards() {
    assert!(pattern_covers("svc.>", "svc.*"));
    assert!(pattern_covers("svc.*", "svc.echo"));
    assert!(!pattern_covers("svc.*", "svc.echo.more"));
    assert!(pattern_covers(">", "anything.at.all"));
    assert!(!pattern_covers("svc.*", "other.echo"));
    // A literal partial wildcard covers only itself.
    assert!(pattern_covers("foo*", "foo*"));
    assert!(!pattern_covers("foo*", "foobar"));
}

#[test]
fn translate_subject_mismatch_and_star() {
    assert_eq!(
        translate_subject("a.*.c", "b.*.d", "a.x.c"),
        Some("b.x.d".to_string())
    );
    assert_eq!(translate_subject("a.*.c", "b.*.d", "a.x.y"), None);
    assert_eq!(translate_subject("a.>", "b.>", "a"), None);
    assert_eq!(
        translate_subject("a.>", "b.>", "a.x.y"),
        Some("b.x.y".to_string())
    );
}

// --- protocol parser details ---

fn parse_all(data: &[u8]) -> Vec<ClientOp> {
    let mut p = Parser::new(1024 * 1024);
    p.feed(data);
    let mut out = Vec::new();
    while let Ok(Some(op)) = p.next_op() {
        out.push(op);
    }
    out
}

#[test]
fn parser_connect_unknown_fields_ignored_and_flags_parsed() {
    let ops = parse_all(
        b"CONNECT {\"verbose\":true,\"pedantic\":true,\"unknown_field\":123,\"name\":\"x\"}\r\n",
    );
    match &ops[0] {
        ClientOp::Connect(info) => {
            assert!(info.verbose);
            assert!(info.pedantic);
            assert_eq!(info.name, "x");
        }
        other => panic!("expected Connect, got {other:?}"),
    }
}

#[test]
fn parser_ping_pong_and_unsub_forms() {
    let ops = parse_all(b"PING\r\nPONG\r\nUNSUB 3\r\nUNSUB 3 5\r\n");
    assert!(matches!(ops[0], ClientOp::Ping));
    assert!(matches!(ops[1], ClientOp::Pong));
    assert!(matches!(&ops[2], ClientOp::Unsub { sid, max: None } if sid == "3"));
    assert!(matches!(&ops[3], ClientOp::Unsub { sid, max: Some(5) } if sid == "3"));
}

#[test]
fn parser_payload_split_across_many_feeds() {
    let mut p = Parser::new(1024 * 1024);
    // Feed one byte at a time, including inside the payload.
    for b in b"PUB split.me 5\r\nhello\r\n" {
        p.feed(&[*b]);
    }
    match p.next_op().unwrap().unwrap() {
        ClientOp::Pub { subject, payload, .. } => {
            assert_eq!(subject, "split.me");
            assert_eq!(payload.as_ref(), b"hello");
        }
        other => panic!("expected Pub, got {other:?}"),
    }
}

#[test]
fn parser_hpub_header_payload_split() {
    // Header block "NATS/1.0\r\nK: v\r\n\r\n" is 18 bytes; total 21.
    let hdrs = b"NATS/1.0\r\nK: v\r\n\r\n";
    let mut p = Parser::new(1024 * 1024);
    p.feed(b"HPUB h.sub 18 21\r\n");
    p.feed(hdrs);
    p.feed(b"abc\r\n");
    match p.next_op().unwrap().unwrap() {
        ClientOp::HPub { subject, headers, payload, .. } => {
            assert_eq!(subject, "h.sub");
            assert_eq!(headers.as_ref(), hdrs);
            assert_eq!(payload.as_ref(), b"abc");
        }
        other => panic!("expected HPub, got {other:?}"),
    }
}

// --- config helpers ---

#[test]
fn config_parse_size_variants() {
    assert_eq!(parse_size("1024").unwrap(), 1024);
    assert_eq!(parse_size("1k").unwrap(), 1024);
    assert_eq!(parse_size("1KB").unwrap(), 1024);
    assert_eq!(parse_size("2m").unwrap(), 2 * 1024 * 1024);
    assert_eq!(parse_size("1MB").unwrap(), 1024 * 1024);
    assert!(parse_size("abc").is_err());
    assert!(parse_size("1g").is_err());
    assert!(parse_size("").is_err());
}

#[test]
fn config_parse_duration_variants() {
    use std::time::Duration;
    assert_eq!(parse_duration("200ms").unwrap(), Duration::from_millis(200));
    assert_eq!(parse_duration("2s").unwrap(), Duration::from_secs(2));
    assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
    // A bare number is milliseconds (Go .conf convention used here).
    assert_eq!(parse_duration("200").unwrap(), Duration::from_millis(200));
    assert_eq!(parse_duration("1.5s").unwrap(), Duration::from_millis(1500));
    assert!(parse_duration("").is_err());
    assert!(parse_duration("soon").is_err());
    assert!(parse_duration("-5s").is_err());
}

#[test]
fn config_parse_route_url_variants() {
    assert_eq!(
        parse_route_url("nats://127.0.0.1:6222").unwrap(),
        ("127.0.0.1".to_string(), 6222)
    );
    assert_eq!(
        parse_route_url("nats-route://example.com:6222").unwrap(),
        ("example.com".to_string(), 6222)
    );
    assert_eq!(
        parse_route_url("127.0.0.1:4222").unwrap(),
        ("127.0.0.1".to_string(), 4222)
    );
    assert!(parse_route_url("127.0.0.1").is_err());
    assert!(parse_route_url("nats://127.0.0.1:notaport").is_err());
    assert_eq!(split_list("a, b,, c "), vec!["a", "b", "c"]);
}

#[test]
fn permissions_deny_beats_allow_and_fallback_fills() {
    let p = Permissions {
        publish_allow: Some(vec!["pub.>".into()]),
        publish_deny: Some(vec!["pub.secret".into()]),
        subscribe_allow: None,
        subscribe_deny: None,
    };
    assert!(p.can_publish("pub.news"));
    assert!(!p.can_publish("pub.secret"));
    assert!(!p.can_publish("other.news"));
    // No allow list at all means allow-all (deny still applies).
    let open = Permissions::default();
    assert!(open.can_publish("anything"));
    assert!(open.can_subscribe("anything"));

    let user = Permissions {
        publish_allow: None,
        publish_deny: Some(vec!["x".into()]),
        subscribe_allow: None,
        subscribe_deny: None,
    };
    let account = Permissions {
        publish_allow: Some(vec!["a.>".into()]),
        publish_deny: None,
        subscribe_allow: Some(vec!["s.>".into()]),
        subscribe_deny: None,
    };
    let merged = user.fallback(&account);
    // User's own deny survives; missing allow fills from the account.
    assert_eq!(merged.publish_allow, Some(vec!["a.>".into()]));
    assert_eq!(merged.publish_deny, Some(vec!["x".into()]));
    assert_eq!(merged.subscribe_allow, Some(vec!["s.>".into()]));
}

// --- .conf parsing branches ---

#[test]
fn conf_unknown_keys_ignored_everywhere() {
    let cfg = conf::apply_to(
        &Config::default(),
        "port: 0\nfuture_option: 42\nweird_block {\n  nested: true\n}\nauthorization {\n  user: alice\n  password: pw\n  unknown_inside: 1\n}\n",
    )
    .expect("unknown keys must be ignored");
    assert_eq!(cfg.user.as_deref(), Some("alice"));
    assert_eq!(cfg.pass.as_deref(), Some("pw"));
}

#[test]
fn conf_malformed_inputs_error() {
    assert!(conf::apply_to(&Config::default(), "port: \"unterminated").is_err());
    assert!(conf::apply_to(&Config::default(), "cluster {\n  port: 1\n").is_err());
    assert!(conf::apply_to(&Config::default(), "port: [1, 2").is_err());
    assert!(conf::apply_to(&Config::default(), "port: notaport").is_err());
    assert!(conf::apply_to(&Config::default(), "ping_interval: bogus").is_err());
}

#[test]
fn conf_size_duration_and_identity_keys() {
    let cfg = conf::apply_to(
        &Config::default(),
        "server_name: unit-srv\nhttp_port: 0\nmax_payload: 2MB\nmax_pending: 1MB\nping_interval: 200ms\nping_max: 3\nsystem_account: $SYS\nno_auth_user: guest\n",
    )
    .expect("conf parses");
    assert_eq!(cfg.name, "unit-srv");
    assert_eq!(cfg.max_payload, 2 * 1024 * 1024);
    assert_eq!(cfg.max_pending, 1024 * 1024);
    assert_eq!(cfg.ping_interval, std::time::Duration::from_millis(200));
    assert_eq!(cfg.ping_max, 3);
    assert_eq!(cfg.system_account.as_deref(), Some("$SYS"));
    assert_eq!(cfg.no_auth_user.as_deref(), Some("guest"));
}

#[test]
fn conf_jetstream_store_dir_and_cluster_name() {
    let cfg = conf::apply_to(
        &Config::default(),
        "jetstream {\n  store_dir: \"/tmp/js-unit\"\n}\ncluster {\n  name: unit-cluster\n  port: 0\n}\n",
    )
    .expect("conf parses");
    assert!(cfg.jetstream);
    assert_eq!(cfg.js_store_dir.as_deref(), Some("/tmp/js-unit"));
    assert_eq!(cfg.cluster_name.as_deref(), Some("unit-cluster"));
}

// --- JWT claim-validation branches ---

fn jwt_token(claims: serde_json::Value, signer: &nkeys::KeyPair) -> String {
    encode_jwt(&claims, signer).expect("encode jwt")
}

fn jwt_claims(iss: &str, sub: &str, extra: serde_json::Value) -> serde_json::Value {
    let mut claims = json!({
        "iss": iss,
        "sub": sub,
        "nats": {"type": "user"},
    });
    if let Some(obj) = extra.as_object() {
        for (k, v) in obj {
            claims[k] = v.clone();
        }
    }
    claims
}

#[test]
fn jwt_verify_time_branches() {
    let op = nkeys::KeyPair::new_operator();
    let user = nkeys::KeyPair::new_user();
    let op_pub = op.public_key();
    let user_pub = user.public_key();
    let now = 1_700_000_000i64;

    // Missing exp is accepted (Go behaviour for operator/account/user JWTs).
    let tok = jwt_token(jwt_claims(&op_pub, &user_pub, json!({})), &op);
    assert!(verify_jwt(&tok, &op_pub, now).is_ok());

    // exp in the past rejects; exp in the future accepts.
    let tok = jwt_token(jwt_claims(&op_pub, &user_pub, json!({"exp": now - 1})), &op);
    assert!(verify_jwt(&tok, &op_pub, now).is_err());
    let tok = jwt_token(jwt_claims(&op_pub, &user_pub, json!({"exp": now + 3600})), &op);
    assert!(verify_jwt(&tok, &op_pub, now).is_ok());

    // nbf in the future rejects.
    let tok = jwt_token(jwt_claims(&op_pub, &user_pub, json!({"nbf": now + 60})), &op);
    assert!(verify_jwt(&tok, &op_pub, now).is_err());

    // iat far in the future rejects; modest skew is tolerated.
    let tok = jwt_token(jwt_claims(&op_pub, &user_pub, json!({"iat": now + 3600})), &op);
    assert!(verify_jwt(&tok, &op_pub, now).is_err());
    let tok = jwt_token(jwt_claims(&op_pub, &user_pub, json!({"iat": now + 30})), &op);
    assert!(verify_jwt(&tok, &op_pub, now).is_ok());
}

#[test]
fn jwt_verify_shape_branches() {
    let op = nkeys::KeyPair::new_operator();
    let other = nkeys::KeyPair::new_operator();
    let user = nkeys::KeyPair::new_user();
    let op_pub = op.public_key();
    let user_pub = user.public_key();
    let now = 1_700_000_000i64;

    // Malformed: not three parts, bad base64 payload.
    assert!(verify_jwt("onlyonepart", &op_pub, now).is_err());
    assert!(verify_jwt("a.b", &op_pub, now).is_err());
    assert!(verify_jwt("a.!!!.c", &op_pub, now).is_err());

    // Wrong typ / wrong alg.
    let bad_typ = {
        let header = "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ"; // typ JWT ok, replaced below
        let _ = header;
        // Build via encode then swap the header segment for typ "FOO".
        let tok = jwt_token(jwt_claims(&op_pub, &user_pub, json!({})), &op);
        let parts: Vec<&str> = tok.split('.').collect();
        let foo_header = base64_url(b"{\"typ\":\"FOO\",\"alg\":\"ed25519-nkey\"}");
        format!("{}.{}.{}", foo_header, parts[1], parts[2])
    };
    assert!(verify_jwt(&bad_typ, &op_pub, now).is_err());

    // Missing nats object rejects.
    let mut claims = jwt_claims(&op_pub, &user_pub, json!({}));
    claims.as_object_mut().unwrap().remove("nats");
    let tok = jwt_token(claims, &op);
    assert!(verify_jwt(&tok, &op_pub, now).is_err());

    // Signed by a different operator rejects (issuer mismatch is checked
    // before signature, and the signature would not verify either).
    let tok = jwt_token(jwt_claims(&op_pub, &user_pub, json!({})), &other);
    assert!(verify_jwt(&tok, &op_pub, now).is_err());
}

fn base64_url(data: &[u8]) -> String {
    // Minimal base64url without padding, for test header construction.
    const ALPH: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(ALPH[(n >> 18) as usize & 63] as char);
        out.push(ALPH[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(ALPH[(n >> 6) as usize & 63] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPH[n as usize & 63] as char);
        }
    }
    out
}

#[test]
fn jwt_permissions_from_nats_shapes() {
    // Bare string counts as a one-element allow list.
    let p = permissions_from_nats(&json!({"pub": "news.>", "sub": {"allow": ["a"], "deny": ["a.secret"]}}));
    assert!(p.can_publish("news.today"));
    assert!(!p.can_publish("other"));
    assert!(p.can_subscribe("a"));
    assert!(!p.can_subscribe("a.secret"));
    // Missing axes mean unrestricted.
    let p = permissions_from_nats(&json!({}));
    assert!(p.can_publish("x"));
    assert!(p.can_subscribe("x"));
}

// --- TLS cert identity candidate ordering (pure logic; DER parsing
// itself is covered by the e2e cert-mapping tests with real certs) ---

#[test]
fn tls_cert_identity_candidates_order() {
    let id = CertIdentity {
        email_sans: vec!["user@example.com".into()],
        dns_sans: vec!["host.example.com".into()],
        uri_sans: vec![],
        cn: Some("alice".into()),
        dn: "CN=alice".into(),
    };
    assert_eq!(
        id.candidates(),
        vec!["user@example.com", "host.example.com", "alice", "CN=alice"]
    );
    let bare = CertIdentity {
        email_sans: vec![],
        dns_sans: vec![],
        uri_sans: vec![],
        cn: None,
        dn: String::new(),
    };
    assert!(bare.candidates().is_empty());
}
