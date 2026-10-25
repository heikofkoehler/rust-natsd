//! Gateway (supercluster) wire helpers, verified against Go
//! nats-server v2.15.1 (Phase 24 probe, re-probed in Phase 33).
//!
//! Handshake (gateway port, INFO-first — unlike routes, where the
//! dialer sends CONNECT first):
//!   accepter: INFO {server_id, server_name, version, proto: 3,
//!     host, port (gateway port), headers: true, max_payload,
//!     gateway: "<name>", gateway_urls: ["<host>:<port>"],
//!     gateway_url, gateway_nrp: true, gateway_iom: true}
//!   dialer:   CONNECT {echo: false, verbose: false, pedantic: false,
//!     tls_required: false, headers: false, name: "<dialer server_id>",
//!     cluster: "", gateway: "<dialer gateway name>"}
//!   dialer:   INFO (same shape as the accepter's, its own values)
//!   accepter: INFO {gateway, gateway_cmd: 2,
//!     gateway_cmd_payload: base64(account)} once per account, then
//!     RS+ interest, then INFO {gateway, gateway_cmd: 3, ...} once
//!     per account.
//!
//! Cmd semantics (probed fact vs inference, kept separate): the cmd
//! numbers and base64 account payloads are probed fact, as is the
//! ordering (cmd 2 per account, then interest, then cmd 3 per
//! account). The reading of cmd 2 as "announce account" and cmd 3
//! as "switch this account to interest-only mode" is the plausible
//! interpretation of that ordering plus Go's `-DV` log line
//! (`switching account "X" to Interest-Only mode` on cmd 3); it is
//! NOT source-verified. This implementation therefore treats cmd 3
//! as a marker only: receiving it records the account as
//! interest-only but changes no forwarding behaviour, because all
//! forwarding here is already interest-driven (RS+/RS-). That is
//! the conservative reading — nothing is sent on a gateway
//! connection without matching RS+ interest, so interest-only mode
//! cannot leak traffic even if the inference is wrong.
//!
//! Interest and messages are route framing, verbatim (Phase 24):
//!   RS+ <account> <subject> [queue weight]
//!   RS- <account> <subject> [queue]
//!   RMSG <account> <subject> [reply] [| queues] <size>
//!   HMSG <account> <subject> [reply] <hdr_len> <total_len>
//! There is no gateway-specific parser: `crate::route::RouteParser`
//! parses these, and gateway INFO frames arrive as
//! `RouteOp::Info` (their `gateway_cmd` fields are read here).
//!
//! Directional rule (probed against Go 2.15.1, and fatal when
//! violated): on one gateway connection, interest flows exactly one
//! way and messages exactly the other way. The ACCEPTER sends RS+/
//! RS- (its subscriptions) to the dialer; the DIALER sends RMSG/
//! HMSG (messages) back to the accepter. A dialer that sends RS+
//! gets its connection closed by Go with
//! `Received RS+ from gateway on inbound connection` / `Protocol
//! Violation` — this is what killed rust-natsd's solicited
//! connection in the first Go-interop run, and why the dialer side
//! here never sends RS+ (neither the initial burst nor later
//! propagations). Both traffic directions work only because the
//! supercluster forms one connection per direction (the implicit
//! back-connect): each side is accepter on one connection and
//! dialer on the other, so each side's subscriptions reach the
//! other over the connection the other side accepted.
//!
//! Loop prevention (enforced in `delivery::deliver`): a message
//! arriving on a gateway is delivered locally only — never back to
//! any gateway (and never to routes/leafs, which would duplicate it
//! on nodes that hold their own gateway connections to the same
//! peer cluster). A local message is forwarded to gateways with
//! matching interest, with global queue selection across local,
//! route, leaf, and gateway destinations, so a queue group spanning
//! a gateway still delivers exactly once.
//!
//! Gateway TLS (probed in Phase 33 against Go 2.15.1): implicit TLS,
//! like routes — the server handshake comes first, no plaintext
//! INFO is ever sent on a TLS gateway port, and the dialer carries
//! its TLS block in the gateways entry (`tls: { cert_file, key_file,
//! ca_file }`). Go's TLS gateway server requires a CA-verified
//! client certificate: a dialer that presents no certificate (or an
//! unverifiable one) fails the handshake even when the server block
//! says `verify: false`.

use base64::Engine;

/// Accounts announced to a gateway peer, in announcement order.
/// `$SYS` first (Go announces it first), then configured accounts in
/// config order, then `$G` (the legacy account) if not already named.
/// Go announces every account it knows, including accounts the peer
/// may not have; interest for an account the peer does not know is
/// simply never matched on its side, and the account token on every
/// frame is what keeps the boundary.
pub fn gateway_accounts(config: &crate::config::Config) -> Vec<String> {
    let mut out = vec!["$SYS".to_string()];
    for account in config.all_accounts() {
        if !out.contains(&account.name) {
            out.push(account.name.clone());
        }
    }
    if !out.contains(&"$G".to_string()) {
        out.push("$G".to_string());
    }
    out
}

/// The gateway INFO JSON this server sends (accepter first frame,
/// and the dialer's INFO after CONNECT), in Go's probed shape.
pub fn gateway_info_json(
    server_id: &str,
    server_name: &str,
    host: &str,
    port: u16,
    max_payload: usize,
    gateway_name: &str,
    gateway_urls: &[String],
) -> String {
    serde_json::json!({
        "server_id": server_id,
        "server_name": server_name,
        "version": env!("CARGO_PKG_VERSION"),
        "proto": 3,
        "go": "rust",
        "host": host,
        "port": port,
        "headers": true,
        "max_payload": max_payload,
        "gateway": gateway_name,
        "gateway_urls": gateway_urls,
        "gateway_url": gateway_urls.first().cloned().unwrap_or_default(),
        "gateway_nrp": true,
        "gateway_iom": true,
    })
    .to_string()
}

/// The dialer's CONNECT JSON, in Go's probed shape. `name` is the
/// dialer's server id, `gateway` its gateway name.
pub fn gateway_connect_json(server_id: &str, gateway_name: &str, tls: bool) -> String {
    serde_json::json!({
        "echo": false,
        "verbose": false,
        "pedantic": false,
        "tls_required": tls,
        "headers": false,
        "name": server_id,
        "cluster": "",
        "gateway": gateway_name,
    })
    .to_string()
}

/// An accepter's per-account INFO frame: only `gateway`,
/// `gateway_cmd`, and `gateway_cmd_payload` carry meaning; every
/// other field is empty/zero, exactly as Go sends them.
pub fn gateway_cmd_info_json(gateway_name: &str, cmd: u64, account: &str) -> String {
    serde_json::json!({
        "server_id": "",
        "server_name": "",
        "version": "",
        "proto": 0,
        "go": "",
        "host": "",
        "port": 0,
        "headers": false,
        "max_payload": 0,
        "gateway": gateway_name,
        "gateway_cmd": cmd,
        "gateway_cmd_payload": base64::engine::general_purpose::STANDARD.encode(account),
    })
    .to_string()
}

/// Parsed fields of a gateway INFO frame (full handshake INFO or a
/// cmd INFO). `gateway_cmd` is None for a full INFO.
#[derive(Debug, Default)]
pub struct GatewayInfo {
    pub server_id: Option<String>,
    pub gateway: Option<String>,
    pub gateway_urls: Vec<String>,
    pub gateway_cmd: Option<u64>,
    pub gateway_cmd_account: Option<String>,
}

/// Parse a gateway INFO JSON body. Returns None when the body is not
/// JSON at all (the connection is then a protocol violation).
pub fn parse_gateway_info(json: &str) -> Option<GatewayInfo> {
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    let get_str = |field: &str| {
        value
            .get(field)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    };
    let gateway_cmd = value.get("gateway_cmd").and_then(|v| v.as_u64());
    let gateway_cmd_account = value
        .get("gateway_cmd_payload")
        .and_then(|v| v.as_str())
        .and_then(|payload| {
            base64::engine::general_purpose::STANDARD
                .decode(payload)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
        });
    let gateway_urls = value
        .get("gateway_urls")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    Some(GatewayInfo {
        server_id: get_str("server_id"),
        gateway: get_str("gateway"),
        gateway_urls,
        gateway_cmd,
        gateway_cmd_account,
    })
}

/// Parse a `host:port` gateway URL (bare, or with a `nats://`
/// scheme prefix, as Go's `gateway_urls` entries are bare but
/// configured `urls` carry the scheme).
pub fn parse_gateway_url(url: &str) -> Option<(String, u16)> {
    let stripped = url
        .strip_prefix("nats://")
        .or_else(|| url.strip_prefix("nats-gateway://"))
        .unwrap_or(url);
    let stripped = stripped.trim_end_matches('/');
    let (host, port) = stripped.rsplit_once(':')?;
    Some((host.to_string(), port.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_shape_matches_probe() {
        let info = gateway_info_json(
            "SID",
            "edge",
            "127.0.0.1",
            7222,
            1048576,
            "EDGE",
            &["127.0.0.1:7222".to_string()],
        );
        let parsed = parse_gateway_info(&info).unwrap();
        assert_eq!(parsed.gateway.as_deref(), Some("EDGE"));
        assert_eq!(parsed.gateway_urls, vec!["127.0.0.1:7222"]);
        assert_eq!(parsed.gateway_cmd, None);
    }

    #[test]
    fn cmd_round_trip() {
        let info = gateway_cmd_info_json("EDGE", 2, "APP");
        let parsed = parse_gateway_info(&info).unwrap();
        assert_eq!(parsed.gateway_cmd, Some(2));
        assert_eq!(parsed.gateway_cmd_account.as_deref(), Some("APP"));
        let info3 = gateway_cmd_info_json("EDGE", 3, "$SYS");
        let parsed3 = parse_gateway_info(&info3).unwrap();
        assert_eq!(parsed3.gateway_cmd, Some(3));
        assert_eq!(parsed3.gateway_cmd_account.as_deref(), Some("$SYS"));
    }

    #[test]
    fn connect_shape() {
        let connect = gateway_connect_json("SID", "EDGE", false);
        let value: serde_json::Value = serde_json::from_str(&connect).unwrap();
        assert_eq!(value["echo"], false);
        assert_eq!(value["headers"], false);
        assert_eq!(value["name"], "SID");
        assert_eq!(value["gateway"], "EDGE");
    }

    #[test]
    fn url_forms() {
        assert_eq!(
            parse_gateway_url("127.0.0.1:7222"),
            Some(("127.0.0.1".to_string(), 7222))
        );
        assert_eq!(
            parse_gateway_url("nats://127.0.0.1:7222"),
            Some(("127.0.0.1".to_string(), 7222))
        );
        assert_eq!(parse_gateway_url("no-port"), None);
    }

    #[test]
    fn accounts_include_sys_g_and_configured() {
        let mut config = crate::config::Config::default();
        config.accounts.push(crate::config::Account {
            name: "APP".to_string(),
            ..Default::default()
        });
        assert_eq!(gateway_accounts(&config), vec!["$SYS", "APP", "$G"]);
    }
}
