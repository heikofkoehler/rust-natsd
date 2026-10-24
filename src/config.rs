//! Server configuration and CLI parsing.

use crate::protocol::DEFAULT_MAX_PAYLOAD;
use crate::subject::subject_matches;

/// Default queued-outbound limit per connection, matching Go
/// `nats-server`'s default `max_pending` (64 MiB).
pub const DEFAULT_MAX_PENDING: usize = 64 * 1024 * 1024;

/// Subject permissions for one user (or one account, as a default).
/// Each field is `Option`: absent means "no restriction on this axis"
/// (allow-all for that axis, except denies, which always win). Present
/// allow lists close off everything not listed, matching `nats-server`:
/// deny takes precedence over allow.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Permissions {
    pub publish_allow: Option<Vec<String>>,
    pub publish_deny: Option<Vec<String>>,
    pub subscribe_allow: Option<Vec<String>>,
    pub subscribe_deny: Option<Vec<String>>,
}

impl Permissions {
    fn check(allow: &Option<Vec<String>>, deny: &Option<Vec<String>>, subject: &str) -> bool {
        if let Some(deny) = deny {
            if deny.iter().any(|p| subject_matches(p, subject)) {
                return false;
            }
        }
        if let Some(allow) = allow {
            return allow.iter().any(|p| subject_matches(p, subject));
        }
        true
    }

    pub fn can_publish(&self, subject: &str) -> bool {
        Self::check(&self.publish_allow, &self.publish_deny, subject)
    }

    pub fn can_subscribe(&self, subject: &str) -> bool {
        Self::check(&self.subscribe_allow, &self.subscribe_deny, subject)
    }

    /// Fill any absent field from `default` (account-level permissions
    /// are the fallback for per-user permissions).
    pub fn fallback(&self, default: &Permissions) -> Permissions {
        Permissions {
            publish_allow: self
                .publish_allow
                .clone()
                .or_else(|| default.publish_allow.clone()),
            publish_deny: self
                .publish_deny
                .clone()
                .or_else(|| default.publish_deny.clone()),
            subscribe_allow: self
                .subscribe_allow
                .clone()
                .or_else(|| default.subscribe_allow.clone()),
            subscribe_deny: self
                .subscribe_deny
                .clone()
                .or_else(|| default.subscribe_deny.clone()),
        }
    }

    pub fn has_any(&self) -> bool {
        self.publish_allow.is_some()
            || self.publish_deny.is_some()
            || self.subscribe_allow.is_some()
            || self.subscribe_deny.is_some()
    }
}

/// One user inside an account. `name` is empty for nkey-only users
/// (configured with an `nkey` field and no `user` field, as in
/// `nats-server`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AccountUser {
    pub name: String,
    pub pass: Option<String>,
    pub token: Option<String>,
    pub nkey: Option<String>,
    pub permissions: Permissions,
}

/// One account (tenant). Users in different accounts share no subject
/// space: subscriptions and deliveries are filtered by account.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Account {
    pub name: String,
    pub users: Vec<AccountUser>,
    /// Account-level permissions, used when a user has none on that axis.
    /// Parsed from both the real `nats-server` shape (per-user
    /// `permissions` blocks) and the account-level `authorization`
    /// block some configs use.
    pub permissions: Permissions,
    /// Parsed from `accounts { NAME { jetstream: enabled } }`; stored
    /// but not enforced — when `--jetstream` is on, all accounts get
    /// JetStream (see README non-goals/notes).
    pub jetstream_enabled: bool,
}

/// Result of a successful authentication.
#[derive(Debug, Clone)]
pub struct AuthOutcome {
    pub account: String,
    pub user: Option<String>,
    pub permissions: Permissions,
}

/// Verify an nkey signature: base64-decode `sig_b64` and check it
/// against `nonce` with the public key `nkey` (Ed25519, via the `nkeys`
/// crate). Returns false on any decode/parse/verify failure.
/// Both encodings are accepted: standard padded base64 (as the Rust
/// `nkeys` test client sends) and raw URL-safe base64 without padding
/// (as nats.go sends — `base64.RawURLEncoding`), since a 64-byte
/// signature encodes to 86 unpadded chars that standard decoding
/// rejects.
pub fn verify_nkey_sig(nkey: &str, nonce: &[u8], sig_b64: &str) -> bool {
    use base64::Engine;
    let trimmed = sig_b64.trim();
    let sig = base64::engine::general_purpose::STANDARD
        .decode(trimmed)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(trimmed));
    let Ok(sig) = sig else {
        return false;
    };
    let Ok(kp) = nkeys::KeyPair::from_public_key(nkey) else {
        return false;
    };
    kp.verify(nonce, &sig).is_ok()
}

/// TLS settings for the client listener. Presence of this struct (with a
/// cert and key) means TLS is *required* on the client port: the server
/// handshakes before sending INFO, so plain-text clients fail at the
/// handshake. There is no mixed plain/TLS mode on one port, matching
/// `nats-server` with a `tls {}` block.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TlsSettings {
    pub cert_file: String,
    pub key_file: String,
    pub ca_file: Option<String>,
    pub verify: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub max_payload: usize,
    /// Maximum queued outbound bytes per connection before the
    /// connection is dropped as a slow consumer (Go's `max_pending`,
    /// default 64 MiB). This is a *bytes* limit, not a frame count: a
    /// burst of small messages must not be dropped at an arbitrary
    /// frame count while the writer is still draining.
    pub max_pending: usize,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub auth_token: Option<String>,
    /// Cluster name; defaults to the server name (like `nats-server`).
    pub cluster_name: Option<String>,
    pub cluster_host: String,
    /// Cluster (route) listener port; 0 disables the route listener.
    pub cluster_port: u16,
    /// Route endpoints to solicit, e.g. `nats://127.0.0.1:6222` (comma list).
    pub routes: Vec<String>,
    /// Leafnode listener port; 0 disables the leaf listener.
    pub leaf_port: u16,
    /// Hub leafnode endpoints to solicit (comma list).
    pub leaf_urls: Vec<String>,
    pub leaf_user: Option<String>,
    pub leaf_pass: Option<String>,
    /// Enable JetStream (in-memory streams; see jetstream.rs).
    pub jetstream: bool,
    /// JetStream store directory (reserved; current engine is in-memory).
    pub js_store_dir: Option<String>,
    /// Monitoring HTTP port; 0 disables the monitor.
    pub http_port: u16,
    /// TLS for client connections; None means plain text.
    pub tls: Option<TlsSettings>,
    /// Multi-account model; empty means the legacy single-account mode
    /// (global --user/--pass/--auth-token as account `$G`).
    pub accounts: Vec<Account>,
    /// Name of the system account, if configured (parsed and exposed
    /// via monitoring; `$SYS` subjects are not special-cased).
    pub system_account: Option<String>,
    /// Username that credential-less connections map to, if configured
    /// (must name a configured user; otherwise they are rejected).
    pub no_auth_user: Option<String>,
    /// Server-initiated PING interval (Go default 2m).
    pub ping_interval: std::time::Duration,
    /// Missed PONGs before closing with `-ERR 'Stale Connection'`
    /// (Go default 2).
    pub ping_max: u32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: "0.0.0.0".to_string(),
            port: 4222,
            name: "rust-natsd".to_string(),
            max_payload: DEFAULT_MAX_PAYLOAD,
            max_pending: DEFAULT_MAX_PENDING,
            user: None,
            pass: None,
            auth_token: None,
            cluster_name: None,
            cluster_host: "0.0.0.0".to_string(),
            cluster_port: 6222,
            routes: Vec::new(),
            leaf_port: 0,
            leaf_urls: Vec::new(),
            leaf_user: None,
            leaf_pass: None,
            jetstream: false,
            js_store_dir: None,
            http_port: 0,
            tls: None,
            accounts: Vec::new(),
            system_account: None,
            no_auth_user: None,
            ping_interval: std::time::Duration::from_secs(120),
            ping_max: 2,
        }
    }
}

/// Parse a Go-style duration: `200ms`, `2m`, `120s`, `1m30s`, `500us`,
/// `100ns`, or a bare number (interpreted as milliseconds, the useful
/// unit for a ping interval). Used by `--ping-interval` and by .conf
/// string values; a .conf numeric value is nanoseconds (Go's
/// time.Duration representation) and is handled in conf.rs.
pub fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration".to_string());
    }
    if let Ok(n) = s.parse::<f64>() {
        if n < 0.0 {
            return Err(format!("invalid duration: {s}"));
        }
        return Ok(std::time::Duration::from_secs_f64(n / 1000.0));
    }
    let mut total_nanos: f64 = 0.0;
    let mut rest = s;
    let mut seen = false;
    while !rest.is_empty() {
        let num_end = rest
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(rest.len());
        if num_end == 0 {
            return Err(format!("invalid duration: {s}"));
        }
        let num: f64 = rest[..num_end]
            .parse()
            .map_err(|_| format!("invalid duration: {s}"))?;
        rest = &rest[num_end..];
        let unit_end = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let unit = &rest[..unit_end];
        rest = &rest[unit_end..];
        let mult = match unit {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1_000.0,
            "ms" => 1_000_000.0,
            "s" => 1_000_000_000.0,
            "m" => 60_000_000_000.0,
            "h" => 3_600_000_000_000.0,
            _ => return Err(format!("invalid duration: {s}")),
        };
        total_nanos += num * mult;
        seen = true;
    }
    if !seen {
        return Err(format!("invalid duration: {s}"));
    }
    Ok(std::time::Duration::from_nanos(total_nanos as u64))
}

/// Split a comma-separated CLI list, dropping empties.
pub fn split_list(s: &str) -> Vec<String> {
    s.split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

/// Parse a route/leaf URL (`nats://host:port`, `nats-route://host:port`,
/// or bare `host:port`) into (host, port).
pub fn parse_route_url(url: &str) -> Result<(String, u16), String> {
    let stripped = url
        .strip_prefix("nats://")
        .or_else(|| url.strip_prefix("nats-route://"))
        .or_else(|| url.strip_prefix("nats-leaf://"))
        .unwrap_or(url);
    let stripped = stripped.trim_end_matches('/');
    let (host, port) = stripped
        .rsplit_once(':')
        .ok_or_else(|| format!("invalid route url (need host:port): {url}"))?;
    let port: u16 = port
        .parse()
        .map_err(|_| format!("invalid route port: {url}"))?;
    Ok((host.to_string(), port))
}

pub fn parse_size(s: &str) -> Result<usize, String> {
    let lower = s.to_ascii_lowercase();
    let (num, mult) = if let Some(p) = lower.strip_suffix("kb") {
        (p, 1024usize)
    } else if let Some(p) = lower.strip_suffix("mb") {
        (p, 1024 * 1024)
    } else if let Some(p) = lower.strip_suffix('k') {
        (p, 1024)
    } else if let Some(p) = lower.strip_suffix('m') {
        (p, 1024 * 1024)
    } else {
        (lower.as_str(), 1)
    };
    num.trim()
        .parse::<usize>()
        .map(|n| n * mult)
        .map_err(|_| format!("invalid size: {s}"))
}

impl Config {
    /// Parse from process args (excluding argv[0]). `--help` prints usage to
    /// stdout via the returned Err message with exit code 0 semantics handled
    /// by the caller through `Config::usage()`.
    pub fn from_args(args: &[String]) -> Result<Config, String> {
        // A config file (-c/--config) loads first; explicit CLI flags
        // applied afterwards win over file values.
        let mut config_path: Option<String> = None;
        let mut filtered: Vec<String> = Vec::new();
        let mut i = 0;
        while i < args.len() {
            let arg = args[i].as_str();
            if arg == "-c" || arg == "--config" {
                i += 1;
                config_path = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| format!("missing value for {arg}"))?,
                );
            } else if let Some(path) = arg.strip_prefix("--config=") {
                config_path = Some(path.to_string());
            } else {
                filtered.push(args[i].clone());
            }
            i += 1;
        }
        let mut cfg = match &config_path {
            Some(path) => crate::conf::load_file(&Config::default(), path)?,
            None => Config::default(),
        };
        let args = &filtered;
        let mut i = 0;
        while i < args.len() {
            let arg = args[i].as_str();
            let next = |i: &mut usize| -> Result<String, String> {
                *i += 1;
                args.get(*i)
                    .cloned()
                    .ok_or_else(|| format!("missing value for {arg}"))
            };
            match arg {
                "-a" | "--addr" | "--host" => cfg.host = next(&mut i)?,
                "-p" | "--port" => {
                    cfg.port = next(&mut i)?
                        .parse()
                        .map_err(|_| "invalid port".to_string())?
                }
                "-n" | "--name" => cfg.name = next(&mut i)?,
                "--max-payload" => cfg.max_payload = parse_size(&next(&mut i)?)?,
                "--max-pending" => cfg.max_pending = parse_size(&next(&mut i)?)?,
                "--ping-interval" => cfg.ping_interval = parse_duration(&next(&mut i)?)?,
                "--ping-max" => {
                    cfg.ping_max = next(&mut i)?
                        .parse()
                        .map_err(|_| "invalid ping max".to_string())?
                }
                "--user" => cfg.user = Some(next(&mut i)?),
                "--pass" => cfg.pass = Some(next(&mut i)?),
                "--auth-token" => cfg.auth_token = Some(next(&mut i)?),
                "--cluster-name" => cfg.cluster_name = Some(next(&mut i)?),
                "--cluster-host" => cfg.cluster_host = next(&mut i)?,
                "--cluster-port" => {
                    cfg.cluster_port = next(&mut i)?
                        .parse()
                        .map_err(|_| "invalid cluster port".to_string())?
                }
                "--routes" => cfg.routes.extend(split_list(&next(&mut i)?)),
                "--leaf-port" => {
                    cfg.leaf_port = next(&mut i)?
                        .parse()
                        .map_err(|_| "invalid leaf port".to_string())?
                }
                "--leaf-urls" => cfg.leaf_urls.extend(split_list(&next(&mut i)?)),
                "--leaf-user" => cfg.leaf_user = Some(next(&mut i)?),
                "--leaf-pass" => cfg.leaf_pass = Some(next(&mut i)?),
                "--jetstream" => cfg.jetstream = true,
                "--js-store-dir" => cfg.js_store_dir = Some(next(&mut i)?),
                "--http-port" => {
                    cfg.http_port = next(&mut i)?
                        .parse()
                        .map_err(|_| "invalid http port".to_string())?
                }
                "--tls" => {
                    cfg.tls.get_or_insert_with(TlsSettings::default);
                }
                "--tlscert" => {
                    let path = next(&mut i)?;
                    cfg.tls.get_or_insert_with(TlsSettings::default).cert_file = path;
                }
                "--tlskey" => {
                    let path = next(&mut i)?;
                    cfg.tls.get_or_insert_with(TlsSettings::default).key_file = path;
                }
                "--tlsverify" => {
                    cfg.tls.get_or_insert_with(TlsSettings::default).verify = true;
                }
                "--tlscacert" => {
                    let path = next(&mut i)?;
                    cfg.tls.get_or_insert_with(TlsSettings::default).ca_file = Some(path);
                }
                "-h" | "--help" => return Err(Self::usage()),
                other => return Err(format!("unknown argument: {other}\n\n{}", Self::usage())),
            }
            i += 1;
        }
        if cfg.pass.is_some() && cfg.user.is_none() {
            return Err("--pass requires --user".to_string());
        }
        if let Some(t) = &cfg.tls {
            if t.cert_file.is_empty() || t.key_file.is_empty() {
                return Err(
                    "--tls requires --tlscert and --tlskey (or a tls block in the config file)"
                        .to_string(),
                );
            }
            if t.verify && t.ca_file.is_none() {
                return Err("--tlsverify requires --tlscacert".to_string());
            }
        }
        Ok(cfg)
    }

    pub fn usage() -> String {
        "rust-natsd — NATS server in Rust (Core NATS pub-sub)\n\
         \n\
         USAGE: rust-natsd [OPTIONS]\n\
         \n\
         OPTIONS:\n\
         \x20   -a, --addr <HOST>       Bind host (default 0.0.0.0)\n\
         \x20   -p, --port <PORT>       Client port (default 4222)\n\
         \x20   -n, --name <NAME>       Server name (default rust-natsd)\n\
         \x20   --max-payload <SIZE>    Max payload, e.g. 1048576, 512k, 8mb (default 1mb)\n\
         \x20   --max-pending <SIZE>    Queued outbound bytes per conn before slow-consumer drop (default 64mb)\n\
         \x20   --ping-interval <DUR>   Server PING interval, e.g. 200ms, 2m (default 2m)\n\
         \x20   --ping-max <N>          Missed PONGs before stale close (default 2)\n\
         \x20   --user <USER>           Require this username for CONNECT\n\
         \x20   --pass <PASS>           Require this password for CONNECT\n\
         \x20   --auth-token <TOKEN>    Require this auth token for CONNECT\n\
         \x20   --cluster-name <NAME>   Cluster name (default: server name)\n\
         \x20   --cluster-host <HOST>   Route listener host (default 0.0.0.0)\n\
         \x20   --cluster-port <PORT>   Route listener port, 0 disables (default 6222)\n\
         \x20   --routes <URLS>         Routes to solicit, comma list\n\
         \x20   --leaf-port <PORT>      Leafnode listener port, 0 disables (default 0)\n\
         \x20   --leaf-urls <URLS>      Hub leafnode endpoints, comma list\n\
         \x20   --leaf-user <USER>      Username for solicited leaf connections\n\
         \x20   --leaf-pass <PASS>      Password for solicited leaf connections\n\
         \x20   --jetstream             Enable JetStream (in-memory)\n\
         \x20   --js-store-dir <DIR>    JetStream store dir (reserved)\n\
         \x20   --http-port <PORT>      Monitoring HTTP port, 0 disables (default 0)\n\
         \x20   --tls                   Require TLS on the client port\n\
         \x20   --tlscert <FILE>        TLS server certificate (PEM)\n\
         \x20   --tlskey <FILE>         TLS server private key (PEM)\n\
         \x20   --tlsverify             Require and verify client certificates\n\
         \x20   --tlscacert <FILE>      CA file for client-certificate verification\n\
         \x20   -c, --config <FILE>     Config file (nats-server .conf subset)\n\
         \x20   -h, --help              Show this help"
            .to_string()
    }

    /// True when client connections must use TLS (a TLS config is set).
    pub fn tls_required(&self) -> bool {
        self.tls.is_some()
    }

    /// Effective cluster name: explicit value, else the server name.
    pub fn effective_cluster_name(&self) -> String {
        self.cluster_name
            .clone()
            .unwrap_or_else(|| self.name.clone())
    }

    /// True when any authentication is configured (legacy user/token
    /// or any account with users).
    pub fn auth_required(&self) -> bool {
        self.user.is_some()
            || self.auth_token.is_some()
            || self.accounts.iter().any(|a| !a.users.is_empty())
    }

    /// Authenticate a CONNECT. `nonce` is the per-connection INFO nonce
    /// bytes (required for nkey verification; None means nkey auth
    /// always fails). Returns the account, user, and effective
    /// permissions on success, None on failure.
    pub fn authenticate(
        &self,
        info: &crate::protocol::ConnectInfo,
        nonce: Option<&[u8]>,
    ) -> Option<AuthOutcome> {
        if !self.accounts.is_empty() {
            for account in &self.accounts {
                for user in &account.users {
                    // NKey: claimed public key must match, and the
                    // signature must verify against the INFO nonce.
                    if let Some(nkey) = &user.nkey {
                        if info.nkey.as_deref() == Some(nkey.as_str()) {
                            if let (Some(nonce), Some(sig)) = (nonce, &info.sig) {
                                if verify_nkey_sig(nkey, nonce, sig) {
                                    return Some(AuthOutcome {
                                        account: account.name.clone(),
                                        user: if user.name.is_empty() {
                                            None
                                        } else {
                                            Some(user.name.clone())
                                        },
                                        permissions: user
                                            .permissions
                                            .fallback(&account.permissions),
                                    });
                                }
                            }
                            // Wrong/missing signature for a claimed
                            // nkey user: do not fall through to other
                            // credential types for this user.
                            continue;
                        }
                    }
                    if let Some(token) = &user.token {
                        if info.auth_token.as_deref() == Some(token.as_str()) {
                            return Some(AuthOutcome {
                                account: account.name.clone(),
                                user: if user.name.is_empty() {
                                    None
                                } else {
                                    Some(user.name.clone())
                                },
                                permissions: user.permissions.fallback(&account.permissions),
                            });
                        }
                    }
                    if !user.name.is_empty()
                        && info.user.as_deref() == Some(user.name.as_str())
                        && info.pass.as_deref() == user.pass.as_deref()
                    {
                        return Some(AuthOutcome {
                            account: account.name.clone(),
                            user: Some(user.name.clone()),
                            permissions: user.permissions.fallback(&account.permissions),
                        });
                    }
                }
            }
            // Credential-less connections map to no_auth_user, if it
            // names a configured user.
            if info.user.is_none() && info.nkey.is_none() && info.auth_token.is_none() {
                if let Some(no_auth) = &self.no_auth_user {
                    for account in &self.accounts {
                        if let Some(user) = account.users.iter().find(|u| &u.name == no_auth) {
                            return Some(AuthOutcome {
                                account: account.name.clone(),
                                user: Some(user.name.clone()),
                                permissions: user.permissions.fallback(&account.permissions),
                            });
                        }
                    }
                }
            }
            return None;
        }
        // Legacy single-account ($G) mode.
        if let Some(token) = &self.auth_token {
            // Go parity: a token is accepted only via `auth_token`.
            // Presenting it as `pass` is rejected (verified against
            // nats-server v2.15.1); an earlier rust-natsd build accepted
            // it as a deliberate deviation, removed for parity.
            if info.auth_token.as_deref() == Some(token.as_str()) {
                return Some(AuthOutcome {
                    account: "$G".to_string(),
                    user: info.user.clone(),
                    permissions: Permissions::default(),
                });
            }
            // Token failed and no user auth configured: rejected.
            self.user.as_ref()?;
        }
        if let Some(user) = &self.user {
            if info.user.as_deref() == Some(user.as_str())
                && info.pass.as_deref() == self.pass.as_deref()
            {
                return Some(AuthOutcome {
                    account: "$G".to_string(),
                    user: Some(user.clone()),
                    permissions: Permissions::default(),
                });
            }
            // no_auth_user maps credential-less connections onto the
            // configured user (single-user legacy equivalent of Go's
            // no_auth_user, which must name a user in the users list).
            if info.user.is_none() && self.no_auth_user.as_deref() == Some(user.as_str()) {
                return Some(AuthOutcome {
                    account: "$G".to_string(),
                    user: Some(user.clone()),
                    permissions: Permissions::default(),
                });
            }
            return None;
        }
        Some(AuthOutcome {
            account: "$G".to_string(),
            user: info.user.clone(),
            permissions: Permissions::default(),
        })
    }

    /// Check a client's CONNECT credentials against the configured auth.
    /// Returns true when no auth is configured. (Kept for existing
    /// tests; new code uses `authenticate`, which also returns the
    /// account and permissions.)
    pub fn authorize(&self, info: &crate::protocol::ConnectInfo) -> bool {
        self.authenticate(info, None).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::ConnectInfo;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn defaults() {
        let c = Config::from_args(&[]).unwrap();
        assert_eq!(c.port, 4222);
        assert_eq!(c.host, "0.0.0.0");
        assert_eq!(c.max_payload, 1024 * 1024);
    }

    #[test]
    fn parses_all_flags() {
        let c = Config::from_args(&args(&[
            "-a",
            "127.0.0.1",
            "-p",
            "4333",
            "-n",
            "edge-1",
            "--max-payload",
            "8mb",
            "--user",
            "u",
            "--pass",
            "p",
        ]))
        .unwrap();
        assert_eq!(c.host, "127.0.0.1");
        assert_eq!(c.port, 4333);
        assert_eq!(c.name, "edge-1");
        assert_eq!(c.max_payload, 8 * 1024 * 1024);
        assert_eq!(c.user.as_deref(), Some("u"));
        assert_eq!(c.pass.as_deref(), Some("p"));
    }

    #[test]
    fn auth_token_flag() {
        let c = Config::from_args(&args(&["--auth-token", "tok123"])).unwrap();
        assert_eq!(c.auth_token.as_deref(), Some("tok123"));
        let mut ci = ConnectInfo::default();
        assert!(!c.authorize(&ci));
        ci.auth_token = Some("tok123".into());
        assert!(c.authorize(&ci));
    }

    #[test]
    fn arg_errors() {
        assert!(Config::from_args(&args(&["--bogus"])).is_err());
        assert!(Config::from_args(&args(&["-p"])).is_err());
        assert!(Config::from_args(&args(&["-p", "abc"])).is_err());
        assert!(Config::from_args(&args(&["--pass", "p"])).is_err());
        assert!(Config::from_args(&args(&["--help"])).is_err()); // usage via Err
    }

    #[test]
    fn size_parsing() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("512k").unwrap(), 512 * 1024);
        assert_eq!(parse_size("2KB").unwrap(), 2 * 1024);
        assert_eq!(parse_size("1m").unwrap(), 1024 * 1024);
        assert!(parse_size("x").is_err());
    }

    #[test]
    fn authorize_matrix() {
        let open = Config::default();
        assert!(open.authorize(&ConnectInfo::default()));

        let token_cfg = Config {
            auth_token: Some("s3cret".into()),
            ..Default::default()
        };
        let mut ci = ConnectInfo::default();
        assert!(!token_cfg.authorize(&ci));
        ci.auth_token = Some("s3cret".into());
        assert!(token_cfg.authorize(&ci));
        // Go parity: a token presented as `pass` is rejected.
        ci.auth_token = None;
        ci.pass = Some("s3cret".into());
        assert!(!token_cfg.authorize(&ci), "token as password rejected");

        let user_cfg = Config {
            user: Some("derek".into()),
            pass: Some("pw".into()),
            ..Default::default()
        };
        ci.pass = Some("pw".into());
        assert!(!user_cfg.authorize(&ci));
        ci.user = Some("derek".into());
        assert!(user_cfg.authorize(&ci));
        ci.pass = Some("wrong".into());
        assert!(!user_cfg.authorize(&ci));
    }
}
