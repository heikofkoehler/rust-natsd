//! Server configuration and CLI parsing.

use crate::protocol::DEFAULT_MAX_PAYLOAD;

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub max_payload: usize,
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
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: "0.0.0.0".to_string(),
            port: 4222,
            name: "rust-natsd".to_string(),
            max_payload: DEFAULT_MAX_PAYLOAD,
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
        }
    }
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
        let mut cfg = Config::default();
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
                "-h" | "--help" => return Err(Self::usage()),
                other => return Err(format!("unknown argument: {other}\n\n{}", Self::usage())),
            }
            i += 1;
        }
        if cfg.pass.is_some() && cfg.user.is_none() {
            return Err("--pass requires --user".to_string());
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
         \x20   -h, --help              Show this help"
            .to_string()
    }

    /// Effective cluster name: explicit value, else the server name.
    pub fn effective_cluster_name(&self) -> String {
        self.cluster_name
            .clone()
            .unwrap_or_else(|| self.name.clone())
    }

    /// Check a client's CONNECT credentials against the configured auth.
    /// Returns true when no auth is configured.
    pub fn authorize(&self, info: &crate::protocol::ConnectInfo) -> bool {
        if let Some(token) = &self.auth_token {
            // Clients may present the token in `auth_token` or as password.
            if info.auth_token.as_deref() == Some(token.as_str())
                || info.pass.as_deref() == Some(token.as_str())
            {
                return true;
            }
            // If user/pass auth is also configured, fall through to it.
            if self.user.is_none() {
                return false;
            }
        }
        if let Some(user) = &self.user {
            return info.user.as_deref() == Some(user.as_str())
                && info.pass.as_deref() == self.pass.as_deref();
        }
        true
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
        ci.auth_token = None;
        ci.pass = Some("s3cret".into());
        assert!(token_cfg.authorize(&ci), "token as password also accepted");

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
