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
        }
    }
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
         \x20   -h, --help              Show this help"
            .to_string()
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
