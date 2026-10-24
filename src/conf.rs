//! nats-server `.conf` file support (Phase 11, subset).
//!
//! Parses the config format's core grammar: `key: value` pairs, nested
//! `{ ... }` maps, `[ ... ]` lists, quoted strings, numbers, booleans,
//! and `#` / `//` comments. Keys may also be written `key value` (the
//! colon is optional) and `key:value` (no spaces).
//!
//! Supported keys (unknown keys are ignored, leniently):
//! - `port`, `host`, `server_name`, `http_port`, `max_payload`
//! - `authorization { user, password, token }`
//! - `cluster { name, host, port, routes }`
//! - `leafnodes { port, remotes, authorization { user, password } }`
//! - `jetstream` (boolean, or a block with `store_dir`)
//!
//! Not supported (ignored or, for `include`, rejected): `include`,
//! TLS sections, `accounts`, `system_account`, `no_auth_user`,
//! logging, limits, websocket/mqtt/gateway sections. A config using
//! those will load, but the features stay off — the README says so.

use std::collections::HashMap;

use crate::config::{parse_size, Config};

/// A parsed config value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Str(String),
    Num(i64),
    Bool(bool),
    Map(Vec<(String, Value)>),
    List(Vec<Value>),
}

impl Value {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&[(String, Value)]> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.as_map()
            .and_then(|m| m.iter().rev().find(|(k, _)| k == key).map(|(_, v)| v))
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Str(String),
    /// A string that was quoted in the file (never re-interpreted as a
    /// number or boolean).
    Quoted(String),
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Comma,
    Colon,
}

fn tokenize(input: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = input.chars().collect();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            ' ' | '\t' | '\r' | '\n' => i += 1,
            '#' => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '/' if i + 1 < chars.len() && chars[i + 1] == '/' => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '{' => {
                toks.push(Tok::LBrace);
                i += 1;
            }
            '}' => {
                toks.push(Tok::RBrace);
                i += 1;
            }
            '[' => {
                toks.push(Tok::LBracket);
                i += 1;
            }
            ']' => {
                toks.push(Tok::RBracket);
                i += 1;
            }
            ',' => {
                toks.push(Tok::Comma);
                i += 1;
            }
            ':' => {
                toks.push(Tok::Colon);
                i += 1;
            }
            '"' | '\'' => {
                let quote = c;
                i += 1;
                let mut out = String::new();
                while i < chars.len() && chars[i] != quote {
                    if chars[i] == '\\' && quote == '"' && i + 1 < chars.len() {
                        let esc = chars[i + 1];
                        match esc {
                            'n' => out.push('\n'),
                            't' => out.push('\t'),
                            'r' => out.push('\r'),
                            other => out.push(other),
                        }
                        i += 2;
                    } else {
                        out.push(chars[i]);
                        i += 1;
                    }
                }
                if i >= chars.len() {
                    return Err("unterminated quoted string".to_string());
                }
                i += 1;
                toks.push(Tok::Quoted(out));
            }
            _ => {
                // Bare token: runs until whitespace or a structural
                // character. Colons and slashes stay inside the token so
                // URLs like nats://127.0.0.1:6222 survive; the parser
                // splits a trailing or embedded colon for key/value pairs.
                let mut out = String::new();
                while i < chars.len() {
                    let c = chars[i];
                    if c.is_whitespace() || matches!(c, '{' | '}' | '[' | ']' | ',' | '#') {
                        break;
                    }
                    out.push(c);
                    i += 1;
                }
                toks.push(Tok::Str(out));
            }
        }
    }
    Ok(toks)
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn parse_map_body(&mut self) -> Result<Vec<(String, Value)>, String> {
        let mut entries = Vec::new();
        loop {
            match self.peek() {
                None => return Err("unexpected end of config".to_string()),
                Some(Tok::RBrace) => {
                    self.next();
                    return Ok(entries);
                }
                Some(Tok::Comma) => {
                    self.next();
                }
                Some(Tok::Str(_)) | Some(Tok::Quoted(_)) => {
                    let (key, value) = self.parse_entry()?;
                    entries.push((key, value));
                }
                Some(other) => {
                    return Err(format!("unexpected token in map: {other:?}"));
                }
            }
        }
    }

    /// Parse one `key [:] value` entry. Returns (key, value).
    fn parse_entry(&mut self) -> Result<(String, Value), String> {
        let key_tok = self.next().ok_or("expected key")?;
        let mut key = match key_tok {
            Tok::Str(s) | Tok::Quoted(s) => s,
            other => return Err(format!("expected key, got {other:?}")),
        };
        // Forms: `key:` (trailing colon in the token), `key:value`
        // (embedded colon), `key : value`, or `key value`.
        let mut inline_value: Option<String> = None;
        if key.ends_with(':') {
            key.pop();
        } else if let Some(idx) = key.find(':') {
            let rest = key[idx + 1..].to_string();
            key.truncate(idx);
            inline_value = Some(rest);
        } else if matches!(self.peek(), Some(Tok::Colon)) {
            self.next();
        }
        if key.is_empty() {
            return Err("empty key".to_string());
        }
        let value = if let Some(rest) = inline_value {
            if rest.is_empty() {
                self.parse_value()?
            } else {
                interpret_bare(&rest)
            }
        } else {
            self.parse_value()?
        };
        Ok((key, value))
    }

    fn parse_value(&mut self) -> Result<Value, String> {
        match self.next() {
            Some(Tok::Quoted(s)) => Ok(Value::Str(s)),
            Some(Tok::Str(s)) => Ok(interpret_bare(&s)),
            Some(Tok::LBrace) => Ok(Value::Map(self.parse_map_body()?)),
            Some(Tok::LBracket) => {
                let mut items = Vec::new();
                loop {
                    match self.peek() {
                        None => return Err("unterminated list".to_string()),
                        Some(Tok::RBracket) => {
                            self.next();
                            return Ok(Value::List(items));
                        }
                        Some(Tok::Comma) => {
                            self.next();
                        }
                        _ => items.push(self.parse_value()?),
                    }
                }
            }
            other => Err(format!("expected value, got {other:?}")),
        }
    }
}

fn interpret_bare(s: &str) -> Value {
    match s {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ => match s.parse::<i64>() {
            Ok(n) => Value::Num(n),
            Err(_) => Value::Str(s.to_string()),
        },
    }
}

/// Parse config file text into a top-level map.
pub fn parse(text: &str) -> Result<Vec<(String, Value)>, String> {
    let toks = tokenize(text)?;
    let mut parser = Parser { toks, pos: 0 };
    let mut entries = Vec::new();
    loop {
        match parser.peek() {
            None => return Ok(entries),
            Some(Tok::Comma) => {
                parser.next();
            }
            Some(Tok::Str(_)) | Some(Tok::Quoted(_)) => {
                let (key, value) = parser.parse_entry()?;
                entries.push((key, value));
            }
            Some(other) => return Err(format!("unexpected token: {other:?}")),
        }
    }
}

fn map_of(value: &Value) -> Option<HashMap<String, &Value>> {
    value.as_map().map(|m| {
        let mut out = HashMap::new();
        for (k, v) in m {
            out.insert(k.clone(), v);
        }
        out
    })
}

fn as_u16(v: &Value, what: &str) -> Result<u16, String> {
    match v {
        Value::Num(n) if *n >= 0 && *n <= 65535 => Ok(*n as u16),
        Value::Str(s) => s.parse::<u16>().map_err(|_| format!("invalid {what}: {s}")),
        _ => Err(format!("invalid {what}: expected a number")),
    }
}

fn as_string_list(v: &Value) -> Result<Vec<String>, String> {
    match v {
        Value::List(items) => {
            let mut out = Vec::new();
            for item in items {
                match item {
                    Value::Str(s) => out.push(s.clone()),
                    _ => return Err("expected a list of strings".to_string()),
                }
            }
            Ok(out)
        }
        Value::Str(s) => Ok(vec![s.clone()]),
        _ => Err("expected a string or list of strings".to_string()),
    }
}

/// Apply a parsed config file to `base` (usually the defaults), returning
/// the merged config. CLI flags applied afterwards win.
pub fn apply_to(base: &Config, text: &str) -> Result<Config, String> {
    let top = parse(text)?;
    let mut cfg = base.clone();
    let lookup = |entries: &[(String, Value)], key: &str| -> Option<Value> {
        entries
            .iter()
            .rev()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    };
    if let Some(v) = lookup(&top, "port") {
        cfg.port = as_u16(&v, "port")?;
    }
    if let Some(v) = lookup(&top, "host") {
        cfg.host = v.as_str().ok_or("host must be a string")?.to_string();
    }
    if let Some(v) = lookup(&top, "server_name") {
        cfg.name = v
            .as_str()
            .ok_or("server_name must be a string")?
            .to_string();
    }
    if let Some(v) = lookup(&top, "http_port") {
        cfg.http_port = as_u16(&v, "http_port")?;
    }
    if let Some(v) = lookup(&top, "max_payload") {
        cfg.max_payload = match &v {
            Value::Num(n) if *n >= 0 => *n as usize,
            Value::Str(s) => parse_size(s)?,
            _ => return Err("max_payload must be a number or size".to_string()),
        };
    }
    if let Some(v) = lookup(&top, "authorization") {
        let map = map_of(&v).ok_or("authorization must be a block")?;
        if let Some(u) = map.get("user").and_then(|v| v.as_str()) {
            cfg.user = Some(u.to_string());
        }
        if let Some(p) = map.get("password").and_then(|v| v.as_str()) {
            cfg.pass = Some(p.to_string());
        }
        if let Some(t) = map.get("token").and_then(|v| v.as_str()) {
            cfg.auth_token = Some(t.to_string());
        }
    }
    if let Some(v) = lookup(&top, "cluster") {
        let map = map_of(&v).ok_or("cluster must be a block")?;
        if let Some(n) = map.get("name").and_then(|v| v.as_str()) {
            cfg.cluster_name = Some(n.to_string());
        }
        if let Some(h) = map.get("host").and_then(|v| v.as_str()) {
            cfg.cluster_host = h.to_string();
        }
        if let Some(p) = map.get("port") {
            cfg.cluster_port = as_u16(p, "cluster port")?;
        }
        if let Some(r) = map.get("routes") {
            cfg.routes = as_string_list(r)?;
        }
    }
    if let Some(v) = lookup(&top, "leafnodes") {
        let map = map_of(&v).ok_or("leafnodes must be a block")?;
        if let Some(p) = map.get("port") {
            cfg.leaf_port = as_u16(p, "leaf port")?;
        }
        if let Some(r) = map.get("remotes") {
            match r {
                Value::List(items) => {
                    let mut urls = Vec::new();
                    for item in items {
                        match item {
                            Value::Map(_) => {
                                let m = map_of(item).unwrap_or_default();
                                if let Some(u) = m.get("url").and_then(|v| v.as_str()) {
                                    urls.push(u.to_string());
                                }
                            }
                            Value::Str(s) => urls.push(s.clone()),
                            _ => return Err("leafnodes remotes must be urls or blocks".to_string()),
                        }
                    }
                    cfg.leaf_urls = urls;
                }
                _ => return Err("leafnodes remotes must be a list".to_string()),
            }
        }
        if let Some(a) = map.get("authorization") {
            let amap = map_of(a).ok_or("leafnodes authorization must be a block")?;
            if let Some(u) = amap.get("user").and_then(|v| v.as_str()) {
                cfg.leaf_user = Some(u.to_string());
            }
            if let Some(p) = amap.get("password").and_then(|v| v.as_str()) {
                cfg.leaf_pass = Some(p.to_string());
            }
        }
    }
    if let Some(v) = lookup(&top, "jetstream") {
        match &v {
            Value::Bool(b) => cfg.jetstream = *b,
            Value::Map(_) => {
                cfg.jetstream = true;
                let map = map_of(&v).unwrap_or_default();
                if let Some(d) = map.get("store_dir").and_then(|v| v.as_str()) {
                    cfg.js_store_dir = Some(d.to_string());
                }
            }
            _ => return Err("jetstream must be a boolean or a block".to_string()),
        }
    }
    Ok(cfg)
}

/// Read a config file and apply it to `base`.
pub fn load_file(base: &Config, path: &str) -> Result<Config, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read config file {path}: {e}"))?;
    apply_to(base, &text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_pairs_and_comments() {
        let entries = parse(
            "# a comment\nport: 4333 // trailing\nhost: \"127.0.0.1\"\nserver_name: edge-1\n",
        )
        .unwrap();
        let get = |k: &str| {
            entries
                .iter()
                .rev()
                .find(|(kk, _)| kk == k)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(get("port"), Some(Value::Num(4333)));
        assert_eq!(get("host").unwrap().as_str(), Some("127.0.0.1"));
        assert_eq!(get("server_name").unwrap().as_str(), Some("edge-1"));
    }

    #[test]
    fn parses_blocks_lists_and_urls() {
        let text = r#"
            cluster {
              name: test-cluster
              port: 6223
              routes: ["nats://127.0.0.1:6222", nats://127.0.0.1:6224]
            }
            leafnodes {
              port: 7433
              remotes: [{url: "nats://127.0.0.1:7432"}]
            }
        "#;
        let cfg = apply_to(&Config::default(), text).unwrap();
        assert_eq!(cfg.cluster_name.as_deref(), Some("test-cluster"));
        assert_eq!(cfg.cluster_port, 6223);
        assert_eq!(
            cfg.routes,
            vec!["nats://127.0.0.1:6222", "nats://127.0.0.1:6224"]
        );
        assert_eq!(cfg.leaf_port, 7433);
        assert_eq!(cfg.leaf_urls, vec!["nats://127.0.0.1:7432"]);
    }

    #[test]
    fn parses_jetstream_and_auth() {
        let text = r#"
            authorization { user: alice password: secret }
            jetstream { store_dir: "/tmp/js" }
        "#;
        let cfg = apply_to(&Config::default(), text).unwrap();
        assert_eq!(cfg.user.as_deref(), Some("alice"));
        assert_eq!(cfg.pass.as_deref(), Some("secret"));
        assert!(cfg.jetstream);
        assert_eq!(cfg.js_store_dir.as_deref(), Some("/tmp/js"));
    }

    #[test]
    fn rejects_bad_types() {
        assert!(apply_to(&Config::default(), "port: notaport").is_err());
        assert!(apply_to(&Config::default(), "port: {").is_err());
    }
}
