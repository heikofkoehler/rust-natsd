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
//! - `cluster { name, host, port, routes, tls {...} }`
//! - `leafnodes { port, tls {...}, remotes [{url, tls {...}}],
//!   authorization { user, password } }`
//! - `jetstream` (boolean, or a block with `store_dir`)
//! - `tls { cert_file, key_file, ca_file, verify, verify_and_map }`
//!   (client listener; `verify_and_map` maps a client certificate to
//!   a user by SAN/CN/DN)
//! - `accounts { NAME { users [...], permissions/authorization {...},
//!   jetstream: enabled, exports [...], imports [...] } }`,
//!   `system_account`, `no_auth_user`
//!
//! Not supported (ignored or, for `include`, rejected): `include`,
//! logging, limits, websocket/mqtt/gateway sections. A config using
//! those will load, but the features stay off — the README says so.

use std::collections::HashMap;

use crate::config::{
    parse_duration, parse_size, Account, AccountUser, Config, Export, Import, ImportKind,
    Permissions, TlsSettings,
};

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

/// A permissions value: a single subject, a list of subjects (the allow
/// list), or a `{ allow: [...], deny: [...] }` map.
fn parse_subject_permission(v: &Value) -> (Option<Vec<String>>, Option<Vec<String>>) {
    match v {
        Value::Str(s) => (Some(vec![s.clone()]), None),
        Value::List(items) => {
            let mut out = Vec::new();
            for item in items {
                if let Value::Str(s) = item {
                    out.push(s.clone());
                }
            }
            (Some(out), None)
        }
        Value::Map(_) => {
            let map = map_of(v).unwrap_or_default();
            let allow = map.get("allow").and_then(|a| match a {
                Value::Str(s) => Some(vec![(*s).clone()]),
                Value::List(items) => Some(
                    items
                        .iter()
                        .filter_map(|i| i.as_str().map(|s| s.to_string()))
                        .collect(),
                ),
                _ => None,
            });
            let deny = map.get("deny").and_then(|a| match a {
                Value::Str(s) => Some(vec![(*s).clone()]),
                Value::List(items) => Some(
                    items
                        .iter()
                        .filter_map(|i| i.as_str().map(|s| s.to_string()))
                        .collect(),
                ),
                _ => None,
            });
            (allow, deny)
        }
        _ => (None, None),
    }
}

/// Parse a `permissions` / `authorization` block: `{ publish: ..., subscribe: ... }`.
fn parse_permissions_block(v: &Value) -> Permissions {
    let mut perms = Permissions::default();
    if let Some(map) = map_of(v) {
        if let Some(p) = map.get("publish") {
            let (allow, deny) = parse_subject_permission(p);
            perms.publish_allow = allow;
            perms.publish_deny = deny;
        }
        if let Some(s) = map.get("subscribe") {
            let (allow, deny) = parse_subject_permission(s);
            perms.subscribe_allow = allow;
            perms.subscribe_deny = deny;
        }
    }
    perms
}

fn parse_account_user(v: &Value) -> Option<AccountUser> {
    let map = map_of(v)?;
    let mut user = AccountUser::default();
    if let Some(u) = map.get("user").and_then(|v| v.as_str()) {
        user.name = u.to_string();
    }
    if let Some(p) = map.get("password").and_then(|v| v.as_str()) {
        user.pass = Some(p.to_string());
    }
    if let Some(t) = map.get("token").and_then(|v| v.as_str()) {
        user.token = Some(t.to_string());
    }
    if let Some(n) = map.get("nkey").and_then(|v| v.as_str()) {
        user.nkey = Some(n.to_string());
    }
    // Real Go shape is `permissions`; accept `authorization` as an alias.
    if let Some(p) = map.get("permissions") {
        user.permissions = parse_permissions_block(p);
    } else if let Some(p) = map.get("authorization") {
        user.permissions = parse_permissions_block(p);
    }
    Some(user)
}

fn parse_account(name: &str, v: &Value) -> Option<Account> {
    let map = map_of(v)?;
    let mut account = Account {
        name: name.to_string(),
        ..Default::default()
    };
    if let Some(users) = map.get("users") {
        match users {
            Value::List(items) => {
                for item in items {
                    if let Some(u) = parse_account_user(item) {
                        account.users.push(u);
                    }
                }
            }
            Value::Map(_) => {
                if let Some(u) = parse_account_user(users) {
                    account.users.push(u);
                }
            }
            _ => {}
        }
    }
    if let Some(p) = map.get("permissions") {
        account.permissions = parse_permissions_block(p);
    }
    // Account-level `authorization` block (used by some configs as the
    // account default): same publish/subscribe shape as permissions.
    if let Some(p) = map.get("authorization") {
        let parsed = parse_permissions_block(p);
        if parsed.has_any() {
            account.permissions = parsed;
        }
    }
    if let Some(js) = map.get("jetstream") {
        account.jetstream_enabled =
            matches!(js, Value::Bool(true)) || matches!(js.as_str(), Some("enabled"));
    }
    // Account imports/exports (Go v2.15.1 shape, probed):
    //   exports: [ { service: "svc.>", accounts: ["B"] },
    //              { stream: "events.>", accounts: ["B"] } ]
    //   imports: [ { service: { account: A, subject: "svc.>" },
    //                to: "local.>" } ]
    // `local_subject` is accepted as an alias for `to`.
    if let Some(Value::List(items)) = map.get("exports") {
        for m in items.iter().filter_map(map_of) {
            let (kind, subject) = if let Some(s) = m.get("service").and_then(|v| v.as_str()) {
                (Some(ImportKind::Service), s.to_string())
            } else if let Some(s) = m.get("stream").and_then(|v| v.as_str()) {
                (Some(ImportKind::Stream), s.to_string())
            } else {
                continue;
            };
            let accounts = m
                .get("accounts")
                .and_then(|v| as_string_list(v).ok())
                .unwrap_or_default();
            account.exports.push(Export {
                kind,
                subject,
                accounts,
            });
        }
    }
    if let Some(Value::List(items)) = map.get("imports") {
        for m in items.iter().filter_map(map_of) {
            let (kind, source, remote) = if let Some(svc) = m.get("service").and_then(|v| map_of(v))
            {
                (
                    Some(ImportKind::Service),
                    svc.get("account").and_then(|v| v.as_str()).unwrap_or(""),
                    svc.get("subject").and_then(|v| v.as_str()).unwrap_or(""),
                )
            } else if let Some(st) = m.get("stream").and_then(|v| map_of(v)) {
                (
                    Some(ImportKind::Stream),
                    st.get("account").and_then(|v| v.as_str()).unwrap_or(""),
                    st.get("subject").and_then(|v| v.as_str()).unwrap_or(""),
                )
            } else {
                continue;
            };
            let local = m
                .get("to")
                .or_else(|| m.get("local_subject"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if source.is_empty() || remote.is_empty() || local.is_empty() {
                continue;
            }
            account.imports.push(Import {
                kind,
                source_account: source.to_string(),
                remote_subject: remote.to_string(),
                local_subject: local.to_string(),
            });
        }
    }
    Some(account)
}

/// Parse a `tls { cert_file, key_file, ca_file, verify,
/// verify_and_map }` block into `TlsSettings` (missing fields keep
/// the default/previous value when `base` is given).
fn parse_tls_block(v: &Value, mut tls: TlsSettings) -> TlsSettings {
    if let Some(map) = map_of(v) {
        if let Some(c) = map.get("cert_file").and_then(|v| v.as_str()) {
            tls.cert_file = c.to_string();
        }
        if let Some(k) = map.get("key_file").and_then(|v| v.as_str()) {
            tls.key_file = k.to_string();
        }
        if let Some(ca) = map.get("ca_file").and_then(|v| v.as_str()) {
            tls.ca_file = Some(ca.to_string());
        }
        if let Some(b) = map.get("verify") {
            tls.verify = matches!(b, Value::Bool(true));
        }
        if let Some(b) = map.get("verify_and_map") {
            tls.verify_and_map = matches!(b, Value::Bool(true));
            if tls.verify_and_map {
                tls.verify = true;
            }
        }
    }
    tls
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
    if let Some(v) = lookup(&top, "max_pending") {
        cfg.max_pending = match &v {
            Value::Num(n) if *n >= 0 => *n as usize,
            Value::Str(s) => parse_size(s)?,
            _ => return Err("max_pending must be a number or size".to_string()),
        };
    }
    // Go .conf durations: a string is a Go duration ("200ms"), a number
    // is nanoseconds (Go's time.Duration representation).
    if let Some(v) = lookup(&top, "ping_interval") {
        cfg.ping_interval = match &v {
            Value::Num(n) if *n >= 0 => std::time::Duration::from_nanos(*n as u64),
            Value::Str(s) => parse_duration(s)?,
            _ => return Err("ping_interval must be a duration".to_string()),
        };
    }
    if let Some(v) = lookup(&top, "ping_max") {
        cfg.ping_max = match &v {
            Value::Num(n) if *n >= 0 => *n as u32,
            _ => return Err("ping_max must be a number".to_string()),
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
        // A `users` list in the top-level authorization block becomes
        // the $G account (the legacy single user above stays as-is for
        // configs that use it).
        if let Some(users) = map.get("users") {
            let mut account = Account {
                name: "$G".to_string(),
                ..Default::default()
            };
            if let Value::List(items) = users {
                for item in items {
                    if let Some(u) = parse_account_user(item) {
                        account.users.push(u);
                    }
                }
            }
            if let Some(dp) = map.get("default_permissions") {
                account.permissions = parse_permissions_block(dp);
            }
            if !account.users.is_empty() || account.permissions.has_any() {
                cfg.accounts.push(account);
            }
        }
    }
    if let Some(v) = lookup(&top, "accounts") {
        let entries = v.as_map().ok_or("accounts must be a block")?;
        for (name, value) in entries {
            if let Some(account) = parse_account(name, value) {
                cfg.accounts.push(account);
            }
        }
    }
    if let Some(v) = lookup(&top, "system_account") {
        cfg.system_account = v.as_str().map(|s| s.to_string());
    }
    if let Some(v) = lookup(&top, "no_auth_user") {
        cfg.no_auth_user = v.as_str().map(|s| s.to_string());
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
        if let Some(t) = map.get("tls") {
            cfg.cluster_tls = Some(parse_tls_block(
                t,
                cfg.cluster_tls.clone().unwrap_or_default(),
            ));
        }
    }
    if let Some(v) = lookup(&top, "leafnodes") {
        let map = map_of(&v).ok_or("leafnodes must be a block")?;
        if let Some(p) = map.get("port") {
            cfg.leaf_port = as_u16(p, "leaf port")?;
        }
        if let Some(t) = map.get("tls") {
            cfg.leaf_tls = Some(parse_tls_block(t, cfg.leaf_tls.clone().unwrap_or_default()));
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
                                    if let Some(t) = m.get("tls") {
                                        cfg.leaf_remote_tls.insert(
                                            u.to_string(),
                                            parse_tls_block(t, TlsSettings::default()),
                                        );
                                    }
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
    if let Some(v) = lookup(&top, "tls") {
        let tls = parse_tls_block(&v, cfg.tls.clone().unwrap_or_default());
        cfg.tls = Some(tls);
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
