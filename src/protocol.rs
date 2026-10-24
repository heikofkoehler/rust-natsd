//! NATS client wire protocol: incremental parser for client operations and
//! encoders for server operations.
//!
//! Reference: <https://docs.nats.io/reference/reference-protocol>
//!
//! The parser is a pure state machine over an internal buffer — no I/O — so
//! it can be unit-tested by feeding input split at arbitrary byte offsets.

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// Maximum length of a control line (op line) we accept before payload.
pub const DEFAULT_MAX_CONTROL_LINE: usize = 4096;
/// Default maximum payload, matching `nats-server`.
pub const DEFAULT_MAX_PAYLOAD: usize = 1024 * 1024;

/// Client `CONNECT` options. Unknown fields are ignored by serde.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct ConnectInfo {
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub pedantic: bool,
    #[serde(default)]
    pub tls_required: bool,
    #[serde(default)]
    pub auth_token: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub pass: Option<String>,
    #[serde(default, deserialize_with = "de_string_null_as_empty")]
    pub name: String,
    #[serde(default, deserialize_with = "de_string_null_as_empty")]
    pub lang: String,
    #[serde(default, deserialize_with = "de_string_null_as_empty")]
    pub version: String,
    #[serde(default)]
    pub protocol: i32,
    #[serde(default = "default_true")]
    pub echo: bool,
    #[serde(default)]
    pub headers: bool,
    #[serde(default)]
    pub no_responders: bool,
    /// NKey public key (standalone nkey auth; JWT operator mode is out of
    /// scope — a `jwt` field alone does not authenticate).
    #[serde(default, deserialize_with = "de_opt_string_null_as_none")]
    pub nkey: Option<String>,
    /// Base64-encoded Ed25519 signature of the INFO nonce bytes.
    #[serde(default, deserialize_with = "de_opt_string_null_as_none")]
    pub sig: Option<String>,
    #[serde(default, deserialize_with = "de_opt_string_null_as_none")]
    pub jwt: Option<String>,
}

fn de_opt_string_null_as_none<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
}

fn default_true() -> bool {
    true
}

/// Some clients (e.g. async-nats) send explicit `null` for string fields
/// instead of omitting them; treat null as the empty default.
fn de_string_null_as_empty<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(|o| o.unwrap_or_default())
}

/// Server `INFO` sent on connect.
///
/// `nonce` is sent only when authentication is configured, matching Go
/// (Go omits it for open servers). It is a per-connection challenge
/// string; the client signs its bytes for nkey auth. On Linux it is read
/// from /dev/urandom (see server.rs); elsewhere it falls back to a
/// time/pid/counter mix and is therefore not a strong nonce on
/// non-Linux builds.
fn is_false(b: &bool) -> bool {
    !*b
}

fn str_is_empty(s: &str) -> bool {
    s.is_empty()
}

/// Client INFO, serialized with Go's omitempty semantics: `auth_required`,
/// `tls_required`, and `jetstream` appear only when true, `nonce` only
/// when authentication is configured (otherwise Go sends no nonce), and
/// `cluster` only when a cluster name is explicitly configured. Deliberate
/// omissions (documented in parity/KNOWN_DIFFS.md): `git_commit` and `go`
/// are Go-build facts Rust cannot honestly report, `xkey` needs XKey
/// support Rust does not have, and `api_lvl` is Go's JetStream API level
/// — advertising Go's 5 would gate clients into JetStream features Rust
/// does not implement, so it is omitted rather than faked.
#[derive(Debug, Clone, Serialize)]
pub struct ServerInfo {
    pub server_id: String,
    pub server_name: String,
    pub version: String,
    pub proto: i32,
    pub headers: bool,
    pub max_payload: usize,
    pub host: String,
    pub port: u16,
    pub client_id: u64,
    #[serde(skip_serializing_if = "str_is_empty")]
    pub nonce: String,
    #[serde(skip_serializing_if = "is_false")]
    pub auth_required: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub tls_required: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub jetstream: bool,
    pub client_ip: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
}

/// A parsed client operation.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientOp {
    Connect(Box<ConnectInfo>),
    Ping,
    Pong,
    Pub {
        subject: String,
        reply_to: Option<String>,
        payload: Bytes,
    },
    HPub {
        subject: String,
        reply_to: Option<String>,
        /// Raw header block, `hdr_len` bytes, including its trailing CRLFCRLF.
        headers: Bytes,
        payload: Bytes,
    },
    Sub {
        subject: String,
        queue: Option<String>,
        sid: String,
    },
    Unsub {
        sid: String,
        max: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    UnknownOperation(String),
    InvalidArgs(String),
    InvalidSubject(String),
    InvalidSize(String),
    PayloadTooLarge(usize),
    ControlLineTooLong,
    InvalidPayloadTerminator,
    InvalidConnect(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Wire error texts are Go's generic texts exactly: no op
            // name, size, subject, or serde detail is ever appended, so no
            // parser internals leak onto the wire.
            ParseError::UnknownOperation(_) => write!(f, "Unknown Protocol Operation"),
            ParseError::InvalidArgs(_) => write!(f, "Invalid Protocol Operation"),
            ParseError::InvalidSubject(_) => write!(f, "Invalid Subject"),
            ParseError::InvalidSize(_) => write!(f, "Invalid Payload Size"),
            ParseError::PayloadTooLarge(_) => write!(f, "Maximum Payload Violation"),
            ParseError::ControlLineTooLong => write!(f, "Control Line Too Long"),
            ParseError::InvalidPayloadTerminator => write!(f, "Invalid Payload Terminator"),
            // Never sent to the wire: malformed CONNECT closes silently
            // (see handle_conn), matching Go.
            ParseError::InvalidConnect(_) => write!(f, "Invalid CONNECT"),
        }
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug, Clone)]
struct PendingPayload {
    subject: String,
    reply_to: Option<String>,
    /// `Some(hdr_len)` for HPUB, `None` for PUB.
    hdr_len: Option<usize>,
    total_len: usize,
}

#[derive(Debug, Clone, Default)]
enum State {
    #[default]
    OpLine,
    Payload(PendingPayload),
}

/// Incremental parser for the client protocol.
pub struct Parser {
    buf: BytesMut,
    state: State,
    max_payload: usize,
    max_control_line: usize,
}

impl Default for Parser {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_PAYLOAD)
    }
}

impl Parser {
    pub fn new(max_payload: usize) -> Self {
        Parser {
            buf: BytesMut::new(),
            state: State::OpLine,
            max_payload,
            max_control_line: DEFAULT_MAX_CONTROL_LINE,
        }
    }

    pub fn set_max_payload(&mut self, max_payload: usize) {
        self.max_payload = max_payload;
    }

    /// Feed raw bytes received from the socket.
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Try to parse the next complete operation. Returns `Ok(None)` when more
    /// bytes are needed.
    pub fn next_op(&mut self) -> Result<Option<ClientOp>, ParseError> {
        match &self.state {
            State::OpLine => self.parse_op_line(),
            State::Payload(_) => self.parse_payload(),
        }
    }

    fn parse_op_line(&mut self) -> Result<Option<ClientOp>, ParseError> {
        let Some(nl) = self.buf.iter().position(|&b| b == b'\n') else {
            if self.buf.len() > self.max_control_line {
                return Err(ParseError::ControlLineTooLong);
            }
            return Ok(None);
        };
        if nl + 1 > self.max_control_line {
            return Err(ParseError::ControlLineTooLong);
        }
        let mut line = self.buf.split_to(nl + 1);
        // Strip trailing \n and \r.
        let _ = line.split_off(line.len() - 1);
        if line.last() == Some(&b'\r') {
            let _ = line.split_off(line.len() - 1);
        }
        let line_str = String::from_utf8_lossy(&line).into_owned();
        self.dispatch_line(&line_str)
    }

    fn dispatch_line(&mut self, line: &str) -> Result<Option<ClientOp>, ParseError> {
        let (verb, rest) = match line.find(' ') {
            Some(i) => (&line[..i], line[i + 1..].trim_start()),
            None => (line, ""),
        };
        match verb.to_ascii_uppercase().as_str() {
            "CONNECT" => {
                let info: ConnectInfo = serde_json::from_str(rest).map_err(|e| {
                    eprintln!("rust-natsd: bad CONNECT ({e}): {rest}");
                    ParseError::InvalidConnect(e.to_string())
                })?;
                Ok(Some(ClientOp::Connect(Box::new(info))))
            }
            "PING" => Ok(Some(ClientOp::Ping)),
            "PONG" => Ok(Some(ClientOp::Pong)),
            "PUB" | "HPUB" => {
                let is_hpub = verb.eq_ignore_ascii_case("HPUB");
                let args: Vec<&str> = rest.split_whitespace().collect();
                let (subject, reply_to, sizes) = match args.len() {
                    2 => (args[0], None, &args[1..]),
                    3 if is_hpub => (args[0], None, &args[1..]),
                    3 => (args[0], Some(args[1].to_string()), &args[2..]),
                    4 if is_hpub => (args[0], Some(args[1].to_string()), &args[2..]),
                    _ => {
                        return Err(ParseError::InvalidArgs(format!(
                            "{} expects subject [reply-to] size",
                            verb.to_ascii_uppercase()
                        )))
                    }
                };
                // Go accepts publish subjects at parse time regardless of
                // validity (including wildcards and empty tokens); pedantic
                // errors and the empty-token delivery drop are applied in
                // process_op, never here, and never by closing the conn.
                let pending = if is_hpub {
                    if sizes.len() != 2 {
                        return Err(ParseError::InvalidArgs(
                            "HPUB expects hdr_len total_len".into(),
                        ));
                    }
                    let hdr_len: usize = sizes[0]
                        .parse()
                        .map_err(|_| ParseError::InvalidSize(sizes[0].into()))?;
                    let total_len: usize = sizes[1]
                        .parse()
                        .map_err(|_| ParseError::InvalidSize(sizes[1].into()))?;
                    if hdr_len > total_len {
                        return Err(ParseError::InvalidSize(
                            "header length exceeds total length".into(),
                        ));
                    }
                    if total_len - hdr_len > self.max_payload {
                        return Err(ParseError::PayloadTooLarge(total_len - hdr_len));
                    }
                    PendingPayload {
                        subject: subject.to_string(),
                        reply_to,
                        hdr_len: Some(hdr_len),
                        total_len,
                    }
                } else {
                    let size: usize = sizes[0]
                        .parse()
                        .map_err(|_| ParseError::InvalidSize(sizes[0].into()))?;
                    if size > self.max_payload {
                        return Err(ParseError::PayloadTooLarge(size));
                    }
                    PendingPayload {
                        subject: subject.to_string(),
                        reply_to,
                        hdr_len: None,
                        total_len: size,
                    }
                };
                self.state = State::Payload(pending);
                self.parse_payload()
            }
            "SUB" => {
                let args: Vec<&str> = rest.split_whitespace().collect();
                let (subject, queue, sid) = match args.len() {
                    2 => (args[0], None, args[1]),
                    3 => (args[0], Some(args[1].to_string()), args[2]),
                    _ => {
                        return Err(ParseError::InvalidArgs(
                            "SUB expects subject [queue] sid".into(),
                        ))
                    }
                };
                // Go validates SUB subjects in process_op (non-fatal
                // -ERR, connection stays alive), not in the parser.
                Ok(Some(ClientOp::Sub {
                    subject: subject.to_string(),
                    queue,
                    sid: sid.to_string(),
                }))
            }
            "UNSUB" => {
                let args: Vec<&str> = rest.split_whitespace().collect();
                match args.len() {
                    1 => Ok(Some(ClientOp::Unsub {
                        sid: args[0].to_string(),
                        max: None,
                    })),
                    2 => {
                        let max: u64 = args[1]
                            .parse()
                            .map_err(|_| ParseError::InvalidSize(args[1].into()))?;
                        Ok(Some(ClientOp::Unsub {
                            sid: args[0].to_string(),
                            max: Some(max),
                        }))
                    }
                    _ => Err(ParseError::InvalidArgs("UNSUB expects sid [max]".into())),
                }
            }
            other => Err(ParseError::UnknownOperation(other.to_string())),
        }
    }

    fn parse_payload(&mut self) -> Result<Option<ClientOp>, ParseError> {
        let State::Payload(pending) = self.state.clone() else {
            unreachable!()
        };
        let need = pending.total_len + 2; // payload + CRLF
        if self.buf.len() < need {
            return Ok(None);
        }
        if &self.buf[pending.total_len..need] != b"\r\n" {
            return Err(ParseError::InvalidPayloadTerminator);
        }
        let mut data = self.buf.split_to(need);
        let _ = data.split_off(pending.total_len);
        let payload_bytes: Bytes = data.freeze();
        self.state = State::OpLine;
        match pending.hdr_len {
            None => Ok(Some(ClientOp::Pub {
                subject: pending.subject,
                reply_to: pending.reply_to,
                payload: payload_bytes,
            })),
            Some(hdr_len) => {
                let headers = payload_bytes.slice(..hdr_len);
                let payload = payload_bytes.slice(hdr_len..);
                Ok(Some(ClientOp::HPub {
                    subject: pending.subject,
                    reply_to: pending.reply_to,
                    headers,
                    payload,
                }))
            }
        }
    }
}

/// Encode the server `INFO` operation (line included, CRLF terminated).
pub fn encode_info(info: &ServerInfo) -> Bytes {
    let json = serde_json::to_string(info).expect("ServerInfo serializes");
    Bytes::from(format!("INFO {json}\r\n"))
}

/// Encode a `MSG` delivery.
pub fn encode_msg(subject: &str, sid: &str, reply_to: Option<&str>, payload: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(payload.len() + subject.len() + sid.len() + 32);
    match reply_to {
        Some(r) => out
            .extend_from_slice(format!("MSG {subject} {sid} {r} {}\r\n", payload.len()).as_bytes()),
        None => {
            out.extend_from_slice(format!("MSG {subject} {sid} {}\r\n", payload.len()).as_bytes())
        }
    }
    out.extend_from_slice(payload);
    out.extend_from_slice(b"\r\n");
    out.freeze()
}

/// Encode an `HMSG` delivery. `headers` is the raw header block as received
/// in `HPUB` (starts with `NATS/1.0\r\n`, ends with `\r\n\r\n`).
pub fn encode_hmsg(
    subject: &str,
    sid: &str,
    reply_to: Option<&str>,
    headers: &[u8],
    payload: &[u8],
) -> Bytes {
    let total = headers.len() + payload.len();
    let mut out = BytesMut::with_capacity(total + subject.len() + sid.len() + 40);
    match reply_to {
        Some(r) => out.extend_from_slice(
            format!("HMSG {subject} {sid} {r} {} {total}\r\n", headers.len()).as_bytes(),
        ),
        None => out.extend_from_slice(
            format!("HMSG {subject} {sid} {} {total}\r\n", headers.len()).as_bytes(),
        ),
    }
    out.extend_from_slice(headers);
    out.extend_from_slice(payload);
    out.extend_from_slice(b"\r\n");
    out.freeze()
}

pub fn encode_pong() -> Bytes {
    Bytes::from_static(b"PONG\r\n")
}

pub fn encode_ping() -> Bytes {
    Bytes::from_static(b"PING\r\n")
}

pub fn encode_ok() -> Bytes {
    Bytes::from_static(b"+OK\r\n")
}

pub fn encode_err(msg: &str) -> Bytes {
    let escaped = msg.replace('\'', " ");
    Bytes::from(format!("-ERR '{escaped}'\r\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_all(input: &[u8]) -> Result<Vec<ClientOp>, ParseError> {
        let mut p = Parser::default();
        p.feed(input);
        let mut ops = Vec::new();
        while let Some(op) = p.next_op()? {
            ops.push(op);
        }
        Ok(ops)
    }

    /// Feed the input split at every possible byte boundary (and byte-by-byte)
    /// and require identical results — the parser must be split-safe.
    fn parse_all_split(input: &[u8]) -> Result<Vec<ClientOp>, ParseError> {
        let mut p = Parser::default();
        let mut ops = Vec::new();
        for i in 0..input.len() {
            p.feed(&input[i..i + 1]);
            while let Some(op) = p.next_op()? {
                ops.push(op);
            }
        }
        Ok(ops)
    }

    #[test]
    fn connect_and_ping() {
        let ops = parse_all(b"CONNECT {\"name\":\"t\",\"headers\":true}\r\nPING\r\n").unwrap();
        assert_eq!(ops.len(), 2);
        match &ops[0] {
            ClientOp::Connect(c) => {
                assert_eq!(c.name, "t");
                assert!(c.headers);
                assert!(c.echo, "echo defaults to true");
            }
            other => panic!("expected CONNECT, got {other:?}"),
        }
        assert_eq!(ops[1], ClientOp::Ping);
    }

    #[test]
    fn connect_with_explicit_nulls_like_async_nats() {
        // Real CONNECT line captured from async-nats 0.38: string fields are
        // sent as explicit null and must not fail parsing.
        let ops = parse_all(
            b"CONNECT {\"verbose\":false,\"pedantic\":false,\"jwt\":null,\"nkey\":null,\"sig\":null,\"name\":null,\"echo\":true,\"lang\":\"rust\",\"version\":\"0.38.0\",\"protocol\":1,\"tls_required\":false,\"user\":null,\"pass\":null,\"auth_token\":null,\"headers\":true,\"no_responders\":true}\r\n",
        )
        .unwrap();
        match &ops[0] {
            ClientOp::Connect(c) => {
                assert_eq!(c.name, "");
                assert_eq!(c.lang, "rust");
                assert!(c.headers);
                assert!(c.no_responders);
                assert_eq!(c.user, None);
            }
            other => panic!("expected CONNECT, got {other:?}"),
        }
    }

    #[test]
    fn pub_with_and_without_reply() {
        let ops = parse_all(b"PUB foo 5\r\nhello\r\nPUB foo inbox 3\r\nbye\r\n").unwrap();
        assert_eq!(
            ops,
            vec![
                ClientOp::Pub {
                    subject: "foo".into(),
                    reply_to: None,
                    payload: Bytes::from_static(b"hello"),
                },
                ClientOp::Pub {
                    subject: "foo".into(),
                    reply_to: Some("inbox".into()),
                    payload: Bytes::from_static(b"bye"),
                },
            ]
        );
    }

    #[test]
    fn pub_binary_payload_with_crlf_inside() {
        let ops = parse_all(b"PUB foo 4\r\na\r\nb\r\n").unwrap();
        match &ops[0] {
            ClientOp::Pub { payload, .. } => assert_eq!(payload.as_ref(), b"a\r\nb"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn hpub_splits_headers_and_payload() {
        let hdrs = b"NATS/1.0\r\nX-Test: 1\r\n\r\n";
        let mut wire = format!("HPUB foo {} {}\r\n", hdrs.len(), hdrs.len() + 5).into_bytes();
        wire.extend_from_slice(hdrs);
        wire.extend_from_slice(b"hello\r\n");
        let ops = parse_all(&wire).unwrap();
        match &ops[0] {
            ClientOp::HPub {
                headers, payload, ..
            } => {
                assert_eq!(headers.as_ref(), hdrs.as_slice());
                assert_eq!(payload.as_ref(), b"hello");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn sub_unsub_variants() {
        let ops = parse_all(b"SUB foo 1\r\nSUB foo q1 2\r\nUNSUB 1\r\nUNSUB 2 5\r\n").unwrap();
        assert_eq!(
            ops,
            vec![
                ClientOp::Sub {
                    subject: "foo".into(),
                    queue: None,
                    sid: "1".into()
                },
                ClientOp::Sub {
                    subject: "foo".into(),
                    queue: Some("q1".into()),
                    sid: "2".into()
                },
                ClientOp::Unsub {
                    sid: "1".into(),
                    max: None
                },
                ClientOp::Unsub {
                    sid: "2".into(),
                    max: Some(5)
                },
            ]
        );
    }

    #[test]
    fn split_at_every_byte() {
        let mut input = Vec::new();
        input.extend_from_slice(
            b"CONNECT {\"verbose\":true}\r\nSUB foo.> 9\r\nPUB foo.bar 5\r\nhello\r\nPING\r\n",
        );
        let whole = parse_all(&input).unwrap();
        let split = parse_all_split(&input).unwrap();
        assert_eq!(whole, split);
        assert_eq!(split.len(), 4);
    }

    #[test]
    fn incomplete_input_waits() {
        let mut p = Parser::default();
        p.feed(b"PUB foo 5\r\nhel");
        assert_eq!(p.next_op().unwrap(), None);
        p.feed(b"lo\r\n");
        assert!(matches!(p.next_op().unwrap(), Some(ClientOp::Pub { .. })));
    }

    #[test]
    fn errors() {
        assert!(matches!(
            parse_all(b"WAT\r\n"),
            Err(ParseError::UnknownOperation(_))
        ));
        // Go behaviour: a wildcard publish subject parses fine; it is
        // delivered literally, with a pedantic -ERR only in process_op.
        assert!(matches!(
            parse_all(b"PUB foo.* 1\r\nx\r\n"),
            Ok(v) if matches!(v[0], ClientOp::Pub { .. })
        ));
        assert!(matches!(
            parse_all(b"PUB foo abc\r\n"),
            Err(ParseError::InvalidSize(_))
        ));
        assert!(matches!(
            parse_all(b"CONNECT {nope}\r\n"),
            Err(ParseError::InvalidConnect(_))
        ));
        // Go behaviour: an invalid SUB subject parses fine; process_op
        // replies -ERR without creating the subscription or closing.
        assert!(matches!(
            parse_all(b"SUB foo.>.bar 1\r\n"),
            Ok(v) if matches!(v[0], ClientOp::Sub { .. })
        ));
        // Payload exceeding max.
        let mut p = Parser::new(4);
        p.feed(b"PUB foo 5\r\nhello\r\n");
        assert!(matches!(p.next_op(), Err(ParseError::PayloadTooLarge(5))));
        // Bad payload terminator.
        assert!(matches!(
            parse_all(b"PUB foo 2\r\nabXX"),
            Err(ParseError::InvalidPayloadTerminator)
        ));
        // Control line too long.
        let mut p = Parser::default();
        p.feed(&vec![b'A'; DEFAULT_MAX_CONTROL_LINE + 1]);
        assert!(matches!(p.next_op(), Err(ParseError::ControlLineTooLong)));
    }

    #[test]
    fn pong_and_hpub_with_reply() {
        let ops = parse_all(b"PONG\r\n").unwrap();
        assert_eq!(ops, vec![ClientOp::Pong]);
        let hdrs = b"NATS/1.0\r\n\r\n";
        let mut wire =
            format!("HPUB foo inbox.1 {} {}\r\n", hdrs.len(), hdrs.len() + 2).into_bytes();
        wire.extend_from_slice(hdrs);
        wire.extend_from_slice(b"hi\r\n");
        let ops = parse_all(&wire).unwrap();
        match &ops[0] {
            ClientOp::HPub {
                reply_to, payload, ..
            } => {
                assert_eq!(reply_to.as_deref(), Some("inbox.1"));
                assert_eq!(payload.as_ref(), b"hi");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn more_error_branches() {
        // HPUB header length beyond total length.
        assert!(matches!(
            parse_all(b"HPUB foo 10 5\r\n"),
            Err(ParseError::InvalidSize(_))
        ));
        // PUB with wildcard reply subject: Go accepts it at parse time
        // (reply subjects are carried, not validated, in this probe).
        assert!(matches!(
            parse_all(b"PUB foo a.* 1\r\nx\r\n"),
            Ok(v) if matches!(v[0], ClientOp::Pub { .. })
        ));
        // Wrong arg counts.
        assert!(matches!(
            parse_all(b"SUB foo\r\n"),
            Err(ParseError::InvalidArgs(_))
        ));
        assert!(matches!(
            parse_all(b"UNSUB 1 2 3\r\n"),
            Err(ParseError::InvalidArgs(_))
        ));
        assert!(matches!(
            parse_all(b"PUB foo bar baz 1\r\nx\r\n"),
            Err(ParseError::InvalidArgs(_))
        ));
        // UNSUB with non-numeric max.
        assert!(matches!(
            parse_all(b"UNSUB 1 x\r\n"),
            Err(ParseError::InvalidSize(_))
        ));
        // HPUB payload exceeding max payload (accounting headers).
        let mut p = Parser::new(4);
        let hdrs = b"NATS/1.0\r\n\r\n";
        p.feed(format!("HPUB foo {} {}\r\n", hdrs.len(), hdrs.len() + 5).as_bytes());
        assert!(matches!(p.next_op(), Err(ParseError::PayloadTooLarge(5))));
        // Display strings are stable (they go on the wire in -ERR).
        assert!(ParseError::ControlLineTooLong
            .to_string()
            .contains("Control Line Too Long"));
    }

    #[test]
    fn lowercase_verbs_accepted() {
        let ops = parse_all(b"ping\r\npub foo 1\r\nx\r\n").unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0], ClientOp::Ping);
    }

    #[test]
    fn encoders() {
        let info = ServerInfo {
            server_id: "ND123".into(),
            server_name: "rust-natsd".into(),
            version: "0.1.0".into(),
            proto: 1,
            headers: true,
            max_payload: 1024,
            host: "0.0.0.0".into(),
            port: 4222,
            client_id: 7,
            nonce: "test-nonce".into(),
            auth_required: false,
            tls_required: false,
            jetstream: false,
            client_ip: "127.0.0.1".into(),
            cluster: None,
        };
        let bytes = encode_info(&info);
        let s = String::from_utf8_lossy(&bytes);
        assert!(s.starts_with("INFO {"));
        assert!(s.contains("\"server_name\":\"rust-natsd\""));
        assert!(s.ends_with("}\r\n"));

        let msg = encode_msg("foo", "1", None, b"hi");
        assert_eq!(msg.as_ref(), b"MSG foo 1 2\r\nhi\r\n");
        let msg = encode_msg("foo", "1", Some("_INBOX.x"), b"hi");
        assert_eq!(msg.as_ref(), b"MSG foo 1 _INBOX.x 2\r\nhi\r\n");

        let hdrs = b"NATS/1.0\r\nA: b\r\n\r\n";
        let hmsg = encode_hmsg("foo", "2", None, hdrs, b"yo");
        let mut want = format!("HMSG foo 2 {} {}\r\n", hdrs.len(), hdrs.len() + 2).into_bytes();
        want.extend_from_slice(hdrs);
        want.extend_from_slice(b"yo\r\n");
        assert_eq!(hmsg.as_ref(), want.as_slice());

        assert_eq!(encode_ping().as_ref(), b"PING\r\n");
        assert_eq!(encode_pong().as_ref(), b"PONG\r\n");
        assert_eq!(encode_ok().as_ref(), b"+OK\r\n");
        assert_eq!(
            encode_err("Authorization Violation").as_ref(),
            b"-ERR 'Authorization Violation'\r\n"
        );
    }
}
