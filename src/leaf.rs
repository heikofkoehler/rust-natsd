//! Leaf node protocol: the hub-and-spoke protocol spoken by `nats-server`
//! on its leafnode port, per docs.nats.io/reference/protocols/leafnode.
//!
//! Handshake (leaf solicits the hub):
//!   hub:  INFO {server_id, version, proto: 1, host, port, headers,
//!                max_payload, jetstream, cluster, domain}
//!   leaf: INFO {same shape, port = its own leafnode port}
//!   leaf: CONNECT {version, server_id, name, cluster, hub: false, headers,
//!                  jetstream, proto: 1, [user, pass, auth_token]}
//!   both: LS+ <subject> [queue [weight]] for every local subscription
//!
//! Message framing:
//!   LS+ <subject> [queue [weight]]     LS- <subject> [queue]
//!   LMSG <subject> [+ reply] [| queue...] <size>
//!   LMSG <subject> [+ reply] [| queue...] <hdr_len> <total_len>  (headers)
//!   LDS <subject>                        (loop detection; accepted, ignored)

use bytes::{Bytes, BytesMut};

#[derive(Debug, Clone, PartialEq)]
pub enum LeafOp {
    Info(String),
    Connect(String),
    LsPlus {
        subject: String,
        queue: Option<String>,
    },
    LsMinus {
        subject: String,
        queue: Option<String>,
    },
    LMsg {
        subject: String,
        reply_to: Option<String>,
        /// Queue restriction from an LMSG `|` list. `None` for HMSG/MSG
        /// frames (Go sends header messages to leafs as HMSG, which
        /// carries no queue list) — those deliver to all matching groups.
        queues: Option<Vec<String>>,
        headers: Option<Bytes>,
        payload: Bytes,
    },
    Ping,
    Pong,
    Err(String),
}

#[derive(Debug)]
pub enum LeafParseError {
    InvalidArgs(String),
    InvalidSubject(String),
    InvalidSize(String),
    ControlLineTooLong,
    PayloadTooLarge(usize),
}

impl std::fmt::Display for LeafParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeafParseError::InvalidArgs(s) => write!(f, "Invalid Args: {s}"),
            LeafParseError::InvalidSubject(s) => write!(f, "Invalid Subject: {s}"),
            LeafParseError::InvalidSize(s) => write!(f, "Invalid Size: {s}"),
            LeafParseError::ControlLineTooLong => write!(f, "Control Line Too Long"),
            LeafParseError::PayloadTooLarge(n) => {
                write!(f, "Maximum Payload Violation: {n} bytes")
            }
        }
    }
}

const MAX_CONTROL_LINE: usize = 4096;

enum Pending {
    None,
    Msg {
        op: LeafOp,
        hdr_len: usize,
        total: usize,
    },
}

pub struct LeafParser {
    buf: BytesMut,
    pending: Pending,
    max_payload: usize,
}

impl LeafParser {
    pub fn new(max_payload: usize) -> Self {
        LeafParser {
            buf: BytesMut::new(),
            pending: Pending::None,
            max_payload,
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    pub fn next_op(&mut self) -> Result<Option<LeafOp>, LeafParseError> {
        loop {
            if let Pending::Msg { op, hdr_len, total } = &mut self.pending {
                let need = *total + 2;
                if self.buf.len() < need {
                    return Ok(None);
                }
                let frame = self.buf.split_to(need);
                let body = &frame[..*total];
                let (headers, payload) = if *hdr_len > 0 {
                    (
                        Some(Bytes::copy_from_slice(&body[..*hdr_len])),
                        Bytes::copy_from_slice(&body[*hdr_len..]),
                    )
                } else {
                    (None, Bytes::copy_from_slice(body))
                };
                let mut op = std::mem::replace(op, LeafOp::Ping);
                if let LeafOp::LMsg {
                    headers: h,
                    payload: p,
                    ..
                } = &mut op
                {
                    *h = headers;
                    *p = payload;
                }
                self.pending = Pending::None;
                return Ok(Some(op));
            }

            let Some(pos) = self.buf.windows(2).position(|w| w == b"\r\n") else {
                if self.buf.len() > MAX_CONTROL_LINE {
                    return Err(LeafParseError::ControlLineTooLong);
                }
                return Ok(None);
            };
            let line = self.buf.split_to(pos + 2);
            let line = &line[..line.len() - 2];
            let text = match std::str::from_utf8(line) {
                Ok(t) => t,
                Err(_) => return Err(LeafParseError::InvalidArgs("non-utf8 line".into())),
            };
            match parse_leaf_line(text, self.max_payload, &mut self.pending)? {
                Some(op) => return Ok(Some(op)),
                None => continue,
            }
        }
    }
}

fn split_verb(text: &str) -> (&str, &str) {
    match text.find([' ', '\t']) {
        Some(i) => (&text[..i], text[i..].trim_start()),
        None => (text, ""),
    }
}

fn parse_leaf_line(
    text: &str,
    max_payload: usize,
    pending: &mut Pending,
) -> Result<Option<LeafOp>, LeafParseError> {
    let (verb, rest) = split_verb(text);
    let verb_up = verb.to_ascii_uppercase();
    match verb_up.as_str() {
        "INFO" => Ok(Some(LeafOp::Info(rest.to_string()))),
        "CONNECT" => Ok(Some(LeafOp::Connect(rest.to_string()))),
        "PING" => Ok(Some(LeafOp::Ping)),
        "PONG" => Ok(Some(LeafOp::Pong)),
        "LDS" => Ok(None), // loop-detection subject: accepted, ignored
        "LS+" | "LS-" => {
            let args: Vec<&str> = rest.split_whitespace().collect();
            if args.is_empty() || args.len() > 3 {
                return Err(LeafParseError::InvalidArgs(format!("{verb} {rest}")));
            }
            let subject = args[0].to_string();
            if !crate::subject::is_valid_subscribe_subject(&subject) {
                return Err(LeafParseError::InvalidSubject(subject));
            }
            let queue = if args.len() >= 2 {
                Some(args[1].to_string())
            } else {
                None
            };
            if verb_up == "LS+" {
                Ok(Some(LeafOp::LsPlus { subject, queue }))
            } else {
                Ok(Some(LeafOp::LsMinus { subject, queue }))
            }
        }
        "LMSG" => {
            let args: Vec<&str> = rest.split_whitespace().collect();
            if args.len() < 2 {
                return Err(LeafParseError::InvalidArgs(format!("LMSG {rest}")));
            }
            // Leaf publish subjects are literal; never reject them here.
            let subject = args[0].to_string();
            let mut mid = args[1..].to_vec();
            // Trailing numerics are sizes: 1 = plain, 2 = hdr + total.
            let mut sizes: Vec<usize> = Vec::new();
            while let Some(last) = mid.last() {
                if let Ok(n) = last.parse::<usize>() {
                    sizes.push(n);
                    mid.pop();
                } else {
                    break;
                }
            }
            sizes.reverse();
            let (hdr_len, total) = match sizes.len() {
                1 => (0, sizes[0]),
                2 => (sizes[0], sizes[1]),
                _ => return Err(LeafParseError::InvalidArgs(format!("LMSG {rest}"))),
            };
            if total > max_payload {
                return Err(LeafParseError::PayloadTooLarge(total));
            }
            // Middle tokens: `+` reply and/or `|` queues, in either order.
            let mut reply_to = None;
            let mut queues = Vec::new();
            let mut i = 0;
            while i < mid.len() {
                match mid[i] {
                    "+" => {
                        if i + 1 < mid.len() {
                            reply_to = Some(mid[i + 1].to_string());
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    "|" => {
                        queues = mid[i + 1..].iter().map(|s| s.to_string()).collect();
                        break;
                    }
                    other => {
                        // Bare token before a `|`: treat as reply (tolerated).
                        if reply_to.is_none() {
                            reply_to = Some(other.to_string());
                        }
                        i += 1;
                    }
                }
            }
            let op = LeafOp::LMsg {
                subject,
                reply_to,
                queues: Some(queues),
                headers: None,
                payload: Bytes::new(),
            };
            *pending = Pending::Msg { op, hdr_len, total };
            Ok(None)
        }
        // Go sends header messages to leafs as HMSG (client-style, but
        // with no subscription id and no account): HMSG subject [reply]
        // hdr_len total_len. Plain MSG (subject sid [reply] size) is
        // accepted the same way for robustness.
        "HMSG" | "MSG" => {
            let args: Vec<&str> = rest.split_whitespace().collect();
            if verb_up == "HMSG" {
                if args.len() < 3 || args.len() > 4 {
                    return Err(LeafParseError::InvalidArgs(format!("HMSG {rest}")));
                }
                // Leaf publish subjects are literal; never reject them here.
                let subject = args[0].to_string();
                let (reply_to, hdr_len, total) = if args.len() == 3 {
                    (
                        None,
                        args[1]
                            .parse::<usize>()
                            .map_err(|_| LeafParseError::InvalidSize(args[1].to_string()))?,
                        args[2]
                            .parse::<usize>()
                            .map_err(|_| LeafParseError::InvalidSize(args[2].to_string()))?,
                    )
                } else {
                    (
                        Some(args[1].to_string()),
                        args[2]
                            .parse::<usize>()
                            .map_err(|_| LeafParseError::InvalidSize(args[2].to_string()))?,
                        args[3]
                            .parse::<usize>()
                            .map_err(|_| LeafParseError::InvalidSize(args[3].to_string()))?,
                    )
                };
                if total > max_payload {
                    return Err(LeafParseError::PayloadTooLarge(total));
                }
                let op = LeafOp::LMsg {
                    subject,
                    reply_to,
                    queues: None,
                    headers: None,
                    payload: Bytes::new(),
                };
                *pending = Pending::Msg { op, hdr_len, total };
                Ok(None)
            } else {
                if args.len() < 3 || args.len() > 4 {
                    return Err(LeafParseError::InvalidArgs(format!("MSG {rest}")));
                }
                // Leaf publish subjects are literal; never reject them here.
                let subject = args[0].to_string();
                let (reply_to, total) = if args.len() == 3 {
                    (
                        None,
                        args[2]
                            .parse::<usize>()
                            .map_err(|_| LeafParseError::InvalidSize(args[2].to_string()))?,
                    )
                } else {
                    (
                        Some(args[2].to_string()),
                        args[3]
                            .parse::<usize>()
                            .map_err(|_| LeafParseError::InvalidSize(args[3].to_string()))?,
                    )
                };
                if total > max_payload {
                    return Err(LeafParseError::PayloadTooLarge(total));
                }
                let op = LeafOp::LMsg {
                    subject,
                    reply_to,
                    queues: None,
                    headers: None,
                    payload: Bytes::new(),
                };
                *pending = Pending::Msg {
                    op,
                    hdr_len: 0,
                    total,
                };
                Ok(None)
            }
        }
        "-ERR" => Ok(Some(LeafOp::Err(rest.to_string()))),
        _ => Err(LeafParseError::InvalidArgs(format!(
            "unknown leaf operation: {verb}"
        ))),
    }
}

/// Leaf INFO JSON for this server.
#[allow(clippy::too_many_arguments)]
pub fn leaf_info_json(
    server_id: &str,
    server_name: &str,
    host: &str,
    port: u16,
    max_payload: usize,
    jetstream: bool,
    cluster_name: &str,
    client_id: u64,
    leafnode_urls: Option<&str>,
) -> String {
    serde_json::json!({
        "server_id": server_id,
        "server_name": server_name,
        "version": env!("CARGO_PKG_VERSION"),
        "proto": 1,
        "go": "rust",
        "host": host,
        "port": port,
        "headers": true,
        "max_payload": max_payload,
        "jetstream": jetstream,
        "cluster": cluster_name,
        "domain": "",
        // Go's soliciting leaf rejects a hub INFO without client_id
        // ("attempted to connect to wrong port"): it uses the field to
        // confirm the remote is a leaf listener, not a client port.
        "client_id": client_id,
        "leafnode_urls": leafnode_urls.map(|u| vec![u.to_string()]),
    })
    .to_string()
}

/// Leaf CONNECT JSON (leaf sends this to the hub; hub: false).
pub fn leaf_connect_json(
    server_id: &str,
    server_name: &str,
    cluster_name: &str,
    jetstream: bool,
    user: Option<&str>,
    pass: Option<&str>,
) -> String {
    let mut obj = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "server_id": server_id,
        "name": server_name,
        "cluster": cluster_name,
        "hub": false,
        "headers": true,
        "jetstream": jetstream,
        "proto": 1,
    });
    if let Some(u) = user {
        obj["user"] = serde_json::Value::String(u.to_string());
    }
    if let Some(p) = pass {
        obj["pass"] = serde_json::Value::String(p.to_string());
    }
    obj.to_string()
}

pub fn encode_ls_plus(subject: &str, queue: Option<&str>) -> Bytes {
    // Go's leaf parser requires the weight for queue subscriptions
    // (`LS+ subject queue weight`); a 2-arg queue form is a protocol
    // violation and Go closes the leaf connection.
    match queue {
        Some(q) => Bytes::from(format!("LS+ {subject} {q} 1\r\n")),
        None => Bytes::from(format!("LS+ {subject}\r\n")),
    }
}

pub fn encode_ls_minus(subject: &str, queue: Option<&str>) -> Bytes {
    match queue {
        Some(q) => Bytes::from(format!("LS- {subject} {q}\r\n")),
        None => Bytes::from(format!("LS- {subject}\r\n")),
    }
}

/// Encode a header message for a leaf connection. Go sends header
/// messages over leaf connections as HMSG (no subscription id, no
/// account) — not as LMSG with two sizes, which Go parses as a plain
/// message with the header block embedded in the payload.
pub fn encode_leaf_hmsg(
    subject: &str,
    reply_to: Option<&str>,
    headers: &[u8],
    payload: &[u8],
) -> Bytes {
    let total = headers.len() + payload.len();
    let mut line = format!("HMSG {subject}");
    if let Some(r) = reply_to {
        line.push(' ');
        line.push_str(r);
    }
    line.push_str(&format!(" {} {total}\r\n", headers.len()));
    let mut out = BytesMut::from(line.as_str());
    out.extend_from_slice(headers);
    out.extend_from_slice(payload);
    out.extend_from_slice(b"\r\n");
    out.freeze()
}

pub fn encode_lmsg(
    subject: &str,
    reply_to: Option<&str>,
    queues: &[String],
    headers: Option<&[u8]>,
    payload: &[u8],
) -> Bytes {
    let mut line = format!("LMSG {subject}");
    if let Some(r) = reply_to {
        line.push_str(&format!(" + {r}"));
    }
    if !queues.is_empty() {
        line.push_str(" |");
        for q in queues {
            line.push(' ');
            line.push_str(q);
        }
    }
    match headers {
        Some(h) => {
            let total = h.len() + payload.len();
            line.push_str(&format!(" {} {total}\r\n", h.len()));
            let mut out = BytesMut::from(line.as_str());
            out.extend_from_slice(h);
            out.extend_from_slice(payload);
            out.extend_from_slice(b"\r\n");
            out.freeze()
        }
        None => {
            line.push_str(&format!(" {}\r\n", payload.len()));
            let mut out = BytesMut::from(line.as_str());
            out.extend_from_slice(payload);
            out.extend_from_slice(b"\r\n");
            out.freeze()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ops(data: &[u8]) -> Vec<LeafOp> {
        let mut p = LeafParser::new(1024 * 1024);
        p.feed(data);
        let mut out = Vec::new();
        while let Ok(Some(op)) = p.next_op() {
            out.push(op);
        }
        out
    }

    #[test]
    fn ls_and_lmsg_forms() {
        let got = ops(b"LS+ foo\r\nLS+ jobs workers\r\nLS- foo\r\nLMSG foo 5\r\nhello\r\nLMSG foo + _INBOX.1 5\r\nworld\r\nLMSG jobs | workers 3\r\njob\r\n");
        assert_eq!(
            got[0],
            LeafOp::LsPlus {
                subject: "foo".into(),
                queue: None,
            }
        );
        match &got[3] {
            LeafOp::LMsg {
                subject,
                reply_to,
                payload,
                ..
            } => {
                assert_eq!(subject, "foo");
                assert_eq!(reply_to, &None);
                assert_eq!(payload.as_ref(), b"hello");
            }
            other => panic!("{other:?}"),
        }
        match &got[4] {
            LeafOp::LMsg { reply_to, .. } => {
                assert_eq!(reply_to.as_deref(), Some("_INBOX.1"));
            }
            other => panic!("{other:?}"),
        }
        match &got[5] {
            LeafOp::LMsg { queues, .. } => {
                assert_eq!(queues, &Some(vec!["workers".to_string()]));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn lmsg_with_headers() {
        let hdrs = b"NATS/1.0\r\nK: V\r\n\r\n";
        let mut wire = format!("LMSG foo {} {}\r\n", hdrs.len(), hdrs.len() + 2).into_bytes();
        wire.extend_from_slice(hdrs);
        wire.extend_from_slice(b"hi\r\n");
        let got = ops(&wire);
        match &got[0] {
            LeafOp::LMsg {
                headers, payload, ..
            } => {
                assert_eq!(headers.as_ref().unwrap().as_ref(), hdrs);
                assert_eq!(payload.as_ref(), b"hi");
            }
            other => panic!("{other:?}"),
        }
    }
}
