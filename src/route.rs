//! Route (cluster) protocol: the inter-server protocol spoken by
//! `nats-server` on its cluster port, verified against Go nats-server v2.15.1.
//!
//! Handshake (both directions):
//!   dialer:   CONNECT {name: <server_id>, cluster, headers, lnoc, ln}
//!   accepter: INFO {server_id, server_name, version, proto: 3, host, port,
//!                    headers, max_payload, cluster, connect_urls, ip}
//!   dialer:   INFO {same shape, port = its own cluster port}
//!   both:     RS+ $G <subject> [queue weight] for every local subscription
//!
//! Message framing (account is `$G` for the default account, `$SYS` for the
//! system account; the account token is the message's account and is
//! used to scope delivery on the receiving node):
//!   RS+ $G <subject> [queue weight]   RS- $G <subject> [queue]
//!   RMSG $G <subject> [reply] [| queue...] <size>
//!   HMSG $G <subject> [reply] <hdr_len> <total_len>   (headers, like client
//!     HMSG but with the account token and no subscription id)
//! Queue groups on RMSG are listed after a `|` marker, as Go sends them.
//! Plain (non-header) messages with a reply put the reply before the `|`.

use bytes::{Bytes, BytesMut};

/// One parsed route operation.
#[derive(Debug, Clone, PartialEq)]
pub enum RouteOp {
    Info(String),
    Connect(String),
    RsPlus {
        account: String,
        subject: String,
        queue: Option<String>,
    },
    RsMinus {
        account: String,
        subject: String,
        queue: Option<String>,
    },
    /// A routed message. `headers` is the raw header block when the frame
    /// was HMSG (starts with `NATS/1.0\r\n`, ends with `\r\n\r\n`).
    RMsg {
        account: String,
        subject: String,
        reply_to: Option<String>,
        queues: Vec<String>,
        headers: Option<Bytes>,
        payload: Bytes,
    },
    Ping,
    Pong,
    Err(String),
}

#[derive(Debug)]
pub enum RouteParseError {
    InvalidArgs(String),
    InvalidSubject(String),
    InvalidSize(String),
    ControlLineTooLong,
    PayloadTooLarge(usize),
}

impl std::fmt::Display for RouteParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteParseError::InvalidArgs(s) => write!(f, "Invalid Args: {s}"),
            RouteParseError::InvalidSubject(s) => write!(f, "Invalid Subject: {s}"),
            RouteParseError::InvalidSize(s) => write!(f, "Invalid Size: {s}"),
            RouteParseError::ControlLineTooLong => write!(f, "Control Line Too Long"),
            RouteParseError::PayloadTooLarge(n) => {
                write!(f, "Maximum Payload Violation: {n} bytes")
            }
        }
    }
}

const MAX_CONTROL_LINE: usize = 4096;

enum Pending {
    None,
    /// Waiting for `total` bytes of frame (headers + payload) plus CRLF.
    Msg {
        op: RouteOp,
        hdr_len: usize,
        total: usize,
    },
}

/// Incremental parser for the route protocol. Feed arbitrary byte chunks;
/// `next_op` yields complete operations. Payloads are delivered as `Bytes`.
pub struct RouteParser {
    buf: BytesMut,
    pending: Pending,
    max_payload: usize,
}

impl RouteParser {
    pub fn new(max_payload: usize) -> Self {
        RouteParser {
            buf: BytesMut::new(),
            pending: Pending::None,
            max_payload,
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    pub fn next_op(&mut self) -> Result<Option<RouteOp>, RouteParseError> {
        loop {
            // Finish a pending message frame first.
            if let Pending::Msg { op, hdr_len, total } = &mut self.pending {
                let need = *total + 2;
                if self.buf.len() < need {
                    return Ok(None);
                }
                // Zero-copy payload: freeze the split-off frame and
                // slice it, exactly like the client parser. (This used
                // to Bytes::copy_from_slice the body here.)
                let mut frame = self.buf.split_to(need);
                let _ = frame.split_off(*total); // drop trailing CRLF
                let body = frame.freeze();
                let (headers, payload) = if *hdr_len > 0 {
                    (Some(body.slice(..*hdr_len)), body.slice(*hdr_len..))
                } else {
                    (None, body)
                };
                let mut op = std::mem::replace(
                    op,
                    RouteOp::Ping, // placeholder, replaced below
                );
                if let RouteOp::RMsg {
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

            // Find a complete control line.
            let Some(pos) = find_crlf(&self.buf) else {
                if self.buf.len() > MAX_CONTROL_LINE {
                    return Err(RouteParseError::ControlLineTooLong);
                }
                return Ok(None);
            };
            let line = self.buf.split_to(pos + 2);
            let line = &line[..line.len() - 2];
            let text = match std::str::from_utf8(line) {
                Ok(t) => t,
                Err(_) => return Err(RouteParseError::InvalidArgs("non-utf8 line".into())),
            };
            match parse_route_line(text, self.max_payload, &mut self.pending)? {
                Some(op) => return Ok(Some(op)),
                // Control line consumed a frame header (pending set) or was
                // ignorable: loop to finish the frame / read the next op.
                None => continue,
            }
        }
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

/// Split a control line into (verb, rest). Verbs are case-insensitive.
fn split_verb(text: &str) -> (&str, &str) {
    match text.find([' ', '\t']) {
        Some(i) => (&text[..i], text[i..].trim_start()),
        None => (text, ""),
    }
}

fn parse_route_line(
    text: &str,
    max_payload: usize,
    pending: &mut Pending,
) -> Result<Option<RouteOp>, RouteParseError> {
    let (verb, rest) = split_verb(text);
    let verb_up = verb.to_ascii_uppercase();
    match verb_up.as_str() {
        "INFO" => Ok(Some(RouteOp::Info(rest.to_string()))),
        "CONNECT" => Ok(Some(RouteOp::Connect(rest.to_string()))),
        "PING" => Ok(Some(RouteOp::Ping)),
        "PONG" => Ok(Some(RouteOp::Pong)),
        "RS+" | "RS-" => {
            let args: Vec<&str> = rest.split_whitespace().collect();
            if args.len() < 2 || args.len() > 4 {
                return Err(RouteParseError::InvalidArgs(format!("{verb} {rest}")));
            }
            let account = args[0].to_string();
            let subject = args[1].to_string();
            if !crate::subject::is_valid_subscribe_subject(&subject) {
                return Err(RouteParseError::InvalidSubject(subject));
            }
            let queue = if args.len() >= 3 {
                Some(args[2].to_string())
            } else {
                None
            };
            // args[3], when present, is the queue weight; accepted, ignored.
            if verb_up == "RS+" {
                Ok(Some(RouteOp::RsPlus {
                    account,
                    subject,
                    queue,
                }))
            } else {
                Ok(Some(RouteOp::RsMinus {
                    account,
                    subject,
                    queue,
                }))
            }
        }
        "RMSG" | "HMSG" => {
            let args: Vec<&str> = rest.split_whitespace().collect();
            if args.len() < 3 {
                return Err(RouteParseError::InvalidArgs(format!("{verb} {rest}")));
            }
            let account = args[0].to_string();
            // Route publish subjects are literal (a client may publish
            // `foo.*` literally); never reject them here.
            let subject = args[1].to_string();
            let rest_args = &args[2..];
            // Pull trailing numeric sizes off the end (1 = plain size,
            // 2 = hdr_len + total_len for HMSG).
            let mut sizes: Vec<usize> = Vec::new();
            let mut mid = rest_args.to_vec();
            while let Some(last) = mid.last() {
                if let Ok(n) = last.parse::<usize>() {
                    sizes.push(n);
                    mid.pop();
                } else {
                    break;
                }
            }
            sizes.reverse();
            let (reply_to, queues, hdr_len, total) = if verb_up == "HMSG" {
                if sizes.len() != 2 {
                    return Err(RouteParseError::InvalidArgs(format!("{verb} {rest}")));
                }
                let reply_to = if !mid.is_empty() {
                    Some(mid[0].to_string())
                } else {
                    None
                };
                (reply_to, Vec::new(), sizes[0], sizes[1])
            } else {
                // RMSG: middle tokens may hold a reply and/or `|` queues.
                let mut reply_to = None;
                let mut queues = Vec::new();
                let mut i = 0;
                while i < mid.len() {
                    if mid[i] == "|" {
                        queues = mid[i + 1..].iter().map(|s| s.to_string()).collect();
                        break;
                    }
                    if reply_to.is_none() {
                        reply_to = Some(mid[i].to_string());
                    }
                    i += 1;
                }
                if sizes.len() != 1 {
                    return Err(RouteParseError::InvalidArgs(format!("{verb} {rest}")));
                }
                (reply_to, queues, 0, sizes[0])
            };
            if total > max_payload {
                return Err(RouteParseError::PayloadTooLarge(total));
            }
            let op = RouteOp::RMsg {
                account,
                subject,
                reply_to,
                queues,
                headers: None,
                payload: Bytes::new(),
            };
            *pending = Pending::Msg { op, hdr_len, total };
            Ok(None)
        }
        "-ERR" => Ok(Some(RouteOp::Err(rest.to_string()))),
        _ => Err(RouteParseError::InvalidArgs(format!(
            "unknown route operation: {verb}"
        ))),
    }
}

/// Route INFO JSON for this server (cluster listener address).
pub fn route_info_json(
    server_id: &str,
    server_name: &str,
    cluster_name: &str,
    cluster_host: &str,
    cluster_port: u16,
    max_payload: usize,
    client_addr: std::net::SocketAddr,
) -> String {
    serde_json::json!({
        "server_id": server_id,
        "server_name": server_name,
        "version": env!("CARGO_PKG_VERSION"),
        "proto": 3,
        "go": "rust",
        "host": cluster_host,
        "port": cluster_port,
        "headers": true,
        "max_payload": max_payload,
        "cluster": cluster_name,
        "connect_urls": [client_addr.to_string()],
        "ip": format!("nats-route://{cluster_host}:{cluster_port}/"),
    })
    .to_string()
}

/// Route CONNECT JSON (dialer sends this first; name is the server id).
pub fn route_connect_json(server_id: &str, cluster_name: &str) -> String {
    serde_json::json!({
        "echo": true,
        "verbose": false,
        "pedantic": false,
        "tls_required": false,
        "headers": true,
        "name": server_id,
        "cluster": cluster_name,
        "lnoc": true,
        "ln": true,
    })
    .to_string()
}

/// Encode `RS+ <account> <subject> [queue weight]`.
pub fn encode_rs_plus(account: &str, subject: &str, queue: Option<&str>) -> Bytes {
    match queue {
        Some(q) => Bytes::from(format!("RS+ {account} {subject} {q} 1\r\n")),
        None => Bytes::from(format!("RS+ {account} {subject}\r\n")),
    }
}

/// Encode `RS- <account> <subject> [queue]`.
pub fn encode_rs_minus(account: &str, subject: &str, queue: Option<&str>) -> Bytes {
    match queue {
        Some(q) => Bytes::from(format!("RS- {account} {subject} {q}\r\n")),
        None => Bytes::from(format!("RS- {account} {subject}\r\n")),
    }
}

/// Encode a routed plain message. Queue groups, when present, are listed
/// after a `|` marker, as Go sends them.
pub fn encode_rmsg(
    account: &str,
    subject: &str,
    reply_to: Option<&str>,
    queues: &[String],
    payload: &[u8],
) -> Bytes {
    let mut line = format!("RMSG {account} {subject}");
    if let Some(r) = reply_to {
        line.push(' ');
        line.push_str(r);
    }
    if !queues.is_empty() {
        line.push_str(" |");
        for q in queues {
            line.push(' ');
            line.push_str(q);
        }
    }
    line.push_str(&format!(" {}\r\n", payload.len()));
    let mut out = BytesMut::from(line.as_str());
    out.extend_from_slice(payload);
    out.extend_from_slice(b"\r\n");
    out.freeze()
}

/// Encode a routed header message (Go's route form: HMSG with account and
/// no subscription id).
pub fn encode_route_hmsg(
    account: &str,
    subject: &str,
    reply_to: Option<&str>,
    headers: &[u8],
    payload: &[u8],
) -> Bytes {
    let total = headers.len() + payload.len();
    let mut line = format!("HMSG {account} {subject}");
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ops(data: &[u8]) -> Vec<RouteOp> {
        let mut p = RouteParser::new(1024 * 1024);
        p.feed(data);
        let mut out = Vec::new();
        while let Ok(Some(op)) = p.next_op() {
            out.push(op);
        }
        out
    }

    #[test]
    fn rs_plus_minus_forms() {
        let got =
            ops(b"RS+ $G foo\r\nRS+ $G jobs workers 3\r\nRS- $G foo\r\nRS- $G jobs workers\r\n");
        assert_eq!(
            got[0],
            RouteOp::RsPlus {
                account: "$G".into(),
                subject: "foo".into(),
                queue: None,
            }
        );
        assert_eq!(
            got[1],
            RouteOp::RsPlus {
                account: "$G".into(),
                subject: "jobs".into(),
                queue: Some("workers".into()),
            }
        );
        assert_eq!(
            got[3],
            RouteOp::RsMinus {
                account: "$G".into(),
                subject: "jobs".into(),
                queue: Some("workers".into()),
            }
        );
    }

    #[test]
    fn rmsg_plain_reply_and_queue() {
        let got = ops(b"RMSG $G foo 5\r\nhello\r\nRMSG $G foo _INBOX.1 5\r\nworld\r\nRMSG $G jobs | workers 3\r\njob\r\n");
        match &got[0] {
            RouteOp::RMsg {
                subject,
                reply_to,
                queues,
                payload,
                headers,
                ..
            } => {
                assert_eq!(subject, "foo");
                assert_eq!(reply_to, &None);
                assert!(queues.is_empty());
                assert_eq!(headers, &None);
                assert_eq!(payload.as_ref(), b"hello");
            }
            other => panic!("{other:?}"),
        }
        match &got[2] {
            RouteOp::RMsg {
                queues, payload, ..
            } => {
                assert_eq!(queues, &vec!["workers".to_string()]);
                assert_eq!(payload.as_ref(), b"job");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn hmsg_route_with_headers() {
        let hdrs = b"NATS/1.0\r\nX-A: 1\r\n\r\n";
        let mut wire = format!("HMSG $G foo {} {}\r\n", hdrs.len(), hdrs.len() + 5).into_bytes();
        wire.extend_from_slice(hdrs);
        wire.extend_from_slice(b"hello\r\n");
        let got = ops(&wire);
        match &got[0] {
            RouteOp::RMsg {
                headers, payload, ..
            } => {
                assert_eq!(headers.as_ref().unwrap().as_ref(), hdrs);
                assert_eq!(payload.as_ref(), b"hello");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn split_at_every_byte() {
        let wire = b"RS+ $G foo\r\nRMSG $G foo 5\r\nhello\r\nPING\r\n";
        for split in 0..wire.len() {
            let mut p = RouteParser::new(1024 * 1024);
            p.feed(&wire[..split]);
            let mut got = Vec::new();
            while let Ok(Some(op)) = p.next_op() {
                got.push(op);
            }
            p.feed(&wire[split..]);
            while let Ok(Some(op)) = p.next_op() {
                got.push(op);
            }
            assert_eq!(got.len(), 3, "split at {split}");
        }
    }
}
