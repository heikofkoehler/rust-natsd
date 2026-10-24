//! Monitoring HTTP endpoint (Phase 10, upgraded in Phase 16): a minimal
//! hand-rolled HTTP/1.1 server answering nats-server-style JSON on
//! `/varz`, `/connz`, `/subz`, `/routez`, `/leafz`, `/jsz`,
//! `/accounz`, `/gatewayz` and `/healthz`.
//!
//! Only fields the server actually tracks are filled in; counts it does
//! not maintain (RTT, pending bytes) are reported as 0 rather than
//! invented. `/connz` honours `acc`, `state`, `limit` and `offset`
//! query parameters; other endpoints ignore the query string.
//! One request per connection; the connection is closed afterwards.

use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::jetstream::rfc3339;
use crate::server::Shared;

pub async fn accept_loop(listener: TcpListener, shared: std::sync::Arc<Shared>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            handle_conn(stream, &shared).await;
        });
    }
}

async fn handle_conn(mut stream: TcpStream, shared: &std::sync::Arc<Shared>) {
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let mut header_end = None;
    while buf.len() < 65536 {
        let Ok(n) = stream.read(&mut chunk).await else {
            return;
        };
        if n == 0 {
            return;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            header_end = Some(pos);
            break;
        }
    }
    let Some(end) = header_end else {
        return;
    };
    let head = String::from_utf8_lossy(&buf[..end]);
    let mut lines = head.lines();
    let Some(request_line) = lines.next() else {
        return;
    };
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("");
    if method != "GET" {
        respond(&mut stream, 405, json!({"error": "method not allowed"})).await;
        return;
    }
    let path = target.split('?').next().unwrap_or("");
    let query = target.split_once('?').map(|(_, q)| q).unwrap_or("");
    let (status, body) = match path {
        "/varz" => (200, varz(shared)),
        "/connz" => (200, connz(shared, query)),
        "/accounz" => (200, accounz(shared)),
        "/gatewayz" => (200, gatewayz(shared)),
        "/subz" => (200, subz(shared)),
        "/routez" => (200, routez(shared)),
        "/leafz" => (200, leafz(shared)),
        "/jsz" => jsz(shared),
        "/healthz" => (200, json!({"status": "ok"})),
        _ => (404, json!({"error": "unknown endpoint"})),
    };
    respond(&mut stream, status, body).await;
}

async fn respond(stream: &mut TcpStream, status: u16, body: Value) {
    let text = body.to_string();
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "OK",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        text.len()
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(text.as_bytes()).await;
    let _ = stream.flush().await;
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn go_duration(d: Duration) -> String {
    let secs = d.as_secs();
    let (h, m, s) = (secs / 3600, secs / 60 % 60, secs % 60);
    if h > 0 {
        format!("{h}h{m}m{s}s")
    } else if m > 0 {
        format!("{m}m{s}s")
    } else {
        format!("{s}s")
    }
}

fn uptime(shared: &Shared) -> Duration {
    shared.started.elapsed()
}

/// Resident memory in bytes, from /proc/self/statm on Linux (second
/// field = resident pages, times the 4096-byte page size); 0 elsewhere.
/// Only the process's own memory is reported, never invented.
#[cfg(target_os = "linux")]
fn resident_memory_bytes() -> u64 {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    statm
        .split_whitespace()
        .nth(1)
        .and_then(|pages| pages.parse::<u64>().ok())
        .map(|pages| pages * 4096)
        .unwrap_or(0)
}

#[cfg(not(target_os = "linux"))]
fn resident_memory_bytes() -> u64 {
    0
}

/// Parse a `k=v&k=v` query string into a lookup (no URL-decoding:
/// account names and the supported values need none).
fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        if k == key {
            Some(v)
        } else {
            None
        }
    })
}

fn varz(shared: &Shared) -> Value {
    let cfg = &shared.config;
    let stats = &shared.stats;
    let routes = shared.routes.lock().unwrap().len();
    let leafs = shared.leafs.lock().unwrap().len();
    let conns = shared.conns.lock().unwrap().len();
    let subs = shared.sublist.lock().unwrap().len();
    let mut body = json!({
        "server_id": shared.server_id,
        "server_name": cfg.name,
        "version": env!("CARGO_PKG_VERSION"),
        "proto": 1,
        "go": "rust",
        "host": cfg.host,
        "port": shared.listen_addr.port(),
        "http_port": shared.http_addr.map(|a| a.port()).unwrap_or(0),
        "auth_required": cfg.auth_required(),
        "tls_required": cfg.tls_required(),
        "max_payload": cfg.max_payload,
        "max_connections": 0,
        "ping_interval": cfg.ping_interval.as_nanos() as i64,
        "ping_max": cfg.ping_max,
        "http_base_path": "",
        "start": rfc3339(
            shared
                .started_wall
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
        ),
        "uptime": go_duration(uptime(shared)),
        "mem": resident_memory_bytes(),
        "cores": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
        "connections": conns,
        "total_connections": stats.total_connections.load(Ordering::Relaxed),
        "routes": routes,
        "leafnodes": leafs,
        "in_msgs": stats.in_msgs.load(Ordering::Relaxed),
        "out_msgs": stats.out_msgs.load(Ordering::Relaxed),
        "in_bytes": stats.in_bytes.load(Ordering::Relaxed),
        "out_bytes": stats.out_bytes.load(Ordering::Relaxed),
        "subscriptions": subs,
        "slow_consumers": stats.slow_consumers.load(Ordering::Relaxed),
    });
    if let Some(js) = &shared.jetstream {
        body["jetstream"] = json!({
            "config": {
                "max_memory": 0,
                "max_file": 0,
                "store_dir": cfg.js_store_dir,
            },
            "stats": js.snapshot_totals(),
        });
    }
    body
}

fn connz(shared: &Shared, query: &str) -> Value {
    let acc = query_param(query, "acc");
    let state = query_param(query, "state");
    let limit = query_param(query, "limit").and_then(|v| v.parse::<usize>().ok());
    let offset = query_param(query, "offset")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let mut infos: Vec<_> = shared.conn_info.lock().unwrap().values().cloned().collect();
    infos.sort_by_key(|i| i.id);
    // Filters: only client connections are listed; `acc` restricts to
    // one account; `state=closed` matches nothing (only open
    // connections are tracked), `state=open` matches everything.
    if state == Some("closed") {
        infos.clear();
    }
    infos.retain(|i| i.kind == "client" && acc.map(|a| i.account == a).unwrap_or(true));
    let subs = shared.sublist.lock().unwrap();
    let handles = shared.conns.lock().unwrap();
    let total = infos.len();
    let mut connections = Vec::new();
    for info in infos.iter().skip(offset).take(limit.unwrap_or(usize::MAX)) {
        let (ip, port) = match info.addr.rsplit_once(':') {
            Some((ip, port)) => (ip.to_string(), port.parse::<u16>().unwrap_or(0)),
            None => (info.addr.clone(), 0),
        };
        let (in_msgs, out_msgs, in_bytes, out_bytes) = match handles.get(&info.id) {
            Some(h) => (
                h.in_msgs.load(Ordering::Relaxed),
                h.out_msgs.load(Ordering::Relaxed),
                h.in_bytes.load(Ordering::Relaxed),
                h.out_bytes.load(Ordering::Relaxed),
            ),
            None => (0, 0, 0, 0),
        };
        let sub_list: Vec<String> = subs
            .patterns_for_conn(info.id)
            .into_iter()
            .map(|(_account, subject, queue)| match queue {
                Some(q) => format!("{subject} {q}"),
                None => subject,
            })
            .collect();
        connections.push(json!({
            "cid": info.id,
            "kind": info.kind,
            "ip": ip,
            "port": port,
            "start": rfc3339(
                shared
                    .started_wall
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0),
            ),
            "uptime": go_duration(info.started.elapsed()),
            "idle": 0,
            "pending_bytes": 0,
            "in_msgs": in_msgs,
            "out_msgs": out_msgs,
            "in_bytes": in_bytes,
            "out_bytes": out_bytes,
            "subscriptions": sub_list,
            "subscriptions_count": subs.patterns_for_conn(info.id).len(),
            "account": info.account,
            "user": info.user,
            "tls": info.tls,
        }));
    }
    json!({
        "now": rfc3339(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
        ),
        "num_connections": connections.len(),
        "total": total,
        "connections": connections,
    })
}

/// Accounts with live connection and subscription counts. `$G` is
/// always listed (it is the legacy account when no accounts are
/// configured); configured accounts follow in config order.
fn accounz(shared: &Shared) -> Value {
    let mut names: Vec<String> = vec!["$G".to_string()];
    for a in &shared.config.accounts {
        if !names.contains(&a.name) {
            names.push(a.name.clone());
        }
    }
    let infos = shared.conn_info.lock().unwrap();
    let subs = shared.sublist.lock().unwrap();
    let accounts: Vec<Value> = names
        .iter()
        .map(|name| {
            let connections = infos
                .values()
                .filter(|i| i.kind == "client" && &i.account == name)
                .count();
            let subscriptions = subs
                .subscriptions()
                .iter()
                .filter(|s| &s.account == name)
                .count();
            json!({
                "name": name,
                "connections": connections,
                "subscriptions": subscriptions,
            })
        })
        .collect();
    let total = accounts.len();
    json!({ "total": total, "accounts": accounts })
}

/// No gateways are implemented, so the list is always empty.
fn gatewayz(shared: &Shared) -> Value {
    json!({ "name": shared.config.name, "gateways": [] })
}

fn subz(shared: &Shared) -> Value {
    let sl = shared.sublist.lock().unwrap();
    let mut subscriptions: Vec<Value> = sl
        .subscriptions()
        .iter()
        .map(|s| {
            json!({
                "subject": s.subject,
                "queue": s.queue,
                "sid": s.sid,
                "cid": s.conn_id,
                "account": s.account,
            })
        })
        .collect();
    for (rid, interests) in shared.route_subs.lock().unwrap().iter() {
        for (account, subject, queue) in interests.keys() {
            subscriptions.push(json!({
                "subject": subject,
                "queue": queue,
                "sid": Value::Null,
                "cid": rid,
                "account": account,
            }));
        }
    }
    for (lid, interests) in shared.leaf_subs.lock().unwrap().iter() {
        for (account, subject, queue) in interests.keys() {
            subscriptions.push(json!({
                "subject": subject,
                "queue": queue,
                "sid": Value::Null,
                "cid": lid,
                "account": account,
            }));
        }
    }
    let total = subscriptions.len();
    json!({
        "num_subscriptions": total,
        "num_cache": 0,
        "total": total,
        "subscriptions": subscriptions,
    })
}

fn routez(shared: &Shared) -> Value {
    let routes = shared.routes.lock().unwrap();
    let subs = shared.route_subs.lock().unwrap();
    let mut out = Vec::new();
    for (rid, route) in routes.iter() {
        let peer_id = route.peer_id.lock().unwrap().clone().unwrap_or_default();
        let listen = route
            .peer_listen_addr
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_default();
        let (ip, port) = match listen.rsplit_once(':') {
            Some((ip, port)) => (ip.to_string(), port.parse::<u16>().unwrap_or(0)),
            None => (String::new(), 0),
        };
        let sub_list: Vec<String> = subs
            .get(rid)
            .map(|m| {
                m.keys()
                    .map(|(_account, subject, queue)| match queue {
                        Some(q) => format!("{subject} {q}"),
                        None => subject.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.push(json!({
            "rid": rid,
            "remote_id": peer_id,
            "did_solicit": route.initiator == shared.server_id,
            "ip": ip,
            "port": port,
            "in_msgs": route.handle.in_msgs.load(Ordering::Relaxed),
            "out_msgs": route.handle.out_msgs.load(Ordering::Relaxed),
            "in_bytes": route.handle.in_bytes.load(Ordering::Relaxed),
            "out_bytes": route.handle.out_bytes.load(Ordering::Relaxed),
            "subscriptions": sub_list,
            "subscriptions_count": sub_list.len(),
        }));
    }
    let total = out.len();
    json!({ "num_routes": total, "routes": out })
}

fn leafz(shared: &Shared) -> Value {
    let leafs = shared.leafs.lock().unwrap();
    let subs = shared.leaf_subs.lock().unwrap();
    let infos = shared.conn_info.lock().unwrap();
    let mut out = Vec::new();
    for (lid, leaf) in leafs.iter() {
        let addr = infos.get(lid).map(|i| i.addr.clone()).unwrap_or_default();
        let (ip, port) = match addr.rsplit_once(':') {
            Some((ip, port)) => (ip.to_string(), port.parse::<u16>().unwrap_or(0)),
            None => (String::new(), 0),
        };
        let sub_list: Vec<String> = subs
            .get(lid)
            .map(|m| {
                m.keys()
                    .map(|(_account, subject, queue)| match queue {
                        Some(q) => format!("{subject} {q}"),
                        None => subject.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.push(json!({
            "account": leaf.account,
            "ip": ip,
            "port": port,
            "in_msgs": leaf.handle.in_msgs.load(Ordering::Relaxed),
            "out_msgs": leaf.handle.out_msgs.load(Ordering::Relaxed),
            "in_bytes": leaf.handle.in_bytes.load(Ordering::Relaxed),
            "out_bytes": leaf.handle.out_bytes.load(Ordering::Relaxed),
            "subscriptions": sub_list,
            "subscriptions_count": sub_list.len(),
        }));
    }
    let total = out.len();
    json!({ "num_leafnodes": total, "leafnodes": out })
}

fn jsz(shared: &Shared) -> (u16, Value) {
    match &shared.jetstream {
        Some(js) => (200, js.snapshot()),
        None => (
            404,
            json!({"error": {"code": 404, "description": "jetstream not enabled"}}),
        ),
    }
}
