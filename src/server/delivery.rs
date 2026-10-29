//! Message delivery: `deliver` and its fan-out, global queue-group
//! selection across local clients, routes, and leaf nodes, subscription
//! interest propagation, and inbound route/leaf frame processing
//! (`route_read_loop` / `process_route_op`, `leaf_read_loop` /
//! `process_leaf_op`), which feeds `deliver` and interest state.
//!
//! Invariants preserved from the pre-split `server.rs` (Phase 29 moves
//! code only): the sublist `RwLock` is never held across socket or
//! channel operations — `deliver` takes the lock only to compute
//! matches / note deliveries, then releases it before sending frames.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::sync::mpsc;

use super::conn::{try_send, ConnHandle};
use super::imports::parse_import_reply_subject;
use super::{InterestMap, InterestTargets, Shared};
use crate::leaf::{
    encode_leaf_hmsg, encode_lmsg, encode_ls_minus, encode_ls_plus, LeafOp, LeafParser,
};
use crate::protocol::{encode_err, encode_hmsg, encode_msg};
use crate::route::{
    encode_rmsg, encode_route_hmsg, encode_rs_minus, encode_rs_plus, RouteOp, RouteParser,
};

/// A message in flight: subject, optional reply, optional raw header block
/// (starts with `NATS/1.0\r\n`, ends with `\r\n\r\n`), payload.
#[derive(Debug, Clone)]
pub struct Message {
    pub subject: String,
    pub reply_to: Option<String>,
    pub headers: Option<Bytes>,
    pub payload: Bytes,
    /// Account the message belongs to; delivery is scoped to it.
    /// Defaults to `$G` (Message::new); JetStream and route/leaf paths
    /// set it explicitly.
    pub account: String,
    /// The publishing connection set `no_responders:true` in CONNECT.
    /// Only meaningful for Local origin; route/leaf/internal messages
    /// never carry it.
    pub no_responders: bool,
    /// True for a stream-import copy (already translated once): it is
    /// delivered to local subscribers only and is never translated
    /// again, so import chains cannot loop.
    pub import_copy: bool,
    /// True for a service-import translated request (already rewritten
    /// into the exporting account): it must not be service-translated
    /// a second time, but it may still fan out to stream importers.
    pub service_translated: bool,
    /// Phase 31: when set, sublist matching routes on this subject
    /// while frames still carry `subject`. JetStream consumer
    /// deliveries need it: the frame subject must be the original
    /// stream message subject (clients read it — async-nats KV
    /// Watch derives the key from it), but the only subscription
    /// that must receive it is the consumer's deliver subject /
    /// pull reply inbox. Without the override the server used to
    /// send the inbox as the frame subject, which broke Watch.
    pub route_via: Option<String>,
}

impl Message {
    pub fn new(
        subject: &str,
        reply_to: Option<String>,
        headers: Option<Bytes>,
        payload: Bytes,
    ) -> Self {
        Message {
            subject: subject.to_string(),
            reply_to,
            headers,
            payload,
            account: "$G".to_string(),
            no_responders: false,
            import_copy: false,
            service_translated: false,
            route_via: None,
        }
    }

    pub fn with_account(mut self, account: &str) -> Self {
        self.account = account.to_string();
        self
    }

    pub fn with_route_via(mut self, via: &str) -> Self {
        self.route_via = Some(via.to_string());
        self
    }

    pub fn with_no_responders(mut self, no_responders: bool) -> Self {
        self.no_responders = no_responders;
        self
    }

    /// Parse the raw header block into (name, value) pairs. Malformed lines
    /// are skipped; the `NATS/1.0` status line is skipped.
    pub fn header_map(&self) -> HashMap<String, String> {
        let mut out = HashMap::new();
        if let Some(h) = &self.headers {
            let text = String::from_utf8_lossy(h);
            for line in text.lines() {
                if line.starts_with("NATS/") || line.is_empty() {
                    continue;
                }
                if let Some((k, v)) = line.split_once(':') {
                    out.insert(k.trim().to_string(), v.trim().to_string());
                }
            }
        }
        out
    }
}

/// Where a message came from. Local carries (conn_id, echo flag).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Origin {
    Local(u64, bool),
    Route(u64),
    Leaf(u64),
    /// A message arriving on a gateway connection. Delivered locally
    /// only, like Route/Leaf origin: never forwarded back to any
    /// gateway (loop prevention), and never to routes/leafs either,
    /// since each node holds its own gateway connections to the
    /// peer cluster and route-forwarding would duplicate it.
    Gateway(u64),
    Internal,
}

/// Deliver a message: to local subscribers, and (unless it arrived on a
/// route/leaf) forward it to routes and leafs with matching interest.
///
/// Account imports/exports (same node only): a local publish whose
/// subject matches a service import is rewritten into the exporting
/// account instead of being delivered locally, with the reply subject
/// replaced by a synthetic `_RIMPORT.<seq>` subject that routes
/// replies back to the importer's original reply subject; a local
/// publish in an exporting account is additionally copied to local
/// subscribers of every authorized stream import of that subject.
/// Returns true when the message reached at least one subscriber (or
/// was consumed by JetStream), which the no-responders logic uses.
///
/// `queue_filter` restricts queue-group delivery for messages arriving
/// on a route/leaf: only the listed groups are delivered (the sender
/// already ran the global queue selection). `None` means all groups
/// (local publishes and header route frames, which carry no list).
pub fn deliver(
    shared: &Arc<Shared>,
    msg: &Message,
    origin: Origin,
    queue_filter: Option<&[String]>,
) -> bool {
    // JetStream capture / API handling happens before core delivery so a
    // stream message is stored even when nobody is subscribed, and API
    // requests are answered by the hosting node.
    if let Some(js) = &shared.jetstream {
        if crate::jetstream::handle_message(shared, js, msg, origin) {
            // API requests and acks are consumed by JetStream; they are not
            // also delivered as ordinary messages.
            return true;
        }
    }

    // Phase 31 consumer deliveries (see `route_via`): route on the
    // deliver subject / pull reply inbox, but frame with the
    // original subject. Never service-translated, and never a
    // no-responders case. Phase 32: no longer local-only — a
    // consumer delivery is also forwarded to routes/leafs with
    // interest in the original subject, which is what lets a
    // cross-node pull consumer (the mirror loop) receive its
    // messages: the receiving node matches the frame by its original
    // subject, and the mirror loop subscribes to the origin stream's
    // subjects (or the link's filter) for exactly that match. When
    // no remote node has interest in the original subject, this
    // behaves exactly as before (local delivery only).
    if let Some(via) = msg.route_via.clone() {
        let (deliveries, expired) = {
            let mut sl = shared.sublist.write().unwrap();
            let gm = sl.match_groups(&via, u64::MAX, true, Some(&msg.account));
            let mut deliveries = gm.plain;
            for members in gm.groups.values() {
                if !members.is_empty() {
                    let cursor = shared.queue_cursor.fetch_add(1, Ordering::SeqCst);
                    deliveries.push(members[(cursor as usize) % members.len()].clone());
                }
            }
            let expired = sl.note_deliveries(&deliveries);
            (deliveries, expired)
        };
        send_local_frames(shared, msg, &deliveries);
        cleanup_expired(shared, &expired);
        shared
            .stats
            .out_msgs
            .fetch_add(deliveries.len() as u64, Ordering::Relaxed);
        let mut forwarded = !deliveries.is_empty();
        for (rid, handle, plain, _) in route_interest_full(shared, &msg.subject, &msg.account) {
            if !plain {
                continue;
            }
            let frame = match &msg.headers {
                Some(h) => encode_route_hmsg(
                    &msg.account,
                    &msg.subject,
                    msg.reply_to.as_deref(),
                    &[],
                    h,
                    &msg.payload,
                ),
                None => encode_rmsg(
                    &msg.account,
                    &msg.subject,
                    msg.reply_to.as_deref(),
                    &[],
                    &msg.payload,
                ),
            };
            if try_send(&handle, frame) {
                forwarded = true;
            } else {
                eprintln!("rust-natsd: slow route {rid}, closing (consumer delivery)");
                close_route(shared, rid);
            }
        }
        for (_lid, handle, plain, _) in leaf_interest_full(shared, &msg.subject, &msg.account) {
            if !plain {
                continue;
            }
            let frame = match &msg.headers {
                Some(h) => {
                    encode_leaf_hmsg(&msg.subject, msg.reply_to.as_deref(), &[], h, &msg.payload)
                }
                None => encode_lmsg(
                    &msg.subject,
                    msg.reply_to.as_deref(),
                    &[],
                    None,
                    &msg.payload,
                ),
            };
            if try_send(&handle, frame) {
                forwarded = true;
            }
        }
        // Phase 33: gateway plain interest, same route framing with
        // the account token (queue selection does not apply to the
        // route_via path: it is a single consumer delivery).
        for (gid, handle, plain, _) in gateway_interest_full(shared, &msg.subject, &msg.account) {
            if !plain {
                continue;
            }
            let frame = match &msg.headers {
                Some(h) => encode_route_hmsg(
                    &msg.account,
                    &msg.subject,
                    msg.reply_to.as_deref(),
                    &[],
                    h,
                    &msg.payload,
                ),
                None => encode_rmsg(
                    &msg.account,
                    &msg.subject,
                    msg.reply_to.as_deref(),
                    &[],
                    &msg.payload,
                ),
            };
            if try_send(&handle, frame) {
                forwarded = true;
            } else {
                eprintln!("rust-natsd: slow gateway {gid}, closing (consumer delivery)");
                close_gateway(shared, gid);
            }
        }
        return forwarded;
    }

    let (publisher_conn, publisher_echo) = match origin {
        Origin::Local(id, echo) => (id, echo),
        _ => (u64::MAX, true),
    };

    // Messages arriving on a gateway are delivered locally only
    // (supercluster loop prevention).
    if let Origin::Gateway(gid) = origin {
        if msg.subject.starts_with("_RIMPORT.") {
            if let Some((account, orig_reply)) =
                super::imports::lookup_import_reply(shared, &msg.subject)
            {
                deliver(
                    shared,
                    &Message::new(&orig_reply, None, msg.headers.clone(), msg.payload.clone())
                        .with_account(&account),
                    Origin::Internal,
                    None,
                );
                return true;
            }
            return false;
        }
        if let Some(delivered) = super::imports::try_service_import(shared, msg, origin) {
            return delivered;
        }
        let result = {
            let mut sl = shared.sublist.write().unwrap();
            let gm = sl.match_groups(
                &msg.subject,
                publisher_conn,
                publisher_echo,
                Some(&msg.account),
            );
            let mut deliveries = gm.plain;
            for (q, members) in &gm.groups {
                let wanted = match queue_filter {
                    Some(list) => list.iter().any(|g| g == q),
                    None => true,
                };
                if wanted && !members.is_empty() {
                    let cursor = shared.queue_cursor.fetch_add(1, Ordering::SeqCst);
                    deliveries.push(members[(cursor as usize) % members.len()].clone());
                }
            }
            let expired = sl.note_deliveries(&deliveries);
            crate::sublist::MatchResult {
                deliveries,
                expired,
            }
        };
        send_local_frames(shared, msg, &result.deliveries);
        cleanup_expired(shared, &result.expired);
        shared
            .stats
            .out_msgs
            .fetch_add(result.deliveries.len() as u64, Ordering::Relaxed);
        let mut delivered = !result.deliveries.is_empty();
        if !msg.service_translated && super::imports::deliver_stream_imports(shared, msg) {
            delivered = true;
        }
        let mut sent = std::collections::HashSet::new();
        sent.insert(gid);
        if forward_gateway_reply(shared, msg, &sent) {
            delivered = true;
        }
        return delivered;
    }

    if matches!(origin, Origin::Route(_) | Origin::Leaf(_)) && msg.subject.starts_with("_RIMPORT.")
    {
        if let Some((account, orig_reply)) =
            super::imports::lookup_import_reply(shared, &msg.subject)
        {
            deliver(
                shared,
                &Message::new(&orig_reply, None, msg.headers.clone(), msg.payload.clone())
                    .with_account(&account),
                Origin::Internal,
                None,
            );
            return true;
        }
        return false;
    }

    // Phase 32: a reply to a synthetic subject minted by another node
    // (local/internal publish — the client-connection path handles
    // this before the permission check, this is the backstop for any
    // other local publisher) is forwarded to every route/leaf
    // regardless of interest: only the minting node can claim it on
    // arrival, and no subscriber ever legitimately subscribes to a
    // synthetic subject. A same-node or legacy reply is handled by
    // the normal path below (its mapping is local).
    if msg.subject.starts_with("_RIMPORT.") {
        if let Some((node, _)) = parse_import_reply_subject(&msg.subject) {
            if node != shared.server_id {
                forward_remote_import_reply(shared, msg);
                return true;
            }
        }
    }

    // Service import: translation, synthetic `_RIMPORT.<seq>` reply
    // mapping, and no-responders 503 live in `imports::try_service_import`.
    if let Some(delivered) = super::imports::try_service_import(shared, msg, origin) {
        return delivered;
    }

    // Global queue selection across local members, routes, leafs, and
    // gateways (weighted by member count), so a queue group spanning
    // nodes still delivers exactly once per message.
    // Topology rules:
    // - Local / Internal origin: forwards to all routes, all leafs, and all gateways.
    // - Route(src_rid) origin: routes form a full mesh (1-hop), so never
    //   forward to other routes, but DO forward to leafs with interest.
    // - Leaf(src_lid) origin: hub-and-spoke, so forward to all routes and
    //   to all OTHER leafs (`lid != src_lid`), never back to `src_lid`.
    let gm = {
        let sl = shared.sublist.read().unwrap();
        sl.match_groups(
            &msg.subject,
            publisher_conn,
            publisher_echo,
            Some(&msg.account),
        )
    };
    let route_full = match origin {
        Origin::Local(_, _) | Origin::Internal | Origin::Leaf(_) => {
            route_interest_full(shared, &msg.subject, &msg.account)
        }
        Origin::Route(_) | Origin::Gateway(_) => Vec::new(),
    };
    let leaf_full: InterestTargets = match origin {
        Origin::Local(_, _) | Origin::Internal | Origin::Route(_) => {
            leaf_interest_full(shared, &msg.subject, &msg.account)
        }
        Origin::Leaf(src_lid) => leaf_interest_full(shared, &msg.subject, &msg.account)
            .into_iter()
            .filter(|(lid, _, _, _)| *lid != src_lid)
            .collect(),
        Origin::Gateway(_) => Vec::new(),
    };
    let gateway_full = match origin {
        Origin::Local(_, _) | Origin::Internal => {
            gateway_interest_full(shared, &msg.subject, &msg.account)
        }
        Origin::Route(_) | Origin::Leaf(_) | Origin::Gateway(_) => Vec::new(),
    };

    enum Dest {
        Local(crate::sublist::Delivery),
        Route(u64),
        Leaf(u64),
        Gateway(u64),
    }
    let mut local_deliveries: Vec<crate::sublist::Delivery> = gm.plain;
    let mut route_selected: HashMap<u64, Vec<String>> = HashMap::new();
    let mut leaf_selected: HashMap<u64, Vec<String>> = HashMap::new();
    let mut gateway_selected: HashMap<u64, Vec<String>> = HashMap::new();

    let mut all_groups: std::collections::BTreeSet<String> = gm.groups.keys().cloned().collect();
    for (_, _, _, counts) in &route_full {
        all_groups.extend(counts.keys().cloned());
    }
    for (_, _, _, counts) in &leaf_full {
        all_groups.extend(counts.keys().cloned());
    }
    for (_, _, _, counts) in &gateway_full {
        all_groups.extend(counts.keys().cloned());
    }
    for group in &all_groups {
        if let Some(list) = queue_filter {
            if !list.iter().any(|g| g == group) {
                continue;
            }
        }
        let mut dests: Vec<Dest> = gm
            .groups
            .get(group)
            .map(|members| members.iter().map(|m| Dest::Local(m.clone())).collect())
            .unwrap_or_default();
        for (rid, _, _, counts) in &route_full {
            for _ in 0..counts.get(group).copied().unwrap_or(0) {
                dests.push(Dest::Route(*rid));
            }
        }
        for (lid, _, _, counts) in &leaf_full {
            for _ in 0..counts.get(group).copied().unwrap_or(0) {
                dests.push(Dest::Leaf(*lid));
            }
        }
        for (gid, _, _, counts) in &gateway_full {
            for _ in 0..counts.get(group).copied().unwrap_or(0) {
                dests.push(Dest::Gateway(*gid));
            }
        }
        if dests.is_empty() {
            continue;
        }
        let cursor = shared.queue_cursor.fetch_add(1, Ordering::SeqCst);
        match &dests[(cursor as usize) % dests.len()] {
            Dest::Local(d) => local_deliveries.push(d.clone()),
            Dest::Route(rid) => route_selected.entry(*rid).or_default().push(group.clone()),
            Dest::Leaf(lid) => leaf_selected.entry(*lid).or_default().push(group.clone()),
            Dest::Gateway(gid) => gateway_selected
                .entry(*gid)
                .or_default()
                .push(group.clone()),
        }
    }
    let expired = {
        let mut sl = shared.sublist.write().unwrap();
        sl.note_deliveries(&local_deliveries)
    };
    send_local_frames(shared, msg, &local_deliveries);
    cleanup_expired(shared, &expired);
    shared
        .stats
        .out_msgs
        .fetch_add(local_deliveries.len() as u64, Ordering::Relaxed);

    // No-responders accounting is computed before the route/leaf/
    // gateway loops consume those vectors: any local delivery, plain
    // remote interest, or selected queue group counts as a responder.
    let mut has_delivery = !local_deliveries.is_empty()
        || route_selected.values().any(|v| !v.is_empty())
        || leaf_selected.values().any(|v| !v.is_empty())
        || gateway_selected.values().any(|v| !v.is_empty())
        || route_full.iter().any(|(_, _, plain, _)| *plain)
        || leaf_full.iter().any(|(_, _, plain, _)| *plain)
        || gateway_full.iter().any(|(_, _, plain, _)| *plain);

    // Routes: one frame per route with plain interest and/or a selected
    // queue group. The queues list on the frame is exactly the groups
    // this route was selected for, so the receiver delivers to plain
    // subscribers plus one member per listed group — never more.
    for (rid, handle, plain, _counts) in route_full {
        let selected = route_selected.get(&rid).cloned().unwrap_or_default();
        if !plain && selected.is_empty() {
            continue;
        }
        let frame = match &msg.headers {
            Some(h) => encode_route_hmsg(
                &msg.account,
                &msg.subject,
                msg.reply_to.as_deref(),
                &selected,
                h,
                &msg.payload,
            ),
            None => encode_rmsg(
                &msg.account,
                &msg.subject,
                msg.reply_to.as_deref(),
                &selected,
                &msg.payload,
            ),
        };
        if !try_send(&handle, frame) {
            eprintln!("rust-natsd: slow route {rid}, closing");
            close_route(shared, rid);
        }
    }
    // Leafs: same shape, via LMSG/HMSG (which carries queues in both the
    // plain and header forms).
    for (lid, handle, plain, _counts) in leaf_full {
        let selected = leaf_selected.get(&lid).cloned().unwrap_or_default();
        if !plain && selected.is_empty() {
            continue;
        }
        let frame = match &msg.headers {
            Some(h) => encode_leaf_hmsg(
                &msg.subject,
                msg.reply_to.as_deref(),
                &selected,
                h,
                &msg.payload,
            ),
            None => encode_lmsg(
                &msg.subject,
                msg.reply_to.as_deref(),
                &selected,
                None,
                &msg.payload,
            ),
        };
        if !try_send(&handle, frame) {
            eprintln!("rust-natsd: slow leaf {lid}, closing");
            close_leaf(shared, lid);
        }
    }
    // Gateways: route framing with the account token, one frame per
    // gateway with plain interest and/or a selected queue group.
    let mut gateway_sent: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for (gid, handle, plain, _counts) in gateway_full {
        let selected = gateway_selected.get(&gid).cloned().unwrap_or_default();
        if !plain && selected.is_empty() {
            continue;
        }
        let frame = match &msg.headers {
            Some(h) => encode_route_hmsg(
                &msg.account,
                &msg.subject,
                msg.reply_to.as_deref(),
                &selected,
                h,
                &msg.payload,
            ),
            None => encode_rmsg(
                &msg.account,
                &msg.subject,
                msg.reply_to.as_deref(),
                &selected,
                &msg.payload,
            ),
        };
        if try_send(&handle, frame) {
            gateway_sent.insert(gid);
        } else {
            eprintln!("rust-natsd: slow gateway {gid}, closing");
            close_gateway(shared, gid);
        }
    }
    // Phase 33: a publish to a recorded gateway reply subject (a
    // reply to a request that arrived over a gateway) flows back
    // over that connection even without RS+ interest — Go's shadow
    // mechanism, since gateways do not propagate `_INBOX` subs.
    if matches!(origin, Origin::Local(_, _) | Origin::Internal)
        && forward_gateway_reply(shared, msg, &gateway_sent)
    {
        has_delivery = true;
    }

    // Stream imports: copies to authorized stream importers; see
    // `imports::deliver_stream_imports`.
    if !msg.service_translated && super::imports::deliver_stream_imports(shared, msg) {
        has_delivery = true;
    }

    // No-responders (Go behaviour, verified against v2.15.1): a local
    // publish with a reply subject and no deliveries, from a connection
    // that set no_responders:true, generates an HMSG to the reply subject
    // with a 503 header block and an empty payload. JetStream-consumed
    // messages return early above, so they never generate one.
    if matches!(origin, Origin::Local(_, _)) && msg.no_responders && !has_delivery {
        if let Some(reply_to) = &msg.reply_to {
            let headers = Bytes::from(format!(
                "NATS/1.0 503\r\nNats-Subject: {}\r\n\r\n",
                msg.subject
            ));
            deliver(
                shared,
                &Message::new(reply_to, None, Some(headers), Bytes::new())
                    .with_account(&msg.account),
                Origin::Internal,
                None,
            );
        }
    }
    has_delivery
}

/// Deliver a stream-import copy to local subscribers of its (already
/// rewritten) subject/account only — no route/leaf forwarding, no
/// further import translation. Queue groups get exactly one member,
/// like the route/leaf-origin path. Returns true on any delivery.
pub(crate) fn deliver_stream_copy(shared: &Arc<Shared>, msg: &Message) -> bool {
    let (deliveries, expired) = {
        let mut sl = shared.sublist.write().unwrap();
        let gm = sl.match_groups(&msg.subject, u64::MAX, true, Some(&msg.account));
        let mut deliveries = gm.plain;
        for members in gm.groups.values() {
            if !members.is_empty() {
                let cursor = shared.queue_cursor.fetch_add(1, Ordering::SeqCst);
                deliveries.push(members[(cursor as usize) % members.len()].clone());
            }
        }
        let expired = sl.note_deliveries(&deliveries);
        (deliveries, expired)
    };
    send_local_frames(shared, msg, &deliveries);
    cleanup_expired(shared, &expired);
    shared
        .stats
        .out_msgs
        .fetch_add(deliveries.len() as u64, Ordering::Relaxed);
    !deliveries.is_empty()
}

/// Forward a service-import reply minted by another node to every
/// route and leaf, bypassing interest: the minting node claims it on
/// arrival via its reply-mapping lookup, and every other node drops
/// it (route/leaf-origin `_RIMPORT` handling in `deliver`). The
/// account token on route frames is the responder's account, as for
/// any other message in that account; leaf frames carry no account
/// and are claimed account-agnostically on arrival. Bypassing the
/// publish-permission check here mirrors the same-node interception
/// in `conn::process_op`: the synthetic reply channel is import
/// machinery, not a subject the responder's allow list governs.
pub(crate) fn forward_remote_import_reply(shared: &Arc<Shared>, msg: &Message) {
    let route_handles: Vec<(u64, Arc<ConnHandle>)> = shared
        .routes
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, r)| !r.handle.is_closed())
        .map(|(id, r)| (*id, r.handle.clone()))
        .collect();
    for (rid, handle) in route_handles {
        let frame = match &msg.headers {
            Some(h) => encode_route_hmsg(
                &msg.account,
                &msg.subject,
                msg.reply_to.as_deref(),
                &[],
                h,
                &msg.payload,
            ),
            None => encode_rmsg(
                &msg.account,
                &msg.subject,
                msg.reply_to.as_deref(),
                &[],
                &msg.payload,
            ),
        };
        if !try_send(&handle, frame) {
            eprintln!("rust-natsd: slow route {rid}, closing (import reply)");
            close_route(shared, rid);
        }
    }
    let leaf_handles: Vec<Arc<ConnHandle>> = shared
        .leafs
        .lock()
        .unwrap()
        .values()
        .filter(|l| !l.handle.is_closed())
        .map(|l| l.handle.clone())
        .collect();
    for handle in leaf_handles {
        let frame = match &msg.headers {
            Some(h) => {
                encode_leaf_hmsg(&msg.subject, msg.reply_to.as_deref(), &[], h, &msg.payload)
            }
            None => encode_lmsg(
                &msg.subject,
                msg.reply_to.as_deref(),
                &[],
                None,
                &msg.payload,
            ),
        };
        let _ = try_send(&handle, frame);
    }
    // Gateways: route framing with the account token, claimed on
    // arrival by the gateway-origin `_RIMPORT` branch of `deliver`
    // (same as the route branch).
    let gateway_handles: Vec<(u64, Arc<ConnHandle>)> = shared
        .gateways
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, g)| !g.handle.is_closed())
        .map(|(id, g)| (*id, g.handle.clone()))
        .collect();
    for (gid, handle) in gateway_handles {
        let frame = match &msg.headers {
            Some(h) => encode_route_hmsg(
                &msg.account,
                &msg.subject,
                msg.reply_to.as_deref(),
                &[],
                h,
                &msg.payload,
            ),
            None => encode_rmsg(
                &msg.account,
                &msg.subject,
                msg.reply_to.as_deref(),
                &[],
                &msg.payload,
            ),
        };
        if !try_send(&handle, frame) {
            eprintln!("rust-natsd: slow gateway {gid}, closing (import reply)");
            close_gateway(shared, gid);
        }
    }
}

/// Send MSG/HMSG frames for local deliveries, dropping slow consumers.
/// The connection registry is locked once for all targets, not once
/// per message: at burst rates a per-message lock was measurable
/// contention on the publisher thread.
pub(crate) fn send_local_frames(
    shared: &Arc<Shared>,
    msg: &Message,
    deliveries: &[crate::sublist::Delivery],
) {
    let targets: Vec<(crate::sublist::Delivery, Arc<ConnHandle>)> = {
        let conns = shared.conns.lock().unwrap();
        deliveries
            .iter()
            .filter_map(|d| conns.get(&d.conn_id).cloned().map(|h| (d.clone(), h)))
            .collect()
    };
    for (d, target) in targets {
        let frame = match &msg.headers {
            Some(h) if target.headers.load(Ordering::SeqCst) => encode_hmsg(
                &msg.subject,
                &d.sid,
                msg.reply_to.as_deref(),
                h,
                &msg.payload,
            ),
            _ => encode_msg(&msg.subject, &d.sid, msg.reply_to.as_deref(), &msg.payload),
        };
        if !try_send(&target, frame) {
            shared.stats.slow_consumers.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "rust-natsd: slow consumer, dropping connection {}",
                d.conn_id
            );
            shared.sublist.write().unwrap().remove_conn(d.conn_id);
            if let Some(h) = shared.conns.lock().unwrap().remove(&d.conn_id) {
                h.mark_closed();
            }
            shared.conn_info.lock().unwrap().remove(&d.conn_id);
        }
    }
}

/// Remove auto-unsubscribed subscriptions and propagate RS-/LS- for
/// patterns that lost their last local subscriber.
pub(crate) fn cleanup_expired(shared: &Arc<Shared>, expired: &[(u64, String)]) {
    if expired.is_empty() {
        return;
    }
    let patterns: Vec<(String, String, Option<String>)> = {
        let sl = shared.sublist.read().unwrap();
        expired
            .iter()
            .filter_map(|(cid, sid)| sl.pattern_of(*cid, sid))
            .collect()
    };
    {
        let mut sl = shared.sublist.write().unwrap();
        for (cid, sid) in expired {
            sl.remove(*cid, sid);
        }
    }
    for (account, subject, queue) in patterns {
        notify_interest_removed(shared, &account, &subject, queue.as_deref());
    }
}

/// Snapshot of one connection's interest: (plain_match, queue -> count).
pub(crate) fn interest_of(
    subs: &InterestMap,
    subject: &str,
    account: &str,
) -> (bool, HashMap<String, u32>) {
    let mut plain = false;
    let mut counts: HashMap<String, u32> = HashMap::new();
    for ((acct, pattern, queue), count) in subs {
        if acct != account {
            continue;
        }
        if *count == 0 {
            continue;
        }
        if !crate::subject::subject_matches(pattern, subject) {
            continue;
        }
        match queue {
            None => plain = true,
            Some(q) => {
                *counts.entry(q.clone()).or_insert(0) += *count;
            }
        }
    }
    (plain, counts)
}

/// Whether any local subscription, route, or leaf has interest in
/// `subject` for `account`. Used by the JetStream tick to defer a push
/// redelivery while nobody is listening on the deliver subject, so it
/// does not burn a delivery cycle on a dropped message. Read-only: it
/// uses `match_groups`, which does not update delivery counters.
pub(crate) fn has_interest(shared: &Arc<Shared>, account: &str, subject: &str) -> bool {
    {
        let sl = shared.sublist.read().unwrap();
        let gm = sl.match_groups(subject, u64::MAX, true, Some(account));
        if !gm.plain.is_empty() || !gm.groups.is_empty() {
            return true;
        }
    }
    for subs in shared.route_subs.lock().unwrap().values() {
        let (plain, counts) = interest_of(subs, subject, account);
        if plain || !counts.is_empty() {
            return true;
        }
    }
    for subs in shared.leaf_subs.lock().unwrap().values() {
        let (plain, counts) = interest_of(subs, subject, account);
        if plain || !counts.is_empty() {
            return true;
        }
    }
    for subs in shared.gateway_subs.lock().unwrap().values() {
        let (plain, counts) = interest_of(subs, subject, account);
        if plain || !counts.is_empty() {
            return true;
        }
    }
    false
}

pub(crate) fn route_interest_full(
    shared: &Arc<Shared>,
    subject: &str,
    account: &str,
) -> InterestTargets {
    let subs = shared.route_subs.lock().unwrap();
    shared
        .routes
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, r)| !r.handle.is_closed())
        .map(|(id, r)| {
            let (plain, counts) = subs
                .get(id)
                .map(|m| interest_of(m, subject, account))
                .unwrap_or((false, HashMap::new()));
            (*id, r.handle.clone(), plain, counts)
        })
        .collect()
}

pub(crate) fn leaf_interest_full(
    shared: &Arc<Shared>,
    subject: &str,
    account: &str,
) -> InterestTargets {
    let subs = shared.leaf_subs.lock().unwrap();
    shared
        .leafs
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, l)| !l.handle.is_closed() && l.account == account)
        .map(|(id, l)| {
            let (plain, counts) = subs
                .get(id)
                .map(|m| interest_of(m, subject, account))
                .unwrap_or((false, HashMap::new()));
            (*id, l.handle.clone(), plain, counts)
        })
        .collect()
}

pub(crate) fn gateway_interest_full(
    shared: &Arc<Shared>,
    subject: &str,
    account: &str,
) -> InterestTargets {
    let subs = shared.gateway_subs.lock().unwrap();
    shared
        .gateways
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, g)| !g.handle.is_closed())
        .map(|(id, g)| {
            let (plain, counts) = subs
                .get(id)
                .map(|m| interest_of(m, subject, account))
                .unwrap_or((false, HashMap::new()));
            (*id, g.handle.clone(), plain, counts)
        })
        .collect()
}

/// Snapshot of which peer classes already saw interest for `(account, subject, queue)`
/// before a new subscription/interest was recorded: `(routes_had, leafs_that_had, gateways_had)`.
pub(crate) fn interest_targets_before_add(
    shared: &Arc<Shared>,
    account: &str,
    subject: &str,
    queue: Option<&str>,
) -> (bool, std::collections::HashSet<u64>, bool) {
    let routes_had = pattern_in_use_for_routes(shared, account, subject, queue);
    let gateways_had = routes_had;
    let leaf_ids: Vec<(u64, String)> = shared
        .leafs
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, l)| !l.handle.is_closed())
        .map(|(id, l)| (*id, l.account.clone()))
        .collect();
    let mut leafs_had = std::collections::HashSet::new();
    for (lid, l_acc) in leaf_ids {
        if l_acc == account && pattern_in_use_for_leaf(shared, lid, &l_acc, subject, queue) {
            leafs_had.insert(lid);
        }
    }
    (routes_had, leafs_had, gateways_had)
}

/// After adding interest for `(account, subject, queue)`, send `RS+`/`LS+`
/// to any route, leaf, or inbound gateway for which interest transitioned from false -> true.
pub(crate) fn notify_interest_added(
    shared: &Arc<Shared>,
    account: &str,
    subject: &str,
    queue: Option<&str>,
    before: (bool, std::collections::HashSet<u64>, bool),
) {
    let (routes_had, leafs_had, gateways_had) = before;
    if !routes_had && pattern_in_use_for_routes(shared, account, subject, queue) {
        let rframe = encode_rs_plus(account, subject, queue);
        let route_handles: Vec<Arc<ConnHandle>> = shared
            .routes
            .lock()
            .unwrap()
            .values()
            .filter(|r| !r.handle.is_closed())
            .map(|r| r.handle.clone())
            .collect();
        for handle in route_handles {
            let _ = try_send(&handle, rframe.clone());
        }
    }
    if !gateways_had && pattern_in_use_for_routes(shared, account, subject, queue) {
        let rframe = encode_rs_plus(account, subject, queue);
        let gateway_handles: Vec<Arc<ConnHandle>> = shared
            .gateways
            .lock()
            .unwrap()
            .values()
            .filter(|g| !g.outbound && !g.handle.is_closed())
            .map(|g| g.handle.clone())
            .collect();
        for handle in gateway_handles {
            let _ = try_send(&handle, rframe.clone());
        }
    }
    let leafs: Vec<(u64, String, Arc<ConnHandle>)> = shared
        .leafs
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, l)| !l.handle.is_closed())
        .map(|(id, l)| (*id, l.account.clone(), l.handle.clone()))
        .collect();
    let lframe = encode_ls_plus(subject, queue);
    for (lid, l_acc, handle) in leafs {
        if l_acc == account
            && !leafs_had.contains(&lid)
            && pattern_in_use_for_leaf(shared, lid, &l_acc, subject, queue)
        {
            let _ = try_send(&handle, lframe.clone());
        }
    }
}

/// After removing interest for `(account, subject, queue)`, send `RS-`/`LS-`
/// to any route, leaf, or inbound gateway that no longer has any remaining source of interest.
pub(crate) fn notify_interest_removed(
    shared: &Arc<Shared>,
    account: &str,
    subject: &str,
    queue: Option<&str>,
) {
    if !pattern_in_use_for_routes(shared, account, subject, queue) {
        let rframe = encode_rs_minus(account, subject, queue);
        let route_handles: Vec<Arc<ConnHandle>> = shared
            .routes
            .lock()
            .unwrap()
            .values()
            .filter(|r| !r.handle.is_closed())
            .map(|r| r.handle.clone())
            .collect();
        for handle in route_handles {
            let _ = try_send(&handle, rframe.clone());
        }
        let gateway_handles: Vec<Arc<ConnHandle>> = shared
            .gateways
            .lock()
            .unwrap()
            .values()
            .filter(|g| !g.outbound && !g.handle.is_closed())
            .map(|g| g.handle.clone())
            .collect();
        for handle in gateway_handles {
            let _ = try_send(&handle, rframe.clone());
        }
    }
    let leafs: Vec<(u64, String, Arc<ConnHandle>)> = shared
        .leafs
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, l)| !l.handle.is_closed())
        .map(|(id, l)| (*id, l.account.clone(), l.handle.clone()))
        .collect();
    let lframe = encode_ls_minus(subject, queue);
    for (lid, l_acc, handle) in leafs {
        if l_acc == account && !pattern_in_use_for_leaf(shared, lid, &l_acc, subject, queue) {
            let _ = try_send(&handle, lframe.clone());
        }
    }
}

/// Does any local subscription (or JetStream internal pattern) still use
/// this pattern+queue in this account?
pub(crate) fn local_pattern_in_use(
    shared: &Arc<Shared>,
    account: &str,
    subject: &str,
    queue: Option<&str>,
) -> bool {
    if shared.jetstream.is_some()
        && (subject == "$JS.API.>" || subject == "$JS.ACK.>")
        && queue.is_none()
    {
        return true;
    }
    shared
        .sublist
        .read()
        .unwrap()
        .has_pattern(subject, queue, account)
}

/// Does any route have interest in `(account, subject, queue)`?
fn route_pattern_in_use(
    shared: &Arc<Shared>,
    account: &str,
    subject: &str,
    queue: Option<&str>,
) -> bool {
    let key = (
        account.to_string(),
        subject.to_string(),
        queue.map(|q| q.to_string()),
    );
    shared
        .route_subs
        .lock()
        .unwrap()
        .values()
        .any(|m| m.get(&key).copied().unwrap_or(0) > 0)
}

/// Does any leaf (other than `exclude_leaf`) have interest in `(account, subject, queue)`?
fn leaf_pattern_in_use(
    shared: &Arc<Shared>,
    account: &str,
    subject: &str,
    queue: Option<&str>,
    exclude_leaf: Option<u64>,
) -> bool {
    let key = (
        account.to_string(),
        subject.to_string(),
        queue.map(|q| q.to_string()),
    );
    shared
        .leaf_subs
        .lock()
        .unwrap()
        .iter()
        .filter(|(lid, _)| Some(**lid) != exclude_leaf)
        .any(|(_, m)| m.get(&key).copied().unwrap_or(0) > 0)
}

/// Should routes (and inbound gateways) be told this node has interest in `(account, subject, queue)`?
/// Routes form a full mesh (1-hop), so route interest is never re-advertised to
/// other routes; routes learn about local subscriptions AND leaf subscriptions.
fn pattern_in_use_for_routes(
    shared: &Arc<Shared>,
    account: &str,
    subject: &str,
    queue: Option<&str>,
) -> bool {
    local_pattern_in_use(shared, account, subject, queue)
        || leaf_pattern_in_use(shared, account, subject, queue, None)
}

/// Should leaf `lid` (in `leaf_account`) be told this node has interest in `(subject, queue)`?
/// A leaf learns about local subscriptions, route subscriptions, and OTHER leafs' subscriptions.
fn pattern_in_use_for_leaf(
    shared: &Arc<Shared>,
    lid: u64,
    leaf_account: &str,
    subject: &str,
    queue: Option<&str>,
) -> bool {
    local_pattern_in_use(shared, leaf_account, subject, queue)
        || route_pattern_in_use(shared, leaf_account, subject, queue)
        || leaf_pattern_in_use(shared, leaf_account, subject, queue, Some(lid))
}

/// Send every active subscription (local + transitive leaf/route interest + JetStream internal patterns)
/// to one newly connected route or leaf connection as RS+/LS+ frames.
pub(crate) fn send_local_interest(
    shared: &Arc<Shared>,
    handle: &ConnHandle,
    peer_id: u64,
    is_route: bool,
    leaf_account: Option<&str>,
) {
    let mut patterns: std::collections::BTreeSet<(String, String, Option<String>)> = {
        let sl = shared.sublist.read().unwrap();
        sl.patterns().into_iter().collect()
    };
    if is_route {
        // Routes also learn about all leaf subscriptions.
        let leaf_subs = shared.leaf_subs.lock().unwrap();
        for m in leaf_subs.values() {
            for (k, count) in m {
                if *count > 0 {
                    patterns.insert(k.clone());
                }
            }
        }
    } else if let Some(l_acc) = leaf_account {
        // A leaf also learns about route subscriptions and other leafs' subscriptions.
        {
            let route_subs = shared.route_subs.lock().unwrap();
            for m in route_subs.values() {
                for (k, count) in m {
                    if *count > 0 && k.0 == l_acc {
                        patterns.insert(k.clone());
                    }
                }
            }
        }
        {
            let leaf_subs = shared.leaf_subs.lock().unwrap();
            for (lid, m) in leaf_subs.iter() {
                if *lid == peer_id {
                    continue;
                }
                for (k, count) in m {
                    if *count > 0 && k.0 == l_acc {
                        patterns.insert(k.clone());
                    }
                }
            }
        }
    }
    for (account, subject, queue) in patterns {
        if is_route {
            let _ = try_send(handle, encode_rs_plus(&account, &subject, queue.as_deref()));
        } else if leaf_account == Some(account.as_str()) {
            let _ = try_send(handle, encode_ls_plus(&subject, queue.as_deref()));
        }
    }
    if shared.jetstream.is_some() {
        if is_route {
            // Advertise JetStream API interest for every account so
            // remote nodes can forward $JS.API requests per account.
            let mut accounts: Vec<String> = shared
                .config
                .accounts
                .iter()
                .map(|a| a.name.clone())
                .collect();
            if !accounts.contains(&"$G".to_string()) {
                accounts.push("$G".to_string());
            }
            for account in accounts {
                let _ = try_send(handle, encode_rs_plus(&account, "$JS.API.>", None));
                // Phase 32: $JS.ACK interest, so an ack for a stream
                // hosted elsewhere is forwarded instead of being
                // consumed-and-dropped locally (see
                // `jetstream::handle_message`).
                let _ = try_send(handle, encode_rs_plus(&account, "$JS.ACK.>", None));
            }
        } else if let Some(account) = leaf_account {
            let _ = try_send(handle, encode_ls_plus("$JS.API.>", None));
            let _ = try_send(handle, encode_ls_plus("$JS.ACK.>", None));
            let _ = account;
        }
    }
    if is_route {
        for account in shared.config.all_accounts() {
            for import in &account.imports {
                if import.kind == Some(crate::config::ImportKind::Stream) {
                    let _ = try_send(
                        handle,
                        encode_rs_plus(&import.source_account, &import.remote_subject, None),
                    );
                }
            }
        }
    } else if let Some(leaf_account) = leaf_account {
        for account in shared.config.all_accounts() {
            for import in &account.imports {
                if import.kind == Some(crate::config::ImportKind::Stream)
                    && import.source_account == leaf_account
                {
                    let _ = try_send(handle, encode_ls_plus(&import.remote_subject, None));
                }
            }
        }
    }
}

pub(crate) fn close_route(shared: &Arc<Shared>, id: u64) {
    let removed = shared.routes.lock().unwrap().remove(&id);
    let subs = shared.route_subs.lock().unwrap().remove(&id);
    shared.conn_info.lock().unwrap().remove(&id);
    if let Some(r) = removed {
        r.handle.mark_closed();
    }
    if let Some(map) = subs {
        for ((account, subject, queue), count) in map {
            if count > 0 {
                notify_interest_removed(shared, &account, &subject, queue.as_deref());
            }
        }
    }
}

/// Record a gateway reply-subject mapping: a message that arrived
/// on gateway `gid` carried `reply_to`, so a later publish to that
/// subject must flow back over `gid` even without RS+ interest.
/// Expired entries (> 60 s) are pruned on insert.
pub(crate) fn record_gateway_reply(shared: &Arc<Shared>, reply_to: &str, gid: u64) {
    let mut map = shared.gateway_replies.lock().unwrap();
    map.retain(|_, (_, at)| at.elapsed() < Duration::from_secs(60));
    map.insert(reply_to.to_string(), (gid, std::time::Instant::now()));
}

/// Forward a message whose subject is a recorded gateway reply
/// subject back over the connection the request arrived on, unless
/// that connection already received this message (`already_sent`).
/// Returns true when a frame was sent. Expired mappings are dropped.
pub(crate) fn forward_gateway_reply(
    shared: &Arc<Shared>,
    msg: &Message,
    already_sent: &std::collections::HashSet<u64>,
) -> bool {
    let gid = {
        let mut map = shared.gateway_replies.lock().unwrap();
        map.retain(|_, (_, at)| at.elapsed() < Duration::from_secs(60));
        match map.get(&msg.subject) {
            Some((gid, _)) => *gid,
            None => return false,
        }
    };
    if already_sent.contains(&gid) {
        return false;
    }
    let handle = shared
        .gateways
        .lock()
        .unwrap()
        .get(&gid)
        .filter(|g| !g.handle.is_closed())
        .map(|g| g.handle.clone());
    let Some(handle) = handle else {
        return false;
    };
    let frame = match &msg.headers {
        Some(h) => encode_route_hmsg(
            &msg.account,
            &msg.subject,
            msg.reply_to.as_deref(),
            &[],
            h,
            &msg.payload,
        ),
        None => encode_rmsg(
            &msg.account,
            &msg.subject,
            msg.reply_to.as_deref(),
            &[],
            &msg.payload,
        ),
    };
    if !try_send(&handle, frame) {
        eprintln!("rust-natsd: slow gateway {gid}, closing (reply)");
        close_gateway(shared, gid);
        return false;
    }
    true
}

pub(crate) fn close_gateway(shared: &Arc<Shared>, id: u64) {
    let removed = shared.gateways.lock().unwrap().remove(&id);
    shared.gateway_subs.lock().unwrap().remove(&id);
    shared.conn_info.lock().unwrap().remove(&id);
    if let Some(g) = removed {
        g.handle.mark_closed();
    }
}

/// Send every local subscription to one gateway connection as RS+
/// frames (all accounts; the account token scopes each frame).
/// Called ONLY for connections this server accepted: per Go's
/// directional rule the accepter sends RS+ and the dialer never
/// does (see gateway.rs). Unlike routes, no JetStream or
/// stream-import interest is advertised over gateways in Phase 33:
/// JetStream is single-node and cross-gateway imports are not part
/// of the verified matrix (documented in PLAN Phase 33).
pub(crate) fn send_gateway_interest(shared: &Arc<Shared>, handle: &ConnHandle) {
    let subs: Vec<(String, String, Option<String>)> = {
        let sl = shared.sublist.read().unwrap();
        sl.patterns()
    };
    for (account, subject, queue) in subs {
        let _ = try_send(handle, encode_rs_plus(&account, &subject, queue.as_deref()));
    }
}

// ---------------------------------------------------------------------------
// Gateways
// ---------------------------------------------------------------------------

pub(crate) async fn gateway_read_loop<R: AsyncReadExt + Unpin>(
    reader: &mut BufReader<R>,
    shared: &Arc<Shared>,
    id: u64,
    handle: &Arc<ConnHandle>,
    tx: &mpsc::Sender<Bytes>,
) {
    let mut parser = RouteParser::new(shared.config.max_payload);
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        if handle.is_closed() {
            break;
        }
        let n = tokio::select! {
            _ = handle.close.notified() => break,
            _ = shared.shutdown.notified() => break,
            r = reader.read(&mut buf) => match r {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            },
        };
        parser.feed(&buf[..n]);
        let mut close = false;
        loop {
            match parser.next_op() {
                Ok(Some(op)) => {
                    if !process_gateway_op(op, shared, id, handle, tx).await {
                        close = true;
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = tx.send(encode_err(&e.to_string())).await;
                    close = true;
                    break;
                }
            }
        }
        if close {
            break;
        }
    }
}

/// Process one gateway operation. Gateway ops are route ops plus
/// INFO frames carrying `gateway_cmd` (handled here) or a peer INFO
/// update (ignored: the handshake INFOs are consumed before this
/// loop starts, and Go sends no other INFO on a live gateway
/// connection). Returns false when the connection must close.
pub(crate) async fn process_gateway_op(
    op: RouteOp,
    shared: &Arc<Shared>,
    id: u64,
    handle: &Arc<ConnHandle>,
    tx: &mpsc::Sender<Bytes>,
) -> bool {
    match op {
        RouteOp::Info(json) => {
            if let Some(info) = crate::gateway::parse_gateway_info(&json) {
                if let (Some(cmd), Some(account)) = (info.gateway_cmd, info.gateway_cmd_account) {
                    let gateways = shared.gateways.lock().unwrap();
                    if let Some(g) = gateways.get(&id) {
                        let mut list = match cmd {
                            2 => g.announced_accounts.lock().unwrap(),
                            3 => g.interest_only_accounts.lock().unwrap(),
                            _ => return true,
                        };
                        if !list.contains(&account) {
                            list.push(account);
                        }
                    }
                }
            }
            true
        }
        RouteOp::Connect(_) | RouteOp::Pong => true,
        RouteOp::Ping => {
            let _ = tx.send(Bytes::from("PONG\r\n")).await;
            true
        }
        RouteOp::RsPlus {
            account,
            subject,
            queue,
            ..
        } => {
            // Go's directional rule, enforced symmetrically: RS+
            // arrives only from the accepter, i.e. on connections
            // this server solicited (outbound). RS+ arriving on a
            // connection this server accepted means the dialer sent
            // it — Go closes that connection with a protocol
            // violation, and so does this server.
            let inbound = shared
                .gateways
                .lock()
                .unwrap()
                .get(&id)
                .is_some_and(|g| !g.outbound);
            if inbound {
                let _ = tx.send(encode_err("Protocol Violation")).await;
                return false;
            }
            let mut subs = shared.gateway_subs.lock().unwrap();
            if let Some(m) = subs.get_mut(&id) {
                bump_interest(m, &account, &subject, queue.as_deref(), 1);
            }
            true
        }
        RouteOp::RsMinus {
            account,
            subject,
            queue,
            ..
        } => {
            let inbound = shared
                .gateways
                .lock()
                .unwrap()
                .get(&id)
                .is_some_and(|g| !g.outbound);
            if inbound {
                let _ = tx.send(encode_err("Protocol Violation")).await;
                return false;
            }
            let mut subs = shared.gateway_subs.lock().unwrap();
            if let Some(m) = subs.get_mut(&id) {
                bump_interest(m, &account, &subject, queue.as_deref(), -1);
            }
            true
        }
        RouteOp::RMsg {
            account,
            subject,
            reply_to,
            queues,
            headers,
            payload,
            ..
        } => {
            handle.in_msgs.fetch_add(1, Ordering::Relaxed);
            handle.in_bytes.fetch_add(
                (headers.as_ref().map(|h| h.len()).unwrap_or(0) + payload.len()) as u64,
                Ordering::Relaxed,
            );
            shared.stats.in_msgs.fetch_add(1, Ordering::Relaxed);
            shared.stats.in_bytes.fetch_add(
                (headers.as_ref().map(|h| h.len()).unwrap_or(0) + payload.len()) as u64,
                Ordering::Relaxed,
            );
            // Both RMSG and HMSG frames carry the selected queue group list
            // (`| <queues>`), restricting delivery to the listed groups.
            let filter = Some(queues.as_slice());
            // Phase 33: remember the reply subject's return path
            // (see forward_gateway_reply) before delivering.
            if let Some(reply) = &reply_to {
                record_gateway_reply(shared, reply, id);
            }
            deliver(
                shared,
                &Message::new(&subject, reply_to, headers, payload).with_account(&account),
                Origin::Gateway(id),
                filter,
            );
            true
        }
        RouteOp::Err(e) => {
            eprintln!("rust-natsd: gateway {id} error: {e}");
            false
        }
    }
}

pub(crate) fn close_leaf(shared: &Arc<Shared>, id: u64) {
    let removed = shared.leafs.lock().unwrap().remove(&id);
    let subs = shared.leaf_subs.lock().unwrap().remove(&id);
    shared.conn_info.lock().unwrap().remove(&id);
    if let Some(l) = removed {
        l.handle.mark_closed();
    }
    if let Some(map) = subs {
        for ((account, subject, queue), count) in map {
            if count > 0 {
                notify_interest_removed(shared, &account, &subject, queue.as_deref());
            }
        }
    }
}

/// Bump a counted interest entry by `delta` (+1 / -1), removing at zero.
pub(crate) fn bump_interest(
    map: &mut InterestMap,
    account: &str,
    subject: &str,
    queue: Option<&str>,
    delta: i32,
) {
    let key = (
        account.to_string(),
        subject.to_string(),
        queue.map(|q| q.to_string()),
    );
    let entry = map.entry(key).or_insert(0);
    if delta > 0 {
        *entry = entry.saturating_add(delta as u32);
    } else {
        *entry = entry.saturating_sub((-delta) as u32);
        if *entry == 0 {
            map.remove(&(
                account.to_string(),
                subject.to_string(),
                queue.map(|q| q.to_string()),
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Client connections
// ---------------------------------------------------------------------------

pub(crate) fn json_str(json: &str, field: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()?
        .get(field)?
        .as_str()
        .map(|s| s.to_string())
}

pub(crate) fn json_u64(json: &str, field: &str) -> Option<u64> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()?
        .get(field)?
        .as_u64()
}

pub(crate) fn json_bool(json: &str, field: &str) -> Option<bool> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()?
        .get(field)?
        .as_bool()
}

/// Read one CRLF-terminated line from a buffered reader. Generic over
/// the read half so plain TCP and TLS streams share the route/leaf
/// handshake and read loops.
pub(crate) async fn read_line<R: AsyncReadExt + Unpin>(
    reader: &mut BufReader<R>,
) -> io::Result<String> {
    let mut buf = Vec::new();
    let n = reader.read_until(b'\n', &mut buf).await?;
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
    }
    Ok(String::from_utf8_lossy(&buf).trim_end().to_string())
}

pub(crate) async fn route_read_loop<R: AsyncReadExt + Unpin>(
    reader: &mut BufReader<R>,
    shared: &Arc<Shared>,
    id: u64,
    handle: &Arc<ConnHandle>,
    tx: &mpsc::Sender<Bytes>,
) {
    let mut parser = RouteParser::new(shared.config.max_payload);
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        if handle.is_closed() {
            break;
        }
        let n = tokio::select! {
            _ = handle.close.notified() => break,
            _ = shared.shutdown.notified() => break,
            r = reader.read(&mut buf) => match r {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            },
        };
        parser.feed(&buf[..n]);
        let mut close = false;
        loop {
            match parser.next_op() {
                Ok(Some(op)) => {
                    if !process_route_op(op, shared, id, handle, tx).await {
                        close = true;
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = tx.send(encode_err(&e.to_string())).await;
                    close = true;
                    break;
                }
            }
        }
        if close {
            break;
        }
    }
}

/// Process one route operation. Returns false when the connection must close.
pub(crate) async fn process_route_op(
    op: RouteOp,
    shared: &Arc<Shared>,
    id: u64,
    handle: &Arc<ConnHandle>,
    tx: &mpsc::Sender<Bytes>,
) -> bool {
    match op {
        RouteOp::Info(_) | RouteOp::Connect(_) | RouteOp::Pong => true,
        RouteOp::Ping => {
            let _ = tx.send(Bytes::from("PONG\r\n")).await;
            true
        }
        RouteOp::RsPlus {
            account,
            subject,
            queue,
            ..
        } => {
            let before = interest_targets_before_add(shared, &account, &subject, queue.as_deref());
            {
                let mut subs = shared.route_subs.lock().unwrap();
                if let Some(m) = subs.get_mut(&id) {
                    bump_interest(m, &account, &subject, queue.as_deref(), 1);
                }
            }
            notify_interest_added(shared, &account, &subject, queue.as_deref(), before);
            true
        }
        RouteOp::RsMinus {
            account,
            subject,
            queue,
            ..
        } => {
            {
                let mut subs = shared.route_subs.lock().unwrap();
                if let Some(m) = subs.get_mut(&id) {
                    bump_interest(m, &account, &subject, queue.as_deref(), -1);
                }
            }
            notify_interest_removed(shared, &account, &subject, queue.as_deref());
            true
        }
        RouteOp::RMsg {
            account,
            subject,
            reply_to,
            queues,
            headers,
            payload,
            ..
        } => {
            handle.in_msgs.fetch_add(1, Ordering::Relaxed);
            handle.in_bytes.fetch_add(
                (headers.as_ref().map(|h| h.len()).unwrap_or(0) + payload.len()) as u64,
                Ordering::Relaxed,
            );
            shared.stats.in_msgs.fetch_add(1, Ordering::Relaxed);
            shared.stats.in_bytes.fetch_add(
                (headers.as_ref().map(|h| h.len()).unwrap_or(0) + payload.len()) as u64,
                Ordering::Relaxed,
            );
            // Both RMSG and HMSG frames carry the selected queue group list
            // (`| <queues>`), restricting delivery to the listed groups.
            let filter = Some(queues.as_slice());
            deliver(
                shared,
                &Message::new(&subject, reply_to, headers, payload).with_account(&account),
                Origin::Route(id),
                filter,
            );
            true
        }
        RouteOp::Err(e) => {
            eprintln!("rust-natsd: route {id} error: {e}");
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Leaf nodes
// ---------------------------------------------------------------------------

pub(crate) async fn leaf_read_loop<R: AsyncReadExt + Unpin>(
    reader: &mut BufReader<R>,
    shared: &Arc<Shared>,
    id: u64,
    handle: &Arc<ConnHandle>,
    tx: &mpsc::Sender<Bytes>,
) {
    let mut parser = LeafParser::new(shared.config.max_payload);
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        if handle.is_closed() {
            break;
        }
        let n = tokio::select! {
            _ = handle.close.notified() => break,
            _ = shared.shutdown.notified() => break,
            r = reader.read(&mut buf) => match r {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            },
        };
        parser.feed(&buf[..n]);
        let mut close = false;
        loop {
            match parser.next_op() {
                Ok(Some(op)) => {
                    if !process_leaf_op(op, shared, id, handle, tx).await {
                        close = true;
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = tx.send(encode_err(&e.to_string())).await;
                    close = true;
                    break;
                }
            }
        }
        if close {
            break;
        }
    }
}

pub(crate) async fn process_leaf_op(
    op: LeafOp,
    shared: &Arc<Shared>,
    id: u64,
    handle: &Arc<ConnHandle>,
    tx: &mpsc::Sender<Bytes>,
) -> bool {
    match op {
        LeafOp::Info(_) | LeafOp::Connect(_) | LeafOp::Pong => true,
        LeafOp::Ping => {
            let _ = tx.send(Bytes::from("PONG\r\n")).await;
            true
        }
        LeafOp::LsPlus { subject, queue } => {
            let leaf_account = shared
                .leafs
                .lock()
                .unwrap()
                .get(&id)
                .map(|l| l.account.clone())
                .unwrap_or_else(|| "$G".to_string());
            let before =
                interest_targets_before_add(shared, &leaf_account, &subject, queue.as_deref());
            {
                let mut subs = shared.leaf_subs.lock().unwrap();
                if let Some(m) = subs.get_mut(&id) {
                    bump_interest(m, &leaf_account, &subject, queue.as_deref(), 1);
                }
            }
            notify_interest_added(shared, &leaf_account, &subject, queue.as_deref(), before);
            true
        }
        LeafOp::LsMinus { subject, queue } => {
            let leaf_account = shared
                .leafs
                .lock()
                .unwrap()
                .get(&id)
                .map(|l| l.account.clone())
                .unwrap_or_else(|| "$G".to_string());
            {
                let mut subs = shared.leaf_subs.lock().unwrap();
                if let Some(m) = subs.get_mut(&id) {
                    bump_interest(m, &leaf_account, &subject, queue.as_deref(), -1);
                }
            }
            notify_interest_removed(shared, &leaf_account, &subject, queue.as_deref());
            true
        }
        LeafOp::LMsg {
            subject,
            reply_to,
            queues,
            headers,
            payload,
            ..
        } => {
            handle.in_msgs.fetch_add(1, Ordering::Relaxed);
            handle.in_bytes.fetch_add(
                (headers.as_ref().map(|h| h.len()).unwrap_or(0) + payload.len()) as u64,
                Ordering::Relaxed,
            );
            shared.stats.in_msgs.fetch_add(1, Ordering::Relaxed);
            shared.stats.in_bytes.fetch_add(
                (headers.as_ref().map(|h| h.len()).unwrap_or(0) + payload.len()) as u64,
                Ordering::Relaxed,
            );
            let leaf_account = shared
                .leafs
                .lock()
                .unwrap()
                .get(&id)
                .map(|l| l.account.clone())
                .unwrap_or_else(|| "$G".to_string());
            deliver(
                shared,
                &Message::new(&subject, reply_to, headers, payload).with_account(&leaf_account),
                Origin::Leaf(id),
                queues.as_deref(),
            );
            true
        }
        LeafOp::Err(e) => {
            eprintln!("rust-natsd: leaf {id} error: {e}");
            false
        }
    }
}
