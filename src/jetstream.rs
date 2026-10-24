//! JetStream engine (Phase 9 placeholder).
//!
//! This module is intentionally minimal until Phase 9 lands: it exposes the
//! hooks the server core calls so routes/leafs compile and run without
//! JetStream, and documents the contract Phase 9 implements.

use std::sync::Arc;

use crate::server::{Message, Origin, Shared};

/// In-memory JetStream engine handle. Phase 9 fills in streams, consumers,
/// acknowledgements and the $JS.API surface.
pub struct JetStream {}

impl JetStream {
    pub fn new() -> Self {
        JetStream {}
    }
}

impl Default for JetStream {
    fn default() -> Self {
        Self::new()
    }
}

/// Called for every message before core delivery. Returns true when
/// JetStream consumed the message (API request or ack) so it must not also
/// be delivered as an ordinary message. The Phase 9 implementation stores
/// stream messages, answers $JS.API requests from the hosting node, and
/// processes $JS.ACK messages.
pub fn handle_message(
    _shared: &Arc<Shared>,
    _js: &Arc<JetStream>,
    _msg: &Message,
    _origin: Origin,
) -> bool {
    false
}

/// Background task: redelivers consumer messages whose ack deadline passed.
/// Phase 9 implements it; the placeholder simply waits for shutdown.
pub async fn redelivery_loop(_js: Arc<JetStream>, shared: Arc<Shared>) {
    shared.shutdown.notified().await;
}
