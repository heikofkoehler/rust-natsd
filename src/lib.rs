#![forbid(unsafe_code)]
//! rust-natsd: a NATS server implementing the Core NATS pub-sub protocol.
//!
//! See `PLAN.md` in the repository for the phased implementation plan.

pub mod protocol;
pub mod subject;
