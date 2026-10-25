#![forbid(unsafe_code)]
//! rust-natsd: a NATS server implementing the Core NATS pub-sub protocol.
//!
//! See `PLAN.md` in the repository for the phased implementation plan.

pub mod conf;
pub mod config;
pub mod gateway;
pub mod jetstream;
pub mod jwt;
pub mod leaf;
pub mod monitor;
pub mod protocol;
pub mod route;
pub mod server;
pub mod subject;
pub mod sublist;
pub mod tls;
