# rust-natsd

A NATS server implemented in Rust, from scratch — implementing the
**Core NATS pub-sub protocol** (the client protocol of the Go `nats-server`,
formerly `gnatsd`).

Status: **in development** — see [PLAN.md](PLAN.md) for the phased plan and
what is implemented so far.

## Design

- Pure, split-safe incremental parser for the wire protocol (`src/protocol.rs`).
- Subject validation and wildcard matching (`*`, `>`) in `src/subject.rs`.
- Tokio-based server core: one reader/writer task pair per connection, one
  shared subscription registry, queue-group delivery, header support
  (`HPUB`/`HMSG`), simple user/password and token auth.
- No unsafe code (`#![forbid(unsafe_code)]`).

Not affiliated with the NATS project or Synadia.
