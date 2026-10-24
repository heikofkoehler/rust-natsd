# rust-natsd — Plan

A NATS server implemented in Rust, from scratch. Scope for this plan:
**Core NATS pub-sub protocol** (the client protocol spoken by `nats-server`,
formerly `gnatsd`, in Go) — complete enough that unmodified Go (`nats.go`) and
Rust (`async-nats`) clients can connect, publish, subscribe, and request/reply
against it.

Explicitly out of scope (future phases, see end): JetStream persistence,
clustering/routes, gateways, leaf nodes, MQTT/WebSocket gateways, monitoring
HTTP endpoint.

## Phases

### Phase 0 — Scaffolding
- Cargo project (lib + bin), git repo, GitHub repo `heikofkoehler/rust-natsd`.
- This plan, README skeleton, `.gitignore`.
- Definition of done per phase: tests green locally, commit + push.

### Phase 1 — Wire protocol + subject model
- Incremental, split-safe parser for the client protocol:
  `CONNECT`, `PING`, `PONG`, `PUB`, `HPUB`, `SUB`, `UNSUB`, and server→client
  encoders for `INFO`, `MSG`, `HMSG`, `+OK`, `-ERR`.
- Subject model: token validation, wildcard matching (`*` single token,
  `>` terminal multi-token), publish vs. subscribe subject rules.
- Unit tests: parser under arbitrary byte splits, malformed ops, oversized
  control lines / payloads, subject matching truth tables.

### Phase 2 — Server core
- Tokio TCP listener; per-connection reader/writer tasks; bounded outbound
  channel with slow-consumer disconnect.
- Subscription registry (sublist): fan-out delivery, queue groups
  (one-of delivery per queue group per message), `UNSUB` with max-messages,
  auto-unsubscribe accounting, subscription cleanup on disconnect.
- `echo` semantics from `CONNECT`; request/reply works via reply subjects
  (plain pub-sub, covered by e2e).
- Unit + raw-protocol integration tests.

### Phase 3 — Protocol completeness for Core
- Headers: `HPUB` ingest, `HMSG` delivery to header-capable clients, header
  stripping for clients that did not negotiate headers.
- Auth: `--user/--pass` and `--auth-token` checked against `CONNECT`;
  `-ERR 'Authorization Violation'` + close on failure.
- Max payload enforcement (`-ERR 'Maximum Payload Violation'`), unknown op /
  protocol errors, `verbose` `+OK` mode.
- CLI: `--host`, `--port`, `--name`, `--max-payload`, `--user`, `--pass`,
  `--auth-token`. Graceful shutdown on SIGINT/SIGTERM.
- Tests for each behavior.

### Phase 4 — Rust client e2e
- Integration tests using the official Rust client (`async-nats`) as a
  dev-dependency: connect, pub/sub, wildcards, queue groups, request/reply,
  headers, auto-unsubscribe, multiple concurrent clients.

### Phase 5 — Go client e2e + CI
- Go program under `e2e/go` using the official `nats.go` client exercising the
  same matrix; `scripts/e2e-go.sh` builds the server, runs the program,
  reports pass/fail.
- GitHub Actions CI: `cargo fmt --check`, `cargo clippy -D warnings`,
  `cargo test`, Go e2e job.

### Phase 6 — Coverage + docs
- Coverage pass (llvm-cov if available) to find untested branches; add tests.
- README: build/run, protocol support matrix, test instructions, design notes.

## Future phases (not in this plan's execution)
- Phase 7: Route/cluster protocol. Phase 8: Leaf nodes. Phase 9: JetStream.
- Phase 10: Monitoring endpoint (`/varz`, `/connz`). Phase 11: Config file
  compatibility with `nats-server` `.conf` syntax.

## Design notes
- Single shared subscription registry behind a lock; delivery resolves the
  full recipient set under the lock, then writes outside it, so a slow or dead
  subscriber never blocks publishers or the registry.
- Parser is a pure state machine over a byte buffer (no I/O), which is what
  makes the split-at-every-byte unit tests possible.
- No unsafe code. `#![forbid(unsafe_code)]` in the library.
