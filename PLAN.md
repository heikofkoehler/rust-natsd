# rust-natsd — Plan

A NATS server implemented in Rust, from scratch. Scope for this plan:
**Core NATS pub-sub protocol** (the client protocol spoken by `nats-server`,
formerly `gnatsd`, in Go), plus routes/clustering, leaf nodes, in-memory
JetStream, a monitoring endpoint, and `.conf` file support — complete
enough that unmodified Go (`nats.go`) and Rust (`async-nats`) clients can
connect, publish, subscribe, request/reply, cluster, and use JetStream
against it.

Explicitly out of scope (see end): gateways, JetStream
clustering/replication and file persistence, MQTT/WebSocket gateways.

## Status (2026-10-24)

Phases 0–11 **complete**: Core NATS pub-sub, routes/clustering, leaf
nodes, JetStream (in-memory), the monitoring endpoint, and `.conf` file
support are implemented and verified against the unmodified official Go
(`nats.go` v1.55.0) and Rust (`async-nats` 0.38) clients, plus raw-wire
protocol tests. 85 Rust tests green; Go interop scripts green in both
directions (routes, leafs, JetStream).

Remaining non-goals: gateways, JetStream clustering/replication, file
persistence, MQTT/WebSocket gateways, and the KV/object-store client
helpers (their server-side primitives exist).

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

### Phase 7 — Routes / clustering
- Go-compatible route handshake (dialer sends CONNECT first), full mesh,
  solicitation with retry/backoff, duplicate resolution (smaller server id
  wins), RS+/RS- propagation, RMSG/HMSG, connect-time interest sync.
- Global queue selection: one delivery per queue group across local
  members, routes, and leafs, weighted by member count.
- Verified: 6 cluster e2e tests + `scripts/e2e-go-cluster.sh` (Go↔Rust
  routes, both seed directions).

### Phase 8 — Leaf nodes
- Hub accept path + solicited leaf, LS+/LS-, LMSG, leaf restart/reconnect,
  late subscriptions.
- Interop fixes found against Go 2.15.1: LS+ queue weight, HMSG for
  header messages over leafs, hub INFO `client_id` + `leafnode_urls`,
  CONNECT-without-INFO when `info_on_connect` is unset.
- Verified: 6 leaf e2e tests + `scripts/e2e-go-leaf.sh` (both directions).

### Phase 9 — JetStream (in-memory)
- Streams (create/info/list/delete/purge/msg.get), publish acks,
  `Nats-Msg-Id` dedup, expected-last-sequence/subject checks,
  max_msgs/max_bytes/max_age (discard-old).
- Consumers: durable pull (MSG.NEXT, batch/expires/no_wait, waiting pulls,
  404 status on expiry) and push (backlog drain at creation), explicit /
  none / all ack policies, ack floor, max_ack_pending, max_deliver,
  -NAK/+TERM/+WPI, ack-wait redelivery.
- Flat nats-server JSON shapes (verified against real 2.15.1 responses),
  matching error codes (10059/10014/10037), legacy
  `CONSUMER.DURABLE.CREATE`, `STREAM.NAMES`.
- Verified: 8 async-nats e2e tests + `scripts/e2e-go-js.sh` (nats.go JS
  client). Per-node streams (no replication); file storage accepted but
  stored in memory.

### Phase 10 — Monitoring endpoint
- Hand-rolled HTTP/1.1 server on `--http-port`: `/varz`, `/connz`,
  `/subz`, `/routez`, `/leafz`, `/jsz`, `/healthz`. Only tracked fields
  filled in; untracked ones report 0. `/jsz` is 404 without JetStream.
- Verified: `tests/e2e_monitor.rs`.

### Phase 11 — Config file compatibility
- `.conf` parser (blocks, lists, quoted strings, comments, URLs intact)
  for: port, host, server_name, http_port, max_payload,
  `authorization{}`, `cluster{name,host,port,routes[]}`,
  `leafnodes{port,remotes[],authorization{}}`, `jetstream` (bool or block).
- `-c` / `--config` flag; file loads first, explicit CLI flags win.
- Verified: conf unit tests + `tests/e2e_conf.rs`.

## Future work (not in this plan)
- Gateways, JetStream clustering/replication, file persistence,
  MQTT/WebSocket gateways, KV/object-store client-helper verification.

## Design notes
- Single shared subscription registry behind a lock; delivery resolves the
  full recipient set under the lock, then writes outside it, so a slow or dead
  subscriber never blocks publishers or the registry.
- Parser is a pure state machine over a byte buffer (no I/O), which is what
  makes the split-at-every-byte unit tests possible.
- No unsafe code. `#![forbid(unsafe_code)]` in the library.
