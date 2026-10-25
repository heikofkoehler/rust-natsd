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

Phases 0–16 **complete**: Core NATS pub-sub, routes/clustering, leaf
nodes, JetStream (in-memory or file-persisted), TLS client connections,
NKey authentication, multi-account scoping, the monitoring endpoint,
and `.conf` file support are implemented and verified against the
unmodified official Go (`nats.go` v1.55.0) and Rust (`async-nats` 0.38)
clients, plus raw-wire protocol tests. 97 Rust tests green; Go interop
scripts green in both directions (routes, leafs, JetStream).

Remaining non-goals: gateways, JetStream clustering/replication,
mirroring/sourcing, MQTT/WebSocket gateways, JWT operator mode,
route/leaf TLS, TLS client-certificate-to-user mapping, and the
KV/object-store client helpers (their server-side primitives exist).

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
  Matching is a subject trie (exact / `*` / `>` nodes, insertion order
  restored after collection); the 10k-subscription harness in
  `examples/sublist_bench.rs` measures it.
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

### Phase 12 — TLS (client connections)
- rustls (ring provider; aws-lc-rs is not in the offline crate cache)
  + tokio-rustls + rustls-pemfile. `--tls/--tlscert/--tlskey/
  --tlsverify/--tlscacert` flags and a `tls {}` block in `.conf`;
  `tls_required` in `INFO`. Go's STARTTLS order is implemented: INFO
  goes out in plaintext first, then the client upgrades the same socket
  to TLS (a direct/implicit-TLS client fails on its own side, and a
  plain client after INFO is closed silently — both verified against
  Go). Route/leaf paths stay plain TCP.
- Verified: `tests/e2e_tls.rs` (openssl-generated CA/certs; TLS 1.3
  note: a no-cert client `connect()` can succeed while the server's
  certificate-required alert surfaces on first read — the test
  asserts on first read, not on connect).

### Phase 13 — NKey authentication
- `nkey`/`sig`/`jwt` fields in `ConnectInfo`; per-connection `nonce`
  sent in INFO only when authentication is configured (Go omitempty);
  signatures verified with the `nkeys` crate (base64 via the `base64`
  crate). JWT operator mode is out of scope.
- Verified: `tests/e2e_nkey.rs` (in-memory throwaway keypair).

### Phase 14 — Multi-account
- `Account`/`AccountUser`/`Permissions` model; `.conf` `accounts {}`
  (both Go's per-user `permissions` and account-level `authorization`
  shapes), `system_account`, `no_auth_user`. Legacy
  `--user/--pass/--auth-token` is the `$G` account.
- Subscriptions, deliveries, route frames (account token, no longer
  hardcoded `$G`) and leaf connections are account-scoped;
  publish/subscribe violations send Go's exact `-ERR` text and keep
  the connection alive (verified against nats-server v2.15.1).
  JetStream streams are account-scoped; stream names stay node-global.
- Verified: `tests/e2e_accounts.rs` (cross-account isolation both
  ways, violation text, conf shape).

### Phase 15 — JetStream upgrades
- File persistence behind `--jetstream` + `--js-store-dir` /
  `jetstream.store_dir`: whole-engine JSON snapshot (base64 payloads)
  written atomically after every mutation, reloaded on startup;
  pending ack state is NOT persisted. Workqueue retention, 
  `max_msgs_per_subject`, deliver_policy `last` (filter-aware) and
  `by_start_sequence`. Filtered consumer creates
  (`CREATE.<stream>.<consumer>.<filter…>`) fixed.
- Verified: `tests/e2e_js_persistence.rs`.

### Phase 16 — Monitoring upgrades
- `/varz`: real `http_port`, `auth_required`/`tls_required` from
  config, `mem` from `/proc/self/statm` on Linux, `slow_consumers`
  from a real counter. New `/accounz` (connection/subscription counts
  per account) and `/gatewayz` (empty list). `/connz` gains
  `acc`/`state`/`limit`/`offset` and `account`/`user`/`tls` fields.
  `/subz` and `/leafz` report the real account per entry.
- Verified: `tests/e2e_monitor.rs`.

### Phase 17 — Go-vs-Rust parity suite (test-only, no `src/` changes)
- `scripts/parity.sh` + `parity/probes.py` + `parity/go_client/`:
  repeatable side-by-side run of Go nats-server v2.15.1 and rust-natsd
  on free ports with matching configs. Raw-wire probes (core, auth,
  token, limits, subject validation, reply, TLS, nkey, accounts,
  monitoring, raw $KV) plus a nats.go probe client (JetStream pull/push,
  acks/redelivery, MSG.GET, purge/delete, workqueue, persistence across
  restart, KV basics) and the existing cluster/leaf smoke scripts.
- Normalisation: strip server_id, client_id, ports, timestamps,
  version, git_commit and other per-run values; compare semantic
  outcomes. The parity-fix pass eliminated every known server diff
  (subjects, pedantic, verbose order, duplicate sid, no-responders,
  server PING, token-as-password, malformed CONNECT, error texts,
  INFO richness, STARTTLS order, empty-stream FirstSeq); what remains
  is documented exceptions only (Go-specific INFO fields, monitoring
  superset, TLS refusal-layer variance, the nats.go KV version gate —
  see `parity/KNOWN_DIFFS.md`). The run is green in both default and
  strict mode. `cargo test` unchanged and green.
- Verified: suite green in default and strict mode against HEAD (53
  raw probes: 52 pass + 1 exception, 0 fail; JS full PASS;
  persistence PASS; cluster/leaf smoke PASS); see
  `parity/KNOWN_DIFFS.md` for the per-area results.

### Phase 18 — Slow-consumer fix (writer batching + byte-based pending limit)
- Diagnosis (2026-10-24 benchmark): a burst publisher enqueued far
  faster than the per-connection writer drained (one `write_all`
  syscall per frame, ~100-200k small frames/s), so the old 1024-frame
  queue filled after only ~100 deliveries to a fast reader, which was
  then dropped as a "slow consumer". Go, by contrast, batches its
  outbound writes, stalls the producer under pending pressure, and
  drops only after a 10 s write deadline (never-reading subscriber
  probe: Go dropped at ~424k published 256 B messages; Rust dropped
  at ~254k enqueued frames once the limit below was in place).
- Fix, in `src/server.rs`: the writer coalesces queued frames into
  64 KiB batched socket writes; the frame-count bound is replaced by
  a Go-like bytes-based pending limit, `Config::max_pending`
  (default 64 MiB, Go's `max_pending` default), configurable via
  `--max-pending` / .conf `max_pending`, reported in `/varz` as
  `max_pending` and per-connection in `/connz` as `pending_bytes`.
  Reserved on enqueue (CAS) in `try_send`, released by the writer
  after a successful write. A fast reader at bench rates is not
  dropped; a never-reading reader still is, at the byte limit. No
  publisher-side backpressure was needed, so parity semantics are
  unchanged (Go drops slow consumers; so does rust-natsd). The
  connection registry lock in `send_local_frames` is now taken once
  per message batch of deliveries, not once per delivery.
- Tests: `burst_to_fast_reader_not_dropped` (50k x 256 B to a fast
  reader, all delivered, connection alive) and
  `never_reading_subscriber_dropped_at_byte_limit` (4 KiB limit,
  5000 x 256 B to a never-reading subscriber, connection closed)
  in `tests/e2e_raw.rs`.

### Phase 19 — JetStream persistence completeness (gap area 4)
- Pending ack state is now persisted in the whole-engine JSON
  snapshot (version 2; version 1 snapshots still load, with empty ack
  state): per-consumer pending entries (consumer seq, stream seq,
  delivery count, wall-clock delivery time), ack floors, redelivery
  counts, and pull redelivery queues. On reload, each pending entry's
  deadline is recomputed as delivery_time + ack_wait, preserving the
  remaining ack window; pull entries already past their deadline are
  queued for immediate redelivery, push entries get a now deadline
  for the next tick. Pull redelivery now carries the incremented
  delivery count (previously reset to 1), and tick redelivery order
  is stream order. Push redelivery defers while the deliver subject
  has no local/route/leaf interest (new `has_interest` helper in
  `src/server.rs`) instead of burning a delivery cycle on a dropped
  message, matching Go's late-subscriber behaviour. Waiting pulls are
  deliberately not persisted (they belong to a dead connection).
- Go semantics probed first against nats-server v2.15.1 (pull and
  push, file store): no redelivery immediately after a restart
  inside the ack window (fresh messages still served); redelivery
  after the window with delivered=2 and fresh consumer seqs, in
  stream order; acked messages never redelivered; a restart after
  the original window expired redelivers immediately.
- Verified: 4 new tests in `tests/e2e_js_persistence.rs` (pull
  redelivery, acked-not-redelivered, push redelivery, dedup after
  restart); cargo test 110 green; parity GREEN (52 pass + 1
  exception); bench no regression vs 86edbe8.

## Future work (not in this plan)
- Gateways, JetStream clustering/replication, mirroring/sourcing,
  MQTT/WebSocket gateways, JWT operator mode, route/leaf TLS,
  TLS client-certificate-to-user mapping,
  KV/object-store client-helper verification.

## Design notes
- Single shared subscription registry behind a lock; delivery resolves the
  full recipient set under the lock, then writes outside it, so a slow or dead
  subscriber never blocks publishers or the registry.
- Parser is a pure state machine over a byte buffer (no I/O), which is what
  makes the split-at-every-byte unit tests possible.
- No unsafe code. `#![forbid(unsafe_code)]` in the library.
- Parser copy audit (gap area 3): the client parser was already
  zero-copy on the PUB/HPUB hot path — `BytesMut::split_to` + `freeze`,
  with HPUB headers/payload as `Bytes::slice` views — so it was left
  alone. The route and leaf parsers were not: they
  `Bytes::copy_from_slice`d every forwarded body, and now freeze+slice
  like the client parser. Copies that remain are necessary, not
  oversights: `feed()` copies socket bytes into the parser buffer once
  (the readers use `read` into a stack buffer; removing that copy would
  mean a `read_buf`-into-`BytesMut` reader-loop change, not a parser
  change), control lines are small `String`s, and `encode_msg`/`encode_hmsg`
  copy the payload once per recipient into the contiguous
  header+payload+CRLF frame the writer sends (a scatter/gather writer
  would be needed to avoid that, out of scope).
