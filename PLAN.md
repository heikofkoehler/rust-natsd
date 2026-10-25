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
cross-node mirroring/sourcing, MQTT/WebSocket gateways, JWT operator
mode, and the
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

### Phase 20 — JetStream mirroring/sourcing, same node (gap area 5)
- `STREAM.CREATE` accepts `mirror` and `sources` (each: `name`,
  `filter_subject`, `opt_start_seq`, `opt_start_time`; wire shape
  and honoured fields probed first against Go v2.15.1 with nats.go
  and raw `$JS.API` MSG.GET). Backfill at creation — sources in
  config order, from `opt_start_seq` / `opt_start_time` when given,
  honouring `filter_subject` — then live copies hooked into
  `capture()` under the engine lock, so backfill + link
  registration are atomic (no loss, no duplicates, no polling
  loop). Chains propagate (mirror of a mirror, source of a source)
  via recursion with a visited guard. Mirror copies are exact
  (origin subject, fresh mirror sequence, original headers and
  timestamp preserved); source copies add a `Nats-Stream-Source:
  <origin> <origin_seq> <filter|">"> <subject> <origin_token>`
  header (Go's trailing token is a per-origin-stream id; here a
  per-stream token generated at creation and persisted in the
  snapshot). Mirror streams are normal streams (MSG.GET,
  consumers, INFO with `mirror`/`sources` in config all work).
- Validation, matching Go where probed: mirror with subjects and
  mirror with sources are rejected, as are config-graph cycles
  (transitive, counting dead links — Go rejects recreating a
  stream that would close a loop through a deleted stream's
  config). One deliberate deviation, required by the task: a
  mirror/source of a nonexistent stream is rejected with
  404/10059, where Go accepts it and leaves the stream empty.
  `external` and `subject_transforms` are rejected: cross-node
  mirroring/sourcing is out of scope and is not faked.
- Origin lifecycle (probed): deleting an origin keeps the mirror's
  copies but kills the live link permanently — recreating a
  same-named origin does not revive it (the `live` flag on each
  link is persisted, so restarts do not revive dead links either);
  purging an origin leaves the mirror untouched. Backfill is
  synchronous here where Go's is asynchronous; the final state
  matches. A sourced stream may also carry subjects (direct
  publishes are then captured as well as source copies, as Go does).
- Verified: 6 new tests in `tests/e2e_js_mirror.rs` (backfill +
  live, two origins, filter + start_seq, nonexistent rejection,
  config validation, delete/no-resume, restart + live resume);
  cargo test 116 green; parity GREEN (52 pass + 1 exception);
  bench no regression vs 86edbe8.

### Phase 21 — Account imports/exports (gap area 6)
- Go semantics probed first against nats-server v2.15.1: the `.conf`
  import key is `to` (v2.15.1 rejects `local_subject` as an unknown
  field); a service request is delivered to the responder on the
  exported subject with the reply rewritten to a Go-generated
  `_R_.<...>` subject plus a `Nats-Request-Info: {"acc":"B",...}`
  header, and a reply to `_R_.` returns to the importer's original
  inbox; a stream publish on the exported subject is copied to the
  importer's local subject; an import by an account not listed on
  the export makes Go refuse to start ("service import not
  authorized").
- Implementation (same node): `Export`/`Import` types in
  `src/config.rs` (kind service/stream, export subject + account
  list, import source account + remote/local subjects);
  `translate_subject` (wildcard captures carried across patterns)
  and `pattern_covers` (export-coverage check) in `src/subject.rs`;
  `.conf` parsing in `src/conf.rs` (`exports`/`imports`, `to` with
  `local_subject` as an alias). In `src/server.rs`, a local publish
  matching a service import is rewritten into the exporting account
  (the import shadows the local subject) with a synthetic
  `_RIMPORT.<seq>` reply subject registered in
  `Shared::import_replies`; the responder's publish to that subject
  is intercepted before the permission check and delivered to the
  importer's original reply subject. Local publishes in an exporting
  account are additionally copied (subject rewritten) to local
  subscribers of authorized stream imports, via
  `deliver_stream_copy` (local only, marked `import_copy` so it
  never translates again). `deliver()` now returns whether anything
  was delivered, used for the translated no-responders 503 (sent to
  the original reply subject).
- Authorization is enforced on both sides and never bypassed: the
  importing account's publish allow/deny is checked on the local
  subject before translation, the responder's subscribe allow/deny
  on the exported subject at SUB time, and the export must cover
  the import's remote pattern and list the importer — otherwise the
  message is dropped (with a startup warning). Deliberate
  deviations from Go: an unauthorized import does not stop the
  server (Go refuses to start); no `Nats-Request-Info` header is
  injected on translated requests; reply mappings are retained for
  the server lifetime (Go's expire by TTL); imports do not cross
  routes/leafs, and route/leaf-origin messages do not fan out to
  stream importers (remainder of this area).
- Verified: 6 new tests in `tests/e2e_imports.rs` (service
  request/reply through an import, stream copy, denied import
  dropped, import subject outside the export dropped, local
  allow/deny still enforced, `.conf` shape + authorization logic)
  plus 2 unit tests for translation/coverage; cargo test 124
  green; parity GREEN (52 pass + 1 exception); bench no regression
  vs 86edbe8 (pub/sub 422,899/297,869 vs 418,910/226,910; req/reply
  mean 74 µs vs 196 µs; JS 21,735 vs 8,019 — run-to-run noise, all
  at or above baseline).

### Phase 22 — Route/leaf TLS + cert-to-user mapping (gap area 7)
- Go handshake order probed first against nats-server v2.15.1:
  route TLS is implicit TLS (a plaintext connect to a Go cluster
  TLS port gets nothing; a TLS wrap succeeds, then fails on the
  missing client cert when verify is on). Leaf TLS is
  INFO-first-then-upgrade: the hub sends a plaintext INFO with
  `tls_required`/`tls_verify`/`info_on_connect`, a plaintext
  CONNECT after it is closed silently, and the leaf INFO + CONNECT
  travel inside TLS.
- Go `verify_and_map` probed: the certificate is the credential
  (no password needed); Go matches email SANs, then DNS SANs, then
  URI SANs, then the subject DN in RFC 2253 form against the user
  list (a user named `alice` with a CN=alice cert is rejected;
  a user named `CN=alice` authenticates). Rust reuses the
  `verify_and_map` key but accepts a superset of identities —
  email/DNS/URI SANs, then CN, then full DN — so both `alice` and
  `CN=alice` users work; an unmapped cert is rejected even with a
  valid password, and a mapped cert authenticates even with a wrong
  password (mapping is the credential); a mapped user's
  publish/subscribe allow/deny still applies.
- Implementation: `TlsSettings.verify_and_map` (implies verify) in
  `src/config.rs`, plus `cluster_tls`/`leaf_tls`/
  `leaf_remote_tls` and CLI flags `--tlsverify-and-map`,
  `--cluster-tls*`, `--leaf-tls*`; `.conf` parsing in
  `src/conf.rs` (`cluster { tls {} }`, `leafnodes { tls {} }`,
  remote `tls {}`). `src/tls.rs` gains `build_client_config` and a
  hand-rolled DER certificate identity parser (no new dependency:
  SANs by tag, CN + full DN from the subject, BMPString/T61/
  Printable/IA5/UTF8 string forms). `src/server.rs` generalizes
  the route/leaf handlers over plain vs TLS halves: implicit-TLS
  route accept/solicit, leaf accept STARTTLS dance (plaintext
  INFO, peek for 0x16, silent close otherwise, then TLS accept),
  and leaf solicitation that reads the hub INFO on the unsplit
  socket before upgrading (hub advertisement drives the upgrade;
  a missing remote TLS config for a TLS-advertising hub logs and
  retries).
- The hard-won detail: a Go leaf soliciting a hub that sets
  `info_on_connect` arms a 2s timer when it sends CONNECT and
  only cancels it on a hub INFO with `connect_info: true`
  (nats-server v2.15.1 leafNodeResumeConnectProcess /
  leafNodeFinishConnectProcess, confirmed against the v2.15.1
  source after a TLS-intercepting proxy showed Go hubs send a
  second INFO inside TLS). Without it the Go leaf dies at exactly
  2s with `-ERR 'Stale Leaf Node Connection - Closing'` — which
  is what the first version of this phase did. The hub now sends
  a `connect_info: true` INFO when the leaf sends INFO first
  (reply, before CONNECT) and when a TLS hub receives CONNECT
  straight away (after CONNECT parsing); plain hubs, whose INFO
  sets no `info_on_connect`, send nothing extra, matching Go.
  `leaf_info_json` gained a `connect_info` parameter for this.
- Verified: 9 new tests in `tests/e2e_route_leaf_tls.rs`
  (Rust-to-Rust route and leaf over TLS both directions, plain
  route peer rejected, CN mapping without password + allow/deny
  enforced, unmapped CN rejected, DN-form user, SAN-over-CN
  priority, mapping beating a wrong password, `.conf`/CLI shape);
  `scripts/e2e-go-route-leaf-tls.sh` green on all 4 interop
  scenarios (Go seed + Rust solicits route TLS; Rust seed + Go
  solicits route TLS; Go hub + Rust leaf TLS; Rust hub + Go leaf
  TLS — the last one required the `connect_info` fix above);
  cargo test 133 green; parity GREEN (52 pass + 1 exception,
  unchanged); bench no regression vs 86edbe8 (pub/sub median
  413,764/310,341 vs 418,910/226,910 — 256 B within run-to-run
  noise; req/reply mean 81 µs vs 196 µs; JS 22,160 vs 8,019).
- Remainder of this area: none for the required scope. Known
  limitations carried forward: leaf TLS verification uses the
  configured CA only (no `verify_and_map` on leaf/route
  connections — mapping applies to client connections); imports
  still do not cross routes/leafs (Phase 21 remainder).

## Future work (not in this plan)
- Gateways, JetStream clustering/replication, cross-node
  mirroring/sourcing, MQTT/WebSocket gateways, JWT operator mode,
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
