# rust-natsd — Plan

A NATS server implemented in Rust, from scratch. Scope for this plan:
**Core NATS pub-sub protocol** (the client protocol spoken by `nats-server`,
formerly `gnatsd`, in Go), plus routes/clustering, leaf nodes, JetStream
(in-memory or file-persisted, single-node), TLS client, route, and leaf
connections, NKey and JWT-operator authentication, multi-account
scoping with imports/exports, JetStream mirroring/sourcing (same node),
a monitoring endpoint, and `.conf` file support — complete enough that
unmodified Go (`nats.go`) and Rust (`async-nats`) clients can connect,
publish, subscribe, request/reply, cluster, and use JetStream against
it.

Explicitly out of scope (see end, and kept identical in README and
`parity/KNOWN_DIFFS.md`): JetStream clustering/replication
(Raft/WAL), MQTT/WebSocket gateways, cross-node mirroring/sourcing,
cross-node imports/exports, JWT imports across routes/leafs, XKeys,
cert-to-user mapping on route/leaf connections, and Go-only INFO
fields. File persistence is **in** scope (Phases 15, 19) — an earlier
version of this plan listed it out of scope, which was superseded.
Gateways were out of scope until Phase 33 implemented them.

## Status (2026-10-25)

Phases 0–23 **complete**: Core NATS pub-sub, routes/clustering, leaf
nodes, JetStream (in-memory or file-persisted, single-node),
TLS client/route/leaf connections, NKey authentication, JWT operator
mode, multi-account scoping with imports/exports, JetStream
mirroring/sourcing (same node), the monitoring endpoint, and `.conf`
file support, plus the gap areas 1–3 (trie sublist, RwLock sublist,
parser copy audit — Phase 18a) and the slow-consumer fix (Phase 18).
All are implemented and verified against the unmodified official Go
(`nats.go` v1.55.0) and Rust (`async-nats` 0.38) clients, plus
raw-wire protocol tests. **256 Rust tests green, 0 failed**
(measured in the Phase 33 full run); Go interop scripts green in
both directions (routes, leafs, gateways, JetStream, route/leaf/
gateway TLS, imports, mirroring). Parity suite green in default
and strict mode (53 probes, 52 pass + 1 documented KV-gate
exception, 0 fail).

Remaining items: JetStream clustering/replication is a
**documented remainder, partially implemented in Phase 34** —
the Raft core, per-group WAL, and Raft-over-routes transport are
implemented and tested at the group layer, but the JetStream
engine publish/ack/consumer path is NOT wired through them, so
replicated streams are refused rather than served (see Phase
34). The Phase 25 design note stands. Gateways were a remainder
in Phase 24 and are implemented in Phase 33. Still non-goals:
cross-node mirroring/sourcing, cross-node imports/exports, JWT
imports across routes/leafs, MQTT/WebSocket gateways, XKeys,
cert-to-user mapping on route/leaf connections, and the
object-store client helpers (KV helpers are verified since Phase
31 — async-nats end to end, nats.go still version-gated; object
store is not yet exercised against this server).

Note on phase sections below: they are a historical record in
phase order. Where a later phase supersedes an earlier one, the
earlier section says so explicitly (Phases 9, 10, 12, 13, 15);
the Status above and the README feature matrix are authoritative
for current behaviour.

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
  stored in memory. **Superseded by Phase 15:** file persistence is
  implemented there (whole-engine JSON snapshot), and by Phase 19
  (pending ack state included).

### Phase 10 — Monitoring endpoint
- Hand-rolled HTTP/1.1 server on `--http-port`: `/varz`, `/connz`,
  `/subz`, `/routez`, `/leafz`, `/jsz`, `/healthz`. Only tracked fields
  filled in; untracked ones report 0. `/jsz` is 404 without JetStream.
  **Superseded in part by Phase 16:** `mem` is now real (from
  `/proc/self/statm` on Linux), `slow_consumers` is a real counter,
  and `/accounz` / `/gatewayz` were added; untracked fields still
  report 0.
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
  Go). **Superseded by Phase 22:** route/leaf paths now support TLS
  (implicit route TLS, INFO-first leaf TLS).
- Verified: `tests/e2e_tls.rs` (openssl-generated CA/certs; TLS 1.3
  note: a no-cert client `connect()` can succeed while the server's
  certificate-required alert surfaces on first read — the test
  asserts on first read, not on connect).

### Phase 13 — NKey authentication
- `nkey`/`sig`/`jwt` fields in `ConnectInfo`; per-connection `nonce`
  sent in INFO only when authentication is configured (Go omitempty);
  signatures verified with the `nkeys` crate (base64 via the `base64`
  crate). **Superseded by Phase 23:** JWT operator mode is
  implemented there.
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
  pending ack state is NOT persisted **at this phase — superseded by
  Phase 19, which persists it (snapshot v2)**. Workqueue retention, 
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

### Phase 18a — Gap areas 1–3: trie sublist, RwLock, parser copy audit
- **Trie sublist (gap area 1).** `src/sublist.rs`'s linear `Vec`
  scan was replaced by a subject trie (exact-token children, `*`
  child, `>` terminator on the prefix node; partial wildcards like
  `foo*`/`f>o` are literal exact tokens). API and semantics
  preserved exactly: keep-first duplicate `(conn_id, sid)`,
  insertion order via per-key sequence numbers, queue-cursor
  rotation, account scoping, echo filtering, auto-unsub via
  `note_deliveries`. `has_pattern` is O(1) via a pattern-count
  map. Measured with the 10k-subscription harness in
  `examples/sublist_bench.rs` (release): sparse-subject matching
  357.8 µs → 0.394 µs/iter (~900×); all-subjects matching
  387.9 → 173.9 µs/iter (smaller gain — the 2,500-member
  queue-group clone cost dominates there). Phase 2's description
  of the sublist was updated to the trie.
- **RwLock sublist (gap area 2).** `Shared.sublist` is
  `RwLock<Sublist>`: read lock for matching/lookup paths, write
  lock for mutations, combined blocks kept under a single guard
  (no read-to-write upgrades). `propagate_local_sub` collects
  route/leaf handles first and sends after registry locks drop.
  Other `Shared` mutexes were audited and left with reasons
  (short critical sections, Arc collection before sending, no I/O
  under lock). No hand-rolled lock-free structures, no
  connection-model change.
- **Parser copy audit (gap area 3).** The client parser was
  already zero-copy on the PUB/HPUB hot path
  (`BytesMut::split_to` + `freeze`, HPUB headers/payload as
  `Bytes::slice`) and was left untouched. The route/leaf parsers
  did copy every forwarded body via `Bytes::copy_from_slice`;
  they now freeze+slice like the client parser. Remaining copies
  are documented in Design notes (one `feed()` copy, small
  control-line `String`s, per-recipient frame copies).

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

### Phase 23 — JWT operator mode (gap area 8)
- Feasibility first: no new dependency was needed. `nkeys` 0.4.5
  (already depended on) verifies Ed25519 against a public-only
  keypair built from an NKey string, and `base64` + `serde_json`
  cover the rest; a scratch crate also confirmed cargo can fetch
  new crates in this environment, but `jsonwebtoken` does not speak
  `ed25519-nkey` and was not added.
- Implementation: `src/jwt.rs` — `encode_jwt` (tests/tooling) and
  `verify_jwt` (header `typ: JWT` / `alg: ed25519-nkey`, Ed25519
  over `header.payload` against the issuer's NKey, `iss` match,
  `exp` past / `nbf` future / `iat` >60 s future all reject),
  `OperatorTrust::from_jwts` (operator self-signed `O…` key,
  account `iss` must be the operator key or an operator signing
  key, account `sub` an `A…` key, duplicate pubkey/name rejected,
  everything verified at trust-establishment time so a server
  never starts half-trusting), and `verify_user` (user `iss` must
  be a trusted account key or account signing key, CONNECT `nkey`
  must equal the JWT `sub`, and the INFO-nonce signature must
  verify — a `jwt` alone, or with a missing/wrong nonce signature,
  never authenticates). `Config.operator` holds the trust;
  `authenticate` routes a CONNECT carrying `jwt` exclusively
  through the trust (failure rejects outright, never falls through
  to static credentials), while CONNECTs without `jwt` use the
  static accounts unchanged, so standalone nkey auth keeps
  working. `auth_required` includes operator trust, so INFO
  carries a nonce in operator mode with no static users.
- Account JWTs convert into the existing `Account` machinery at
  trust time: `default_permissions` become the account fallback,
  imports/exports become `Import`/`Export` with pubkey references
  translated to account names where trusted (imports from, and
  exports to, untrusted accounts dropped, fail-closed). This
  required `Config::all_accounts` (static + JWT accounts):
  service-import delivery and stream-import fan-out resolve
  accounts through it — without that, JWT accounts' imports were
  invisible to area-6 routing and a service-import test hung.
  A Go-shaped export with no `accounts` field maps to `*`
  (authorizes any importer, matching Go's subject-match-only
  check); a present list restricts to it.
- `.conf`: `operator: "<jwt>"`, `operator { jwt | jwt_file,
  accounts: [...] }`, `resolver { type: memory }` +
  `resolver_preload: { <pubkey>: "<jwt>" }`, and a `preload` map
  inside `resolver` as a Rust convenience. Bad operator/account
  JWTs, preload keys matching no account JWT, and JWT account
  names colliding with static accounts are config errors.
- Go probing (nats-server v2.15.1 + its jwt v2.8.2 source, fetched
  for this phase) settled four details: (1) Go's real preload key
  is top-level `resolver_preload` — a `preload` map inside
  `resolver` is ignored and the server then dies with "error
  resolving system account: account missing"; (2) Go account
  limits keys are `conn`/`subs`/`payload`/`data`/`leaf`/
  `imports`/`exports`, and an absent numeric decodes to 0, which
  Go enforces as a real limit (only -1 is unlimited) — a
  limits-less account JWT fails Go on the first connection with
  "maximum account active connections exceeded" and the first
  subscription with "maximum subscriptions exceeded", and the
  same applies to user-claim limits (`payload`/`subs`/`data`
  under `nats`); (3) claim `name` is top-level in Go (also
  accepted inside `nats` by rust-natsd, which now prefers
  top-level to match); (4) Go rejects `exp` 2 s in the past (no
  expiry leeway) but accepts `iat` 300 s in the future — the
  Rust 60 s `iat` cap is therefore stricter than Go, fail-closed.
- Verified: 3 unit tests in `src/jwt.rs` + 14 tests in
  `tests/e2e_jwt.rs` (valid chain + claims enforcement, account
  default-permission fallback, expired, tampered payload, wrong
  issuer, untrusted account, revoked account (empty trust),
  missing/bad nonce signature, nkey/sub mismatch, account
  signing-key issuance, JWT service import through area-6
  machinery, standalone nkey alongside operator mode, `.conf`
  operator + resolver_preload + jwt_file shapes, bad-JWT config
  error) and 2 Go interop tests (Go 2.15.1 accepts a
  Rust-generated chain and delivers pub/sub; Go rejects an
  expired Rust-generated user JWT). Go tests serialize on a
  tokio mutex because parallel tests re-picked the same freed
  port and cross-connected to the wrong fixture's server.
  cargo test 150 green; parity GREEN (52 pass + 1 exception,
  unchanged, nkey probes still pass); bench no regression vs
  86edbe8 (pub/sub median 398,243/265,119 vs 418,910/226,910 —
  256 B inside this run's own 283k–415k range; req/reply mean
  89 µs vs 196 µs; JS 17,558 vs 8,019).
- Remainder of this area: none for the required scope. Known
  limitations carried forward: account-JWT `limits` are parsed
  by neither enforcement path in rust-natsd (ignored; Go
  enforces them, see above); account-JWT user `revocations`
  lists are not honored (revocation = removing the account JWT
  from trust and restarting); user `bearer_token` is not honored
  (the nonce signature is always required, stricter than Go);
  the system account is an ordinary JWT account (`$SYS`
  subjects are not special-cased, as elsewhere); JWT accounts
  do not appear in route/leaf account advertisement beyond
  their connections (imports do not cross routes/leafs, Phase 21
  remainder).

### Phase 24 — Gateways (super-cluster): probe + design note ONLY (gap area 9)
- **Status: REMAINDER, NOT IMPLEMENTED. No gateway server code was
  written in this phase, and no stub gateway endpoint was added.**
  The existing empty `/gatewayz` response stays exactly what it is
  (an empty list, honestly labelled in the README), and `gateway {}`
  sections in `.conf` are still ignored (see `src/conf.rs`). A
  gateway that accepts connections but cannot interop with Go is
  worse than none (see "why a partial gateway is worse" below),
  so this phase deliberately ships notes, not code.
- **Probe method (empirical, Go nats-server v2.15.1, 2026-10-24/25).**
  Two Go servers, A (client 4222, gateway 7222) and B (client 4223,
  gateway 7223, soliciting A). A raw-TCP intercept proxy sat between
  B's gateway solicitation and A's gateway port and logged every
  byte in both directions; plain raw clients then subscribed on one
  side and published on the other. Config note found the hard way:
  the solicitation key is `gateways: [{ name, urls }]`, not
  `remotes` — Go rejects `remotes` as an unknown field.
- **Handshake, exact order as probed (gateway port, INFO-first —
  unlike routes, where the dialer sends CONNECT first):**
  1. Accepter → dialer: `INFO` (proto 3) with `server_id`,
     `server_name`, `version` "2.15.1", `host`, `port` (the gateway
     port), `headers: true`, `max_payload`, plus gateway-only
     fields `gateway: "<name>"`, `gateway_urls: ["<host>:<port>"]`,
     `gateway_url`, `gateway_nrp: true`, `gateway_iom: true`.
     (`gateway_nrp`/`gateway_iom` are advertised, not behaviourally
     probed here — see below.)
  2. Dialer → accepter: `CONNECT {"echo": false, "verbose": false,
     "pedantic": false, "tls_required": false, "headers": false,
     "name": "<dialer server_id>", "cluster": "", "gateway": "<dialer
     gateway name>"}` followed immediately by the dialer's own
     `INFO` in the same shape as step 1 (its own gateway name and
     `gateway_urls`).
  3. Accepter → dialer: `INFO` frames whose only meaningful fields
     are `gateway`, `gateway_cmd`, and `gateway_cmd_payload` (all
     other fields empty/zero). `gateway_cmd: 2` arrives once per
     account, payload = base64(account name) — observed order
     `$SYS`, `APP`, `$G`, `OTHER` on a server with two named
     accounts, `$SYS`, `$G` on a default-only server. Then
     `gateway_cmd: 3` per account, payload = base64(account name).
     Interpretation (inference, not source-verified): cmd 2
     announces an account to the peer; cmd 3 marks it switched to
     interest-only mode — B's `-DV` log at the same moment says
     `switching account "$SYS" to Interest-Only mode` (and `$G`).
     The cmd numbers and payloads are probed fact; the cmd
     semantics are the plausible reading of the ordering plus that
     log line, and an implementation must confirm them against
     Go's gateway source before relying on them.
  4. Interest, then messages, then `PING`/`PONG` in both directions
     on the same connection. A naive fake peer (INFO first, then a
     CONNECT naming an unconfigured gateway) was answered with
     `-ERR 'Authorization Violation'` and a reset — evidence that
     handshake order and gateway-name recognition are enforced,
     and that the proxy intercept, not the fake, is the
     authoritative record.
- **Interest propagation (probed): gateway interest IS route
  framing.** Accepter → dialer after the handshake, and on every
  later local subscribe/unsubscribe: `RS+ <account> <subject>
  [queue weight]` / `RS- <account> <subject> [queue]` — literally
  the route verbs, account token first. Observed: `RS+ $G
  events.>`, `RS+ $G jobs workers 1` (queue weight 1, as on
  routes), `RS+ APP orders.>`, `RS+ OTHER orders.>`, and on
  disconnect `RS- APP orders.>`, `RS- OTHER orders.>`.
- **Message framing (probed): gateway messages ARE route framing.**
  `RMSG <account> <subject> [reply] [| queues] <size>` (HMSG form
  for headers, by analogy with routes — not separately probed
  here). Observed: `RMSG $G events.click 5`, `RMSG $G jobs |
  workers 3`, `RMSG APP orders.new 4`. The client-facing delivery
  on the far side is an ordinary `MSG` to the subscriber.
- **Account scoping (probed, not assumed).** With accounts `APP`
  and `OTHER` on both servers: an `APP` publish on B was delivered
  only to the `APP` subscriber on A (`RMSG APP orders.new` on the
  wire); the `OTHER` subscriber on A received nothing. Accounts
  are matched by name across gateways, and the account token on
  every RS+/RMSG frame is what enforces the boundary. `$SYS`
  traffic also crosses: an `RMSG $SYS
  $SYS.SERVER.ACCOUNT.APP.CONNS` advisory was observed on the
  gateway connection, so the system account is not filtered out.
  Queue subscriptions cross with their weight (`jobs workers 1`).
- **Implicit gateways (probed, partially).** On learning B from
  B's INFO `gateway_urls`, A solicits back: A's log shows
  `Connecting to implicit gateway "B"`. A supercluster therefore
  forms one connection per direction, not one shared socket. The
  reverse direction (A publish → B subscribe) was NOT verified in
  this probe: in this sandbox A's back-connect went to the
  advertised non-local address (198.19.0.2:7223), which hit a
  middlebox (198.19.0.1:3128) and died with a gateway parser
  error, while the 127.0.0.1 B→A direction carried all verified
  traffic. That is a sandbox networking artifact, not a Go
  behaviour claim, and the reverse-direction test is on the
  interop plan below.
- **What would be reused vs what is genuinely new.** Reuse:
  `src/route.rs` parser/encoder framing (RS+/RS-/RMSG/HMSG are
  shared verbs — a gateway parser is a route parser plus INFO
  `gateway_cmd` handling, not a new wire language); the trie
  sublist and `deliver()`'s global queue selection; the account
  model and per-account interest maps; route solicitation
  retry/backoff; the TLS machinery's patterns (gateway TLS mode
  was NOT probed — implicit vs STARTTLS on the gateway port is an
  open question for implementation, not something to guess).
  Genuinely new: a gateway listener + accept path (there is none:
  no gateway port, no gateway config fields, `gateway {}` in
  `.conf` is ignored); gateway INFO handling (gateway name,
  `gateway_urls` advertisement, `gateway_nrp`/`gateway_iom`
  negotiation, `gateway_cmd` 2/3 state machine per account);
  explicit-vs-implicit solicitation state (configured gateways
  persist; implicit ones are discovered from peer INFO and must
  be deduped against explicit ones); supercluster account mapping
  (same-name matching, what happens to interest for an account
  the peer does not have, interest-only-mode semantics — what
  exactly changes when an account switches to it, beyond the log
  line, is unprobed); loop safety across a supercluster (a message
  must not bounce between gateways — routes in a full mesh do not
  have this problem); real `/gatewayz` (inbound vs outbound,
  configured flag — Go reports the solicitor's side
  `configured: true`, the accepter's `configured: false`).
- **Go-interop test plan (the 4-direction matrix used for
  route/leaf TLS, extended).** Seed direction × traffic direction:
  (1) Go seed + Rust solicits, traffic Go→Rust; (2) Go seed + Rust
  solicits, traffic Rust→Go; (3) Rust seed + Go solicits, traffic
  Go→Rust; (4) Rust seed + Go solicits, traffic Rust→Go — plus an
  implicit-back-connect case (Rust learns Go from INFO and
  solicits back, and vice versa), an account-scoped case (APP
  delivers, OTHER does not, as probed), and a queue-group case
  (one delivery across the gateway). All against Go 2.15.1, raw
  wire first, then nats.go clients. `gateway_nrp` behaviour
  (no-responders across gateways) needs its own probe before it is
  implemented, not after.
- **Scope estimate.** At least a full phase on the scale of Phase 7
  + Phase 8 combined: a new `src/gateway.rs` (parser/encoder/state
  machine, comparable to `route.rs` + `leaf.rs`), gateway
  connection handlers and interest hooks in `src/server.rs`,
  gateway config in `src/config.rs`/`src/conf.rs`, real
  `/gatewayz` in `src/monitor.rs`, `tests/e2e_gateway.rs`,
  `scripts/e2e-go-gateway.sh`, and parity probes. The wire framing
  is the easy part (it is route framing); the state machine
  (implicit discovery, per-account interest-only mode, supercluster
  account mapping, loop safety) is the work.
- **Why a partial gateway is worse than none.** A gateway listener
  that accepts a connection but gets the handshake wrong is not a
  stepping stone: Go answers it with `Authorization Violation` and
  the supercluster silently never forms, while `/gatewayz` would
  claim a gateway exists. Worse, a gateway that forwards without
  the account-token boundary or interest-only semantics leaks
  traffic across accounts — a correctness and isolation
  regression versus advertising no gateway at all. Until all four
  interop directions plus account scoping pass against Go, the
  honest state is: not implemented, design note only.
- **Conclusion: remainder, not implemented.** Probe evidence above
  is evidence about Go's wire behaviour; this note is a plan.
  Neither is an implementation.

### Phase 25 — JetStream clustering / Raft / WAL: design note ONLY (gap area 10)
- **Status: REMAINDER, NOT IMPLEMENTED. No Raft code, no WAL code,
  no multi-node JetStream code was written in this phase.**
  JetStream remains single-node (whole-engine JSON snapshot,
  Phase 19/20). This note scopes the work; it does not start it.
- **Sourcing honesty.** The Go model below is from Go documentation
  (docs.nats.io JetStream clustering deep-dive: Raft groups, meta
  leader and stream leaders, quorum commits, placement, peer
  catch-up/scaling) and secondary descriptions of nats-server's
  `jetstream_cluster.go`/`raft.go`. It was NOT wire-probed in this
  task: no multi-node Go JetStream cluster was stood up, no Raft
  traffic was captured, and no Go Raft wire format is claimed here.
  What IS verified in this repo, and is the contrast point, is the
  single-node snapshot: whole-engine JSON rewritten after every
  mutation and reloaded on startup, including pending ack state
  (snapshot v2, Phase 19).
- **What Go JetStream clustering actually consists of (per the
  documentation above).** A meta group — a Raft group over every
  JetStream server in the cluster — whose leader assigns streams
  and consumers to peers (placement honours cluster/tags when
  configured). Each stream gets its own Raft group over its
  replicas (`num_replicas`, R1/R3/...); each consumer gets its own
  Raft group for its delivery/ack state. Every group elects a
  leader; writes are proposed to the leader, replicated through
  the Raft log, and committed only on a quorum (majority) before
  followers apply them. The Raft log is persisted to a WAL on disk
  (file storage), with snapshots and log compaction bounding log
  growth, and a peer that restarts or falls behind catches up from
  a snapshot plus log replay. Three servers is the recommended
  minimum: a 3-node group survives one loss; a 2-node group has a
  quorum of 2, so losing either node halts it — strictly worse
  than a single node. A single server's R1 streams are standalone
  by definition, not a degraded cluster.
- **Why the single-node JSON snapshot differs fundamentally.** One
  writer, no election, no quorum, no log: the snapshot is a
  last-write-wins whole-state rewrite, and restart reloads exactly
  that state. There is no ordering of concurrent writes from two
  nodes, no commit point a client ack can be tied to, no replay
  for a peer that missed writes, and no protection against two
  nodes both believing they own a stream. Copying a snapshot file
  to a second node does not create a replica — it creates a fork:
  the first publish on either side (new sequence, dedup window,
  consumer ack floor, redelivery count) diverges state that
  nothing will ever reconcile.
- **Tempting shortcuts that must NOT be shipped.** (1) Shared
  snapshot files (NFS/shared disk, or copying snapshots between
  nodes): two writers, stale reads, no write ordering — a fork
  with extra steps. (2) Leader election without log replication
  (a "primary" that others merely follow by polling state): two
  primaries after a partition both accept publishes; the loser
  silently discards acknowledged data on rejoin. (3)
  Primary-backup copying without quorum (async copy, ack on the
  primary before the backup confirms): an acked publish is lost
  when the primary dies before the copy lands — the exact loss
  clustering exists to prevent. (4) Advertising `num_replicas > 1`
  or cluster/leader fields in STREAM.INFO/`$JS.API` responses while
  storing R1: clients and operators would believe data is
  replicated when it is not. Any of these would be worse than the
  current honest single-node state.
- **Component list a correct implementation would need in this
  codebase.** (a) `src/raft.rs`: a Raft state machine — persistent
  term/vote/log, election timers, RequestVote/AppendEntries,
  snapshot install + log compaction, membership changes — with its
  own unit tests (election, log matching, commit advancement,
  snapshot restore) before any JetStream integration. (b) A WAL
  store per Raft group on disk under `--js-store-dir`, replacing
  the whole-engine snapshot as the durability mechanism for
  clustered streams (the snapshot format can survive as the
  snapshot half of Raft, not as the log). (c) `src/js_cluster.rs`:
  the meta group (assignments, placement, peer set management)
  plus per-stream and per-consumer group lifecycle (create on
  assignment, tear down on delete, reassign on replica loss).
  (d) Peer RPC transport: the design must choose and document it
  (over the existing route connections or dedicated peer
  connections). Go-interop for clustered JetStream would additionally
  require matching Go's Raft RPC, which is not a documented wire
  protocol and was not probed — a Rust-only cluster (own Raft
  transport, own wire framing) is the honest first scope, and Go
  interop for clustered JS is a separate reverse-engineering
  effort, not a free consequence of this note. (e) Integration
  with the existing JetStream engine's apply path in
  `src/jetstream.rs`: every mutation (publish, ack, consumer
  create/delete, purge, delete) becomes a Raft proposal applied
  only on commit; reads are served by the group leader (or
  explicitly stale-tolerant followers — that choice must be
  documented, not accidental); the snapshot-v2 pending ack state
  becomes consumer-group replicated state, not a local file. (f)
  Config (`num_replicas`, placement cluster/tags, store dir per
  group) and monitoring (cluster/leader fields in STREAM.INFO and
  `/jsz`, honestly absent until groups exist).
- **Test plan.** 3-node Rust cluster, R3 stream created through
  the meta leader, publish + pull/push consume end to end; kill
  the stream leader (SIGKILL, not graceful stop) and verify
  election, continued service, and no lost or duplicated messages;
  network partition (sever route links between one node and the
  other two) and verify the minority side refuses writes rather
  than forking, and rejoins cleanly; restart catch-up (a node down
  through N publishes rejoins and serves current state, verified
  by MSG.GET across the full sequence range); replica loss +
  reassignment by the meta leader; quorum loss (2 of 3 down)
  verified to refuse writes, not silently serve R1. API behaviour
  compared against a 3-node Go 2.15.1 cluster for leader/error
  reporting, once — and only once — the Rust cluster passes its
  own tests.
- **Conclusion: remainder, not implemented.** The single-node
  snapshot is the shipped durability story; anything calling
  itself clustered must have Raft groups, quorum commit, a WAL,
  and the test plan above, or it must not ship.

### Phase 26 — Test-gap work (audit + missing unit/e2e tests)
- Audit of every module/feature area against existing tests
  (unit, `tests/e2e_*.rs`, Go scripts, parity probes — parity
  probes explicitly not counted as cargo tests), then tests added
  for the genuine gaps, test-only (no `src/` changes). Result:
  **150 → 200 Rust tests passing, 2 ignored.**
- Added: unit gaps (21: subject/protocol edges, config/conf
  parsing, JWT claim branches, TLS cert identity ordering);
  `tests/e2e_wire_gaps.rs` (13: pedantic, verbose order,
  duplicate sid, echo, no-responders, PING/stale, token modes,
  malformed CONNECT, unknown op, second CONNECT, 3-member queue,
  HPUB max-payload, graceful shutdown) with the weak
  `protocol_error_closes_connection` test strengthened to assert
  EOF as well as `-ERR`; `tests/e2e_js_gaps.rs` (6 pass:
  deliver-policy all, filter subject, ack-policy none, push
  ack-wait redelivery, filtered purge, NAMES filter,
  CONSUMER.INFO); monitoring/conf gaps (subz/routez/leafz/
  gatewayz shapes, varz max_pending/ping values, jsz 404,
  connz filters, file-based accounts isolation, file store_dir);
  rust-client gaps (fanout, unsubscribe, request_with_headers,
  publish with no subscribers); new `scripts/e2e-go-advanced.sh`
  + `e2e/go/advanced.go` (service imports and mirror
  backfill/live via nats.go, green; JWT mode unwired — see below).
- **Two bugs found by the new tests, NOT fixed in this phase**
  (both tests committed `#[ignore]`d naming the bug, see README
  Known issues): (1) pull-consumer `max_deliver` is not enforced
  (redeliveries continue past `max_deliver: 2`); (2) JWT auth via
  the official Go client can never succeed, because nats.go's
  UserJWT flow never sends a CONNECT `nkey` field while
  rust-natsd requires `nkey == JWT sub` (fix: derive the user
  key from the JWT `sub`, as Go does). Also noted:
  CONSUMER.INFO `num_pending` is hardcoded to 0.
- Deliberately not tested (features that do not exist, a test
  would fail by design): gateways, JetStream clustering/Raft/WAL,
  MQTT/WebSocket, cross-node mirroring/imports, JWT imports
  across routes/leafs.

### Phase 27 — Documentation refresh (this phase, 2026-10-25)
- Full audit of README.md, PLAN.md, and parity/KNOWN_DIFFS.md
  against the code at HEAD `0cb8650`, docs-only (no src/test/
  script changes). Fixed: PLAN status section (was phases 0–16
  and 97 tests; now phases 0–23 complete, 24–25 design notes,
  200 tests + 2 ignored) and its stale out-of-scope line (file
  persistence is in scope since Phase 15); superseded-phase
  annotations on Phases 9, 10, 12, 13, 15; a new README
  Configuration reference (every CLI flag and `.conf` key,
  verified against `src/config.rs`/`src/conf.rs`); a new README
  Known issues and limitations section (the two ignored-test
  bugs, `num_pending`, deliberate deviations, JWT limitations,
  and the single consistent out-of-scope list, mirrored in
  KNOWN_DIFFS); README benchmark table refreshed from a fresh
  `bash scripts/bench.sh` run at HEAD (2026-10-25, medians in
  README); README monitor design note corrected (memory is real
  from `/proc/self/statm`, not 0); README Tests section gained
  `scripts/e2e-go-route-leaf-tls.sh`; Phase 18a (gap areas 1–3)
  and Phase 26 (test-gap work) recorded here, since neither had
  a phase entry before.
- Flagged, not changed (docs-only task, code change needed):
  the CLI help text in `src/config.rs` describes
  `--js-store-dir` as "(reserved)", but the store dir is in fact
  implemented (whole-engine JSON snapshot, Phases 15/19) — the
  help text is stale, the docs are right.

### Phase 28A — Enforce max_deliver on pull AND push consumers (2026-10-25)
- Renumbering: this work was planned as Phase 27A, but PLAN.md
  already has a Phase 27 (docs refresh), so the bug-fix series is
  recorded as 28A–28C (plan 27A→28A, 27B→28B, 27C→28C). Phase 27
  itself is untouched.
- `src/jetstream.rs`: -NAK handler now enforces the cap for both
  kinds (pull: drop + no re-queue; push: drop + no delivery, where
  the old code already dropped but silently); the ack-wait tick
  sweep already dropped but now also advises and advances the ack
  floor; a defensive guard in `fulfill_pulls` drops redelivery
  queue entries whose queued count is past the cap (e.g. restored
  from an old snapshot), and snapshot restore drops such entries.
  Exhaustion never deletes the message (workqueue included) and
  never increments `num_redelivered` for the terminal attempt.
- Go v2.15.1 probed first: exhaustion publishes
  `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>`
  with `{"type":"io.nats.jetstream.advisory.v1.max_deliver",
  "stream", "consumer", "stream_seq", "deliveries"}` (deliveries =
  the cap, e.g. 2); rust-natsd now publishes that advisory on
  every exhaustion path, exactly once (pending + queue removed at
  the same site). Workqueue probed separately: on both NAK and
  ack-wait exhaustion the message stays in the stream
  (`STREAM.MSG.GET` still succeeds, stream msgs unchanged); only a
  plain +ACK deletes it. One Go timing nuance not replicated:
  Go's NAK path queues the redelivery and only checks the cap on
  the next delivery attempt, so `num_ack_pending` stays 1 between
  the terminal NAK and the next Fetch; rust-natsd drops at the NAK
  (the task's specified behaviour), which is observably stricter
  in that window but identical in deliveries, advisory, and final
  consumer state.
- Tests: `#[ignore]` removed from
  `max_deliver_exhaustion_stops_redelivery`; new
  `push_max_deliver_exhaustion_stops_redelivery_and_advises`,
  `workqueue_max_deliver_exhaustion_keeps_message_and_advises`,
  and `pull_max_deliver_exhaustion_via_ack_wait_timeout` in
  `tests/e2e_js_gaps.rs`. The exhausted pull Fetch yields Go's
  404 "No Messages" sentinel rather than None; the tests accept
  that sentinel and assert the exhausted payload never reappears.
- Docs synced in the same commit: README feature matrix and Known
  issues (max_deliver bullet removed), `parity/KNOWN_DIFFS.md`.

### Phase 28B — Go-client UserJWT login without CONNECT.nkey (2026-10-25)
- `src/jwt.rs` `verify_user`: when CONNECT carries `nkey` it must
  still equal the user JWT `sub` (a mismatched nkey rejects, as
  before); when it carries none, the nonce-signature key is
  derived from `sub` itself. The signature is still required and
  still verified against the INFO nonce, so a JWT alone never
  authenticates — only the key source changed, matching Go's
  server (nats.go UserJWT and Nkey are mutually exclusive, so the
  Go client shape never sends `nkey`).
- Tests: `#[ignore]` removed from
  `jwt_login_without_nkey_field_authenticates` in
  `tests/e2e_jwt.rs`; the JWT mode in `e2e/go/advanced.go` is now
  wired into `scripts/e2e-go-advanced.sh` (it was deliberately
  unwired while the bug was open, and its stale "reproduces a bug"
  comment is updated). No new test file layout was invented:
  the mode already existed in `advanced.go` (there is no
  `e2e/go/main_test.go`).
- Docs synced in the same commit: README feature matrix and Known
  issues (JWT bullet removed; the suite now has no `#[ignore]`d
  tests), `parity/KNOWN_DIFFS.md`.

### Phase 28C — num_pending, import reply TTL, JWT/conf enforcement (2026-10-25)
- Renumbering note (as for 28A/28B): these phases were plan
  Phases 27A–C; PLAN already has a Phase 27 docs refresh, so they
  are recorded as 28A–C and the Phase 27 entry is untouched.
- `src/jetstream.rs`: `ConsumerInfo.num_pending` is now the exact
  count of non-deleted stream messages with sequence past the
  consumer's last delivered stream sequence matching
  `filter_subject` / `filter_subjects` (the latter takes
  precedence, as in Go); `filter_subjects` is parsed at
  consumer-create, stored in snapshot v2, round-tripped in
  CONSUMER.INFO config, and used for delivery matching in
  `drain_push`, `fulfill_pulls`, and MSG.NEXT (which also gained
  the 28A max-deliver guard). `Engine::process` takes a
  per-account JetStream-enabled flag; when false it returns Go's
  503/10039 "JetStream not enabled for account" with the
  op-specific type marker (INFO maps to
  `account_info_response`, matching Go's probed shape).
- `src/conf.rs` / `src/config.rs`: account `jetstream` now
  accepts `true`, `enabled`, and a `jetstream { ... }` limits
  block (presence enables, unless it says `enabled: false` /
  `disabled`); `Config::jetstream_enabled_for_account` gates
  only when static multi-account configuration is present —
  single-account and JWT-operator servers return true (JWT
  accounts carry no `.conf` flag; Go gates those via account-claim
  JetStream limits, which rust-natsd does not yet parse).
- `src/server.rs`: import-reply mappings are now
  `(account, orig_reply, Instant)` and expire after 60 s, pruned
  opportunistically on insert and lookup (no background sweep;
  the TTL rule is unit-tested with an explicit `now` so no test
  waits 60 s). Translated service-import requests carry
  `Nats-Request-Info: {"acc": "<importing account>"}` — Go's
  probed shape minus `rtt`, which Go omits when 0 and rust-natsd
  never measures; a forged pre-existing header is replaced.
  JWT account limits are enforced for JWT accounts only (static
  accounts have no JWT limits): `conn` account-wide at CONNECT
  (Go's "maximum account active connections exceeded", close),
  `subs` per-connection at SUB (Go's "maximum subscriptions
  exceeded", connection stays alive, duplicate sids exempt),
  `payload` per-message via the connection parser's ceiling
  (synced after CONNECT so a PUB in the same TCP chunk is
  checked) plus a belt-and-braces check in PUB/HPUB (Go's
  "Maximum Payload Violation", close), and `data` as an
  account-wide cumulative inbound payload budget ("Maximum Data
  Exceeded", message dropped, connection stays alive). The data
  interpretation is rust-natsd's own: Go's server does not
  visibly enforce `NatsLimits.Data` on the wire, and this is
  stated as such in README and the code.
- `src/jwt.rs`: account `limits` and `revocations` are parsed
  from the account JWT (`AccountLimits`, `revocations` map);
  `verify_user` rejects a user JWT issued at or before its (or
  `"*"`) revocation timestamp (missing `iat` counts as 0, as in
  Go), skips the nonce-signature check for `bearer_token: true`
  unless the account sets `limits.disallow_bearer` (then it
  rejects), and keeps the nonce check otherwise. Limits
  semantics: a field absent inside a present `limits` object
  decodes to 0 and is enforced as a real limit (Go's rule, only
  -1 is unlimited); an account JWT with no `limits` object at
  all is treated as unlimited — a deliberate deviation from Go
  (which would reject its first connection) that keeps
  hand-written limits-less tokens, as used by several existing
  tests, working.
- Tests (existing files only): `tests/e2e_jetstream.rs` gains
  `consumer_info_num_pending_before_and_after_consumption`
  (3 → 0 across fetch+ack, plus a `filter_subjects` consumer
  counting 2 of 3) and
  `per_account_jetstream_gate_blocks_disabled_account` (503/10039
  for the disabled account, success for the enabled one, INFO
  type `account_info_response`); `tests/e2e_imports.rs` gains an
  HMSG reader and asserts `Nats-Request-Info` for B on the
  translated request; `tests/e2e_jwt.rs` gains revocation
  (per-user future/past and `"*"`), bearer (no-sig success,
  `disallow_bearer` rejection), and conn/subs/payload/data limit
  tests; the JWT service-import test there also moved to HMSG
  with a Request-Info assertion for ACCT2. The server unit tests
  cover the TTL prune rule and Request-Info injection/replacement.
  `scripts/e2e-go-advanced.sh` gains `jetstream: enabled` on
  account A (its mirror mode needs JetStream there) now the gate
  is enforced. Note for future tests: async-nats 0.38 ignores URL
  userinfo (it sends `user`/`pass` null), so authenticated
  async-nats clients must use `ConnectOptions::user_and_password`.
- Docs synced in the same commit: README feature matrix, Known
  issues (num_pending, import-TTL, Request-Info, JWT limitation
  bullets rewritten), the `.conf` `jetstream` line, and the stale
  Tests-section counts; `parity/KNOWN_DIFFS.md` (same items
  rewritten); this PLAN entry. The suite still has no
  `#[ignore]`d tests.

### Phase 29 — Modularization: split `server.rs` and `jetstream.rs` (2026-10-25)
- Renumbering: this work was planned as Phase 28, but PLAN.md
  already has a Phase 27 (docs refresh) and the bug-fix series is
  recorded as 28A–C, so the modularization is recorded as Phase 29
  (plan 28 → 29). The Phase 27 and 28A–C entries are untouched.
- Pure refactor, no behaviour change: code moved verbatim except
  visibility bumps (private → `pub(crate)`) for items now used
  across sibling modules, and the two `deliver()` import blocks
  below, which became functions. No test file was modified; the
  test count is unchanged (215 passed / 0 failed / 0 ignored).
  No `parity/KNOWN_DIFFS.md` change: no behavioural difference
  closed (any closure would have been a bug in the refactor).
- `src/server.rs` (3,305 lines) → `src/server/`:
  `mod.rs` (1,155: `Shared`, `RunningServer`, `start`, supervisor,
  listener loops, route/leaf handshakes + solicitation),
  `conn.rs` (1,031: `ConnHandle`/writer/slow-consumer byte queue,
  `ConnState`, `handle_conn`, `process_op`, client/route/leaf info
  helpers), `delivery.rs` (991: `Message`/`Origin`, `deliver`,
  fan-out, global queue-group selection, interest propagation,
  inbound route/leaf frame processing), `imports.rs` (275:
  service/stream import translation, `_RIMPORT` TTL table with
  the 60 s prune rule from Phase 28C). `Message`/`Origin` live in
  `delivery.rs` and are re-exported from `server/mod.rs`, so
  `crate::server::{deliver, Message, Origin, Shared}` and the
  integration-test imports (`server::{self, RunningServer}`) are
  unchanged. Route/leaf read loops and `process_route_op` /
  `process_leaf_op` live in `delivery.rs` (they feed `deliver`
  and interest propagation directly); route/leaf handshakes stay
  in `mod.rs` (listener/lifecycle). `ConnHandle.headers` /
  `ConnHandle.close` became `pub(crate)` (read from
  `delivery.rs`); `ConnState` became `pub(crate)` (named in
  `process_op`'s now-`pub(crate)` signature). Server unit tests
  moved with their code: `server_id_shape` stays in
  `server/mod.rs`, the import-TTL prune and Request-Info tests
  moved to `server/imports.rs`.
- `deliver()` import extraction (the only non-verbatim moves in
  the server split, both in `imports.rs`, both behaviour-identical):
  the service-import block became `try_service_import`
  (guard → early `None` return; `return delivered` /
  `return false` → `Some(..)`), and the stream-import block
  became `deliver_stream_imports` (`has_delivery = true` →
  local `delivered_any` flag). `deliver()` calls them at the
  same points, in the same order. The `_RIMPORT` reply
  interception in `process_op` still calls `lookup_import_reply`.
- `src/jetstream.rs` (3,448 lines) → `src/jetstream/`:
  `mod.rs` (740: `JetStream` facade + lock coordinator,
  `Engine`/`Stream`/`Consumer`/… types, helpers, `Stream` +
  `Consumer` method impls), `api.rs` (736: `handle_message`,
  `Engine::process` dispatch, `process_api` + stream API
  handlers, API helpers, error codes), `stream.rs` (590:
  `stream_for`, `store_message`, `fan_out`,
  `copy_to_dependents`, `backfill`, `would_cycle`, `capture`,
  `process_ack`), `consumer.rs` (947: `deliver_to_consumer`,
  `drain_push`, `fulfill_pulls`, consumer API handlers, `tick`,
  `redelivery_loop`), `storage.rs` (536: `JetStream::persist` /
  `load_snapshot`, `Engine::snapshot_json` /
  `from_snapshot_json`). Snapshot format (v2, incl. pending ack
  state) and the WAL design are untouched — the WAL is still a
  later phase. `JetStream::load_snapshot` became `pub(crate)`
  (called from `mod.rs`'s `with_store_dir`); all `Engine` /
  `Stream` / `Consumer` fields and methods used across modules
  became `pub(crate)`; `DEDUP_WINDOW`, `DEFAULT_ACK_WAIT`,
  `TICK` became `pub(crate)` consts. Public exports unchanged
  (`JetStream`, `handle_message`, `redelivery_loop`,
  `rfc3339` — the last two re-exported from `jetstream/mod.rs`).
- Invariants verified by reading the moved code, not assumed:
  the sublist `RwLock` is never held across socket/channel ops
  (`deliver` computes matches / notes deliveries under the lock
  and sends after it drops; `send_local_frames` snapshots
  targets under the `conns` Mutex and only takes the sublist
  write lock for `remove_conn` on the slow-consumer path;
  interest propagation collects handles first, sends after the
  locks drop); JetStream deliveries still go through the
  actions pattern (`handle_message` and `redelivery_loop` lock,
  collect actions, release, persist, then dispatch `deliver` —
  never delivering while holding `engine.lock()`).
- Gate: no `src/` file exceeds 1,200 lines after the split
  (largest is `src/server/mod.rs` at 1,155; next
  `src/config.rs`, untouched, at 1,100).
- Verification (full suite, same commit): `cargo fmt --check`
  clean; `cargo clippy --all-targets -- -D warnings` clean;
  `cargo test` 215 passed / 0 failed / 0 ignored (same as the
  pre-refactor baseline); `bash scripts/e2e-go.sh` OK (all Go
  e2e checks passed, plain + auth); `bash scripts/parity.sh`
  GREEN — raw probes 52 pass / 1 exception / 0 fail of 53
  (the exception is the documented nats.go KV client version
  gate), JS full PASS, JS persistence-restart PASS, cluster
  smoke PASS, leaf smoke PASS. Docs synced in the same commit:
  README Design section file layout; this PLAN entry.
  `parity/KNOWN_DIFFS.md` intentionally untouched.

### Phase 30 — JetStream WAL: replace the whole-engine JSON snapshot write path (2026-10-25)
- Renumbering: this work was planned as Phase 29, but PLAN.md
  already has a Phase 29 (modularization), so the WAL is recorded
  as Phase 30 (plan 29 → 30). The Phase 27, 28A–C, and 29 entries
  are untouched. One commit, local only (not pushed).
- Storage abstraction (`src/jetstream/storage.rs`, `JsStore`
  trait): `save_stream_meta` / `delete_stream` / `append_msg` /
  `delete_msg` / `save_consumer_state` / `delete_consumer` /
  `needs_compaction` / `compact_msgs`. `FileWalStore` is the only
  implementation. Trait signatures were adapted to the actual
  post-modularization types (`Stream`, `StoredMsg`, `Consumer`
  as they exist in `jetstream/mod.rs`); there is no separate
  `StreamConfig`/`ConsumerSnapshot` type in this codebase, so
  the plan's nominal signatures are realised as: stream meta
  from `&Stream`, consumer state from `&Consumer` (serialised
  with the snapshot-v2 consumer fragment, shared with the
  legacy snapshot code via `consumer_snapshot_json`).
- Layout: `<store_dir>/<account>/streams/<stream>/meta.json`
  (stream config/metadata, base sequence, dedup ids),
  `msgs.wal`, `consumers/<consumer>.wal`. Names are sanitised
  for path components (`[^A-Za-z0-9._-]` → `_`); `meta.json`
  carries the authoritative names. A sanitised collision would
  merge two names — documented, not silently worked around.
- Frame format, exactly as planned: `[magic u32][crc32 u32]
  [record_type u8][seq u64][timestamp i64][subject_len u16]
  [headers_len u32][payload_len u32][bytes...]` with `bytes` =
  `subject ++ headers ++ payload`; AppendMsg `0x01`,
  TombstoneSeq `0x02` (msgs.wal), ConsumerState `0x03`
  (consumers/*.wal, payload is the consumer snapshot JSON).
  CRC32 is IEEE, hand-rolled (no new dependency); it covers
  everything after the CRC field. Appends are O(1) single
  writes with cached file handles and no fsync (a graceful
  restart sees every completed write; only a machine crash
  could lose the OS-buffered tail).
- Write path is a diff, not a rewrite: `JetStream::persist`
  (still called after every `handle_message` and every
  redelivery tick, still never dispatching deliveries while
  holding `engine.lock()` — the actions pattern is unchanged)
  compares the live engine against a shadow of the last
  persisted state and appends only the delta: new tail
  messages, tombstones for newly deleted/evicted sequences,
  a fresh `meta.json` only when config, base sequence,
  mirror/source liveness, or dedup ids changed, and one
  consumer record per changed consumer. Dedup ids live in
  `meta.json` (adaptation: no third record type; meta is
  rewritten only when the dedup map changes, and the dedup
  window still restarts on reload, as in snapshot v2). The
  one in-place renumbering operation — filtered purge — sets
  `Stream::wal_rewrite`, forcing a `msgs.wal` rewrite; a
  wholesale stream replace (new `created_ns`) and a base
  sequence moving backwards take the same defensive path.
- CRC/corruption behaviour (fail-closed, chosen option):
  replay stops at the first record failing magic, length, or
  CRC, and the corrupt/truncated tail is truncated on load
  (logged) — corrupt bytes never enter the engine. A corrupt
  `meta.json` skips that one stream (logged), matching the
  legacy snapshot's start-without-unreadable-data behaviour
  rather than refusing the whole server. Tested: flipping
  the last payload byte of `msgs.wal` drops exactly that
  message on restart, keeps the rest, and shrinks the file.
- Startup migration: if `rust-natsd-js.json` exists and no
  per-account WAL layout exists yet, the legacy snapshot
  (v2, including pending ack state) is loaded once and
  written out through the WAL. The legacy file is left in
  place; a second start sees the WAL layout and never
  re-migrates. Re-migration would still be idempotent
  (replay keys messages by sequence; consumer state is
  last-record-wins), but the layout check makes it
  unnecessary. Tested end to end: legacy snapshot → start
  (migrated, WAL files present) → publish → restart →
  restart again, message set identical each time.
- Compaction: when a stream's tombstoned/evicted bytes exceed
  50% of its `msgs.wal` size AND the file is larger than
  1 MiB, the next persist rewrites `msgs.wal` with only the
  live messages. Tested against the store directly with a
  small threshold override (a store-level test setter, not
  a production config knob): 10 appends, 8 tombstoned,
  trigger fires, rewrite leaves exactly the 2 live messages.
  Consumer logs checkpoint after 128 appended state
  records (rewrite to just the latest record).
- Persisted fields: nothing dropped. Streams, messages,
  headers, timestamps, base sequence, dedup ids, consumer
  definitions, positions, pending ack state, ack floors,
  redelivery counts, and pull redelivery queues all reload
  identically to snapshot v2, because WAL load assembles a
  snapshot-v2 document (meta + replayed messages + last
  consumer records) and feeds the unchanged
  `from_snapshot_json`, including its deadline recomputation.
  Waiting pulls are still not persisted (dead reply inbox).
  The old whole-engine snapshot path is no longer the write
  path: `snapshot_json` survives only as the migration-source
  format reference (round-trip unit test) and `load_snapshot`
  only as the migration loader.
- `tests/e2e_js_persistence.rs` passes unchanged in behaviour
  (no test in it asserts on the legacy file layout, so no
  update was needed). New `tests/e2e_js_wal.rs`: legacy
  migration + double-restart idempotency, corrupt-tail
  truncation, filtered-purge WAL rewrite, and the 10,000-
  message stress test.
- Stress test, honest numbers (payload `msg-%05d` +
  `-0123456789abcdef`, 26 bytes): direct WAL appends of
  10,000 records take ~53 ms in a debug build — the <1 s
  plan gate, asserted in the store unit test, passes with
  20x headroom. The end-to-end sequential async-nats
  publish+ack loop over the same 10,000 messages takes
  ~6.5–7.6 s in debug (run-to-run VM noise), and that is
  NOT a WAL cost: the in-memory (no persistence at all)
  baseline for the same loop is ~225 us per publish
  (~2.25 s per 10,000), fire-and-forget WAL sends cost
  ~212 us vs ~71 us in-memory (~140 us WAL overhead per
  publish, of which the diff-sync itself is ~41 us), and
  pipelined WAL publishes take ~2.9 s per 10,000. So the
  end-to-end loop cannot meet <1 s in a debug build for any
  persistence implementation; the integration stress test
  therefore keeps the full 10,000 messages, real payloads,
  and real WAL with reload verification, prints its wall
  time, and does not assert <1 s on the client loop (the
  gate assertion lives in the store unit test, where
  persistence is what is measured). No test was tuned to
  pass: no fewer messages, no smaller payloads, no
  in-memory shortcut.
- Note: `src/jetstream/storage.rs` is now ~1,800 lines,
  over the Phase 29 "no file exceeds 1,200 lines" gate for
  the split; that gate was for the refactor, not this
  phase, but a future storage split (frames vs. store vs.
  sync) is the obvious follow-up if the file keeps growing.
- Verification (same commit): `cargo fmt --check` clean;
  `cargo clippy --all-targets -- -D warnings` clean;
  `cargo test` 224 passed / 0 failed / 0 ignored (baseline
  215 + 5 new lib unit tests + 4 new WAL integration tests);
  `tests/e2e_js_persistence.rs` 8 passed (unchanged);
  `bash scripts/e2e-go.sh` OK (all Go e2e checks passed,
  plain + auth); `bash scripts/parity.sh` GREEN — raw probes
  52 pass / 1 exception / 0 fail of 53 (the exception is the
  documented nats.go KV client version gate), JS full PASS,
  JS persistence-restart PASS, cluster smoke PASS, leaf smoke
  PASS. Docs synced in
  the same commit: README JetStream persistence section
  (WAL layout, migration, compaction) and this PLAN entry.
  `parity/KNOWN_DIFFS.md` intentionally untouched: no
  behavioural difference closed (persistence behaviour is
  identical; only the on-disk mechanism changed).

### Phase 31 — KV/Object Store interop: DIRECT.GET, stream flags, STREAM.UPDATE, STREAM.MSG.DELETE (2026-10-25)
- Renumbering: this work was planned as Phase 30, but PLAN.md
  already has a Phase 30 (WAL), so it is recorded as Phase 31
  (plan 30 → 31). The Phase 27, 28A–C, 29, and 30 entries are
  untouched. One commit, local only (not pushed).
- GATE EVIDENCE FIRST (before any feature code). Live probes
  against Go v2.15.1 and rust-natsd at the Phase 30 HEAD, plus
  the nats.go v1.55.0 source in the module cache:
  - nats.go KV gate is ONLY the server-version check, not
    api_lvl: `kv.go:399` (`KeyValue`) and `kv.go:424`
    (`CreateKeyValue`) return `nats: key-value requires at
    least server version 2.6.2` when `serverMinVersion(2,6,2)`
    (`nats.go:2811`, parsed from the INFO `version` field) is
    false. The parity go-client `kv` mode confirms it live:
    Go passes (`{"get_value":"v1",...}`), Rust fails with
    exactly that error before any KV traffic (INFO versions
    2.15.1 vs 0.1.0). The plan's api_lvl theory is wrong for
    this gate: in nats.go v1.55.0 `api_lvl` appears only as
    the INFO field (`nats.go:1102`) and the
    `ConnectedServerJetStream()` accessor (`nats.go:2864`) —
    no KV or JS path gates on it. async-nats 0.38 has no
    version gate and no api_lvl use at all: its KV getter
    chooses the direct path purely from the stream info's
    `allow_direct` flag (`kv/mod.rs` entry path).
  - api_lvl / version decision (HONESTY GATE, unchanged):
    rust-natsd still reports its own crate version 0.1.0 and
    still omits `api_lvl`. Faking a Go version to clear the
    2.6.2 gate, or advertising Go's api_lvl 5, would imply JS
    features Rust does not implement (clustering, full
    consumer/producer surface) and is explicitly not done.
    Consequence, stated plainly: nats.go KV helpers cannot
    reach green against this server until its own version
    reaches 2.6.2, so parity stays 52 pass + 1 documented
    exception (the KV gate), 0 fail — NOT 53/53, and the
    exception stays in `parity/KNOWN_DIFFS.md`. What Phase 31
    does earn: async-nats KV helpers verified end to end
    (below), and every server primitive nats.go KV would use
    probed identical to Go (below).
- DIRECT.GET (`src/jetstream/api.rs`, `api_direct_get`):
  `$JS.API.DIRECT.GET.<stream>` with a JSON body (`seq`,
  `next_by_subj`, `last_by_subj`) and
  `$JS.API.DIRECT.GET.<stream>.<last_by_subj subject...>`
  with an empty body (subject tokens rejoined with '.', so
  dotted KV subjects work) — both shapes verified against
  what nats.go (`jsm.go` `getMsg`) and async-nats
  (`stream.rs` `direct_get*`) actually request. Success is
  the raw payload with `Nats-Stream`, `Nats-Sequence`,
  `Nats-Subject`, `Nats-Time-Stamp` (RFC3339) headers plus
  the stored message's own headers passthrough. Errors are
  HMSG status lines, because both clients parse status from
  the version line: not-found `NATS/1.0 404 Message Not
  Found`; empty request (no seq/next/last) `NATS/1.0 408
  Empty Request`; not servable (stream missing, or
  allow_direct off and not a mirror_direct mirror)
  `NATS/1.0 503` with no payload — Go answers that way too
  (no direct-get responder), so nats.go reports "no
  responders" and async-nats NoResponders on both servers.
  A Go probe program run against both servers after the
  change produces identical output for every direct-get,
  deny, rollup, and update case (modulo JSON key order and
  the mirror-name case below).
- Stream flags (`Stream` in `src/jetstream/mod.rs`,
  STREAM.CREATE/UPDATE, `info_json`, WAL `meta.json` +
  snapshot load in `storage.rs`; pre-Phase-31 snapshots load
  the flags as false): `allow_direct`, `mirror_direct`,
  `deny_delete`, `deny_purge`, `allow_rollup_hdrs`.
  STREAM.UPDATE is new (it did not exist): it replaces
  subjects, retention (limits/workqueue), max_msgs,
  max_bytes, max_age, max_msgs_per_subject, and the five
  flags on an existing stream, preserving messages,
  consumers, creation time, and mirror/source links.
  Storage changes are ignored (not rejected — clients send
  a storage value on update) and a mirror/sources config
  naming a different origin is rejected (relinking a live
  stream is not implemented); both limits are documented in
  the handler.
- Enforcement, all shapes probed against Go v2.15.1 first:
  STREAM.MSG.DELETE is new (it did not exist): success
  marks the sequence deleted in place (first_seq jumps past
  a deleted prefix, as Go does); errors are missing stream
  404/10059, deny_delete 500/10057 "message delete not
  permitted", seq past the end 500/10057 "stream store
  EOF", deleted/pre-base seq 500/10057 "no message
  found"; `no_erase` is accepted (the message is
  unretrievable either way). STREAM.PURGE on a deny_purge
  stream returns Go's shape exactly: 500/10110 "stream
  purge not permitted" plus `"purged": 0` in the same body.
  Rollup in `capture` (`src/jetstream/stream.rs`): a publish
  with `Nats-Rollup: sub` deletes all prior live messages
  for that subject, `all` deletes the stream's live
  messages, then the new message stores at the next
  sequence (no renumbering — earlier sequences become
  gaps); without allow_rollup_hdrs, or with any other
  value, the publish fails with 500/10111 ("rollup not
  permitted" / `rollup value invalid: "<value>"`). Rollup
  runs only for actually-stored direct publishes (after
  dedup and expected-sequence checks), never for
  mirror/source copies. KV delete (DEL marker publish)
  and KV purge (rollup-sub PURGE marker publish) are
  therefore NOT blocked by a KV bucket's deny_delete —
  deny_delete gates only the MSG.DELETE API, as on Go.
  MSG.GET `last_by_subj` now shares the direct-get
  `last_for_subject` helper, so a MSG.DELETE'd message is
  not resurfaced by either endpoint (Go returns not-found
  for it, probed).
- Mirror-direct, Go quirk documented, not copied: on Go,
  a direct get addressed to an origin with allow_direct is
  answered by one of its mirrors (the `Nats-Stream` header
  names the mirror — nats.go explicitly tolerates this),
  whether or not the mirror has mirror_direct, and a
  direct get addressed to a mirror's own name always gets
  no responders (even with mirror_direct, even when the
  origin lacks allow_direct). Rust answers the requested
  stream directly when it is servable, including a
  mirror_direct mirror's own copies: the client-visible
  outcome (message + valid headers) is identical, and the
  mirror-name case is a permissive superset, not a gap.
  (Caught while probing: Go `-js` without `-sd` persists
  JetStream state in the working directory, so early mirror
  probes that reused a cwd shared state across server
  processes; the mirror conclusions above come from
  clean-store reruns only.)
- Drive-by fixes the KV tests exposed (all Go-matching,
  all probed or standard NATS semantics, none pre-existing
  test depended on the old behaviour):
  - Ephemeral consumers: CONSUMER.CREATE with no name
    anywhere (subject, config `name`, `durable_name`) now
    gets a server-assigned `eph-<hex>` name instead of
    400/10002. async-nats KV watch and ordered consumers
    create exactly this way; Go assigns ephemeral names.
  - Ack-none deliveries now carry the `$JS.ACK` reply
    subject (Go v2.15.1, probed: an ack-none push delivery
    still carries `$JS.ACK.<stream>.<consumer>...` as its
    reply). Pending tracking is still skipped for ack
    none, so no redelivery ever happens (the existing
    ack-none test still passes). async-nats KV watch
    parses every delivered message's reply and failed
    without it.
  - Consumer delivery subjects (the Watch blocker):
    push/pull/NAK/tick deliveries used to frame the
    message with the deliver subject / pull reply inbox
    as its subject. They now frame the original stream
    message subject and route on the inbox via a new
    `Message::route_via` field (`src/server/delivery.rs`):
    `deliver()` matches the sublist on `route_via` but
    frames with `subject`, local-only (no import
    translation, no route/leaf forwarding). nats.go and
    async-nats push/pull subscribers now see the real
    subject, as on Go; KV Watch derives the key from it.
- Tests (`tests/e2e_js_kv.rs`, 5 tests, all green):
  async-nats `create_key_value` put/get/delete/purge
  (config echo, DEL/PURGE operations, post-purge stream
  state); revisions + watch (direct and MSG.GET paths,
  watch entry for a later put); DIRECT.GET forms and
  errors (seq, last, first, next-wildcard, not-found
  kinds, allow_direct-off 503, STREAM.UPDATE flipping
  allow_direct on); deny_delete/deny_purge/MSG.DELETE
  (codes, gap semantics, EOF vs no-message-found);
  rollup sub/all/denied/bogus. Full verification:
  `cargo fmt --check` clean, `cargo clippy --all-targets
  -- -D warnings` clean, `cargo test` 229 passed / 0
  failed / 0 ignored, `scripts/e2e-go.sh` OK,
  `scripts/parity.sh` GREEN (52 pass + 1 KV-gate
  exception + 0 fail; the KV exception is unchanged and
  honest). Docs synced in the same commit: README KV
  matrix row + parity paragraph + deviations bullet, this
  PLAN entry, `parity/KNOWN_DIFFS.md` KV section
  (exception kept; async-nats verification and the
  mirror-direct difference added).

### Phase 32 — Cross-node imports/exports + cross-node JetStream mirroring (renumbered from plan Phase 31)

- Task renumbering: the plan's Phase 31 (this work) became Phase 32
  because the KV/Object Store phase landed as Phase 31 first; the
  existing PLAN.md entries for Phases 27–31 were not overwritten,
  this entry is appended. `_RIMPORT` form verified in code before
  building on it: the implementation uses `_RIMPORT.<seq>` (not the
  plan's `_RIMPORT.<node_id>.<seq>`), so Phase 32 keeps the prefix
  and adds the node id, minting `_RIMPORT.<originating server
  id>.<seq>`. Same-node replies are unchanged in behaviour (the
  existing `tests/e2e_imports.rs` tests pass unmodified, and the new
  form still matches their `starts_with("_RIMPORT.")` assertions);
  the node id exists because per-node sequence counters can
  collide across nodes, and it lets a responder's node tell a
  remote reply from a local one.
- Cross-node account imports/exports (`src/server/imports.rs`,
  `src/server/delivery.rs`, `src/server/conn.rs`): the importing
  node advertises stream-import interest at connection
  establishment — RS+ in the exporting account on routes, LS+ on a
  leaf whose connection account is the exporting account — so the
  exporter node forwards those publishes and the importer copies
  them to the local subject on arrival with the same
  `deliver_stream_imports` table and the same authorization checks
  (export must cover the concrete subject and list the importer).
  Service imports use the same `try_service_import` on arrival
  (Local, Route, and Leaf origins now, with the translated message
  delivered locally only on an arrival node, so it cannot loop):
  the importing node translates locally and forwards the rewritten
  request via the responder's own advertised interest, and a
  responder's reply to a remote synthetic subject is forwarded to
  every route/leaf (bypassing interest and, like the same-node
  interception, the responder's publish allow list — the synthetic
  channel is import machinery) and claimed on the minting node by
  its reply-mapping lookup, which delivers to the importer's
  original reply subject; any other node drops it. Authorization is
  enforced on both sides exactly as same-node: the importer's
  publish allow/deny on the local subject (checked in `conn` before
  translation), the responder's subscribe allow/deny on the exported
  subject at SUB time, and the export coverage/listing check at
  translation and copy time — no cross-node path bypasses a check
  the local path performs. Denied cross-node imports are dropped
  on the importing node and never forwarded.
- `$JS.API` routing fixed first, as the task required verifying it:
  it did NOT actually route before this phase — `handle_message`
  consumed every API request locally, so the advertised `$JS.API.>`
  interest never carried a request to the hosting node. Now
  (`src/jetstream/api.rs::should_forward_api`) a stream-referencing
  `$JS.API` request or `$JS.ACK` from a local client, for a stream
  absent from this node's engine, is not consumed when the node has
  route/leaf peers, so `deliver` forwards it via the peers'
  `$JS.API.>` / `$JS.ACK.>` interest (advertised per account at
  connection establishment); Route/Leaf/Internal origins are always
  answered locally, which is the loop guard. `STREAM.CREATE` is
  never forwarded (creation is always local) and `STREAM.LIST` /
  `NAMES` stay per-node. Verified by a test: `STREAM.INFO` for a
  stream hosted on node 1, issued from node 2, returns its message
  count instead of a local 404.
- Cross-node stream mirroring/sourcing (`src/jetstream/mirror.rs`,
  `src/jetstream/stream.rs::append_remote_copy`,
  `src/jetstream/api.rs::api_stream_create`): a mirror/source whose
  origin is not on this node is created in remote mode (flagged
  `SourceCfg.remote`, persisted in stream meta) when the node has
  peers, and served by a background pull loop — one durable pull
  consumer per link on the origin stream, created via forwarded
  `$JS.API.CONSUMER.CREATE` with `ack_policy: none`,
  `deliver_policy` replaying the link's backfill range (`all` /
  `by_start_sequence` / `by_start_time`, the last newly supported
  by the consumer engine for this), and the link's
  `filter_subject`, then repeated `MSG.NEXT` (`no_wait`, batch 32).
  The loop talks to its own node over a loopback raw client
  (authenticating with the first password user of the mirror
  stream's account when the config has one), subscribes to the
  origin stream's subjects (or the link's filter) before pulling —
  pull deliveries are framed with the original subject by the
  `route_via` path, which Phase 32 also extended to forward to
  routes/leafs with interest in the original subject — and stores
  each message via `append_remote_copy`: mirror copies exact,
  source copies add the `Nats-Stream-Source` header (trailing token
  is the mirror's own token, since the origin's is not visible
  across the cluster), filter honoured by the consumer, timestamps
  are mirror-arrival time (a pull delivery carries no timestamp,
  unlike the same-node capture hook). The same-node capture hook
  never feeds remote links, and remote links are never backfilled
  locally. Progress is the origin consumer's `next_stream_seq`
  plus a persisted per-link `remote_progress` watermark, so a
  mirror-node restart reuses the existing consumer (create returns
  10013) and resumes without duplicates; an origin restart with a
  store dir (consumer persisted) resumes the same way, and a
  MSG.NEXT error for a lost consumer drops back to consumer
  creation. Remaining honest limits: a mirror-node crash between
  receiving a pull message and storing it loses that one message
  (ack-none already advanced the origin consumer past it); an
  origin recreated after deletion starts a new sequence incarnation
  that the watermark cannot distinguish from the old one, so that
  path follows the same-node delete-kills-the-link rule only
  loosely (see below). Leaf-node mirroring falls out naturally
  through the same forwarded API + leaf interest and is tested
  (hub stream, leaf mirror, backfill + live).
- Behaviour changed, deliberately and documented: (1) the synthetic
  reply subject gains the node id (above); (2) `route_via`
  consumer deliveries now also forward to routes/leafs with
  interest in the original subject (previously local-only) — this
  is what the mirror loop relies on, and it changes nothing when
  no remote node has such interest; (3) cross-node mirror/source
  of a nonexistent stream is accepted and left empty (Go's
  behaviour), because the creating node cannot synchronously
  verify a remote origin — the same-node 404/10059 deviation is
  preserved exactly when the node has no peers, and the mirror
  loop's consumer creation is the async existence check (it
  retries while the origin is absent). `external` /
  `subject_transforms` are still rejected.
- Not implemented / remainders: a JetStream consumer whose client
  sits on another node does not receive push/pull deliveries across
  the cluster — a pull delivery is framed with the original subject
  and the route protocol carries no deliver-subject field, so only
  a subscriber to the original subject (the mirror loop) can be
  matched on arrival; a leaf import from an account other than the
  leaf connection's account cannot be carried on that connection
  (leaf frames carry no account token); the mirror loopback client
  cannot use a TLS-required client port or a JWT-only account
  (no password user in config); JWT imports across routes/leafs
  are not separately verified.
- Verified: 10 new tests (`tests/e2e_imports.rs`: cross-node
  service request/reply both directions, cross-node stream copy,
  cross-node denied import, leaf service + stream imports through
  an exporter-account leaf; `tests/e2e_js_mirror.rs`: API
  forwarding to the hosting node, cross-node mirror backfill +
  live, cross-node source filter + start_seq, cross-node
  nonexistent-mirror-stays-empty, leaf-node mirror, cross-node
  mirror resume after origin restart with a store dir) plus 1 unit
  test (reply-subject parsing); full verification numbers in the
  commit message's verification block (fmt, clippy, cargo test,
  e2e-go.sh, parity.sh).

### Phase 33 — Gateways (super-cluster) (renumbered from plan Phase 32)
- Task renumbering: the plan's Phase 32 (this work) became Phase
  33 because the cross-node imports/mirroring phase landed as
  Phase 32 first; the existing PLAN.md entries for Phases 27–32
  were not overwritten, this entry is appended. This phase
  implements the Phase 24 design note — it does not edit it.
- Config (`src/config.rs`, `src/conf.rs`): `gateway { name,
  host, port, tls {...}, gateways: [{ name, urls: [...], tls
  {...} }] }`. The solicitation key is `gateways`, confirmed
  against live Go 2.15.1 (Go rejects `remotes` as unknown; a
  rust-natsd config using `remotes` loads but solicits nothing,
  covered by a test). Unknown gateway keys are ignored like other
  `.conf` sections. CLI: `--gateway-name/--gateway-host/
  --gateway-port` (solicitations are `.conf`-only, like leaf
  remotes). A `gateway {}` block with a port now binds a listener;
  it is no longer silently ignored.
- Wire (`src/gateway.rs`, reusing `src/route.rs` framing — no new
  wire language): INFO-first handshake exactly as probed in
  Phase 24 (accepter INFO with `gateway`, `gateway_urls`,
  `gateway_url`, `gateway_nrp`, `gateway_iom`; dialer CONNECT
  with echo/verbose/pedantic/headers false, name = dialer server
  id, gateway = dialer gateway name, then dialer INFO; accepter
  cmd 2 per account (base64 account name), RS+ interest, cmd 3
  per account). Cmd semantics as in Phase 24: cmd numbers and
  payloads are probed fact; cmd 2 = announce account and cmd 3 =
  switch to interest-only mode is the ordering-plus-log-line
  inference, and this implementation treats cmd 3 as a marker
  only — all forwarding is already interest-driven, so
  interest-only mode cannot leak traffic even if the inference
  is wrong. Accounts announced: `$SYS`, configured accounts in
  config order, `$G`.
- The directional rule (the finding that made the first Go-interop
  run fail, then fixed it): on one gateway connection, interest
  flows exactly one way and messages exactly the other way. The
  ACCEPTER sends RS+/RS-; the DIALER sends RMSG/HMSG. Go closes
  a connection whose dialer sends RS+ with `Received RS+ from
  gateway on inbound connection` / `Protocol Violation` (observed
  in Go's log when rust-natsd's dialer sent its initial RS+
  burst). rust-natsd therefore never sends RS+ on a solicited
  connection (neither the initial burst nor later propagations —
  `propagate_local_sub` sends gateway RS+ to inbound connections
  only), and symmetrically closes an inbound connection that
  receives RS+/RS- with `-ERR 'Protocol Violation'`. Both traffic
  directions work only because the supercluster forms one
  connection per direction (the implicit back-connect, below):
  each side is accepter on one connection and dialer on the other.
- Account scoping: accounts matched by name; the account token on
  every RS+/RMSG frame enforces the boundary (APP delivers, OTHER
  does not — tested explicitly, both Rust-Rust and Go-Rust).
  `$SYS` traffic is not filtered out (no subject filtering on the
  gateway path; the account token is the whole boundary). Queue
  subscriptions cross with their weight, and local publishes run
  global queue selection across local, route, leaf, and gateway
  destinations, so a queue group spanning a gateway delivers
  exactly once.
- Loop prevention (`src/server/delivery.rs`, `Origin::Gateway`):
  a message arriving on a gateway is delivered locally only —
  never back to any gateway, and never to routes/leafs (each node
  holds its own gateway connections to the peer cluster, so
  route-forwarding would duplicate it). Gateway-origin messages
  get the same service-import reply and stream-import handling as
  route/leaf-origin messages.
- Reply subjects (the second Go-interop finding): Go gateways do
  not propagate `_INBOX` subscriptions as RS+. When a request
  crosses a gateway, the receiving side records its reply subject
  -> gateway connection (60 s TTL, pruned on insert/lookup, same
  shape as the import-reply mappings), and a later publish to
  that subject flows back over the recorded connection even
  without RS+ interest; a reply arriving on a gateway whose
  subject is itself recorded is forwarded onward (the multi-hop
  return path). Without this, Go-requester/Rust-responder
  request/reply timed out while plain pub/sub both ways passed.
- Implicit gateways: on learning a peer from its INFO
  `gateway_urls`, the accepter solicits back (one connection per
  direction), deduped by peer gateway name and by URL against
  explicitly configured gateways, existing outbound connections,
  and already-spawned implicit loops. Solicitation retry/backoff
  reuses the route pattern (1 s, doubling to 5 s). The advertised
  URL uses the gateway block's host when it names a real
  interface, else the listener IP, with 0.0.0.0 replaced by
  127.0.0.1 — a documented simplification of Go's external-IP
  detection (Go advertises 198.19.0.2 in this sandbox when bound
  to 0.0.0.0, which is why the Phase 24 probe's back-connect died
  in a middlebox; binding the gateway host to 127.0.0.1 makes the
  advertisement loopback and the back-connect work).
- Gateway TLS (probed against Go 2.15.1 in this phase, not
  guessed): implicit TLS, like routes — the server handshake
  comes first, no plaintext INFO on a TLS gateway port; the
  dialer carries `tls: { cert_file, key_file, ca_file }` in its
  gateways entry, and Go's TLS gateway server requires a
  CA-verified client certificate (a dialer presenting no
  certificate fails the handshake even with `verify: false` on
  the server block). rust-natsd implements the same shape
  (gateway listener `tls {}`, per-remote `tls {}`, implicit
  solicitations inheriting the listener's TLS settings — Go's
  implicit dial from a TLS server to a plain peer fails the same
  way). One documented deviation: rust-natsd's TLS listener uses
  the shared TLS builder, so with `verify` unset and no CA it
  does not require a client certificate where Go does; Go dialers
  always present a certificate, so Go interop is unaffected.
- Monitoring (`src/monitor.rs`): `/gatewayz` now returns Go's
  shape — `outbound_gateways` (map name -> `{configured,
  connection}`) and `inbound_gateways` (map name -> list of the
  same), with `configured` true only for explicitly configured
  solicitations (implicit outbound and all inbound report
  false). This replaces the pre-Phase-33 `{"gateways": []}`
  shape; the two monitor tests that asserted the old shape were
  updated to the new empty maps, and the parity probe's
  no-gateway check (empty inbound and outbound) passes
  unchanged. `/varz` gains a `gateways` count; `/subz` lists
  gateway interest.
- Not advertised over gateways in this phase: JetStream API
  interest (`$JS.API.>`, `$JS.ACK.>`) and stream-import interest
  (routes/leafs keep both). JetStream is single-node, and
  cross-gateway imports are not part of the verified matrix.
- Verified: `tests/e2e_gateway.rs` (10 tests: raw-wire both
  directions, pub/sub both directions, headers + request/reply,
  account scoping, queue exactly-once, implicit back-connect +
  configured flags, `$SYS` subject not filtered, `.conf` parsing
  including the `remotes`-ignored case, TLS implicit-mTLS both
  directions) and `scripts/e2e-go-gateway.sh` against Go 2.15.1:
  (1) Go seed + Rust solicits, Go→Rust; (2) same seed, Rust→Go;
  (3) Rust seed + Go solicits, Go→Rust; (4) same seed, Rust→Go —
  each via nats.go cluster mode (both directions, queue,
  request/reply, headers) plus the account/queue gateway mode,
  plus `/gatewayz` configured-flag checks for the implicit
  back-connect in both seed directions, plus a Go↔Rust TLS
  scenario (cluster mode both directions under implicit mTLS).
  Full verification numbers in the commit message's
  verification block (fmt, clippy, cargo test, e2e-go.sh,
  parity.sh).

### Phase 34 — JetStream clustering / Raft (renumbered from plan Phase 33; STOPPED CLEANLY — Raft layer done, engine wiring is the remainder)
- Task renumbering: the plan's Phase 33 (this work) became Phase
  34 because the gateway phase landed as Phase 33 first; the
  existing PLAN.md entries for Phases 27–33 were not overwritten,
  this entry is appended. This phase implements the Phase 25
  design note's components (a), (b), and the transport half of
  (d) — it does not edit Phase 25, and it does NOT claim the
  full Phase 25 test plan (R3 stream end to end) — that gate
  was not met, so per the task's stop rule this phase stopped
  cleanly: only verified code is committed, the cluster path is
  NOT wired into the default JetStream path, and the remainder
  below is the concrete next-step design.
- Step 1 — Raft core (`src/raft.rs`, NEW): deterministic,
  transport-agnostic state machine. Persistent term/vote/log,
  Leader/Follower/Candidate, RequestVote, AppendEntries with
  prevLogIndex/prevLogTerm matching, commit advancement on
  quorum (current-term entries only, §5.4.2 — an old-term entry
  commits only when a newer-term entry from the same leader
  does), snapshot install + log compaction (stale snapshots are
  no-ops, never truncations of committed state), static
  3-node-style membership configured at group formation.
  Election timeouts are values supplied by the caller; the core
  takes an explicit fake clock (`now_ms`) and contains no
  randomness, no sockets, no timers — tests drive it
  deterministically. Dynamic membership change (joint
  consensus) is NOT implemented: it did not fall out cleanly and
  is a remainder, not a stub. Unit tests FIRST, all in
  `src/raft.rs`: election (single candidate wins with quorum,
  split-vote timeout retries in a higher term), log matching
  (follower rejects a mismatched prevLog, leader backs
  `next_index` up using the follower's hint and overwrites the
  divergent entry), commit advancement (no commit with zero
  follower acks, commit on majority; old-term entry not
  committed by counting alone), snapshot restore (log
  truncated to the snapshot prefix, state-machine data
  preserved, stale snapshot ignored), stale-term rejection and
  higher-term step-down.
- Step 2 — WAL backing (`FileRaftStore` in `src/raft.rs`):
  per-group durable state under
  `<store_dir>/raft/<sanitised-group>/` — `term.json`,
  `log.wal` (magic + CRC32 framed entries), `snapshot.json` +
  `snapshot.bin`. This layout is deliberately distinct from
  the single-node stream WAL
  (`<store_dir>/<account>/streams/<stream>/`), so clustered
  WAL data can never corrupt or be mistaken for R1 stream data,
  and R1 streams keep working exactly as before (full suite +
  parity persistence probe green). Crash recovery: a restarted
  node replays its group log and snapshot before rejoining
  (`RaftNode::from_persistent` restores term/vote/log/snapshot
  as a follower, so it must rejoin via election); a corrupt
  WAL tail is truncated on load, never replayed (same
  fail-closed rule as the stream WAL). Unit tests: WAL
  roundtrip + crash recovery (fresh store replays term, vote,
  both entries; recovered node is a follower with the full
  log), snapshot persistence + replay, corrupt-tail truncation.
- Step 3 — cluster binding (`src/js_cluster.rs`, NEW):
  `ClusterManager` holds one meta Raft group plus one Raft group
  per replicated stream (group lifecycle: create on assignment,
  replay on rejoin, tear down on delete is API-level — the
  manager's `create_group` + per-group WAL is the mechanism).
  `NatsRaftTransport` binds Raft RPCs to internal NATS subjects
  `$JS.RAFT.<node-id>` over the existing cluster routes: each
  node holds an internal client connection subscribed to its own
  subject, publishes RPCs to peers' subjects, and route RS+
  propagation carries them cross-node — no new wire protocol,
  no route-parser changes. Rust-only cluster is the honest
  scope: Go Raft interop is a separate reverse-engineering
  effort and is NOT claimed anywhere. Documented choices:
  consumer delivery/ack state is replicated per *stream* group,
  not per consumer group (Go uses a group per consumer) — the
  stream group already orders stream and consumer mutations
  against each other; reads are served by the group leader
  only, followers refuse (`NotLeader`) — there are no
  stale-tolerant follower reads in this phase; Raft RPCs are
  fire-and-forget NATS messages, loss handled by heartbeat /
  append retries, as Raft expects of an unreliable transport;
  node identity is the configured server name (stable across
  restarts); election-timeout randomisation lives only in the
  transport's tick loop, never in `raft.rs`. Two bugs found by
  the integration test and fixed in this phase, both recorded
  because the wiring phase will otherwise rediscover them: (1)
  a detached `tokio::spawn` ticker survived a transport abort,
  so a "killed" leader kept sending heartbeats and survivors
  never timed out — the ticker now runs as a `select!` future
  against the reader loop and dies with the connection; (2)
  the test's quorum-restore expectation was wrong, not the
  core: an entry written under an old term cannot commit by
  counting alone under a new leader (§5.4.2) — the test now
  proposes a second, current-term op, which carries it.
  Every mutation shape is encoded (`GroupOp`: AppendMsg,
  DeleteMsg, Purge, consumer create/delete/ack; `MetaOp`:
  stream assign/delete) — but the JetStream engine does NOT
  propose through these groups yet, so a PUB ack today is still
  a single-node ack. That is the remainder, stated plainly in
  the module docs so no reader mistakes this layer for
  clustered JetStream.
- Step 4 — API/monitoring: `STREAM.CREATE` and `STREAM.UPDATE`
  with `num_replicas > 1` are refused with 400/10002 and a
  message naming the Phase 34 state — with or without cluster
  peers, because refusing only when peerless would still
  advertise-then-store-R1 once peers exist but wiring does
  not. Nothing is silently stored as R1. `STREAM.INFO`,
  `CONSUMER.INFO`, and `/jsz` per-stream entries now carry
  `"cluster": null` — honestly absent for R1/single-node
  streams, not fabricated. Verified: async-nats parses the
  null cluster as `None`.
- Step 5 — tests (the plan's gate, at the layer that exists):
  `tests/e2e_js_cluster_raft.rs` runs 3 real rust-natsd nodes
  meshed by routes and drives the meta group through the
  transport: election + 5 replicated ops committed in order on
  all 3 nodes; leader-transport abort (SIGKILL equivalent at
  this layer — no graceful close) → survivors re-elect,
  continue serving, dead leader commits nothing new; restarted
  node rejoins and serves the full 8-op sequence (no gap, no
  duplicates); minority partition (one follower's transport
  down) → its own propose fails `NotLeader` and it commits
  nothing while the majority advances to 9, then rejoins
  cleanly at 9; quorum loss (2 of 3 transports down) → the
  remaining node commits nothing new for 2 s, then the
  restored cluster serves again (both the pre-restore old-term
  op and a new current-term op committed on all nodes).
  The stream-level gate is NOT met and is NOT claimed: no R3
  stream, no stream-leader SIGKILL with zero acked-message
  loss, no MSG.GET full-range restart catch-up at the stream
  level, no meta-leader reassignment on replica loss — those
  are the wiring phase's tests, not this phase's.
- Forbidden shortcuts — none shipped: no shared snapshot files
  as clustering, no election without log replication, no
  primary-backup copying without quorum, no advertising
  `num_replicas > 1` while storing R1, no PUB ack before
  quorum commit (the engine path that would ack is unwired,
  and the API refuses the replicated case outright).
- Verified: `cargo fmt --check` clean; `cargo clippy
  --all-targets -- -D warnings` clean; `cargo test` 268
  passed, 0 failed (was 256 at the Phase 33 run: +10 Raft unit
  tests + 2 cluster integration tests); `scripts/e2e-go.sh`
  OK; `scripts/parity.sh` GREEN (52 pass + 1 documented KV-gate
  exception + 0 fail; JS full PASS; persistence PASS;
  cluster/leaf smoke PASS). No stray servers: the integration
  test shuts every node down and removes its store dir.
- REMAINDER — concrete next steps, in order, with tests
  (the wiring phase): (1) Wire the server binary to start a
  `ClusterManager` + `NatsRaftTransport` when JetStream and
  clustering are configured (needs: a cluster-node-id source —
  server name is the design — plus peer discovery from route
  peers or an explicit peer list, plus auth for the internal
  connection, which today assumes no client auth; gate the
  whole path behind an explicit opt-in so default single-node
  behaviour is untouched). (2) Route the engine's mutation
  path through stream groups: publish → propose `StreamAppend`
  → apply on commit → only then send the PUB ack; ack →
  propose `ConsumerAck` → apply on commit; consumer
  create/delete, purge, delete likewise; meta group assigns
  streams/consumers to peers on create and reassigns on
  replica loss. The engine's Actions pattern must be preserved:
  never dispatch deliveries while holding `engine.lock()`, and
  never hold the sublist RwLock across socket/channel ops —
  Raft apply effects must be drained after the lock drops, as
  `handle_message` already does for actions. (3) Reads: the
  group leader serves MSG.GET / CONSUMER.INFO; followers
  redirect to the leader (or refuse with the leader's
  identity) — implement one, test it, document it; do not
  silently serve follower reads. (4) Dynamic membership
  (joint consensus) only after (1)–(3) are green, or document
  static membership as the permanent scope. (5) Then, and only
  then, lift the `num_replicas > 1` refusal behind the opt-in,
  populate real `cluster` fields in STREAM.INFO /
  CONSUMER.INFO / `/jsz`, and run the full Phase 25 gate: R3
  stream end to end, stream-leader SIGKILL with zero acked
  loss, minority partition refusing writes and rejoining
  cleanly, restart catch-up verified by MSG.GET across the
  full sequence range, quorum loss refusing writes, replica
  loss + meta reassignment. Go Raft interop stays a separate
  reverse-engineering effort after all of that, not a
  consequence of it.

## Future work (not in this plan)
- JetStream clustering/replication
  (Phase 25 design note; Phase 34 partial): Raft core, per-group
  WAL, and Raft-over-routes transport implemented and tested at
  the group layer; the JetStream engine wiring is the remainder
  (see Phase 34) — replicated streams are refused, not served,
  and there is no accepting stub.
- JWT imports across routes/leafs, MQTT/WebSocket gateways,
  XKeys, cert-to-user mapping on route/leaf connections,
  object-store client-helper verification (KV done in Phase 31).
  (Cross-node mirroring/sourcing and cross-node imports/exports
  are done in Phase 32.)
- Phase 28C is complete (see above); no further 28-series item
  remains.

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
