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
`parity/KNOWN_DIFFS.md`): gateways, JetStream clustering/replication
(Raft/WAL), MQTT/WebSocket gateways, cross-node mirroring/sourcing,
cross-node imports/exports, JWT imports across routes/leafs, XKeys,
cert-to-user mapping on route/leaf connections, and Go-only INFO
fields. File persistence is **in** scope (Phases 15, 19) — an earlier
version of this plan listed it out of scope, which was superseded.

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
raw-wire protocol tests. **200 Rust tests green, 2 ignored** (the
ignored tests name two unfixed bugs — see Phase 26 and README
Known issues); Go interop scripts green in both directions (routes,
leafs, JetStream, route/leaf TLS, imports, mirroring). Parity suite
green in default and strict mode (53 probes, 52 pass + 1 documented
KV-gate exception, 0 fail).

Remaining items: gateways and JetStream clustering/replication are
**documented remainders, not implemented** — each has a design note
below (Phase 24, Phase 25) and no code. Still non-goals:
cross-node mirroring/sourcing, cross-node imports/exports, JWT
imports across routes/leafs, MQTT/WebSocket gateways, XKeys,
cert-to-user mapping on route/leaf connections, and the
KV/object-store client helpers (their server-side primitives exist;
raw `$KV` matches Go, nats.go helpers are version-gated).

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

## Future work (not in this plan)
- Gateways (Phase 24) and JetStream clustering/replication
  (Phase 25): documented remainders with design notes, not
  implemented, not planned-imminent — no code, no accepting stub.
- Cross-node mirroring/sourcing, cross-node imports/exports,
  JWT imports across routes/leafs, MQTT/WebSocket gateways,
  XKeys, cert-to-user mapping on route/leaf connections,
  KV/object-store client-helper verification.
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
