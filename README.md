# rust-natsd

A NATS server implemented in Rust, from scratch — Core NATS pub-sub,
clustering/routes, leaf nodes, JetStream (in-memory or file-persisted),
TLS client connections, NKey authentication, multi-account scoping, a
monitoring endpoint, and `.conf` file support.

Unmodified official clients work against it:

- **Go** (`nats.go` v1.55.0) — core, cluster, leaf, and JetStream
  interop programs under `e2e/go`, all green against real `nats-server`
  v2.15.1 on the other side of routes/leafs
- **Rust** (`async-nats` 0.38) — core, leaf, JetStream, and monitoring
  integration tests

## Quick start

```sh
cargo run --release            # listens on 0.0.0.0:4222
cargo run --release -- -p 4223 --name edge-1
cargo run --release -- --user derek --pass s3cret
cargo run --release -- --auth-token mytoken
cargo run --release -- --jetstream --http-port 8222
cargo run --release -- --jetstream --js-store-dir /var/lib/rust-natsd/js
cargo run --release -- --tls --tlscert server.crt --tlskey server.key
cargo run --release -- -c server.conf   # nats-server .conf subset
```

Then connect with any NATS client, e.g. the `nats` CLI:

```sh
nats --server nats://127.0.0.1:4222 sub 'events.>'
nats --server nats://127.0.0.1:4222 pub events.click 'hello'
```

Clustering and leaf nodes:

```sh
# Node A (seed): routes on 6222, leafs accepted on 7422
cargo run --release -- -p 4222 --cluster-port 6222 --leaf-port 7422
# Node B: solicits a route to A
cargo run --release -- -p 4223 --cluster-port 6223 --routes nats://127.0.0.1:6222
# Node C: a leaf soliciting A
cargo run --release -- -p 4224 --cluster-port 0 --leaf-urls nats://127.0.0.1:7422
```

## Feature matrix

Status is one of **Implemented**, **Partial**, or **Not implemented**.
This table is the single source of truth in this README for what
rust-natsd does; it replaces the older support table.

| Feature | Status | Notes |
|---|---|---|
| Core pub/sub (`PUB` / `SUB` / `UNSUB`) | Implemented | Incl. clients sending explicit JSON nulls in `CONNECT` |
| Subject wildcards `*` (one token), `>` (terminal, 1+ tokens) | Implemented | Go's lax rules: literal wildcard publishes, partial wildcards literal |
| Queue groups (one-of delivery, exactly once) | Implemented | Round-robin spread (Go's is random-ish; totals match, per-member distribution is not compared) |
| Queue groups spanning routes/leafs (global selection) | Implemented | Local + route + leaf members, one delivery per group |
| Request/reply (reply subjects) | Implemented | |
| Headers: `HPUB` → `HMSG` for header-capable clients, stripped `MSG` otherwise | Implemented | |
| Auto-unsubscribe (`UNSUB sid <max>`) | Implemented | |
| `verbose` mode (`+OK`) and `pedantic` mode | Implemented | Go ordering: `+OK` before self-delivery; pedantic invalid publish gets `-ERR` and is still delivered |
| `echo: false` (no self-delivery) | Implemented | |
| Max payload enforcement | Implemented | Go's exact `-ERR 'Maximum Payload Violation'` text |
| Protocol errors | Implemented | Go's exact `-ERR` texts; malformed `CONNECT` closes silently, like Go |
| Auth: `--user/--pass`, `--auth-token` | Implemented | Token only via `auth_token`, matching Go (token-as-password is rejected) |
| NKey auth | Implemented | Per-connection `nonce` in `INFO`, `nkey`/`sig` in `CONNECT`, verified with the `nkeys` crate; standalone nkey users only |
| JWT operator mode | Implemented | `src/jwt.rs`: operator → account → user Ed25519 chain verified with the existing `nkeys` crate (no new dependency); header must be `typ: JWT` / `alg: ed25519-nkey`, `exp` in the past / `nbf` in the future / `iat` >60 s in the future all reject, any failure rejects with no fallback. User JWTs additionally require the INFO-nonce signature (a `jwt` field alone never authenticates; `bearer_token` is honored since Phase 28C (nonce signature skipped unless the account sets `disallow_bearer`); account `revocations` are honored (including `"*"`); account `limits` are enforced since Phase 28C (`conn` account-wide, `subs` per-connection, `payload` per-message, `data` as an account-wide cumulative inbound payload budget — a rust-natsd interpretation, since Go's server does not visibly enforce `Data`)). When CONNECT carries an `nkey` it must equal the JWT `sub`; when it does not (nats.go UserJWT shape), the key is derived from `sub` (Phase 28B). User pub/sub claims apply, falling back per-axis to the account JWT's `default_permissions`; account JWT imports/exports reuse the area-6 machinery (see `Config::all_accounts`). `.conf`: `operator: "<jwt>"` (Go inline shape) or `operator { jwt \| jwt_file, accounts: [...] }`, plus Go's `resolver { type: memory }` + top-level `resolver_preload: { <pubkey>: "<jwt>" }` (Go v2.15.1's real preload key, probed — a `preload` map inside `resolver` is also accepted). Verified against Go 2.15.1: Rust-generated JWTs are accepted by Go (pub/sub delivery works) and expired ones rejected. Go-shape notes: claim `name` is top-level (also accepted inside `nats`); account limits keys are `conn`/`subs`/`payload`/`data`/`leaf`/`imports`/`exports` with -1 = unlimited and an absent field inside a present `limits` object = 0 (enforced, as in Go); an account JWT with no `limits` object at all is treated as unlimited here, where Go would reject its first connection (documented deviation); a Go export with no `accounts` field authorizes any importer, while a present list restricts to it. Not implemented: account-JWT `limits` enforcement, user `revocations` lists (revocation here = removing the account JWT from trust), and `$SYS` special-casing (the system account is an ordinary JWT account) |
| Multi-account + pub/sub allow/deny | Implemented | `accounts {}` in `.conf` (per-user `permissions` or account-level `authorization`), `system_account`, `no_auth_user`; subscriptions/deliveries/routes/leafs/JetStream scoped per account; Go's exact permission-violation `-ERR` text, connection stays alive |
| Account imports/exports (service + stream) | Implemented (same node) | `accounts { NAME { exports: [{service\|stream: "subj", accounts: ["B"]}], imports: [{service\|stream: {account: A, subject: "subj"}, to: "local"}] } }` — Go v2.15.1 `.conf` shape probed (`to`, not `local_subject`; the older spelling is accepted as an alias). Service requests are rewritten into the exporting account with a synthetic `_RIMPORT.<seq>` reply subject that routes replies back; stream publishes are copied to the importer's local subject. Unauthorized imports (account not listed, remote pattern not covered by the export) are dropped, never delivered; the importing account's publish allow/deny is checked on the local subject and the responder's subscribe allow/deny on the exported subject. Same-node only: imports do not cross routes/leafs, and route/leaf-origin messages do not fan out to stream importers. Reply mappings expire after 60 s, pruned on insert and lookup; translated service requests carry `Nats-Request-Info: {"acc": "<importing account>"}` (Go also sends `rtt` when measured; rust-natsd does not measure client RTT, so it is omitted) |
| TLS client connections | Implemented | `--tls/--tlscert/--tlskey/--tlsverify/--tlscacert`, `tls {}` in `.conf`, `tls_required` in `INFO`; STARTTLS order (plaintext `INFO` first, then upgrade), matching Go |
| Route/leaf TLS | Implemented | Route TLS is implicit TLS (no plaintext first, unlike client STARTTLS); leaf TLS is INFO-first then upgrade (hub sends plaintext `INFO` with `tls_required`, then TLS). Both directions interop with Go 2.15.1 (see `scripts/e2e-go-route-leaf-tls.sh`); `--cluster-tls*` / `--leaf-tls*` flags, `cluster { tls {} }` / `leafnodes { tls {} }` / remote `tls {}` in `.conf`. Certificate verification on route/leaf connections is CA-only; cert-to-user mapping does not apply there |
| TLS client-certificate-to-user mapping | Implemented | Client connections only: `tls { verify_and_map: true }` (also `--tlsverify-and-map`): the client certificate's identity — email SANs, then DNS SANs, then URI SANs, then CN, then full DN — is matched against configured user names (a superset of Go's DN-only match, so both `alice` and `CN=alice` users work); no mapping entry means rejection even with a valid password, and a mapped user is still subject to that user's publish/subscribe allow/deny. Certificate identities are parsed from DER by hand (no new dependency) |
| XKeys | Not implemented | Non-goal |
| Routes / clustering | Implemented | Full mesh, solicitation with retry, RS+/RS-, RMSG/HMSG, duplicate resolution |
| Leaf nodes | Implemented | Hub accept + solicited leaf, LS+/LS-, LMSG/HMSG, reconnect |
| Server-initiated PING / stale connection | Implemented | `--ping-interval` / `--ping-max` (Go defaults 2m/2); close with `-ERR 'Stale Connection'` |
| No-responders 503 | Implemented | HMSG 503 when the publisher set `no_responders` and a publish with a reply subject has no deliveries |
| Slow-consumer detection | Implemented | Byte-based pending limit (default 64 MiB, `--max-pending` / .conf `max_pending`, reported in `/varz` as `max_pending` and per-connection in `/connz` as `pending_bytes`) → disconnect when exceeded. A fast reader at bench rates is not dropped; a never-reading reader is, like Go |
| Graceful shutdown on SIGINT/SIGTERM | Implemented | Writer flushes pending frames (e.g. a final `-ERR`) before close |
| JetStream: streams, pull + push consumers | Implemented | In-memory, or file-persisted with `--js-store-dir` |
| JetStream: ack policies (+ACK/-NAK/+TERM/+WPI), redelivery, dedup, expected sequences, MSG.GET, purge/delete | Implemented | `max_deliver` enforced for pull and push consumers on both the -NAK path and the ack-wait sweep; exhaustion publishes the Go `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>` advisory (type `io.nats.jetstream.advisory.v1.max_deliver`) and, for workqueue streams, leaves the message in the stream (only a plain +ACK deletes it), as Go v2.15.1 does |
| JetStream: workqueue retention, `max_msgs_per_subject` | Implemented | |
| JetStream: deliver policies `all` / `last` (filter-aware) / `new` / `by_start_sequence` | Implemented | |
| JetStream persistence | Implemented | Whole-engine JSON snapshot after every mutation, reloaded on startup — including per-consumer pending ack state (in-flight stream seqs, delivery counts, wall-clock delivery times), ack floors, redelivery counts, and pull redelivery queues (snapshot v2; v1 snapshots load with empty ack state). Restart preserves the remaining ack window (Go v2.15.1 semantics, probed): unacked messages redeliver once the window expires with delivered count incremented, acked messages never redeliver, push redelivery defers while the deliver subject has no interest; waiting pulls are not persisted (dead reply inbox) |
| JetStream clustering / replication | Not implemented | Remainder, not implemented — design note in PLAN Phase 25 (Raft/WAL scope); single-node only, no replication, no high availability |
| JetStream mirroring / sourcing | Implemented (same node) | `mirror` / `sources` in STREAM.CREATE (`name`, `filter_subject`, `opt_start_seq`, `opt_start_time`); backfill at creation then live copies via the capture hook (atomic under the engine lock: no loss, no duplicates); mirror copies are exact, source copies add a `Nats-Stream-Source` header; origin delete keeps copies but kills the link permanently, purge leaves the mirror untouched. Cross-node mirroring/sourcing (routes/leafs, `external`) is NOT implemented and is rejected at creation. One deliberate deviation from Go: a mirror/source of a nonexistent stream is rejected with 404/10059 instead of Go's accept-and-leave-empty |
| JetStream `$JS.API` routing across routes/leafs (`$JS.API.>` interest) | Implemented | |
| KV / object store as client helpers | Partial | Stream primitives exist (`KV_<bucket>` streams, `last_by_subj`); raw `$KV` traffic matches Go (parity probe `kv_raw`), but the nats.go/async-nats KV helpers apply a version gate (see below) and are not verified end-to-end |
| Monitoring `/varz` | Implemented | Real `http_port`, `mem`, `slow_consumers`, configured ping values |
| Monitoring `/connz` | Implemented | `acc`/`state`/`limit`/`offset` filters; `account`/`user`/`tls` per connection |
| Monitoring `/subz` | Implemented | Rust superset: Go 2.15.1 answers 404 here |
| Monitoring `/routez` | Implemented | |
| Monitoring `/leafz` | Implemented | Field naming differs from Go (count vs list shape); counts compared in the parity suite |
| Monitoring `/jsz` | Implemented | |
| Monitoring `/accounz` | Implemented | Rust superset: Go 2.15.1 answers 404 here |
| Monitoring `/gatewayz` | Partial | Empty response only; no gateway protocol is implemented (see Phase 24 remainder note) |
| Monitoring `/healthz` | Implemented | |
| Config files (`.conf`) | Partial | nats-server `.conf` subset via `-c` (CLI wins over file); unknown keys ignored |
| `INFO` completeness | Partial | Omits Go-only fields by design: `git_commit`, `go`, `xkey`, and `api_lvl` (Go's JetStream API level — advertising 5 would gate clients into JS features Rust does not implement). All other fields match Go, including omitempty semantics |
| Gateways (super-cluster) | Not implemented | Remainder, not implemented — design note in PLAN Phase 24, with the Go 2.15.1 gateway handshake, RS+/RMSG framing, and account scoping probed on the wire; no gateway protocol code, no accepting stub |
| MQTT / WebSocket gateways | Not implemented | Non-goal |

See [PLAN.md](PLAN.md) for the phased plan and design notes.

## Configuration reference

Every flag and `.conf` key below is implemented (verified against
`src/config.rs` / `src/conf.rs` at HEAD); there is no other
configuration surface. Run `rust-natsd --help` for the same list.
Explicit CLI flags win over the file, which is loaded first with
`-c` / `--config`.

**CLI flags:**

| Flag | Default | Notes |
|---|---|---|
| `-a, --addr <HOST>` | `0.0.0.0` | Client bind host |
| `-p, --port <PORT>` | `4222` | Client port |
| `-n, --name <NAME>` | `rust-natsd` | Server name |
| `--max-payload <SIZE>` | `1mb` | Sizes accept plain bytes or `k`/`mb` suffixes |
| `--max-pending <SIZE>` | `64mb` | Queued outbound bytes per connection before slow-consumer drop |
| `--ping-interval <DUR>` | `2m` | Durations accept `ms`, `s`, `m` (e.g. `200ms`) |
| `--ping-max <N>` | `2` | Missed PONGs before stale close |
| `--user <USER>` / `--pass <PASS>` | — | Global credentials (the `$G` account) |
| `--auth-token <TOKEN>` | — | Token, only via `auth_token` in CONNECT |
| `--cluster-name <NAME>` | server name | |
| `--cluster-host <HOST>` | `0.0.0.0` | Route listener host |
| `--cluster-port <PORT>` | `6222` | Route listener port; `0` disables |
| `--routes <URLS>` | — | Comma list of routes to solicit |
| `--leaf-port <PORT>` | `0` | Leafnode listener port; `0` disables |
| `--leaf-urls <URLS>` | — | Comma list of hub leafnode endpoints |
| `--leaf-user <USER>` / `--leaf-pass <PASS>` | — | Credentials for solicited leaf connections |
| `--jetstream` | off | Enable JetStream |
| `--js-store-dir <DIR>` | — | JetStream store dir (whole-engine JSON snapshot; see JetStream persistence) |
| `--http-port <PORT>` | `0` | Monitoring HTTP port; `0` disables |
| `--tls` / `--tlscert` / `--tlskey` / `--tlscacert` / `--tlsverify` / `--tlsverify-and-map` | — | Client TLS; `--tlsverify` and `--tlsverify-and-map` require `--tlscacert` |
| `--cluster-tls` / `--cluster-tlscert` / `--cluster-tlskey` / `--cluster-tlscacert` / `--cluster-tlsverify` | — | Implicit TLS on the route port |
| `--leaf-tls` / `--leaf-tlscert` / `--leaf-tlskey` / `--leaf-tlscacert` / `--leaf-tlsverify` | — | INFO-first-then-upgrade TLS on the leafnode port |
| `-c, --config <FILE>` | — | `.conf` file |
| `-h, --help` | — | |

**`.conf` keys** (top level unless noted; unknown keys are ignored):

- `port`, `host`, `server_name`, `http_port`, `max_payload`,
  `max_pending`, `ping_interval`, `ping_max`
- `authorization { user, password, token, users: [{ user, password, token, nkey, permissions }] }`
- `accounts { NAME { users: [...], permissions { publish { allow, deny }, subscribe { allow, deny } } | authorization { ... }, jetstream: enabled, exports: [{ service | stream, accounts: [...] }], imports: [{ service | stream: { account, subject }, to }] } }` — `to` is the import's local subject (`local_subject` is accepted as an alias); per-account `jetstream: enabled` is enforced since Phase 28C: with multi-account configuration, an account without it gets Go's 503/10039 "JetStream not enabled for account" on `$JS.API.>` requests; single-account and JWT-operator servers are unaffected
- `system_account`, `no_auth_user`
- `operator: "<jwt>"` or `operator { jwt | jwt_file, accounts: [...] }`; `resolver { type: memory, preload: {...} }` (Rust convenience) and top-level `resolver_preload: { <pubkey>: "<jwt>" }` (Go's real preload key)
- `cluster { name, host, port, routes: [...], tls { ... } }`
- `leafnodes { port, tls { ... }, remotes: [{ url, tls { ... } } | "<url>"], authorization { user, password } }`
- `tls { cert_file, key_file, ca_file, verify, verify_and_map }` — the same block shape is used for `cluster { tls {} }`, `leafnodes { tls {} }`, and leaf remotes
- `jetstream` — boolean, or a block with `store_dir`

JetStream stream/consumer fields (`mirror`, `sources`, `max_deliver`,
etc.) are API fields on `$JS.API` stream/consumer creation, not
`.conf` keys.

## Comparison with mainline (Go) implementation

Mainline here is Go `nats-server` **v2.15.1**, the reference
implementation. The comparison below is deliberately narrow: only
claims traceable to code or to a suite/benchmark result are made.

**Protocol behaviour — at parity on probed behaviour.** The repeatable
suite in `scripts/parity.sh` starts both servers side by side with
matching configs and runs the same raw-wire and Go-client probes
against both: **53 probes, 52 pass + 1 documented exception, 0 fail,
in both default and `--strict` mode** (last verified 2026-10-25
at HEAD `0cb8650`, re-verified during the 2026-10-25 documentation
refresh).
The single exception is not a server difference: `nats.go` gates
key-value on server version >= 2.6.2, and rust-natsd honestly reports
its own crate version (0.1.0), so `CreateKeyValue` fails client-side
before any KV traffic happens. Raw `$KV` traffic bypassing the gate
matches Go. The remaining documented exceptions — Go-only `INFO`
fields, the monitoring superset, TLS refusal-layer variance — are
listed in full in [parity/KNOWN_DIFFS.md](parity/KNOWN_DIFFS.md).
Nothing in this section claims byte-identical monitoring JSON or
identical per-member queue distribution: both are compared on
supported fields / totals only, by design.

**Scope — what mainline does that rust-natsd deliberately does not.**
Gateways (super-cluster) and JetStream clustering/replication are
documented remainders (design notes in PLAN Phases 24–25, not
implemented, not planned-imminent); cross-node mirroring/sourcing,
MQTT/WebSocket gateways,
and XKeys are non-goals. JetStream
here is single-node only (mirroring/sourcing and account imports work
on the same node).
Conversely, rust-natsd is a superset in
exactly one place: it implements `/subz` and `/accounz`, which Go
2.15.1 answers with 404.

**Maturity and operations — not claimed production-ready.** This is
a from-scratch reimplementation, not a hardened production server:
no JetStream clustering (so no JS high availability) and no
production-hardening claims. The benchmark-exposed slow-consumer
drop (below) is fixed: the outbound path now batches socket writes
and enforces a Go-comparable byte-based pending limit instead of a
1024-frame queue. Do not run this as a drop-in replacement for
mainline in production without weighing that list.

**Architecture.** Tokio async runtime, a hand-rolled incremental
protocol parser (no I/O in the parser, which is what makes
split-at-every-byte tests possible), one reader + one writer task per
connection joined by a byte-limited queue (the writer coalesces
queued frames into 64 KiB batched socket writes), and
`#![forbid(unsafe_code)]`
— no unsafe code anywhere. No Go runtime, no vendored Go code; the
protocol, subject matching, routing, leaf, JetStream, TLS
(rustls/ring), and monitoring layers are all written for this
project. That independence is the point of the project, and also its
risk: every mainline behaviour has to be re-derived (the parity
suite exists for exactly that reason).

## Benchmarks

Benchmarks are run by `bash scripts/bench.sh` (client:
`bench/go_client/`, the same unmodified-dependency `nats.go` v1.55.0
binary against both servers, so client bias cancels). Raw output is
written to `bench/results/bench-<timestamp>.txt` (gitignored); only
the aggregated table below is committed.

**Methodology.** Localhost, single node, no TLS, no auth, JetStream
on, benchmark stream on memory storage for both servers. rust-natsd
is built in `--release` mode — a debug build would be a rigged
comparison. Each scenario runs 3 times per server, alternating which
server runs first, with a warm-up phase before measurement
(10k pub/sub, 1k request/reply, 1k JetStream). Numbers below are the
**median across the 3 runs, with the range in brackets — never a
single best run**. This box is a shared VM (2 vCPUs), so the numbers
are noisy; treat them as an order-of-magnitude comparison, not a
precise ranking. One fairness note: rust-natsd runs *without*
`--js-store-dir` in the benchmark, because with a store dir it
snapshots the whole engine JSON after every mutating operation,
which would dominate the JetStream scenario; Go's `-sd` then holds
only JetStream metadata for a memory stream, so both message paths
are in-memory.

**Results — 2026-10-25, refreshed at HEAD `0cb8650`** (one full
`bash scripts/bench.sh` run, 3 runs per server, medians [ranges]).
Go `nats-server` v2.15.1 vs rust-natsd at HEAD; the bench harness
does not change server behaviour. Machine:
Linux x86_64, AMD EPYC 9D64, 2 vCPUs, Go 1.27.3, rustc 1.98.1.
Raw output: `bench/results/bench-2026-10-25T045508.txt` (gitignored).

| Scenario | Go nats-server v2.15.1 (median [range]) | rust-natsd (median [range]) |
|---|---|---|
| Core pub/sub, 256 B, 200k msgs (msgs/s) | 621,740 [617,203..669,713] | 391,090 [358,140..401,949] |
| Core pub/sub, 1 KiB, 200k msgs (msgs/s) | 303,720 [268,943..319,209] | 202,768 [199,269..242,006] |
| Request/reply, 256 B, 20k requests (mean / p50 / p99 µs) | 290 / 268 / 701 | 203 / 184 / 601 |
| JetStream publish with ack, 1 KiB, 20k msgs (msgs/s) | 5,609 [5,246..5,644] | 7,930 [7,757..8,191] |

**Read it plainly.** All four scenarios complete on both servers.
On core pub/sub throughput rust-natsd trails Go in this run
(391,090 vs 621,740 msgs/s at 256 B; 202,768 vs 303,720 at 1 KiB) —
the gap is writer/parse throughput, not drops. Do not read the
change from the 2026-10-24 table (418,910 / 226,910) as a regression
from the gap-fix work: run-to-run noise on this shared VM is large
enough that Go's own pub/sub-256 number moved 497,531 → 621,740
between the same two runs, and request/reply and JetStream are
essentially unchanged (203 vs 196 µs mean; 7,930 vs 8,019 msgs/s).
On request/reply latency and JetStream publish throughput,
rust-natsd is faster here (lower latency, higher msgs/s) — that is
a localhost, single-client, uncontended result, not a general
performance claim. For history: before the slow-consumer fix,
rust-natsd pub/sub **failed** in all attempts (subscriber dropped
as a slow consumer after 1–500 of 200,000 deliveries) because of
the old 1024-frame outbound queue; that failure mode is gone, and
a never-reading subscriber is still dropped at the 64 MiB byte
limit (see PLAN Phase 18).

**Repeatability.** No stray servers were left running after the run
(verified by process and port check).

## Known issues and limitations

These are known, not hidden. There are currently no `#[ignore]`d
tests in the suite.

- **Deliberate deviations from Go** (all verified against Go
  2.15.1, documented here rather than fixed): JetStream mirror
  backfill is synchronous where Go's is asynchronous (final state
  matches); a mirror/source of a nonexistent stream is rejected
  with 404/10059 where Go accepts it and leaves the stream empty;
  an unauthorized account import drops the message instead of Go
  refusing to start the server; import reply mappings expire after
  60 s (pruned on insert/lookup); translated service requests carry
  `Nats-Request-Info: {"acc": ...}` without Go's `rtt` (no client
  RTT measurement here).
- **JWT limitations:** account-JWT `limits` are enforced since
  Phase 28C, except `data` is a rust-natsd interpretation (cumulative
  inbound payload budget; Go does not visibly enforce `Data`) and a
  limits-less account JWT is treated as unlimited where Go would
  reject its first connection; the Rust `iat` >60 s future cap is
  stricter than Go (Go accepted `iat` 300 s in the future in
  probing); `$SYS` is an ordinary JWT account, not special-cased;
  Go-generated JWT → Rust was not run (no `nsc` generator
  available), though claim shapes were verified against the JWT
  library source.
- **Scope limits (also the out-of-scope list, kept identical in
  PLAN.md and `parity/KNOWN_DIFFS.md`):** gateways, JetStream
  clustering/Raft/WAL, MQTT/WebSocket, cross-node
  mirroring/sourcing, cross-node imports/exports, JWT imports
  across routes/leafs, XKeys, cert-to-user mapping on route/leaf
  connections (CA verification only there), and Go-only INFO
  fields (`git_commit`, `go`, `xkey`, `api_lvl` — advertising
  Go's JS API level would gate clients into unimplemented JS
  features). Per-account `jetstream: enabled` in `.conf` is
  enforced since Phase 28C (see above).

## Tests

```sh
cargo test                    # Rust tests passing, 0 ignored (unit + integration; count in PLAN Phase 28C)
bash scripts/e2e-go-advanced.sh # Go client imports, mirror, JWT minting (JWT mode wired since Phase 28B)
bash scripts/e2e-go.sh        # Go client core e2e
bash scripts/e2e-go-cluster.sh  # Go <-> Rust route interop, both seed directions
bash scripts/e2e-go-leaf.sh     # Go <-> Rust leaf interop, both directions
bash scripts/e2e-go-js.sh       # Go JetStream interop (nats.go JS client)
bash scripts/e2e-go-route-leaf-tls.sh  # Go <-> Rust route/leaf TLS interop, both directions
```


Test layers, by design:

1. **Unit** — parser fed split at every byte boundary, subject-matching truth
   tables, sublist queue/auto-unsub accounting, auth matrix, CLI parsing,
   `.conf` parsing.
2. **Raw-protocol e2e** — literal wire bytes over TCP, including error and
   edge behavior no client library would generate (bad ops, oversized
   payloads, `UNSUB` max races resolved via PING/PONG sync).
3. **Real-client e2e** — the official Go and Rust clients, unmodified. These
   caught real bugs: e.g. `async-nats` sends explicit `null` for CONNECT
   string fields, which the parser originally rejected; Go's leaf protocol
   differs from the docs in several places (see below).

## Parity suite

```sh
bash scripts/parity.sh                 # full run (raw probes + JS/KV + persistence + cluster/leaf smoke)
bash scripts/parity.sh --skip-cluster  # skip the cluster/leaf smoke scripts
bash scripts/parity.sh --strict        # also fail on known diffs
PARITY_KEEP_TMP=1 bash scripts/parity.sh  # keep configs/logs/results under /tmp for debugging
```

The suite builds rust-natsd (release), locates Go nats-server v2.15.1
(`GO_NATS_SERVER`, the `~/workspace/tools/` copy, or a download of the
`nats-server-v2.15.1-linux-amd64.tar.gz` release asset), starts both
servers side by side on free ports with matching configs, runs the same
probes against both, and compares semantic outcomes — delivered
subjects/payloads, counts, `-ERR` class, connection alive/closed, and
monitoring field values for supported fields. Per-run random values
(server_id, client_id, ports, timestamps, version, git_commit, …) are
stripped, never compared.

Reading the table: each case is `pass` (identical semantics),
`exception` (a documented non-failure listed explicitly — Go-specific
INFO fields, the nats.go KV version gate — see below), `fail` (a new
divergence), or `harness_error` (the probe itself broke). The suite is
green in both default and strict mode: there are no remaining known
server diffs. Exit codes: 0 green, 1 divergence, 2 setup failure.

Layout: `scripts/parity.sh` (orchestration, servers, certs, JS/KV,
persistence, smoke), `parity/probes.py` (raw-wire probes + comparison),
`parity/go_client/` (nats.go probe client: JetStream + KV, and the
nkeytool signer), `parity/KNOWN_DIFFS.md` (the documented exceptions
and the fixed-diff history).

Adding a case: write a probe function in `parity/probes.py` returning
`{"go": …, "rust": …}` semantic outcomes, register it in `CASES`, and
run the suite. If it finds a real server bug, fix the server — do not
change the suite to hide the diff (no normalising away a real
behavioural difference, no weakening a probe to make it pass). A probe
that was wrong about Go's behaviour may only be changed after a fresh
probe against real Go proves it.

## Interop notes (Go behavior that differs from the docs)

These were all found empirically against `nats-server` v2.15.1 and are
handled in the implementation:

- Route handshake: the dialer sends `CONNECT` first, then reads INFO,
  then sends its own INFO. INFO-first gets `Authorization Violation`.
- Leaf queue subscriptions require a weight: `LS+ subject queue 1`.
  The two-argument form is a protocol violation and Go closes the leaf.
- Header messages over leaf connections travel as `HMSG` (no sid, no
  account), not as `LMSG` with two sizes — Go parses the latter as a
  plain message with the header block embedded in the payload.
- A soliciting Go leaf closes a hub connection with `attempted to
  connect to wrong port` unless the hub INFO carries both `client_id`
  and `leafnode_urls` (or `info_on_connect`).
- A Go leaf sends `CONNECT` without a preceding `INFO` unless the hub
  INFO sets `info_on_connect`.
- Clients use the legacy `CONSUMER.DURABLE.CREATE` endpoint when the
  server reports a version below 2.9.0; `STREAM.NAMES` backs
  `StreamNameBySubject`; negative ack on the wire is `-NAK`, not `+NAK`.
- JetStream API responses are flat (`config`/`created`/`state` at top
  level, no `response` envelope) and error codes matter: 10059 stream,
  10014 consumer, 10037 message — nats.go maps them to typed errors.

## Design

- `src/protocol.rs` — pure incremental state machine over a byte buffer
  (no I/O), which is what makes split-at-every-byte tests possible.
- `src/subject.rs` — subject validation and wildcard matching.
- `src/sublist.rs` — subscription registry; recipient resolution (fan-out,
  queue groups, auto-unsub accounting, echo filtering) happens under a
  short RwLock read lock for matching and a write lock for mutation,
  writes to sockets happen outside it. Matching is a subject
  trie (exact / `*` / `>` nodes) over insertion-ordered subscriptions,
  not a linear scan; see `examples/sublist_bench.rs` for the 10k-subscription
  harness (sparse-subject match ~0.3 µs vs ~350 µs linear, release).
- `src/server/` — Tokio listener; one reader task and one writer task per
  connection connected by a byte-limited queue (default 64 MiB pending,
  the slow-consumer bound; the writer coalesces queued frames into
  64 KiB batched socket writes). The
  writer holds no channel sender, so closing a connection flushes pending
  frames (like a final `-ERR`) before the socket closes. Global queue
  selection across local members, routes, and leafs lives in `deliver()`.
  Split in Phase 29 (pure move, no behaviour change): `mod.rs` (Shared,
  lifecycle, listener loops, route/leaf handshakes + solicitation),
  `conn.rs` (client connections, writer, CONNECT/PING/PONG state machine),
  `delivery.rs` (`deliver`, fan-out, queue selection, inbound route/leaf
  frame processing), `imports.rs` (service/stream import translation,
  `_RIMPORT` TTL table). Public exports unchanged (`server::start`,
  `RunningServer`, `Message`, `Origin`, `deliver`, …).
- `src/route.rs` / `src/leaf.rs` — route and leaf protocols, parsers and
  encoders.
- `src/jetstream/` — JetStream engine (in-memory, or file-persisted with `--js-store-dir`). API/ack handling runs
  under one lock; deliveries are collected as actions and sent after the
  lock is released (delivery re-enters the engine, so holding the lock
  across it would deadlock). Streams capture only messages published on
  the hosting node; `$JS.API` requests and acks route cluster-wide.
  Split in Phase 29 (pure move, no behaviour change, snapshot v2 format
  unchanged): `mod.rs` (facade + lock coordinator, shared types,
  helpers), `api.rs` (`$JS.API.>` router, stream API handlers, error
  codes), `stream.rs` (stream state, capture, dedup, retention,
  mirroring/sourcing), `consumer.rs` (push/pull state machines,
  `MSG.NEXT`, tick, redelivery loop), `storage.rs` (atomic JSON snapshot
  persistence). Public exports unchanged (`JetStream`, `handle_message`,
  `redelivery_loop`, `rfc3339`).
- `src/tls.rs` — rustls (ring) TLS server config for client connections.
- `src/monitor.rs` — hand-rolled HTTP/1.1 monitoring server (one request
  per connection, then close). Memory is real (`/varz` `mem` from
  `/proc/self/statm` on Linux); untracked fields report 0.
  Per-connection pending bytes are tracked (`/connz`
  `pending_bytes`), and `/varz` reports the configured
  `max_pending`, `ping_interval`, and `ping_max`.
- `src/conf.rs` — `.conf` parser (blocks, lists, quoted strings,
  comments, URLs intact).
- `src/config.rs` — CLI and CONNECT authorization.
- No unsafe code: `#![forbid(unsafe_code)]`.

## License

Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).
Copyright 2026 Heiko Koehler. Not affiliated with the NATS project, CNCF,
or Synadia.
