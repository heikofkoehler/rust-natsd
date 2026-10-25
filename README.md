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
| JWT operator mode | Not implemented | Non-goal |
| Multi-account + pub/sub allow/deny | Implemented | `accounts {}` in `.conf` (per-user `permissions` or account-level `authorization`), `system_account`, `no_auth_user`; subscriptions/deliveries/routes/leafs/JetStream scoped per account; Go's exact permission-violation `-ERR` text, connection stays alive |
| Account imports/exports (service + stream) | Implemented (same node) | `accounts { NAME { exports: [{service\|stream: "subj", accounts: ["B"]}], imports: [{service\|stream: {account: A, subject: "subj"}, to: "local"}] } }` — Go v2.15.1 `.conf` shape probed (`to`, not `local_subject`; the older spelling is accepted as an alias). Service requests are rewritten into the exporting account with a synthetic `_RIMPORT.<seq>` reply subject that routes replies back; stream publishes are copied to the importer's local subject. Unauthorized imports (account not listed, remote pattern not covered by the export) are dropped, never delivered; the importing account's publish allow/deny is checked on the local subject and the responder's subscribe allow/deny on the exported subject. Same-node only: imports do not cross routes/leafs, and route/leaf-origin messages do not fan out to stream importers. Reply mappings are retained for the server lifetime (Go's `_R_.` mappings expire by TTL) |
| TLS client connections | Implemented | `--tls/--tlscert/--tlskey/--tlsverify/--tlscacert`, `tls {}` in `.conf`, `tls_required` in `INFO`; STARTTLS order (plaintext `INFO` first, then upgrade), matching Go |
| Route/leaf TLS | Not implemented | Non-goal |
| TLS client-certificate-to-user mapping | Not implemented | Non-goal (CA verification only, via `--tlsverify`) |
| XKeys | Not implemented | Non-goal |
| Routes / clustering | Implemented | Full mesh, solicitation with retry, RS+/RS-, RMSG/HMSG, duplicate resolution |
| Leaf nodes | Implemented | Hub accept + solicited leaf, LS+/LS-, LMSG/HMSG, reconnect |
| Server-initiated PING / stale connection | Implemented | `--ping-interval` / `--ping-max` (Go defaults 2m/2); close with `-ERR 'Stale Connection'` |
| No-responders 503 | Implemented | HMSG 503 when the publisher set `no_responders` and a publish with a reply subject has no deliveries |
| Slow-consumer detection | Implemented | Byte-based pending limit (default 64 MiB, `--max-pending` / .conf `max_pending`, reported in `/varz` as `max_pending` and per-connection in `/connz` as `pending_bytes`) → disconnect when exceeded. A fast reader at bench rates is not dropped; a never-reading reader is, like Go |
| Graceful shutdown on SIGINT/SIGTERM | Implemented | Writer flushes pending frames (e.g. a final `-ERR`) before close |
| JetStream: streams, pull + push consumers | Implemented | In-memory, or file-persisted with `--js-store-dir` |
| JetStream: ack policies (+ACK/-NAK/+TERM/+WPI), redelivery, dedup, expected sequences, MSG.GET, purge/delete | Implemented | |
| JetStream: workqueue retention, `max_msgs_per_subject` | Implemented | |
| JetStream: deliver policies `all` / `last` (filter-aware) / `new` / `by_start_sequence` | Implemented | |
| JetStream persistence | Implemented | Whole-engine JSON snapshot after every mutation, reloaded on startup — including per-consumer pending ack state (in-flight stream seqs, delivery counts, wall-clock delivery times), ack floors, redelivery counts, and pull redelivery queues (snapshot v2; v1 snapshots load with empty ack state). Restart preserves the remaining ack window (Go v2.15.1 semantics, probed): unacked messages redeliver once the window expires with delivered count incremented, acked messages never redeliver, push redelivery defers while the deliver subject has no interest; waiting pulls are not persisted (dead reply inbox) |
| JetStream clustering / replication | Not implemented | Single-node only; non-goal |
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
| Monitoring `/gatewayz` | Partial | Empty response only; no gateway protocol is implemented |
| Monitoring `/healthz` | Implemented | |
| Config files (`.conf`) | Partial | nats-server `.conf` subset via `-c` (CLI wins over file); unknown keys ignored |
| `INFO` completeness | Partial | Omits Go-only fields by design: `git_commit`, `go`, `xkey`, and `api_lvl` (Go's JetStream API level — advertising 5 would gate clients into JS features Rust does not implement). All other fields match Go, including omitempty semantics |
| Gateways (super-cluster) | Not implemented | Non-goal |
| MQTT / WebSocket gateways | Not implemented | Non-goal |

See [PLAN.md](PLAN.md) for the phased plan and design notes.

## Comparison with mainline (Go) implementation

Mainline here is Go `nats-server` **v2.15.1**, the reference
implementation. The comparison below is deliberately narrow: only
claims traceable to code or to a suite/benchmark result are made.

**Protocol behaviour — at parity on probed behaviour.** The repeatable
suite in `scripts/parity.sh` starts both servers side by side with
matching configs and runs the same raw-wire and Go-client probes
against both: **53 probes, 52 pass + 1 documented exception, 0 fail,
in both default and `--strict` mode** (last verified 2026-10-24).
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
Gateways (super-cluster), JetStream clustering/replication,
cross-node mirroring/sourcing, MQTT/WebSocket gateways, JWT operator mode,
route/leaf TLS, TLS certificate-to-user mapping, and XKeys. JetStream
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

**Results — 2026-10-24 (post slow-consumer fix).** Go `nats-server`
v2.15.1 vs rust-natsd server code at the fix commit (the HEAD at
benchmark time; the bench harness does not change server behaviour).
Machine:
Linux x86_64, AMD EPYC 9D64, 2 vCPUs, Go 1.27.3, rustc 1.98.1.

| Scenario | Go nats-server v2.15.1 (median [range]) | rust-natsd (median [range]) |
|---|---|---|
| Core pub/sub, 256 B, 200k msgs (msgs/s) | 497,531 [411,559..636,226] | 418,910 [359,904..431,465] |
| Core pub/sub, 1 KiB, 200k msgs (msgs/s) | 282,695 [244,584..306,766] | 226,910 [222,978..227,694] |
| Request/reply, 256 B, 20k requests (mean / p50 / p99 µs) | 320 / 293 / 1,001 | 196 / 180 / 537 |
| JetStream publish with ack, 1 KiB, 20k msgs (msgs/s) | 5,249 [5,096..5,558] | 8,019 [7,941..8,053] |

**Read it plainly.** All four scenarios now complete on both
servers. On core pub/sub throughput rust-natsd is ~15–20% behind Go
in this run (418,910 vs 497,531 msgs/s at 256 B; 226,910 vs 282,695
at 1 KiB) — the gap is writer/parse throughput, not drops. On
request/reply latency and JetStream publish throughput, rust-natsd
is faster here (lower latency, higher msgs/s) — that is a localhost,
single-client, uncontended result, not a general performance claim,
and the JetStream gain vs the pre-fix run (6,274 → 8,019 msgs/s)
tracks the batched writer, since JS acks and deliveries go through
the same outbound path. Request/reply is unchanged vs the pre-fix
run (mean 195 → 196 µs): no regression from the fix. For history:
before the fix, rust-natsd pub/sub **failed** in all attempts
(subscriber dropped as a slow consumer after 1–500 of 200,000
deliveries) because of the old 1024-frame outbound queue; that
failure mode is gone, and a never-reading subscriber is still
dropped at the 64 MiB byte limit (see PLAN Phase 18).

**Repeatability.** No stray servers were left running after the run
(verified by process and port check).

## Tests

```sh
cargo test                    # 100 Rust tests (unit + integration)
bash scripts/e2e-go.sh        # Go client core e2e
bash scripts/e2e-go-cluster.sh  # Go <-> Rust route interop, both seed directions
bash scripts/e2e-go-leaf.sh     # Go <-> Rust leaf interop, both directions
bash scripts/e2e-go-js.sh       # Go JetStream interop (nats.go JS client)
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
- `src/server.rs` — Tokio listener; one reader task and one writer task per
  connection connected by a byte-limited queue (default 64 MiB pending,
  the slow-consumer bound; the writer coalesces queued frames into
  64 KiB batched socket writes). The
  writer holds no channel sender, so closing a connection flushes pending
  frames (like a final `-ERR`) before the socket closes. Global queue
  selection across local members, routes, and leafs lives in `deliver()`.
- `src/route.rs` / `src/leaf.rs` — route and leaf protocols, parsers and
  encoders.
- `src/jetstream.rs` — JetStream engine (in-memory, or file-persisted with `--js-store-dir`). API/ack handling runs
  under one lock; deliveries are collected as actions and sent after the
  lock is released (delivery re-enters the engine, so holding the lock
  across it would deadlock). Streams capture only messages published on
  the hosting node; `$JS.API` requests and acks route cluster-wide.
- `src/tls.rs` — rustls (ring) TLS server config for client connections.
- `src/monitor.rs` — hand-rolled HTTP/1.1 monitoring server (one request
  per connection, then close). Only tracked fields are filled in;
  untracked ones (memory, RTT) report 0. Per-connection pending bytes
  are tracked (`/connz` `pending_bytes`).
- `src/conf.rs` — `.conf` parser (blocks, lists, quoted strings,
  comments, URLs intact).
- `src/config.rs` — CLI and CONNECT authorization.
- No unsafe code: `#![forbid(unsafe_code)]`.

## License

Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).
Copyright 2026 Heiko Koehler. Not affiliated with the NATS project, CNCF,
or Synadia.
