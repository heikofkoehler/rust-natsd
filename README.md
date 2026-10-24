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
| TLS client connections | Implemented | `--tls/--tlscert/--tlskey/--tlsverify/--tlscacert`, `tls {}` in `.conf`, `tls_required` in `INFO`; STARTTLS order (plaintext `INFO` first, then upgrade), matching Go |
| Route/leaf TLS | Not implemented | Non-goal |
| TLS client-certificate-to-user mapping | Not implemented | Non-goal (CA verification only, via `--tlsverify`) |
| XKeys | Not implemented | Non-goal |
| Routes / clustering | Implemented | Full mesh, solicitation with retry, RS+/RS-, RMSG/HMSG, duplicate resolution |
| Leaf nodes | Implemented | Hub accept + solicited leaf, LS+/LS-, LMSG/HMSG, reconnect |
| Server-initiated PING / stale connection | Implemented | `--ping-interval` / `--ping-max` (Go defaults 2m/2); close with `-ERR 'Stale Connection'` |
| No-responders 503 | Implemented | HMSG 503 when the publisher set `no_responders` and a publish with a reply subject has no deliveries |
| Slow-consumer detection | Implemented | Bounded outbound queue (1024 frames) → disconnect. Note: this bound is low enough that a burst publisher trips it — see [Benchmarks](#benchmarks) |
| Graceful shutdown on SIGINT/SIGTERM | Implemented | Writer flushes pending frames (e.g. a final `-ERR`) before close |
| JetStream: streams, pull + push consumers | Implemented | In-memory, or file-persisted with `--js-store-dir` |
| JetStream: ack policies (+ACK/-NAK/+TERM/+WPI), redelivery, dedup, expected sequences, MSG.GET, purge/delete | Implemented | |
| JetStream: workqueue retention, `max_msgs_per_subject` | Implemented | |
| JetStream: deliver policies `all` / `last` (filter-aware) / `new` / `by_start_sequence` | Implemented | |
| JetStream persistence | Partial | Whole-engine JSON snapshot after every mutation, reloaded on startup; pending ack state is NOT persisted (see `jetstream.rs` scope notes) |
| JetStream clustering / replication | Not implemented | Single-node only; non-goal |
| JetStream mirroring / sourcing | Not implemented | Non-goal |
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
mirroring/sourcing, MQTT/WebSocket gateways, JWT operator mode,
route/leaf TLS, TLS certificate-to-user mapping, and XKeys. JetStream
here is single-node only. Conversely, rust-natsd is a superset in
exactly one place: it implements `/subz` and `/accounz`, which Go
2.15.1 answers with 404.

**Maturity and operations — not claimed production-ready.** This is
a from-scratch reimplementation, not a hardened production server:
no JetStream clustering (so no JS high availability), no
production-hardening claims, and one benchmark-exposed limitation
(see below): the bounded outbound queue (1024 frames) drops a
subscriber as a slow consumer under a burst publisher, where Go
absorbs the same burst. Do not run this as a drop-in replacement for
mainline in production without weighing that list.

**Architecture.** Tokio async runtime, a hand-rolled incremental
protocol parser (no I/O in the parser, which is what makes
split-at-every-byte tests possible), one reader + one writer task per
connection joined by a bounded channel, and `#![forbid(unsafe_code)]`
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

**Results — 2026-10-24.** Go `nats-server` v2.15.1 vs rust-natsd
server code at commit `e752c9c` (the parity-green HEAD at benchmark
time; the bench harness is new in this commit and does not change
server behaviour). Machine:
Linux x86_64, AMD EPYC 9D64, 2 vCPUs, Go 1.27.3, rustc 1.98.1.

| Scenario | Go nats-server v2.15.1 (median [range]) | rust-natsd (median [range]) |
|---|---|---|
| Core pub/sub, 256 B, 200k msgs (msgs/s) | 639,635 [586,290..686,963] | **FAILED** — see below |
| Core pub/sub, 1 KiB, 200k msgs (msgs/s) | 295,161 [286,663..302,366] | **FAILED** — see below |
| Request/reply, 256 B, 20k requests (mean / p50 / p99 µs) | 300 / 274 / 860 | 195 / 181 / 434 |
| JetStream publish with ack, 1 KiB, 20k msgs (msgs/s) | 5,257 [5,199..5,535] | 6,274 [6,256..6,567] |

**Read it plainly.** On request/reply latency and JetStream publish
throughput, rust-natsd is faster in this run (lower latency, higher
msgs/s) — that is a localhost, single-client, uncontended result, not
a general performance claim. On core pub/sub throughput, rust-natsd
does not finish at all: in all 6 attempts per full bench run (3 runs
× 2 sizes), the subscriber was dropped as a slow consumer after
only 1–500 of 200,000 deliveries reached the client, and the run
timed out. The cause is confirmed independently of the benchmark
client: a raw fast-reading subscriber sees the same drop, the server
log prints `slow consumer, dropping connection`, and `/varz`
reports `slow_consumers: 1`. The mechanism is the 1024-frame bounded
outbound queue in `src/server.rs` — a burst publisher outruns the
per-connection writer, the queue fills, and the subscriber is
disconnected, by design but with a bound far below what Go absorbs
at these message counts. That is a real limitation, not a benchmark
artifact, and it is **not fixed** as part of this README/benchmark
work (server behaviour is out of scope here); it needs a separate
decision (larger/dynamic pending limits, or backpressure instead of
drop) before any pub/sub throughput claim can be made for rust-natsd.

**Repeatability.** A second full `bench.sh` run the same day
reproduced the pattern: Go pub/sub 624,700 [402,795..652,805] (256 B)
and 316,907 [279,466..318,750] (1 KiB) msgs/s; Go request/reply mean
312 µs, p50 278 µs, p99 1,059 µs; Go JetStream 5,382
[5,174..5,497] msgs/s; Rust request/reply mean 198 µs, p50 177 µs,
p99 558 µs; Rust JetStream 6,547 [6,337..6,642] msgs/s; Rust pub/sub
failed again in all 6 attempts. No stray servers were left running
after either run (verified by process and port check).

## Tests

```sh
cargo test                    # 98 Rust tests (unit + integration)
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
  short lock, writes to sockets happen outside it.
- `src/server.rs` — Tokio listener; one reader task and one writer task per
  connection connected by a bounded channel (the slow-consumer bound). The
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
  untracked ones (memory, RTT, pending bytes) report 0.
- `src/conf.rs` — `.conf` parser (blocks, lists, quoted strings,
  comments, URLs intact).
- `src/config.rs` — CLI and CONNECT authorization.
- No unsafe code: `#![forbid(unsafe_code)]`.

## License

Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).
Copyright 2026 Heiko Koehler. Not affiliated with the NATS project, CNCF,
or Synadia.
