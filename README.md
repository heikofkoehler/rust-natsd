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

## Protocol support

| Feature | Status |
|---|---|
| `INFO` / `CONNECT` handshake | ✅ (incl. clients sending explicit JSON nulls) |
| `PUB` / `SUB` / `UNSUB` | ✅ |
| Wildcards `*` (one token), `>` (terminal, 1+ tokens) | ✅ |
| Queue groups (one-of delivery, round-robin spread) | ✅ |
| Queue groups spanning routes/leafs (global selection, exactly once) | ✅ |
| Request/reply (reply subjects, no-responder is client-side) | ✅ |
| Headers: `HPUB` → `HMSG` for header-capable clients, stripped `MSG` otherwise | ✅ |
| Auto-unsubscribe (`UNSUB sid <max>`) | ✅ |
| `verbose` mode (`+OK`) | ✅ |
| `echo: false` (no self-delivery) | ✅ |
| Auth: `--user/--pass`, `--auth-token` (token in `auth_token` or as password) | ✅ |
| TLS client connections: `--tls/--tlscert/--tlskey/--tlsverify/--tlscacert`, `tls {}` in `.conf`, `tls_required` in `INFO` | ✅ (client port only; route/leaf TLS is a non-goal) |
| NKey auth: per-connection `nonce` in `INFO`, `nkey`/`sig` in `CONNECT` verified with the `nkeys` crate | ✅ (standalone nkey users; JWT operator mode is a non-goal) |
| Multi-account: `accounts {}` in `.conf` (per-user `permissions` or account-level `authorization`), `system_account`, `no_auth_user`; subscriptions/deliveries/routes/leafs/JetStream scoped per account; Go's exact permission-violation `-ERR` text, connection stays alive | ✅ |
| JetStream: streams, pull + push consumers, acks (+ACK/-NAK/+TERM/+WPI), redelivery, dedup, expected sequences, MSG.GET, purge/delete | ✅ (in-memory, or file-persisted with `--js-store-dir`) |
| JetStream persistence: whole-engine JSON snapshot after every mutation, reloaded on startup | ✅ (pending ack state is NOT persisted; see jetstream.rs scope notes) |
| JetStream workqueue retention, `max_msgs_per_subject`, deliver_policy `all`/`last` (filter-aware)/`new`/`by_start_sequence` | ✅ |
| Monitoring HTTP: `/varz`, `/connz`, `/subz`, `/routez`, `/leafz`, `/jsz`, `/healthz` | ✅ |
| Monitoring HTTP: `/accounz`, `/gatewayz` (empty); `/connz` `acc`/`state`/`limit`/`offset` filters and `account`/`user`/`tls` per connection; `/varz` real `http_port`, `mem`, `slow_consumers` | ✅ |
| Config files: nats-server `.conf` subset via `-c` (CLI wins over file) | ✅ |
| Gateways, JetStream clustering/replication | ❌ non-goals |
| MQTT / WebSocket gateways | ❌ non-goals |
| Max payload enforcement (`-ERR 'Maximum Payload Violation'`) | ✅ |
| Protocol errors → `-ERR` + connection close | ✅ |
| Slow-consumer detection (bounded outbound queue → disconnect) | ✅ |
| Graceful shutdown on SIGINT/SIGTERM | ✅ |
| Routes: full mesh, solicitation with retry, RS+/RS-, RMSG/HMSG, duplicate resolution | ✅ |
| Leaf nodes: hub accept + solicited leaf, LS+/LS-, LMSG/HMSG, reconnect | ✅ |
| JetStream `$JS.API` routing across routes/leafs (`$JS.API.>` interest) | ✅ |
| KV / object store as client helpers | ❌ (primitives exist: `KV_<bucket>` streams, `last_by_subj`; the nats.go/async-nats KV helpers are not verified) |

See [PLAN.md](PLAN.md) for the phased plan and design notes.

## Tests

```sh
cargo test                    # 85 Rust tests (unit + integration)
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

Reading the table: each case is `pass` (identical semantics), `known`
(diverges exactly as recorded in `parity/KNOWN_DIFFS.md`), `fail` (a new
divergence), or `harness_error` (the probe itself broke). Default mode
is green with only known diffs; strict mode fails on known diffs too.
Exit codes: 0 green, 1 divergence, 2 setup failure.

Layout: `scripts/parity.sh` (orchestration, servers, certs, JS/KV,
persistence, smoke), `parity/probes.py` (raw-wire probes + comparison),
`parity/go_client/` (nats.go probe client: JetStream + KV, and the
nkeytool signer), `parity/KNOWN_DIFFS.md` (every accepted divergence,
classified intentional vs accepted gap, with fix sketches).

Adding a case: write a probe function in `parity/probes.py` returning
`{"go": …, "rust": …}` semantic outcomes, register it in `CASES`, and
run the suite. If it finds a real server bug, record it as a candidate
fix in `parity/KNOWN_DIFFS.md` — do not change `src/` to make the
suite pass. Updating KNOWN_DIFFS: add the entry (class, severity,
reason or fix sketch) and a shape check in `known_shape_ok` so the case
only counts as known when it diverges in the recorded shape.

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
