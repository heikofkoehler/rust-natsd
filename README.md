# rust-natsd

A NATS server implemented in Rust, from scratch — implementing the
**Core NATS pub-sub protocol** (the client protocol of the Go `nats-server`,
formerly `gnatsd`).

Unmodified official clients work against it today:

- **Go** (`nats.go` v1.55.0) — verified by `e2e/go` (33 checks)
- **Rust** (`async-nats` 0.38) — verified by `tests/e2e_rust_client.rs`

## Quick start

```sh
cargo run --release            # listens on 0.0.0.0:4222
cargo run --release -- -p 4223 --name edge-1
cargo run --release -- --user derek --pass s3cret
cargo run --release -- --auth-token mytoken
```

Then connect with any NATS client, e.g. the `nats` CLI:

```sh
nats --server nats://127.0.0.1:4222 sub 'events.>'
nats --server nats://127.0.0.1:4222 pub events.click 'hello'
```

## Protocol support

| Feature | Status |
|---|---|
| `INFO` / `CONNECT` handshake | ✅ (incl. clients sending explicit JSON nulls) |
| `PUB` / `SUB` / `UNSUB` | ✅ |
| Wildcards `*` (one token), `>` (terminal, 1+ tokens) | ✅ |
| Queue groups (one-of delivery, round-robin spread) | ✅ |
| Request/reply (reply subjects, no-responder is client-side) | ✅ |
| Headers: `HPUB` → `HMSG` for header-capable clients, stripped `MSG` otherwise | ✅ |
| Auto-unsubscribe (`UNSUB sid <max>`) | ✅ |
| `verbose` mode (`+OK`) | ✅ |
| `echo: false` (no self-delivery) | ✅ |
| Auth: `--user/--pass`, `--auth-token` (token in `auth_token` or as password) | ✅ |
| Max payload enforcement (`-ERR 'Maximum Payload Violation'`) | ✅ |
| Protocol errors → `-ERR` + connection close | ✅ |
| Slow-consumer detection (bounded outbound queue → disconnect) | ✅ |
| Graceful shutdown on SIGINT/SIGTERM | ✅ |
| Clustering / routes, gateways, leaf nodes | ❌ future phase |
| JetStream, KV, object store | ❌ future phase |
| MQTT / WebSocket gateways, monitoring endpoint, `.conf` files | ❌ future phase |

See [PLAN.md](PLAN.md) for the phased plan and design notes.

## Tests

```sh
cargo test                    # 31 unit + 12 raw-protocol e2e + 7 async-nats e2e
bash scripts/e2e-go.sh        # Go client e2e, 33 checks (needs Go toolchain)
cargo llvm-cov --all-targets  # optional coverage report
```

Test layers, by design:

1. **Unit** — parser fed split at every byte boundary, subject-matching truth
   tables, sublist queue/auto-unsub accounting, auth matrix, CLI parsing.
2. **Raw-protocol e2e** — literal wire bytes over TCP, including error and
   edge behavior no client library would generate (bad ops, oversized
   payloads, `UNSUB` max races resolved via PING/PONG sync).
3. **Real-client e2e** — the official Go and Rust clients, unmodified. These
   caught real bugs: e.g. `async-nats` sends explicit `null` for CONNECT
   string fields, which the parser originally rejected.

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
  frames (like a final `-ERR`) before the socket closes.
- `src/config.rs` — CLI and CONNECT authorization.
- No unsafe code: `#![forbid(unsafe_code)]`.

## License

Apache-2.0. Not affiliated with the NATS project, CNCF, or Synadia.
