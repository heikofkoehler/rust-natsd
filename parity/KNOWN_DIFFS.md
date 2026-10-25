# Parity residue: rust-natsd vs Go nats-server v2.15.1

As of the parity-fix work (2026-10-24), **there are no remaining known
server diffs**. Every divergence in the previous edition of this file
is fixed and passes in both default and `--strict` mode:

| Former diff | Fix |
|---|---|
| Subject validation (lax Go vs strict Rust) | Go rules implemented: literal publish subjects, pedantic `-ERR` + delivery, empty-token subjects route nowhere, SUB errors non-fatal, partial wildcards literal |
| Pedantic publish still delivers | Implemented (error + `+OK` + delivery, in that order) |
| Verbose `+OK` ordering | `+OK` sent before self-delivery |
| Duplicate sid replace | Keep-first in `Sublist::add`; duplicate SUB not propagated |
| No-responders 503 | HMSG 503 emitted in `deliver()` when publisher has `no_responders` and no deliveries |
| Server PING / stale connection | Ping task in `handle_conn`; `--ping-interval` / `--ping-max` CLI flags and `.conf` keys (Go defaults 2m/2); `/varz` reports configured values |
| Token as password | Rejected (token only via `auth_token`); the earlier deliberate deviation is removed |
| Malformed CONNECT serde leak | Closes silently, no `-ERR` |
| Error-text suffixes (`: FOO`, `: 65`) | Go's exact generic texts; `unknown_op` and `max_payload` probes now compare exact text |
| INFO sparseness | Go omitempty semantics, plus `client_ip` and `cluster` (when explicitly configured); `nonce` only when auth is configured |
| TLS handshake order (implicit vs STARTTLS) | INFO in plaintext first, then TLS upgrade; direct TLS fails on both, plain client closed silently on both |
| Empty-stream FirstSeq 0 vs 1 | `first_seq()` returns 0 for an empty stream |
| `/subz` / `/accounz` superset | Reclassified as a pass (see below), not a diff |
| `tlsverify` refusal layer | Reclassified as a pass (see below), not a diff |
| Raw `$KV` traffic | Probed directly (`kv_raw` case): create/put/MSG.GET last_by_subj/watch all match |

The suite run is GREEN in both modes: 53 raw probes, 52 pass +
1 exception (below), 0 fail; JS full PASS; JS persistence PASS;
cluster/leaf smoke PASS. The 2026-10-24 side-by-side report
(`your_files/nats-comparison-2026-10-24.md`) is superseded by this
file where they disagree.

## Documented exceptions (not server failures)

These appear explicitly in the suite output as `exception` /
`DOCUMENTED EXCEPTION` in both default and strict mode. They are never
silently ignored, and they do not fail the run.

### INFO fields Rust cannot honestly report (`info_fields` exception)

Go INFO carries four fields Rust deliberately omits:

- `git_commit` — a Go build fact (Go's commit hash). Rust has no
  equivalent value to report honestly.
- `go` — the Go toolchain version the Go binary was built with.
- `xkey` — Go's server XKey (X25519 public key for encrypted auth
  callouts). Rust has no XKey support.
- `api_lvl` — Go's **JetStream API level** (verified: `JSApiLevel`,
  the JetStream API version advertised for feature negotiation;
  nats-server 2.15.1 reports 5, and clients gate JS features on it,
  e.g. fast ingest needs `api_lvl >= 4`). Rust's JetStream is a subset
  of Go's, so advertising Go's 5 would mislead clients into using JS
  features Rust does not implement. It is omitted rather than faked.

All other INFO fields match: shared values (proto, headers,
max_payload) and presence (auth_required, tls_required, jetstream,
nonce, client_ip, cluster) are compared exactly.

### Monitoring superset (pass, not a diff)

Go nats-server v2.15.1 answers `/subz` and `/accounz` with
`404 page not found`; Rust implements both. A superset endpoint is a
pass in the suite: for every endpoint Go implements, the values must
match; for endpoints Go 404s, Rust may be 200. Field-name differences
that are only naming (`/leafz` count vs list shape, `/gatewayz`
`gateways` vs `inbound/outbound_gateways`) are normalised in the
probe, not ignored — the counts and emptiness are compared.

### TLS refusal-layer variance (pass, not a diff)

- `tlsverify_no_cert`: both servers refuse a client without a
  certificate. The refusal point varies run to run on Go (handshake
  failure vs post-CONNECT EOF) and is consistently a handshake failure
  on Rust. Refusal is the behavioural outcome; the layer is not.
- `tls_direct_fails`: both servers send plaintext INFO first, so a
  direct (implicit) TLS wrap fails on the client side on both.

### KV via nats.go: a client limitation, not a server failure

`nats.go` gates key-value on server version >= 2.6.2. rust-natsd
honestly reports its own crate version (0.1.0) in INFO, so
`CreateKeyValue` fails client-side with
`nats: key-value requires at least server version 2.6.2` before any
KV traffic happens. Faking a Go version to clear the gate would be
dishonest version reporting and is explicitly not done. Raw `$KV`
traffic bypassing the gate is probed directly (`kv_raw` case in
`parity/probes.py`: stream create, puts, `MSG.GET last_by_subj`,
watch via `$KV.>`) and matches Go. KV is therefore usable through any
client that does not apply nats.go's version gate, and will become
usable through nats.go itself when rust-natsd's own version reaches
2.6.2.

## Deliberately out of scope (unchanged)

- Gateways (super-cluster): Rust returns an empty `/gatewayz`; no
  gateway protocol is implemented (design note: PLAN Phase 24).
- JetStream clustering/replication: single-node only (design note:
  PLAN Phase 25). Mirroring/sourcing are implemented same-node only;
  cross-node mirroring/sourcing remain out of scope.
- MQTT/WebSocket: not implemented in rust-natsd (see README
  non-goals); not probed. JWT operator mode, route/leaf TLS, and TLS
  client-certificate-to-user mapping are now implemented (see
  README/PLAN) and are no longer in this out-of-scope list.
- Byte-identical monitoring JSON: compared on supported fields only.
- Exact per-member queue distribution: compared on totals only (Go
  random-ish, Rust round-robin, both exactly once).
- Token with no credentials / bare-TCP connect: not reproduced as a
  diff — with CONNECT-without-credentials both servers answer `-ERR`
  auth and close (`token_none` passes). The 2026-10-24 reset was
  likely a bare-TCP connect, which this suite does not probe.
