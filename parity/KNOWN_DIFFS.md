# Parity residue: rust-natsd vs Go nats-server v2.15.1

As of the parity-fix work (2026-10-24), re-verified 2026-10-25 at
HEAD `0cb8650` and again during Phase 31 (2026-10-25: 53 probes,
52 pass + 1 documented exception, 0 fail), **there are no
remaining known server diffs**. Every divergence in the previous edition of this file
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

Phase 31 update (2026-10-25): the gate was re-verified against the
nats.go v1.55.0 source before any feature code — it is ONLY the
version check (`kv.go:399`/`424`, via `serverMinVersion` on the
INFO `version` field). `api_lvl` gates nothing in nats.go: it is
only the INFO field and the `ConnectedServerJetStream()` accessor,
and async-nats 0.38 has no version gate and no api_lvl use at all
(its KV getter keys off the stream info's `allow_direct` flag).
Accordingly rust-natsd still reports 0.1.0 and still omits
`api_lvl`: advertising Go's api_lvl 5 would imply JS features
Rust does not implement. The server side is now complete for KV:
`DIRECT.GET` (seq / last / next-by-subject, with the Go error
shapes), `allow_direct` / `mirror_direct` / `deny_delete` /
`deny_purge` / `allow_rollup_hdrs`, `STREAM.UPDATE`, and
`STREAM.MSG.DELETE` are implemented and were probed identical to
Go v2.15.1 with a nats.go probe program (direct gets, deny codes
10057/10110, rollup sub/all/denied/bogus, update flipping
allow_direct on) — and async-nats KV helpers are verified end to
end in `tests/e2e_js_kv.rs` (put/get/delete/purge, revisions,
watch). One documented difference remains: Go answers a direct
get addressed to an origin from one of its mirrors (the
`Nats-Stream` header names the mirror) and answers nothing for a
mirror's own name, while Rust answers the requested stream
directly when servable, including a mirror_direct mirror's own
copies. The parity KV exception therefore stays: it is the
version gate, not a server gap, and 53/53 is not claimed.

## Known issues and deliberate deviations (not parity diffs)

These are server behaviours that differ from Go or are unfinished,
found by tests (not by parity probes) and recorded here so this file,
README, and PLAN agree. They do not fail the parity run, because the
parity probes do not cover them — that is a coverage statement, not
a claim that they match Go.

- **Deliberate deviations from Go** (verified against 2.15.1):
  mirror backfill synchronous (Go async, final state matches);
  mirror/source of a nonexistent stream rejected 404/10059 (Go
  accepts and leaves empty); unauthorized import drops the message
  (Go refuses to start); import reply mappings expire after 60 s
  (Phase 28C, pruned on insert/lookup); translated service requests
  carry `Nats-Request-Info: {"acc": ...}` without Go's `rtt` (no
  client RTT measurement); JWT `limits` enforced since Phase 28C
  (`data` as a cumulative inbound budget — a rust-natsd
  interpretation, Go does not visibly enforce it; a limits-less
  account JWT is unlimited here where Go rejects its first
  connection), `revocations` and `bearer_token` honored since Phase
  28C, `iat` >60 s future cap stricter than Go, `$SYS` not
  special-cased, Go-generated JWT → Rust not run (no `nsc`);
  cert-to-user mapping is client-connections only (route/leaf use
  CA verification only).

## Deliberately out of scope (unchanged, and identical to the
README Known issues list and PLAN's Future work list)

- Gateways (super-cluster): implemented in Phase 33 (design note:
  PLAN Phase 24) and verified Go↔Rust in both seed directions
  plus TLS via `scripts/e2e-go-gateway.sh`; `/gatewayz` now
  returns Go's `inbound/outbound_gateways` shape. The parity
  suite's no-gateway run still compares empty inbound/outbound
  maps. Known gateway deviations: JetStream API and stream-import
  interest are not advertised over gateways, and a gateway TLS
  listener without `verify` and no CA does not require a client
  certificate where Go's does (see PLAN Phase 33).
- JetStream clustering/replication: single-node only (design note:
  PLAN Phase 25). Mirroring/sourcing and imports/exports are
  implemented same-node and cross-node (Phase 32); the remaining
  cross-node limits are documented in PLAN Phase 32 (cross-node
  consumer deliveries to a foreign client, leaf imports outside the
  leaf connection's account, TLS/JWT loopback limits).
- JWT imports across routes/leafs (not separately verified).
- MQTT/WebSocket: not implemented in rust-natsd (see README
  non-goals); not probed. JWT operator mode, route/leaf TLS, and TLS
  client-certificate-to-user mapping are now implemented (see
  README/PLAN) and are no longer in this out-of-scope list.
- XKeys: not implemented.
- Byte-identical monitoring JSON: compared on supported fields only.
- Exact per-member queue distribution: compared on totals only (Go
  random-ish, Rust round-robin, both exactly once).
- Token with no credentials / bare-TCP connect: not reproduced as a
  diff — with CONNECT-without-credentials both servers answer `-ERR`
  auth and close (`token_none` passes). The 2026-10-24 reset was
  likely a bare-TCP connect, which this suite does not probe.
