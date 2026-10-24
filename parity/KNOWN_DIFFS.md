# Known differences: rust-natsd vs Go nats-server v2.15.1

This file records every divergence `scripts/parity.sh` currently finds,
classified as **intentional** (a deliberate rust-natsd choice) or
**accepted gap** (a real difference, deferred with a fix sketch). The
suite's default run is GREEN with exactly these diffs; any other
divergence fails the run. `--strict` (or `PARITY_STRICT=1`) fails on
known diffs too.

Each entry is shape-checked by the suite (`parity/probes.py`
`known_shape_ok`, or the KV branch in `scripts/parity.sh`): a case only
counts as "known" when it diverges in the recorded shape. A different
divergence in the same case is a new failure.

Re-verified against Go nats-server v2.15.1 and rust-natsd HEAD on
2026-10-24. The 2026-10-24 side-by-side report
(`your_files/nats-comparison-2026-10-24.md`) is superseded by this file
where they disagree; its diff list is re-verified case by case below.

## Intentional

### token_as_pass — Rust accepts the token as a password
Go rejects a connection that presents the server token in the `pass`
field; Rust accepts it. Deliberate (documented in README): the token is
a shared secret either way, and accepting it in both fields is more
forgiving for hand-rolled clients. Severity: low. No fix planned.

## Accepted gaps

### info_fields — Rust INFO is sparser (low)
Rust INFO carries the shared values (proto, headers, max_payload,
auth_required, tls_required, nonce, jetstream) but lacks Go's
informational fields: `client_ip`, `api_lvl`, `git_commit`, `go`,
`xkey` (and `cluster` when clustered). Fix: add the missing fields
to Rust's INFO builder. Low severity: no client behaviour observed to
depend on them except the version gate below.

### kv_version_gate — nats.go refuses KV against Rust (medium)
`nats.go` gates key-value on server version >= 2.6.2. rust-natsd
reports its own crate version (0.1.0) in INFO, so `CreateKeyValue`
fails client-side with `nats: key-value requires at least server
version 2.6.2` before any KV traffic happens. The Go side of the suite
passes KV; the Rust side fails with exactly that error. This is NOT
evidence that Rust's `$KV` JetStream layer works or fails — the client
never tries. Fix options: (a) report a version that clears the gate
(misleading — do not fake a Go version); (b) implement KV fully and
accept that nats.go still gates it until rust-natsd's own version
reaches 2.6.2; (c) test raw `$KV` API traffic directly (future suite
work). Medium severity: KV is unusable through the official Go client
today.

### js_stream_first_seq — empty stream FirstSeq 0 vs 1 (low)
The `AddStream` response for a brand-new empty stream reports
`State.FirstSeq` 0 on Go ("no first message") and 1 on Rust ("next
sequence"). Once a message exists both report 1. Fix: report FirstSeq
0 when the stream holds no messages. Low severity: only the empty
state differs.

### verbose_order — +OK before vs after self-delivery (low)
With `verbose: true`, Go acks a PUB (+OK) before delivering it back to
the publisher; Rust delivers first, then acks. Same frames, different
order. Fix: ack before delivery in Rust's publish path. Low severity.

### Subject validation — Go is lax, Rust is strict (medium)
Go's non-pedantic mode accepts subjects Rust rejects, and pedantic
mode often errors without closing. Cases:

- **pub_wildcard_nonpedantic**: PUB `foo.*` — Go treats it as the
  literal subject `foo.*` (delivered to `>` and `foo.*` subs, not
  `foo.bar`); Rust sends `-ERR Invalid Subject` and closes.
- **pub_wildcard_pedantic**: PUB `foo.*` with `pedantic: true` — Go
  sends `-ERR 'Invalid Publish Subject'` but STILL delivers and keeps
  the connection; Rust errors, closes, no delivery.
- **pub_empty_token**: PUB `foo..bar` — Go accepts it with no error
  but does NOT deliver it to a `>` subscription (the empty token
  breaks routing); Rust rejects and closes.
- **sub_gt_not_last**: SUB `foo.>.bar` — Go sends `-ERR 'Invalid
  Subject'` and keeps the connection; Rust sends `-ERR` and closes.
- **sub_partial_wildcard**: SUB `foo*` — Go accepts it as a literal
  token; Rust rejects and closes.
- **sub_foostar_literal**: consequence — Go's `foo*` subscription
  matches only a literal `foo*` publish; Rust has no subscription.

Fix sketch: validate subjects Go-style (error class + keep-alive
semantics per pedantic mode) instead of close-on-invalid. Medium
severity: strictness breaks Go-tolerant clients that publish odd
subjects, but no well-behaved client is affected.

### duplicate_sid — Go keeps the first subscription (medium)
Reusing a sid: Go keeps the original subscription, Rust replaces it.
Fix: keep-first in the subscription table (`sublist.add` /
connection subscription map). Medium severity: affects clients that
accidentally reuse sids.

### malformed_connect — Rust leaks a serde error (low)
`CONNECT notjson`: Go closes silently; Rust sends
`-ERR 'Invalid CONNECT: …'` containing a serde_json message (internal
detail on the wire). Fix: send a generic `-ERR 'Invalid CONNECT'` (or
close silently like Go). Low severity.

### no_responders_true — Rust drops requests silently (medium)
With `no_responders: true`, Go answers a request with no subscribers
via HMSG `NATS/1.0 503` (`Nats-Subject: <request>`); Rust silently
drops it. Fix: emit the 503 in `deliver()` when the publisher has
no_responders and there are no recipients. Medium severity: request/
reply clients hang instead of failing fast.

### server_ping_silent — Rust never sends server PINGs (medium)
Go with `ping_interval 200ms, ping_max 2` closes a silent client after
~0.6s with `-ERR 'Stale Connection'`; Rust never pings, so dead
connections linger. Fix: a ping timer in `handle_conn` with the same
interval/max semantics. Medium severity: resource hygiene, not
correctness for live clients.

### tls_plain_fails — TLS handshake order differs (medium)
Go sends INFO (`tls_required: true`) in plaintext, then upgrades to
TLS (STARTTLS-style); Rust does implicit TLS (handshake before INFO).
Both refuse a plain client, but a client that wraps immediately in TLS
works only against Rust, and a client that upgrades after INFO works
against both (nats.go's flow). Fix: send INFO with `tls_required`
before the TLS handshake, matching Go. Medium severity: real-client
interop — verify with nats.go `Secure()` against a Rust TLS port
before closing this gap.

### tlsverify_no_cert — refusal at different layers (low)
Both refuse a client with no certificate under `tlsverify`, but Rust
consistently fails the handshake itself (client sees SSLError), while
Go's refusal point varies run to run: sometimes the handshake fails
(client sees SSLEOFError), sometimes it completes and Go closes after
CONNECT (client sees EOF). Same refusal, different observable point;
the suite accepts both Go shapes. (This variance was caught by repeat
runs — a single run would have recorded only one shape.) Fix: none
needed unless a client distinguishes the two. Low severity.

### monitoring_main / monitoring_accounts — Go 2.15.1 lacks endpoints (low)
Go nats-server v2.15.1 answers `/subz` and `/accounz` with
`404 page not found`; Rust implements both (a superset, not a gap in
Rust). Every endpoint Go does implement compares equal on the fields
both claim (`/varz`, `/connz`, `/subz`-n/a, `/routez`, `/leafz`,
`/jsz`, `/gatewayz`, `/healthz`; `/connz` entry field names differ —
Go has no `account`/`user`/`tls` keys, Rust adds them — recorded under
`_reported`, not compared). Also normalised: Go `/leafz` omits
`num_leafnodes` when empty (derived from the list); Go `/gatewayz`
reports `inbound/outbound_gateways` where Rust reports `gateways`.
Fix: none (Rust already covers Go's surface). Low severity.

## Re-verified 2026-10-24 report diffs: still present or fixed

| 2026-10-24 diff | Status now |
|---|---|
| Go lax subjects (non-pedantic literal, pedantic deliver-anyway) | Still present (see Subject validation) |
| Invalid SUB subject: Go errors + alive, Rust closes | Still present (`sub_gt_not_last`) |
| Duplicate sid: Go keep-first, Rust replace | Still present |
| No-responders 503 only in Go | Still present |
| Server PING / stale connection only in Go | Still present |
| Token as password accepted only by Rust | Still present, intentional |
| Token with no credentials: Go silent reset, Rust `-ERR` + graceful close | Not reproduced: with CONNECT-without-credentials both servers answer `-ERR` auth and close (`token_none` passes). The 2026-10-24 reset was likely a bare-TCP connect, which this suite does not probe. |
| Malformed CONNECT: Go silent, Rust serde detail | Still present |
| Unknown op error text (`: FOO` suffix) | Still present in Rust's bytes; suite normalises `-ERR` to a class, so it passes — see note |
| Max-payload error text (`: 65` suffix) | Same: still present in Rust's bytes, classed not failed |
| Verbose +OK ordering | Still present |
| Queue distribution (Go random-ish, Rust round-robin) | Still differs by design; suite compares totals only (pass) |
| INFO richness gap | Still present, narrowed to presence differences |

Note on error-text suffixes: Rust still appends `": detail"` to some
`-ERR` texts (unknown op, max payload). The suite normalises `-ERR`
text to a class before comparing, so these pass; a byte-exact
comparison would still flag them. Tightening Rust's error texts to
Go's exact strings is a small, safe follow-up.

## Deliberately out of scope

- Gateways (super-cluster): Rust returns an empty `/gatewayz`; no
  gateway protocol is implemented on either side of this suite.
- JetStream clustering/replication, mirroring/sourcing, stream
  clustering: single-node only.
- MQTT/WebSocket gateways, JWT operator mode, route/leaf TLS, TLS
  client-certificate-to-user mapping: not implemented in rust-natsd
  (see README non-goals); not probed.
- Raw `$KV` API traffic (bypassing the nats.go version gate): not yet
  probed; see kv_version_gate.
- Cluster/leaf interop beyond the existing smoke scripts
  (`scripts/e2e-go-cluster.sh`, `scripts/e2e-go-leaf.sh`): wired into
  the suite as smoke, not expanded.
- Byte-identical monitoring JSON: compared on supported fields only.
- Exact per-member queue distribution: compared on totals only.
