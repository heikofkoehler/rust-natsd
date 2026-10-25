#!/usr/bin/env bash
# Go (nats.go) client e2e for the newer rust-natsd features: account
# service imports, JetStream mirroring, and JWT operator auth.
# Requires: cargo, go, openssl-free (JWTs are minted by the Go program).
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build --release
BIN="$PWD/target/release/rust-natsd"

TMPDIR_WORK=$(mktemp -d)
cleanup() {
  [[ -n "${PID1:-}" ]] && kill "$PID1" 2>/dev/null || true
  [[ -n "${PID2:-}" ]] && kill "$PID2" 2>/dev/null || true
  rm -rf "$TMPDIR_WORK"
}
trap cleanup EXIT

# Server 1: accounts A/B with a service export/import, JetStream on.
cat > "$TMPDIR_WORK/accounts.conf" <<'EOF'
host: 127.0.0.1
port: 0
cluster {
  port: 0
}
jetstream {
}
accounts {
  A {
    users: [{user: alice, password: alicepw}]
    # Per-account JetStream gate (Phase 28C): the mirror mode uses
    # account A, so A must opt in explicitly now.
    jetstream: enabled
    exports: [{service: "svc.>", accounts: ["B"]}]
  }
  B {
    users: [{user: bob, password: bobpw}]
    imports: [{service: {account: A, subject: "svc.>"}, to: "local.>"}]
  }
}
EOF
"$BIN" -c "$TMPDIR_WORK/accounts.conf" -p 14250 --cluster-port 0 &>"$TMPDIR_WORK/accounts.log" &
PID1=$!

# Server 2: JWT operator mode. The Go program mints the chain and
# writes the .conf (port 0 in the file; -p below wins) + creds JSON.
cd e2e/go
go mod tidy
go run . -mode genjwt -out "$TMPDIR_WORK/jwt.conf" -creds "$TMPDIR_WORK/creds.json"
cd ../..
"$BIN" -c "$TMPDIR_WORK/jwt.conf" -p 14251 --cluster-port 0 &>"$TMPDIR_WORK/jwt.log" &
PID2=$!

for port in 14250 14251; do
  for _ in $(seq 1 50); do
    if (echo >"/dev/tcp/127.0.0.1/$port") &>/dev/null; then break; fi
    sleep 0.1
  done
done

cd e2e/go
go run . -addr nats://127.0.0.1:14250 -mode imports
go run . -addr nats://127.0.0.1:14250 -mode mirror
# JWT mode: nats.go UserJWT (no CONNECT nkey field) against the
# JWT-operator server; fixed in Phase 28B (key derived from JWT sub).
go run . -addr nats://127.0.0.1:14251 -mode jwt -creds "$TMPDIR_WORK/creds.json"
echo "GO ADVANCED E2E: OK"
