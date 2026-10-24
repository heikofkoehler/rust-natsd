#!/usr/bin/env bash
# Build rust-natsd and run the Go (nats.go) client e2e program against it.
# Requires: cargo, go.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build --release
BIN="$PWD/target/release/rust-natsd"

cleanup() {
  [[ -n "${PID1:-}" ]] && kill "$PID1" 2>/dev/null || true
  [[ -n "${PID2:-}" ]] && kill "$PID2" 2>/dev/null || true
}
trap cleanup EXIT

# Single-node tests: disable the route listener, or both servers would
# fight over the default cluster port 6222.
"$BIN" -a 127.0.0.1 -p 14231 --cluster-port 0 &>/tmp/rust-natsd-go-e2e.log &
PID1=$!
"$BIN" -a 127.0.0.1 -p 14232 --cluster-port 0 --user derek --pass s3cret &>/tmp/rust-natsd-go-e2e-auth.log &
PID2=$!

# Wait for both ports.
for port in 14231 14232; do
  for _ in $(seq 1 50); do
    if (echo >"/dev/tcp/127.0.0.1/$port") &>/dev/null; then break; fi
    sleep 0.1
  done
done

cd e2e/go
go mod tidy
go run . -addr nats://127.0.0.1:14231 -mode basic
go run . -addr nats://127.0.0.1:14232 -mode auth
echo "GO E2E: OK"
