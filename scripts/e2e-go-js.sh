#!/usr/bin/env bash
# Go nats.go JetStream interop: rust-natsd with --jetstream, exercised
# with the official nats.go JetStream client (streams, publish acks,
# dedup, expected sequences, MSG.GET, pull fetch/ack/nak, push, purge).
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build --release
BIN="$PWD/target/release/rust-natsd"

cleanup() {
  [[ -n "${RUST_PID:-}" ]] && kill "$RUST_PID" 2>/dev/null || true
}
trap cleanup EXIT

"$BIN" -a 127.0.0.1 -p 14246 --cluster-port 0 --jetstream &>/tmp/rust-js-go.log &
RUST_PID=$!
for _ in $(seq 1 50); do
  if (echo >"/dev/tcp/127.0.0.1/14246") &>/dev/null; then break; fi
  sleep 0.1
done
cd e2e/go
go run . -addr nats://127.0.0.1:14246 -mode js
cd ../..
echo "GO<->RUST JETSTREAM INTEROP: OK"
