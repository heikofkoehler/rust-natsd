#!/usr/bin/env bash
# Go <-> rust-natsd route interop: a real Go nats-server clustered with a
# rust-natsd node, exercised with the official nats.go client across nodes.
#
# Requires: cargo, go, and the Go nats-server binary at
# $GO_NATS_SERVER (default: ~/workspace/tools/nats-server-v2.15.1-linux-amd64/nats-server).
set -euo pipefail
cd "$(dirname "$0")/.."

GO_BIN="${GO_NATS_SERVER:-$HOME/workspace/tools/nats-server-v2.15.1-linux-amd64/nats-server}"
if [[ ! -x "$GO_BIN" ]]; then
  echo "Go nats-server binary not found at $GO_BIN" >&2
  echo "Download it: https://github.com/nats-io/nats-server/releases" >&2
  exit 1
fi

cargo build --release
BIN="$PWD/target/release/rust-natsd"

cleanup() {
  [[ -n "${GO_PID:-}" ]] && kill "$GO_PID" 2>/dev/null || true
  [[ -n "${RUST_PID:-}" ]] && kill "$RUST_PID" 2>/dev/null || true
}
trap cleanup EXIT

wait_port() {
  local port=$1
  for _ in $(seq 1 50); do
    if (echo >"/dev/tcp/127.0.0.1/$port") &>/dev/null; then return 0; fi
    sleep 0.1
  done
  echo "timed out waiting for port $port" >&2
  return 1
}

# Scenario 1: Go server is the seed; rust node solicits it.
"$GO_BIN" -p 14240 -cluster_name test -cluster nats://127.0.0.1:16222 &>/tmp/go-cluster1.log &
GO_PID=$!
"$BIN" -a 127.0.0.1 -p 14241 --cluster-name test --cluster-host 127.0.0.1 --cluster-port 16223 --routes nats://127.0.0.1:16222 &>/tmp/rust-cluster1.log &
RUST_PID=$!
wait_port 14240
wait_port 14241
cd e2e/go
go run . -addr nats://127.0.0.1:14240 -addr2 nats://127.0.0.1:14241 -mode cluster
cd ../..
kill "$GO_PID" "$RUST_PID" 2>/dev/null || true
wait "$GO_PID" 2>/dev/null || true
wait "$RUST_PID" 2>/dev/null || true
GO_PID=""; RUST_PID=""

# Scenario 2: rust node is the seed; Go server solicits it.
"$BIN" -a 127.0.0.1 -p 14241 --cluster-name test --cluster-host 127.0.0.1 --cluster-port 16223 &>/tmp/rust-cluster2.log &
RUST_PID=$!
"$GO_BIN" -p 14240 -cluster_name test -cluster nats://127.0.0.1:16222 -routes nats://127.0.0.1:16223 &>/tmp/go-cluster2.log &
GO_PID=$!
wait_port 14240
wait_port 14241
cd e2e/go
go run . -addr nats://127.0.0.1:14240 -addr2 nats://127.0.0.1:14241 -mode cluster
cd ../..
echo "GO<->RUST ROUTE INTEROP: OK"
