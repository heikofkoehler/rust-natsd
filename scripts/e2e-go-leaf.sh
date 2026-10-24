#!/usr/bin/env bash
# Go <-> rust-natsd leaf interop: a real Go nats-server as hub/leaf with a
# rust-natsd node on the other side, exercised with nats.go across the leaf.
#
# Requires: cargo, go, and the Go nats-server binary at $GO_NATS_SERVER.
set -euo pipefail
cd "$(dirname "$0")/.."

GO_BIN="${GO_NATS_SERVER:-$HOME/workspace/tools/nats-server-v2.15.1-linux-amd64/nats-server}"
if [[ ! -x "$GO_BIN" ]]; then
  echo "Go nats-server binary not found at $GO_BIN" >&2
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

# Scenario 1: Go hub, rust leaf solicits it.
cat > /tmp/go-hub.conf <<'EOF'
port: 14240
leafnodes {
  port: 17422
}
EOF
"$GO_BIN" -config /tmp/go-hub.conf &>/tmp/go-leaf1.log &
GO_PID=$!
"$BIN" -a 127.0.0.1 -p 14241 --cluster-port 0 --leaf-urls nats://127.0.0.1:17422 &>/tmp/rust-leaf1.log &
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

# Scenario 2: rust hub, Go leaf solicits it.
cat > /tmp/go-leaf.conf <<'EOF'
port: 14242
leafnodes {
  remotes: [{ url: "nats://127.0.0.1:17423" }]
}
EOF
"$BIN" -a 127.0.0.1 -p 14241 --cluster-port 0 --leaf-port 17423 &>/tmp/rust-leaf2.log &
RUST_PID=$!
"$GO_BIN" -config /tmp/go-leaf.conf &>/tmp/go-leaf2.log &
GO_PID=$!
wait_port 14241
wait_port 14242
cd e2e/go
go run . -addr nats://127.0.0.1:14242 -addr2 nats://127.0.0.1:14241 -mode cluster
cd ../..
echo "GO<->RUST LEAF INTEROP: OK"
