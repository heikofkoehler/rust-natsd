#!/usr/bin/env bash
# Go <-> rust-natsd gateway (supercluster) interop, the 4-direction
# matrix from PLAN Phase 24/33:
#   (1) Go seed + Rust solicits, traffic Go->Rust
#   (2) Go seed + Rust solicits, traffic Rust->Go
#   (3) Rust seed + Go solicits, traffic Go->Rust
#   (4) Rust seed + Go solicits, traffic Rust->Go
# Directions (1)/(2) and (3)/(4) are both covered by the nats.go
# cluster mode (subscribes on one node, publishes on the other, and
# the reverse), plus the account-scoped + queue gateway mode.
# Implicit back-connect is checked via /gatewayz in both scenarios:
# the accepter must show an outbound (implicit, configured: false)
# connection to the solicitor's gateway, alongside the inbound one.
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

# gatewayz field check: $1 = http port, $2 = python expression over d
gwz_check() {
  local port=$1 expr=$2 label=$3
  for _ in $(seq 1 50); do
    if curl -s "http://127.0.0.1:$port/gatewayz" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if ($expr) else 1)" 2>/dev/null; then
      echo "GATEWAYZ OK: $label"
      return 0
    fi
    sleep 0.2
  done
  echo "GATEWAYZ FAIL: $label" >&2
  curl -s "http://127.0.0.1:$port/gatewayz" || true
  echo >&2
  exit 1
}

ACCOUNTS='
accounts {
  APP { users: [{user: "app", password: "app"}] }
  OTHER { users: [{user: "oth", password: "oth"}] }
}
'

# Scenario 1+2: Go seed (gateway A), Rust solicits (gateway B).
cat > /tmp/go-gw-seed.conf <<EOF
port: 14270
http_port: 18270
server_name: GOSEED
gateway {
  name: A
  host: "127.0.0.1"
  port: 17270
}
$ACCOUNTS
EOF
cat > /tmp/rust-gw-solicit.conf <<EOF
port: 14271
http_port: 18271
server_name: RUSTSOL
gateway {
  name: B
  host: "127.0.0.1"
  port: 17271
  gateways: [{ name: "A", urls: ["nats://127.0.0.1:17270"] }]
}
$ACCOUNTS
EOF
"$GO_BIN" -config /tmp/go-gw-seed.conf &>/tmp/go-gw1.log &
GO_PID=$!
"$BIN" -a 127.0.0.1 -c /tmp/rust-gw-solicit.conf --cluster-port 0 &>/tmp/rust-gw1.log &
RUST_PID=$!
wait_port 14270
wait_port 14271
cd e2e/go
go run . -addr nats://127.0.0.1:14270 -addr2 nats://127.0.0.1:14271 -mode cluster -user app -pass app
go run . -addr nats://127.0.0.1:14270 -addr2 nats://127.0.0.1:14271 -mode gateway
cd ../..
# Rust solicitor: outbound A configured:true; Go accepter: implicit
# outbound to B configured:false (the back-connect).
gwz_check 18271 "d['outbound_gateways'].get('A',{}).get('configured') is True" "Rust solicitor outbound A configured:true"
gwz_check 18270 "d['outbound_gateways'].get('B',{}).get('configured') is False" "Go accepter implicit outbound B configured:false"
kill "$GO_PID" "$RUST_PID" 2>/dev/null || true
wait "$GO_PID" 2>/dev/null || true
wait "$RUST_PID" 2>/dev/null || true
GO_PID=""; RUST_PID=""

# Scenario 3+4: Rust seed (gateway A), Go solicits (gateway B).
cat > /tmp/rust-gw-seed.conf <<EOF
port: 14271
http_port: 18271
server_name: RUSTSEED
gateway {
  name: A
  host: "127.0.0.1"
  port: 17271
}
$ACCOUNTS
EOF
cat > /tmp/go-gw-solicit.conf <<EOF
port: 14270
http_port: 18270
server_name: GOSOL
gateway {
  name: B
  host: "127.0.0.1"
  port: 17270
  gateways: [{ name: "A", urls: ["nats://127.0.0.1:17271"] }]
}
$ACCOUNTS
EOF
"$BIN" -a 127.0.0.1 -c /tmp/rust-gw-seed.conf --cluster-port 0 &>/tmp/rust-gw2.log &
RUST_PID=$!
"$GO_BIN" -config /tmp/go-gw-solicit.conf &>/tmp/go-gw2.log &
GO_PID=$!
wait_port 14271
wait_port 14270
cd e2e/go
go run . -addr nats://127.0.0.1:14270 -addr2 nats://127.0.0.1:14271 -mode cluster -user app -pass app
go run . -addr nats://127.0.0.1:14270 -addr2 nats://127.0.0.1:14271 -mode gateway
cd ../..
# Go solicitor: outbound A configured:true; Rust accepter: implicit
# outbound to B configured:false (the back-connect).
gwz_check 18270 "d['outbound_gateways'].get('A',{}).get('configured') is True" "Go solicitor outbound A configured:true"
gwz_check 18271 "d['outbound_gateways'].get('B',{}).get('configured') is False" "Rust accepter implicit outbound B configured:false"
kill "$GO_PID" "$RUST_PID" 2>/dev/null || true
wait "$GO_PID" 2>/dev/null || true
wait "$RUST_PID" 2>/dev/null || true
GO_PID=""; RUST_PID=""

# Scenario 3 (TLS): Go TLS seed (gateway A) + Rust TLS solicits
# (gateway B). Gateway TLS is implicit mTLS (probed in Phase 33):
# both listeners carry tls { cert, key, verify, ca }, and the
# solicitor's gateways entry carries its own tls { cert, key, ca }
# (the client certificate Go's server requires). The implicit
# back-connect from Go to Rust also uses TLS (Go inherits its local
# gateway TLS settings), so the Rust listener is TLS too.
CERTDIR=$(mktemp -d)
openssl req -x509 -newkey rsa:2048 -keyout "$CERTDIR/ca.key" -out "$CERTDIR/ca.crt" -days 2 -nodes -subj "/CN=TestCA" -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
mkcert() {
  local stem=$1
  openssl req -newkey rsa:2048 -keyout "$CERTDIR/$stem.key" -out "$CERTDIR/$stem.csr" -nodes -subj "/CN=localhost" 2>/dev/null
  printf "subjectAltName=DNS:localhost,IP:127.0.0.1\n" > "$CERTDIR/$stem.ext"
  openssl x509 -req -in "$CERTDIR/$stem.csr" -CA "$CERTDIR/ca.crt" -CAkey "$CERTDIR/ca.key" -CAcreateserial -out "$CERTDIR/$stem.crt" -days 2 -extfile "$CERTDIR/$stem.ext" 2>/dev/null
}
mkcert go-srv
mkcert rust-srv
cat > /tmp/go-gw-tls-seed.conf <<EOF
port: 14272
http_port: 18272
server_name: GOTLS
gateway {
  name: A
  host: "127.0.0.1"
  port: 17272
  tls {
    cert_file: "$CERTDIR/go-srv.crt"
    key_file: "$CERTDIR/go-srv.key"
    verify: true
    ca_file: "$CERTDIR/ca.crt"
  }
}
$ACCOUNTS
EOF
cat > /tmp/rust-gw-tls-solicit.conf <<EOF
port: 14273
http_port: 18273
server_name: RUSTTLS
gateway {
  name: B
  host: "127.0.0.1"
  port: 17273
  tls {
    cert_file: "$CERTDIR/rust-srv.crt"
    key_file: "$CERTDIR/rust-srv.key"
    verify: true
    ca_file: "$CERTDIR/ca.crt"
  }
  gateways: [{ name: "A", urls: ["nats://127.0.0.1:17272"], tls: { cert_file: "$CERTDIR/rust-srv.crt", key_file: "$CERTDIR/rust-srv.key", ca_file: "$CERTDIR/ca.crt" } }]
}
$ACCOUNTS
EOF
"$GO_BIN" -config /tmp/go-gw-tls-seed.conf &>/tmp/go-gw3.log &
GO_PID=$!
"$BIN" -a 127.0.0.1 -c /tmp/rust-gw-tls-solicit.conf --cluster-port 0 &>/tmp/rust-gw3.log &
RUST_PID=$!
wait_port 14272
wait_port 14273
cd e2e/go
go run . -addr nats://127.0.0.1:14272 -addr2 nats://127.0.0.1:14273 -mode cluster -user app -pass app
cd ../..
gwz_check 18273 "d['outbound_gateways'].get('A',{}).get('configured') is True" "Rust TLS solicitor outbound A configured:true"
rm -rf "$CERTDIR"
echo "GO<->RUST GATEWAY INTEROP: OK"
