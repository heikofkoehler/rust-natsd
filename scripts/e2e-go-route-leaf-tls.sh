#!/usr/bin/env bash
# Go <-> rust-natsd route/leaf TLS interop: real Go nats-server v2.15.1
# clustered / leafed with a rust-natsd node over TLS, in both dial
# directions, verified with raw clients publishing across the link.
#
# Probed Go orders (see README/PLAN Phase 22): routes use implicit
# TLS (ClientHello first, no plaintext INFO); leafs send INFO in
# plaintext first (tls_required) and then upgrade before CONNECT.
#
# Requires: cargo, openssl, python3, and the Go nats-server binary at
# $GO_NATS_SERVER (default: ~/workspace/tools/nats-server-v2.15.1-linux-amd64/nats-server).
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.cargo/bin:$PATH"

GO_BIN="${GO_NATS_SERVER:-$HOME/workspace/tools/nats-server-v2.15.1-linux-amd64/nats-server}"
if [[ ! -x "$GO_BIN" ]]; then
  echo "Go nats-server binary not found at $GO_BIN" >&2
  exit 1
fi

cargo build --release
BIN="$PWD/target/release/rust-natsd"

TMP="$(mktemp -d)"
cleanup() {
  [[ -n "${GO_PID:-}" ]] && kill "$GO_PID" 2>/dev/null || true
  [[ -n "${RUST_PID:-}" ]] && kill "$RUST_PID" 2>/dev/null || true
  rm -rf "$TMP"
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

stop_both() {
  [[ -n "${GO_PID:-}" ]] && kill "$GO_PID" 2>/dev/null || true
  [[ -n "${RUST_PID:-}" ]] && kill "$RUST_PID" 2>/dev/null || true
  wait "$GO_PID" 2>/dev/null || true
  wait "$RUST_PID" 2>/dev/null || true
  GO_PID=""; RUST_PID=""
}

# --- certificates (one CA, one server cert used by both sides) --------
openssl req -x509 -newkey rsa:2048 -keyout "$TMP/ca.key" -out "$TMP/ca.crt" -days 2 -nodes -subj "/CN=TestCA" 2>/dev/null
openssl req -newkey rsa:2048 -keyout "$TMP/srv.key" -out "$TMP/srv.csr" -nodes -subj "/CN=localhost" 2>/dev/null
printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\n" > "$TMP/srv.ext"
openssl x509 -req -in "$TMP/srv.csr" -CA "$TMP/ca.crt" -CAkey "$TMP/ca.key" -CAcreateserial -out "$TMP/srv.crt" -days 2 -extfile "$TMP/srv.ext" 2>/dev/null

cat > "$TMP/probe.py" <<'PY'
import select, socket, sys, time

class Buf:
    """Line/byte reader over a socket using select() for timeouts
    (socket timeouts + makefile do not mix: a timed-out makefile
    becomes unusable)."""
    def __init__(self, s):
        self.s = s
        self.buf = bytearray()
    def _fill(self, timeout):
        r, _, _ = select.select([self.s], [], [], timeout)
        if not r:
            return False
        data = self.s.recv(65536)
        if not data:
            raise ConnectionError("server closed connection")
        self.buf += data
        return True
    def line(self, timeout):
        deadline = time.time() + timeout
        while True:
            idx = self.buf.find(b"\r\n")
            if idx >= 0:
                out = bytes(self.buf[:idx])
                del self.buf[:idx + 2]
                return out.decode()
            if not self._fill(max(0.0, deadline - time.time())):
                return None
    def exact(self, n, timeout=5):
        deadline = time.time() + timeout
        while len(self.buf) < n:
            if not self._fill(max(0.0, deadline - time.time())):
                raise TimeoutError("timed out reading payload")
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out

def client(port):
    s = socket.create_connection(("127.0.0.1", port))
    b = Buf(s)
    info = b.line(5)
    assert info is not None and info.startswith("INFO"), info
    s.sendall(b'CONNECT {"verbose":false,"headers":true}\r\n')
    return s, b

def cross(pub_port, sub_port, subject, payload):
    # Retry until delivery: the route/leaf link and RS+/LS+ interest
    # need a moment to establish, and Go servers send PINGs that a
    # raw client must answer with PONG (not treat as the message).
    s, b = client(sub_port)
    s.sendall(f"SUB {subject} 1\r\n".encode())
    time.sleep(0.5)
    p, _ = client(pub_port)
    deadline = time.time() + 15
    while time.time() < deadline:
        p.sendall(f"PUB {subject} {len(payload)}\r\n{payload}\r\n".encode())
        while True:
            line = b.line(1)
            if line is None:
                break  # republish and wait again
            if line == "PING":
                s.sendall(b"PONG\r\n")
                continue
            assert line.startswith(f"MSG {subject} 1 "), f"expected MSG, got: {line!r}"
            data = b.exact(len(payload) + 2)
            assert data[:-2].decode() == payload, f"payload: {data!r}"
            print(f"  OK {pub_port} -> {sub_port} {subject}={payload}")
            return
    raise AssertionError(f"no delivery {pub_port} -> {sub_port} {subject}")
cross(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], sys.argv[4])
cross(int(sys.argv[2]), int(sys.argv[1]), sys.argv[5], sys.argv[6])
PY

TLS_ARGS=(--cluster-tlscert "$TMP/srv.crt" --cluster-tlskey "$TMP/srv.key" --cluster-tlscacert "$TMP/ca.crt" --cluster-tlsverify)
LEAF_TLS_ARGS=(--leaf-tlscert "$TMP/srv.crt" --leaf-tlskey "$TMP/srv.key" --leaf-tlscacert "$TMP/ca.crt" --leaf-tlsverify)

# --- Scenario 1: Go route seed (TLS), Rust node solicits it (TLS) -----
cat > "$TMP/go-route.conf" <<EOF
port: 14340
cluster {
  name: CL
  port: 16340
  tls { cert_file: $TMP/srv.crt, key_file: $TMP/srv.key, ca_file: $TMP/ca.crt, verify: true }
}
EOF
"$GO_BIN" -config "$TMP/go-route.conf" &>"$TMP/go-route.log" &
GO_PID=$!
"$BIN" -a 127.0.0.1 -p 14341 --cluster-name CL --cluster-host 127.0.0.1 --cluster-port 16341 --routes nats://127.0.0.1:16340 "${TLS_ARGS[@]}" &>"$TMP/rust-route.log" &
RUST_PID=$!
wait_port 14340; wait_port 14341
python3 "$TMP/probe.py" 14341 14340 route.tls.1 up route.tls.2 down
echo "SCENARIO 1 (Go seed, Rust solicits, route TLS): OK"
stop_both

# --- Scenario 2: Rust route seed (TLS), Go server solicits it (TLS) ---
"$BIN" -a 127.0.0.1 -p 14341 --cluster-name CL --cluster-host 127.0.0.1 --cluster-port 16341 "${TLS_ARGS[@]}" &>"$TMP/rust-route2.log" &
RUST_PID=$!
cat > "$TMP/go-route2.conf" <<EOF
port: 14340
cluster {
  name: CL
  port: 16340
  tls { cert_file: $TMP/srv.crt, key_file: $TMP/srv.key, ca_file: $TMP/ca.crt, verify: true }
  routes: [ "nats://127.0.0.1:16341" ]
}
EOF
"$GO_BIN" -config "$TMP/go-route2.conf" &>"$TMP/go-route2.log" &
GO_PID=$!
wait_port 14340; wait_port 14341
python3 "$TMP/probe.py" 14340 14341 route.tls.3 up route.tls.4 down
echo "SCENARIO 2 (Rust seed, Go solicits, route TLS): OK"
stop_both

# --- Scenario 3: Go leaf hub (TLS), Rust leaf solicits it (TLS) -------
# Leaf hub and leaf must have different cluster names (Go rejects a
# leaf with the hub's cluster name).
cat > "$TMP/go-hub.conf" <<EOF
port: 14340
cluster { name: HUB }
leafnodes {
  port: 17340
  tls { cert_file: $TMP/srv.crt, key_file: $TMP/srv.key, ca_file: $TMP/ca.crt, verify: true }
}
EOF
"$GO_BIN" -config "$TMP/go-hub.conf" &>"$TMP/go-hub.log" &
GO_PID=$!
cat > "$TMP/rust-leaf.conf" <<EOF
port: 14341
cluster { name: LEAF, port: 0 }
leafnodes {
  remotes: [ { url: "nats://127.0.0.1:17340", tls: { cert_file: $TMP/srv.crt, key_file: $TMP/srv.key, ca_file: $TMP/ca.crt } } ]
}
EOF
"$BIN" -a 127.0.0.1 -c "$TMP/rust-leaf.conf" &>"$TMP/rust-leaf.log" &
RUST_PID=$!
wait_port 14340; wait_port 14341
python3 "$TMP/probe.py" 14341 14340 leaf.tls.1 up leaf.tls.2 down
echo "SCENARIO 3 (Go hub, Rust leaf solicits, leaf TLS): OK"
stop_both

# --- Scenario 4: Rust leaf hub (TLS), Go leaf solicits it (TLS) -------
"$BIN" -a 127.0.0.1 -p 14341 --cluster-name HUBR --cluster-port 0 --leaf-port 17341 "${LEAF_TLS_ARGS[@]}" &>"$TMP/rust-hub.log" &
RUST_PID=$!
cat > "$TMP/go-leaf.conf" <<EOF
port: 14340
cluster { name: LEAFG }
leafnodes {
  remotes: [ { url: "nats://127.0.0.1:17341", tls: { cert_file: $TMP/srv.crt, key_file: $TMP/srv.key, ca_file: $TMP/ca.crt } } ]
}
EOF
"$GO_BIN" -config "$TMP/go-leaf.conf" &>"$TMP/go-leaf2.log" &
GO_PID=$!
wait_port 14340; wait_port 14341
python3 "$TMP/probe.py" 14340 14341 leaf.tls.3 up leaf.tls.4 down
echo "SCENARIO 4 (Rust hub, Go leaf solicits, leaf TLS): OK"
stop_both

echo "GO<->RUST ROUTE/LEAF TLS INTEROP: OK"
