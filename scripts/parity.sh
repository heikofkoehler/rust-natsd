#!/usr/bin/env bash
# Go-vs-Rust parity suite for rust-natsd.
#
# Starts Go nats-server v2.15.1 and rust-natsd side by side with matching
# configs (plain, auth, token, max-payload, ping, TLS, tlsverify, nkey,
# multi-account, JetStream), runs the same probes against both, and
# compares semantic outcomes. See parity/KNOWN_DIFFS.md for the diffs
# accepted in default mode, and the README section "Parity suite" for
# how to read the table and add a case.
#
# Usage:
#   bash scripts/parity.sh [--strict] [--skip-cluster]
# Env:
#   GO_NATS_SERVER   path to the Go nats-server binary (default:
#                    ~/workspace/tools/nats-server-v2.15.1-linux-amd64/nats-server)
#   PARITY_STRICT=1  same as --strict: known diffs also fail
#   PARITY_KEEP_TMP=1 keep the temp dir (configs, logs, results) for debugging
#
# Exit codes: 0 = green (default mode: only KNOWN_DIFFS differ),
#             1 = new divergence or case failure,
#             2 = harness/setup failure (missing binary, build failure, ...).
set -uo pipefail

STRICT=0
SKIP_CLUSTER=0
for arg in "$@"; do
  case "$arg" in
    --strict) STRICT=1 ;;
    --skip-cluster) SKIP_CLUSTER=1 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done
[ "${PARITY_STRICT:-0}" = "1" ] && STRICT=1

cd "$(dirname "$0")/.."
export PATH="$HOME/.cargo/bin:$PATH"

GO_BIN="${GO_NATS_SERVER:-$HOME/workspace/tools/nats-server-v2.15.1-linux-amd64/nats-server}"
RUST_BIN="$PWD/target/release/rust-natsd"
TMPDIR_PARITY="$(mktemp -d /tmp/rust-natsd-parity.XXXXXX)"
LOGDIR="$TMPDIR_PARITY/logs"
CONFDIR="$TMPDIR_PARITY/conf"
mkdir -p "$LOGDIR" "$CONFDIR" "$TMPDIR_PARITY/go-js" "$TMPDIR_PARITY/rust-js" "$TMPDIR_PARITY/go-main-js"

PIDS=()
cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
  if [ "${PARITY_KEEP_TMP:-0}" != "1" ]; then rm -rf "$TMPDIR_PARITY"; else echo "parity tmp kept at $TMPDIR_PARITY"; fi
}
trap cleanup EXIT

fail() { echo "PARITY SETUP FAILED: $*" >&2; exit 2; }

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

wait_port() {
  local port=$1
  for _ in $(seq 1 100); do
    if (echo >"/dev/tcp/127.0.0.1/$port") &>/dev/null; then return 0; fi
    sleep 0.1
  done
  echo "timed out waiting for port $port" >&2
  return 1
}

# --- locate / fetch the Go server -------------------------------------------
if [ ! -x "$GO_BIN" ]; then
  echo "Go nats-server not found at $GO_BIN; attempting download of v2.15.1..."
  # Extract into the run's tmpdir (not the workspace): CI runners have
  # no ~/workspace/tools, and the download is a test dependency.
  mkdir -p "$TMPDIR_PARITY/goserver"
  if command -v curl >/dev/null && curl -fsSL -o "$TMPDIR_PARITY/nats-server.tgz" \
      "https://github.com/nats-io/nats-server/releases/download/v2.15.1/nats-server-v2.15.1-linux-amd64.tar.gz"; then
    tar -xzf "$TMPDIR_PARITY/nats-server.tgz" -C "$TMPDIR_PARITY/goserver"
    GO_BIN="$TMPDIR_PARITY/goserver/nats-server-v2.15.1-linux-amd64/nats-server"
  fi
  [ -x "$GO_BIN" ] || fail "Go nats-server binary not found and download failed. Set GO_NATS_SERVER. (Note: the correct release asset is nats-server-v2.15.1-linux-amd64.tar.gz; the unversioned name 404s.)"
fi
command -v go >/dev/null || export PATH="$HOME/workspace/tools/go/bin:$PATH"
command -v go >/dev/null || fail "Go toolchain not found (need it for the probe client)."

# --- build -------------------------------------------------------------------
echo "== building rust-natsd (release) =="
cargo build --release || fail "cargo build failed"
echo "== building Go probe client =="
(cd parity/go_client && GOFLAGS=-mod=mod GOPROXY=off GOSUMDB=off go build -o "$TMPDIR_PARITY/parity-go-client" . 2>"$LOGDIR/goclient-build.log" \
  || go build -o "$TMPDIR_PARITY/parity-go-client" . 2>>"$LOGDIR/goclient-build.log") \
  || { cat "$LOGDIR/goclient-build.log"; fail "Go probe client build failed"; }
(cd parity/go_client/nkeytool && GOFLAGS=-mod=mod GOPROXY=off GOSUMDB=off go build -o "$TMPDIR_PARITY/nkeytool" . 2>"$LOGDIR/nkeytool-build.log" \
  || go build -o "$TMPDIR_PARITY/nkeytool" . 2>>"$LOGDIR/nkeytool-build.log") \
  || { cat "$LOGDIR/nkeytool-build.log"; fail "nkeytool build failed"; }

# --- certificates (throwaway test certs, openssl, never committed) ------------
echo "== generating test certificates =="
openssl req -x509 -newkey rsa:2048 -keyout "$TMPDIR_PARITY/ca.key" -out "$TMPDIR_PARITY/ca.crt" -days 2 -nodes -subj "/CN=ParityTestCA" 2>/dev/null \
  || fail "openssl CA generation failed"
mk_cert() { # $1 = CN, $2 = stem, $3 = extfile content (or empty)
  local cn=$1 stem=$2 ext=$3
  openssl req -newkey rsa:2048 -keyout "$TMPDIR_PARITY/$stem.key" -out "$TMPDIR_PARITY/$stem.csr" -nodes -subj "/CN=$cn" 2>/dev/null || return 1
  if [ -n "$ext" ]; then
    printf '%s' "$ext" > "$TMPDIR_PARITY/$stem.ext"
    openssl x509 -req -in "$TMPDIR_PARITY/$stem.csr" -CA "$TMPDIR_PARITY/ca.crt" -CAkey "$TMPDIR_PARITY/ca.key" -CAcreateserial -out "$TMPDIR_PARITY/$stem.crt" -days 2 -extfile "$TMPDIR_PARITY/$stem.ext" 2>/dev/null || return 1
  else
    openssl x509 -req -in "$TMPDIR_PARITY/$stem.csr" -CA "$TMPDIR_PARITY/ca.crt" -CAkey "$TMPDIR_PARITY/ca.key" -CAcreateserial -out "$TMPDIR_PARITY/$stem.crt" -days 2 2>/dev/null || return 1
  fi
}
mk_cert localhost server "subjectAltName=DNS:localhost,IP:127.0.0.1" || fail "server cert generation failed"
mk_cert client client "basicConstraints=CA:FALSE" || fail "client cert generation failed"

# --- nkey (throwaway test keypair, tmpdir only) -------------------------------
NKEY_JSON="$("$TMPDIR_PARITY/nkeytool" gen)" || fail "nkeytool gen failed"
NKEY_PUB="$(python3 -c "import json,sys; print(json.load(sys.stdin)['pubkey'])" <<<"$NKEY_JSON")"
NKEY_SEED="$(python3 -c "import json,sys; print(json.load(sys.stdin)['seed'])" <<<"$NKEY_JSON")"

# --- ports (free, never the user's 4222/6222/8222) ---------------------------
declare -A P
for pair in main auth token max64 ping tls tlsverify nkey accounts jsfile; do
  P["${pair}_go"]=$(free_port); P["${pair}_rust"]=$(free_port)
  P["${pair}_go_http"]=$(free_port); P["${pair}_rust_http"]=$(free_port)
  P["${pair}_go_cluster"]=$(free_port); P["${pair}_rust_cluster"]=$(free_port)
done

# --- Go configs ---------------------------------------------------------------
go_conf() { # $1=file $2=client port $3=http port $4=cluster port (empty = no cluster block) $5=extra block
  {
    echo "port: $2"
    echo "server_name: parity"
    echo "http_port: $3"
    if [ -n "$4" ]; then
      echo "cluster {"
      echo "  name: parity"
      echo "  port: $4"
      echo "}"
    fi
    echo "$5"
  } > "$1"
}
ACCOUNTS_BLOCK='
accounts {
  A {
    users: [
      { user: alice, password: alicepw, permissions: { publish: { allow: ["allowed.>"] }, subscribe: { allow: ["allowed.>", "_INBOX.>"] } } },
      { user: dave, password: davepw, permissions: { publish: { allow: [">"], deny: ["secret.>"] }, subscribe: { allow: [">"], deny: ["secret.>"] } } }
    ]
  }
  B {
    users: [ { user: bob, password: bobpw } ]
  }
}
'
go_conf "$CONFDIR/go-main.conf"   "${P[main_go]}"   "${P[main_go_http]}"   ""   "jetstream { store_dir: \"$TMPDIR_PARITY/go-main-js\" }"
go_conf "$CONFDIR/go-auth.conf"   "${P[auth_go]}"   0                      "${P[auth_go_cluster]}"   "authorization { user: derek, password: s3cret }"
go_conf "$CONFDIR/go-token.conf"  "${P[token_go]}"  0                      "${P[token_go_cluster]}"  "authorization { token: tok123 }"
go_conf "$CONFDIR/go-max64.conf"  "${P[max64_go]}"  0                      "${P[max64_go_cluster]}"  "max_payload: 64"
go_conf "$CONFDIR/go-ping.conf"   "${P[ping_go]}"   0                      "${P[ping_go_cluster]}"   'ping_interval: "200ms"
ping_max: 2'
go_conf "$CONFDIR/go-tls.conf"    "${P[tls_go]}"    0                      "${P[tls_go_cluster]}"    "tls { cert_file: \"$TMPDIR_PARITY/server.crt\", key_file: \"$TMPDIR_PARITY/server.key\" }"
go_conf "$CONFDIR/go-tlsverify.conf" "${P[tlsverify_go]}" 0                "${P[tlsverify_go_cluster]}" "tls { cert_file: \"$TMPDIR_PARITY/server.crt\", key_file: \"$TMPDIR_PARITY/server.key\", ca_file: \"$TMPDIR_PARITY/ca.crt\", verify: true }"
go_conf "$CONFDIR/go-nkey.conf"   "${P[nkey_go]}"   0                      "${P[nkey_go_cluster]}"   "accounts { NK { users: [ { nkey: \"$NKEY_PUB\" } ] } }"
go_conf "$CONFDIR/go-accounts.conf" "${P[accounts_go]}" "${P[accounts_go_http]}" "${P[accounts_go_cluster]}" "$ACCOUNTS_BLOCK"
go_conf "$CONFDIR/go-jsfile.conf" "${P[jsfile_go]}" 0                     "" "jetstream { store_dir: \"$TMPDIR_PARITY/go-js\" }"
# Rust accounts/nkey configs: same account shapes, Rust .conf syntax.
rust_accounts_conf() { # $1=file $2=client port $3=http port $4=cluster port
  cat > "$1" <<EOF
port: $2
host: 127.0.0.1
server_name: parity
http_port: $3
cluster {
  port: $4
}
$ACCOUNTS_BLOCK
EOF
}
rust_accounts_conf "$CONFDIR/rust-accounts.conf" "${P[accounts_rust]}" "${P[accounts_rust_http]}" "${P[accounts_rust_cluster]}"
cat > "$CONFDIR/rust-nkey.conf" <<EOF
port: ${P[nkey_rust]}
host: 127.0.0.1
server_name: parity
cluster {
  port: ${P[nkey_rust_cluster]}
}
accounts { NK { users: [ { nkey: "$NKEY_PUB" } ] } }
EOF

# --- start servers -------------------------------------------------------------
start_go() { # $1=conf $2=logname
  "$GO_BIN" -config "$1" &>"$LOGDIR/$2.log" &
  PIDS+=($!)
}
start_go "$CONFDIR/go-main.conf" go-main
"$RUST_BIN" -a 127.0.0.1 -p "${P[main_rust]}" -n parity --cluster-port "${P[main_rust_cluster]}" --jetstream --http-port "${P[main_rust_http]}" &>"$LOGDIR/rust-main.log" &
PIDS+=($!)
start_go "$CONFDIR/go-auth.conf" go-auth
"$RUST_BIN" -a 127.0.0.1 -p "${P[auth_rust]}" -n parity --cluster-port 0 --user derek --pass s3cret &>"$LOGDIR/rust-auth.log" &
PIDS+=($!)
start_go "$CONFDIR/go-token.conf" go-token
"$RUST_BIN" -a 127.0.0.1 -p "${P[token_rust]}" -n parity --cluster-port 0 --auth-token tok123 &>"$LOGDIR/rust-token.log" &
PIDS+=($!)
start_go "$CONFDIR/go-max64.conf" go-max64
"$RUST_BIN" -a 127.0.0.1 -p "${P[max64_rust]}" -n parity --cluster-port 0 --max-payload 64 &>"$LOGDIR/rust-max64.log" &
PIDS+=($!)
start_go "$CONFDIR/go-ping.conf" go-ping
"$RUST_BIN" -a 127.0.0.1 -p "${P[ping_rust]}" -n parity --cluster-port 0 &>"$LOGDIR/rust-ping.log" &
PIDS+=($!)
start_go "$CONFDIR/go-tls.conf" go-tls
"$RUST_BIN" -a 127.0.0.1 -p "${P[tls_rust]}" -n parity --cluster-port 0 --tls --tlscert "$TMPDIR_PARITY/server.crt" --tlskey "$TMPDIR_PARITY/server.key" &>"$LOGDIR/rust-tls.log" &
PIDS+=($!)
start_go "$CONFDIR/go-tlsverify.conf" go-tlsverify
"$RUST_BIN" -a 127.0.0.1 -p "${P[tlsverify_rust]}" -n parity --cluster-port 0 --tls --tlscert "$TMPDIR_PARITY/server.crt" --tlskey "$TMPDIR_PARITY/server.key" --tlsverify --tlscacert "$TMPDIR_PARITY/ca.crt" &>"$LOGDIR/rust-tlsverify.log" &
PIDS+=($!)
start_go "$CONFDIR/go-nkey.conf" go-nkey
"$RUST_BIN" -a 127.0.0.1 -p "${P[nkey_rust]}" -n parity -c "$CONFDIR/rust-nkey.conf" &>"$LOGDIR/rust-nkey.log" &
PIDS+=($!)
start_go "$CONFDIR/go-accounts.conf" go-accounts
"$RUST_BIN" -a 127.0.0.1 -p "${P[accounts_rust]}" -n parity -c "$CONFDIR/rust-accounts.conf" &>"$LOGDIR/rust-accounts.log" &
PIDS+=($!)

for pair in main auth token max64 ping tls tlsverify nkey accounts; do
  wait_port "${P[${pair}_go]}" || fail "Go $pair server did not start (see $LOGDIR/go-$pair.log)"
  wait_port "${P[${pair}_rust]}" || fail "Rust $pair server did not start (see $LOGDIR/rust-$pair.log)"
done

python3 - "$TMPDIR_PARITY" "${P[main_go]}" "${P[main_go_http]}" "${P[main_rust]}" "${P[main_rust_http]}" \
  "${P[auth_go]}" "${P[auth_rust]}" "${P[token_go]}" "${P[token_rust]}" "${P[max64_go]}" "${P[max64_rust]}" \
  "${P[ping_go]}" "${P[ping_rust]}" "${P[tls_go]}" "${P[tls_rust]}" "${P[tlsverify_go]}" "${P[tlsverify_rust]}" \
  "${P[nkey_go]}" "${P[nkey_rust]}" "${P[accounts_go]}" "${P[accounts_go_http]}" "${P[accounts_rust]}" "${P[accounts_rust_http]}" \
  "$TMPDIR_PARITY/ca.crt" "$TMPDIR_PARITY/client.crt" "$TMPDIR_PARITY/client.key" "$NKEY_PUB" "$NKEY_SEED" "$TMPDIR_PARITY/nkeytool" <<'PYEOF'
import json, sys
tmp = sys.argv[1]
def pair(go, go_http, rust, rust_http):
    return {"go": {"client": int(go), "http": int(go_http)}, "rust": {"client": int(rust), "http": int(rust_http)}}
a = sys.argv
cfg = {
  "ports": {
    "main": pair(a[2], a[3], a[4], a[5]),
    "auth": pair(a[6], 0, a[7], 0),
    "token": pair(a[8], 0, a[9], 0),
    "max64": pair(a[10], 0, a[11], 0),
    "ping": pair(a[12], 0, a[13], 0),
    "tls": pair(a[14], 0, a[15], 0),
    "tlsverify": pair(a[16], 0, a[17], 0),
    "nkey": pair(a[18], 0, a[19], 0),
    "accounts": pair(a[20], a[21], a[22], a[23]),
  },
  "ca_cert": a[24], "client_cert": a[25], "client_key": a[26],
  "nkey_pubkey": a[27], "nkey_seed": a[28], "nkeytool": a[29],
}
with open(f"{tmp}/probes-config.json", "w") as f:
    json.dump(cfg, f, indent=2)
PYEOF

# --- raw + monitoring probes ---------------------------------------------------
echo ""
echo "== raw-wire + monitoring probes =="
STRICT_FLAG=""
[ "$STRICT" = "1" ] && STRICT_FLAG="--strict"
set +e
python3 parity/probes.py --config "$TMPDIR_PARITY/probes-config.json" $STRICT_FLAG --out "$TMPDIR_PARITY/probes-results.json"
PROBES_RC=$?
set -e

# --- Go client JetStream + KV ---------------------------------------------------
echo ""
echo "== Go client JetStream + KV =="
js_compare() { # $1=mode $2=go port $3=rust port $4=label
  local go_out rust_out
  go_out="$("$TMPDIR_PARITY/parity-go-client" -addr "nats://127.0.0.1:$2" -mode "$1" 2>"$LOGDIR/js-$4-go.err")" || { echo "JS $4: Go client failed against Go server:"; echo "  $go_out"; cat "$LOGDIR/js-$4-go.err"; return 1; }
  rust_out="$("$TMPDIR_PARITY/parity-go-client" -addr "nats://127.0.0.1:$3" -mode "$1" 2>"$LOGDIR/js-$4-rust.err")" || { echo "JS $4: Go client failed against Rust server:"; echo "  $rust_out"; cat "$LOGDIR/js-$4-rust.err"; return 1; }
  if [ "$go_out" = "$rust_out" ]; then echo "JS $4: PASS"; return 0; fi
  # Known JS diffs (see parity/KNOWN_DIFFS.md): for the full mode, the
  # only accepted difference is a fresh empty stream's first_seq
  # (Go 0 = "none", Rust 1 = "next"). Anything else is a real DIFF.
  if [ "$1" = "js" ] && python3 - "$go_out" "$rust_out" <<'PY' 2>/dev/null; then
import json, sys
go, rust = json.loads(sys.argv[1]), json.loads(sys.argv[2])
if set(go) != set(rust):
    sys.exit(1)
diffs = {k for k in go if go[k] != rust[k]}
if diffs != {"stream"}:
    sys.exit(1)
gs, rs = dict(go["stream"]), dict(rust["stream"])
if gs.pop("first_seq", None) != 0 or rs.pop("first_seq", None) != 1 or gs != rs:
    sys.exit(1)
PY
    if [ "$STRICT" = "1" ]; then echo "JS $4: KNOWN (strict: FAIL)"; echo "  GO:   $go_out"; echo "  RUST: $rust_out"; return 1; fi
    echo "JS $4: KNOWN (empty-stream first_seq 0 vs 1; see parity/KNOWN_DIFFS.md)"; return 0
  fi
  echo "JS $4: DIFF"; echo "  GO:   $go_out"; echo "  RUST: $rust_out"; return 1
}
JS_RC=0
js_compare js "${P[main_go]}" "${P[main_rust]}" "full" || JS_RC=1
# KV via nats.go is a known diff, not a pass: nats.go refuses KV against
# any server reporting version < 2.6.2, and rust-natsd reports its own
# 0.1.0 in INFO (see parity/KNOWN_DIFFS.md). The Go side must succeed;
# the Rust side must fail with exactly that version-gate error. In
# strict mode the known diff fails too.
KV_GO_OUT="$("$TMPDIR_PARITY/parity-go-client" -addr "nats://127.0.0.1:${P[main_go]}" -mode kv 2>"$LOGDIR/js-kv-go.err")" || { echo "JS kv: Go client failed against Go server:"; echo "  $KV_GO_OUT"; JS_RC=1; }
KV_RUST_OUT="$("$TMPDIR_PARITY/parity-go-client" -addr "nats://127.0.0.1:${P[main_rust]}" -mode kv 2>"$LOGDIR/js-kv-rust.err")" || true
if [ -n "${KV_GO_OUT:-}" ] && [ "${KV_GO_OUT:0:1}" = "{" ] && python3 -c "import json,sys; d=json.loads(sys.argv[1]); sys.exit(0 if 'error' not in d else 1)" "$KV_GO_OUT"; then
  if [ "$KV_RUST_OUT" = '{"error":"createkv: nats: key-value requires at least server version 2.6.2"}' ]; then
    if [ "$STRICT" = "1" ]; then echo "JS kv: KNOWN (strict: FAIL)"; JS_RC=1; else echo "JS kv: KNOWN (nats.go version gate; see parity/KNOWN_DIFFS.md)"; fi
  elif [ "$KV_RUST_OUT" = "$KV_GO_OUT" ]; then echo "JS kv: PASS"
  else echo "JS kv: FAIL (unexpected Rust output)"; echo "  GO:   $KV_GO_OUT"; echo "  RUST: $KV_RUST_OUT"; JS_RC=1; fi
fi

# --- JetStream persistence across restart ---------------------------------------
echo ""
echo "== JetStream persistence across restart =="
PERSIST_RC=0
start_go "$CONFDIR/go-jsfile.conf" go-jsfile; GO_JS_PID=$!
"$RUST_BIN" -a 127.0.0.1 -p "${P[jsfile_rust]}" -n parity --cluster-port 0 --jetstream --js-store-dir "$TMPDIR_PARITY/rust-js" &>"$LOGDIR/rust-jsfile.log" &
RUST_JS_PID=$!
PIDS+=($GO_JS_PID $RUST_JS_PID)
wait_port "${P[jsfile_go]}" || fail "Go JS-file server did not start"
wait_port "${P[jsfile_rust]}" || fail "Rust JS-file server did not start"
"$TMPDIR_PARITY/parity-go-client" -addr "nats://127.0.0.1:${P[jsfile_go]}" -mode jscreate >/dev/null || PERSIST_RC=1
"$TMPDIR_PARITY/parity-go-client" -addr "nats://127.0.0.1:${P[jsfile_rust]}" -mode jscreate >/dev/null || PERSIST_RC=1
kill $GO_JS_PID $RUST_JS_PID 2>/dev/null || true
wait $GO_JS_PID 2>/dev/null || true; wait $RUST_JS_PID 2>/dev/null || true
start_go "$CONFDIR/go-jsfile.conf" go-jsfile2; GO_JS_PID=$!
"$RUST_BIN" -a 127.0.0.1 -p "${P[jsfile_rust]}" -n parity --cluster-port 0 --jetstream --js-store-dir "$TMPDIR_PARITY/rust-js" &>"$LOGDIR/rust-jsfile2.log" &
RUST_JS_PID=$!
PIDS+=($GO_JS_PID $RUST_JS_PID)
wait_port "${P[jsfile_go]}" || fail "Go JS-file server did not restart"
wait_port "${P[jsfile_rust]}" || fail "Rust JS-file server did not restart"
js_compare jscheck "${P[jsfile_go]}" "${P[jsfile_rust]}" "persistence-restart" || PERSIST_RC=1

# --- cluster / leaf interop smoke (existing scripts) ----------------------------
CLUSTER_RC=0
if [ "$SKIP_CLUSTER" = "0" ]; then
  echo ""
  echo "== cluster / leaf interop smoke (existing scripts) =="
  if bash scripts/e2e-go-cluster.sh >"$LOGDIR/cluster.log" 2>&1; then echo "cluster smoke: PASS"; else echo "cluster smoke: FAIL (see $LOGDIR/cluster.log)"; CLUSTER_RC=1; fi
  if bash scripts/e2e-go-leaf.sh >"$LOGDIR/leaf.log" 2>&1; then echo "leaf smoke: PASS"; else echo "leaf smoke: FAIL (see $LOGDIR/leaf.log)"; CLUSTER_RC=1; fi
else
  echo "cluster/leaf smoke: SKIPPED (--skip-cluster)"
fi

# --- summary ---------------------------------------------------------------------
echo ""
echo "== parity summary =="
echo "raw probes rc=$PROBES_RC  js/kv rc=$JS_RC  persistence rc=$PERSIST_RC  cluster/leaf rc=$CLUSTER_RC"
if [ "$PROBES_RC" -ne 0 ] || [ "$JS_RC" -ne 0 ] || [ "$PERSIST_RC" -ne 0 ] || [ "$CLUSTER_RC" -ne 0 ]; then
  echo "PARITY: FAIL"
  exit 1
fi
echo "PARITY: GREEN"
exit 0
