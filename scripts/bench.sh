#!/usr/bin/env bash
# Benchmark rust-natsd against Go nats-server v2.15.1 with the SAME client
# (bench/go_client, nats.go) against both servers, so client bias cancels.
#
# Methodology (also summarised in the README):
# - Localhost, single node, no TLS, no auth, matching configs (JetStream
#   on, memory storage for the JS scenario).
# - rust-natsd is built in --release mode. A debug build would be a rigged
#   comparison and is not used here.
# - Each scenario runs 3 times per server, alternating which server runs
#   first, to cancel warm-up/drift bias. Every run has a warm-up phase
#   before measurement (inside the client).
# - Results report the MEDIAN across runs plus the range, never a single
#   best run. This box is a shared VM, so numbers are noisy; treat them
#   as an order-of-magnitude comparison, not a precise ranking.
# - Raw output goes to bench/results/bench-<timestamp>.txt (gitignored).
#   Only the aggregated table is committed (in the README).
#
# Usage: bash scripts/bench.sh
# Requires: cargo, go, python3. Cleans up its servers on exit (trap).
set -euo pipefail
cd "$(dirname "$0")/.."

export PATH="$HOME/.cargo/bin:$PATH"
GO_BIN="${GO_BIN:-$HOME/workspace/tools/go/bin/go}"
GO_SERVER="${GO_NATS_SERVER:-$HOME/workspace/tools/nats-server-v2.15.1-linux-amd64/nats-server}"
if [[ ! -x "$GO_BIN" ]]; then GO_BIN="go"; fi
if [[ ! -x "$GO_SERVER" ]]; then
  echo "bench: Go nats-server not found at $GO_SERVER (set GO_NATS_SERVER)" >&2
  exit 2
fi

PORT=14421
URL="nats://127.0.0.1:$PORT"
RUNS="${BENCH_RUNS:-3}"
# Scenario counts: pubsub 200k, reqreply 20k, js 20k (task minimums).
# BENCH_SCALE=small (env) cuts counts 10x for a smoke run.
SCALE="${BENCH_SCALE:-1}"
if [[ "$SCALE" == "small" ]]; then
  PUB_N=20000; REQ_N=2000; JS_N=2000; WARM_PUB=1000; WARM_REQ=100; WARM_JS=100
else
  PUB_N=200000; REQ_N=20000; JS_N=20000; WARM_PUB=10000; WARM_REQ=1000; WARM_JS=1000
fi

cargo build --release
RUST_BIN="$PWD/target/release/rust-natsd"

cd bench/go_client
GOFLAGS=-mod=mod GOPROXY=off "$GO_BIN" build -o ../bench-client .
cd ../..

TMP="$(mktemp -d)"
OUT="bench/results/bench-$(date +%Y-%m-%dT%H%M%S).txt"
mkdir -p bench/results
SERVER_PID=""

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then kill "$SERVER_PID" 2>/dev/null || true; fi
  rm -rf "$TMP"
}
trap cleanup EXIT

wait_port() {
  for _ in $(seq 1 100); do
    if (echo >"/dev/tcp/127.0.0.1/$PORT") &>/dev/null; then return 0; fi
    sleep 0.1
  done
  echo "bench: server did not come up on $PORT" >&2
  return 1
}

start_go() {
  "$GO_SERVER" -a 127.0.0.1 -p "$PORT" -js -sd "$TMP/go-js" >"$TMP/go.log" 2>&1 &
  SERVER_PID=$!
  wait_port
}
start_rust() {
  # No --js-store-dir on purpose: with a store dir, rust-natsd snapshots
  # the whole engine JSON after every mutating operation (see
  # jetstream.rs), which would dominate the JS scenario and grow
  # quadratically with message count. The benchmark stream uses memory
  # storage on both servers; Go's -sd then holds only JetStream metadata
  # for a memory stream, so the two message paths are both in-memory.
  "$RUST_BIN" -a 127.0.0.1 -p "$PORT" --cluster-port 0 --jetstream >"$TMP/rust.log" 2>&1 &
  SERVER_PID=$!
  wait_port
}
stop_server() {
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
  rm -rf "$TMP/go-js" "$TMP/rust-js"
}

run_scenarios() {
  local server="$1"
  echo "## run server=$server" | tee -a "$OUT"
  "$PWD/bench/bench-client" -addr "$URL" -mode pubsub -size 256 -count "$PUB_N" -warmup "$WARM_PUB" 2>&1 | sed "s/^/server=$server /" | tee -a "$OUT"
  "$PWD/bench/bench-client" -addr "$URL" -mode pubsub -size 1024 -count "$PUB_N" -warmup "$WARM_PUB" 2>&1 | sed "s/^/server=$server /" | tee -a "$OUT"
  "$PWD/bench/bench-client" -addr "$URL" -mode reqreply -size 256 -count "$REQ_N" -warmup "$WARM_REQ" 2>&1 | sed "s/^/server=$server /" | tee -a "$OUT"
  "$PWD/bench/bench-client" -addr "$URL" -mode js -size 1024 -count "$JS_N" -warmup "$WARM_JS" 2>&1 | sed "s/^/server=$server /" | tee -a "$OUT"
}

{
  echo "# bench run $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "# uname: $(uname -a)"
  echo "# cpu: $(grep -m1 'model name' /proc/cpuinfo | sed 's/.*: //')"
  echo "# nproc: $(nproc)"
  echo "# go: $("$GO_BIN" version)"
  echo "# rustc: $(rustc --version)"
  echo "# go-server: $("$GO_SERVER" -v 2>&1 | head -1)"
  echo "# rust-natsd: $(git rev-parse HEAD) $(git rev-parse --short HEAD)"
  echo "# rust-natsd: built --release from the working tree above"
} | tee "$OUT"

for run in $(seq 1 "$RUNS"); do
  if (( run % 2 == 1 )); then order="go rust"; else order="rust go"; fi
  for server in $order; do
    if [[ "$server" == "go" ]]; then start_go; else start_rust; fi
    run_scenarios "$server" || { echo "bench: scenario failed for $server (run $run), continuing" | tee -a "$OUT"; }
    stop_server
  done
done

echo
echo "== benchmark summary (median [range] across $RUNS runs) =="
python3 - "$OUT" <<'PYEOF'
import re, statistics, sys
rows = {}
for line in open(sys.argv[1]):
    m = re.search(r'server=(\S+) RESULT scenario=(\S+) count=(\d+) size=(\d+) elapsed_ms=([\d.]+) msgs_per_sec=([\d.]+) mb_per_sec=([\d.]+) mean_us=([\d.]+) p50_us=([\d.]+) p99_us=([\d.]+)', line)
    if not m:
        continue
    srv, scen = m.group(1), m.group(2)
    rows.setdefault((srv, scen), []).append(m.groups()[5:])
for (srv, scen) in sorted(rows):
    vals = rows[(srv, scen)]
    def med_range(idx):
        xs = sorted(float(v[idx]) for v in vals)
        return f"{statistics.median(xs):.1f} [{xs[0]:.1f}..{xs[-1]:.1f}]"
    # msgs_per_sec idx0, mb_per_sec idx1, mean idx2, p50 idx3, p99 idx4
    if scen.startswith("reqreply"):
        print(f"{srv:5s} {scen:14s} n={len(vals)} mean_us={med_range(2)} p50_us={med_range(3)} p99_us={med_range(4)}")
    else:
        print(f"{srv:5s} {scen:14s} n={len(vals)} msgs/s={med_range(0)} MB/s={med_range(1)}")
PYEOF
echo
echo "Raw output: $OUT"
echo "BENCH: DONE"
