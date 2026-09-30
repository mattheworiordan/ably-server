#!/usr/bin/env bash
# Local smoke of the load generator and conductor (TASK-loadgen F): a
# three-node compose cluster with two ably-loadgen agents inside its
# network; shapes M, D and F at 1% through the conductor (running on the
# host) with the pass criteria evaluated, then shape M again with a node
# killed mid-hold. Laptop numbers are not results (plus or minus
# 50%); the point is that the pipeline works end to end.
#
#   bench/aws/local-smoke.sh              # M, D, then the node-kill run
#   SHAPES="d" FAULT=0 bench/aws/local-smoke.sh
#
# Environment: SHAPES (default "m d f"), FAULT (1: add the node-kill run),
# SCALE (0.01), RAMP (30s), HOLD (60s), DRAIN (10s), RESULTS (results
# root, default ./results/smoke), LOG (a LOG.md to append to), BUS and
# SMOKE_IMAGE (see bench/docker-compose.loadgen-smoke.yml), KEEP (1:
# leave the stack up).
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
compose=(docker compose -f bench/docker-compose.loadgen-smoke.yml)
shapes="${SHAPES:-m d f}"
scale="${SCALE:-0.01}"
ramp="${RAMP:-30s}"
hold="${HOLD:-60s}"
drain="${DRAIN:-10s}"
results="${RESULTS:-$root/results/smoke}"
log="${LOG:-}"
bus_label="${BUS:-pgnotify}"
# Agents reach the nodes by service name; the conductor reaches metrics
# and agents through the published ports.
endpoints="node1:8080,node2:8080,node3:8080"
metrics="http://localhost:8291/metrics,http://localhost:8292/metrics,http://localhost:8293/metrics"

mkdir -p bin/linux "$results"
go build -o bin/ ./cmd/ably-conductor
GOOS=linux GOARCH="$(go env GOARCH)" CGO_ENABLED=0 go build -o bin/linux/ ./cmd/ably-loadgen

cleanup() {
  if [ "${KEEP:-0}" != "1" ]; then
    "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

up() {
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  if [ -z "${SMOKE_IMAGE:-}" ]; then
    "${compose[@]}" build node1
  fi
  "${compose[@]}" up -d --wait
}

run() {
  local shape="$1" id="$2"
  shift 2
  local args=(run --scenario "bench/scenarios/shape-$shape.toml" --scale "$scale"
    --ramp "$ramp" --hold "$hold" --drain "$drain"
    --endpoints "$endpoints" --node-metrics "$metrics"
    --agent http://localhost:9281 --agent http://localhost:9282
    --results "$results/$id" --run-id "$id"
    --key app.key:secret --env "bus=$bus_label" --env "host=laptop" --poll 5s)
  if [ -n "$log" ]; then
    args+=(--log "$log")
  fi
  # A failing run is a result, not a script error: keep going.
  bin/ably-conductor "${args[@]}" "$@" || true
}

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
for shape in $shapes; do
  up
  run "$shape" "smoke-$shape-$stamp" || true
done
if [ "${FAULT:-1}" = "1" ]; then
  up
  run m "smoke-m-nodekill-$stamp" --fault-hook "${compose[*]} kill node2" --fault-at 20s
fi
bin/ably-conductor report "$results"/smoke-*-"$stamp"/summary.json > "$results/report-$stamp.md" || true
echo "results: $results (report-$stamp.md)"
