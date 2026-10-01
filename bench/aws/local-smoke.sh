#!/usr/bin/env bash
# Local smoke of the load generator and conductor (TASK-loadgen F): a
# three-node compose cluster with two ably-loadgen agents inside its
# network; shapes M, D and F at 1% through the conductor (running on the
# host) with the pass criteria evaluated, then shape M again with a node
# killed mid-hold. Laptop numbers are not results (plus or minus
# 50%); the point is that the pipeline works end to end.
#
#   bench/aws/local-smoke.sh              # M, D, F, then the node-kill run
#   SHAPES="d" FAULT=0 bench/aws/local-smoke.sh
#   SHAPES="" SCENARIOS="smoke-1pct" FAULT=0 HOLD=3m bench/aws/local-smoke.sh
#
# Environment: SHAPES (default "m d f"; an empty SHAPES="" runs no shape
# file), SCENARIOS (scenario names under bench/scenarios, or paths, run
# after the shapes: for instance "smoke-1pct"; each keeps its own scale
# unless SCALE is set), FAULT (1: add the node-kill run), SCALE (0.01 for
# the shapes), RAMP (30s), HOLD (60s), DRAIN (10s), SERVER_IDLE_TIMEOUT
# (the nodes' channel idle timeout, passed to the conductor's growth
# baseline; default the scenario's, else 60s), RESULTS (results root,
# default ./results/smoke), LOG (a LOG.md to append to), BUS and
# SMOKE_IMAGE (see bench/docker-compose.loadgen-smoke.yml), KEEP (1:
# leave the stack up).
#
# Node memory and goroutine growth are judged only from hold start plus
# the idle timeout, so a hold of 60 s with the default 60 s timeout
# reports growth as not measured; use HOLD=3m or more to judge it.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
compose=(docker compose -f bench/docker-compose.loadgen-smoke.yml)
shapes="${SHAPES-m d f}"
scenarios="${SCENARIOS:-}"
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

# scenario_file resolves a SCENARIOS entry: a path, or a name under
# bench/scenarios with or without .toml.
scenario_file() {
  local s="$1"
  for f in "$s" "bench/scenarios/$s" "bench/scenarios/$s.toml"; do
    if [ -f "$f" ]; then
      echo "$f"
      return 0
    fi
  done
  echo "local-smoke: no scenario file for '$s'" >&2
  return 1
}

# run <scenario file> <run id> [conductor args...]
run() {
  local file="$1" id="$2"
  shift 2
  local args=(run --scenario "$file"
    --ramp "$ramp" --hold "$hold" --drain "$drain"
    --endpoints "$endpoints" --node-metrics "$metrics"
    --agent http://localhost:9281 --agent http://localhost:9282
    --results "$results/$id" --run-id "$id"
    --key app.key:secret --env "bus=$bus_label" --env "host=laptop" --poll 5s)
  if [ -n "$log" ]; then
    args+=(--log "$log")
  fi
  if [ -n "${SERVER_IDLE_TIMEOUT:-}" ]; then
    args+=(--server-idle-timeout "$SERVER_IDLE_TIMEOUT")
  fi
  # A failing run is a result, not a script error: keep going.
  bin/ably-conductor "${args[@]}" "$@" || true
}

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
for shape in $shapes; do
  up
  run "bench/scenarios/shape-$shape.toml" "smoke-$shape-$stamp" --scale "$scale" || true
done
for sc in $scenarios; do
  file="$(scenario_file "$sc")"
  scale_args=()
  if [ -n "${SCALE:-}" ]; then
    scale_args=(--scale "$SCALE")
  fi
  up
  run "$file" "smoke-$(basename "$file" .toml)-$stamp" ${scale_args[@]+"${scale_args[@]}"} || true
done
if [ "${FAULT:-1}" = "1" ]; then
  up
  run bench/scenarios/shape-m.toml "smoke-m-nodekill-$stamp" --scale "$scale" --fault-hook "${compose[*]} kill node2" --fault-kind node-kill --fault-at 20s
fi
bin/ably-conductor report "$results"/smoke-*-"$stamp"/summary.json > "$results/report-$stamp.md" || true
echo "results: $results (report-$stamp.md)"
