#!/usr/bin/env bash
# Sample `docker stats --no-stream` in a loop into a CSV until killed.
# CPU% is per-core (100% = one full core; the Docker VM has 18).
#
#   bench/sample-docker-stats.sh out.csv [container-name-regex] [pause-seconds]
set -uo pipefail
out="$1"
filter="${2:-.}"
pause="${3:-0.2}"
echo "epoch,name,cpu_pct,mem_usage,mem_pct,net_io,block_io,pids" > "$out"
while true; do
  now="$(gdate +%s.%N)"
  docker stats --no-stream \
    --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}' 2>/dev/null \
    | grep -E "$filter" | sed "s/^/${now},/" >> "$out"
  sleep "$pause"
done
