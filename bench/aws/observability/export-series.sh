#!/usr/bin/env bash
# Export named Prometheus series over a time range as one JSON file per query.
# Runs on the conductor box (or anywhere that can reach Prometheus).
#
#   export-series.sh <start-rfc3339> <end-rfc3339> <out-dir> [series-file] [step]
#
# The series file has one "name<TAB>PromQL" line per export; # starts a comment.
set -euo pipefail
start=${1:?start time}
end=${2:?end time}
out=${3:?output directory}
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
series=${4:-$here/export-series.txt}
step=${5:-5s}
prom=${PROMETHEUS_URL:-http://127.0.0.1:9090}
mkdir -p "$out"
while IFS=$'\t' read -r name query; do
  case "$name" in '' | '#'*) continue ;; esac
  if ! curl -fsS --get "$prom/api/v1/query_range" \
    --data-urlencode "query=$query" --data-urlencode "start=$start" \
    --data-urlencode "end=$end" --data-urlencode "step=$step" >"$out/$name.json"; then
    echo "export-series: $name failed" >&2
    rm -f "$out/$name.json"
  fi
done <"$series"
echo "exported $(find "$out" -name '*.json' | wc -l | tr -d ' ') series to $out"
