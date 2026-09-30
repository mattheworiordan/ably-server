#!/usr/bin/env bash
# summarise.sh turns run-0a CSVs into markdown tables (fixture data, no Postgres).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out=$("$HERE/../pgbench/summarise.sh" "$HERE/fixtures/results-io2.csv" "$HERE/fixtures/results-gp3.csv")
fails=0
has() { # <description> <fixed text>
  if grep -qF -- "$2" <<<"$out"; then printf 'ok   %s\n' "$1"; else printf 'FAIL %s (missing: %s)\n' "$1" "$2"; fails=$((fails + 1)); fi
}
has "io2 publishes per second table" "### publishes (messages) per second: io2"
has "gp3 publishes per second table" "### publishes (messages) per second: gp3"
has "batch30 at 8 clients on io2" "| batch30 | 30000 | 150000 |"
has "failed cells are marked" "120000 (FAILED)"
has "p99 latency table" "### p99 transaction latency, ms (the ACK floor): io2"
has "gp3 over io2 ratio" "| shipped | 0.83 | 0.70 |"
has "ratio uses the first file as the base" "### publishes per second, gp3 / io2"
[ "$fails" = 0 ] || exit 1
echo "summary tests passed"
