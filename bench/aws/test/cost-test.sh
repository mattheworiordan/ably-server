#!/usr/bin/env bash
# cost-estimate.sh reads STATE and prints the hourly rate (fixture state, no AWS).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cp "$HERE/fixtures/STATE.json" "$tmp/state.json"
out=$(NO_AWS=1 STATE_FILE="$tmp/state.json" PROJECT_TAG=fixture "$HERE/../cost-estimate.sh")
json=$(NO_AWS=1 STATE_FILE="$tmp/state.json" PROJECT_TAG=fixture "$HERE/../cost-estimate.sh" --json)
fails=0
check() { if [ "$2" = "$3" ]; then printf 'ok   %s\n' "$1"; else printf 'FAIL %s: want %s got %s\n' "$1" "$2" "$3"; fails=$((fails + 1)); fi; }
# 4 x c7i.2xlarge (0.36) + c7i.4xlarge (0.71) + c7i.8xlarge (1.43) + r7i.4xlarge (1.06) + io2 data volume ((1000*0.125 + 20000*0.065)/730 = 1.952)
check "hourly rate" 6.592 "$(jq -r '.hourly_usd * 1000 | round / 1000' <<<"$json")"
check "alarm level" 750 "$(jq -r .alarm_usd <<<"$json")"
contains() { if grep -q -- "$2" <<<"$out"; then printf 'ok   %s\n' "$1"; else printf 'FAIL %s\n' "$1"; fails=$((fails + 1)); fi; }
contains "table shows the rate" 'running fleet: *\$6.59 per hour'
contains "table lists the database volume" 'EBS data fixture-pg-io2'
[ "$fails" = 0 ] || exit 1
echo "cost tests passed"
