#!/usr/bin/env bash
# 60-run.sh resolves a scenario name with or without .toml and builds the
# conductor command from the loadgen's flags (dry run, fixture state).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin" "$tmp/scenarios"
for t in aws ssh scp docker; do printf '#!/bin/sh\necho "real %s called" >&2\nexit 99\n' "$t" >"$tmp/bin/$t"; chmod +x "$tmp/bin/$t"; done
: >"$tmp/scenarios/smoke-1pct.toml"
fails=0
for arg in smoke-1pct smoke-1pct.toml; do
  cp "$HERE/fixtures/STATE.json" "$tmp/state.json"
  rm -f "$tmp"/cmd-*.sh
  out=$(PATH="$tmp/bin:$PATH" DRY_RUN=1 DRY_STATE_FILE="$tmp/state.json" PROJECT_TAG=fixture AWS_REGION=test-region-1 \
    SCENARIO_DIR="$tmp/scenarios" RUN_TIME_LIMIT=20m KEEP_WORK_DIR=1 LOG_FILE="$tmp/log" RESULTS_DIR="$tmp/results" \
    "$HERE/../60-run.sh" "$arg" 2>&1 >/dev/null) || { echo "FAIL 60-run.sh $arg exited non-zero"; echo "$out" | tail -5; fails=$((fails + 1)); continue; }
  if grep -q "scenario .* not found" <<<"$out"; then echo "FAIL $arg: scenario not resolved"; fails=$((fails + 1)); continue; fi
  if grep -q "tmp.*/scenarios/smoke-1pct.toml" <<<"$out"; then echo "ok   $arg resolves to smoke-1pct.toml"; else echo "FAIL $arg: wrong scenario file"; fails=$((fails + 1)); fi
  cmdfile=$(grep -o '[^ ]*/conductor-cmd.sh' <<<"$out" | head -1 || true)
  wd=$(find "${TMPDIR:-/tmp}" -maxdepth 2 -name conductor-cmd.sh -newer "$tmp/state.json" 2>/dev/null | head -1 || true)
  cmdfile=${wd:-$cmdfile}
  if [ -n "$cmdfile" ] && grep -q 'ably-conductor run --scenario /run-input/smoke-1pct.toml --inventory /run-input/inventory.json --results /results --run-id run-.* --node-vcpu 8 --node-memory-gb 16' "$cmdfile"; then
    echo "ok   $arg: conductor command has the run subcommand and node size"
  else
    echo "FAIL $arg: conductor command wrong: $(cat "${cmdfile:-/dev/null}" 2>/dev/null | tail -1)"
    fails=$((fails + 1))
  fi
  [ -z "${cmdfile:-}" ] || rm -rf "$(dirname "$cmdfile")"
done
[ "$fails" = 0 ] || exit 1
echo "run scenario tests passed"
