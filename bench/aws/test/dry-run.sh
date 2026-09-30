#!/usr/bin/env bash
# Runs the numbered scripts with DRY_RUN=1 against fake aws/ssh/scp commands
# that fail if they are ever reached, and compares the printed call sequence
# with test/expected-calls.txt.
#
#   bench/aws/test/dry-run.sh             compare
#   UPDATE_EXPECTED=1 bench/aws/test/dry-run.sh   rewrite the expected file
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWS_DIR="$(cd "$HERE/.." && pwd)"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin"
for tool in aws ssh scp docker; do
  cat >"$tmp/bin/$tool" <<EOT
#!/bin/sh
echo "TEST FAILURE: real $tool was called: \$*" >&2
exit 99
EOT
  chmod +x "$tmp/bin/$tool"
done
# Keep the fakes first on PATH, but leave the rest for jq, awk and friends.
export PATH="$tmp/bin:$PATH"

export DRY_RUN=1 DRY_STATE_FILE="$tmp/state.json"
export AWS_ACCOUNT_ID=111111111111 AWS_REGION=test-region-1 PROJECT_TAG=test-scale
export ADMIN_CIDR=203.0.113.9/32 SSH_PUBLIC_KEY_PATH="$tmp/bench.pub" ALARM_EMAIL=ops@example.invalid
export RDS_PASSWORD=Secretpassword0123456789 RUN_TIME_LIMIT=45m
export NODE_COUNT=2 LOADGEN_COUNT=1 PUBLISHER_COUNT=1 SHARDS=1 BUS=nats
export LOG_FILE="$tmp/LOG.md" RESULTS_DIR="$tmp/results"
export KEEP_WORK_DIR=0 SKIP_BOOT_WAIT=0
: >"$tmp/bench.pub"

DEFAULT_SEQUENCE="00-preflight.sh 10-network.sh 20-postgres.sh 25-pgdriver.sh 30-nats.sh 40-nodes.sh 50-loadgen.sh 60-run.sh 70-collect.sh 80-stop.sh 80-start.sh 90-teardown.sh"
read -r -a SEQUENCE <<<"${SEQUENCE:-$DEFAULT_SEQUENCE}"
out="$tmp/calls.txt"
: >"$out"
status=0
for s in "${SEQUENCE[@]}"; do
  if [ ! -x "$AWS_DIR/$s" ]; then
    echo "skip $s (not present yet)"
    continue
  fi
  args=()
  case "$s" in
    60-run.sh) args=(smoke-1pct.yaml) ;;
    70-collect.sh) args=(run-TEST) ;;
    90-teardown.sh) args=(--yes) ;;
  esac
  rc=0
  "$AWS_DIR/$s" "${args[@]}" >/dev/null 2>"$tmp/stderr.$s" || rc=$?
  {
    echo "### $s ${args[*]}"
    grep -E '^(DRYRUN|TEST FAILURE)' "$tmp/stderr.$s" || true
  } >>"$out"
  if [ "$rc" != 0 ]; then
    echo "FAIL: $s exited $rc:" >&2
    tail -5 "$tmp/stderr.$s" >&2
    status=1
  fi
done

# Normalise volatile text.
sed -E \
  -e "s#file://[^ ]*/bench-aws\.[A-Za-z0-9]+/#file://<work>/#g" \
  -e "s#$tmp#<tmp>#g" \
  -e 's#[0-9]{8}T[0-9]{6}Z#<timestamp>#g' \
  "$out" >"$tmp/calls.norm"

if grep -q "$RDS_PASSWORD" "$tmp/calls.norm" "$tmp"/stderr.* 2>/dev/null; then
  echo "FAIL: the database password leaked into the dry-run output" >&2
  status=1
fi
if grep -q 'TEST FAILURE' "$tmp/calls.norm"; then
  echo "FAIL: a real aws/ssh/scp/docker command was reached" >&2
  status=1
fi

if [ "${UPDATE_EXPECTED:-0}" = 1 ]; then
  cp "$tmp/calls.norm" "$HERE/expected-calls.txt"
  echo "rewrote $HERE/expected-calls.txt ($(wc -l <"$HERE/expected-calls.txt") lines)"
else
  if ! diff -u "$HERE/expected-calls.txt" "$tmp/calls.norm"; then
    echo "FAIL: dry-run call sequence differs from expected-calls.txt" >&2
    status=1
  else
    echo "dry-run call sequence matches expected-calls.txt ($(wc -l <"$HERE/expected-calls.txt") lines)"
  fi
fi
exit "$status"
