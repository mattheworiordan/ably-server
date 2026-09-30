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
REPO="$(cd "$AWS_DIR/../.." && pwd)"
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
export NODE_COUNT=2 LOADGEN_COUNT=1 PUBLISHER_COUNT=1 SHARDS=1 BUS=nats IMAGE_TAG=testtag ALLOW_DIRTY=1
export LOG_FILE="$tmp/LOG.md" RESULTS_DIR="$tmp/results"
export KEEP_WORK_DIR=0 SKIP_BOOT_WAIT=0
: >"$tmp/bench.pub"

DEFAULT_SEQUENCE="00-preflight.sh 10-network.sh build-push.sh 20-postgres.sh 25-pgdriver.sh 30-nats.sh 40-nodes.sh 50-loadgen.sh 60-run.sh 65-run-0a.sh 70-collect.sh 80-terminate.sh 90-teardown.sh"
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
    60-run.sh) args=(smoke-1pct) ;;
    65-run-0a.sh) args=(io2) ;;
    70-collect.sh) args=(run-TEST) ;;
    80-terminate.sh | 90-teardown.sh) args=(--yes) ;;
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

# The CloudWatch billing-alarm path (used when the role cannot create a Budget)
# must call us-east-1 only, once per call.
cw_out="$tmp/cw.txt"
BUDGET_METHOD=cloudwatch "$AWS_DIR/00-preflight.sh" 2>&1 >/dev/null | grep -E '^DRYRUN aws (sns|cloudwatch)' >"$cw_out" || true
if [ "$(grep -c 'put-metric-alarm --alarm-name test-scale-billing-' "$cw_out")" != 2 ] ||
  ! grep -q 'sns create-topic --name test-scale-billing .*--region us-east-1' "$cw_out" ||
  grep -q 'test-region-1' <(grep -E 'test-scale-billing' "$cw_out"); then
  echo "FAIL: the CloudWatch billing alarm path did not produce the expected us-east-1 calls:" >&2
  cat "$cw_out" >&2
  status=1
else
  echo "cloudwatch alarm path calls us-east-1 only"
fi

# The default billing guard is CloudWatch (Budgets can be created but not read by the Operator role),
# and the optional Budgets path tolerates an existing budget.
default_out="$tmp/default-alarm.txt"
"$AWS_DIR/00-preflight.sh" 2>&1 >/dev/null | grep -E '^DRYRUN aws (budgets|cloudwatch put-metric-alarm)' >"$default_out" || true
if grep -q 'budgets create-budget --account-id 111111111111 --budget <' "$default_out" ||
  [ "$(grep -c 'put-metric-alarm --alarm-name test-scale-billing-' "$default_out")" != 2 ]; then
  echo "FAIL: the default preflight should create CloudWatch billing alarms and no budget:" >&2
  cat "$default_out" >&2
  status=1
else
  echo "default billing guard is CloudWatch"
fi
bud_out="$tmp/budgets.txt"
BUDGET_METHOD=budgets "$AWS_DIR/00-preflight.sh" 2>&1 >/dev/null | grep -E '^DRYRUN aws (budgets|cloudwatch put-metric-alarm)' >"$bud_out" || true
if ! grep -q 'budgets create-budget --account-id 111111111111 --budget .*budget.json' "$bud_out" || grep -q 'put-metric-alarm --alarm-name test-scale-billing-' "$bud_out"; then
  echo "FAIL: BUDGET_METHOD=budgets should create a budget and no CloudWatch billing alarm:" >&2
  cat "$bud_out" >&2
  status=1
else
  echo "budgets path creates a budget"
fi

# A placement group is attempted only on request; by default no script mentions one at launch.
pg_default=$(grep -c 'create-placement-group' "$out" || true)
pg_on=$(USE_PLACEMENT_GROUP=1 "$AWS_DIR/10-network.sh" 2>&1 >/dev/null | grep -c 'DRYRUN aws ec2 create-placement-group' || true)
if [ "$pg_default" != 0 ] || [ "$pg_on" != 1 ] || grep -q -- '--key-name\|import-key-pair\|create-key-pair\|--placement GroupName' "$out"; then
  echo "FAIL: placement groups and key pairs: default calls=$pg_default, with USE_PLACEMENT_GROUP=1 calls=$pg_on (want 0 and 1), or a key pair / group name was used" >&2
  status=1
else
  echo "no placement group or key pair by default; the group is attempted with USE_PLACEMENT_GROUP=1"
fi
# Nothing may call RDS, stop or start an instance, or delete a standalone volume: the role is denied all of them.
if grep -Eq 'aws (rds|budgets describe|ce |servicequotas) |ec2 (stop-instances|start-instances|delete-volume|create-volume|attach-volume)' "$out"; then
  echo "FAIL: a call the Operator role is denied (or that a volume-deletion-free design avoids) was printed:" >&2
  grep -E 'aws (rds|budgets describe|ce |servicequotas) |ec2 (stop-instances|start-instances|delete-volume|create-volume|attach-volume)' "$out" >&2
  status=1
else
  echo "no rds, stop/start, or standalone volume calls"
fi

# Normalise volatile text.
sed -E \
  -e "s#[^ ]*/bench-aws\.[A-Za-z0-9]+/#<work>/#g" \
  -e "s#$tmp#<tmp>#g" \
  -e "s#$REPO#<repo>#g" \
  -e 's#[0-9]{8}T[0-9]{6}Z#<timestamp>#g' \
  -e 's#Start=[0-9-]{10},End=[0-9-]{10}#Start=<date>,End=<date>#' \
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
