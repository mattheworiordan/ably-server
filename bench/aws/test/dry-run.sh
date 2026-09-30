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
# The main sequence uses ghcr; the ecr and none variants are checked further down.
export IMAGE_REGISTRY=ghcr.io/test-owner IMAGE_REGISTRY_KIND=ghcr GHCR_USER=test-user
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

# ---- image registry kinds -------------------------------------------------------------------
# ghcr (the main sequence above): no ECR call anywhere, and no invalid-parameter ECR probe.
if grep -q 'aws ecr' "$out"; then
  echo "FAIL: IMAGE_REGISTRY_KIND=ghcr printed an ecr call:" >&2
  grep 'aws ecr' "$out" >&2
  status=1
else
  echo "ghcr: no ecr calls"
fi
if ! grep -q 'gh auth token | docker login ghcr.io -u test-user --password-stdin' "$out" ||
  ! grep -q 'docker push ghcr.io/test-owner/ably-server:testtag' "$out" ||
  ! grep -q 'docker push ghcr.io/test-owner/ably-loadgen:testtag' "$out"; then
  echo "FAIL: ghcr build-push did not log in with gh and push to ghcr.io/test-owner" >&2
  status=1
else
  echo "ghcr: build-push logs in with gh and pushes both images"
fi

# Default kind: with no IMAGE_REGISTRY and no kind the registry is ghcr, and preflight and the network step
# make no ECR and no IAM call (the owner is only looked up when an image reference is first needed).
def_out="$tmp/default-kind.txt"
for s in 00-preflight.sh 10-network.sh; do
  IMAGE_REGISTRY_KIND='' IMAGE_REGISTRY='' DRY_STATE_FILE="$tmp/state-def.json" "$AWS_DIR/$s" >/dev/null 2>"$tmp/def.$s" || {
    echo "FAIL: $s exited non-zero with no registry settings" >&2
    status=1
  }
  grep -E '^DRYRUN' "$tmp/def.$s" >>"$def_out" || true
done
if grep -Eqi 'aws ecr|aws iam' "$def_out" || ! grep -q 'image registry: ghcr.io' "$tmp/def.00-preflight.sh"; then
  echo "FAIL: the default registry kind is not ghcr, or the default path touches ECR or IAM" >&2
  status=1
else
  echo "default registry kind is ghcr; no ECR or IAM call"
fi

# none: the run 0a path (00, 10, 20, 25, 65, 90) needs no registry, no ECR and no IAM entity.
none_out="$tmp/none-calls.txt"
: >"$none_out"
for s in 00-preflight.sh 10-network.sh 20-postgres.sh 25-pgdriver.sh 65-run-0a.sh 90-teardown.sh; do
  args=()
  case "$s" in
    65-run-0a.sh) args=(io2) ;;
    90-teardown.sh) args=(--yes) ;;
  esac
  rc=0
  IMAGE_REGISTRY_KIND=none IMAGE_REGISTRY='' DRY_STATE_FILE="$tmp/state-none.json" \
    "$AWS_DIR/$s" "${args[@]}" >/dev/null 2>"$tmp/none.$s" || rc=$?
  grep -E '^(DRYRUN|TEST FAILURE)' "$tmp/none.$s" >>"$none_out" || true
  if [ "$rc" != 0 ]; then
    echo "FAIL: $s exited $rc with IMAGE_REGISTRY_KIND=none:" >&2
    tail -5 "$tmp/none.$s" >&2
    status=1
  fi
done
if grep -Eqi 'aws ecr|ecr\.|ghcr|aws iam|resourcegroupstagging.*ecr' "$none_out"; then
  echo "FAIL: the run 0a path with IMAGE_REGISTRY_KIND=none touched ECR, ghcr or IAM:" >&2
  grep -Ei 'aws ecr|ecr\.|ghcr|aws iam' "$none_out" >&2
  status=1
else
  echo "none: the run 0a path (00, 10, 20, 25, 65, 90) makes no ECR, ghcr or IAM call"
fi
# ... and the fleet scripts refuse, naming the variable.
IMAGE_REGISTRY_KIND=none IMAGE_REGISTRY='' DRY_STATE_FILE="$tmp/state-none2.json" "$AWS_DIR/10-network.sh" >/dev/null 2>&1
fleet_ok=1
for s in 40-nodes.sh 50-loadgen.sh; do
  rc=0
  IMAGE_REGISTRY_KIND=none IMAGE_REGISTRY='' DRY_STATE_FILE="$tmp/state-none2.json" "$AWS_DIR/$s" >/dev/null 2>"$tmp/none.$s" || rc=$?
  if [ "$rc" = 0 ] || ! grep -q 'no registry to pull .* from. Set IMAGE_REGISTRY' "$tmp/none.$s"; then
    echo "FAIL: $s should stop when IMAGE_REGISTRY_KIND=none and say to set IMAGE_REGISTRY (exit $rc):" >&2
    tail -3 "$tmp/none.$s" >&2
    status=1
    fleet_ok=0
  fi
done
if [ "$fleet_ok" = 1 ]; then echo "none: 40-nodes.sh and 50-loadgen.sh stop and name IMAGE_REGISTRY"; fi

# ecr: preflight looks for each repository and creates a missing one (no invalid-name probe);
# build-push logs in to ECR and pushes.
ecr_out="$tmp/ecr-calls.txt"
for s in 00-preflight.sh build-push.sh 10-network.sh; do
  IMAGE_REGISTRY_KIND=ecr IMAGE_REGISTRY='' DRY_STATE_FILE="$tmp/state-ecr.json" "$AWS_DIR/$s" >/dev/null 2>"$tmp/ecr.$s" || {
    echo "FAIL: $s exited non-zero with IMAGE_REGISTRY_KIND=ecr" >&2
    status=1
  }
  grep -E '^DRYRUN' "$tmp/ecr.$s" >>"$ecr_out" || true
done
if grep -q "create-repository --repository-name 'INVALID NAME'" "$ecr_out" ||
  [ "$(grep -c 'ecr describe-repositories --repository-names test-scale/ably-server' "$ecr_out")" -lt 1 ] ||
  ! grep -q 'ecr create-repository --repository-name test-scale/ably-server --tags' "$ecr_out" ||
  ! grep -q 'docker push 111111111111.dkr.ecr.test-region-1.amazonaws.com/test-scale/ably-server:testtag' "$ecr_out" ||
  ! grep -q 'iam create-role --role-name test-scale-instance' "$ecr_out"; then
  echo "FAIL: the ecr calls are not the expected describe, create-if-missing, push and instance profile:" >&2
  grep -E 'ecr|iam' "$ecr_out" >&2
  status=1
else
  echo "ecr: describe then create-if-missing, no invalid-name probe, push, instance profile"
fi

# Tagging API denied: teardown lists service by service (TAGGING_API=off skips the first attempt).
fb_out="$tmp/fallback-calls.txt"
TAGGING_API=off DRY_STATE_FILE="$tmp/state-fb.json" "$AWS_DIR/90-teardown.sh" --yes 2>&1 >/dev/null | grep -E '^DRYRUN' >"$fb_out" || true
for want in 'ec2 describe-network-interfaces --filters Name=tag:Project' 'iam get-role --role-name test-scale-instance' \
  'iam get-instance-profile --instance-profile-name test-scale-instance' \
  'cloudwatch describe-alarms --alarm-name-prefix test-scale-' 'sns list-topics'; do
  if ! grep -q -- "$want" "$fb_out"; then
    echo "FAIL: per-service teardown listing lacks: $want" >&2
    status=1
  fi
done
if grep -q resourcegroupstaggingapi "$fb_out" || grep -Eq 'iam list-' "$fb_out" "$out" "$ecr_out"; then
  echo "FAIL: TAGGING_API=off still called the tagging API, or a script listed IAM entities (list-roles, list-instance-profiles)" >&2
  status=1
else
  echo "teardown falls back to per-service listings without the tagging API"
fi
if ! grep -q resourcegroupstaggingapi "$out"; then
  echo "FAIL: the default teardown should try the tagging API first" >&2
  status=1
else
  echo "teardown tries the tagging API first"
fi

# Docker Hub pull limits: preflight warns for a big fleet on docker.io, not with a mirror.
warn_default=$("$AWS_DIR/00-preflight.sh" 2>&1 >/dev/null | grep -c 'Docker Hub' || true)
warn_mirror=$(BASE_IMAGE_REGISTRY=ghcr.io/test-owner "$AWS_DIR/00-preflight.sh" 2>&1 >/dev/null | grep -c 'Docker Hub' || true)
if [ "$warn_default" != 1 ] || [ "$warn_mirror" != 0 ]; then
  echo "FAIL: Docker Hub warning: default=$warn_default (want 1), with a mirror=$warn_mirror (want 0)" >&2
  status=1
else
  echo "preflight warns about Docker Hub pull limits on a big fleet, and not with BASE_IMAGE_REGISTRY set"
fi

# mirror: pull, retag, push every third party image; refuses when BASE_IMAGE_REGISTRY is not Docker Hub.
mir_out="$tmp/mirror-calls.txt"
"$AWS_DIR/build-push.sh" mirror 2>&1 >/dev/null | grep -E '^DRYRUN' >"$mir_out" || true
if [ "$(grep -c '^DRYRUN docker pull --platform linux/amd64 ' "$mir_out")" != 8 ] ||
  [ "$(grep -c '^DRYRUN docker push ghcr.io/test-owner/' "$mir_out")" != 8 ] ||
  ! grep -q 'docker tag quay.io/prometheuscommunity/postgres-exporter:v0.15.0 ghcr.io/test-owner/postgres-exporter:v0.15.0' "$mir_out" ||
  ! grep -q 'docker tag prom/prometheus:v2.55.1 ghcr.io/test-owner/prometheus:v2.55.1' "$mir_out"; then
  echo "FAIL: build-push.sh mirror did not pull, retag and push the 8 third party images:" >&2
  cat "$mir_out" >&2
  status=1
else
  echo "mirror: 8 images pulled, retagged and pushed to the registry"
fi
if BASE_IMAGE_REGISTRY=ghcr.io/test-owner "$AWS_DIR/build-push.sh" mirror >/dev/null 2>&1; then
  echo "FAIL: mirror should refuse to run with BASE_IMAGE_REGISTRY already pointing at a mirror" >&2
  status=1
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
