#!/usr/bin/env bash
# 80-terminate: the lunch-break step. The Operator role may not stop or start
# instances, so "between runs" means terminate and re-create: this terminates every
# tagged instance (their EBS volumes go with them, the Postgres data included)
# and keeps the security group, instance profile, ECR images, STATE (runs,
# images, budget record) and the billing alarm. To bring the fleet back, run
# 20-postgres.sh, 25-pgdriver.sh (run 0a), 30-nats.sh, 40-nodes.sh, 50-loadgen.sh.
# Collect results first (70-collect.sh): nothing on the boxes survives.
# 90-teardown.sh is the end-of-day step; it also removes the security group.
#
#   bench/aws/80-terminate.sh --yes
#
# Optional: KEEP_POSTGRES=1 leaves the Postgres instances (and their data) up
#           and terminates the rest.
SCRIPT_NAME=80-terminate
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
require_env AWS_REGION
state_init

confirmed=0
for a in "$@"; do
  case "$a" in -y | --yes) confirmed=1 ;; *) die "unknown argument: $a" ;; esac
done
if [ "${ASSUME_YES:-0}" = 1 ]; then confirmed=1; fi
if [ "$confirmed" = 0 ] && ! is_dry; then
  die "this terminates every instance tagged Project=$PROJECT_TAG and deletes their disks (Postgres data included). Re-run with --yes."
fi
: "${KEEP_POSTGRES:=0}"

cost_checkpoint
rows=$(aws_r "" ec2 describe-instances --filters "$(project_filter)" Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].[InstanceId,Tags[?Key==`Role`]|[0].Value]') ||
  die "could not list the project's instances (credentials, permissions or throttling); nothing was terminated"
ids=$(
  {
    awk -v keep="$KEEP_POSTGRES" 'NF >= 1 && !(keep == 1 && $2 == "postgres") { print $1 }' <<<"$rows"
    if [ "$KEEP_POSTGRES" = 1 ]; then
      state_get '.instances // {} | to_entries[] | select(.value.role != "postgres") | .value.id'
    else
      state_get '.instances // {} | to_entries[] | .value.id'
    fi
  } | sed '/^$/d' | sort -u
)
if [ -n "$ids" ]; then
  # shellcheck disable=SC2086
  aws_w "" ec2 terminate-instances --instance-ids $ids >/dev/null
  # shellcheck disable=SC2086
  aws_w "" ec2 wait instance-terminated --instance-ids $ids
  log "terminated $(wc -w <<<"$ids" | tr -d ' ') instances"
else
  log "no instances to terminate"
fi

if [ "$KEEP_POSTGRES" = 1 ]; then
  _state_update '.instances |= ((. // {}) | with_entries(select(.value.role == "postgres")))'
else
  state_set_json '.instances' '{}'
  state_set_json '.postgres.instances' '{}'
fi
state_set_json '.nats' '{}'
state_set_json '.deployment' '{}'
state_set_json '.loadgen' '{}'
for id in $ids; do state_del_resource "$id"; done
cost_checkpoint
log_line 80-terminate "instances terminated$([ "$KEEP_POSTGRES" = 1 ] && echo ' (Postgres kept)'); now costing \$$(state_get '.budget.rate_usd_h')/h" "re-create what the next run needs (20-postgres.sh, 30-nats.sh, 40-nodes.sh, 50-loadgen.sh), or 90-teardown.sh --yes at end of day"
