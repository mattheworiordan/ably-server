#!/usr/bin/env bash
# 80-stop: stop (not terminate) every tagged instance and, by default, the RDS
# instances, between runs. Stopped instances still bill for their disks, and
# stopped RDS instances still bill for storage and restart by themselves after
# seven days; 90-teardown is the end-of-day step, this is the lunch-break step.
#
#   bench/aws/80-stop.sh
#
# Optional: STOP_RDS=0 (leave Postgres running).
SCRIPT_NAME=80-stop
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
require_env AWS_REGION
state_init
: "${STOP_RDS:=1}"

cost_checkpoint
running=$(aws_r "" ec2 describe-instances --filters "$(project_filter)" Name=instance-state-name,Values=pending,running \
  --query 'Reservations[].Instances[].InstanceId')
ids=$(
  {
    tr -s '[:space:]' '\n' <<<"$running"
    state_get '.instances // {} | to_entries[] | select(.value.running == true) | .value.id'
  } | sed '/^$/d' | sort -u
)
if [ -n "$ids" ]; then
  # shellcheck disable=SC2086
  aws_w "" ec2 stop-instances --instance-ids $ids >/dev/null
  # shellcheck disable=SC2086
  aws_w "" ec2 wait instance-stopped --instance-ids $ids
  log "stopped $(wc -w <<<"$ids" | tr -d ' ') instances"
else
  log "no running instances"
fi
_state_update '.instances |= (. // {} | map_values(.running = false))'

if [ "$STOP_RDS" = 1 ]; then
  for id in $(state_get '.postgres.instances // {} | keys[]'); do
    aws_w_tolerate 'InvalidDBInstanceState' "" rds stop-db-instance --db-instance-identifier "$id" >/dev/null
    log "stopping RDS instance $id"
  done
  for id in $(state_get '.postgres.instances // {} | keys[]'); do
    wait_until "RDS instance $id stopped" 2400 30 rds_status_is "$id" stopped
  done
  _state_update '.postgres.instances |= (. // {} | map_values(.running = false))'
fi
cost_checkpoint
log_line 80-stop "fleet stopped (RDS $([ "$STOP_RDS" = 1 ] && echo stopped || echo left running)); now costing \$$(state_get '.budget.rate_usd_h')/h" "80-start.sh to resume, or 90-teardown.sh --yes at end of day"
