#!/usr/bin/env bash
# 80-start: start the stopped fleet again (Postgres first), then refresh the
# public addresses in STATE (private addresses, and so every inventory and
# scrape config, stay the same).
#
#   bench/aws/80-start.sh
SCRIPT_NAME=80-start
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
require_env AWS_REGION
state_init

cost_checkpoint
rds_ids=$(state_get '.postgres.instances // {} | keys[]')
for id in $rds_ids; do
  aws_w_tolerate 'InvalidDBInstanceState' "" rds start-db-instance --db-instance-identifier "$id" >/dev/null
  log "starting RDS instance $id"
done
for id in $rds_ids; do
  aws_w "" rds wait db-instance-available --db-instance-identifier "$id"
done
if [ -n "$rds_ids" ]; then _state_update '.postgres.instances |= map_values(.running = true)'; fi

ids=$(state_get '.instances // {} | to_entries[] | .value.id' | sort -u)
if [ -n "$ids" ]; then
  # shellcheck disable=SC2086
  aws_w "" ec2 start-instances --instance-ids $ids >/dev/null
  # shellcheck disable=SC2086
  aws_w "" ec2 wait instance-running --instance-ids $ids
  _state_update '.instances |= map_values(.running = true)'
  refresh_instances
  if [ "${SKIP_REARM:-0}" != 1 ]; then
    # The boot-time dead-man switch does not survive a stop: arm it again.
    for name in $(state_instance_names); do
      wait_ssh "$name"
      ssh_do "$name" "sudo shutdown -c 2>/dev/null; sudo shutdown -h +$((FLEET_MAX_UPTIME_H * 60))"
    done
  fi
  log "started $(wc -w <<<"$ids" | tr -d ' ') instances"
fi
cost_checkpoint
log_line 80-start "fleet started; now costing \$$(state_get '.budget.rate_usd_h')/h" "check readiness (curl each node /readyz), then 60-run.sh"
