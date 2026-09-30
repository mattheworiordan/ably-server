#!/usr/bin/env bash
# 25-pgdriver: one driver box for run 0a (pgbench against RDS). It needs
# only Docker; pgbench runs from the postgres image. Create or reuse.
#
#   bench/aws/25-pgdriver.sh
SCRIPT_NAME=25-pgdriver
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
require_env AWS_REGION
state_init
require_preflight
require_network_state

spend_gate
init_work_dir
ud="$BENCH_WORK_DIR/userdata-pgdriver.sh"
ECR_LOGIN_REGISTRY='' render_userdata "$ud" pgdriver "PGBENCH_IMAGE=$PGBENCH_IMAGE"
name=$(iname pgdriver 1)
id=$(launch_instance "$name" pgdriver "$PGDRIVER_INSTANCE_TYPE" "$ud")
wait_instances_running "$id"
refresh_instances
wait_boot "$name"
cost_checkpoint
log_line 25-pgdriver "pgbench driver $name up ($PGDRIVER_INSTANCE_TYPE)" "65-run-0a.sh io2"
