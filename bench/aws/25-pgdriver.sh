#!/usr/bin/env bash
# 25-pgdriver: one driver box for run 0a (pgbench against the Postgres box). It needs
# only Docker; pgbench runs from the postgres image (from BASE_IMAGE_REGISTRY). No image
# registry of ours is involved. Create or reuse.
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
render_userdata "$ud" pgdriver "PGBENCH_IMAGE=$PGBENCH_IMAGE"
name=$(iname pgdriver 1)
fresh=0
if [ -z "$(find_instance "$name")" ]; then fresh=1; fi
id=$(launch_instance "$name" pgdriver "$PGDRIVER_INSTANCE_TYPE" "$ud")
wait_instances_running "$id"
refresh_instances
# Only a registry pull token (if any) goes over SSH; nothing secret is in the user-data.
if [ "$fresh" = 1 ]; then deliver_boot_secrets "$name"; fi
wait_boot "$name"
cost_checkpoint
log_line 25-pgdriver "pgbench driver $name up ($PGDRIVER_INSTANCE_TYPE)" "65-run-0a.sh io2"
