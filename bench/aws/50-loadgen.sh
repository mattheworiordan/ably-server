#!/usr/bin/env bash
# 50-loadgen: load generators, REST publishers and the conductor box
# (Prometheus and Grafana run there). Create or reuse; RECONFIGURE=1 re-applies
# the image and command to live generator and publisher boxes.
#
#   bench/aws/50-loadgen.sh
#
# Optional: LOADGEN_COUNT (3), PUBLISHER_COUNT (2), LOADGEN_TAG, LOADGEN_CMD,
#           PUBLISHER_CMD, SKIP_OBSERVABILITY=1.
#
# LOADGEN_CMD and PUBLISHER_CMD are the commands run inside the ably-loadgen
# image on each box. The defaults below are provisional until the load
# generator branch fixes its flags; override them in the environment.
SCRIPT_NAME=50-loadgen
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
require_env AWS_REGION
state_init
require_network_state
: "${LOADGEN_CMD:=ably-loadgen serve --listen=:${LOADGEN_AGENT_PORT} --metrics-listen=:${LOADGEN_METRICS_PORT}}"
: "${PUBLISHER_CMD:=ably-loadgen serve --role=publisher --listen=:${LOADGEN_AGENT_PORT} --metrics-listen=:${LOADGEN_METRICS_PORT}}"

tag=$(image_tag ably-loadgen "${LOADGEN_TAG:-}")
image="${ECR_REGISTRY}/${ECR_REPO_LOADGEN}:${tag}"
dsns=$(postgres_dsns)

cost_checkpoint
init_work_dir
ids=()
names=()
launch_group() { # <role> <count> <type> <command>
  local role=$1 count=$2 type=$3 cmd=$4 i name existed ud role_only
  ud="$BENCH_WORK_DIR/userdata-$role.sh"
  render_userdata "$ud" loadgen "IMAGE=$image" "CMD=$cmd"
  role_only="$BENCH_WORK_DIR/role-$role.sh"
  render_template "$BENCH_AWS_DIR/templates/loadgen.sh" "IMAGE=$image" "CMD=$cmd" >"$role_only"
  for i in $(seq 1 "$count"); do
    name=$(iname "$role" "$i")
    names+=("$name")
    existed=$(find_instance "$name")
    ids+=("$(launch_instance "$name" "$role" "$type" "$ud")")
    if [ -n "$existed" ] && [ "${RECONFIGURE:-0}" = 1 ]; then
      apply_role_script "$name" "$role_only"
      log "reconfigured $name"
    fi
  done
}
launch_group loadgen "$LOADGEN_COUNT" "$LOADGEN_INSTANCE_TYPE" "$LOADGEN_CMD"
launch_group publisher "$PUBLISHER_COUNT" "$PUBLISHER_INSTANCE_TYPE" "$PUBLISHER_CMD"

# Conductor: no placement group (it only talks to the others), compose plugin on.
cud="$BENCH_WORK_DIR/userdata-conductor.sh"
INSTALL_COMPOSE=1 render_userdata "$cud" conductor "IMAGE=$image" "PGBENCH_IMAGE=$PGBENCH_IMAGE" "DSNS=$dsns"
cname=$(iname conductor 1)
names+=("$cname")
ids+=("$(launch_instance "$cname" conductor "$CONDUCTOR_INSTANCE_TYPE" "$cud" "" 0)")

wait_instances_running "${ids[@]}"
refresh_instances
wait_boot "${names[@]}"
_state_update '.loadgen = {image:$img,generators:($g|tonumber),publishers:($p|tonumber),generator_cmd:$gc,publisher_cmd:$pc}' \
  --arg img "$image" --arg g "$LOADGEN_COUNT" --arg p "$PUBLISHER_COUNT" --arg gc "$LOADGEN_CMD" --arg pc "$PUBLISHER_CMD"
cost_checkpoint

if [ "${SKIP_OBSERVABILITY:-0}" != 1 ]; then
  "$BENCH_AWS_DIR/55-observability.sh"
fi
log_line 50-loadgen "$LOADGEN_COUNT generators ($LOADGEN_INSTANCE_TYPE), $PUBLISHER_COUNT publishers ($PUBLISHER_INSTANCE_TYPE), conductor up; image $tag" "60-run.sh smoke-1pct"
