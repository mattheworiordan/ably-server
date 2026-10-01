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
# image on each box (--role generator or publisher). Override in the environment.
# --ntp-server is the Amazon Time Sync address chrony uses: the agent measures
# its box's clock offset from it (GET /v1/clock), the conductor records it at
# the start and end of every run and fails a run whose offset is above 5 ms.
SCRIPT_NAME=50-loadgen
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
require_env AWS_REGION
state_init
require_preflight
require_network_state
: "${LOADGEN_NTP_SERVER:=169.254.169.123:123}"
: "${LOADGEN_CMD:=ably-loadgen serve --listen=:${LOADGEN_AGENT_PORT} --metrics-listen=:${LOADGEN_METRICS_PORT} --role=generator --ntp-server=${LOADGEN_NTP_SERVER}}"
: "${PUBLISHER_CMD:=ably-loadgen serve --listen=:${LOADGEN_AGENT_PORT} --metrics-listen=:${LOADGEN_METRICS_PORT} --role=publisher --ntp-server=${LOADGEN_NTP_SERVER}}"

require_registry
tag=$(image_tag ably-loadgen "${LOADGEN_TAG:-}")
image=$(image_ref ably-loadgen "$tag")
check_image_pullable ably-loadgen "$tag"
dsns=$(postgres_dsns)

spend_gate
init_work_dir
ids=()
names=()
fresh=()
launch_group() { # <role> <count> <type> <command>
  local role=$1 count=$2 type=$3 cmd=$4 i name existed ud role_only
  ud="$BENCH_WORK_DIR/userdata-$role.sh"
  render_userdata "$ud" loadgen "IMAGE=$image" "CMD=$cmd"
  role_only="$BENCH_WORK_DIR/role-$role.sh"
  render_role_script loadgen "IMAGE=$image" "CMD=$cmd" >"$role_only"
  for i in $(seq 1 "$count"); do
    name=$(iname "$role" "$i")
    names+=("$name")
    existed=$(find_instance "$name")
    if [ -z "$existed" ]; then fresh+=("$name"); fi
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
INSTALL_COMPOSE=1 render_userdata "$cud" conductor "IMAGE=$image" "PGBENCH_IMAGE=$PGBENCH_IMAGE"
cname=$(iname conductor 1)
names+=("$cname")
if [ -z "$(find_instance "$cname")" ]; then conductor_fresh=1; else conductor_fresh=0; fi
ids+=("$(launch_instance "$cname" conductor "$CONDUCTOR_INSTANCE_TYPE" "$cud" "" 0)")

wait_instances_running "${ids[@]}"
refresh_instances
# No secret is in any user-data: the registry pull token (if any) and the conductor's DSNs go over SSH.
for name in ${fresh[@]+"${fresh[@]}"}; do deliver_boot_secrets "$name"; done
if [ "$conductor_fresh" = 1 ]; then deliver_boot_secrets "$cname" "BENCH_DSNS=$dsns"; fi
wait_boot "${names[@]}"
_state_update '.loadgen = {image:$img,generators:($g|tonumber),publishers:($p|tonumber),generator_cmd:$gc,publisher_cmd:$pc}' \
  --arg img "$image" --arg g "$LOADGEN_COUNT" --arg p "$PUBLISHER_COUNT" --arg gc "$LOADGEN_CMD" --arg pc "$PUBLISHER_CMD"
cost_checkpoint

if [ "${SKIP_OBSERVABILITY:-0}" != 1 ]; then
  "$BENCH_AWS_DIR/55-observability.sh"
fi
log_line 50-loadgen "$LOADGEN_COUNT generators ($LOADGEN_INSTANCE_TYPE), $PUBLISHER_COUNT publishers ($PUBLISHER_INSTANCE_TYPE), conductor up; image $tag" "60-run.sh smoke-1pct"
