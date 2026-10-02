#!/usr/bin/env bash
# 40-nodes: NODE_COUNT ably-server nodes (in the placement group when there is one). Boot installs
# Docker and chrony, raises fd limits and the port range, pulls the image from the image
# registry (IMAGE_REGISTRY; stops when IMAGE_REGISTRY_KIND=none) and starts it with --mode=cluster.
# /metrics is on the debug listener.
# Create or reuse; RECONFIGURE=1 re-applies image, bus and flags to live nodes;
# SCALE_DOWN=1 terminates nodes above NODE_COUNT (run 7).
#
#   BUS=nats bench/aws/40-nodes.sh
#   BUS=none SERVER_TAG=<main sha> bench/aws/40-nodes.sh      # run 1a: the shipped bus (no --bus flag on main)
#
# BUS: nats | postgres | pgnotify | none (none omits --bus: main runs
# pgnotify, this branch infers postgres; DESIGN.md §7.2).
# Optional: SERVER_TAG, NODE_COUNT, ABLY_SERVER_EXTRA_FLAGS, NODE_GOMAXPROCS,
#           NODE_GOMEMLIMIT (13GiB), RECONFIGURE, SCALE_DOWN.
SCRIPT_NAME=40-nodes
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
require_env AWS_REGION
state_init
require_preflight
require_network_state
: "${BUS:=nats}"
: "${NODE_GOMEMLIMIT:=13GiB}"
: "${ABLY_SERVER_EXTRA_FLAGS:=}"

require_registry
tag=$(image_tag ably-server "${SERVER_TAG:-}")
image=$(image_ref ably-server "$tag")
check_image_pullable ably-server "$tag"
dsns=$(postgres_dsns)
api_key=$(ensure_api_key)

flags=""
case "$BUS" in
  nats)
    nats_urls=$(state_get '.nats.urls')
    [ -n "$nats_urls" ] || die "BUS=nats but STATE has no NATS cluster; run 30-nats.sh first"
    flags="--bus=nats --nats-url=$nats_urls"
    ;;
  postgres | pgnotify) flags="--bus=$BUS" ;;
  none) flags="" ;;
  *) die "BUS must be nats, postgres, pgnotify or none (got $BUS)" ;;
esac
flags="${flags}${ABLY_SERVER_EXTRA_FLAGS:+ $ABLY_SERVER_EXTRA_FLAGS}"
env_args="-e GOMEMLIMIT=$NODE_GOMEMLIMIT"
if [ -n "${NODE_GOMAXPROCS:-}" ]; then env_args+=" -e GOMAXPROCS=$NODE_GOMAXPROCS"; fi

spend_gate
init_work_dir
role_args=("IMAGE=$image" "ENV_ARGS=$env_args" "PORT=$SERVER_PORT"
  "DEBUG_PORT=$SERVER_DEBUG_PORT" "FLAGS=$flags")
# The API key and the DSNs (database password) are not in the user-data: they go over SSH.
node_secrets=("BENCH_API_KEY=$api_key" "BENCH_DSN=$dsns")
ud="$BENCH_WORK_DIR/userdata-node.sh"
render_userdata "$ud" node "${role_args[@]}"
role_only="$BENCH_WORK_DIR/role-node.sh"
render_role_script node "${role_args[@]}" >"$role_only"

# Scale down first (run 7 goes 20 -> 10 -> 5).
if [ "${SCALE_DOWN:-0}" = 1 ]; then
  doomed=$(state_get ".instances // {} | to_entries[] | select(.value.role == \"node\") | .key" | awk -F- -v keep="$NODE_COUNT" '{ if ($NF+0 > keep) print $0 }')
  for name in $doomed; do
    doomed_id=$(inst_field "$name" id)
    aws_w "" ec2 terminate-instances --instance-ids "$doomed_id" >/dev/null
    _state_update 'del(.instances[$n])' --arg n "$name"
    state_del_resource "$doomed_id"
    log "terminated $name (scale down to $NODE_COUNT)"
  done
fi

ids=()
names=()
for i in $(seq 1 "$NODE_COUNT"); do
  name=$(iname node "$i")
  names+=("$name")
  existed=$(find_instance "$name")
  ids+=("$(launch_instance "$name" node "$NODE_INSTANCE_TYPE" "$ud")")
  if [ -n "$existed" ] && [ "${RECONFIGURE:-0}" = 1 ]; then
    apply_role_script "$name" "$role_only" "${node_secrets[@]}"
    log "reconfigured $name"
  fi
done
wait_instances_running "${ids[@]}"
refresh_instances
# Every node that has not finished booting gets them (also one an interrupted earlier run launched).
for name in "${names[@]}"; do deliver_boot_secrets "$name" "${node_secrets[@]}"; done
wait_boot "${names[@]}"

_state_update '.deployment = {bus:$bus,server_image:$img,extra_flags:$fl,node_count:($n|tonumber),node_type:$t,gomemlimit:$gm}' \
  --arg bus "$BUS" --arg img "$image" --arg fl "$flags" --arg n "$NODE_COUNT" --arg t "$NODE_INSTANCE_TYPE" --arg gm "$NODE_GOMEMLIMIT"
cost_checkpoint
log_line 40-nodes "$NODE_COUNT nodes up ($NODE_INSTANCE_TYPE) image $tag bus=$BUS flags='${ABLY_SERVER_EXTRA_FLAGS}'" "50-loadgen.sh"
