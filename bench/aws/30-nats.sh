#!/usr/bin/env bash
# 30-nats: a NATS core cluster (default three servers, no JetStream), on
# fixed private addresses so every server can list its routes at boot.
# Create or reuse.
#
#   bench/aws/30-nats.sh
#
# Optional: NATS_COUNT (3), NATS_IP_OFFSET (10), NATS_IMAGE (nats:2.11).
SCRIPT_NAME=30-nats
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
require_env AWS_REGION
state_init
require_network_state
: "${NATS_IP_OFFSET:=10}"

cidr=$(state_get '.network.subnet_cidr')
ips=()
for i in $(seq 1 "$NATS_COUNT"); do
  ips+=("$(cidr_host_ip "$cidr" $((NATS_IP_OFFSET + i - 1)))")
done

cost_checkpoint
init_work_dir
ids=()
names=()
urls=""
for i in $(seq 1 "$NATS_COUNT"); do
  ip=${ips[$((i - 1))]}
  routes=""
  for j in $(seq 1 "$NATS_COUNT"); do
    if [ "$j" != "$i" ]; then routes+="${routes:+, }\"nats-route://${ips[$((j - 1))]}:$NATS_ROUTE_PORT\""; fi
  done
  urls+="${urls:+,}nats://$ip:$NATS_CLIENT_PORT"
  name=$(iname nats "$i")
  names+=("$name")
  ud="$BENCH_WORK_DIR/userdata-nats-$i.sh"
  render_userdata "$ud" nats "SERVER_NAME=nats-$i" "CLIENT_PORT=$NATS_CLIENT_PORT" "MONITOR_PORT=$NATS_MONITOR_PORT" \
    "ROUTE_PORT=$NATS_ROUTE_PORT" "ROUTES=$routes" "NATS_IMAGE=$NATS_IMAGE" \
    "EXPORTER_IMAGE=$NATS_EXPORTER_IMAGE" "EXPORTER_PORT=$NATS_EXPORTER_PORT"
  ids+=("$(launch_instance "$name" nats "$NATS_INSTANCE_TYPE" "$ud" "$ip")")
done
wait_instances_running "${ids[@]}"
refresh_instances
state_set '.nats.urls' "$urls"
state_set '.nats.image' "$NATS_IMAGE"
state_set_json '.nats.count' "$NATS_COUNT"
wait_boot "${names[@]}"
cost_checkpoint
log_line 30-nats "NATS cluster of $NATS_COUNT up ($NATS_INSTANCE_TYPE, $NATS_IMAGE): $urls" "40-nodes.sh"
