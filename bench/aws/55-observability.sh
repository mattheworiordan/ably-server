#!/usr/bin/env bash
# 55-observability: render the Prometheus scrape config from STATE (every node,
# NATS server, generator, publisher and host) and start or reload Prometheus,
# Grafana and postgres_exporter on the conductor box. Re-run after the fleet
# changes size. Reach the UIs with an SSH tunnel (RUNBOOK, 'Watching a run').
#
#   bench/aws/55-observability.sh
SCRIPT_NAME=55-observability
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
state_init
cname=$(iname conductor 1)
[ -n "$(inst_field "$cname" id)" ] || die "no conductor in STATE; run 50-loadgen.sh first"
init_work_dir

grafana_pw=$(state_get '.grafana_password')
if [ -z "$grafana_pw" ]; then
  grafana_pw=$(openssl rand -hex 12)
  state_set '.grafana_password' "$grafana_pw"
fi

obs="$BENCH_AWS_DIR/observability"
stage="$BENCH_WORK_DIR/observability"
mkdir -p "$stage"
cp -R "$obs/docker-compose.yml" "$obs/grafana" "$stage/"
"$obs/render-prometheus.sh" "$ACTIVE_STATE" >"$stage/prometheus.yml"
(
  umask 077
  cat >"$stage/.env" <<ENV
PROMETHEUS_IMAGE=$PROMETHEUS_IMAGE
GRAFANA_IMAGE=$GRAFANA_IMAGE
POSTGRES_EXPORTER_IMAGE=$POSTGRES_EXPORTER_IMAGE
GRAFANA_PASSWORD=$grafana_pw
POSTGRES_EXPORTER_DSN=$(postgres_dsns)
COMPOSE_PROFILES=postgres
ENV
)
ssh_do "$cname" 'mkdir -p /opt/bench/observability'
scp_to "$cname" "$stage/." /opt/bench/observability/
ssh_do "$cname" 'cd /opt/bench/observability && docker compose --env-file .env up -d && curl -fsS -X POST http://127.0.0.1:9090/-/reload || true'
log_line 55-observability "Prometheus, Grafana and postgres_exporter up on $cname; scrape config rendered from STATE" "60-run.sh smoke-1pct"
