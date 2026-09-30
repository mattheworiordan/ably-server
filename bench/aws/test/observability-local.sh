#!/usr/bin/env bash
# Starts the conductor-box observability stack locally from a rendered scrape
# config (fixture STATE.json) and checks Prometheus and Grafana come up with
# the provisioned datasource and dashboard. Needs Docker; pulls images.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWS_DIR="$(cd "$HERE/.." && pwd)"
command -v docker >/dev/null || { echo "docker not found" >&2; exit 1; }
tmp=$(mktemp -d)
proj="benchobs-test-$$"
stage="$tmp/observability"
cleanup() {
  (cd "$stage" 2>/dev/null && docker compose -p "$proj" --env-file .env down -v >/dev/null 2>&1) || true
  rm -rf "$tmp"
}
trap cleanup EXIT

prom_port=$((20000 + RANDOM % 10000))
graf_port=$((prom_port + 1))
mkdir -p "$stage"
cp -R "$AWS_DIR/observability/docker-compose.yml" "$AWS_DIR/observability/grafana" "$stage/"
PROJECT_TAG=fixture "$AWS_DIR/observability/render-prometheus.sh" "$HERE/fixtures/STATE.json" >"$stage/prometheus.yml"
cat >"$stage/.env" <<ENV
GRAFANA_PASSWORD=localtest-password
PROMETHEUS_PORT=$prom_port
GRAFANA_PORT=$graf_port
ENV

prom_image=$(grep -E '^: "\$\{PROMETHEUS_IMAGE' "$AWS_DIR/lib.sh" | sed -E 's/.*:=([^}]*)\}.*/\1/')
echo "== promtool check config ($prom_image)"
docker run --rm --entrypoint promtool -v "$stage/prometheus.yml:/cfg/prometheus.yml:ro" "$prom_image" check config /cfg/prometheus.yml

echo "== compose up"
(cd "$stage" && PROMETHEUS_IMAGE="$prom_image" docker compose -p "$proj" --env-file .env up -d)

wait_for() { # <description> <url> [curl args...]
  local desc=$1 url=$2
  shift 2
  for _ in $(seq 1 60); do
    if curl -fsS "$@" "$url" >/dev/null 2>&1; then echo "ok   $desc"; return 0; fi
    sleep 2
  done
  echo "FAIL $desc ($url)" >&2
  (cd "$stage" && docker compose -p "$proj" --env-file .env logs --tail 30) >&2
  return 1
}
wait_for "prometheus ready" "http://127.0.0.1:$prom_port/-/ready"
wait_for "grafana healthy" "http://127.0.0.1:$graf_port/api/health"

want="ably-server,loadgen,nats,node,postgres,prometheus"
targets=""
for _ in $(seq 1 15); do
  targets=$(curl -fsS "http://127.0.0.1:$prom_port/api/v1/targets" | jq -r '[.data.activeTargets[].labels.job] | unique | join(",")')
  [ "$targets" = "$want" ] && break
  sleep 2
done
if [ "$targets" != "$want" ]; then
  echo "FAIL prometheus jobs: want $want got $targets" >&2
  exit 1
fi
echo "ok   prometheus loaded jobs: $targets"

found=$(curl -fsS -u admin:localtest-password "http://127.0.0.1:$graf_port/api/search?query=Scale" | jq -r '.[].title' | tr '\n' ',')
case "$found" in
  *"Scale proof overview"*) echo "ok   grafana provisioned dashboard: $found" ;;
  *) echo "FAIL grafana dashboard missing: $found" >&2; exit 1 ;;
esac
ds=$(curl -fsS -u admin:localtest-password "http://127.0.0.1:$graf_port/api/datasources/uid/prometheus" | jq -r .type)
[ "$ds" = prometheus ] || { echo "FAIL grafana datasource: $ds" >&2; exit 1; }
echo "ok   grafana datasource provisioned"
echo "observability stack test passed"
