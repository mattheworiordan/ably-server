#!/usr/bin/env bash
# 70-collect: gather everything a run left behind into results/<run-id>/collect/:
# a Prometheus snapshot and range exports of the named series, container logs
# and docker stats samples from every box, pg_stat_statements from each Postgres
# instance, RDS Performance Insights load, and a redacted copy of STATE.
# Every step is best effort: a failure is logged and the rest continue.
#
#   bench/aws/70-collect.sh <run-id> [start-rfc3339 end-rfc3339]
#
# Start and end default to the run record in STATE.
SCRIPT_NAME=70-collect
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
run_id=${1:?usage: 70-collect.sh <run-id> [start end]}
state_init
load_postgres_password
start=${2:-$(state_get ".runs[]? | select(.id == \"$run_id\") | .started_at")}
end=${3:-$(state_get ".runs[]? | select(.id == \"$run_id\") | .finished_at")}
if is_dry; then : "${start:=2000-01-01T00:00:00Z}" "${end:=2000-01-01T01:00:00Z}"; fi
[ -n "$start" ] || die "no start time for $run_id (not in STATE); pass start and end"
: "${end:=$(date -u +%FT%TZ)}"

out="$RESULTS_DIR/$run_id/collect"
if ! is_dry; then mkdir -p "$out"; fi
cname=$(iname conductor 1)
step() { # <description> <command...>: run, never abort the script
  local desc=$1
  shift
  if "$@"; then log "collected: $desc"; else log "WARNING: could not collect: $desc"; fi
}

# 1. Prometheus: snapshot plus range exports of the named series.
scp_to "$cname" "$BENCH_AWS_DIR/observability/export-series.sh" /opt/bench/observability/export-series.sh || true
scp_to "$cname" "$BENCH_AWS_DIR/observability/export-series.txt" /opt/bench/observability/export-series.txt || true
step "prometheus range export" ssh_do "$cname" \
  "bash /opt/bench/observability/export-series.sh '$start' '$end' /opt/bench/results/$run_id/series /opt/bench/observability/export-series.txt"
step "prometheus snapshot" ssh_do "$cname" \
  "snap=\$(curl -fsS -X POST http://127.0.0.1:9090/api/v1/admin/tsdb/snapshot | jq -r .data.name) && docker cp bench-observability-prometheus-1:/prometheus/snapshots/\$snap /opt/bench/results/$run_id/prometheus-snapshot && tar czf /opt/bench/results/$run_id/prometheus-snapshot.tgz -C /opt/bench/results/$run_id prometheus-snapshot && rm -rf /opt/bench/results/$run_id/prometheus-snapshot"

# 2. pg_stat_statements from each Postgres instance, run from the conductor.
for id in $(state_get '.postgres.instances // {} | keys[]'); do
  dsn=$(state_get ".postgres.instances[\"$id\"].dsn")
  step "pg_stat_statements $id" ssh_do "$cname" \
    "docker run --rm $PGBENCH_IMAGE psql '$dsn' -c \"\\copy (SELECT queryid, calls, round(total_exec_time::numeric,1) AS total_ms, round(mean_exec_time::numeric,3) AS mean_ms, rows, left(query,200) AS query FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 50) TO STDOUT CSV HEADER\" > /opt/bench/results/$run_id/pg_stat_statements-$id.csv"
done

# 3. RDS Performance Insights: average database load over the run.
for id in $(state_get '.postgres.instances // {} | keys[]'); do
  resid=$(aws_r "db-dryrun" rds describe-db-instances --db-instance-identifier "$id" --query 'DBInstances[0].DbiResourceId') || resid=""
  [ -n "$resid" ] || continue
  if pi=$(aws_r "{}" pi get-resource-metrics --service-type RDS --identifier "$resid" \
    --metric-queries '[{"Metric":"db.load.avg"},{"Metric":"db.load.avg","GroupBy":{"Group":"db.wait_event","Limit":10}}]' \
    --start-time "$start" --end-time "$end" --period-in-seconds 60 --output json); then
    if ! is_dry; then printf '%s\n' "$pi" >"$out/pi-$id.json"; fi
    log "collected: performance insights $id"
  else
    log "WARNING: could not collect: performance insights $id"
  fi
done

# 4. Container logs, image ids and docker stats from every server-side box.
for name in $(state_get '.instances // {} | to_entries[] | select(.value.role == "node" or .value.role == "nats" or .value.role == "loadgen" or .value.role == "publisher") | .key'); do
  role=$(inst_field "$name" role)
  case "$role" in node) c=ably-server ;; nats) c=nats ;; *) c=loadgen ;; esac
  ssh_do "$name" "rm -rf /tmp/collect && mkdir -p /tmp/collect && docker logs $c --since '$start' 2>&1 | gzip > /tmp/collect/$c.log.gz; docker inspect $c --format '{{.Config.Image}} {{.Image}}' > /tmp/collect/image.txt 2>&1; cp /var/tmp/docker-stats-$run_id.jsonl /tmp/collect/ 2>/dev/null; tar czf /tmp/collect-$name.tgz -C /tmp collect" || log "WARNING: collection on $name failed"
  if ! is_dry; then mkdir -p "$out/boxes"; fi
  scp_from "$name" "/tmp/collect-$name.tgz" "$out/boxes/" || log "WARNING: could not copy from $name"
done

# 5. Everything the conductor wrote, then a redacted copy of STATE.
scp_from "$cname" "/opt/bench/results/$run_id/." "$out/conductor/" || log "WARNING: could not copy conductor results"
if ! is_dry; then
  jq 'del(.api_key, .grafana_password) | (.postgres.instances // {}) |= map_values(del(.dsn))' "$ACTIVE_STATE" >"$out/state-redacted.json"
fi
log_line 70-collect "collected $run_id into $out" "write the run up (plan section 13); 80-stop.sh when the day's runs are done"
