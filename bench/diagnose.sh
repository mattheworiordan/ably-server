#!/usr/bin/env bash
# Capture what saturates during one fixed-rate run against the scale stack:
# pg_notification_queue_usage() and connection states once a second,
# docker stats, a 10 s CPU profile from the first node, and every node's
# Prometheus /metrics before and after.
#
#   bench/diagnose.sh <label> <profile> <rate> [duration]
#
# Env (required): BENCH_BIN, LOG_DIR. The scale stack must be up and its
# nodes must have been started with the debug listener on :6060 (the scale
# compose file does that).
set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${BENCH_BIN:?}" "${LOG_DIR:?}"
label="${1:?label}"; profile="${2:?profile}"; rate="${3:?rate}"; dur="${4:-30s}"
PG=ablyscale-postgres-1
ENDPOINTS="$("$here/scale-endpoints.sh")"
case "$profile" in
  default)  flags="--channels 4 --publishers 4 --subscribers 4" ;;
  fanout)   flags="--channels 1 --publishers 1 --subscribers 300" ;;
  many)     flags="--channels 300 --publishers 300 --subscribers 300" ;;
  allnodes) flags="--channels 25 --publishers 25 --subscribers 300" ;;
  *) echo "unknown profile" >&2; exit 2 ;;
esac
nodes="$(docker ps --format '{{.Names}}' | grep '^ablyscale-node-' | sort)"
first="$(echo "$nodes" | head -1)"
out="$LOG_DIR/$label"
mkdir -p "$out"

scrape() { # scrape <tag>
  for n in $nodes; do
    docker exec "$n" wget -qO- http://localhost:6060/metrics 2>/dev/null > "$out/metrics-$n-$1.txt" || true
  done
}
docker exec "$PG" psql -U ably -d ably -qc 'TRUNCATE channel_messages, messages' >/dev/null
docker exec "$PG" psql -U ably -d ably -qc "select pg_stat_statements_reset()" >/dev/null 2>&1 || true
scrape before

# 1 Hz poller: notification queue fill, connection counts by client address/state.
(
  echo "epoch,notify_queue_usage,pg_conns,pg_active,pg_idle_in_tx" > "$out/pg-poll.csv"
  while true; do
    now="$(gdate +%s.%N)"
    docker exec "$PG" psql -U ably -d ably -At -F ',' -c \
      "select pg_notification_queue_usage(), count(*), count(*) filter (where state='active'), count(*) filter (where state like 'idle in transaction%') from pg_stat_activity where datname='ably'" \
      2>/dev/null | sed "s/^/$now,/" >> "$out/pg-poll.csv"
    sleep 0.5
  done
) &
poll_pid=$!
"$here/sample-docker-stats.sh" "$out/dockerstats.csv" '^ablyscale-' &
stats_pid=$!

# CPU profile of the first node in the middle of the measurement window
# (warm-up 3 s + connection setup), and goroutine dump.
(
  sleep 14
  docker exec "$first" wget -qO /tmp/cpu.pprof "http://localhost:6060/debug/pprof/profile?seconds=10" 2>/dev/null
  docker cp "$first:/tmp/cpu.pprof" "$out/cpu-$first.pprof" >/dev/null 2>&1
  docker exec "$first" wget -qO- "http://localhost:6060/debug/pprof/goroutine?debug=1" 2>/dev/null | head -60 > "$out/goroutines-$first.txt"
) &
prof_pid=$!

echo "# $label $(date -u +%FT%TZ) profile=$profile rate=$rate duration=$dur endpoints=$ENDPOINTS" | tee "$out/run.log"
"$BENCH_BIN" --endpoints "$ENDPOINTS" --rate "$rate" --duration "$dur" --warmup 3s $flags 2>&1 | tee -a "$out/run.log"
wait "$prof_pid" 2>/dev/null || true
kill "$poll_pid" "$stats_pid" >/dev/null 2>&1 || true
wait "$poll_pid" "$stats_pid" 2>/dev/null || true
scrape after

docker exec "$PG" psql -U ably -d ably -At -F ' | ' -c \
  "select calls, round(mean_exec_time::numeric,4) as mean_ms, round(total_exec_time::numeric,0) as total_ms, left(regexp_replace(query, '\s+', ' ', 'g'), 110) from pg_stat_statements where dbid = (select oid from pg_database where datname='ably') order by total_exec_time desc limit 14" \
  > "$out/pg_stat_statements.txt" 2>&1 || true
docker exec "$PG" psql -U ably -d ably -At -F ' | ' -c \
  "select 'commits', xact_commit, 'rollbacks', xact_rollback, 'inserted', tup_inserted, 'fetched', tup_fetched from pg_stat_database where datname='ably'" \
  >> "$out/pg_stat_statements.txt" 2>&1 || true
echo "done -> $out"
