#!/usr/bin/env bash
# Run one ably-bench invocation with timestamped output and resource
# samplers, saving raw logs under $LOG_DIR.
#
#   bench/run-search.sh <label> <endpoints> [ably-bench flags...]
#
# Env:
#   BENCH_BIN     ably-bench binary (required)
#   LOG_DIR       output directory (required)
#   STATS_FILTER  regex of docker container names to sample with
#                 `docker stats` (empty: no docker sampling)
#   PG_CONTAINER  name of the postgres container; when set, connection
#                 counts and pg_stat_database deltas are recorded
#   METRICS_FILTER regex of node container names whose /metrics (debug
#                 listener :6060, scale stack only) is scraped before and
#                 after the run into <label>.metrics-<container>-{before,after}.txt
#
# Files written: <label>.log (timestamped bench output),
# <label>.top.log (host top, every 2s), <label>.dockerstats.csv.
set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
label="$1"
endpoints="$2"
shift 2
: "${BENCH_BIN:?set BENCH_BIN}"
: "${LOG_DIR:?set LOG_DIR}"
mkdir -p "$LOG_DIR"
log="$LOG_DIR/$label.log"

{
  echo "# label: $label"
  echo "# started: $(date -u +%FT%TZ)"
  echo "# host uptime: $(uptime)"
  echo "# command: $BENCH_BIN --endpoints $endpoints $*"
} > "$log"

pg_snapshot() {
  # xact_commit, tup_inserted, tup_fetched, blks_read, blks_hit, plus the
  # connection count, from the postgres container (empty if unset).
  [ -n "${PG_CONTAINER:-}" ] || return 0
  docker exec "$PG_CONTAINER" psql -U ably -d ably -At -F ' ' -c \
    "SELECT 'pg_stat_database', xact_commit, xact_rollback, tup_inserted, tup_fetched, blks_read, blks_hit FROM pg_stat_database WHERE datname='ably'" 2>/dev/null
  docker exec "$PG_CONTAINER" psql -U ably -d ably -At -F ' ' -c \
    "SELECT 'pg_connections', count(*), count(*) FILTER (WHERE state='active') FROM pg_stat_activity WHERE datname='ably'" 2>/dev/null
}

scrape_metrics() { # scrape_metrics before|after
  [ -n "${METRICS_FILTER:-}" ] || return 0
  local c
  for c in $(docker ps --format '{{.Names}}' | grep -E "$METRICS_FILTER" | sort); do
    docker exec "$c" wget -qO- http://localhost:6060/metrics 2>/dev/null \
      | grep -E '^(ably_|go_goroutines|process_cpu_seconds_total)' > "$LOG_DIR/$label.metrics-$c-$1.txt" || true
  done
}

top -l 0 -s 2 -n 8 -o cpu -stats pid,command,cpu,mem > "$LOG_DIR/$label.top.log" 2>&1 &
top_pid=$!
stats_pid=""
if [ -n "${STATS_FILTER:-}" ]; then
  "$here/sample-docker-stats.sh" "$LOG_DIR/$label.dockerstats.csv" "$STATS_FILTER" &
  stats_pid=$!
fi

{ echo "# pg before:"; pg_snapshot | sed 's/^/#   /'; } >> "$log"
scrape_metrics before
start="$(gdate +%s.%N)"
"$BENCH_BIN" --endpoints "$endpoints" "$@" 2>&1 \
  | while IFS= read -r line; do printf '%s %s\n' "$(gdate +%H:%M:%S.%3N)" "$line"; done \
  | tee -a "$log"
rc="${PIPESTATUS[0]}"
end="$(gdate +%s.%N)"
scrape_metrics after
{ echo "# pg after:"; pg_snapshot | sed 's/^/#   /'; } >> "$log"

kill "$top_pid" >/dev/null 2>&1 || true
[ -n "$stats_pid" ] && kill "$stats_pid" >/dev/null 2>&1 || true
wait "$top_pid" 2>/dev/null || true
[ -n "$stats_pid" ] && wait "$stats_pid" 2>/dev/null || true

{
  echo "# ended: $(date -u +%FT%TZ)"
  echo "# start_epoch: $start end_epoch: $end"
  echo "# wall clock seconds: $(echo "$end - $start" | bc)"
  echo "# exit code: $rc"
} | tee -a "$log"
exit "$rc"
