#!/usr/bin/env bash
# Driver for the cluster benchmark experiments (see BASELINE.md in the
# research folder). Everything is sequential: overlapping runs would fight
# over the same CPUs and invalidate each other.
#
#   bench/run-experiments.sh exp2 [profile...]          # repo compose, 3 nodes, 8081-8083
#   bench/run-experiments.sh exp3 <N> [profile...]      # scale stack with N nodes
#   bench/run-experiments.sh confirm <base|scale> <profile> <rate> <label>
#
# Profiles: default fanout many allnodes   (default set: see each command)
# Env (required): BENCH_BIN, LOG_DIR.  Optional: RUNS (default 2).
# INNET=1 runs the load generator inside the compose network (needs
# DOCKER_BENCH_BIN, a linux/arm64 ably-bench build; see docker-bench.sh)
# instead of on the host through Docker Desktop's port proxy; log labels are
# then prefixed "innet-".
#
# Each search is: ably-bench --search --duration 15s --warmup 3s --p50 20ms
# --p99 100ms --max-rate 100000 --rate 250 (start rate; the search doubles).
# Between searches the message tables are truncated so no run inherits the
# previous run's rows, WAL or checkpoint backlog.
set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"
: "${BENCH_BIN:?set BENCH_BIN}"
: "${LOG_DIR:?set LOG_DIR}"
RUNS="${RUNS:-2}"
INNET="${INNET:-0}"
PFX=""
if [ "$INNET" = 1 ]; then
  : "${DOCKER_BENCH_BIN:?INNET=1 needs DOCKER_BENCH_BIN}"
  export DOCKER_BENCH_BIN
  BENCH_BIN="$here/docker-bench.sh"
  PFX="innet-"
fi
export BENCH_BIN LOG_DIR

SEARCH=(--search --duration 15s --warmup 3s --p50 20ms --p99 100ms --max-rate 100000 --rate 250)

profile_flags() {
  case "$1" in
    default)  echo "--channels 4 --publishers 4 --subscribers 4" ;;
    fanout)   echo "--channels 1 --publishers 1 --subscribers 300" ;;
    many)     echo "--channels 300 --publishers 300 --subscribers 300" ;;
    allnodes) echo "--channels 25 --publishers 25 --subscribers 300" ;;
    *) echo "unknown profile $1" >&2; return 1 ;;
  esac
}

stamp() { date '+%H:%M:%S'; }

truncate_tables() {
  docker exec "$PG" psql -U ably -d ably -qc 'TRUNCATE channel_messages, messages' >/dev/null
}

record_pg_settings() {
  local out="$LOG_DIR/pg-settings-$1.txt"
  {
    echo "# $(date -u +%FT%TZ) postgres settings for stack $1"
    docker exec "$PG" psql -U ably -d ably -At -c 'select version()'
    docker exec "$PG" psql -U ably -d ably -At -F ' = ' -c \
      "select name, setting || coalesce(unit,'') from pg_settings where name in
       ('max_connections','shared_buffers','synchronous_commit','fsync','wal_sync_method',
        'wal_level','max_wal_size','min_wal_size','checkpoint_timeout','full_page_writes',
        'wal_buffers','commit_delay','commit_siblings','effective_cache_size','work_mem',
        'max_worker_processes','autovacuum','track_io_timing','jit') order by name"
  } > "$out" 2>&1
  cat "$out"
}

warm_up() {
  # Short low-rate trial whose result is discarded: opens the pool
  # connections, creates the channel rows and warms the page cache so the
  # first measured search does not pay cold-start costs.
  echo "[$(stamp)] warm-up on $ENDPOINTS (result discarded)"
  "$BENCH_BIN" --endpoints "$ENDPOINTS" --rate 1000 --duration 8s --warmup 2s 2>&1 \
    | tee "$LOG_DIR/warmup-$WARM_LABEL.log" | grep -E 'achieved|latency|correct'
}

up_base() {
  ( cd "$repo" && docker compose up -d ) || return 1
  PG=ably-cluster-postgres-1
  ENDPOINTS=localhost:8081,localhost:8082,localhost:8083
  STATS_FILTER='^ably-cluster-'
  "$here/wait-ready.sh" "$ENDPOINTS" 120
  if [ "$INNET" = 1 ]; then
    export DOCKER_BENCH_NET=ably-cluster_default
    ENDPOINTS="$(docker ps --filter label=com.docker.compose.project=ably-cluster --filter label=com.docker.compose.service -q \
      | xargs docker inspect -f '{{.Name}} {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
      | grep -E 'node[0-9]' | awk '{print $2":8080"}' | sort | paste -sd, -)"
    STATS_FILTER='^(ably-cluster-|ablybench)'
  fi
  WARM_LABEL="${PFX}base"; warm_up
}

up_scale() {
  local n="$1"
  ( cd "$repo" && docker compose -f bench/docker-compose.scale.yml -p ablyscale down -v --remove-orphans >/dev/null 2>&1
    docker compose -f bench/docker-compose.scale.yml -p ablyscale up -d --scale node="$n" ) || return 1
  PG=ablyscale-postgres-1
  ENDPOINTS="$("$here/scale-endpoints.sh")"
  STATS_FILTER='^ablyscale-'
  local have; have="$(echo "$ENDPOINTS" | tr ',' '\n' | grep -c .)"
  if [ "$have" != "$n" ]; then echo "expected $n endpoints, found $have: $ENDPOINTS" >&2; return 1; fi
  "$here/wait-ready.sh" "$ENDPOINTS" 180
  if [ "$INNET" = 1 ]; then
    export DOCKER_BENCH_NET=ablyscale_default
    ENDPOINTS="$("$here/scale-endpoints.sh" ablyscale internal)"
    STATS_FILTER='^(ablyscale-|ablybench)'
  fi
  WARM_LABEL="${PFX}scale-n$n"; warm_up
}

run_profile() {
  # run_profile <stack-label> <profile> <run-number>
  local stack="$1" profile="$2" run="$3"
  local label="${PFX}${stack}-${profile}-run${run}"
  truncate_tables
  echo "[$(stamp)] === $label  endpoints=$ENDPOINTS"
  # shellcheck disable=SC2046
  PG_CONTAINER="$PG" STATS_FILTER="$STATS_FILTER" \
    "$here/run-search.sh" "$label" "$ENDPOINTS" "${SEARCH[@]}" $(profile_flags "$profile") \
    | grep -E 'MAX SUSTAINED|at that load|FAIL|note:|failed|wall clock|exit code'
}

cmd="${1:-}"; shift || true
case "$cmd" in
  exp2)
    profiles=("$@"); [ ${#profiles[@]} -gt 0 ] || profiles=(default fanout many)
    up_base || exit 1
    [ "$INNET" = 1 ] || record_pg_settings base
    for r in $(seq 1 "$RUNS"); do
      for p in "${profiles[@]}"; do run_profile exp2 "$p" "$r"; done
    done
    ;;
  exp3)
    n="${1:?node count}"; shift
    profiles=("$@"); [ ${#profiles[@]} -gt 0 ] || profiles=(default many)
    up_scale "$n" || exit 1
    [ "$INNET" = 1 ] || record_pg_settings "scale-n$n"
    for r in $(seq 1 "$RUNS"); do
      for p in "${profiles[@]}"; do run_profile "exp3-n$n" "$p" "$r"; done
    done
    ;;
  confirm)
    stack="${1:?base|scale}"; profile="${2:?profile}"; rate="${3:?rate}"; label="${4:?label}"
    if [ "$stack" = base ]; then PG=ably-cluster-postgres-1; ENDPOINTS=localhost:8081,localhost:8082,localhost:8083; STATS_FILTER='^ably-cluster-'
    else PG=ablyscale-postgres-1; ENDPOINTS="$("$here/scale-endpoints.sh")"; STATS_FILTER='^ablyscale-'; fi
    truncate_tables
    # shellcheck disable=SC2046
    PG_CONTAINER="$PG" STATS_FILTER="$STATS_FILTER" \
      "$here/run-search.sh" "$label" "$ENDPOINTS" --rate "$rate" --duration 20s --warmup 3s $(profile_flags "$profile")
    ;;
  *)
    sed -n '2,20p' "$0"; exit 2 ;;
esac
