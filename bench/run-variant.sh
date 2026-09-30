#!/usr/bin/env bash
# Run the same searches against any 3-node compose stack (the repo's own file,
# or a variant from a sibling worktree such as the NATS bus or the scaled
# Postgres bus), so the results are like for like.
#
#   bench/run-variant.sh <label> <compose-file> <project> <pg-container> <host-ports> [profile...]
#
#   bench/run-variant.sh nats  /path/to/wt/bench/docker-compose.nats.yml ablynats ablynats-postgres-1 8181,8182,8183 default manyfix
#
# With the scale stack, pass `auto` as <host-ports> and UP_ARGS="--scale node=3":
#   UP_ARGS="--scale node=3" PG_IMAGE=postgres:17-alpine CTX_DIR=<repo> bench/run-variant.sh base-pg17 \
#     <repo>/bench/docker-compose.scale.yml ablyscale ablyscale-postgres-1 auto default manyfix#
# Env (required): BENCH_BIN (an ably-bench with --stagger), LOG_DIR.
# Optional: START_RATE (default 500), BUILD=0 to skip --build, DOWN=0 to
# leave the stack up afterwards (default: take it down), CTX_DIR to run
# docker compose from a directory other than the compose file's parent
# (needed for the repo's own root docker-compose.yml).
#
# Profiles: default | manyfix (300 channels/publishers/subscribers,
# --stagger, 10 s warm-up). Searches: 15 s trials, p50 20 ms, p99 100 ms,
# --max-rate 100000, one run per profile.
set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
label="${1:?label}"; compose="${2:?compose file}"; project="${3:?project}"
pg="${4:?postgres container}"; ports="${5:?host ports}"; shift 5
profiles=("$@"); [ ${#profiles[@]} -gt 0 ] || profiles=(default manyfix)
: "${BENCH_BIN:?}" "${LOG_DIR:?}"
START_RATE="${START_RATE:-500}"
stamp() { date '+%H:%M:%S'; }

endpoints=""
if [ "$ports" != auto ]; then
  IFS=',' read -r -a parr <<< "$ports"
  for p in "${parr[@]}"; do endpoints="${endpoints:+$endpoints,}localhost:$p"; done
fi

ctx="${CTX_DIR:-$(dirname "$compose")/..}"   # where `docker compose` runs and which git tree is recorded
build=(--build); [ "${BUILD:-1}" = 0 ] && build=()
echo "[$(stamp)] up $label: $compose (project $project)"
# shellcheck disable=SC2086
( cd "$ctx" && time docker compose -f "$compose" -p "$project" up -d "${build[@]}" ${UP_ARGS:-} ) 2>&1 | tail -15 || exit 1
# "auto" ports: discover the mapped host ports of the scale stack's nodes.
[ "$ports" = auto ] && endpoints="$("$here/scale-endpoints.sh" "$project")"
echo "[$(stamp)] endpoints: $endpoints"
"$here/wait-ready.sh" "$endpoints" 240 || { echo "[$(stamp)] $label FAILED to become ready"; exit 1; }

# Record what is being tested, and switch on slow-statement logging so the
# Postgres log shows whether commits queue on the notify lock (same 50 ms
# threshold as the baseline evidence).
{
  echo "# variant $label, compose $compose, project $project"
  echo "# git: $(git -C "$ctx" rev-parse HEAD) ($(git -C "$ctx" rev-parse --abbrev-ref HEAD)), uncommitted files: $(git -C "$ctx" status --short | wc -l | tr -d ' ')"
  echo "# postgres: $(docker exec "$pg" psql -U ably -d ably -At -c 'select version()')"
  echo "# env: PGBUS_NOTIFY_MODE=${PGBUS_NOTIFY_MODE:-unset} PGBUS_POOL_MAX_CONNS=${PGBUS_POOL_MAX_CONNS:-unset}"
} | tee "$LOG_DIR/variant-$label.info"
docker exec "$pg" psql -U ably -d ably -qAt \
  -c "ALTER SYSTEM SET log_min_duration_statement = '50ms'" \
  -c "ALTER SYSTEM SET log_lock_waits = on" \
  -c "ALTER SYSTEM SET deadlock_timeout = '200ms'" \
  -c "select pg_reload_conf()" >/dev/null 2>&1

echo "[$(stamp)] warm-up $label (result discarded)"
"$BENCH_BIN" --endpoints "$endpoints" --rate 1000 --duration 8s --warmup 2s 2>&1 \
  | tee "$LOG_DIR/warmup-$label.log" | grep -E 'achieved|latency|correct'

for prof in "${profiles[@]}"; do
  case "$prof" in
    default) flags="--channels 4 --publishers 4 --subscribers 4" ;;
    manyfix) flags="--channels 300 --publishers 300 --subscribers 300 --stagger --warmup 10s" ;;
    *) echo "unknown profile $prof"; continue ;;
  esac
  docker exec "$pg" psql -U ably -d ably -qc 'TRUNCATE channel_messages, messages' >/dev/null 2>&1 \
    || echo "[$(stamp)] note: truncate failed on $pg (schema differs?), continuing"
  echo "[$(stamp)] === $label-$prof endpoints=$endpoints"
  # shellcheck disable=SC2086
  PG_CONTAINER="$pg" STATS_FILTER="^${project}-" \
    "$here/run-search.sh" "variant-$label-$prof" "$endpoints" \
      --search --duration 15s --warmup 3s --p50 20ms --p99 100ms --max-rate 100000 --rate "$START_RATE" $flags \
    | grep -E 'MAX SUSTAINED|at that load|FAIL|note:|failed|wall clock|exit code'
done

# Keep the Postgres and node logs (slow commits, lock waits, LISTEN reconnects).
docker logs "$pg" > "$LOG_DIR/pg-log-$label.log" 2>&1
for c in $(docker ps --format '{{.Names}}' | grep "^${project}-" | grep -v postgres | sort); do
  docker logs "$c" > "$LOG_DIR/container-log-$c-$label.log" 2>&1
done
{
  echo "# slow-statement summary for $label (statements >= 50 ms in the Postgres log)"
  echo "slow statements: $(grep -c 'duration:' "$LOG_DIR/pg-log-$label.log")"
  echo "of which commit: $(grep 'duration:' "$LOG_DIR/pg-log-$label.log" | grep -c 'statement: commit')"
  echo "lock-wait lines: $(grep -c 'still waiting' "$LOG_DIR/pg-log-$label.log")"
  grep 'still waiting' "$LOG_DIR/pg-log-$label.log" | sed -E 's/.*(still waiting for [A-Za-z]+ on [a-z0-9 ]+ of database [0-9]+).*/\1/' | sort | uniq -c
} | tee "$LOG_DIR/pg-slow-summary-$label.txt"

if [ "${DOWN:-1}" = 1 ]; then
  ( cd "$ctx" && docker compose -f "$compose" -p "$project" down -v >/dev/null 2>&1 )
  echo "[$(stamp)] $label stack down"
fi
