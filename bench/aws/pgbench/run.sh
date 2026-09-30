#!/usr/bin/env bash
# Run 0a: Postgres alone. pgbench scripts that replay the publish write path
# against one Postgres instance, with no ably-server node in the loop. It
# measures the ACK latency floor and the single-primary write ceiling.
#
#   run.sh setup    create the scratch database, schema and channels
#   run.sh run      run every variant at every client count
#   run.sh all      setup, run, summary           (default)
#   run.sh summary  rebuild the CSV summary tables from raw results
#
# Connection: the usual libpq variables (PGHOST, PGPORT, PGUSER, PGPASSWORD,
# PGDATABASE for the maintenance database). Everything happens in BENCH_DB.
#
# Variants (one script each, see the .sql files for what each one does):
#   trivial           one autocommit INSERT: the latency floor
#   shipped           the shipped seven-round-trip publish transaction
#   pipelined         one publish in one round trip, no pg_notify
#   pipelined_notify  the same with pg_notify inside the transaction
#   batch10|batch30|batch100   N publishes to N channels in one transaction
#
# Settings (environment):
#   STORAGE_LABEL   label for this instance in file names (io2, gp3, local)
#   OUT_DIR         where raw output, logs and CSV go (default ./results-0a)
#   DURATION_S      seconds per cell (60)            WARMUP_S  (10)
#   CLIENTS         "1 8 32 128"                     VARIANTS  (all of the above)
#   CHANNELS        channels to spread over (200000) PAYLOAD_BYTES (600)
#   SAMPLE_RATE     fraction of transactions in the latency log (0.05)
#   PGTOOL          local | docker | auto: how to run pgbench and psql. docker
#                   uses PGBENCH_IMAGE; DOCKER_NETWORK (default host) is its
#                   --network, for example container:<name> in tests.
#   BENCH_DB        scratch database name (ably_pgbench)
#   DRY_RUN=1       print the tool commands, run nothing
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${STORAGE_LABEL:=local}"
: "${OUT_DIR:=$PWD/results-0a}"
: "${DURATION_S:=60}"
: "${WARMUP_S:=10}"
: "${CLIENTS:=1 8 32 128}"
: "${VARIANTS:=trivial shipped pipelined pipelined_notify batch10 batch30 batch100}"
: "${CHANNELS:=200000}"
: "${PAYLOAD_BYTES:=600}"
: "${SAMPLE_RATE:=0.05}"
: "${BENCH_DB:=ably_pgbench}"
: "${PGBENCH_IMAGE:=postgres:17-alpine}"
: "${PGTOOL:=auto}"
: "${DOCKER_NETWORK:=host}"
: "${DRY_RUN:=0}"
mode=${1:-all}

log() { printf '%s [0a] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

mkdir -p "$OUT_DIR/raw" "$OUT_DIR/logs"
OUT_DIR="$(cd "$OUT_DIR" && pwd -P)"

if [ "$PGTOOL" = auto ]; then
  if command -v pgbench >/dev/null 2>&1 && command -v psql >/dev/null 2>&1; then PGTOOL=local; else PGTOOL=docker; fi
fi
case "$PGTOOL" in
  local)
    script_dir=$DIR
    out_in=$OUT_DIR
    tool() { "$@"; }
    ;;
  docker)
    script_dir=/work
    out_in=/out
    tool() {
      local t=$1
      shift
      docker run --rm --network "$DOCKER_NETWORK" -e PGHOST -e PGPORT -e PGUSER -e PGPASSWORD -e PGDATABASE \
        -v "$DIR:/work:ro" -v "$OUT_DIR:/out" "$PGBENCH_IMAGE" "$t" "$@"
    }
    ;;
  *) die "PGTOOL must be local, docker or auto" ;;
esac
dry() { [ "$DRY_RUN" = 1 ]; }
run_tool() { # <pgbench|psql> args...
  if dry; then
    printf 'DRYRUN %s %s\n' "$PGTOOL" "$*" >&2
    return 0
  fi
  tool "$@"
}
psql_admin() { run_tool psql -X -q -At -v ON_ERROR_STOP=1 "$@"; }
psql_b() { run_tool psql -X -q -At -v ON_ERROR_STOP=1 -d "$BENCH_DB" "$@"; }

threads_for() { # clients -> pgbench threads
  local c=$1 cpus
  cpus=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
  if [ "$c" -lt "$cpus" ]; then echo "$c"; else echo "$cpus"; fi
}

# variant_spec <variant>: sets V_SCRIPT, V_BATCH (0 = not batched), V_MSGS (messages per transaction).
variant_spec() {
  V_BATCH=0
  V_MSGS=1
  case "$1" in
    trivial) V_SCRIPT=trivial.sql ;;
    shipped) V_SCRIPT=publish_shipped.sql ;;
    pipelined) V_SCRIPT=publish_pipelined.sql ;;
    pipelined_notify) V_SCRIPT=publish_pipelined_notify.sql ;;
    batch[0-9]*)
      V_SCRIPT=publish_batched.sql
      V_BATCH=${1#batch}
      V_MSGS=$V_BATCH
      ;;
    *) die "unknown variant: $1" ;;
  esac
}

setup() {
  log "setup: database $BENCH_DB, $CHANNELS channels"
  local have
  have=$(psql_admin -d "${PGDATABASE:-postgres}" -c "SELECT 1 FROM pg_database WHERE datname = '$BENCH_DB'" || true)
  if [ -z "$have" ] && ! dry; then
    psql_admin -d "${PGDATABASE:-postgres}" -c "CREATE DATABASE $BENCH_DB"
  elif dry; then
    psql_admin -d "${PGDATABASE:-postgres}" -c "CREATE DATABASE $BENCH_DB"
  fi
  have=$(psql_b -c "SELECT to_regclass('public.channel_messages')" || true)
  if [ -z "$have" ] || dry; then
    psql_b -f "$script_dir/schema.sql"
  fi
  psql_b -v "channels=$CHANNELS" -f "$script_dir/setup.sql"
  psql_b -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" 2>/dev/null || log "pg_stat_statements not available (ok)"
  if ! dry; then
    {
      echo "date: $(date -u +%FT%TZ)"
      echo "storage_label: $STORAGE_LABEL"
      echo "channels: $CHANNELS"
      echo "payload_bytes: $PAYLOAD_BYTES"
      echo "pgbench: $(tool pgbench --version)"
      psql_b -c "SELECT 'server_version: ' || version()"
      psql_b -c "SELECT 'setting ' || name || ': ' || setting || coalesce(unit, '') FROM pg_settings WHERE name IN ('synchronous_commit','fsync','full_page_writes','wal_level','shared_buffers','max_wal_size','checkpoint_timeout','max_connections','wal_compression','track_io_timing','shared_preload_libraries') ORDER BY name"
      if command -v sha256sum >/dev/null 2>&1; then (cd "$DIR" && sha256sum ./*.sql); else (cd "$DIR" && shasum -a 256 ./*.sql); fi
    } >"$OUT_DIR/environment-$STORAGE_LABEL.txt"
  fi
}

percentiles() { # <log prefix path on the host>: "mean p50 p95 p99 max" in ms
  local files=("$1".*)
  if [ ! -e "${files[0]}" ]; then
    echo "nan nan nan nan nan"
    return 0
  fi
  cat "${files[@]}" | awk '{print $3}' | sort -n | awk '
    { a[NR] = $1; s += $1 }
    function pct(p,  i) { i = int(NR * p); if (i < 1) i = 1; return a[i] / 1000 }
    END { if (NR == 0) { print "nan nan nan nan nan"; exit }
          printf "%.3f %.3f %.3f %.3f %.3f\n", s / NR / 1000, pct(0.50), pct(0.95), pct(0.99), a[NR] / 1000 }'
}

run_cell() { # <variant> <clients> <final 0|1>: returns 1 (and writes no CSV row) for a failed, non-final attempt
  local variant=$1 clients=$2 final=${3:-1} threads cell raw logprefix
  variant_spec "$variant"
  threads=$(threads_for "$clients")
  cell="$STORAGE_LABEL-$variant-c$clients"
  raw="$OUT_DIR/raw/$cell.txt"
  logprefix="$OUT_DIR/logs/$cell"
  rm -f "$logprefix".*
  local -a common=(pgbench -n -M prepared -c "$clients" -j "$threads"
    -D "channels=$CHANNELS" -D "payload_bytes=$PAYLOAD_BYTES" -D "batch=$V_BATCH"
    -f "$script_dir/$V_SCRIPT" -d "$BENCH_DB")

  psql_b -c "TRUNCATE channel_messages, messages, commit_probe" >/dev/null
  psql_b -c "CHECKPOINT" >/dev/null 2>&1 || true
  if [ "$WARMUP_S" -gt 0 ]; then
    run_tool "${common[@]}" -T "$WARMUP_S" >/dev/null 2>&1 || log "warm-up of $cell reported errors (ignored)"
    psql_b -c "TRUNCATE channel_messages, messages, commit_probe" >/dev/null
  fi
  psql_b -c "SELECT pg_stat_statements_reset()" >/dev/null 2>&1 || true

  local lsn0 lsn1 rc=0
  lsn0=$(psql_b -c "SELECT pg_current_wal_lsn()" || echo 0/0)
  log "cell $cell: ${DURATION_S}s ($V_SCRIPT, batch $V_BATCH)"
  run_tool "${common[@]}" -T "$DURATION_S" -l --sampling-rate "$SAMPLE_RATE" --log-prefix "$out_in/logs/$cell" >"$raw" 2>&1 || rc=$?
  lsn1=$(psql_b -c "SELECT pg_current_wal_lsn()" || echo 0/0)
  if dry; then return 0; fi

  local tx failed tps lat status=ok wal walpm
  tx=$(awk '/number of transactions actually processed:/ {print $NF}' "$raw")
  failed=$(awk '/number of failed transactions:/ {print $5}' "$raw")
  tps=$(awk '/^tps =/ {print $3}' "$raw")
  lat=$(awk '/^latency average =/ {print $4}' "$raw")
  : "${tx:=0}" "${failed:=0}" "${tps:=0}" "${lat:=nan}"
  if [ "$rc" != 0 ] || grep -q 'aborted' "$raw"; then status=failed; fi
  if [ "$failed" != 0 ] || [ "$tx" = 0 ]; then status=failed; fi
  if [ "$status" != ok ] && [ "$final" != 1 ]; then
    log "cell $cell attempt failed (status $status, $tx transactions); retrying once"
    return 1
  fi
  wal=$(psql_b -c "SELECT pg_wal_lsn_diff('$lsn1', '$lsn0')" || echo 0)
  walpm=$(awk -v w="$wal" -v t="$tx" -v m="$V_MSGS" 'BEGIN { if (t > 0) printf "%.0f", w / (t * m); else print "nan" }')
  local p p50 p95 p99 pmax
  p=$(percentiles "$logprefix")
  read -r _ p50 p95 p99 pmax <<<"$p"
  awk -v s="$STORAGE_LABEL" -v v="$variant" -v b="$V_BATCH" -v c="$clients" -v d="$DURATION_S" -v tx="$tx" -v tps="$tps" \
    -v m="$V_MSGS" -v lat="$lat" -v p50="$p50" -v p95="$p95" -v p99="$p99" -v pmax="$pmax" -v f="$failed" -v w="$walpm" -v st="$status" \
    'BEGIN { printf "%s,%s,%d,%d,%d,%d,%.1f,%.0f,%s,%s,%s,%s,%s,%d,%s,%s\n", s, v, b, c, d, tx, tps, tps * m, lat, p50, p95, p99, pmax, f, w, st }' \
    >>"$OUT_DIR/results-$STORAGE_LABEL.csv"
  psql_b -c "\\copy (SELECT queryid, calls, round(total_exec_time::numeric,1), round(mean_exec_time::numeric,3), rows, left(query,120) FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 10) TO STDOUT CSV" \
    >"$OUT_DIR/raw/$cell.pgss.csv" 2>/dev/null || rm -f "$OUT_DIR/raw/$cell.pgss.csv"
  if [ "$status" != ok ]; then log "WARNING: cell $cell status $status (see $raw)"; fi
  return 0
}

run_all() {
  local csv="$OUT_DIR/results-$STORAGE_LABEL.csv" v c
  if [ ! -s "$csv" ] && ! dry; then
    echo "storage,variant,batch,clients,duration_s,transactions,tps,msgs_per_s,lat_avg_ms,p50_ms,p95_ms,p99_ms,max_ms,failed,wal_bytes_per_msg,status" >"$csv"
  fi
  for v in $VARIANTS; do
    for c in $CLIENTS; do
      run_cell "$v" "$c" 0 || run_cell "$v" "$c" 1
    done
  done
  log "cells done: $csv"
}

case "$mode" in
  setup) setup ;;
  run) run_all ;;
  summary) "$DIR/summarise.sh" "$OUT_DIR"/results-*.csv ;;
  all)
    setup
    run_all
    "$DIR/summarise.sh" "$OUT_DIR"/results-*.csv >"$OUT_DIR/summary-$STORAGE_LABEL.md"
    log "summary: $OUT_DIR/summary-$STORAGE_LABEL.md"
    ;;
  *) die "usage: run.sh [setup|run|all|summary]" ;;
esac
