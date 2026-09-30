#!/usr/bin/env bash
# 65-run-0a: run 0a, Postgres alone. Copies bench/aws/pgbench to the driver box
# (25-pgdriver.sh), runs pgbench against one Postgres instance there, detached and
# under RUN_TIME_LIMIT, then copies the raw output, CSV and markdown tables to
# results/0a-<storage>-<timestamp>/ and writes a combined summary.
#
#   RUN_TIME_LIMIT=2h bench/aws/65-run-0a.sh io2
#   RUN_TIME_LIMIT=2h bench/aws/65-run-0a.sh gp3     # after 20-postgres.sh with PG_STORAGE=gp3
#
# The argument names the storage type; its instance must be in STATE
# (20-postgres.sh). Cells: VARIANTS x CLIENTS at DURATION_S seconds each plus
# WARMUP_S; the defaults (7 variants, 4 client counts, 60 s + 10 s) take about
# 35 minutes. Tune with DURATION_S, WARMUP_S, CLIENTS, VARIANTS, CHANNELS,
# PAYLOAD_BYTES (see pgbench/run.sh).
SCRIPT_NAME=65-run-0a
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
storage=${1:?usage: 65-run-0a.sh <io2|gp3>}
state_init
require_preflight
load_postgres_password
require_run_limit
limit_s=$RUN_LIMIT_S

: "${DURATION_S:=60}" "${WARMUP_S:=10}" "${CLIENTS:=1 8 32 128}"
: "${VARIANTS:=trivial shipped pipelined pipelined_notify batch10 batch30 batch100}"
: "${CHANNELS:=200000}" "${PAYLOAD_BYTES:=600}"
ncells=$(($(wc -w <<<"$VARIANTS") * $(wc -w <<<"$CLIENTS")))
est_s=$((ncells * (DURATION_S + WARMUP_S + 12)))
log "$ncells cells, about $((est_s / 60)) minutes of pgbench"
if [ "$est_s" -gt "$limit_s" ]; then
  die "the cells need about ${est_s}s but RUN_TIME_LIMIT is ${limit_s}s. Raise RUN_TIME_LIMIT or shorten DURATION_S, CLIENTS or VARIANTS."
fi

id="${PROJECT_TAG}-pg-${storage}"
dsn=$(state_get ".postgres.instances[\"$id\"].dsn")
[ -n "$dsn" ] || die "no Postgres instance $id in STATE; run 20-postgres.sh with PG_STORAGE=$storage"
[ "$(state_get ".postgres.instances[\"$id\"].running")" = true ] || die "$id is not running (terminated by the dead-man switch?): re-run 20-postgres.sh with PG_STORAGE=$storage"
parse_dsn "$dsn"
driver=$(iname pgdriver 1)
[ -n "$(inst_field "$driver" id)" ] || die "no driver box in STATE; run 25-pgdriver.sh"

budget_guard "$limit_s"
init_work_dir
stamp=$(date -u +%Y%m%dT%H%M%SZ)
run_id="0a-${storage}-${stamp}"
rdir="/opt/bench/results/$run_id"

# Connection settings go to the box in a private env file, never on a command line.
(
  umask 077
  cat >"$BENCH_WORK_DIR/pg.env" <<ENV
export PGHOST='$DSN_HOST' PGPORT='$DSN_PORT' PGUSER='$DSN_USER' PGPASSWORD='$DSN_PASSWORD' PGDATABASE='postgres'
ENV
  cat >"$BENCH_WORK_DIR/0a-cmd.sh" <<CMD
#!/usr/bin/env bash
set -euo pipefail
cd /opt/bench/pgbench
. $rdir/pg.env
export PGTOOL=docker PGBENCH_IMAGE='$PGBENCH_IMAGE' STORAGE_LABEL='$storage' OUT_DIR='$rdir/out'
export DURATION_S='$DURATION_S' WARMUP_S='$WARMUP_S' CLIENTS='$CLIENTS' VARIANTS='$VARIANTS' CHANNELS='$CHANNELS' PAYLOAD_BYTES='$PAYLOAD_BYTES'
exec ./run.sh all
CMD
)
_state_update '.runs += [{id:$id,scenario:"0a",storage:$st,started_at:$at,status:"running",limit_s:($lim|tonumber),pg_type:(.postgres.instances[$pg].type // ""),pg_storage_gb:(.postgres.instances[$pg].storage_gb // 0),pg_iops:(.postgres.instances[$pg].iops // 0),pg_engine:(.postgres.instances[$pg].engine_version // ""),est_spend_before_usd:($acc|tonumber)}]' \
  --arg id "$run_id" --arg st "$storage" --arg at "$(date -u +%FT%TZ)" --arg lim "$limit_s" --arg pg "$id" --arg acc "$BUDGET_ACCRUED"
log_line 65-run-0a "run 0a on $storage started ($run_id): $ncells cells, limit $RUN_TIME_LIMIT" "wait for exit code; results in $RESULTS_DIR/$run_id"

ssh_do "$driver" "mkdir -p /opt/bench/pgbench $rdir"
scp_to "$driver" "$BENCH_AWS_DIR/pgbench/." /opt/bench/pgbench/
scp_to "$driver" "$BENCH_WORK_DIR/pg.env" "$rdir/pg.env"
run_detached "$driver" "$rdir" "$BENCH_WORK_DIR/0a-cmd.sh" "$limit_s"
rc=$RUN_RC

status=ok
case "$rc" in 0) ;; 124 | 137) status=timeout ;; unknown) status=lost ;; *) status=failed ;; esac
mkdir -p "$RESULTS_DIR" 2>/dev/null || true
scp_from "$driver" "$rdir" "$RESULTS_DIR/" || log "WARNING: could not copy $rdir; fetch it by hand"
ssh_do "$driver" "rm -f $rdir/pg.env" || true
if ! is_dry; then
  rm -f "$RESULTS_DIR/$run_id/pg.env"
  if ls "$RESULTS_DIR/$run_id"/out/results-*.csv >/dev/null 2>&1; then
    # Combined tables across every 0a run for this project (io2 and gp3 side by side).
    "$BENCH_AWS_DIR/pgbench/summarise.sh" "$RESULTS_DIR/$run_id"/out/results-*.csv >"$RESULTS_DIR/$run_id/summary.md"
  fi
fi
_state_update '.runs |= map(if .id == $id then . + {status:$st,exit_code:$rc,finished_at:$at} else . end)' \
  --arg id "$run_id" --arg st "$status" --arg rc "$rc" --arg at "$(date -u +%FT%TZ)"
cost_checkpoint
log_line 65-run-0a "run 0a on $storage finished: $status (exit $rc); results in $RESULTS_DIR/$run_id" "repeat with the other storage type, then summarise both (pgbench/summarise.sh results-io2.csv results-gp3.csv)"
[ "$status" = ok ] || exit 1
