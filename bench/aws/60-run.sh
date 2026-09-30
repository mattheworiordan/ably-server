#!/usr/bin/env bash
# 60-run: render the inventory from STATE and run one scenario on the
# conductor. Refuses without RUN_TIME_LIMIT and when the estimated spend plus
# this run would pass the cap. The conductor runs detached on its box (a
# dropped SSH session does not stop a run); this script polls for its exit
# code, enforces the time limit remotely with timeout(1), and copies the
# results to $RESULTS_DIR/<run-id>/ on the way out.
#
#   RUN_TIME_LIMIT=45m bench/aws/60-run.sh smoke-1pct.yaml
#
# Needs: RUN_TIME_LIMIT (300, 45m, 2h). Scenario: a path, or a name resolved
# against SCENARIO_DIR (default: bench/scenarios in the repository).
# Optional: CONDUCTOR_CMD (see below), AUTO_STOP_AFTER_RUN=1, OVERRIDE_BUDGET_GUARD=1.
#
# CONDUCTOR_CMD is the command run inside the ably-loadgen image. Tokens
# {RUN_ID} and {SCENARIO} are replaced. The default is provisional until the
# load generator branch fixes its flags:
#   ably-conductor --scenario=/run-input/{SCENARIO} --inventory=/run-input/inventory.json --results=/results --run-id={RUN_ID}
SCRIPT_NAME=60-run
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
scenario_arg=${1:?usage: 60-run.sh <scenario-file-or-name>}
state_init
load_postgres_password

require_run_limit
limit_s=$RUN_LIMIT_S

# Scenario file.
: "${SCENARIO_DIR:=$REPO_ROOT/bench/scenarios}"
scenario=""
for cand in "$scenario_arg" "$SCENARIO_DIR/$scenario_arg" "$SCENARIO_DIR/$scenario_arg.yaml" "$SCENARIO_DIR/$scenario_arg.yml" "$SCENARIO_DIR/$scenario_arg.json"; do
  if [ -f "$cand" ]; then
    scenario=$cand
    break
  fi
done
if [ -z "$scenario" ]; then
  if is_dry; then
    scenario="$SCENARIO_DIR/$scenario_arg"
    log "dry run: scenario $scenario_arg not found, continuing with a placeholder"
  else
    die "scenario not found: $scenario_arg (looked in $SCENARIO_DIR)"
  fi
fi
scenario_file=$(basename "$scenario")

budget_guard "$limit_s"
accrued=$BUDGET_ACCRUED

cname=$(iname conductor 1)
[ -n "$(inst_field "$cname" id)" ] || die "no conductor in STATE; run 50-loadgen.sh first"
nodes=$(state_get '[.instances // {} | to_entries[] | select(.value.role == "node")] | length')
[ "${nodes:-0}" -gt 0 ] || die "no ably-server nodes in STATE; run 40-nodes.sh first"

run_id="run-$(date -u +%Y%m%dT%H%M%SZ)-${scenario_file%.*}"
rdir="/opt/bench/results/$run_id"
idir="/opt/bench/run/$run_id"
init_work_dir

# Inventory: what the conductor needs to address the fleet. No secrets but the API key.
jq --arg run "$run_id" '
  def members($role): [(.instances // {}) | to_entries[] | select(.value.role == $role) | .value.private_ip] | sort;
  {run_id: $run, bus: (.deployment.bus // ""), server_image: (.deployment.server_image // ""),
   api_key: .api_key,
   nodes: [members("node")[] | {http: ("http://" + . + ":'"$SERVER_PORT"'"), ws: ("ws://" + . + ":'"$SERVER_PORT"'"), metrics: ("http://" + . + ":'"$SERVER_DEBUG_PORT"'/metrics")}],
   nats: (.nats.urls // ""),
   generators: [members("loadgen")[] | {agent: (. + ":'"$LOADGEN_AGENT_PORT"'"), metrics: ("http://" + . + ":'"$LOADGEN_METRICS_PORT"'/metrics")}],
   publishers: [members("publisher")[] | {agent: (. + ":'"$LOADGEN_AGENT_PORT"'"), metrics: ("http://" + . + ":'"$LOADGEN_METRICS_PORT"'/metrics")}],
   postgres: {shards: [.postgres.active_storage as $a | (.postgres.instances // {}) | to_entries[] | .value | select(.storage == $a) | {id, storage, shard, endpoint}]},
   prometheus: "http://127.0.0.1:9090"}' "$ACTIVE_STATE" >"$BENCH_WORK_DIR/inventory.json"

_state_update '.runs += [{id:$id,scenario:$sc,started_at:$at,status:"running",limit_s:($lim|tonumber),bus:(.deployment.bus // ""),server_image:(.deployment.server_image // ""),loadgen_image:(.loadgen.image // ""),nodes:($n|tonumber),est_spend_before_usd:($acc|tonumber)}]' \
  --arg id "$run_id" --arg sc "$scenario_file" --arg at "$(date -u +%FT%TZ)" --arg lim "$limit_s" --arg n "$nodes" --arg acc "$accrued"
log "run $run_id: scenario $scenario_file, limit ${limit_s}s"
log_line 60-run "run $run_id started: $scenario_file, limit $RUN_TIME_LIMIT, bus=$(state_get '.deployment.bus'), $nodes nodes" "wait for exit code; 70-collect.sh $run_id"

ssh_do "$cname" "mkdir -p $idir $rdir"
scp_to "$cname" "$BENCH_WORK_DIR/inventory.json" "$idir/inventory.json"
scp_to "$cname" "$scenario" "$idir/$scenario_file"

# Docker stats sampler on every server-side box; it stops itself after the limit.
stat_limit=$((limit_s + 120))
for name in $(state_get '.instances // {} | to_entries[] | select(.value.role == "node" or .value.role == "nats" or .value.role == "loadgen" or .value.role == "publisher") | .key'); do
  ssh_do "$name" "nohup timeout $stat_limit sh -c 'while true; do docker stats --no-stream --format \"{{json .}}\" >> /var/tmp/docker-stats-$run_id.jsonl; sleep 10; done' >/dev/null 2>&1 </dev/null &"
done

: "${CONDUCTOR_CMD:=ably-conductor --scenario=/run-input/{SCENARIO} --inventory=/run-input/inventory.json --results=/results --run-id={RUN_ID}}"
cmd=${CONDUCTOR_CMD//\{RUN_ID\}/$run_id}
cmd=${cmd//\{SCENARIO\}/$scenario_file}
image=$(state_get '.loadgen.image')
[ -n "$image" ] || die "STATE has no loadgen image; run 50-loadgen.sh"
printf '#!/usr/bin/env bash\nexec docker run --rm --name conductor-run --network host -v %s:/run-input:ro -v %s:/results %s %s\n' \
  "$idir" "$rdir" "$image" "$cmd" >"$BENCH_WORK_DIR/conductor-cmd.sh"
run_detached "$cname" "$rdir" "$BENCH_WORK_DIR/conductor-cmd.sh" "$limit_s"
rc=$RUN_RC

status=ok
case "$rc" in
  0) status=ok ;;
  124 | 137) status=timeout ;;
  unknown) status=lost ;;
  *) status=failed ;;
esac
mkdir -p "$RESULTS_DIR" 2>/dev/null || true
scp_from "$cname" "$rdir" "$RESULTS_DIR/" || log "WARNING: could not copy $rdir; fetch it by hand (RUNBOOK, 'Collect')"
_state_update '.runs |= map(if .id == $id then . + {status:$st,exit_code:$rc,finished_at:$at} else . end)' \
  --arg id "$run_id" --arg st "$status" --arg rc "$rc" --arg at "$(date -u +%FT%TZ)"
cost_checkpoint
log_line 60-run "run $run_id finished: $status (exit $rc); results in $RESULTS_DIR/$run_id" "70-collect.sh $run_id; then judge against the pass criteria (plan section 8)"
if [ "${AUTO_STOP_AFTER_RUN:-0}" = 1 ]; then "$BENCH_AWS_DIR/80-stop.sh"; fi
[ "$status" = ok ] || exit 1
