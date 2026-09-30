#!/usr/bin/env bash
# shellcheck shell=bash
# Shared helpers for the bench/aws scripts. Source this file; do not run it.
#
#   * Account, region, tags and paths come from environment variables
#     (see env.example). Nothing account specific is written in a script.
#   * DRY_RUN=1 prints every aws, ssh and scp call as "DRYRUN <command>" on
#     stderr and never runs it. In a dry run, state writes go to a scratch
#     file (DRY_STATE_FILE), never to STATE_FILE, and LOG_FILE is not touched.
#   * aws_r is for reads. In a dry run it returns its FAKE argument, which is
#     usually empty (the resource is "not found", so the create path shows).
#     aws_w is for writes. In a dry run it returns its FAKE argument as the
#     "new id".

if [ -n "${_BENCH_LIB_LOADED:-}" ]; then return 0; fi
_BENCH_LIB_LOADED=1

if [ "${BASH_VERSINFO[0]}" -lt 4 ] || { [ "${BASH_VERSINFO[0]}" -eq 4 ] && [ "${BASH_VERSINFO[1]}" -lt 4 ]; }; then
  echo "bench/aws needs bash 4.4 or newer (found $BASH_VERSION). On macOS: brew install bash." >&2
  return 1 2>/dev/null || exit 1
fi

set -euo pipefail

BENCH_AWS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$BENCH_AWS_DIR/../.." && pwd)"
export BENCH_AWS_DIR REPO_ROOT
SCRIPT_NAME="${SCRIPT_NAME:-$(basename "${0:-lib}" .sh)}"

: "${PROJECT_TAG:=ably-server-scale}"
export PROJECT_TAG
: "${DRY_RUN:=0}"
_default_workshop="$HOME/Workshop/work/research/ably-server-scale-proof-2026-10"
: "${LOG_FILE:=$_default_workshop/LOG.md}"
: "${RESULTS_DIR:=$_default_workshop/results}"
: "${STATE_FILE:=$_default_workshop/STATE.json}"
: "${DRY_STATE_FILE:=${TMPDIR:-/tmp}/${PROJECT_TAG}-dryrun-STATE.json}"

: "${ECR_REPO_SERVER:=ably-server}"
: "${ECR_REPO_LOADGEN:=ably-loadgen}"
: "${DB_NAME:=ably}"
: "${DB_USER:=ably}"
: "${SERVER_PORT:=8080}"
: "${SERVER_DEBUG_PORT:=6060}"
: "${NODE_EXPORTER_PORT:=9100}"
: "${NATS_CLIENT_PORT:=4222}"
: "${NATS_ROUTE_PORT:=6222}"
: "${NATS_MONITOR_PORT:=8222}"
: "${NATS_EXPORTER_PORT:=7777}"
: "${LOADGEN_AGENT_PORT:=9100}"
: "${LOADGEN_METRICS_PORT:=9101}"

: "${NODE_INSTANCE_TYPE:=c7i.2xlarge}"
: "${NATS_INSTANCE_TYPE:=c7i.2xlarge}"
: "${LOADGEN_INSTANCE_TYPE:=c7i.8xlarge}"
: "${PUBLISHER_INSTANCE_TYPE:=c7i.4xlarge}"
: "${CONDUCTOR_INSTANCE_TYPE:=c7i.2xlarge}"
: "${PGDRIVER_INSTANCE_TYPE:=c7i.4xlarge}"
: "${RDS_INSTANCE_CLASS:=db.r7g.4xlarge}"
: "${RDS_STORAGE:=io2}"
: "${RDS_STORAGE_GB:=1000}"
: "${RDS_ENGINE_VERSION:=17}"
: "${NODE_COUNT:=10}"
: "${NATS_COUNT:=3}"
: "${LOADGEN_COUNT:=3}"
: "${PUBLISHER_COUNT:=2}"
: "${SHARDS:=1}"

# Image tags of the pinned third party images. Nothing else is bespoke.
: "${NATS_IMAGE:=nats:2.11}"
: "${NATS_EXPORTER_IMAGE:=natsio/prometheus-nats-exporter:0.15.0}"
: "${NODE_EXPORTER_IMAGE:=prom/node-exporter:v1.8.2}"
: "${PROMETHEUS_IMAGE:=prom/prometheus:v2.55.1}"
: "${GRAFANA_IMAGE:=grafana/grafana:11.3.0}"
: "${POSTGRES_EXPORTER_IMAGE:=quay.io/prometheuscommunity/postgres-exporter:v0.15.0}"
: "${PGBENCH_IMAGE:=postgres:17-alpine}"
: "${DOCKER_COMPOSE_VERSION:=v2.29.7}"

is_dry() { [ "$DRY_RUN" = 1 ]; }

if is_dry; then
  ACTIVE_STATE="$DRY_STATE_FILE"
  : "${AWS_ACCOUNT_ID:=000000000000}"
  : "${AWS_REGION:=dry-region-1}"
  : "${ADMIN_CIDR:=203.0.113.7/32}"
  : "${SSH_PUBLIC_KEY_PATH:=$HOME/.ssh/bench-dryrun.pub}"
  : "${RDS_PASSWORD:=dryrunpassword0123456789}"
  : "${ALARM_EMAIL:=alarm@example.invalid}"
  : "${RUN_TIME_LIMIT:=1h}"
else
  ACTIVE_STATE="$STATE_FILE"
fi
: "${AZ:=${AWS_REGION:-}a}"
: "${SSH_PRIVATE_KEY_PATH:=${SSH_PUBLIC_KEY_PATH:-}}"
SSH_PRIVATE_KEY_PATH="${SSH_PRIVATE_KEY_PATH%.pub}"
: "${BUDGET_ALARM_USD:=750}"
: "${BUDGET_CAP_USD:=1500}"
: "${ECR_REGISTRY:=${AWS_ACCOUNT_ID:-}.dkr.ecr.${AWS_REGION:-}.amazonaws.com}"
export ACTIVE_STATE

# ---------------------------------------------------------------- logging

log() { printf '%s [%s] %s\n' "$(date -u +%H:%M:%S)" "$SCRIPT_NAME" "$(_mask "$*")" >&2; }
die() { log "ERROR: $*"; exit 1; }

# log_line <step> <result> <next action>: one line in LOG.md format.
log_line() {
  local line
  line="$(date -u +'%Y-%m-%d %H:%M UTC') | AWS $1 | $(_mask "$2") | $3"
  if is_dry; then
    printf 'DRYLOG %s\n' "$line" >&2
  else
    mkdir -p "$(dirname "$LOG_FILE")"
    printf '%s\n' "$line" >>"$LOG_FILE"
  fi
}

_mask() {
  local s=$1
  if [ -n "${RDS_PASSWORD:-}" ]; then s=${s//"$RDS_PASSWORD"/***}; fi
  printf '%s' "$s"
}

need_cmd() {
  local c
  for c in "$@"; do
    command -v "$c" >/dev/null 2>&1 || die "required command not found: $c"
  done
}

require_env() {
  local v missing=()
  for v in "$@"; do
    if [ -z "${!v:-}" ]; then missing+=("$v"); fi
  done
  if [ "${#missing[@]}" -gt 0 ]; then
    die "set these environment variables first (see bench/aws/env.example): ${missing[*]}"
  fi
}

# RDS disallows / @ " and space; the password is also embedded in shell
# and DSN text, so keep it to a safe set.
check_password() {
  case "${RDS_PASSWORD:-}" in
    '') die "RDS_PASSWORD is not set" ;;
    *[!A-Za-z0-9_-]*) die "RDS_PASSWORD may contain only letters, digits, _ and -" ;;
  esac
  [ "${#RDS_PASSWORD}" -ge 16 ] || die "RDS_PASSWORD must be at least 16 characters"
}

# ------------------------------------------------------------ call wrapper

# _fmt <tool> args...: one printable line, secrets masked.
_fmt() {
  local out="" a mask_next=0
  for a in "$@"; do
    if [ "$mask_next" = 1 ]; then a='***'; mask_next=0; fi
    case "$a" in --master-user-password | --password) mask_next=1 ;; esac
    case "$a" in *[[:space:]]*) a="'$a'" ;; esac
    out+="${out:+ }$a"
  done
  _mask "$out"
}

# _ext <fake-output> <tool> args...: run a tool, or print it in a dry run.
_ext() {
  local fake=$1 tool=$2
  shift 2
  if is_dry; then
    local line
    line=$(_fmt "$tool" "$@")
    printf 'DRYRUN %s\n' "$line" >&2
    if [ -n "${DRYRUN_CALLS_FILE:-}" ]; then printf '%s\n' "$line" >>"$DRYRUN_CALLS_FILE"; fi
    if [ -n "$fake" ]; then printf '%s\n' "$fake"; fi
    return 0
  fi
  "$tool" "$@"
}

# aws_w <fake> args...: a write call.
aws_w() {
  local fake=$1
  shift
  _ext "$fake" aws "$@" --region "$AWS_REGION"
}

# aws_r <fake> args...: a read call. Text output unless --output is given;
# "None" becomes the empty string. AWS_R_QUIET=1 hides the error text of a
# real call (for "does it exist" probes).
aws_r() {
  local fake=$1
  shift
  local a have_out=0
  local -a extra=(--region "$AWS_REGION")
  for a in "$@"; do
    if [ "$a" = --output ]; then have_out=1; fi
  done
  if [ "$have_out" = 0 ]; then extra+=(--output text); fi
  if is_dry; then
    _ext "$fake" aws "$@" "${extra[@]}"
    return 0
  fi
  local out
  if [ "${AWS_R_QUIET:-0}" = 1 ]; then
    out=$(aws "$@" "${extra[@]}" 2>/dev/null) || return 1
  else
    out=$(aws "$@" "${extra[@]}") || return 1
  fi
  if [ "$out" = None ]; then out=""; fi
  printf '%s\n' "$out"
}

# aws_w_tolerate <stderr-pattern> <fake> args...: a write that may already
# be in place; an error matching the pattern is logged and ignored.
aws_w_tolerate() {
  local pattern=$1 fake=$2
  shift 2
  if is_dry; then
    aws_w "$fake" "$@"
    return 0
  fi
  local out
  if ! out=$(aws "$@" --region "$AWS_REGION" 2>&1); then
    if printf '%s' "$out" | grep -q -- "$pattern"; then
      log "already in place ($pattern): aws $1 $2"
      return 0
    fi
    printf '%s\n' "$out" >&2
    return 1
  fi
  if [ -n "$out" ]; then printf '%s\n' "$out"; fi
}

# ----------------------------------------------------------------- tags

tag_spec() { # <resource-type> <name> [role]
  local rt=$1 name=$2 role=${3:-}
  local tags="{Key=Project,Value=$PROJECT_TAG},{Key=Name,Value=$name}"
  if [ -n "$role" ]; then tags+=",{Key=Role,Value=$role}"; fi
  printf 'ResourceType=%s,Tags=[%s]' "$rt" "$tags"
}

# rds_tags <name> [role]: fills the RDS_TAGS array (for --tags).
rds_tags() {
  RDS_TAGS=("Key=Project,Value=$PROJECT_TAG" "Key=Name,Value=$1")
  if [ -n "${2:-}" ]; then RDS_TAGS+=("Key=Role,Value=$2"); fi
}

# iam_tags <name>: fills the IAM_TAGS array (IAM wants Key=,Value= pairs).
# shellcheck disable=SC2034
iam_tags() { IAM_TAGS=("Key=Project,Value=$PROJECT_TAG" "Key=Name,Value=$1"); }

project_filter() { printf 'Name=tag:Project,Values=%s' "$PROJECT_TAG"; }

# ---------------------------------------------------------------- state

state_init() {
  if is_dry && [ "${DRY_RESET:-0}" = 1 ] && [ -z "${_DRY_RESET_DONE:-}" ]; then
    rm -f "$ACTIVE_STATE"
    export _DRY_RESET_DONE=1
  fi
  mkdir -p "$(dirname "$ACTIVE_STATE")"
  if [ ! -s "$ACTIVE_STATE" ]; then
    printf '%s\n' '{"resources":[],"images":{},"runs":[]}' >"$ACTIVE_STATE"
  fi
}

state_get() { # <jq filter>; prints "" for null/missing
  [ -s "$ACTIVE_STATE" ] || return 0
  jq -r "($1) // empty" "$ACTIVE_STATE"
}

_state_update() { # <filter> [jq args...]
  local filter=$1 tmp
  shift
  state_init
  tmp=$(mktemp "${ACTIVE_STATE}.XXXXXX")
  if jq "$@" "$filter" "$ACTIVE_STATE" >"$tmp"; then
    mv "$tmp" "$ACTIVE_STATE"
  else
    rm -f "$tmp"
    die "state update failed: $filter"
  fi
}

state_set() { # <jq path> <string value>
  _state_update "$1 = \$v" --arg v "$2"
}

state_set_json() { # <jq path> <json value>
  _state_update "$1 = \$v" --argjson v "$2"
}

state_add_resource() { # <type> <id> <role> <name>
  _state_update '.resources = ((.resources // []) | map(select(.id != $id)) + [{type:$t,id:$id,role:$r,name:$n,at:$at}])' \
    --arg t "$1" --arg id "$2" --arg r "$3" --arg n "$4" --arg at "$(date -u +%FT%TZ)"
}

state_del_resource() { # <id>
  _state_update '.resources = ((.resources // []) | map(select(.id != $id)))' --arg id "$1"
}

state_resource_ids() { # <type>
  state_get ".resources[]? | select(.type == \"$1\") | .id"
}

state_instance_names() { state_get '.instances // {} | keys[]'; }

inst_field() { # <name> <field>
  state_get ".instances[\"$1\"].$2"
}

# state_put_instance <name> <id> <role> <type> <private_ip> <public_ip>
state_put_instance() {
  _state_update '.instances[$n] = ((.instances[$n] // {}) + {id:$id,role:$r,type:$t,private_ip:$p,public_ip:$q,running:true})' \
    --arg n "$1" --arg id "$2" --arg r "$3" --arg t "$4" --arg p "$5" --arg q "$6"
  state_add_resource instance "$2" "$3" "$1"
}

# ---------------------------------------------------------------- misc

# parse_duration 90m -> 5400. Plain numbers are seconds.
parse_duration() {
  local v=$1 n u
  case "$v" in
    *[!0-9smh]* | '') return 1 ;;
  esac
  n=${v%[smh]}
  u=${v#"$n"}
  [ -n "$n" ] || return 1
  case "$u" in
    '' | s) echo "$n" ;;
    m) echo $((n * 60)) ;;
    h) echo $((n * 3600)) ;;
  esac
}

ip_to_int() {
  local a b c d
  IFS=. read -r a b c d <<<"$1"
  echo $(((a << 24) + (b << 16) + (c << 8) + d))
}

int_to_ip() {
  local n=$1
  echo "$(((n >> 24) & 255)).$(((n >> 16) & 255)).$(((n >> 8) & 255)).$((n & 255))"
}

# cidr_host_ip <cidr> <offset>: the address <offset> above the network base.
cidr_host_ip() {
  local cidr=$1 off=$2 base prefix size
  base=${cidr%/*}
  prefix=${cidr#*/}
  size=$((1 << (32 - prefix)))
  [ "$off" -gt 4 ] && [ "$off" -lt $((size - 2)) ] || die "offset $off is outside subnet $cidr"
  int_to_ip $(($(ip_to_int "$base") + off))
}

# render_template <file> KEY=VALUE...: replaces @@KEY@@ markers, to stdout.
render_template() {
  local f=$1 content kv k v
  shift
  content=$(
    cat "$f"
    printf x
  )
  content=${content%x}
  for kv in "$@"; do
    k=${kv%%=*}
    v=${kv#*=}
    content=${content//"@@$k@@"/"$v"}
  done
  if printf '%s' "$content" | grep -q '@@[A-Z_]*@@'; then
    die "unreplaced marker in $f: $(printf '%s' "$content" | grep -o '@@[A-Z_]*@@' | sort -u | tr '\n' ' ')"
  fi
  printf '%s' "$content"
}

# init_work_dir: create a private scratch directory (rendered user-data, JSON)
# in the CURRENT shell and set BENCH_WORK_DIR. Do not call it in $( ).
init_work_dir() {
  if [ -z "${BENCH_WORK_DIR:-}" ]; then
    BENCH_WORK_DIR=$(umask 077 && mktemp -d "${TMPDIR:-/tmp}/bench-aws.XXXXXX")
    export BENCH_WORK_DIR
    if [ "${KEEP_WORK_DIR:-0}" != 1 ]; then
      # shellcheck disable=SC2064
      trap "rm -rf '$BENCH_WORK_DIR'" EXIT
    fi
  fi
}

# wait_until <description> <timeout-s> <interval-s> <command...>
wait_until() {
  local desc=$1 timeout=$2 interval=$3 start now
  shift 3
  if is_dry; then
    printf 'DRYWAIT %s\n' "$desc" >&2
    return 0
  fi
  start=$(date +%s)
  until "$@"; do
    now=$(date +%s)
    if [ $((now - start)) -ge "$timeout" ]; then die "timed out after ${timeout}s waiting for: $desc"; fi
    sleep "$interval"
  done
}

fake_ip() { # deterministic fake private address for dry runs
  local h
  h=$(printf '%s' "$1" | cksum | cut -d' ' -f1)
  echo "10.0.$((h % 200 + 1)).$(((h / 200) % 200 + 10))"
}

ensure_api_key() {
  local k
  k=$(state_get '.api_key')
  if [ -z "$k" ]; then
    k="bench.$(openssl rand -hex 4):$(openssl rand -hex 16)"
    state_set '.api_key' "$k"
  fi
  printf '%s' "$k"
}

require_network_state() {
  local f
  for f in vpc_id subnet_id sg_id key_name ami_id; do
    [ -n "$(state_get ".network.$f")" ] || die "STATE has no network.$f; run 10-network.sh first"
  done
}

# ------------------------------------------------------------- instances

resolve_ami() {
  local ami
  ami=$(state_get '.network.ami_id')
  if [ -z "$ami" ]; then
    ami=$(aws_r ami-dryrun ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 --query Parameter.Value)
    state_set '.network.ami_id' "$ami"
  fi
  printf '%s' "$ami"
}

find_instance() { # <name> -> instance id (non-terminated) or ""
  aws_r "" ec2 describe-instances \
    --filters "$(project_filter)" "Name=tag:Name,Values=$1" Name=instance-state-name,Values=pending,running,stopping,stopped \
    --query 'Reservations[].Instances[].InstanceId'
}

# launch_instance <name> <role> <type> <userdata-file> [private-ip] [placement 1|0]
# Prints the instance id. Reuses an instance with the same Name tag.
launch_instance() {
  local name=$1 role=$2 type=$3 ud=$4 pip=${5:-} place=${6:-1}
  local existing id sg subnet key ami profile
  existing=$(find_instance "$name")
  if [ -n "$existing" ]; then
    log "reuse $name ($existing)"
    if [ -z "$(inst_field "$name" id)" ]; then state_put_instance "$name" "$existing" "$role" "$type" "" ""; fi
    printf '%s' "$existing"
    return 0
  fi
  sg=$(state_get '.network.sg_id')
  subnet=$(state_get '.network.subnet_id')
  key=$(state_get '.network.key_name')
  ami=$(state_get '.network.ami_id')
  profile=$(state_get '.network.instance_profile')
  local -a args=(ec2 run-instances
    --image-id "$ami" --instance-type "$type" --key-name "$key"
    --security-group-ids "$sg" --subnet-id "$subnet"
    --user-data "file://$ud"
    --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=60,VolumeType=gp3,DeleteOnTermination=true}'
    --metadata-options 'HttpTokens=required,HttpPutResponseHopLimit=2'
    --tag-specifications "$(tag_spec instance "$name" "$role")" "$(tag_spec volume "$name" "$role")"
    --query 'Instances[0].InstanceId' --output text)
  if [ -n "$profile" ]; then args+=(--iam-instance-profile "Name=$profile"); fi
  if [ "$place" = 1 ]; then
    args+=(--placement "GroupName=$(state_get '.network.placement_group'),AvailabilityZone=$AZ")
  else
    args+=(--placement "AvailabilityZone=$AZ")
  fi
  if [ -n "$pip" ]; then args+=(--private-ip-address "$pip"); fi
  id=$(aws_w "i-dry-$name" "${args[@]}")
  if is_dry; then
    state_put_instance "$name" "$id" "$role" "$type" "${pip:-$(fake_ip "$name")}" "198.51.100.$(($(printf '%s' "$name" | cksum | cut -d' ' -f1) % 200 + 10))"
  else
    state_put_instance "$name" "$id" "$role" "$type" "$pip" ""
  fi
  log "launched $name ($id, $type)"
  printf '%s' "$id"
}

# refresh_instances: copy addresses and run state of every live project
# instance into STATE (real runs only; a dry run set fake values at launch).
refresh_instances() {
  local json
  json=$(aws_r '[]' ec2 describe-instances --filters "$(project_filter)" Name=instance-state-name,Values=pending,running,stopping,stopped \
    --query 'Reservations[].Instances[].{id:InstanceId,name:Tags[?Key==`Name`]|[0].Value,role:Tags[?Key==`Role`]|[0].Value,type:InstanceType,private_ip:PrivateIpAddress,public_ip:PublicIpAddress,state:State.Name}' \
    --output json)
  if is_dry; then return 0; fi
  local row n
  while IFS= read -r row; do
    [ -n "$row" ] || continue
    n=$(jq -r .name <<<"$row")
    state_put_instance "$n" "$(jq -r .id <<<"$row")" "$(jq -r '.role // ""' <<<"$row")" "$(jq -r .type <<<"$row")" \
      "$(jq -r '.private_ip // ""' <<<"$row")" "$(jq -r '.public_ip // ""' <<<"$row")"
    _state_update '.instances[$n].running = ($s == "running" or $s == "pending")' --arg n "$n" --arg s "$(jq -r .state <<<"$row")"
  done < <(jq -c '.[]' <<<"$json")
}

# wait_instances_running <id>...
wait_instances_running() {
  [ "$#" -gt 0 ] || return 0
  aws_w "" ec2 wait instance-running --instance-ids "$@"
}

# ssh helpers. The host is the instance's public address from STATE.
_ssh_opts() {
  printf '%s\n' -i "$SSH_PRIVATE_KEY_PATH" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    -o LogLevel=ERROR -o ConnectTimeout=15 -o ServerAliveInterval=30
}

ssh_do() { # <instance-name> <remote command...>
  local name=$1 ip
  shift
  ip=$(inst_field "$name" public_ip)
  [ -n "$ip" ] || die "no public address for $name in STATE (run refresh via 80-start.sh or 40/50)"
  local -a opts
  mapfile -t opts < <(_ssh_opts)
  _ext "" ssh "${opts[@]}" "ec2-user@$ip" "$@"
}

scp_to() { # <instance-name> <local path> <remote path> (recursive)
  local name=$1 ip
  ip=$(inst_field "$name" public_ip)
  [ -n "$ip" ] || die "no public address for $name in STATE"
  local -a opts
  mapfile -t opts < <(_ssh_opts)
  _ext "" scp -r "${opts[@]}" "$2" "ec2-user@$ip:$3"
}

scp_from() { # <instance-name> <remote path> <local path> (recursive)
  local name=$1 ip
  ip=$(inst_field "$name" public_ip)
  [ -n "$ip" ] || die "no public address for $name in STATE"
  local -a opts
  mapfile -t opts < <(_ssh_opts)
  _ext "" scp -r "${opts[@]}" "ec2-user@$ip:$2" "$3"
}

wait_boot() { # <instance-name>... wait for cloud-init to finish
  local n
  [ "${SKIP_BOOT_WAIT:-0}" = 1 ] && return 0
  for n in "$@"; do
    ssh_do "$n" 'sudo cloud-init status --wait'
  done
}

# render_userdata <outfile> <role-template> KEY=VALUE...
# Output is templates/common.sh plus templates/<role-template>.sh.
render_userdata() {
  local out=$1 role=$2
  shift 2
  {
    render_template "$BENCH_AWS_DIR/templates/common.sh" \
      "REGION=$AWS_REGION" "ECR_REGISTRY=${ECR_LOGIN_REGISTRY-$ECR_REGISTRY}" \
      "NODE_EXPORTER_IMAGE=$NODE_EXPORTER_IMAGE" "NODE_EXPORTER_PORT=$NODE_EXPORTER_PORT" \
      "COMPOSE_VERSION=$DOCKER_COMPOSE_VERSION" "INSTALL_COMPOSE=${INSTALL_COMPOSE:-0}"
    printf '\n'
    render_template "$BENCH_AWS_DIR/templates/$role.sh" "$@"
    printf '\n'
  } >"$out"
  chmod 600 "$out"
  local size
  size=$(wc -c <"$out")
  [ "$size" -lt 16000 ] || die "user-data for $role is $size bytes; the EC2 limit is 16384"
}

# apply_role_script <instance-name> <role-script-file>: re-run a role part of
# the user-data on a live box (new image tag, new flags). The role scripts
# remove their own container first.
apply_role_script() {
  scp_to "$1" "$2" /tmp/role.sh
  ssh_do "$1" 'sudo bash -euxo pipefail /tmp/role.sh'
}

# iname <role> <index>: the Name tag of an instance.
iname() { printf '%s-%s-%s' "$PROJECT_TAG" "$1" "$2"; }

# image_tag <ECR repo key in STATE.images> <override>: a tag from the
# environment, else the one build-push.sh recorded.
image_tag() {
  local key=$1 override=${2:-} t
  t=${override:-$(state_get ".images[\"$key\"].tag")}
  [ -n "$t" ] || die "no image tag for $key: set it in the environment or run build-push.sh"
  printf '%s' "$t"
}

# ------------------------------------------------------------- postgres

# postgres_dsns: comma separated DSNs of the active storage type, shard order.
postgres_dsns() {
  local active
  active=$(state_get '.postgres.active_storage')
  [ -n "$active" ] || die "STATE has no postgres.active_storage; run 20-postgres.sh first"
  state_get "[.postgres.instances[] | select(.storage == \"$active\")] | sort_by(.shard) | map(.dsn) | join(\",\")"
}

# load_postgres_password: when RDS_PASSWORD is not in the environment, take it
# from the first DSN in STATE so that masking still hides it in logs and
# dry-run output. Call in the main shell (not inside $( )).
load_postgres_password() {
  local dsn
  if [ -n "${RDS_PASSWORD:-}" ]; then return 0; fi
  dsn=$(state_get '.postgres.instances // {} | to_entries | map(.value.dsn) | first')
  if [ -n "$dsn" ]; then
    RDS_PASSWORD=$(printf '%s' "$dsn" | sed -E 's#^[a-z]+://[^:]*:([^@]*)@.*#\1#')
    export RDS_PASSWORD
  fi
  return 0
}

# ------------------------------------------------------------------ cost

# price_of <key>: USD per hour. PRICE_<KEY> overrides (dots become _).
# These are approximate on-demand list prices (plan section 11 figures);
# check the AWS pricing pages before relying on them.
price_of() {
  local key=$1 var
  var="PRICE_$(printf '%s' "$key" | tr '.-' '__')"
  if [ -n "${!var:-}" ]; then
    printf '%s' "${!var}"
    return 0
  fi
  case "$key" in
    c7i.large) echo 0.09 ;;
    c7i.xlarge) echo 0.18 ;;
    c7i.2xlarge) echo 0.36 ;;
    c7i.4xlarge) echo 0.71 ;;
    c7i.8xlarge) echo 1.43 ;;
    c7i.12xlarge) echo 2.14 ;;
    c7i.16xlarge) echo 2.86 ;;
    db.r7g.2xlarge) echo 0.95 ;;
    db.r7g.4xlarge) echo 1.90 ;;
    db.r7g.8xlarge) echo 3.80 ;;
    *)
      log "WARNING: no price for $key; counting it as 0 (set PRICE_$(printf '%s' "$key" | tr '.-' '__'))"
      echo 0
      ;;
  esac
}

# vcpus_of <instance type> for c7i sizes (quota arithmetic).
vcpus_of() {
  case "${1##*.}" in
    large) echo 2 ;;
    xlarge) echo 4 ;;
    2xlarge) echo 8 ;;
    4xlarge) echo 16 ;;
    8xlarge) echo 32 ;;
    12xlarge) echo 48 ;;
    16xlarge) echo 64 ;;
    24xlarge) echo 96 ;;
    48xlarge) echo 192 ;;
    *) die "unknown instance size: $1" ;;
  esac
}

# io2 provisioned IOPS and storage are a real part of the RDS bill.
: "${IO2_USD_PER_IOPS_MONTH:=0.10}"
: "${IO2_USD_PER_GB_MONTH:=0.125}"
: "${GP3_USD_PER_GB_MONTH:=0.12}"

rds_hourly() { # <class> <storage> <gb> <iops>
  awk -v inst="$(price_of "$1")" -v st="$2" -v gb="$3" -v iops="${4:-0}" \
    -v i_iops="$IO2_USD_PER_IOPS_MONTH" -v i_gb="$IO2_USD_PER_GB_MONTH" -v g_gb="$GP3_USD_PER_GB_MONTH" \
    'BEGIN { s = inst; if (st == "io2") s += (gb*i_gb + iops*i_iops)/730; else s += gb*g_gb/730; printf "%.3f\n", s }'
}

# fleet_hourly_rate: USD per hour of everything STATE says is running.
fleet_hourly_rate() {
  local total=0 type cls st gb iops row
  while IFS= read -r type; do
    [ -n "$type" ] || continue
    total=$(awk -v t="$total" -v p="$(price_of "$type")" 'BEGIN{printf "%.4f", t+p}')
  done < <(state_get '.instances // {} | to_entries[] | select(.value.running == true) | .value.type')
  while IFS= read -r row; do
    [ -n "$row" ] || continue
    cls=$(jq -r .class <<<"$row")
    st=$(jq -r .storage <<<"$row")
    gb=$(jq -r '.storage_gb // 0' <<<"$row")
    iops=$(jq -r '.iops // 0' <<<"$row")
    total=$(awk -v t="$total" -v p="$(rds_hourly "$cls" "$st" "$gb" "$iops")" 'BEGIN{printf "%.4f", t+p}')
  done < <(state_get '.postgres.instances // {} | to_entries[] | select(.value.running == true) | .value' | jq -c . 2>/dev/null || true)
  printf '%s' "$total"
}

# cost_checkpoint: close the open interval at the stored rate, then start a
# new one at the current rate. Call after any change to what is running.
cost_checkpoint() {
  local now since rate acc add
  now=$(date +%s)
  since=$(state_get '.budget.since_epoch')
  rate=$(state_get '.budget.rate_usd_h')
  acc=$(state_get '.budget.accrued_usd')
  : "${acc:=0}"
  if [ -n "$since" ] && [ -n "$rate" ]; then
    add=$(awk -v n="$now" -v s="$since" -v r="$rate" 'BEGIN{printf "%.4f", (n-s)/3600*r}')
    acc=$(awk -v a="$acc" -v d="$add" 'BEGIN{printf "%.4f", a+d}')
  fi
  state_set_json '.budget.accrued_usd' "$acc"
  state_set_json '.budget.since_epoch' "$now"
  state_set_json '.budget.rate_usd_h' "$(fleet_hourly_rate)"
}
