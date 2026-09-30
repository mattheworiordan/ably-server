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

: "${BILLING_REGION:=us-east-1}" # the only region that has billing metrics
: "${FLEET_MAX_UPTIME_H:=10}"    # dead-man switch: every box terminates itself after this long
: "${USE_PLACEMENT_GROUP:=0}"    # the Operator role may not create one; 1 tries it and falls back
: "${DB_NAME:=ably}"
: "${DB_USER:=ably}"
: "${SERVER_PORT:=8080}"
: "${SERVER_DEBUG_PORT:=6060}"
: "${NODE_EXPORTER_PORT:=9100}"
: "${NATS_CLIENT_PORT:=4222}"
: "${NATS_ROUTE_PORT:=6222}"
: "${NATS_MONITOR_PORT:=8222}"
: "${NATS_EXPORTER_PORT:=7777}"
: "${LOADGEN_AGENT_PORT:=9200}"
: "${LOADGEN_METRICS_PORT:=9101}"

: "${NODE_INSTANCE_TYPE:=c7i.2xlarge}"
: "${NATS_INSTANCE_TYPE:=c7i.2xlarge}"
: "${LOADGEN_INSTANCE_TYPE:=c7i.8xlarge}"
: "${PUBLISHER_INSTANCE_TYPE:=c7i.4xlarge}"
: "${CONDUCTOR_INSTANCE_TYPE:=c7i.2xlarge}"
: "${PGDRIVER_INSTANCE_TYPE:=c7i.4xlarge}"
# Postgres runs in Docker on an EC2 instance with an EBS data volume (the
# account's permission set denies RDS). The RDS_* names from the RDS version
# of these scripts still work; PG_* are the same settings under their new
# names and win when both are set.
_alias() { # <PG name> <RDS name> <default>: sets and exports both
  local pg=$1 rds=$2 def=${3:-} v
  if [ -n "${!pg:-}" ]; then v=${!pg}; elif [ -n "${!rds:-}" ]; then v=${!rds}; else v=$def; fi
  printf -v "$pg" '%s' "$v"
  printf -v "$rds" '%s' "$v"
  export "${pg?}" "${rds?}"
}
_alias PG_STORAGE RDS_STORAGE io2
_alias PG_STORAGE_GB RDS_STORAGE_GB 1000
_alias PG_IOPS RDS_IOPS ""
_alias PG_PASSWORD RDS_PASSWORD ""
: "${PG_INSTANCE_TYPE:=r7i.4xlarge}" # 16 vCPU, 128 GB, x86_64, like the db.r7g.4xlarge the plan named
: "${PG_THROUGHPUT_MBPS:=}"          # gp3 only; default 500 (see 20-postgres.sh)
if [ -n "${RDS_ENGINE_VERSION:-}" ] && [ -z "${PG_IMAGE:-}" ]; then PG_IMAGE="postgres:${RDS_ENGINE_VERSION%%.*}"; fi
: "${PG_IMAGE:=postgres:17}"
: "${PG_DATA_DEVICE:=/dev/sdf}"
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

# BASE_IMAGE_REGISTRY: where the third party images above are pulled from.
# The default, docker.io, leaves every name as written (Docker Hub, and quay.io
# for the postgres exporter). Any other value is a mirror with a flat layout
# (<registry>/<name>:<tag>, no owner in the path): "build-push.sh mirror" makes
# one under IMAGE_REGISTRY. Docker Hub limits anonymous pulls per source address.
: "${BASE_IMAGE_REGISTRY:=docker.io}"
BASE_IMAGE_REGISTRY=${BASE_IMAGE_REGISTRY%/}
# base_image <ref>: the ref to pull, under BASE_IMAGE_REGISTRY. Idempotent.
base_image() {
  if [ "$BASE_IMAGE_REGISTRY" = docker.io ]; then
    printf '%s' "$1"
  else
    printf '%s/%s' "$BASE_IMAGE_REGISTRY" "${1##*/}"
  fi
}
# The refs as written, for "build-push.sh mirror" (which must read Docker Hub),
# then every *_IMAGE variable rewritten so every use site pulls from the mirror.
BASE_IMAGE_VARS=(PG_IMAGE PGBENCH_IMAGE NATS_IMAGE NATS_EXPORTER_IMAGE NODE_EXPORTER_IMAGE PROMETHEUS_IMAGE GRAFANA_IMAGE POSTGRES_EXPORTER_IMAGE)
BASE_IMAGE_SOURCES=()
for _v in "${BASE_IMAGE_VARS[@]}"; do
  BASE_IMAGE_SOURCES+=("${!_v}")
  printf -v "$_v" '%s' "$(base_image "${!_v}")"
done
unset _v

is_dry() { [ "$DRY_RUN" = 1 ]; }

# Containers use host networking, so the ports of one box must differ.
_check_ports() { # <box> <port>...
  local box=$1 seen=" " p
  shift
  for p in "$@"; do
    case "$seen" in *" $p "*) echo "port clash on $box boxes: $p is used twice (check the *_PORT variables)" >&2; exit 1 ;; esac
    seen+="$p "
  done
}

if is_dry; then
  ACTIVE_STATE="$DRY_STATE_FILE"
  : "${AWS_ACCOUNT_ID:=000000000000}"
  : "${AWS_REGION:=dry-region-1}"
  : "${ADMIN_CIDR:=203.0.113.7/32}"
  : "${SSH_PUBLIC_KEY_PATH:=$HOME/.ssh/bench-dryrun.pub}"
  _alias PG_PASSWORD RDS_PASSWORD dryrunpassword0123456789
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
export ACTIVE_STATE
_check_ports node "$SERVER_PORT" "$SERVER_DEBUG_PORT" "$NODE_EXPORTER_PORT"
_check_ports nats "$NATS_CLIENT_PORT" "$NATS_ROUTE_PORT" "$NATS_MONITOR_PORT" "$NATS_EXPORTER_PORT" "$NODE_EXPORTER_PORT"
_check_ports loadgen "$LOADGEN_AGENT_PORT" "$LOADGEN_METRICS_PORT" "$NODE_EXPORTER_PORT"

# Ably internal: mint credentials from ablyctl when none are set. Skipped in a
# dry run and when NO_AWS=1 (local image builds).
if [ -f "$BENCH_AWS_DIR/ably-internal.sh" ]; then
  # shellcheck source=ably-internal.sh
  source "$BENCH_AWS_DIR/ably-internal.sh"
  if ! is_dry && [ "${NO_AWS:-0}" != 1 ]; then ensure_credentials; fi
fi

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
  if [ -n "${GHCR_PULL_TOKEN:-}" ]; then s=${s//"$GHCR_PULL_TOKEN"/***}; fi
  printf '%s' "$s"
}

# ------------------------------------------------------- image registry
# Where the ably-server and ably-loadgen images live. The boxes pull from here.
#   IMAGE_REGISTRY       ghcr.io/<github-owner> or
#                        <account>.dkr.ecr.<region>.amazonaws.com[/<prefix>]
#   IMAGE_REGISTRY_KIND  ghcr | ecr | none. Default ghcr (inferred from IMAGE_REGISTRY when that
#                        is set). none: no registry at all; enough for run 0a, which runs only
#                        third party images.
# ghcr with no IMAGE_REGISTRY takes ghcr.io/<your GitHub login> (gh api user) when an image
# reference is first needed (require_registry); run 0a never needs one. No owner is written
# in a script. ECR with no IMAGE_REGISTRY uses <account>.dkr.ecr.<region>.amazonaws.com/$PROJECT_TAG
# (the repositories the earlier version of these scripts created).
: "${IMAGE_REGISTRY:=}"
IMAGE_REGISTRY=${IMAGE_REGISTRY%/}
if [ -z "${IMAGE_REGISTRY_KIND:-}" ]; then
  case "$IMAGE_REGISTRY" in
    '' | ghcr.io | ghcr.io/*) IMAGE_REGISTRY_KIND=ghcr ;;
    *.dkr.ecr.*.amazonaws.com | *.dkr.ecr.*.amazonaws.com/*) IMAGE_REGISTRY_KIND=ecr ;;
    *) die "cannot tell the registry kind of IMAGE_REGISTRY: set IMAGE_REGISTRY_KIND to ghcr, ecr or none" ;;
  esac
fi
case "$IMAGE_REGISTRY_KIND" in
  ghcr)
    if [ -n "$IMAGE_REGISTRY" ]; then
      case "$IMAGE_REGISTRY" in
        ghcr.io/?*) ;;
        *) die "IMAGE_REGISTRY_KIND=ghcr needs IMAGE_REGISTRY=ghcr.io/<github-owner>" ;;
      esac
      case "$IMAGE_REGISTRY" in
        *[A-Z]*) die "IMAGE_REGISTRY=$IMAGE_REGISTRY: ghcr.io wants lower case names; use ${IMAGE_REGISTRY,,}" ;;
      esac
    fi
    ;;
  ecr)
    if [ -z "$IMAGE_REGISTRY" ]; then
      [ -n "${AWS_ACCOUNT_ID:-}" ] && [ -n "${AWS_REGION:-}" ] ||
        die "IMAGE_REGISTRY_KIND=ecr needs IMAGE_REGISTRY, or AWS_ACCOUNT_ID and AWS_REGION to derive it"
      IMAGE_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${PROJECT_TAG}"
    fi
    ;;
  none) ;;
  *) die "IMAGE_REGISTRY_KIND must be ghcr, ecr or none (got $IMAGE_REGISTRY_KIND)" ;;
esac
REGISTRY_HOST=${IMAGE_REGISTRY%%/*}
if [ "$IMAGE_REGISTRY_KIND" = ghcr ]; then REGISTRY_HOST=ghcr.io; fi
REGISTRY_PATH=""
case "$IMAGE_REGISTRY" in */*) REGISTRY_PATH=${IMAGE_REGISTRY#*/} ;; esac
: "${ECR_REPO_SERVER:=${REGISTRY_PATH:+$REGISTRY_PATH/}ably-server}"
: "${ECR_REPO_LOADGEN:=${REGISTRY_PATH:+$REGISTRY_PATH/}ably-loadgen}"
: "${GHCR_PULL_USER:=${GHCR_USER:-token}}"
export IMAGE_REGISTRY IMAGE_REGISTRY_KIND REGISTRY_HOST REGISTRY_PATH
# The instance profile (an IAM role) exists so boxes can pull from ECR and use SSM. It is optional
# everywhere: only ecr wants one, ghcr and none skip the IAM calls unless CREATE_INSTANCE_PROFILE=1
# (SSM Session Manager), and a role that may not create one gets a warning and boxes without.
if [ -z "${CREATE_INSTANCE_PROFILE:-}" ]; then
  if [ "$IMAGE_REGISTRY_KIND" = ecr ]; then CREATE_INSTANCE_PROFILE=1; else CREATE_INSTANCE_PROFILE=0; fi
fi

# require_registry: the fleet scripts need images somewhere the boxes can pull from. Call it in
# the main shell (not in $( )) so that a registry taken from the GitHub login is kept.
require_registry() {
  [ "$IMAGE_REGISTRY_KIND" != none ] ||
    die "IMAGE_REGISTRY_KIND is none: there is no registry to pull ably-server or ably-loadgen from. Set IMAGE_REGISTRY (ghcr.io/<github-owner> or an ECR registry), unset IMAGE_REGISTRY_KIND and run build-push.sh. Run 0a does not need images."
  if [ "$IMAGE_REGISTRY_KIND" = ghcr ] && [ -z "$IMAGE_REGISTRY" ]; then
    local owner=${GHCR_USER:-}
    if [ -z "$owner" ] && ! is_dry && command -v gh >/dev/null 2>&1; then owner=$(gh api user --jq .login 2>/dev/null || true); fi
    if is_dry; then owner=${owner:-github-login}; fi
    [ -n "$owner" ] ||
      die "IMAGE_REGISTRY is not set and the GitHub login could not be read (gh api user). Set IMAGE_REGISTRY=ghcr.io/<github-owner> (lower case), or IMAGE_REGISTRY_KIND=none for run 0a."
    IMAGE_REGISTRY="ghcr.io/${owner,,}"
    REGISTRY_PATH=${owner,,}
    export IMAGE_REGISTRY REGISTRY_PATH
    log "IMAGE_REGISTRY is not set: using $IMAGE_REGISTRY (your GitHub login)"
  fi
}

# image_ref <ably-server|ably-loadgen> <tag>: the full reference the boxes pull.
image_ref() {
  local name=$1 tag=$2 repo
  require_registry
  case "$IMAGE_REGISTRY_KIND" in
    ecr)
      case "$name" in
        ably-server) repo=$ECR_REPO_SERVER ;;
        ably-loadgen) repo=$ECR_REPO_LOADGEN ;;
        *) die "image_ref: unknown image $name" ;;
      esac
      printf '%s/%s:%s' "$REGISTRY_HOST" "$repo" "$tag"
      ;;
    *) printf '%s/%s:%s' "$IMAGE_REGISTRY" "$name" "$tag" ;;
  esac
}

# registry_repo_path <ably-server|ably-loadgen>: the path below the host (owner/name on ghcr,
# the repository name on ECR).
registry_repo_path() {
  local ref
  ref=$(image_ref "$1" x)
  ref=${ref#*/}
  printf '%s' "${ref%:x}"
}

# ghcr_is_public <owner/name> <tag>: 0 when a client with no credentials can read the
# manifest, which is what a box without GHCR_PULL_TOKEN is. curl only; nothing is changed.
ghcr_is_public() {
  local path=$1 tag=$2 tok code
  tok=$(curl -fsS --max-time 20 "https://ghcr.io/token?scope=repository:${path}:pull" 2>/dev/null | jq -r '.token // empty' 2>/dev/null) || return 1
  [ -n "$tok" ] || return 1
  code=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $tok" \
    -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.docker.distribution.manifest.list.v2+json' \
    "https://ghcr.io/v2/${path}/manifests/${tag}" 2>/dev/null) || return 1
  [ "$code" = 200 ]
}

# check_image_pullable <ably-server|ably-loadgen> <tag>: stop before launching boxes that
# cannot pull. ghcr without GHCR_PULL_TOKEN needs a public package; SKIP_REGISTRY_CHECK=1
# skips the test (no network from here, or a package you know is readable).
check_image_pullable() {
  local name=$1 tag=$2 path
  if is_dry || [ "${SKIP_REGISTRY_CHECK:-0}" = 1 ]; then return 0; fi
  [ "$IMAGE_REGISTRY_KIND" = ghcr ] || return 0
  if [ -n "${GHCR_PULL_TOKEN:-}" ]; then return 0; fi
  path=$(registry_repo_path "$name")
  ghcr_is_public "$path" "$tag" ||
    die "the boxes cannot pull ghcr.io/$path:$tag anonymously: it is missing or private. After the first push set the package to public (https://github.com/users/${path%%/*}/packages, open the package, Package settings, Change visibility), or export GHCR_PULL_TOKEN (a token with read:packages; RUNBOOK section 3). SKIP_REGISTRY_CHECK=1 skips this test."
}

# ecr_ensure_repo <repository name>: 0 when the repository exists or was created; 3 when
# creating it is denied (the reason is logged); 1 on another failure. Looks first with
# describe-repositories and creates only a missing one. A denied tag on create falls back
# to an untagged repository. There is no probe: ECR answers InvalidParameter before it
# checks permission, so a deliberately invalid create says nothing about the permission.
ecr_ensure_repo() {
  local repo=$1 have rc=0
  have=$(aws_r_soft "" ecr describe-repositories --repository-names "$repo" --query 'repositories[0].repositoryName') || rc=$?
  if [ "$rc" = 3 ]; then log "WARNING: ecr:DescribeRepositories is denied; trying to create $repo"; fi
  if [ "$rc" = 0 ] && [ -n "$have" ]; then
    log "ECR repository $repo exists"
    return 0
  fi
  rc=0
  aws_create_tagged "" "the ECR repository $repo" ecr create-repository --repository-name "$repo" \
    --tags "Key=Project,Value=$PROJECT_TAG" "Key=Name,Value=$repo" -- ecr create-repository --repository-name "$repo" >/dev/null || rc=$?
  case "$rc" in
    0) log "created ECR repository $repo" ;;
    3) log "ecr:CreateRepository is DENIED for $repo in this account: the repository does not exist and this role cannot create it. Ask for the repositories to be created with push rights, or use IMAGE_REGISTRY_KIND=ghcr (or none for run 0a)." ;;
    *) log "ecr create-repository failed for $repo" ;;
  esac
  return "$rc"
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

# The password is embedded in shell and DSN text (user-data, the DSN in STATE),
# so keep it to a safe set. PG_PASSWORD and RDS_PASSWORD are the same setting.
check_password() {
  case "${RDS_PASSWORD:-}" in
    '') die "PG_PASSWORD (or RDS_PASSWORD) is not set" ;;
    change-me*) die "PG_PASSWORD is still the placeholder from env.example" ;;
    *[!A-Za-z0-9_-]*) die "PG_PASSWORD may contain only letters, digits, _ and -" ;;
  esac
  [ "${#RDS_PASSWORD}" -ge 16 ] || die "PG_PASSWORD must be at least 16 characters"
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

# _region_args <args...>: prints "--region <AWS_REGION>" unless the caller
# already passed --region (billing alarms must use one fixed region).
_region_args() {
  local a
  for a in "$@"; do
    if [ "$a" = --region ]; then return 0; fi
  done
  printf '%s\n' --region "$AWS_REGION"
}

# aws_w <fake> args...: a write call.
aws_w() {
  local fake=$1
  shift
  local -a reg
  mapfile -t reg < <(_region_args "$@")
  _ext "$fake" aws "$@" "${reg[@]}"
}

# aws_r <fake> args...: a read call. Text output unless --output is given;
# "None" becomes the empty string. AWS_R_QUIET=1 hides the error text of a
# real call (for "does it exist" probes).
aws_r() {
  local fake=$1
  shift
  local a have_out=0
  local -a extra
  mapfile -t extra < <(_region_args "$@")
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

# aws_probe <allowed-regex> <args...>: a harmless call used to learn whether
# an action is permitted. Prints "ALLOWED <basis>", "DENIED" or "UNKNOWN". A call
# that succeeds, or fails with a message matching <allowed-regex>, counts as ALLOWED.
# The basis says how firm that is: "dryrun" (EC2 --dry-run answered DryRunOperation,
# which is given only after the authorisation check: sound), "success", or
# "validation" (a deliberately invalid parameter was rejected: NOT sound, because a
# service may validate parameters before it checks permission; ECR does, and a role
# denied ecr:CreateRepository got "ok" from such a probe). In a dry run it prints the
# call and reports "ALLOWED dryrun".
aws_probe() {
  local ok=$1 out rc=0
  shift
  local -a reg
  mapfile -t reg < <(_region_args "$@")
  if is_dry; then
    _ext "" aws "$@" "${reg[@]}"
    echo "ALLOWED dryrun"
    return 0
  fi
  out=$(aws "$@" "${reg[@]}" 2>&1) || rc=$?
  if [ "$rc" = 0 ]; then
    echo "ALLOWED success"
  elif printf '%s' "$out" | grep -Eqi 'UnauthorizedOperation|AccessDenied|not authorized|AuthorizationError|explicit deny|is not permitted'; then
    echo DENIED
  elif printf '%s' "$out" | grep -Eq "$ok"; then
    if printf '%s' "$out" | grep -q 'DryRunOperation'; then echo "ALLOWED dryrun"; else echo "ALLOWED validation"; fi
  else
    printf '%s\n' "$out" | head -3 >&2
    echo UNKNOWN
  fi
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
  local -a reg
  mapfile -t reg < <(_region_args "$@")
  if ! out=$(aws "$@" "${reg[@]}" 2>&1); then
    if printf '%s' "$out" | grep -q -- "$pattern"; then
      log "already in place ($pattern): aws $1 $2"
      return 0
    fi
    printf '%s\n' "$out" >&2
    return 1
  fi
  if [ -n "$out" ]; then printf '%s\n' "$out"; fi
}

# aws_create_tagged <fake> <what> <tagged aws args...> -- <untagged aws args...>: create a
# resource with tags, and without them when the role may not tag on create (a missing
# tagging permission, such as ecr:TagResource, iam:TagRole or sns:TagResource, must not
# stop a create that is otherwise allowed). Output of the successful call goes to stdout.
# Returns 0 created, 3 denied even without tags (the create itself is not allowed),
# 1 on another failure.
aws_create_tagged() {
  local fake=$1 what=$2 rc=0
  shift 2
  local -a targs=() pargs=()
  while [ "$#" -gt 0 ] && [ "$1" != -- ]; do
    targs+=("$1")
    shift
  done
  if [ "${1:-}" = -- ]; then shift; fi
  pargs=("$@")
  aws_w_soft "$fake" "${targs[@]}" || rc=$?
  if [ "$rc" = 3 ]; then
    log "WARNING: creating $what with tags was denied (a tagging permission is missing); creating it without tags"
    rc=0
    aws_w_soft "$fake" "${pargs[@]}" || rc=$?
  fi
  return "$rc"
}

# aws_w_soft <fake> args...: a write the caller can live without. Returns 0 on
# success, 3 when the role is not authorised (a warning is logged by the
# caller), 1 on any other failure (the error text is printed). An error that
# matches AWS_SOFT_OK (a regex, for example NoSuchEntity) counts as success.
aws_w_soft() {
  local fake=$1 out
  shift
  if is_dry; then
    aws_w "$fake" "$@"
    return 0
  fi
  local -a reg
  mapfile -t reg < <(_region_args "$@")
  if out=$(aws "$@" "${reg[@]}" 2>&1); then
    if [ -n "$out" ]; then printf '%s\n' "$out"; fi
    return 0
  fi
  if printf '%s' "$out" | grep -Eqi 'UnauthorizedOperation|AccessDenied|not authorized|explicit deny|is not permitted'; then
    return 3
  fi
  if [ -n "${AWS_SOFT_OK:-}" ] && printf '%s' "$out" | grep -Eq "$AWS_SOFT_OK"; then
    log "already in place ($AWS_SOFT_OK): aws $1 $2"
    return 0
  fi
  printf '%s\n' "$out" >&2
  return 1
}

# aws_r_soft <fake> args...: a read whose denial is not fatal. Prints the
# text output; returns 0 (ok, output may be empty), 3 (not authorised), 4 (the error
# matches AWS_R_NOTFOUND, a regex such as NoSuchEntity: the thing is not there) or 1.
aws_r_soft() {
  local fake=$1 out
  shift
  if is_dry; then
    aws_r "$fake" "$@"
    return 0
  fi
  local -a extra
  mapfile -t extra < <(_region_args "$@")
  local a have_out=0
  for a in "$@"; do if [ "$a" = --output ]; then have_out=1; fi; done
  if [ "$have_out" = 0 ]; then extra+=(--output text); fi
  if out=$(aws "$@" "${extra[@]}" 2>&1); then
    if [ "$out" = None ]; then out=""; fi
    printf '%s\n' "$out"
    return 0
  fi
  if printf '%s' "$out" | grep -Eqi 'UnauthorizedOperation|AccessDenied|not authorized|explicit deny|is not permitted'; then
    return 3
  fi
  if [ -n "${AWS_R_NOTFOUND:-}" ] && printf '%s' "$out" | grep -Eq "$AWS_R_NOTFOUND"; then
    return 4
  fi
  return 1
}

# ----------------------------------------------------------------- tags

tag_spec() { # <resource-type> <name> [role]
  local rt=$1 name=$2 role=${3:-}
  local tags="{Key=Project,Value=$PROJECT_TAG},{Key=Name,Value=$name}"
  if [ -n "$role" ]; then tags+=",{Key=Role,Value=$role}"; fi
  printf 'ResourceType=%s,Tags=[%s]' "$rt" "$tags"
}

# iam_tags <name>: fills the IAM_TAGS array (IAM wants Key=,Value= pairs).
# shellcheck disable=SC2034
iam_tags() { IAM_TAGS=("Key=Project,Value=$PROJECT_TAG" "Key=Name,Value=$1"); }

project_filter() { printf 'Name=tag:Project,Values=%s' "$PROJECT_TAG"; }

# project_inventory: fills INVENTORY_LINES with one "<type> <id>" line for everything the
# project may have left in the account (call it in the main shell, not in $( )).
# First attempt: the tagging API (lines "arn <arn>"; it lists only tagged resources).
# When that is denied (tag:GetResources is not in the Operator role), or TAGGING_API=off,
# it asks each service: instances, volumes, security groups, network interfaces (by tag, and
# by the project security group) and placement groups by tag; the IAM role and instance
# profile by exact name (get-role, get-instance-profile: never list-instance-profiles, which
# the scoped IAM grant lacks); CloudWatch billing alarms, SNS topics and ECR repositories by
# the project's name prefix (they are created untagged when the role may not tag). A service
# whose call is denied is named in INVENTORY_UNVERIFIED (and logged) instead of failing.
# Returns 1 when a call fails for any other reason. INVENTORY_SOURCE is tagging-api or per-service.
# shellcheck disable=SC2034  # INVENTORY_SOURCE and INVENTORY_UNVERIFIED are read by the callers
INVENTORY_SOURCE=""
INVENTORY_LINES=()
INVENTORY_UNVERIFIED=()

# _inv_add <type> <aws args...>: one list call; adds "<type> <id>" per word to INVENTORY_LINES.
_inv_add() {
  local type=$1 o rc=0 id l dup
  shift
  o=$(aws_r_soft "" "$@") || rc=$?
  if [ "$rc" = 3 ]; then
    log "WARNING: cannot list $type (denied); not verified"
    INVENTORY_UNVERIFIED+=("$type")
    return 0
  elif [ "$rc" != 0 ]; then
    return 1
  fi
  for id in $o; do
    dup=0
    for l in "${INVENTORY_LINES[@]:-}"; do
      if [ "$l" = "$type $id" ]; then dup=1; fi
    done
    if [ "$dup" = 0 ]; then INVENTORY_LINES+=("$type $id"); fi
  done
}

# _inv_get <type> <aws args...>: a get-by-name call; "not found" (NoSuchEntity) is an empty result.
_inv_get() {
  local type=$1 o rc=0
  shift
  o=$(AWS_R_NOTFOUND='NoSuchEntity' aws_r_soft "" "$@") || rc=$?
  case "$rc" in
    0) if [ -n "$o" ]; then INVENTORY_LINES+=("$type $o"); fi ;;
    4) ;;
    3)
      log "WARNING: cannot look up $type (denied); not verified"
      INVENTORY_UNVERIFIED+=("$type")
      ;;
    *) return 1 ;;
  esac
  return 0
}

# shellcheck disable=SC2034
project_inventory() {
  local out rc=0 id live="pending,running,stopping,stopped,shutting-down"
  INVENTORY_SOURCE=""
  INVENTORY_LINES=()
  INVENTORY_UNVERIFIED=()
  if [ "${TAGGING_API:-on}" != off ]; then
    out=$(aws_r_soft "" resourcegroupstaggingapi get-resources --tag-filters "Key=Project,Values=$PROJECT_TAG" \
      --query 'ResourceTagMappingList[].ResourceARN') || rc=$?
    if [ "$rc" = 0 ]; then
      INVENTORY_SOURCE=tagging-api
      for id in $out; do INVENTORY_LINES+=("arn $id"); done
      return 0
    elif [ "$rc" = 3 ]; then
      log "tag:GetResources is denied; listing the project's resources service by service"
    else
      return 1
    fi
  fi
  INVENTORY_SOURCE=per-service
  _inv_add instance ec2 describe-instances --filters "$(project_filter)" "Name=instance-state-name,Values=$live" \
    --query 'Reservations[].Instances[].InstanceId' || return 1
  _inv_add volume ec2 describe-volumes --filters "$(project_filter)" --query 'Volumes[].VolumeId' || return 1
  _inv_add security-group ec2 describe-security-groups --filters "$(project_filter)" --query 'SecurityGroups[].GroupId' || return 1
  _inv_add network-interface ec2 describe-network-interfaces --filters "$(project_filter)" --query 'NetworkInterfaces[].NetworkInterfaceId' || return 1
  local sgs=""
  for id in "${INVENTORY_LINES[@]}"; do
    case "$id" in "security-group "*) sgs+="${sgs:+,}${id#security-group }" ;; esac
  done
  if [ -n "$sgs" ]; then
    # an interface that still uses the project security group blocks its deletion, tagged or not
    _inv_add network-interface ec2 describe-network-interfaces --filters "Name=group-id,Values=$sgs" \
      --query 'NetworkInterfaces[].NetworkInterfaceId' || return 1
  fi
  _inv_add placement-group ec2 describe-placement-groups --filters "$(project_filter)" --query 'PlacementGroups[].GroupName' || return 1
  # IAM by exact name: the role the scripts create is ${PROJECT_TAG}-instance, and its profile has the
  # same name. get-role and get-instance-profile are in the scoped IAM grant; list-instance-profiles is not.
  _inv_get iam-role iam get-role --role-name "${PROJECT_TAG}-instance" --query Role.RoleName || return 1
  _inv_get iam-instance-profile iam get-instance-profile --instance-profile-name "${PROJECT_TAG}-instance" \
    --query InstanceProfile.InstanceProfileName || return 1
  _inv_add cloudwatch-alarm cloudwatch describe-alarms --alarm-name-prefix "${PROJECT_TAG}-" \
    --query 'MetricAlarms[].AlarmName' --region "$BILLING_REGION" || return 1
  _inv_add sns-topic sns list-topics --query "Topics[?contains(TopicArn, ':${PROJECT_TAG}-')].TopicArn" --region "$BILLING_REGION" || return 1
  if [ "$IMAGE_REGISTRY_KIND" = ecr ]; then
    _inv_add ecr-repository ecr describe-repositories \
      --query "repositories[?starts_with(repositoryName, '${REGISTRY_PATH:-ably-}')].repositoryName" || return 1
  fi
  return 0
}

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

# date_ago <days>: a UTC date (YYYY-MM-DD), GNU or BSD date.
date_ago() {
  date -u -d "$1 days ago" +%Y-%m-%d 2>/dev/null || date -u -v-"$1"d +%Y-%m-%d
}

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
  for f in vpc_id subnet_id sg_id ami_id; do
    [ -n "$(state_get ".network.$f")" ] || die "STATE has no network.$f; run 10-network.sh first"
  done
}

# ------------------------------------------------------------- instances

# resolve_ami: the latest Amazon Linux 2023 x86_64 AMI, pinned in STATE on
# first use. The public SSM parameter first; when ssm:GetParameter is denied,
# the newest matching image from ec2 describe-images.
resolve_ami() {
  local ami
  ami=$(state_get '.network.ami_id')
  if [ -z "$ami" ]; then
    ami=$(AWS_R_QUIET=1 aws_r ami-dryrun ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 --query Parameter.Value) || ami=""
    if [ -z "$ami" ]; then
      log "SSM parameter not readable; resolving the AMI with ec2 describe-images"
      ami=$(aws_r "" ec2 describe-images --owners amazon \
        --filters 'Name=name,Values=al2023-ami-2023.*-kernel-*-x86_64' Name=state,Values=available Name=architecture,Values=x86_64 \
        --query 'sort_by(Images,&CreationDate)[-1].ImageId')
    fi
    [ -n "$ami" ] || die "could not resolve an Amazon Linux 2023 AMI (ssm get-parameter and ec2 describe-images both gave nothing)"
    state_set '.network.ami_id' "$ami"
  fi
  printf '%s' "$ami"
}

# ssh_pubkey: the first public key line in SSH_PUBLIC_KEY_PATH. There is no EC2
# key pair: every box appends this key to ec2-user's authorized_keys in user-data.
ssh_pubkey() {
  local f=${SSH_PUBLIC_KEY_PATH:-} key=""
  if [ -n "$f" ] && [ -r "$f" ]; then
    key=$(grep -m1 -E '^(ssh-(ed25519|rsa)|ecdsa-sha2-nistp[0-9]+|sk-ssh-ed25519@openssh\.com|sk-ecdsa-sha2-nistp256@openssh\.com) [A-Za-z0-9+/=]+' "$f" || true)
  fi
  if [ -z "$key" ]; then
    if is_dry; then
      key='ssh-ed25519 AAAAdryrunkey dry-run'
    else
      die "SSH_PUBLIC_KEY_PATH=$f is not a readable OpenSSH public key file (ssh-ed25519, ssh-rsa or ecdsa line); the boxes authorise this key in user-data"
    fi
  fi
  case "$key" in *"'"* | *@@*) die "the public key line contains a quote or @@; use a plain OpenSSH public key" ;; esac
  printf '%s' "$key"
}

find_instance() { # <name> -> instance id (non-terminated) or ""
  aws_r "" ec2 describe-instances \
    --filters "$(project_filter)" "Name=tag:Name,Values=$1" Name=instance-state-name,Values=pending,running,stopping,stopped \
    --query 'Reservations[].Instances[].InstanceId'
}

# data_volume_mapping <size-gb> <io2|gp3> [iops] [throughput-mbps]: the
# --block-device-mappings entry for an extra EBS volume. It lives in the
# RunInstances call with DeleteOnTermination, because the role may create
# volumes only that way and may not delete standalone ones.
data_volume_mapping() {
  local size=$1 vt=$2 iops=${3:-} tp=${4:-} ebs
  ebs="VolumeSize=$size,VolumeType=$vt"
  if [ -n "$iops" ]; then ebs+=",Iops=$iops"; fi
  if [ -n "$tp" ] && [ "$vt" = gp3 ]; then ebs+=",Throughput=$tp"; fi
  printf 'DeviceName=%s,Ebs={%s,DeleteOnTermination=true}' "$PG_DATA_DEVICE" "$ebs"
}

# launch_instance <name> <role> <type> <userdata-file> [private-ip] [placement 1|0]
# Prints the instance id. Reuses an instance with the same Name tag.
# DATA_VOLUME_SPEC="<size-gb> <io2|gp3> [iops] [throughput]" adds an EBS data
# volume. A box that shuts itself down is terminated (the role may not start
# a stopped instance, and a stopped one would keep billing for its disks).
launch_instance() {
  local name=$1 role=$2 type=$3 ud=$4 pip=${5:-} place=${6:-1}
  local existing id sg subnet ami profile pgroup
  existing=$(find_instance "$name")
  if [ -n "$existing" ]; then
    log "reuse $name ($existing)"
    if [ -z "$(inst_field "$name" id)" ]; then state_put_instance "$name" "$existing" "$role" "$type" "" ""; fi
    printf '%s' "$existing"
    return 0
  fi
  sg=$(state_get '.network.sg_id')
  subnet=$(state_get '.network.subnet_id')
  ami=$(state_get '.network.ami_id')
  profile=$(state_get '.network.instance_profile')
  pgroup=$(state_get '.network.placement_group')
  local -a bdm=('DeviceName=/dev/xvda,Ebs={VolumeSize=60,VolumeType=gp3,DeleteOnTermination=true}')
  if [ -n "${DATA_VOLUME_SPEC:-}" ]; then
    local -a dv
    read -r -a dv <<<"$DATA_VOLUME_SPEC"
    bdm+=("$(data_volume_mapping "${dv[0]}" "${dv[1]}" "${dv[2]:-}" "${dv[3]:-}")")
  fi
  local -a args=(ec2 run-instances
    --instance-initiated-shutdown-behavior terminate
    --image-id "$ami" --instance-type "$type"
    --security-group-ids "$sg" --subnet-id "$subnet"
    --user-data "file://$ud"
    --block-device-mappings "${bdm[@]}"
    --metadata-options 'HttpTokens=required,HttpPutResponseHopLimit=2'
    --tag-specifications "$(tag_spec instance "$name" "$role")" "$(tag_spec volume "$name" "$role")"
    --query 'Instances[0].InstanceId' --output text)
  if [ -n "$profile" ]; then args+=(--iam-instance-profile "Name=$profile"); fi
  if [ "$place" = 1 ] && [ -n "$pgroup" ]; then
    args+=(--placement "GroupName=$pgroup,AvailabilityZone=$AZ")
  else
    args+=(--placement "AvailabilityZone=$AZ")
  fi
  if [ -n "$pip" ]; then args+=(--private-ip-address "$pip"); fi
  # A new instance profile can take a minute to become usable: retry a few times.
  local try=0
  until id=$(aws_w "i-dry-$name" "${args[@]}"); do
    try=$((try + 1))
    [ "$try" -lt 4 ] || die "run-instances failed for $name after $try tries"
    log "run-instances failed for $name; retrying in 15s (an instance profile or capacity may still be settling)"
    sleep 15
  done
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
# An instance STATE knows that no longer exists (terminated by the dead-man
# switch, or by hand) is marked not running, so the cost estimate stops
# counting it. Returns 1, changing nothing, when the listing fails.
refresh_instances() {
  local json
  json=$(aws_r '[]' ec2 describe-instances --filters "$(project_filter)" Name=instance-state-name,Values=pending,running,stopping,stopped \
    --query 'Reservations[].Instances[].{id:InstanceId,name:Tags[?Key==`Name`]|[0].Value,role:Tags[?Key==`Role`]|[0].Value,type:InstanceType,private_ip:PrivateIpAddress,public_ip:PublicIpAddress,state:State.Name}' \
    --output json) || {
    log "WARNING: could not list the project's instances; STATE left as it was"
    return 1
  }
  if is_dry; then return 0; fi
  local row n
  while IFS= read -r row; do
    [ -n "$row" ] || continue
    n=$(jq -r .name <<<"$row")
    state_put_instance "$n" "$(jq -r .id <<<"$row")" "$(jq -r '.role // ""' <<<"$row")" "$(jq -r .type <<<"$row")" \
      "$(jq -r '.private_ip // ""' <<<"$row")" "$(jq -r '.public_ip // ""' <<<"$row")"
    _state_update '.instances[$n].running = ($s == "running" or $s == "pending")' --arg n "$n" --arg s "$(jq -r .state <<<"$row")"
  done < <(jq -c '.[]' <<<"$json")
  _state_update '(.instances // {}) |= with_entries(if (.value.id as $i | $live | index($i)) then . else .value.running = false end)
    | if .postgres.instances then (.instances // {}) as $inst | .postgres.instances |= with_entries(.value.running = ($inst[.key].running // false)) else . end' \
    --argjson live "$(jq -c '[.[].id]' <<<"$json")"
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
  [ -n "$ip" ] || die "no public address for $name in STATE (the box may be gone: re-run the script that creates it)"
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

# wait_ssh <instance-name>: retry until sshd accepts a connection (EC2 reports
# "running" a few tens of seconds before it does).
wait_ssh() {
  local name=$1 ip tries=0
  if is_dry; then
    ssh_do "$name" true
    return 0
  fi
  ip=$(inst_field "$name" public_ip)
  [ -n "$ip" ] || die "no public address for $name in STATE"
  until ssh_do "$name" true >/dev/null 2>&1; do
    tries=$((tries + 1))
    [ "$tries" -lt 36 ] || die "$name ($ip) does not accept SSH after 6 minutes; check ADMIN_CIDR and the security group"
    sleep 10
  done
}

# wait_boot <instance-name>...: wait for SSH, then for cloud-init, then check
# the role script finished (it touches /var/lib/bench-ready only on success).
wait_boot() {
  local n
  if [ "${SKIP_BOOT_WAIT:-0}" = 1 ]; then return 0; fi
  for n in "$@"; do
    wait_ssh "$n"
    ssh_do "$n" 'sudo cloud-init status --wait >/dev/null; test -f /var/lib/bench-ready' ||
      die "$n did not finish booting: ssh in and read /var/log/bench-userdata.log (and 'docker logs')"
  done
}

# render_userdata <outfile> <role-template> KEY=VALUE...
# Output is templates/common.sh plus templates/<role-template>.sh. The common part
# logs in to the image registry: ecr with the instance profile; ghcr only when
# GHCR_PULL_TOKEN is set (public packages need no login; the token then sits in the
# user-data, RUNBOOK section 3); none not at all.
render_userdata() {
  local out=$1 role=$2
  shift 2
  case "${GHCR_PULL_TOKEN:-}" in *[!A-Za-z0-9_]*) die "GHCR_PULL_TOKEN may contain only letters, digits and _ (it is written into user-data)" ;; esac
  case "$GHCR_PULL_USER" in *[!A-Za-z0-9-]*) die "GHCR_PULL_USER may contain only letters, digits and - (it is written into user-data)" ;; esac
  {
    render_template "$BENCH_AWS_DIR/templates/common.sh" \
      "REGION=${AWS_REGION:-}" "REGISTRY_KIND=$IMAGE_REGISTRY_KIND" "REGISTRY_HOST=$REGISTRY_HOST" \
      "GHCR_PULL_USER=$GHCR_PULL_USER" "GHCR_PULL_TOKEN=${GHCR_PULL_TOKEN:-}" \
      "NODE_EXPORTER_IMAGE=$NODE_EXPORTER_IMAGE" "NODE_EXPORTER_PORT=$NODE_EXPORTER_PORT" \
      "COMPOSE_VERSION=$DOCKER_COMPOSE_VERSION" "INSTALL_COMPOSE=${INSTALL_COMPOSE:-0}" \
      "MAX_UPTIME_MIN=$((FLEET_MAX_UPTIME_H * 60))" "SSH_PUBKEY=$(ssh_pubkey)"
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
  ssh_do "$1" 'chmod 600 /tmp/role.sh; sudo bash -euo pipefail /tmp/role.sh; rm -f /tmp/role.sh'
}

# iname <role> <index>: the Name tag of an instance.
iname() { printf '%s-%s-%s' "$PROJECT_TAG" "$1" "$2"; }

# image_tag <key in STATE.images> <override>: a tag from the
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

# render_postgres_conf <listen-address> <max-connections> <ram-mib>: the
# postgresql.conf for the Postgres box, to stdout (templates/postgresql.conf).
render_postgres_conf() {
  render_template "$BENCH_AWS_DIR/templates/postgresql.conf" "LISTEN_ADDRESS=$1" "MAX_CONNECTIONS=$2" \
    "SHARED_BUFFERS_MB=$(($3 / 4))" "EFFECTIVE_CACHE_MB=$(($3 * 3 / 4))"
}

# pg_ready <instance-name> <ip>: pg_isready from inside the container, over SSH.
pg_ready() {
  if is_dry; then
    ssh_do "$1" "docker exec postgres pg_isready -h $2 -p 5432 -U $DB_USER"
    return 0
  fi
  ssh_do "$1" "docker exec postgres pg_isready -h $2 -p 5432 -U $DB_USER" >/dev/null 2>&1
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

# shellcheck disable=SC2034  # outputs are read by the callers
# parse_dsn <dsn>: sets DSN_USER, DSN_PASSWORD, DSN_HOST, DSN_PORT, DSN_DB.
parse_dsn() {
  local rest=${1#*://} auth hostpart
  auth=${rest%%@*}
  hostpart=${rest#*@}
  DSN_USER=${auth%%:*}
  DSN_PASSWORD=${auth#*:}
  DSN_HOST=${hostpart%%[:/]*}
  hostpart=${hostpart#"$DSN_HOST"}
  DSN_PORT=5432
  case "$hostpart" in :*) DSN_PORT=${hostpart#:}; DSN_PORT=${DSN_PORT%%/*} ;; esac
  DSN_DB=${hostpart#*/}
  DSN_DB=${DSN_DB%%\?*}
}

# run_detached <instance> <result-dir> <local-script> <limit-seconds>
# Copies the script to the instance, runs it detached under timeout(1) (a
# dropped SSH session does not stop it), polls for its exit code and sets
# RUN_RC (an exit code, or "unknown" if none arrived in limit + 300 s).
run_detached() {
  local name=$1 rdir=$2 script=$3 limit=$4 deadline
  [ -n "$(inst_field "$name" public_ip)" ] || die "no public address for $name in STATE (the box may be gone: re-run the script that creates it)"
  ssh_do "$name" "mkdir -p $rdir"
  scp_to "$name" "$script" "$rdir/cmd.sh"
  ssh_do "$name" "nohup bash -c 'timeout -k 30 $limit bash $rdir/cmd.sh; echo \$? > $rdir/exit-code' >$rdir/run.log 2>&1 </dev/null &"
  deadline=$(($(date +%s) + limit + 300))
  RUN_RC=""
  while :; do
    RUN_RC=$(ssh_do "$name" "cat $rdir/exit-code 2>/dev/null || true" || true)
    RUN_RC=$(printf '%s' "$RUN_RC" | tr -d '[:space:]')
    if [ -n "$RUN_RC" ] || is_dry; then break; fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      log "WARNING: no exit code $((limit + 300))s after start; collecting what exists"
      RUN_RC=unknown
      break
    fi
    sleep 30
  done
  if is_dry; then RUN_RC=0; fi
}

# ------------------------------------------------------------------ cost

# price_of <key>: USD per hour. PRICE_<KEY> overrides (dots become _).
# These are approximate on-demand list prices (us-east-1; plan section 11
# figures), scaled by PRICE_FACTOR; check the AWS pricing pages before
# relying on them.
price_of() {
  local key=$1 var p
  var="PRICE_$(printf '%s' "$key" | tr '.-' '__')"
  if [ -n "${!var:-}" ]; then
    printf '%s' "${!var}"
    return 0
  fi
  p=$(_list_price "$key") || return 1
  awk -v p="$p" -v f="$PRICE_FACTOR" 'BEGIN { printf "%.4g\n", p*f }'
}
_list_price() {
  local key=$1
  case "$key" in
    c7i.large) echo 0.09 ;;
    c7i.xlarge) echo 0.18 ;;
    c7i.2xlarge) echo 0.36 ;;
    c7i.4xlarge) echo 0.71 ;;
    c7i.8xlarge) echo 1.43 ;;
    c7i.12xlarge) echo 2.14 ;;
    c7i.16xlarge) echo 2.86 ;;
    r7i.2xlarge) echo 0.53 ;;
    r7i.4xlarge) echo 1.06 ;;
    r7i.8xlarge) echo 2.12 ;;
    r7i.12xlarge) echo 3.18 ;;
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

# EBS is a real part of the Postgres bill: io2 bills provisioned IOPS, gp3
# bills IOPS above 3000 and throughput above 125 MB/s. us-east-1 list prices;
# PRICE_FACTOR scales every price (eu-west-1 is typically about 10 percent higher).
: "${IO2_USD_PER_IOPS_MONTH:=0.065}"
: "${IO2_USD_PER_GB_MONTH:=0.125}"
: "${GP3_USD_PER_GB_MONTH:=0.08}"
: "${GP3_USD_PER_IOPS_MONTH:=0.005}"
: "${GP3_USD_PER_MBPS_MONTH:=0.04}"
: "${PRICE_FACTOR:=1}"

# ebs_hourly <io2|gp3> <gb> <iops> [throughput-mbps]: USD per hour of one data volume.
ebs_hourly() {
  awk -v st="$1" -v gb="$2" -v iops="${3:-0}" -v tp="${4:-125}" -v f="$PRICE_FACTOR" \
    -v i_iops="$IO2_USD_PER_IOPS_MONTH" -v i_gb="$IO2_USD_PER_GB_MONTH" -v g_gb="$GP3_USD_PER_GB_MONTH" \
    -v g_iops="$GP3_USD_PER_IOPS_MONTH" -v g_tp="$GP3_USD_PER_MBPS_MONTH" \
    'BEGIN { if (st == "io2") s = gb*i_gb + iops*i_iops;
             else { s = gb*g_gb; if (iops > 3000) s += (iops-3000)*g_iops; if (tp > 125) s += (tp-125)*g_tp }
             printf "%.3f\n", s*f/730 }'
}

# pg_hourly <type> <io2|gp3> <gb> <iops> [throughput]: instance plus data volume.
pg_hourly() {
  awk -v a="$(price_of "$1")" -v b="$(ebs_hourly "$2" "$3" "${4:-0}" "${5:-125}")" 'BEGIN { printf "%.3f\n", a + b }'
}

# fleet_hourly_rate: USD per hour of everything STATE says is running. A
# Postgres instance counts its data volume only while it runs: the volume is
# deleted with the instance (there is no stopped state in this account).
fleet_hourly_rate() {
  local total=0 type st gb iops tp row
  while IFS= read -r type; do
    [ -n "$type" ] || continue
    total=$(awk -v t="$total" -v p="$(price_of "$type")" 'BEGIN{printf "%.4f", t+p}')
  done < <(state_get '.instances // {} | to_entries[] | select(.value.running == true) | .value.type')
  while IFS= read -r row; do
    [ -n "$row" ] || continue
    st=$(jq -r .storage <<<"$row")
    gb=$(jq -r '.storage_gb // 0' <<<"$row")
    iops=$(jq -r '.iops // 0' <<<"$row")
    tp=$(jq -r '.throughput_mbps // 125' <<<"$row")
    total=$(awk -v t="$total" -v p="$(ebs_hourly "$st" "$gb" "$iops" "$tp")" 'BEGIN{printf "%.4f", t+p}')
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

# shellcheck disable=SC2034  # BUDGET_ACCRUED is read by the callers
# budget_guard <limit-seconds>: refuse when the estimated spend plus this
# run, at the current fleet rate, would pass BUDGET_CAP_USD.
budget_guard() {
  local limit_s=$1 rate accrued projected
  refresh_instances || true # boxes the dead-man switch terminated must stop counting
  cost_checkpoint
  rate=$(fleet_hourly_rate)
  accrued=$(state_get '.budget.accrued_usd')
  : "${accrued:=0}"
  projected=$(awk -v a="$accrued" -v r="$rate" -v s="$limit_s" 'BEGIN{printf "%.2f", a + r*s/3600}')
  log "fleet \$$rate/h; spent so far (est.) \$$accrued; with this run up to \$$projected (alarm \$$BUDGET_ALARM_USD, cap \$$BUDGET_CAP_USD)"
  if awk -v p="$projected" -v cap="$BUDGET_CAP_USD" 'BEGIN{exit !(p+0 > cap+0)}'; then
    if [ "${OVERRIDE_BUDGET_GUARD:-0}" = 1 ]; then
      log "WARNING: OVERRIDE_BUDGET_GUARD=1; running past the cap estimate"
    else
      die "this run could take the estimated spend to \$$projected, over the \$$BUDGET_CAP_USD cap. Shorten RUN_TIME_LIMIT, or set OVERRIDE_BUDGET_GUARD=1 after deciding with the budget owner."
    fi
  fi
  BUDGET_ACCRUED=$accrued
}

# require_run_limit: sets RUN_LIMIT_S from RUN_TIME_LIMIT or dies.
require_run_limit() {
  [ -n "${RUN_TIME_LIMIT:-}" ] || die "RUN_TIME_LIMIT is not set. Every run needs a hard time limit (for example RUN_TIME_LIMIT=45m)."
  RUN_LIMIT_S=$(parse_duration "$RUN_TIME_LIMIT") || die "RUN_TIME_LIMIT=$RUN_TIME_LIMIT is not a duration (use 300, 45m or 2h)"
  [ "$RUN_LIMIT_S" -gt 0 ] || die "RUN_TIME_LIMIT must be positive"
}

# require_preflight: the billing alarm is checked by 00-preflight.sh only, so
# every script that creates or starts anything insists it has run.
require_preflight() {
  if is_dry; then return 0; fi
  [ -n "$(state_get '.preflight.ok_at')" ] || die "run 00-preflight.sh first (it checks permissions and the billing alarm)"
  if [ "$(state_get '.preflight.alarm_method')" = none ] && [ "${FORCE_NO_ALARM:-0}" != 1 ]; then
    die "00-preflight.sh found no way to create a billing alarm; refusing to create resources"
  fi
}

# spend_gate: refuse to add to the fleet once the estimate has reached the cap.
spend_gate() {
  local accrued
  cost_checkpoint
  accrued=$(state_get '.budget.accrued_usd')
  : "${accrued:=0}"
  if awk -v a="$accrued" -v cap="$BUDGET_CAP_USD" 'BEGIN{exit !(a+0 >= cap+0)}'; then
    [ "${OVERRIDE_BUDGET_GUARD:-0}" = 1 ] || die "the spend estimate (\$$accrued) has reached the cap (\$$BUDGET_CAP_USD). Decide with the budget owner; OVERRIDE_BUDGET_GUARD=1 continues."
  elif awk -v a="$accrued" -v al="$BUDGET_ALARM_USD" 'BEGIN{exit !(a+0 >= al+0)}'; then
    log "WARNING: the spend estimate (\$$accrued) is past the alarm level (\$$BUDGET_ALARM_USD)"
  fi
}
