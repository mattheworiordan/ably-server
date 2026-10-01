#!/usr/bin/env bash
# Unit tests for the pure helpers in lib.sh: no network, no aws.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
export DRY_RUN=1 DRY_STATE_FILE="$tmp/state.json" PROJECT_TAG=test-scale AWS_REGION=test-region-1
export RDS_PASSWORD=Secretpassword0123456789
# shellcheck source=lib.sh
source "$HERE/../lib.sh"

fails=0
check() { # <description> <expected> <actual>
  if [ "$2" = "$3" ]; then
    printf 'ok   %s\n' "$1"
  else
    printf 'FAIL %s\n  want: %s\n  got:  %s\n' "$1" "$2" "$3"
    fails=$((fails + 1))
  fi
}

# parse_duration
check "duration seconds" 300 "$(parse_duration 300)"
check "duration minutes" 2700 "$(parse_duration 45m)"
check "duration hours" 7200 "$(parse_duration 2h)"
check "duration rejects junk" bad "$(parse_duration 5x || echo bad)"
check "duration rejects empty" bad "$(parse_duration '' || echo bad)"

# addresses
check "cidr host ip" 172.31.16.10 "$(cidr_host_ip 172.31.16.0/20 10)"
check "cidr host ip carry" 10.0.1.4 "$(int_to_ip $(($(ip_to_int 10.0.0.250) + 10)))"
check "cidr offset out of range" bad "$( (cidr_host_ip 172.31.16.0/28 15 2>/dev/null) || echo bad)"

# tag_spec
check "tag spec" 'ResourceType=instance,Tags=[{Key=Project,Value=test-scale},{Key=Name,Value=n1},{Key=Role,Value=node}]' "$(tag_spec instance n1 node)"
check "tag spec without role" 'ResourceType=volume,Tags=[{Key=Project,Value=test-scale},{Key=Name,Value=n1}]' "$(tag_spec volume n1)"

# secret masking
check "mask in _fmt flag value" 'aws x create --password *** --x y' "$(_fmt aws x create --password "$RDS_PASSWORD" --x y)"
check "mask in free text" 'dsn=postgres://u:***@h/db' "$(_mask "dsn=postgres://u:$RDS_PASSWORD@h/db")"
check "space args are quoted" "aws x 'a b'" "$(_fmt aws x 'a b')"

# host-network port clashes
check "port clash is fatal" bad "$( (bash -c 'LOADGEN_AGENT_PORT=9100 source "$0"' "$HERE/../lib.sh" 2>/dev/null) || echo bad)"

# password rules
check "password ok" ok "$(check_password && echo ok)"
check "password too short" bad "$( (RDS_PASSWORD=short check_password 2>/dev/null) || echo bad)"
check "password placeholder rejected" bad "$( (RDS_PASSWORD=change-me-16-chars-min check_password 2>/dev/null) || echo bad)"
check "password bad char" bad "$( (RDS_PASSWORD='abcdefghijklmnop/qrstuvwxyz' check_password 2>/dev/null) || echo bad)"

# templates
printf 'a=@@ONE@@ b=@@TWO@@\n' >"$tmp/t1"
check "template render" 'a=1 b=x&y/z' "$(render_template "$tmp/t1" ONE=1 'TWO=x&y/z')"
check "template unreplaced marker dies" bad "$( (render_template "$tmp/t1" ONE=1 2>/dev/null) || echo bad)"

# state
state_init
state_set '.network.vpc_id' vpc-1
check "state set/get" vpc-1 "$(state_get '.network.vpc_id')"
state_set_json '.budget.cap_usd' 1500
check "state set json" 1500 "$(state_get '.budget.cap_usd')"
state_add_resource instance i-1 node n1
state_add_resource instance i-1 node n1
check "add resource twice keeps one" 1 "$(state_get '.resources | length')"
state_put_instance n1 i-1 node c7i.2xlarge 10.0.0.5 1.2.3.4
check "instance private ip" 10.0.0.5 "$(inst_field n1 private_ip)"
check "instance listed once" 1 "$(state_get '.resources | length')"
state_del_resource i-1
check "del resource" 0 "$(state_get '.resources | length')"
check "missing key is empty" "" "$(state_get '.nope.nothing')"
check "api key stable" "$(ensure_api_key)" "$(ensure_api_key)"

# cost
cat >"$DRY_STATE_FILE" <<'JSON'
{"resources":[],"images":{},"runs":[],
 "instances":{"node-1":{"type":"c7i.2xlarge","running":true},"node-2":{"type":"c7i.2xlarge","running":true},
              "gen-1":{"type":"c7i.8xlarge","running":true},"pub-1":{"type":"c7i.4xlarge","running":false},
              "pg":{"type":"r7i.4xlarge","role":"postgres","running":true},"pg2":{"type":"r7i.4xlarge","role":"postgres","running":false}},
 "postgres":{"instances":{"pg":{"type":"r7i.4xlarge","storage":"gp3","storage_gb":1000,"iops":0,"running":true},
                          "pg2":{"type":"r7i.4xlarge","storage":"io2","storage_gb":1000,"iops":20000,"running":false}}}}
JSON
# 2 x 0.36 + 1.43 = 2.15 ; running pg box 1.06 + its gp3 volume at the EBS baseline 1000*0.08/730 = 0.110 ;
# the terminated io2 box and its (deleted) volume cost nothing
check "fleet rate (a terminated database costs nothing)" 3.3200 "$(awk -v r="$(fleet_hourly_rate)" 'BEGIN{printf "%.4f", r}')"
check "io2 volume rate" 1.952 "$(ebs_hourly io2 1000 20000)"
check "gp3 volume rate with RDS-equivalent IOPS and throughput" 0.192 "$(ebs_hourly gp3 1000 12000 500)"
check "gp3 volume at the EBS baseline" 0.110 "$(ebs_hourly gp3 1000 3000 125)"
check "pg instance plus volume" 3.012 "$(pg_hourly r7i.4xlarge io2 1000 20000)"
check "price factor scales list prices" 1.166 "$(PRICE_FACTOR=1.1 price_of r7i.4xlarge)"
check "price override" 9 "$(PRICE_c7i_2xlarge=9 price_of c7i.2xlarge)"
check "vcpus" 32 "$(vcpus_of c7i.8xlarge)"
# accrual: pretend the interval opened an hour ago at $2/h
# A fixed clock (the date stub answers only +%s), so the test cannot straddle a second boundary.
fixed_now=$(date +%s)
_state_update '.budget = {since_epoch: ($n - 3600), rate_usd_h: 2, accrued_usd: 1}' --argjson n "$fixed_now"
date() { if [ "${1:-}" = +%s ]; then echo "$fixed_now"; else command date "$@"; fi; }
cost_checkpoint
unset -f date
check "accrual adds one hour" 3.0000 "$(state_get '.budget.accrued_usd' | awk '{printf "%.4f", $1}')"
check "checkpoint sets new rate" 3.3200 "$(state_get '.budget.rate_usd_h' | awk '{printf "%.4f", $1}')"

# ablyctl credential helper (fake ablyctl on PATH; never the real one)
mkdir "$tmp/fakebin"
cat >"$tmp/fakebin/ablyctl" <<'FAKE'
#!/bin/sh
[ "$FAKE_ABLYCTL_FAIL" = 1 ] && exit 3
echo "export AWS_ACCESS_KEY_ID=AKIAFAKE_$(echo "$*" | tr " " "_")"
echo "export AWS_SECRET_ACCESS_KEY=fake"
echo "export AWS_SESSION_TOKEN=fake"
FAKE
chmod +x "$tmp/fakebin/ablyctl"
minted=$( (
  unset AWS_ACCESS_KEY_ID AWS_PROFILE
  unset AWS_SSO_ROLE ABLYCTL_ACCOUNT
  PATH="$tmp/fakebin:$PATH" FAKE_ABLYCTL_FAIL=0 ensure_credentials
  echo "$AWS_ACCESS_KEY_ID"
) )
check "ablyctl default account, default role" "AKIAFAKE_aws_env_--account_dev" "$minted"
minted=$( (
  unset AWS_ACCESS_KEY_ID AWS_PROFILE
  PATH="$tmp/fakebin:$PATH" ABLYCTL_ACCOUNT=acct AWS_SSO_ROLE=Other ensure_credentials
  echo "$AWS_ACCESS_KEY_ID"
) )
check "ablyctl account and role overridable" "AKIAFAKE_aws_env_--account_acct_--aws-role_Other" "$minted"
check "existing credentials are kept" keep "$( (export AWS_ACCESS_KEY_ID=keep; PATH="$tmp/fakebin:$PATH" ensure_credentials; echo "$AWS_ACCESS_KEY_ID") )"
msg=$( (unset AWS_ACCESS_KEY_ID AWS_PROFILE; PATH="$tmp/fakebin:$PATH" FAKE_ABLYCTL_FAIL=1 ensure_credentials 2>&1 >/dev/null) || true)
case "$msg" in *AgentOperator* ) case "$msg" in *"infrastructure provisions"*"own terminal"*) echo "ok   failure message names both paths" ;; *) echo "FAIL failure message lacks a path"; fails=$((fails + 1)) ;; esac ;; *) echo "FAIL failure message lacks AgentOperator"; fails=$((fails + 1)) ;; esac
check "ablyctl failure exits non-zero" bad "$( (unset AWS_ACCESS_KEY_ID AWS_PROFILE; PATH="$tmp/fakebin:$PATH" FAKE_ABLYCTL_FAIL=1 ensure_credentials 2>/dev/null) || echo bad)"

# PG_* and RDS_* names are the same settings; PG_* wins
alias_out() { env -i PATH="$PATH" HOME="$HOME" DRY_RUN=1 DRY_STATE_FILE="$tmp/alias-state.json" PROJECT_TAG=test-scale AWS_REGION=test-region-1 "$@" \
  bash -c 'source "$0"; echo "$PG_STORAGE $RDS_STORAGE $PG_STORAGE_GB $PG_IOPS/$RDS_IOPS $PG_INSTANCE_TYPE $PG_IMAGE"' "$HERE/../lib.sh"; }
check "defaults" "io2 io2 1000 / r7i.4xlarge postgres:17" "$(alias_out)"
check "RDS_ names feed PG_ names" "gp3 gp3 500 12000/12000 r7i.4xlarge postgres:17" "$(alias_out RDS_STORAGE=gp3 RDS_STORAGE_GB=500 RDS_IOPS=12000)"
check "PG_ names win and feed RDS_ names" "gp3 gp3 100 / r7i.8xlarge postgres:17" "$(alias_out PG_STORAGE=gp3 RDS_STORAGE=io2 PG_STORAGE_GB=100 PG_INSTANCE_TYPE=r7i.8xlarge)"
check "RDS_ENGINE_VERSION picks the image tag" "io2 io2 1000 / r7i.4xlarge postgres:16" "$(alias_out RDS_ENGINE_VERSION=16.3)"
check "PG_PASSWORD feeds the masking variable" "Pgpassword012345678" "$(env -i PATH="$PATH" HOME="$HOME" DRY_RUN=1 DRY_STATE_FILE="$tmp/alias-state.json" PROJECT_TAG=t AWS_REGION=r PG_PASSWORD=Pgpassword012345678 bash -c 'source "$0"; echo "$RDS_PASSWORD"' "$HERE/../lib.sh")"

# neutral defaults: nothing in lib.sh points at one person's home directory
defaults_out() { env -i PATH="$PATH" HOME="/home/nobody-in-particular" USER="$1" DRY_RUN=1 DRY_STATE_FILE="$tmp/default-state.json" AWS_REGION=test-region-1 "${@:2}" \
  bash -c 'source "$0"; echo "$PROJECT_TAG|${PROJECT_TAG_DEFAULTED:-0}|$STATE_FILE|$LOG_FILE|$RESULTS_DIR"' "$HERE/../lib.sh"; }
want_user=$(id -un | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9\n' '-')
repo_state="$(cd "$HERE/../../.." && pwd)/bench/aws/state"
check "default PROJECT_TAG names the user" "ably-server-scale-$want_user|1|$repo_state/STATE.json|$repo_state/LOG.md|$repo_state/results" "$(defaults_out ignored)"
check "an explicit PROJECT_TAG is kept and not flagged" "mine|0|$repo_state/STATE.json|$repo_state/LOG.md|$repo_state/results" "$(defaults_out ignored PROJECT_TAG=mine)"
check "STATE, LOG and RESULTS stay overridable" "x|1|/s/STATE.json|/l/LOG.md|/r" "$(defaults_out ignored STATE_FILE=/s/STATE.json LOG_FILE=/l/LOG.md RESULTS_DIR=/r | sed "s/^ably-server-scale-[^|]*/x/")"
case "$(defaults_out ignored)" in */home/nobody-in-particular/*) echo "FAIL a default path points into a home directory"; fails=$((fails + 1)) ;; *) echo "ok   no default path is in a home directory" ;; esac
state_dir_mode=$(env -i PATH="$PATH" HOME="$HOME" DRY_RUN=1 DRY_STATE_FILE="$tmp/newdir/sub/state.json" PROJECT_TAG=t AWS_REGION=r bash -c 'source "$0"; state_init; stat -f %Lp "$(dirname "$ACTIVE_STATE")" 2>/dev/null || stat -c %a "$(dirname "$ACTIVE_STATE")"' "$HERE/../lib.sh")
check "the state directory is private" 700 "$state_dir_mode"

# the operator's public key goes into user-data
printf 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAItestkeytestkeytestkey comment\n' >"$tmp/key.pub"
check "ssh public key is read" 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAItestkeytestkeytestkey comment' "$(SSH_PUBLIC_KEY_PATH="$tmp/key.pub" ssh_pubkey)"
printf '# a comment\nnot a key\n' >"$tmp/bad.pub"
check "ssh dry run falls back to a placeholder" 'ssh-ed25519 AAAAdryrunkey dry-run' "$(SSH_PUBLIC_KEY_PATH="$tmp/bad.pub" ssh_pubkey)"
check "ssh real run rejects a non key file" bad "$( (DRY_RUN=0 SSH_PUBLIC_KEY_PATH="$tmp/bad.pub" ssh_pubkey 2>/dev/null) || echo bad)"
printf "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5 x'; rm -rf /\n" >"$tmp/quote.pub"
check "ssh key with a quote is refused" bad "$( (SSH_PUBLIC_KEY_PATH="$tmp/quote.pub" ssh_pubkey 2>/dev/null) || echo bad)"

# block-device mappings for the data volume
check "io2 mapping" 'DeviceName=/dev/sdf,Ebs={VolumeSize=1000,VolumeType=io2,Iops=20000,DeleteOnTermination=true}' "$(data_volume_mapping 1000 io2 20000)"
check "gp3 mapping with throughput" 'DeviceName=/dev/sdf,Ebs={VolumeSize=1000,VolumeType=gp3,Iops=12000,Throughput=500,DeleteOnTermination=true}' "$(data_volume_mapping 1000 gp3 12000 500)"
check "gp3 mapping at the baseline" 'DeviceName=/dev/sdf,Ebs={VolumeSize=200,VolumeType=gp3,DeleteOnTermination=true}' "$(data_volume_mapping 200 gp3)"

# postgresql.conf: the sizing rules and the settings the plan asks for
conf=$(render_postgres_conf PRIVATE_IP_AT_BOOT 1300 131072)
has() { if grep -qxF -- "$2" <<<"$conf"; then printf 'ok   postgresql.conf: %s\n' "$1"; else printf 'FAIL postgresql.conf lacks: %s\n' "$2"; fails=$((fails + 1)); fi; }
has "shared_buffers is 25 percent of RAM" "shared_buffers = 32768MB"
has "effective_cache_size is 75 percent of RAM" "effective_cache_size = 98304MB"
has "max_connections" "max_connections = 1300"
has "synchronous_commit on" "synchronous_commit = on"
has "slow statements at 50 ms" "log_min_duration_statement = 50ms"
has "WAL compression" "wal_compression = on"
has "checkpoint_timeout" "checkpoint_timeout = 15min"
has "max_wal_size" "max_wal_size = 16GB"
has "pg_stat_statements preloaded" "shared_preload_libraries = 'pg_stat_statements'"
has "listens on the private address only" "listen_addresses = 'PRIVATE_IP_AT_BOOT'"
check "no marker is left in the rendered conf" 0 "$(grep -c '@@' <<<"$conf" || true)"

# Real-mode behaviour against a fake aws (never the real one).
mkdir -p "$tmp/fakeaws"
cat >"$tmp/fakeaws/aws" <<'FAKE'
#!/bin/sh
case "$*" in
  *"ssm get-parameter"*) [ "$FAKE_SSM" = deny ] && { echo "An error occurred (AccessDeniedException) when calling the GetParameter operation: not authorized" >&2; exit 254; }; echo ami-from-ssm ;;
  *"ec2 describe-images"*) echo ami-from-describe-images ;;
  *"ec2 describe-instances"*) [ "$FAKE_LIST" = fail ] && { echo "Throttling" >&2; exit 254; }
    echo '[{"id":"i-a","name":"node-1","role":"node","type":"c7i.2xlarge","private_ip":"10.0.0.1","public_ip":"1.1.1.1","state":"running"}]' ;;
  *"iam delete-role"*)
    case "$FAKE_IAM" in
      deny) echo "An error occurred (AccessDenied) when calling the DeleteRole operation: not authorized" >&2; exit 254 ;;
      gone) echo "An error occurred (NoSuchEntity) when calling the DeleteRole operation: gone" >&2; exit 254 ;;
      other) echo "An error occurred (Throttling)" >&2; exit 254 ;;
      *) exit 0 ;;
    esac ;;
  *) echo "fake aws: unexpected call: $*" >&2; exit 97 ;;
esac
FAKE
chmod +x "$tmp/fakeaws/aws"
real() { # <state json> <bash snippet>: sources lib in real mode with the fake aws
  printf '%s\n' "$1" >"$tmp/real-state.json"
  env -i PATH="$tmp/fakeaws:$PATH" HOME="$HOME" NO_AWS=1 DRY_RUN=0 STATE_FILE="$tmp/real-state.json" PROJECT_TAG=test-scale AWS_REGION=test-region-1 \
    FAKE_SSM="${FAKE_SSM:-ok}" FAKE_LIST="${FAKE_LIST:-ok}" FAKE_IAM="${FAKE_IAM:-ok}" bash -c 'source "$0"; '"$2" "$HERE/../lib.sh" 2>/dev/null
}
check "ami from ssm" ami-from-ssm "$(real '{}' 'resolve_ami')"
check "ami falls back to describe-images when ssm is denied" ami-from-describe-images "$(FAKE_SSM=deny real '{}' 'resolve_ami')"
check "ami is pinned in state" ami-from-ssm "$(real '{"network":{"ami_id":"ami-from-ssm"}}' 'FAKE_SSM=deny; resolve_ami')"
two='{"instances":{"node-1":{"id":"i-a","running":true,"type":"c7i.2xlarge"},"pg":{"id":"i-b","running":true,"type":"r7i.4xlarge","role":"postgres"}},"postgres":{"instances":{"pg":{"running":true}}}}'
running_flags='jq -r "[.instances[\"node-1\"].running, .instances.pg.running, .postgres.instances.pg.running] | map(tostring) | join(\" \")" "$STATE_FILE"'
check "refresh marks a vanished instance and its database not running" "true false false" "$(real "$two" "refresh_instances; $running_flags")"
check "refresh keeps state when the listing fails" "true true true" "$(FAKE_LIST=fail real "$two" "refresh_instances || true; $running_flags")"

soft() { real '{}' "rc=0; $1 >/dev/null || rc=\$?; echo \$rc"; }
check "soft write: success" 0 "$(FAKE_IAM=ok soft 'aws_w_soft "" iam delete-role --role-name r')"
check "soft write: denied is 3" 3 "$(FAKE_IAM=deny soft 'aws_w_soft "" iam delete-role --role-name r')"
check "soft write: other failure is 1" 1 "$(FAKE_IAM=other soft 'aws_w_soft "" iam delete-role --role-name r')"
check "soft write: other failure is 1 even when an unrelated pattern is ok" 1 "$(FAKE_IAM=other soft 'AWS_SOFT_OK=NoSuchEntity aws_w_soft "" iam delete-role --role-name r')"
check "soft write: a tolerated error is success" 0 "$(FAKE_IAM=gone soft 'AWS_SOFT_OK=NoSuchEntity aws_w_soft "" iam delete-role --role-name r')"
check "soft write: denied beats a tolerated pattern" 3 "$(FAKE_IAM=deny soft 'AWS_SOFT_OK=NoSuchEntity aws_w_soft "" iam delete-role --role-name r')"

# ---- image registry kinds, base images, the tagging-API fallback ----------------------------
reg() { # <KEY=VALUE...> -- <bash snippet>: lib in a clean environment (dry run), then the snippet
  local -a envs=()
  while [ "$1" != -- ]; do envs+=("$1"); shift; done
  shift
  env -i PATH="$PATH" HOME="$HOME" DRY_RUN=1 DRY_STATE_FILE="$tmp/reg-state.json" PROJECT_TAG=test-scale AWS_REGION=test-region-1 AWS_ACCOUNT_ID=111111111111 \
    "${envs[@]}" bash -c 'source "$0"; '"$1" "$HERE/../lib.sh" 2>/dev/null
}
check "no registry configured means kind ghcr" "ghcr" "$(reg -- 'echo $IMAGE_REGISTRY_KIND')"
check "kind none is explicit" "none" "$(reg IMAGE_REGISTRY_KIND=none -- 'echo $IMAGE_REGISTRY_KIND')"
check "ghcr with no IMAGE_REGISTRY: the owner is resolved when an image is needed (dry run placeholder)" "ghcr.io/github-login" "$(reg -- 'require_registry; echo $IMAGE_REGISTRY')"
check "ghcr owner from GHCR_USER, lower cased" "ghcr.io/some-user/ably-server:t" "$(reg GHCR_USER=Some-User -- 'require_registry; image_ref ably-server t')"
check "an explicit IMAGE_REGISTRY is never replaced" "ghcr.io/o" "$(reg IMAGE_REGISTRY=ghcr.io/o GHCR_USER=other -- 'require_registry; echo $IMAGE_REGISTRY')"
check "ghcr.io host infers ghcr" "ghcr" "$(reg IMAGE_REGISTRY=ghcr.io/some-owner -- 'echo $IMAGE_REGISTRY_KIND')"
check "an ECR host infers ecr" "ecr" "$(reg IMAGE_REGISTRY=111111111111.dkr.ecr.test-region-1.amazonaws.com/pfx -- 'echo $IMAGE_REGISTRY_KIND')"
check "an unknown host needs an explicit kind" bad "$(reg IMAGE_REGISTRY=registry.example.invalid/x -- 'echo $IMAGE_REGISTRY_KIND' || echo bad)"
check "a bad kind is refused" "" "$(reg IMAGE_REGISTRY_KIND=quay -- 'echo $IMAGE_REGISTRY_KIND')"
check "ghcr needs an owner" "" "$(reg IMAGE_REGISTRY=ghcr.io IMAGE_REGISTRY_KIND=ghcr -- 'echo ok')"
check "ghcr owner must be lower case" "" "$(reg IMAGE_REGISTRY=ghcr.io/SomeOwner -- 'echo ok')"
check "ghcr image ref" "ghcr.io/some-owner/ably-server:abc1234" "$(reg IMAGE_REGISTRY=ghcr.io/some-owner -- 'image_ref ably-server abc1234')"
check "ghcr repo path" "some-owner/ably-loadgen" "$(reg IMAGE_REGISTRY=ghcr.io/some-owner -- 'registry_repo_path ably-loadgen')"
check "trailing slash is dropped" "ghcr.io/some-owner/ably-server:t" "$(reg IMAGE_REGISTRY=ghcr.io/some-owner/ -- 'image_ref ably-server t')"
check "ecr default registry is derived from the account and region" "111111111111.dkr.ecr.test-region-1.amazonaws.com/test-scale/ably-server:t" \
  "$(reg IMAGE_REGISTRY_KIND=ecr -- 'image_ref ably-server t')"
check "ecr repository name follows the prefix" "test-scale/ably-server test-scale/ably-loadgen" \
  "$(reg IMAGE_REGISTRY_KIND=ecr -- 'echo $ECR_REPO_SERVER $ECR_REPO_LOADGEN')"
check "ecr registry with a prefix" "111111111111.dkr.ecr.test-region-1.amazonaws.com/pfx/ably-server:t" \
  "$(reg IMAGE_REGISTRY=111111111111.dkr.ecr.test-region-1.amazonaws.com/pfx -- 'image_ref ably-server t')"
check "ecr kind without account or region and without IMAGE_REGISTRY is refused" "" \
  "$(env -i PATH="$PATH" HOME="$HOME" NO_AWS=1 DRY_RUN=0 STATE_FILE="$tmp/reg2.json" PROJECT_TAG=t AWS_REGION=r IMAGE_REGISTRY_KIND=ecr bash -c 'source "$0"; echo ok' "$HERE/../lib.sh" 2>/dev/null || true)"
check "kind none: image_ref stops and names IMAGE_REGISTRY" bad "$(reg IMAGE_REGISTRY_KIND=none -- 'image_ref ably-server t' || echo bad)"
check "kind none: require_registry stops" bad "$(reg IMAGE_REGISTRY_KIND=none -- 'require_registry' || echo bad)"
check "the instance profile is skipped for ghcr" 0 "$(reg IMAGE_REGISTRY=ghcr.io/o -- 'echo $CREATE_INSTANCE_PROFILE')"
check "the instance profile is skipped by default (ghcr)" 0 "$(reg -- 'echo $CREATE_INSTANCE_PROFILE')"
check "the instance profile is made for ecr" 1 "$(reg IMAGE_REGISTRY_KIND=ecr -- 'echo $CREATE_INSTANCE_PROFILE')"
check "CREATE_INSTANCE_PROFILE=1 forces it for ghcr" 1 "$(reg IMAGE_REGISTRY=ghcr.io/o CREATE_INSTANCE_PROFILE=1 -- 'echo $CREATE_INSTANCE_PROFILE')"

# base images
check "docker.io leaves the image names as written" "postgres:17 nats:2.11 prom/prometheus:v2.55.1 quay.io/prometheuscommunity/postgres-exporter:v0.15.0" \
  "$(reg -- 'echo $PG_IMAGE $NATS_IMAGE $PROMETHEUS_IMAGE $POSTGRES_EXPORTER_IMAGE')"
check "a mirror replaces the registry and drops the owner" "ghcr.io/o/postgres:17 ghcr.io/o/nats:2.11 ghcr.io/o/prometheus:v2.55.1 ghcr.io/o/postgres-exporter:v0.15.0" \
  "$(reg BASE_IMAGE_REGISTRY=ghcr.io/o -- 'echo $PG_IMAGE $NATS_IMAGE $PROMETHEUS_IMAGE $POSTGRES_EXPORTER_IMAGE')"
check "the pgbench image follows the mirror" "ghcr.io/o/postgres:17-alpine" "$(reg BASE_IMAGE_REGISTRY=ghcr.io/o -- 'echo $PGBENCH_IMAGE')"
check "the mirror list keeps the refs as written, once each" "8 postgres:17 prom/prometheus:v2.55.1" \
  "$(reg BASE_IMAGE_REGISTRY=ghcr.io/o -- 'echo ${#BASE_IMAGE_SOURCES[@]} ${BASE_IMAGE_SOURCES[0]} ${BASE_IMAGE_SOURCES[4]:+} ${BASE_IMAGE_SOURCES[5]}' | tr -s ' ')"
check "a custom image variable goes through the mirror too" "ghcr.io/o/postgres:16" "$(reg BASE_IMAGE_REGISTRY=ghcr.io/o PG_IMAGE=docker.io/library/postgres:16 -- 'echo $PG_IMAGE')"
check "base_image is idempotent" "ghcr.io/o/nats:2.11" "$(reg BASE_IMAGE_REGISTRY=ghcr.io/o -- 'base_image "$(base_image nats:2.11)"')"

# fake aws for the fallback tests (logs every call to $FAKE_LOG)
mkdir -p "$tmp/fakeaws2"
cat >"$tmp/fakeaws2/aws" <<'FAKE'
#!/bin/sh
echo "$*" >>"${FAKE_LOG:-/dev/null}"
deny() { echo "An error occurred (AccessDeniedException) when calling the $1 operation: User: x is not authorized to perform: $2 because no identity-based policy allows it" >&2; exit 254; }
case "$*" in
  *resourcegroupstaggingapi*)
    case "$FAKE_TAGAPI" in deny) deny GetResources tag:GetResources ;; fail) echo "Throttling: rate exceeded" >&2; exit 254 ;; esac
    printf 'arn:aws:ec2:r:1:instance/i-1\tarn:aws:ec2:r:1:volume/vol-1\n' ;;
  *"ec2 describe-instances"*) [ "$FAKE_CLEAN" = 1 ] || echo i-aaa ;;
  *"ec2 describe-volumes"*) [ "$FAKE_CLEAN" = 1 ] || echo vol-bbb ;;
  *"ec2 describe-security-groups"*) [ "$FAKE_CLEAN" = 1 ] || echo sg-ccc ;;
  *"ec2 describe-network-interfaces"*) [ "$FAKE_CLEAN" = 1 ] || echo eni-ddd ;;
  *"ec2 describe-placement-groups"*) ;;
  *"iam get-role"*)
    [ "$FAKE_IAM" = deny ] && deny GetRole iam:GetRole
    [ "$FAKE_CLEAN" = 1 ] && { echo "An error occurred (NoSuchEntity) when calling the GetRole operation: not found" >&2; exit 254; }
    echo test-scale-instance ;;
  *"iam get-instance-profile"*)
    [ "$FAKE_IAM" = deny ] && deny GetInstanceProfile iam:GetInstanceProfile
    [ "$FAKE_CLEAN" = 1 ] && { echo "An error occurred (NoSuchEntity) when calling the GetInstanceProfile operation: not found" >&2; exit 254; }
    echo test-scale-instance ;;
  *"iam list-instance-profiles"*) echo "list-instance-profiles must never be called" >&2; exit 99 ;;
  *"iam list-roles"*) echo "list-roles is not needed" >&2; exit 99 ;;
  *"cloudwatch describe-alarms"*) [ "$FAKE_CLEAN" = 1 ] || echo test-scale-billing-750usd ;;
  *"sns list-topics"*) [ "$FAKE_CLEAN" = 1 ] || echo arn:aws:sns:us-east-1:1:test-scale-billing ;;
  *"ecr describe-repositories"*)
    case "$FAKE_ECR" in
      exists) echo test-scale/ably-server ;;
      *) echo "An error occurred (RepositoryNotFoundException) when calling the DescribeRepositories operation: not found" >&2; exit 254 ;;
    esac ;;
  *"ecr create-repository"*)
    case "$FAKE_ECR" in
      create-ok) echo '{}' ;;
      tag-denied) case "$*" in *--tags*) deny CreateRepository ecr:TagResource ;; *) echo '{}' ;; esac ;;
      create-denied) deny CreateRepository ecr:CreateRepository ;;
      *) echo "unexpected create" >&2; exit 97 ;;
    esac ;;
  *"iam create-role"*)
    case "$FAKE_IAM" in
      tag-denied) case "$*" in *--tags*) deny CreateRole iam:TagRole ;; *) echo '{}' ;; esac ;;
      create-denied) deny CreateRole iam:CreateRole ;;
      other) echo "An error occurred (Throttling)" >&2; exit 254 ;;
      *) echo '{}' ;;
    esac ;;
  *) echo "fake aws: unexpected call: $*" >&2; exit 97 ;;
esac
FAKE
chmod +x "$tmp/fakeaws2/aws"
cat >"$tmp/fakeaws2/gh" <<'FAKE'
#!/bin/sh
[ -n "$FAKE_GH_LOGIN" ] || exit 1
echo "$FAKE_GH_LOGIN"
FAKE
chmod +x "$tmp/fakeaws2/gh"
freal() { # <KEY=VALUE...> -- <bash snippet>: real mode (DRY_RUN=0) against the fake aws; calls go to $tmp/fake.log
  local -a envs=()
  while [ "$1" != -- ]; do envs+=("$1"); shift; done
  shift
  : >"$tmp/fake.log"
  env -i PATH="$tmp/fakeaws2:$PATH" HOME="$HOME" NO_AWS=1 DRY_RUN=0 STATE_FILE="$tmp/freal-state.json" PROJECT_TAG=test-scale AWS_REGION=test-region-1 \
    AWS_ACCOUNT_ID=111111111111 FAKE_LOG="$tmp/fake.log" IMAGE_REGISTRY_KIND=none "${envs[@]}" bash -c 'source "$0"; '"$1" "$HERE/../lib.sh" 2>/dev/null
}

check "ghcr owner from gh api user (real mode), lower cased" "ghcr.io/fake-login/ably-loadgen:t" "$(freal IMAGE_REGISTRY_KIND=ghcr FAKE_GH_LOGIN=Fake-Login -- 'require_registry; image_ref ably-loadgen t')"
check "ghcr with no owner and no gh login stops" "" "$(freal IMAGE_REGISTRY_KIND=ghcr FAKE_GH_LOGIN= -- 'require_registry; echo resolved')"

# project_inventory: the tagging API first, each service when it is denied
check "inventory: the tagging API is the first attempt" "tagging-api arn:aws:ec2:r:1:instance/i-1 arn:aws:ec2:r:1:volume/vol-1" \
  "$(freal FAKE_TAGAPI=ok -- 'project_inventory; echo $INVENTORY_SOURCE ${INVENTORY_LINES[@]#arn }' | sed 's/ arn / /g')"
check "inventory: an allowed tagging API means no per-service call" 0 "$(freal FAKE_TAGAPI=ok -- 'project_inventory' >/dev/null; grep -c 'ec2 describe\|iam list\|sns list' "$tmp/fake.log" || true)"
check "inventory: a denied tagging API falls back per service" "per-service 8" \
  "$(freal FAKE_TAGAPI=deny -- 'project_inventory; echo $INVENTORY_SOURCE ${#INVENTORY_LINES[@]}')"
inv=$(freal FAKE_TAGAPI=deny -- 'project_inventory; printf "%s\n" "${INVENTORY_LINES[@]}" | sort -u | tr "\n" ";"')
check "inventory: the fallback lists each kind of resource" "cloudwatch-alarm test-scale-billing-750usd;iam-instance-profile test-scale-instance;iam-role test-scale-instance;instance i-aaa;network-interface eni-ddd;security-group sg-ccc;sns-topic arn:aws:sns:us-east-1:1:test-scale-billing;volume vol-bbb;" "$inv"
check "inventory: interfaces are also found by the project security group" 1 "$(freal FAKE_TAGAPI=deny -- 'project_inventory >/dev/null'; grep -c 'describe-network-interfaces --filters Name=group-id,Values=sg-ccc' "$tmp/fake.log")"
check "inventory: the billing alarms and topics are read in the billing region" 2 "$(freal FAKE_TAGAPI=deny -- 'project_inventory >/dev/null'; grep -c 'us-east-1' "$tmp/fake.log")"
check "inventory: a clean account is an empty list" "per-service 0" "$(freal FAKE_TAGAPI=deny FAKE_CLEAN=1 -- 'project_inventory; echo $INVENTORY_SOURCE ${#INVENTORY_LINES[@]}')"
check "inventory: a denied call is named, not fatal" "iam-role,iam-instance-profile|6" "$(freal FAKE_TAGAPI=deny FAKE_IAM=deny -- 'project_inventory; echo "${INVENTORY_UNVERIFIED[*]}|${#INVENTORY_LINES[@]}"' | sed 's/ /,/g;s/,|/|/')"
check "inventory: IAM is looked up by exact name, never listed" "1 1 0" "$(freal FAKE_TAGAPI=deny -- 'project_inventory >/dev/null'; echo "$(grep -c 'iam get-role --role-name test-scale-instance' "$tmp/fake.log") $(grep -c 'iam get-instance-profile --instance-profile-name test-scale-instance' "$tmp/fake.log") $(grep -c 'iam list-' "$tmp/fake.log" || true)")"
check "inventory: a missing role is an empty result, not an error" "0 0" "$(freal FAKE_TAGAPI=deny FAKE_CLEAN=1 -- 'project_inventory; echo ${#INVENTORY_LINES[@]} $?')"
check "inventory: another failure of the tagging API is an error" bad "$(freal FAKE_TAGAPI=fail -- 'project_inventory || echo bad')"
check "inventory: TAGGING_API=off skips the first attempt" 0 "$(freal TAGGING_API=off -- 'project_inventory >/dev/null'; grep -c resourcegroupstaggingapi "$tmp/fake.log" || true)"
check "inventory: ECR repositories are listed only for the ecr kind" 0 "$(freal TAGGING_API=off -- 'project_inventory >/dev/null'; grep -c 'ecr describe' "$tmp/fake.log" || true)"

# tag-on-create fallbacks
check "create: tags allowed, one call" "0 1" "$(freal -- 'rc=0; aws_create_tagged "" role iam create-role --tags k=v -- iam create-role >/dev/null || rc=$?; echo $rc $(wc -l <"$FAKE_LOG")' | tr -s ' ')"
check "create: a denied tag falls back to an untagged create" "0 2" "$(freal FAKE_IAM=tag-denied -- 'rc=0; aws_create_tagged "" role iam create-role --tags k=v -- iam create-role >/dev/null || rc=$?; echo $rc $(wc -l <"$FAKE_LOG")' | tr -s ' ')"
check "create: denied even untagged is 3" "3" "$(freal FAKE_IAM=create-denied -- 'rc=0; aws_create_tagged "" role iam create-role --tags k=v -- iam create-role >/dev/null || rc=$?; echo $rc')"
check "create: another failure is 1 and is not retried" "1 1" "$(freal FAKE_IAM=other -- 'rc=0; aws_create_tagged "" role iam create-role --tags k=v -- iam create-role >/dev/null || rc=$?; echo $rc $(wc -l <"$FAKE_LOG")' | tr -s ' ')"

# ecr_ensure_repo: look first, create only a missing one, report a denial
check "ecr: an existing repository is left alone" "0 1" "$(freal FAKE_ECR=exists -- 'rc=0; ecr_ensure_repo test-scale/ably-server || rc=$?; echo $rc $(wc -l <"$FAKE_LOG")' | tr -s ' ')"
check "ecr: a missing repository is created" "0 2" "$(freal FAKE_ECR=create-ok -- 'rc=0; ecr_ensure_repo test-scale/ably-server || rc=$?; echo $rc $(wc -l <"$FAKE_LOG")' | tr -s ' ')"
check "ecr: a denied tag creates it untagged" "0 3" "$(freal FAKE_ECR=tag-denied -- 'rc=0; ecr_ensure_repo test-scale/ably-server || rc=$?; echo $rc $(wc -l <"$FAKE_LOG")' | tr -s ' ')"
check "ecr: a denied create is 3" "3" "$(freal FAKE_ECR=create-denied -- 'rc=0; ecr_ensure_repo test-scale/ably-server || rc=$?; echo $rc')"
msg=$(env -i PATH="$tmp/fakeaws2:$PATH" HOME="$HOME" NO_AWS=1 DRY_RUN=0 STATE_FILE="$tmp/freal-state.json" PROJECT_TAG=test-scale AWS_REGION=test-region-1 \
  IMAGE_REGISTRY_KIND=none FAKE_ECR=create-denied bash -c 'source "$0"; ecr_ensure_repo test-scale/ably-server' "$HERE/../lib.sh" 2>&1 || true)
case "$msg" in *"ecr:CreateRepository is DENIED"*"IMAGE_REGISTRY_KIND=ghcr"*) echo "ok   ecr: the denial is reported plainly and names the way out" ;; *) echo "FAIL ecr denial message: $msg"; fails=$((fails + 1)) ;; esac

# probe basis: a dry-run answer is sound, a validation answer is not
mkdir -p "$tmp/fakeprobe"
cat >"$tmp/fakeprobe/aws" <<'FAKE'
#!/bin/sh
case "$FAKE_PROBE" in
  dryrun) echo "An error occurred (DryRunOperation) when calling the RunInstances operation: Request would have succeeded, but DryRun flag is set." >&2; exit 254 ;;
  validation) echo "An error occurred (InvalidParameterException) when calling the CreateRepository operation: bad name" >&2; exit 254 ;;
  denied) echo "An error occurred (AccessDeniedException): User is not authorized to perform: ecr:CreateRepository" >&2; exit 254 ;;
  *) echo "something else" >&2; exit 254 ;;
esac
FAKE
chmod +x "$tmp/fakeprobe/aws"
probe() { env -i PATH="$tmp/fakeprobe:$PATH" HOME="$HOME" NO_AWS=1 DRY_RUN=0 STATE_FILE="$tmp/freal-state.json" PROJECT_TAG=t AWS_REGION=r FAKE_PROBE="$1" \
  bash -c 'source "$0"; aws_probe "DryRunOperation|Invalid" x y' "$HERE/../lib.sh" 2>/dev/null; }
check "probe: DryRunOperation is a sound yes" "ALLOWED dryrun" "$(probe dryrun)"
check "probe: a validation error is a yes marked unproven" "ALLOWED validation" "$(probe validation)"
check "probe: AccessDenied is a no" "DENIED" "$(probe denied)"
check "probe: anything else is unknown" "UNKNOWN" "$(probe other)"

# ghcr public check (a fake curl, never the network)
mkdir -p "$tmp/fakecurl"
cat >"$tmp/fakecurl/curl" <<'FAKE'
#!/bin/sh
case "$*" in
  *ghcr.io/token*) [ "$FAKE_TOKEN" = none ] && exit 22; echo '{"token":"anon"}' ;;
  *-w*) echo "$FAKE_CODE" ;;
esac
FAKE
chmod +x "$tmp/fakecurl/curl"
pub() { env -i PATH="$tmp/fakecurl:$PATH" HOME="$HOME" NO_AWS=1 DRY_RUN=1 DRY_STATE_FILE="$tmp/pub.json" PROJECT_TAG=t AWS_REGION=r FAKE_CODE="$1" FAKE_TOKEN="${2:-ok}" \
  bash -c 'source "$0"; ghcr_is_public o/ably-server t && echo public || echo private' "$HERE/../lib.sh" 2>/dev/null; }
check "ghcr: a readable manifest is public" public "$(pub 200)"
check "ghcr: 401 is private" private "$(pub 401)"
check "ghcr: 404 is private" private "$(pub 404)"
check "ghcr: no anonymous token is private" private "$(pub 200 none)"
pullable() { env -i PATH="$tmp/fakecurl:$PATH" HOME="$HOME" NO_AWS=1 DRY_RUN=0 STATE_FILE="$tmp/pub-state.json" PROJECT_TAG=t AWS_REGION=r IMAGE_REGISTRY=ghcr.io/o FAKE_CODE="$1" "${@:2}" \
  bash -c 'source "$0"; check_image_pullable ably-server t; echo ok' "$HERE/../lib.sh" 2>/dev/null || echo stop; }
check "pull check: public package passes" ok "$(pullable 200)"
check "pull check: private package stops the launch" stop "$(pullable 401)"
check "pull check: a pull token replaces the public requirement" ok "$(pullable 401 GHCR_PULL_TOKEN=ghp_abc123)"
check "pull check: SKIP_REGISTRY_CHECK=1 skips it" ok "$(pullable 401 SKIP_REGISTRY_CHECK=1)"
check "the pull token is masked in logs" "x *** y" "$(reg GHCR_PULL_TOKEN=ghp_secret123 -- '_mask "x ghp_secret123 y"')"

if [ "$fails" -gt 0 ]; then
  echo "$fails check(s) failed" >&2
  exit 1
fi
echo "lib tests passed"
