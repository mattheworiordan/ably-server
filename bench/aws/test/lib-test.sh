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
_state_update '.budget = {since_epoch: ($n - 3600), rate_usd_h: 2, accrued_usd: 1}' --argjson n "$(date +%s)"
cost_checkpoint
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

if [ "$fails" -gt 0 ]; then
  echo "$fails check(s) failed" >&2
  exit 1
fi
echo "lib tests passed"
