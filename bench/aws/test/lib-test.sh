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
rds_tags pg1 db
check "rds tags" 'Key=Project,Value=test-scale Key=Name,Value=pg1 Key=Role,Value=db' "${RDS_TAGS[*]}"

# secret masking
check "mask in _fmt flag value" 'aws rds create-db-instance --master-user-password *** --x y' "$(_fmt aws rds create-db-instance --master-user-password "$RDS_PASSWORD" --x y)"
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
              "gen-1":{"type":"c7i.8xlarge","running":true},"pub-1":{"type":"c7i.4xlarge","running":false}},
 "postgres":{"instances":{"pg":{"class":"db.r7g.4xlarge","storage":"gp3","storage_gb":1000,"iops":0,"running":true},
                          "pg2":{"class":"db.r7g.4xlarge","storage":"io2","storage_gb":1000,"iops":20000,"running":false}}}}
JSON
# 2 x 0.36 + 1.43 = 2.15 ; running rds 1.90 + 1000*0.12/730 = 2.064 ; stopped io2 rds still bills (1000*0.125 + 20000*0.10)/730 = 2.911
check "fleet rate (stopped database still bills storage)" 7.1250 "$(awk -v r="$(fleet_hourly_rate)" 'BEGIN{printf "%.4f", r}')"
check "io2 rds rate" 4.811 "$(IO2_USD_PER_IOPS_MONTH=0.10 IO2_USD_PER_GB_MONTH=0.125 rds_hourly db.r7g.4xlarge io2 1000 20000)"
check "price override" 9 "$(PRICE_c7i_2xlarge=9 price_of c7i.2xlarge)"
check "vcpus" 32 "$(vcpus_of c7i.8xlarge)"
# accrual: pretend the interval opened an hour ago at $2/h
_state_update '.budget = {since_epoch: ($n - 3600), rate_usd_h: 2, accrued_usd: 1}' --argjson n "$(date +%s)"
cost_checkpoint
check "accrual adds one hour" 3.0000 "$(state_get '.budget.accrued_usd' | awk '{printf "%.4f", $1}')"
check "checkpoint sets new rate" 7.1250 "$(state_get '.budget.rate_usd_h' | awk '{printf "%.4f", $1}')"

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

if [ "$fails" -gt 0 ]; then
  echo "$fails check(s) failed" >&2
  exit 1
fi
echo "lib tests passed"
