#!/usr/bin/env bash
# 10-network.sh in real mode against a fake aws: the instance profile is optional. An IAM denial at
# any step is a warning and the fleet launches without a profile; a profile the role may create is
# used; INSTANCE_PROFILE_NAME is looked up by exact name; nothing ever lists instance profiles.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWS_DIR="$(cd "$HERE/.." && pwd)"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin"
printf 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAItestkeytestkeytestkey test\n' >"$tmp/key.pub"
cat >"$tmp/bin/aws" <<'FAKE'
#!/bin/sh
echo "$*" >>"$FAKE_LOG"
nosuch() { echo "An error occurred (NoSuchEntity) when calling the $1 operation: not found" >&2; exit 254; }
deny() { echo "An error occurred (AccessDenied) when calling the $1 operation: User: x is not authorized to perform: iam:$1" >&2; exit 254; }
case "$*" in
  *"ssm get-parameter"*) echo ami-1 ;;
  *"ec2 describe-vpcs"*) echo vpc-1 ;;
  *"ec2 describe-subnets"*) echo "subnet-1 172.31.0.0/20" ;;
  *"ec2 describe-security-groups"*) ;;
  *"ec2 create-security-group"*) echo sg-1 ;;
  *"ec2 authorize-security-group-ingress"*) ;;
  *"iam get-role"*) nosuch GetRole ;;
  *"iam create-role"*) [ "$FAKE_IAM" = create-denied ] && deny CreateRole; echo '{}' ;;
  *"iam attach-role-policy"*) [ "$FAKE_IAM" = attach-denied ] && deny AttachRolePolicy; echo '{}' ;;
  *"iam get-instance-profile"*)
    case "$*" in
      *--instance-profile-name\ existing-profile*) [ "$FAKE_IAM" = profile-missing ] && nosuch GetInstanceProfile; echo existing-profile ;;
      *) nosuch GetInstanceProfile ;;
    esac ;;
  *"iam create-instance-profile"*) [ "$FAKE_IAM" = profile-denied ] && deny CreateInstanceProfile; echo '{}' ;;
  *"iam add-role-to-instance-profile"*) echo '{}' ;;
  *"iam list-instance-profiles"*) echo "LISTED" >&2; exit 99 ;;
  *) echo "fake aws: unexpected call: $*" >&2; exit 97 ;;
esac
FAKE
chmod +x "$tmp/bin/aws"

fails=0
check() { if [ "$2" = "$3" ]; then printf 'ok   %s\n' "$1"; else printf 'FAIL %s\n  want: %s\n  got:  %s\n' "$1" "$2" "$3"; fails=$((fails + 1)); fi; }
run() { # <FAKE_IAM mode> [extra env...]: prints the exit code; state in $tmp/state.json, calls in $tmp/calls.log
  local mode=$1
  shift
  printf '%s\n' '{"preflight":{"ok_at":"x","alarm_method":"cloudwatch"}}' >"$tmp/state.json"
  : >"$tmp/calls.log"
  local rc=0
  env -i PATH="$tmp/bin:$PATH" HOME="$HOME" NO_AWS=1 DRY_RUN=0 STATE_FILE="$tmp/state.json" LOG_FILE="$tmp/LOG.md" PROJECT_TAG=test-scale \
    AWS_REGION=test-region-1 AWS_ACCOUNT_ID=111111111111 ADMIN_CIDR=203.0.113.9/32 SSH_PUBLIC_KEY_PATH="$tmp/key.pub" \
    IMAGE_REGISTRY_KIND=ecr FAKE_LOG="$tmp/calls.log" FAKE_IAM="$mode" "$@" "$AWS_DIR/10-network.sh" >/dev/null 2>"$tmp/err.txt" || rc=$?
  echo "$rc"
}
profile() { jq -r '.network.instance_profile | if . == "" or . == null then "none" else . end' "$tmp/state.json"; }
created() { jq -r '.network.created_profile' "$tmp/state.json"; }

check "ecr: IAM allowed: exit 0" 0 "$(run ok)"
check "ecr: IAM allowed: the profile is used" test-scale-instance "$(profile)"
check "ecr: IAM allowed: teardown will remove it" true "$(created)"

check "ecr: create-role denied: still exit 0" 0 "$(run create-denied)"
check "ecr: create-role denied: no profile" none "$(profile)"
check "ecr: create-role denied: a warning says so" 1 "$(grep -c 'WARNING: launching WITHOUT an instance profile' "$tmp/err.txt")"
check "ecr: create-role denied: nothing for teardown to remove" false "$(created)"
check "ecr: create-role denied: no listing call" 0 "$(grep -c 'list-instance-profiles' "$tmp/calls.log" || true)"

check "ecr: attach-role-policy denied: still exit 0" 0 "$(run attach-denied)"
check "ecr: attach-role-policy denied: no profile, but the role it made is left for teardown" "none true" "$(profile) $(created)"

check "ecr: create-instance-profile denied: still exit 0" 0 "$(run profile-denied)"
check "ecr: create-instance-profile denied: no profile" none "$(profile)"

check "named profile: exit 0" 0 "$(run ok INSTANCE_PROFILE_NAME=existing-profile)"
check "named profile: used, not ours to delete" "existing-profile false" "$(profile) $(created)"
check "named profile: no role is created" 0 "$(grep -c 'iam create-role' "$tmp/calls.log" || true)"
check "named profile: missing one stops with a clear message" 1 "$(run profile-missing INSTANCE_PROFILE_NAME=existing-profile)"
check "named profile: the message names the variable" 1 "$(grep -c 'INSTANCE_PROFILE_NAME=existing-profile does not exist' "$tmp/err.txt")"

check "ghcr: no IAM call at all" 0 "$(run ok IMAGE_REGISTRY_KIND=ghcr IMAGE_REGISTRY=ghcr.io/o)"
check "ghcr: no IAM call at all (calls)" 0 "$(grep -c 'aws iam\|^iam ' "$tmp/calls.log" || true)"
check "ghcr: no profile" none "$(profile)"

[ "$fails" = 0 ] || exit 1
echo "network IAM tests passed"
