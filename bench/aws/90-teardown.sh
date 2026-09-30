#!/usr/bin/env bash
# 90-teardown: delete everything tagged Project=$PROJECT_TAG (and what STATE
# lists, when it carries that tag), then verify nothing is left. Run it at the
# end of every working day. The billing alarm and the ECR repositories stay
# (they cost almost nothing and the alarm must outlive the fleet) unless
# TEARDOWN_ALL=1. Terminating the instances also deletes their EBS volumes, so
# the Postgres data goes with its instance: collect results first.
#
#   bench/aws/90-teardown.sh --yes
#
# Every read it relies on must succeed: an expired sign-in or a denied call
# stops the script instead of looking like "nothing remains". STATE is cleared
# only after the verification passes.
#
# Optional: TEARDOWN_ALL=1 (also delete the ECR repositories and the billing alarms).
SCRIPT_NAME=90-teardown
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
require_env AWS_REGION
state_init

confirmed=0
for a in "$@"; do
  case "$a" in -y | --yes) confirmed=1 ;; *) die "unknown argument: $a" ;; esac
done
if [ "${ASSUME_YES:-0}" = 1 ]; then confirmed=1; fi
if [ "$confirmed" = 0 ] && ! is_dry; then
  die "this deletes every resource tagged Project=$PROJECT_TAG in $AWS_REGION. Re-run with --yes."
fi

# The right account, and working credentials, before anything is deleted.
require_env AWS_ACCOUNT_ID
acct=$(aws_r "$AWS_ACCOUNT_ID" sts get-caller-identity --query Account) ||
  die "cannot read the caller identity: credentials missing or expired. Sign in again; nothing was deleted."
[ "$acct" = "$AWS_ACCOUNT_ID" ] || die "credentials belong to a different account than AWS_ACCOUNT_ID; nothing was deleted"

cost_checkpoint

# words <string>: split tab/space separated text into lines.
words() { tr -s '[:space:]' '\n' <<<"$1" | sed '/^$/d'; }

# rd <fake> aws-args...: a read that must succeed.
rd() {
  local fake=$1 out
  shift
  out=$(aws_r "$fake" "$@") || die "aws $1 $2 failed: credentials, permissions or throttling. Nothing more was deleted; fix it and re-run."
  printf '%s' "$out"
}

# has_project_tag <fake> <aws tag-listing args...>: true when the resource carries Project=$PROJECT_TAG.
has_project_tag() {
  local fake=$1 v
  shift
  v=$(AWS_R_QUIET=1 aws_r "$fake" "$@") || return 1
  [ "$v" = "$PROJECT_TAG" ]
}

# ---- 1. EC2 instances (tag filter, plus STATE ids that still exist). Terminating an
# instance deletes its volumes, the Postgres data volume included.
live_states="pending,running,stopping,stopped,shutting-down"
tagged=$(rd "" ec2 describe-instances --filters "$(project_filter)" "Name=instance-state-name,Values=$live_states" \
  --query 'Reservations[].Instances[].InstanceId')
state_ids=$(state_get '.instances // {} | to_entries[] | .value.id' | paste -sd, -)
from_state=""
if [ -n "$state_ids" ]; then
  if is_dry; then
    from_state=${state_ids//,/ }
  else
    from_state=$(rd "" ec2 describe-instances --filters "Name=instance-id,Values=$state_ids" "Name=instance-state-name,Values=$live_states" \
      --query 'Reservations[].Instances[].InstanceId')
  fi
fi
ids=$({
  words "$tagged"
  words "$from_state"
} | sort -u)
if [ -n "$ids" ]; then
  # shellcheck disable=SC2086
  aws_w "" ec2 terminate-instances --instance-ids $ids >/dev/null
  # shellcheck disable=SC2086
  aws_w "" ec2 wait instance-terminated --instance-ids $ids
  log "terminated instances: $(tr '\n' ' ' <<<"$ids")"
else
  log "no instances to terminate"
fi

# ---- 2. Volumes. Every volume is created inside RunInstances with DeleteOnTermination and
# the role cannot delete a standalone volume, so none should outlive its instance. Any that
# does is reported in the final check (delete it by hand or ask for ec2:DeleteVolume).

# ---- 3. Security group (retries while network interfaces detach), placement group (only if one was made)
sg=$(rd "sg-dryrun" ec2 describe-security-groups --filters "$(project_filter)" "Name=group-name,Values=${PROJECT_TAG}-sg" --query 'SecurityGroups[0].GroupId')
if [ -n "$sg" ]; then
  tries=0
  until aws_w_tolerate 'InvalidGroup.NotFound' "" ec2 delete-security-group --group-id "$sg"; do
    tries=$((tries + 1))
    [ "$tries" -lt 12 ] || die "security group $sg still has dependencies after 12 tries"
    log "security group $sg busy; retrying in 20s"
    sleep 20
  done
fi
pgname=${PROJECT_TAG}-cluster
have_pg=$(rd "" ec2 describe-placement-groups --filters "$(project_filter)" "Name=group-name,Values=$pgname" --query 'PlacementGroups[0].GroupName')
if [ -n "$have_pg" ]; then
  aws_w_tolerate 'InvalidPlacementGroup.Unknown' "" ec2 delete-placement-group --group-name "$pgname"
fi

# ---- 4. IAM role and instance profile, only when 10-network.sh created them
if [ "$(state_get '.network.created_profile')" = true ]; then
  role="${PROJECT_TAG}-instance"
  aws_w_tolerate 'NoSuchEntity' "" iam remove-role-from-instance-profile --instance-profile-name "$role" --role-name "$role"
  aws_w_tolerate 'NoSuchEntity' "" iam delete-instance-profile --instance-profile-name "$role"
  aws_w_tolerate 'NoSuchEntity' "" iam detach-role-policy --role-name "$role" --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
  aws_w_tolerate 'NoSuchEntity' "" iam detach-role-policy --role-name "$role" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
  aws_w_tolerate 'NoSuchEntity' "" iam delete-role --role-name "$role"
fi

# ---- 5. Optional: ECR repositories (only ones tagged for the project) and the budget
if [ "${TEARDOWN_ALL:-0}" = 1 ]; then
  for repo in "$ECR_REPO_SERVER" "$ECR_REPO_LOADGEN"; do
    arn=$(AWS_R_QUIET=1 aws_r "arn:dryrun:$repo" ecr describe-repositories --repository-names "$repo" --query 'repositories[0].repositoryArn') || continue
    [ -n "$arn" ] || continue
    if has_project_tag "$PROJECT_TAG" ecr list-tags-for-resource --resource-arn "$arn" --query "tags[?Key=='Project'].Value | [0]"; then
      aws_w_tolerate 'RepositoryNotFoundException' "" ecr delete-repository --repository-name "$repo" --force >/dev/null
    else
      log "WARNING: ECR repository $repo has no Project tag; leaving it alone"
    fi
  done
  aws_w_tolerate 'NotFoundException' "" budgets delete-budget --account-id "$AWS_ACCOUNT_ID" --budget-name "${BUDGET_NAME:-${PROJECT_TAG}-cap}"
  for usd in "$BUDGET_ALARM_USD" "$BUDGET_CAP_USD"; do
    aws_w_tolerate 'ResourceNotFound' "" cloudwatch delete-alarms --alarm-names "${PROJECT_TAG}-billing-${usd}usd" --region "$BILLING_REGION"
  done
fi

# ---- 6. Verify nothing tagged remains (every read must succeed)
left=0
chk() { # <what> <text>
  if [ -n "$(words "$2")" ]; then
    log "STILL PRESENT ($1): $(words "$2" | tr '\n' ' ')"
    left=1
  fi
}
v_inst=$(rd "" ec2 describe-instances --filters "$(project_filter)" "Name=instance-state-name,Values=$live_states" --query 'Reservations[].Instances[].InstanceId')
v_vol=$(rd "" ec2 describe-volumes --filters "$(project_filter)" --query 'Volumes[].VolumeId')
v_sg=$(rd "" ec2 describe-security-groups --filters "$(project_filter)" --query 'SecurityGroups[].GroupId')
v_pg=$(rd "" ec2 describe-placement-groups --filters "$(project_filter)" --query 'PlacementGroups[].GroupName')
chk instances "$v_inst"
chk volumes "$v_vol"
chk security-groups "$v_sg"
chk placement-groups "$v_pg"
# Any other tagged resource (the ECR repositories, for one) shows here on purpose.
other=$(rd "" resourcegroupstaggingapi get-resources --tag-filters "Key=Project,Values=$PROJECT_TAG" --query 'ResourceTagMappingList[].ResourceARN')
if [ -n "$(words "$other")" ]; then
  log "tagged resources the tagging API still lists (terminated instances can linger for an hour; ECR repositories and snapshots stay by design): $(words "$other" | tr '\n' ' ')"
fi

# ---- 7. State and log (STATE is cleared only when the verification passed)
if [ "$left" = 0 ]; then
  state_set_json '.resources' '[]'
  state_set_json '.instances' '{}'
  state_set_json '.postgres.instances' '{}'
  state_set_json '.nats' '{}'
  state_set_json '.deployment' '{}'
  state_set_json '.network' '{}'
  cost_checkpoint
  log_line 90-teardown "all tagged resources deleted; nothing tagged remains; accrued estimate \$$(state_get '.budget.accrued_usd')" "start a new fleet with 10-network.sh when needed"
  log "teardown complete"
else
  log_line 90-teardown "TEARDOWN INCOMPLETE: resources remain (see log above); fleet may still cost money; STATE kept" "re-run 90-teardown.sh --yes; check the console"
  die "teardown incomplete; STATE kept so you can re-run"
fi
