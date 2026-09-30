#!/usr/bin/env bash
# 90-teardown: delete everything tagged Project=$PROJECT_TAG (and everything
# STATE lists), then verify nothing is left. Run it at the end of every
# working day. The billing alarm and the ECR repositories stay (they cost
# almost nothing and the alarm must outlive the fleet) unless TEARDOWN_ALL=1.
#
#   bench/aws/90-teardown.sh --yes
#
# Optional: KEEP_SNAPSHOT=1 (take a final RDS snapshot), KEEP_KEY_PAIR=1,
#           TEARDOWN_ALL=1 (also delete ECR repositories and the budget).
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
[ "${ASSUME_YES:-0}" = 1 ] && confirmed=1
if [ "$confirmed" = 0 ] && ! is_dry; then
  die "this deletes every resource tagged Project=$PROJECT_TAG in $AWS_REGION. Re-run with --yes."
fi

cost_checkpoint

# words <string>: split tab/space separated text into lines.
words() { tr -s '[:space:]' '\n' <<<"$1" | sed '/^$/d'; }

# ---- 1. EC2 instances
live_instances() {
  aws_r "" ec2 describe-instances --filters "$(project_filter)" Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down \
    --query 'Reservations[].Instances[].InstanceId'
}
ids=$(
  {
    words "$(live_instances)"
    state_get '.instances // {} | to_entries[] | .value.id'
  } | sort -u
)
if [ -n "$ids" ]; then
  # shellcheck disable=SC2086
  aws_w "" ec2 terminate-instances --instance-ids $ids >/dev/null
  # shellcheck disable=SC2086
  aws_w "" ec2 wait instance-terminated --instance-ids $ids
  log "terminated instances: $(tr '\n' ' ' <<<"$ids")"
else
  log "no instances to terminate"
fi

# ---- 2. RDS instances
rds_ids=$(
  {
    words "$(aws_r "" rds describe-db-instances --query "DBInstances[?starts_with(DBInstanceIdentifier, \`${PROJECT_TAG}-\`)].DBInstanceIdentifier")"
    state_get '.postgres.instances // {} | keys[]'
  } | sort -u
)
for id in $rds_ids; do
  have=$(AWS_R_QUIET=1 aws_r "$id" rds describe-db-instances --db-instance-identifier "$id" --query 'DBInstances[0].DBInstanceIdentifier') || have=""
  [ -n "$have" ] || continue
  if [ "${KEEP_SNAPSHOT:-0}" = 1 ]; then
    aws_w "" rds delete-db-instance --db-instance-identifier "$id" --final-db-snapshot-identifier "${id}-final-$(date -u +%Y%m%d%H%M)" >/dev/null
  else
    aws_w "" rds delete-db-instance --db-instance-identifier "$id" --skip-final-snapshot --delete-automated-backups >/dev/null
  fi
  log "deleting RDS instance $id"
done
for id in $rds_ids; do
  aws_w "" rds wait db-instance-deleted --db-instance-identifier "$id"
done

# ---- 3. RDS parameter and subnet groups
for g in $(
  {
    state_resource_ids rds-param-group
    words "$(aws_r "" rds describe-db-parameter-groups --query "DBParameterGroups[?starts_with(DBParameterGroupName, \`${PROJECT_TAG}-\`)].DBParameterGroupName")"
  } | sort -u
); do
  aws_w_tolerate 'DBParameterGroupNotFound' "" rds delete-db-parameter-group --db-parameter-group-name "$g"
done
for g in $(
  {
    state_resource_ids rds-subnet-group
    words "$(aws_r "" rds describe-db-subnet-groups --query "DBSubnetGroups[?starts_with(DBSubnetGroupName, \`${PROJECT_TAG}-\`)].DBSubnetGroupName")"
  } | sort -u
); do
  aws_w_tolerate 'DBSubnetGroupNotFoundFault\|DBSubnetGroupNotFound' "" rds delete-db-subnet-group --db-subnet-group-name "$g"
done

# ---- 4. Network interfaces and volumes that outlived their instances
for eni in $(words "$(aws_r "" ec2 describe-network-interfaces --filters "$(project_filter)" --query 'NetworkInterfaces[].NetworkInterfaceId')"); do
  aws_w_tolerate 'InvalidNetworkInterfaceID.NotFound' "" ec2 delete-network-interface --network-interface-id "$eni"
done
for vol in $(words "$(aws_r "" ec2 describe-volumes --filters "$(project_filter)" Name=status,Values=available --query 'Volumes[].VolumeId')"); do
  aws_w_tolerate 'InvalidVolume.NotFound' "" ec2 delete-volume --volume-id "$vol"
done

# ---- 5. Security group (retries while network interfaces detach), placement group, key pair
sg=$(state_get '.network.sg_id')
if [ -z "$sg" ]; then
  sg=$(aws_r "" ec2 describe-security-groups --filters "Name=group-name,Values=${PROJECT_TAG}-sg" --query 'SecurityGroups[0].GroupId')
fi
if [ -n "$sg" ]; then
  tries=0
  until aws_w_tolerate 'InvalidGroup.NotFound' "" ec2 delete-security-group --group-id "$sg"; do
    tries=$((tries + 1))
    [ "$tries" -lt 12 ] || die "security group $sg still has dependencies after 12 tries"
    log "security group $sg busy; retrying in 20s"
    sleep 20
  done
fi
pgname=$(state_get '.network.placement_group')
: "${pgname:=${PROJECT_TAG}-cluster}"
aws_w_tolerate 'InvalidPlacementGroup.Unknown' "" ec2 delete-placement-group --group-name "$pgname"
if [ "${KEEP_KEY_PAIR:-0}" != 1 ]; then
  aws_w_tolerate 'InvalidKeyPair.NotFound' "" ec2 delete-key-pair --key-name "${PROJECT_TAG}-key"
fi

# ---- 6. IAM role and instance profile, only when 10-network.sh created them
if [ "$(state_get '.network.created_profile')" = true ]; then
  role="${PROJECT_TAG}-instance"
  aws_w_tolerate 'NoSuchEntity' "" iam remove-role-from-instance-profile --instance-profile-name "$role" --role-name "$role"
  aws_w_tolerate 'NoSuchEntity' "" iam delete-instance-profile --instance-profile-name "$role"
  aws_w_tolerate 'NoSuchEntity' "" iam detach-role-policy --role-name "$role" --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
  aws_w_tolerate 'NoSuchEntity' "" iam detach-role-policy --role-name "$role" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
  aws_w_tolerate 'NoSuchEntity' "" iam delete-role --role-name "$role"
fi

# ---- 7. Optional: ECR repositories and the budget
if [ "${TEARDOWN_ALL:-0}" = 1 ]; then
  for repo in "$ECR_REPO_SERVER" "$ECR_REPO_LOADGEN"; do
    aws_w_tolerate 'RepositoryNotFoundException' "" ecr delete-repository --repository-name "$repo" --force >/dev/null
  done
  require_env AWS_ACCOUNT_ID
  aws_w_tolerate 'NotFoundException' "" budgets delete-budget --account-id "$AWS_ACCOUNT_ID" --budget-name "${BUDGET_NAME:-${PROJECT_TAG}-cap}"
fi

# ---- 8. Verify nothing tagged remains
left=0
chk() { # <what> <text>
  if [ -n "$(words "$2")" ]; then
    log "STILL PRESENT ($1): $(words "$2" | tr '\n' ' ')"
    left=1
  fi
}
chk instances "$(live_instances)"
chk rds "$(aws_r "" rds describe-db-instances --query "DBInstances[?starts_with(DBInstanceIdentifier, \`${PROJECT_TAG}-\`)].DBInstanceIdentifier")"
chk volumes "$(aws_r "" ec2 describe-volumes --filters "$(project_filter)" --query 'Volumes[].VolumeId')"
chk interfaces "$(aws_r "" ec2 describe-network-interfaces --filters "$(project_filter)" --query 'NetworkInterfaces[].NetworkInterfaceId')"
chk security-groups "$(aws_r "" ec2 describe-security-groups --filters "$(project_filter)" --query 'SecurityGroups[].GroupId')"
chk placement-groups "$(aws_r "" ec2 describe-placement-groups --filters "$(project_filter)" --query 'PlacementGroups[].GroupName')"
if [ "${KEEP_KEY_PAIR:-0}" != 1 ]; then
  chk key-pairs "$(aws_r "" ec2 describe-key-pairs --filters "$(project_filter)" --query 'KeyPairs[].KeyName')"
fi
# Any other tagged resource (final snapshots with KEEP_SNAPSHOT=1 show here on purpose).
other=$(aws_r "" resourcegroupstaggingapi get-resources --tag-filters "Key=Project,Values=$PROJECT_TAG" --query 'ResourceTagMappingList[].ResourceARN')
if [ -n "$(words "$other")" ]; then
  log "tagged resources the tagging API still lists (terminated instances can linger for an hour): $(words "$other" | tr '\n' ' ')"
fi

# ---- 9. State and log
state_set_json '.resources' '[]'
state_set_json '.instances' '{}'
state_set_json '.postgres.instances' '{}'
state_set_json '.network' '{}'
cost_checkpoint
if [ "$left" = 0 ]; then
  log_line 90-teardown "all tagged resources deleted; nothing tagged remains; accrued estimate \$$(state_get '.budget.accrued_usd')" "start a new fleet with 10-network.sh when needed"
  log "teardown complete"
else
  log_line 90-teardown "TEARDOWN INCOMPLETE: resources remain (see log above); fleet may still cost money" "re-run 90-teardown.sh --yes; check the console"
  die "teardown incomplete"
fi
