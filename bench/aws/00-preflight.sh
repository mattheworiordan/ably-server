#!/usr/bin/env bash
# 00-preflight: check credentials, account, permissions, quotas, ECR repositories
# and the billing alarm before anything is created. Creates the ECR repositories
# and the billing alarm when they are missing. Refuses to continue without an alarm.
#
#   bench/aws/00-preflight.sh
#
# Needs: AWS_ACCOUNT_ID, AWS_REGION, SSH_PUBLIC_KEY_PATH, ALARM_EMAIL (when the
#        alarm must be created).
# Optional: BUDGET_METHOD (cloudwatch|budgets|auto; default cloudwatch), BUDGET_ALARM_USD (750),
#           BUDGET_CAP_USD (1500), BUDGET_SCOPE (account|tag; budgets only),
#           FLEET_PROFILE (2x|1x, sizes the vCPU check), FORCE_NO_ALARM=1 (see RUNBOOK).
SCRIPT_NAME=00-preflight
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq openssl
is_dry || need_cmd aws
require_env AWS_ACCOUNT_ID AWS_REGION
: "${FLEET_PROFILE:=2x}"
: "${BUDGET_SCOPE:=account}"
: "${BUDGET_NAME:=${PROJECT_TAG}-cap}"
state_init

# 1. Credentials and account.
acct=$(aws_r "$AWS_ACCOUNT_ID" sts get-caller-identity --query Account) ||
  die "aws sts get-caller-identity failed: credentials missing or expired. Sign in again (RUNBOOK, 'Sign in')."
[ "$acct" = "$AWS_ACCOUNT_ID" ] ||
  die "credentials belong to a different account than AWS_ACCOUNT_ID; refusing to continue"
log "credentials valid for the expected account"

# 2. Permissions. Each action is probed with the cheapest harmless call: an EC2
# --dry-run, or a call with a deliberately invalid parameter (AWS answers a
# validation error only after the authorisation check has passed, so an
# "Invalid..." reply means the action is allowed and nothing is created).
# A denied action is reported now, not halfway through building the fleet.
perm_denied=()
perm_unknown=()
check_perm() { # <action> <allowed-regex> <aws args...>
  local action=$1 ok=$2 verdict
  shift 2
  verdict=$(aws_probe "$ok" "$@")
  case "$verdict" in
    ALLOWED) log "permission ok:      $action" ;;
    DENIED)
      log "permission DENIED:  $action"
      perm_denied+=("$action")
      ;;
    *)
      log "permission unclear: $action (could not classify the reply)"
      perm_unknown+=("$action")
      ;;
  esac
  PERM_LAST=$verdict
}
ami=$(resolve_ami)
log "AMI $ami (Amazon Linux 2023, x86_64)"
ssh_pubkey >/dev/null # the boxes authorise this key in user-data (no EC2 key pair exists)
check_perm ec2:RunInstances 'DryRunOperation' ec2 run-instances --dry-run --image-id "$ami" --instance-type "$NODE_INSTANCE_TYPE" --count 1
# The Postgres box: an EBS data volume of each type, inside the RunInstances call (the role
# may create volumes only that way, and may not delete a standalone one).
check_perm "ec2:RunInstances with an io2 data volume" 'DryRunOperation' ec2 run-instances --dry-run --image-id "$ami" \
  --instance-type "$PG_INSTANCE_TYPE" --count 1 --placement "AvailabilityZone=$AZ" \
  --block-device-mappings "$(data_volume_mapping "$PG_STORAGE_GB" io2 20000)"
check_perm "ec2:RunInstances with a gp3 data volume" 'DryRunOperation' ec2 run-instances --dry-run --image-id "$ami" \
  --instance-type "$PG_INSTANCE_TYPE" --count 1 --placement "AvailabilityZone=$AZ" \
  --block-device-mappings "$(data_volume_mapping "$PG_STORAGE_GB" gp3 12000 500)"
check_perm ec2:CreateSecurityGroup 'DryRunOperation|InvalidVpc|InvalidGroup' ec2 create-security-group --dry-run --group-name "${PROJECT_TAG}-probe" --description probe
if [ "$USE_PLACEMENT_GROUP" = 1 ]; then
  # Optional: 10-network.sh falls back to no placement group when this is denied.
  check_perm ec2:CreatePlacementGroup 'DryRunOperation' ec2 create-placement-group --dry-run --group-name "${PROJECT_TAG}-probe" --strategy cluster
  if [ "$PERM_LAST" = DENIED ]; then log "  (not required: 10-network.sh will launch without a placement group)"; fi
fi
# A well-formed id for an instance that does not exist: AWS answers NotFound for it, which shows
# the call was well-formed, not that it is authorised (there is no instance to be authorised on).
# 90-teardown.sh is the real test of this permission.
check_perm ec2:TerminateInstances 'DryRunOperation|InvalidInstanceID.NotFound' ec2 terminate-instances --dry-run --instance-ids i-12345678
check_perm ecr:CreateRepository 'InvalidParameter|Invalid' ecr create-repository --repository-name "INVALID NAME"
check_perm cloudwatch:PutMetricAlarm 'InvalidParameter|Validation' cloudwatch put-metric-alarm --alarm-name "${PROJECT_TAG}-probe" --namespace probe --metric-name probe --statistic Maximum --period 7 --evaluation-periods 1 --threshold 1 --comparison-operator GreaterThanThreshold --region "$BILLING_REGION"
perm_cw=$PERM_LAST
check_perm sns:CreateTopic 'InvalidParameter|Invalid|Validation' sns create-topic --name "invalid name!" --region "$BILLING_REGION"
perm_sns=$PERM_LAST
check_perm budgets:CreateBudget 'InvalidParameter|Invalid|Validation' budgets create-budget --account-id "$AWS_ACCOUNT_ID" \
  --budget "BudgetName=${PROJECT_TAG}-probe,BudgetLimit={Amount=-1,Unit=USD},TimeUnit=MONTHLY,BudgetType=COST"
perm_budgets=$PERM_LAST
if [ "${BUDGET_METHOD:-cloudwatch}" != cloudwatch ]; then
  # Only the Budgets path reads the budget back; a role that cannot (ViewBudget) still creates it
  # (see budget_exists below), so a denial here is informational.
  check_perm budgets:ViewBudget '.' budgets describe-budgets --account-id "$AWS_ACCOUNT_ID" --max-results 1
fi
if [ -z "${INSTANCE_PROFILE_NAME:-}" ]; then
  check_perm iam:CreateRole 'InvalidParameter|Invalid|Validation|MalformedPolicy' iam create-role --role-name "invalid name!" --assume-role-policy-document '{}'
  [ "$PERM_LAST" = ALLOWED ] || log "  (set INSTANCE_PROFILE_NAME to an existing instance profile with ECR read access to avoid creating a role)"
fi
# Hard requirements: the fleet (including both Postgres volume types), the security group and images.
# Not required: rds:* (Postgres runs on EC2), key pairs (SSH key goes in user-data), placement groups,
# Stop/Start (boxes are terminated and re-created), DeleteVolume (volumes are deleted with their instance).
hard=()
for a in ec2:RunInstances "ec2:RunInstances with an io2 data volume" "ec2:RunInstances with a gp3 data volume" ec2:CreateSecurityGroup ec2:TerminateInstances ecr:CreateRepository; do
  for d in "${perm_denied[@]:-}"; do
    if [ "$d" = "$a" ]; then hard+=("$a"); fi
  done
done
if [ "${#hard[@]}" -gt 0 ]; then
  die "the current role is denied: ${hard[*]}. Sign in with a role that allows them (RUNBOOK, 'Sign in'); nothing has been created."
fi
# Billing alarm. CloudWatch billing alarms with SNS are the default: the Operator role can create
# them and read them back; Budgets can be created but not viewed there.
alarm_method=none
case "${BUDGET_METHOD:-cloudwatch}" in
  cloudwatch) if [ "$perm_cw" = ALLOWED ] && [ "$perm_sns" = ALLOWED ]; then alarm_method=cloudwatch; fi ;;
  budgets) if [ "$perm_budgets" = ALLOWED ]; then alarm_method=budgets; fi ;;
  auto)
    if [ "$perm_cw" = ALLOWED ] && [ "$perm_sns" = ALLOWED ]; then
      alarm_method=cloudwatch
    elif [ "$perm_budgets" = ALLOWED ]; then
      alarm_method=budgets
    fi
    ;;
  *) die "BUDGET_METHOD must be cloudwatch, budgets or auto (got ${BUDGET_METHOD})" ;;
esac
if [ "$alarm_method" = none ] && [ "${FORCE_NO_ALARM:-0}" != 1 ]; then
  die "BUDGET_METHOD=${BUDGET_METHOD:-cloudwatch} cannot be used with this role (cloudwatch needs cloudwatch:PutMetricAlarm and sns:CreateTopic; budgets needs budgets:CreateBudget). Refusing to continue without a billing alarm (RUNBOOK, 'Billing alarm')."
fi
[ "${#perm_unknown[@]}" -eq 0 ] || log "WARNING: could not classify: ${perm_unknown[*]} (check them by hand if a later step fails)"
state_set '.preflight.alarm_method' "$alarm_method"
log "billing alarm method: $alarm_method (a late backstop: billing data lags by hours. The guards that act are the local estimate, spend_gate and budget_guard, and the FLEET_MAX_UPTIME_H=${FLEET_MAX_UPTIME_H} dead-man switch on every box)"

# 3. vCPU quota for the fleet.
if [ "$FLEET_PROFILE" = 2x ]; then
  nodes=20 loadgens=6 publishers=3
else
  nodes=10 loadgens=3 publishers=2
fi
# Postgres runs on EC2 now, so its instances count against the same quota: the SHARDS
# instances of the active storage type plus one of the other type (run 0a and run 4).
pg_count=$((SHARDS + 1))
need_vcpus=$((nodes * $(vcpus_of "$NODE_INSTANCE_TYPE") +
  NATS_COUNT * $(vcpus_of "$NATS_INSTANCE_TYPE") +
  loadgens * $(vcpus_of "$LOADGEN_INSTANCE_TYPE") +
  publishers * $(vcpus_of "$PUBLISHER_INSTANCE_TYPE") +
  $(vcpus_of "$CONDUCTOR_INSTANCE_TYPE") +
  $(vcpus_of "$PGDRIVER_INSTANCE_TYPE") +
  pg_count * $(vcpus_of "$PG_INSTANCE_TYPE")))
quota=$(AWS_R_QUIET=1 aws_r "$need_vcpus" service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --query Quota.Value) || quota=""""
# vCPUs already in use by other standard-family instances in this account and region.
in_use=$(aws_r "" ec2 describe-instances --filters Name=instance-state-name,Values=pending,running \
  --query 'Reservations[].Instances[].[InstanceType,CpuOptions.CoreCount,CpuOptions.ThreadsPerCore]') || in_use=""
in_use=$(awk '$1 !~ /^(inf|trn|hpc|dl|vt|g|p|f|x|u|mac)/ && NF >= 3 { n += $2 * $3 } END { print n + 0 }' <<<"$in_use")
if [ -z "$quota" ]; then
  log "WARNING: could not read the on-demand vCPU quota (L-1216C47A; servicequotas:* is denied to this role). The $FLEET_PROFILE fleet needs $need_vcpus vCPUs. If it is short, RunInstances fails with VcpuLimitExceeded: then use FLEET_PROFILE=1x or ask for an increase."
elif awk -v q="$quota" -v u="$in_use" -v n="$need_vcpus" 'BEGIN{exit !(q - u >= n)}'; then
  log "vCPU quota $quota, $in_use in use: room for the $FLEET_PROFILE fleet ($need_vcpus vCPUs)"
else
  die "on-demand vCPU quota is $quota with $in_use in use; the $FLEET_PROFILE fleet needs $need_vcpus. Request an increase for quota L-1216C47A, or use FLEET_PROFILE=1x."
fi
state_set_json '.preflight.need_vcpus' "$need_vcpus"

# 4. Instance types offered in the chosen Availability Zone.
for t in "$NODE_INSTANCE_TYPE" "$LOADGEN_INSTANCE_TYPE" "$PUBLISHER_INSTANCE_TYPE" "$PG_INSTANCE_TYPE"; do
  offered=$(aws_r "$t" ec2 describe-instance-type-offerings --location-type availability-zone \
    --filters "Name=location,Values=$AZ" "Name=instance-type,Values=$t" --query 'InstanceTypeOfferings[0].InstanceType') || offered=""
  [ -n "$offered" ] || log "WARNING: $t is not offered in $AZ; set AZ to another zone"
done

# 5. ECR repositories.
for repo in "$ECR_REPO_SERVER" "$ECR_REPO_LOADGEN"; do
  have=$(aws_r "" ecr describe-repositories --repository-names "$repo" --query 'repositories[0].repositoryName') || have=""
  if [ -z "$have" ]; then
    aws_w "" ecr create-repository --repository-name "$repo" \
      --tags "Key=Project,Value=$PROJECT_TAG" "Key=Name,Value=$repo" >/dev/null
    log "created ECR repository $repo"
  else
    log "ECR repository $repo exists"
  fi
done

# 6. Billing alarm: $BUDGET_ALARM_USD warning and $BUDGET_CAP_USD cap, by the method
# chosen above (AWS Budgets, or CloudWatch EstimatedCharges alarms with an SNS topic).
init_work_dir
wd=$BENCH_WORK_DIR
# budget_exists: 0 when it exists, 1 when not, 2 when the role cannot read budgets
# (budgets:ViewBudget is denied to the Operator role).
budget_exists() {
  local b rc=0
  b=$(AWS_R_QUIET=1 aws_r_soft "" budgets describe-budget --account-id "$AWS_ACCOUNT_ID" --budget-name "$BUDGET_NAME" --query Budget.BudgetName) || rc=$?
  if [ "$rc" = 3 ]; then return 2; fi
  [ "$rc" = 0 ] && [ -n "$b" ]
}
cw_alarm_exists() { # <alarm name>
  local a
  a=$(AWS_R_QUIET=1 aws_r "" cloudwatch describe-alarms --alarm-names "$1" --query 'MetricAlarms[0].AlarmName' --region "$BILLING_REGION") || return 1
  [ -n "$a" ]
}
create_budget() {
  local alarm_pct filters
  alarm_pct=$(awk -v a="$BUDGET_ALARM_USD" -v c="$BUDGET_CAP_USD" 'BEGIN{printf "%d", a/c*100}')
  if [ "$BUDGET_SCOPE" = tag ]; then
    filters="{\"TagKeyValue\":[\"user:Project\$${PROJECT_TAG}\"]}"
  else
    filters='{}'
  fi
  jq -n --arg n "$BUDGET_NAME" --arg cap "$BUDGET_CAP_USD" --argjson f "$filters" \
    '{BudgetName:$n,BudgetLimit:{Amount:$cap,Unit:"USD"},TimeUnit:"MONTHLY",BudgetType:"COST",CostFilters:$f}' >"$wd/budget.json"
  jq -n --arg e "$ALARM_EMAIL" --argjson p "$alarm_pct" '
    def n($type; $pct): {Notification:{NotificationType:$type,ComparisonOperator:"GREATER_THAN",Threshold:$pct,ThresholdType:"PERCENTAGE"},
                         Subscribers:[{SubscriptionType:"EMAIL",Address:$e}]};
    [n("ACTUAL";$p), n("ACTUAL";100), n("FORECASTED";100)]' >"$wd/budget-notifications.json"
  aws_w_tolerate DuplicateRecordException "" budgets create-budget --account-id "$AWS_ACCOUNT_ID" \
    --budget "file://$wd/budget.json" --notifications-with-subscribers "file://$wd/budget-notifications.json"
  log "Budgets data lags by hours and forecasts need history: treat the budget as a backstop and rely on cost-estimate.sh and the spend gate day to day"
  if [ "$BUDGET_SCOPE" = tag ]; then log "scope tag: the Project cost allocation tag must be activated in Billing (up to 24 h) or this budget sees no spend"; fi
  log "created budget $BUDGET_NAME: warn at \$$BUDGET_ALARM_USD (${alarm_pct}%), cap \$$BUDGET_CAP_USD, scope $BUDGET_SCOPE"
}
create_cw_alarms() { # billing metrics live in one region only
  local topic a usd
  topic=$(aws_w "arn:aws:sns:$BILLING_REGION:$AWS_ACCOUNT_ID:${PROJECT_TAG}-billing" sns create-topic --name "${PROJECT_TAG}-billing" \
    --tags "Key=Project,Value=$PROJECT_TAG" --query TopicArn --output text --region "$BILLING_REGION")
  aws_w "" sns subscribe --topic-arn "$topic" --protocol email --notification-endpoint "$ALARM_EMAIL" --region "$BILLING_REGION" >/dev/null
  for usd in "$BUDGET_ALARM_USD" "$BUDGET_CAP_USD"; do
    a="${PROJECT_TAG}-billing-${usd}usd"
    aws_w "" cloudwatch put-metric-alarm --alarm-name "$a" --namespace AWS/Billing --metric-name EstimatedCharges \
      --dimensions "Name=Currency,Value=USD" "Name=LinkedAccount,Value=$AWS_ACCOUNT_ID" \
      --statistic Maximum --period 21600 --evaluation-periods 1 --threshold "$usd" \
      --comparison-operator GreaterThanThreshold --alarm-actions "$topic" --region "$BILLING_REGION"
  done
  local pending
  pending=$(AWS_R_QUIET=1 aws_r "" sns list-subscriptions-by-topic --topic-arn "$topic" --region "$BILLING_REGION" \
    --query "Subscriptions[?SubscriptionArn=='PendingConfirmation'] | length(@)") || pending=""
  if [ -n "$pending" ] && [ "$pending" != 0 ]; then
    log "WARNING: the SNS subscription for $ALARM_EMAIL is pending: open the confirmation e-mail, or the alarm will not reach you"
  fi
  log "billing metrics need 'Receive Billing Alerts' enabled on the account; until the first data point the alarms show INSUFFICIENT_DATA"
  log "created CloudWatch billing alarms at \$$BUDGET_ALARM_USD and \$$BUDGET_CAP_USD (whole account, not just this project) -> $topic. Confirm the email subscription."
}
alarm_ok=0
alarm_verified=1
case "$alarm_method" in
  budgets)
    brc=0
    budget_exists || brc=$?
    if [ "$brc" = 0 ]; then
      log "billing alarm $BUDGET_NAME exists"
      alarm_ok=1
    else
      require_env ALARM_EMAIL
      create_budget
      brc=0
      budget_exists || brc=$?
      if is_dry || [ "$brc" = 0 ]; then
        alarm_ok=1
      elif [ "$brc" = 2 ]; then
        # budgets:ViewBudget is denied: the budget cannot be read back. The create call above did not fail
        # (a duplicate counts as success), so the budget exists, but nothing here can confirm it.
        log "WARNING: budgets:ViewBudget is denied, so $BUDGET_NAME cannot be read back. The create call succeeded; confirm the budget in the console. The local spend estimate and FLEET_MAX_UPTIME_H are the guards that act."
        alarm_ok=1
        alarm_verified=0
      fi
    fi
    ;;
  cloudwatch)
    if cw_alarm_exists "${PROJECT_TAG}-billing-${BUDGET_ALARM_USD}usd" && cw_alarm_exists "${PROJECT_TAG}-billing-${BUDGET_CAP_USD}usd"; then
      log "CloudWatch billing alarms exist"
      alarm_ok=1
    else
      require_env ALARM_EMAIL
      create_cw_alarms
      if is_dry || cw_alarm_exists "${PROJECT_TAG}-billing-${BUDGET_CAP_USD}usd"; then alarm_ok=1; fi
    fi
    ;;
esac
if [ "$alarm_ok" != 1 ]; then
  if [ "${FORCE_NO_ALARM:-0}" = 1 ]; then
    log "WARNING: FORCE_NO_ALARM=1; continuing without a verified alarm (you confirmed an alarm exists elsewhere)"
    log_line 00-preflight "no verified billing alarm; FORCE_NO_ALARM=1" "confirm the alarm by hand"
  else
    die "no billing alarm is in place after create; refusing to continue (RUNBOOK, 'Billing alarm')"
  fi
fi
state_set_json '.preflight.alarm_verified' "$([ "$alarm_verified" = 1 ] && echo true || echo false)"
state_set_json '.budget.alarm_usd' "$BUDGET_ALARM_USD"
state_set_json '.budget.cap_usd' "$BUDGET_CAP_USD"
state_set '.budget.name' "$BUDGET_NAME"
ensure_api_key >/dev/null
state_set '.preflight.ok_at' "$(date -u +%FT%TZ)"

log_line 00-preflight "account ok; vcpu quota ok for $FLEET_PROFILE ($need_vcpus); ECR and billing alarm in place" "10-network.sh"
log "preflight complete"
