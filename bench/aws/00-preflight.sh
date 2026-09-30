#!/usr/bin/env bash
# 00-preflight: check credentials, account, quotas, ECR repositories and the
# billing alarm before anything is created. Creates the ECR repositories and
# the billing alarm when they are missing. Refuses to continue without an alarm.
#
#   bench/aws/00-preflight.sh
#
# Needs: AWS_ACCOUNT_ID, AWS_REGION, ALARM_EMAIL (when the alarm must be created).
# Optional: BUDGET_ALARM_USD (750), BUDGET_CAP_USD (1500), BUDGET_SCOPE (tag|account),
#           FLEET_PROFILE (2x|1x, sizes the vCPU check), FORCE_NO_ALARM=1 (see RUNBOOK).
SCRIPT_NAME=00-preflight
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq openssl
is_dry || need_cmd aws
require_env AWS_ACCOUNT_ID AWS_REGION
: "${FLEET_PROFILE:=2x}"
: "${BUDGET_SCOPE:=tag}"
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
check_perm ec2:RunInstances 'DryRunOperation' ec2 run-instances --dry-run --image-id "$ami" --instance-type "$NODE_INSTANCE_TYPE" --min-count 1 --max-count 1
check_perm ec2:CreateSecurityGroup 'DryRunOperation|InvalidVpc|InvalidGroup' ec2 create-security-group --dry-run --group-name "${PROJECT_TAG}-probe" --description probe
check_perm ec2:CreatePlacementGroup 'DryRunOperation' ec2 create-placement-group --dry-run --group-name "${PROJECT_TAG}-probe" --strategy cluster
check_perm ec2:ImportKeyPair 'DryRunOperation|InvalidKey|InvalidParameter' ec2 import-key-pair --dry-run --key-name "${PROJECT_TAG}-probe" --public-key-material "ssh-ed25519"
check_perm ec2:TerminateInstances 'DryRunOperation|InvalidInstanceID' ec2 terminate-instances --dry-run --instance-ids i-00000000000000000
check_perm rds:CreateDBInstance 'InvalidParameter|Invalid|Validation' rds create-db-instance --db-instance-identifier "${PROJECT_TAG}-probe" --db-instance-class db.t3.micro --engine preflight-invalid-engine
check_perm ecr:CreateRepository 'InvalidParameter|Invalid' ecr create-repository --repository-name "INVALID NAME"
check_perm cloudwatch:PutMetricAlarm 'InvalidParameter|Validation' cloudwatch put-metric-alarm --alarm-name "${PROJECT_TAG}-probe" --namespace probe --metric-name probe --statistic Maximum --period 1 --evaluation-periods 1 --threshold 1 --comparison-operator GreaterThanThreshold
perm_cw=$PERM_LAST
check_perm sns:CreateTopic 'InvalidParameter|Invalid|Validation' sns create-topic --name "invalid name!"
perm_sns=$PERM_LAST
check_perm budgets:CreateBudget 'InvalidParameter|Invalid|Validation' budgets create-budget --account-id "$AWS_ACCOUNT_ID" \
  --budget "BudgetName=${PROJECT_TAG}-probe,BudgetLimit={Amount=-1,Unit=USD},TimeUnit=MONTHLY,BudgetType=COST"
perm_budgets=$PERM_LAST
check_perm budgets:ViewBudget '.' budgets describe-budgets --account-id "$AWS_ACCOUNT_ID" --max-results 1
check_perm ce:GetCostAndUsage '.' ce get-cost-and-usage --time-period "Start=$(date_ago 2),End=$(date_ago 0)" --granularity DAILY --metrics UnblendedCost
if [ -z "${INSTANCE_PROFILE_NAME:-}" ]; then
  check_perm iam:CreateRole 'InvalidParameter|Invalid|Validation|MalformedPolicy' iam create-role --role-name "invalid name!" --assume-role-policy-document '{}'
  [ "$PERM_LAST" = ALLOWED ] || log "  (set INSTANCE_PROFILE_NAME to an existing instance profile with ECR read access to avoid creating a role)"
fi
# Hard requirements: fleet, database, images. The billing alarm needs Budgets, or CloudWatch plus SNS.
hard=()
for a in ec2:RunInstances ec2:CreateSecurityGroup ec2:CreatePlacementGroup ec2:ImportKeyPair ec2:TerminateInstances rds:CreateDBInstance ecr:CreateRepository; do
  case " ${perm_denied[*]:-} " in *" $a "*) hard+=("$a") ;; esac
done
if [ "${#hard[@]}" -gt 0 ]; then
  die "the current role is denied: ${hard[*]}. Sign in with a role that allows them (RUNBOOK, 'Sign in'); nothing has been created."
fi
alarm_method=none
if [ "$perm_budgets" = ALLOWED ] && [ "${BUDGET_METHOD:-auto}" != cloudwatch ]; then
  alarm_method=budgets
elif [ "$perm_cw" = ALLOWED ] && [ "$perm_sns" = ALLOWED ]; then
  alarm_method=cloudwatch
fi
if [ "$alarm_method" = none ] && [ "${FORCE_NO_ALARM:-0}" != 1 ]; then
  die "the current role can create neither a Budget nor a CloudWatch billing alarm with an SNS topic (budgets:CreateBudget, cloudwatch:PutMetricAlarm, sns:CreateTopic). Refusing to continue without a billing alarm (RUNBOOK, 'Billing alarm')."
fi
[ "${#perm_unknown[@]}" -eq 0 ] || log "WARNING: could not classify: ${perm_unknown[*]} (check them by hand if a later step fails)"
state_set '.preflight.alarm_method' "$alarm_method"
log "billing alarm method: $alarm_method"

# 3. vCPU quota for the fleet.
if [ "$FLEET_PROFILE" = 2x ]; then
  nodes=20 loadgens=6 publishers=3
else
  nodes=10 loadgens=3 publishers=2
fi
need_vcpus=$((nodes * $(vcpus_of "$NODE_INSTANCE_TYPE") +
  NATS_COUNT * $(vcpus_of "$NATS_INSTANCE_TYPE") +
  loadgens * $(vcpus_of "$LOADGEN_INSTANCE_TYPE") +
  publishers * $(vcpus_of "$PUBLISHER_INSTANCE_TYPE") +
  $(vcpus_of "$CONDUCTOR_INSTANCE_TYPE") +
  $(vcpus_of "$PGDRIVER_INSTANCE_TYPE")))
quota=$(aws_r "$need_vcpus" service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --query Quota.Value) || quota=""
if [ -z "$quota" ]; then
  log "WARNING: could not read the on-demand vCPU quota (L-1216C47A); check it by hand. The $FLEET_PROFILE fleet needs $need_vcpus vCPUs."
elif awk -v q="$quota" -v n="$need_vcpus" 'BEGIN{exit !(q+0 >= n)}'; then
  log "vCPU quota $quota covers the $FLEET_PROFILE fleet ($need_vcpus vCPUs)"
else
  die "on-demand vCPU quota is $quota; the $FLEET_PROFILE fleet needs $need_vcpus. Request an increase for quota L-1216C47A."
fi
state_set_json '.preflight.need_vcpus' "$need_vcpus"

# 4. Instance types offered in the chosen Availability Zone.
for t in "$NODE_INSTANCE_TYPE" "$LOADGEN_INSTANCE_TYPE" "$PUBLISHER_INSTANCE_TYPE"; do
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
budget_exists() {
  local b
  b=$(AWS_R_QUIET=1 aws_r "" budgets describe-budget --account-id "$AWS_ACCOUNT_ID" --budget-name "$BUDGET_NAME" --query Budget.BudgetName) || return 1
  [ -n "$b" ]
}
cw_alarm_exists() { # <alarm name>
  local a
  a=$(AWS_R_QUIET=1 aws_r "" cloudwatch describe-alarms --alarm-names "$1" --query 'MetricAlarms[0].AlarmName' --region us-east-1) || return 1
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
  aws_w "" budgets create-budget --account-id "$AWS_ACCOUNT_ID" \
    --budget "file://$wd/budget.json" --notifications-with-subscribers "file://$wd/budget-notifications.json"
  log "created budget $BUDGET_NAME: warn at \$$BUDGET_ALARM_USD (${alarm_pct}%), cap \$$BUDGET_CAP_USD, scope $BUDGET_SCOPE"
}
create_cw_alarms() { # billing metrics live in one region only
  local topic a usd
  topic=$(aws_w "arn:aws:sns:us-east-1:$AWS_ACCOUNT_ID:${PROJECT_TAG}-billing" sns create-topic --name "${PROJECT_TAG}-billing" \
    --tags "Key=Project,Value=$PROJECT_TAG" --query TopicArn --output text --region us-east-1)
  aws_w "" sns subscribe --topic-arn "$topic" --protocol email --notification-endpoint "$ALARM_EMAIL" --region us-east-1 >/dev/null
  for usd in "$BUDGET_ALARM_USD" "$BUDGET_CAP_USD"; do
    a="${PROJECT_TAG}-billing-${usd}usd"
    aws_w "" cloudwatch put-metric-alarm --alarm-name "$a" --namespace AWS/Billing --metric-name EstimatedCharges \
      --dimensions "Name=Currency,Value=USD" "Name=LinkedAccount,Value=$AWS_ACCOUNT_ID" \
      --statistic Maximum --period 21600 --evaluation-periods 1 --threshold "$usd" \
      --comparison-operator GreaterThanThreshold --alarm-actions "$topic" --region us-east-1
  done
  log "created CloudWatch billing alarms at \$$BUDGET_ALARM_USD and \$$BUDGET_CAP_USD (whole account, not just this project) -> $topic. Confirm the email subscription."
}
alarm_ok=0
case "$alarm_method" in
  budgets)
    if budget_exists; then
      log "billing alarm $BUDGET_NAME exists"
      alarm_ok=1
    else
      require_env ALARM_EMAIL
      create_budget
      if is_dry || budget_exists; then alarm_ok=1; fi
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
state_set_json '.budget.alarm_usd' "$BUDGET_ALARM_USD"
state_set_json '.budget.cap_usd' "$BUDGET_CAP_USD"
state_set '.budget.name' "$BUDGET_NAME"
ensure_api_key >/dev/null

log_line 00-preflight "account ok; vcpu quota ok for $FLEET_PROFILE ($need_vcpus); ECR and billing alarm in place" "10-network.sh"
log "preflight complete"
