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

# 2. vCPU quota for the fleet.
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

# 3. Instance types offered in the chosen Availability Zone.
for t in "$NODE_INSTANCE_TYPE" "$LOADGEN_INSTANCE_TYPE" "$PUBLISHER_INSTANCE_TYPE"; do
  offered=$(aws_r "$t" ec2 describe-instance-type-offerings --location-type availability-zone \
    --filters "Name=location,Values=$AZ" "Name=instance-type,Values=$t" --query 'InstanceTypeOfferings[0].InstanceType') || offered=""
  [ -n "$offered" ] || log "WARNING: $t is not offered in $AZ; set AZ to another zone"
done

# 4. ECR repositories.
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

# 5. Billing alarm (AWS Budgets): $BUDGET_ALARM_USD warning, $BUDGET_CAP_USD cap.
budget_exists() {
  local b
  b=$(AWS_R_QUIET=1 aws_r "" budgets describe-budget --account-id "$AWS_ACCOUNT_ID" --budget-name "$BUDGET_NAME" --query Budget.BudgetName) || return 1
  [ -n "$b" ]
}
if budget_exists; then
  log "billing alarm $BUDGET_NAME exists"
else
  require_env ALARM_EMAIL
  init_work_dir
  wd=$BENCH_WORK_DIR
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
  log "created billing alarm $BUDGET_NAME: warn at \$$BUDGET_ALARM_USD (${alarm_pct}%), cap \$$BUDGET_CAP_USD, scope $BUDGET_SCOPE"
  if ! is_dry && ! budget_exists; then
    if [ "${FORCE_NO_ALARM:-0}" = 1 ]; then
      log "WARNING: FORCE_NO_ALARM=1; continuing without a verified alarm (you confirmed an alarm exists elsewhere)"
      log_line 00-preflight "no verified billing alarm; FORCE_NO_ALARM=1" "confirm the alarm by hand"
    else
      die "billing alarm $BUDGET_NAME does not exist after create; refusing to continue (RUNBOOK, 'Billing alarm')"
    fi
  fi
fi
state_set_json '.budget.alarm_usd' "$BUDGET_ALARM_USD"
state_set_json '.budget.cap_usd' "$BUDGET_CAP_USD"
state_set '.budget.name' "$BUDGET_NAME"
ensure_api_key >/dev/null

log_line 00-preflight "account ok; vcpu quota ok for $FLEET_PROFILE ($need_vcpus); ECR and billing alarm in place" "10-network.sh"
log "preflight complete"
