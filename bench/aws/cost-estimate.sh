#!/usr/bin/env bash
# cost-estimate: what the running fleet costs an hour, and an estimate of
# what this proof has spent so far, from STATE. Prices are approximate
# on-demand list prices (lib.sh price_of; override with PRICE_<type>, scale
# with PRICE_FACTOR). The real bill is in AWS Billing, which this role cannot
# read (no Cost Explorer, no ViewBudget): this estimate is the primary spend
# guard, with FLEET_MAX_UPTIME_H. It asks EC2 which boxes still exist first, then
# lists what carries the project tag (the tagging API, or service by service when
# tag:GetResources is denied) and warns about instances and volumes STATE does not
# know, because their cost is not in the estimate.
#
#   bench/aws/cost-estimate.sh           table
#   bench/aws/cost-estimate.sh --json    one JSON object
SCRIPT_NAME=cost-estimate
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
state_init
# Learn what still exists (the dead-man switch terminates boxes behind our back).
untracked=()
if ! is_dry && [ "${NO_AWS:-0}" != 1 ] && command -v aws >/dev/null 2>&1; then
  refresh_instances || true
  known=" $(state_get '[.instances[]?.id, (.resources[]?.id)] | join(" ")') "
  if project_inventory; then
    for line in "${INVENTORY_LINES[@]}"; do
      case "${line%% *}" in
        instance | volume)
          case "$known" in *" ${line#* } "*) ;; *) untracked+=("$line") ;; esac
          ;;
        arn)
          # the tagging API lists ARNs: pick out instances and volumes
          case "$line" in
            *:instance/* | *:volume/*)
              arn_id=${line##*/}
              case "$known" in *" $arn_id "*) ;; *) untracked+=("${line#arn }") ;; esac
              ;;
          esac
          ;;
      esac
    done
    if [ "${#untracked[@]}" -gt 0 ]; then
      log "WARNING: ${#untracked[@]} instance(s) or volume(s) tagged Project=$PROJECT_TAG are not in STATE, so their cost is not in this estimate: ${untracked[*]}"
    fi
    if [ "${#INVENTORY_UNVERIFIED[@]}" -gt 0 ]; then
      log "could not list (denied): ${INVENTORY_UNVERIFIED[*]}"
    fi
  else
    log "WARNING: could not list the project's resources in the account; the estimate covers only what STATE knows"
  fi
fi
cost_checkpoint
rate=$(state_get '.budget.rate_usd_h')
accrued=$(state_get '.budget.accrued_usd')
: "${rate:=0}" "${accrued:=0}"

if [ "${1:-}" = --json ]; then
  jq -n --argjson rate "$rate" --argjson acc "$accrued" --argjson alarm "$BUDGET_ALARM_USD" --argjson cap "$BUDGET_CAP_USD" \
    '{hourly_usd:$rate, accrued_estimate_usd:$acc, alarm_usd:$alarm, cap_usd:$cap}'
  exit 0
fi

printf '%-34s %-16s %5s %9s %9s\n' "resource" "type" "count" "USD/h" "USD/h all"
state_get '.instances // {} | to_entries | map(select(.value.running == true)) | group_by(.value.type)[] | [.[0].value.type, (length|tostring)] | @tsv' |
  while IFS=$'\t' read -r type count; do
    p=$(price_of "$type")
    printf '%-34s %-16s %5s %9s %9s\n' "EC2" "$type" "$count" "$p" "$(awk -v p="$p" -v c="$count" 'BEGIN{printf "%.2f", p*c}')"
  done
state_get '.postgres.instances // {} | to_entries[] | select(.value.running == true) | .value' | jq -c . 2>/dev/null |
  while IFS= read -r row; do
    [ -n "$row" ] || continue
    h=$(ebs_hourly "$(jq -r .storage <<<"$row")" "$(jq -r '.storage_gb // 0' <<<"$row")" "$(jq -r '.iops // 0' <<<"$row")" "$(jq -r '.throughput_mbps // 125' <<<"$row")")
    printf '%-34s %-16s %5s %9s %9s\n' "EBS data $(jq -r .id <<<"$row")" "$(jq -r .storage <<<"$row")/$(jq -r '.storage_gb // 0' <<<"$row")GB" 1 "$h" "$h"
  done
printf '\nrunning fleet:        $%.2f per hour\n' "$rate"
printf 'spent so far (est.):  $%.2f   alarm $%s   cap $%s\n' "$accrued" "$BUDGET_ALARM_USD" "$BUDGET_CAP_USD"
awk -v a="$accrued" -v r="$rate" -v cap="$BUDGET_CAP_USD" 'BEGIN{ if (r > 0) printf "hours left before the cap at this rate: %.1f\n", (cap-a)/r }'
if awk -v a="$accrued" -v al="$BUDGET_ALARM_USD" 'BEGIN{exit !(a+0 >= al+0)}'; then
  echo "WARNING: the estimate has passed the alarm level; decide before starting more runs."
fi
