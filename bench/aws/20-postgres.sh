#!/usr/bin/env bash
# 20-postgres: RDS for PostgreSQL 17, Single-AZ, synchronous_commit=on, slow
# statements logged at 50 ms, pg_stat_statements loaded, Performance Insights on.
# Create or reuse; waits until available; records each DSN in STATE. The
# instance id carries the storage type (<tag>-pg-io2, <tag>-pg-gp3) so an io2
# and a gp3 instance can exist side by side for run 0a and run 4.
#
#   RDS_STORAGE=io2 bench/aws/20-postgres.sh
#   RDS_STORAGE=gp3 bench/aws/20-postgres.sh      # second instance; set ACTIVE_STORAGE to switch nodes over
#   SHARDS=3 bench/aws/20-postgres.sh             # run 8: three instances per storage type
#
# Needs: RDS_PASSWORD (letters, digits, _ and -; 16+), network state from 10-network.sh.
# Optional: RDS_INSTANCE_CLASS, RDS_STORAGE (io2|gp3), RDS_STORAGE_GB, RDS_IOPS,
#           RDS_ENGINE_VERSION, RDS_FORCE_SSL (0|1), DB_POOL_SIZE, MAX_NODES, SHARDS.
SCRIPT_NAME=20-postgres
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
require_env AWS_REGION
state_init
require_preflight
require_network_state

case "$RDS_STORAGE" in io2 | gp3) ;; *) die "RDS_STORAGE must be io2 or gp3" ;; esac
: "${RDS_FORCE_SSL:=0}"
: "${DB_POOL_SIZE:=50}"
: "${MAX_NODES:=20}"
if [ "$RDS_STORAGE" = io2 ]; then : "${RDS_IOPS:=20000}"; else : "${RDS_IOPS:=}"; fi
max_conn=$((MAX_NODES * DB_POOL_SIZE + 300))
family="postgres${RDS_ENGINE_VERSION%%.*}"
sslmode=disable
if [ "$RDS_FORCE_SSL" = 1 ]; then sslmode=require; fi

spend_gate

# ---- DB subnet group (RDS needs two zones; the instance itself is pinned to one)
sng="${PROJECT_TAG}-db"
have=$(AWS_R_QUIET=1 aws_r "" rds describe-db-subnet-groups --db-subnet-group-name "$sng" --query 'DBSubnetGroups[0].DBSubnetGroupName') || have=""
if [ -z "$have" ]; then
  subnets=$(aws_r "subnet-dryrun-a subnet-dryrun-b" ec2 describe-subnets --filters "Name=vpc-id,Values=$(state_get '.network.vpc_id')" --query 'Subnets[].SubnetId')
  rds_tags "$sng" db
  # shellcheck disable=SC2086
  aws_w "" rds create-db-subnet-group --db-subnet-group-name "$sng" \
    --db-subnet-group-description "ably-server scale proof ($PROJECT_TAG)" --subnet-ids $subnets --tags "${RDS_TAGS[@]}" >/dev/null
  log "created DB subnet group $sng"
else
  log "reuse DB subnet group $sng"
fi
state_add_resource rds-subnet-group "$sng" db "$sng"

# ---- Parameter group
pgroup="${PROJECT_TAG}-pg${RDS_ENGINE_VERSION%%.*}"
have=$(AWS_R_QUIET=1 aws_r "" rds describe-db-parameter-groups --db-parameter-group-name "$pgroup" --query 'DBParameterGroups[0].DBParameterGroupName') || have=""
if [ -z "$have" ]; then
  rds_tags "$pgroup" db
  aws_w "" rds create-db-parameter-group --db-parameter-group-name "$pgroup" --db-parameter-group-family "$family" \
    --description "ably-server scale proof ($PROJECT_TAG)" --tags "${RDS_TAGS[@]}" >/dev/null
  log "created parameter group $pgroup"
fi
state_add_resource rds-param-group "$pgroup" db "$pgroup"
aws_w "" rds modify-db-parameter-group --db-parameter-group-name "$pgroup" --parameters \
  "ParameterName=synchronous_commit,ParameterValue=on,ApplyMethod=immediate" \
  "ParameterName=log_min_duration_statement,ParameterValue=50,ApplyMethod=immediate" \
  "ParameterName=track_io_timing,ParameterValue=1,ApplyMethod=immediate" \
  "ParameterName=pg_stat_statements.track,ParameterValue=all,ApplyMethod=immediate" \
  "ParameterName=rds.force_ssl,ParameterValue=$RDS_FORCE_SSL,ApplyMethod=immediate" \
  "ParameterName=max_connections,ParameterValue=$max_conn,ApplyMethod=pending-reboot" \
  "ParameterName=shared_preload_libraries,ParameterValue=pg_stat_statements,ApplyMethod=pending-reboot" >/dev/null

# ---- Is this storage type offered for this instance class? (RDS rejects it late otherwise.)
offered=$(aws_r "$RDS_STORAGE" rds describe-orderable-db-instance-options --engine postgres --db-instance-class "$RDS_INSTANCE_CLASS" \
  --query "OrderableDBInstanceOptions[?StorageType=='$RDS_STORAGE'].StorageType | [0]") || offered=""
if [ -z "$offered" ]; then
  die "RDS does not offer $RDS_STORAGE storage for $RDS_INSTANCE_CLASS (postgres) in this region. Pick another RDS_INSTANCE_CLASS or RDS_STORAGE."
fi

# ---- Instances
ids=()
for shard in $(seq 1 "$SHARDS"); do
  id="${PROJECT_TAG}-pg-${RDS_STORAGE}"
  if [ "$SHARDS" -gt 1 ]; then id="${id}-s${shard}"; fi
  ids+=("$id")
  status=$(AWS_R_QUIET=1 aws_r "" rds describe-db-instances --db-instance-identifier "$id" --query 'DBInstances[0].DBInstanceStatus') || status=""
  if [ -n "$status" ]; then
    log "reuse RDS instance $id ($status)"
    continue
  fi
  check_password
  rds_tags "$id" db
  args=(rds create-db-instance --db-instance-identifier "$id"
    --engine postgres --engine-version "$RDS_ENGINE_VERSION" --db-instance-class "$RDS_INSTANCE_CLASS"
    --allocated-storage "$RDS_STORAGE_GB" --storage-type "$RDS_STORAGE"
    --master-username "$DB_USER" --master-user-password "$RDS_PASSWORD" --db-name "$DB_NAME"
    --vpc-security-group-ids "$(state_get '.network.sg_id')" --db-subnet-group-name "$sng"
    --availability-zone "$AZ" --no-multi-az --no-publicly-accessible
    --db-parameter-group-name "$pgroup"
    --backup-retention-period 0 --no-auto-minor-version-upgrade --no-deletion-protection --copy-tags-to-snapshot
    --enable-performance-insights --performance-insights-retention-period 7
    --tags "${RDS_TAGS[@]}")
  if [ -n "$RDS_IOPS" ]; then args+=(--iops "$RDS_IOPS"); fi
  aws_w "" "${args[@]}" >/dev/null
  log "creating RDS instance $id ($RDS_INSTANCE_CLASS, $RDS_STORAGE ${RDS_STORAGE_GB} GB${RDS_IOPS:+, $RDS_IOPS IOPS})"
done

for id in "${ids[@]}"; do
  aws_w "" rds wait db-instance-available --db-instance-identifier "$id"
done

# ---- Record endpoints and DSNs
shard=0
for id in "${ids[@]}"; do
  shard=$((shard + 1))
  endpoint=$(aws_r "${id}.dryrun.internal" rds describe-db-instances --db-instance-identifier "$id" --query 'DBInstances[0].Endpoint.Address')
  version=$(aws_r "${RDS_ENGINE_VERSION}.0" rds describe-db-instances --db-instance-identifier "$id" --query 'DBInstances[0].EngineVersion')
  dsn=$(state_get ".postgres.instances[\"$id\"].dsn")
  if [ -z "$dsn" ]; then
    check_password
    dsn="postgres://${DB_USER}:${RDS_PASSWORD}@${endpoint}:5432/${DB_NAME}?sslmode=${sslmode}"
  fi
  _state_update '.postgres.instances[$id] = {id:$id,storage:$st,shard:$sh,class:$cls,storage_gb:$gb,iops:$iops,endpoint:$ep,port:5432,dsn:$dsn,engine_version:$ver,param_group:$pg,running:true}' \
    --arg id "$id" --arg st "$RDS_STORAGE" --argjson sh "$shard" --arg cls "$RDS_INSTANCE_CLASS" \
    --argjson gb "$RDS_STORAGE_GB" --argjson iops "${RDS_IOPS:-0}" --arg ep "$endpoint" --arg dsn "$dsn" \
    --arg ver "$version" --arg pg "$pgroup"
  state_add_resource rds "$id" db "$id"
  log "$id ready: $endpoint (engine $version)"
done
if [ -n "${ACTIVE_STORAGE:-}" ]; then
  state_set '.postgres.active_storage' "$ACTIVE_STORAGE"
elif [ -z "$(state_get '.postgres.active_storage')" ]; then
  state_set '.postgres.active_storage' "$RDS_STORAGE"
fi
state_set_json '.postgres.max_connections' "$max_conn"

cost_checkpoint
log_line 20-postgres "RDS ready: ${ids[*]} ($RDS_INSTANCE_CLASS, $RDS_STORAGE); active storage $(state_get '.postgres.active_storage'); est \$$(state_get '.budget.rate_usd_h')/h fleet" "25-pgdriver.sh (run 0a) or 30-nats.sh"
log "postgres complete"
