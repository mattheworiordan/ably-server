#!/usr/bin/env bash
# 20-postgres: PostgreSQL 17 on an EC2 instance with a separate EBS data volume
# (official postgres image in Docker, host networking, data on xfs), synchronous_commit=on,
# slow statements logged at 50 ms, pg_stat_statements loaded. This replaces RDS:
# the account's permission set denies every rds:* action. Create or reuse; waits
# until pg_isready succeeds over SSH; records each DSN in STATE. The instance
# name carries the storage type (<tag>-pg-io2, <tag>-pg-gp3) so an io2 and a gp3
# instance can exist side by side for run 0a and run 4.
#
#   PG_STORAGE=io2 bench/aws/20-postgres.sh       # RDS_STORAGE works too
#   PG_STORAGE=gp3 bench/aws/20-postgres.sh       # second instance; set ACTIVE_STORAGE to switch nodes over
#   SHARDS=3 bench/aws/20-postgres.sh             # run 8: three instances per storage type
#
# Needs: PG_PASSWORD or RDS_PASSWORD (letters, digits, _ and -; 16+), network
# state from 10-network.sh.
# Optional: PG_INSTANCE_TYPE (r7i.4xlarge), PG_STORAGE (io2|gp3), PG_STORAGE_GB (1000),
#           PG_IOPS (io2 20000; gp3 12000), PG_THROUGHPUT_MBPS (gp3 500), PG_IMAGE (postgres:17),
#           DB_POOL_SIZE, MAX_NODES, SHARDS. The RDS_* names still work.
#
# Both volumes are created inside the RunInstances call with DeleteOnTermination:
# the role may not delete standalone volumes, so the volume lives and dies with
# the instance. There is no stop and start: terminate and re-run this script.
SCRIPT_NAME=20-postgres
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
require_env AWS_REGION
state_init
require_preflight
require_network_state

case "$PG_STORAGE" in io2 | gp3) ;; *) die "PG_STORAGE must be io2 or gp3" ;; esac
if [ "${RDS_FORCE_SSL:-0}" = 1 ]; then die "RDS_FORCE_SSL=1 is not supported: TLS is out of scope for the proof (plan section 1)"; fi
if [ -n "${RDS_INSTANCE_CLASS:-}" ]; then log "WARNING: RDS_INSTANCE_CLASS=$RDS_INSTANCE_CLASS is ignored (db.* classes are RDS; r7g is ARM). Use PG_INSTANCE_TYPE (default r7i.4xlarge)."; fi
: "${DB_POOL_SIZE:=50}"
: "${MAX_NODES:=20}"
# EBS sizing. io2: 20000 IOPS. gp3: RDS gives a gp3 volume of this size 12000 IOPS and
# 500 MB/s, but plain EBS gp3 starts at 3000 and 125, so ask for RDS's figures to compare like with like.
if [ "$PG_STORAGE" = io2 ]; then
  : "${PG_IOPS:=20000}"
  PG_TP=""
else
  : "${PG_IOPS:=12000}"
  PG_TP=${PG_THROUGHPUT_MBPS:-500}
fi
max_conn=$((MAX_NODES * DB_POOL_SIZE + 300))
client_cidr=$(state_get '.network.subnet_cidr')
[ -n "$client_cidr" ] || die "STATE has no network.subnet_cidr; run 10-network.sh first"

spend_gate
check_password
init_work_dir

# Memory of the instance type, for shared_buffers (25 percent) and effective_cache_size (75 percent).
ram_mib=$(aws_r 131072 ec2 describe-instance-types --instance-types "$PG_INSTANCE_TYPE" --query 'InstanceTypes[0].MemoryInfo.SizeInMiB') || ram_mib=""
[ -n "$ram_mib" ] || die "could not read the memory size of $PG_INSTANCE_TYPE (ec2 describe-instance-types)"
conf=$(render_postgres_conf PRIVATE_IP_AT_BOOT "$max_conn" "$ram_mib")

# ---- Instances
names=()
for shard in $(seq 1 "$SHARDS"); do
  name="${PROJECT_TAG}-pg-${PG_STORAGE}"
  if [ "$SHARDS" -gt 1 ]; then name="${name}-s${shard}"; fi
  names+=("$name")
  ud="$BENCH_WORK_DIR/userdata-${name}.sh"
  render_userdata "$ud" postgres "POSTGRESQL_CONF=$conf" "CLIENT_CIDR=$client_cidr" "DATA_DEVICE=$PG_DATA_DEVICE" \
    "DB_USER=$DB_USER" "DB_NAME=$DB_NAME" "PG_IMAGE=$PG_IMAGE" "PG_PASSWORD=$RDS_PASSWORD"
  DATA_VOLUME_SPEC="$PG_STORAGE_GB $PG_STORAGE $PG_IOPS $PG_TP" launch_instance "$name" postgres "$PG_INSTANCE_TYPE" "$ud" >/dev/null
  log "postgres instance $name: $PG_INSTANCE_TYPE, $PG_STORAGE ${PG_STORAGE_GB} GB, ${PG_IOPS} IOPS${PG_TP:+, $PG_TP MB/s}"
done

ids=()
for name in "${names[@]}"; do ids+=("$(inst_field "$name" id)"); done
wait_instances_running "${ids[@]}"
refresh_instances
wait_boot "${names[@]}"

# ---- Wait for pg_isready over SSH, verify the settings that matter, record DSNs
shard=0
for name in "${names[@]}"; do
  shard=$((shard + 1))
  ip=$(inst_field "$name" private_ip)
  [ -n "$ip" ] || die "no private address for $name in STATE"
  if is_dry; then
    pg_ready "$name" "$ip"
  else
    wait_until "pg_isready on $name" 900 10 pg_ready "$name" "$ip"
  fi
  # Through the unix socket inside the container: no password on any command line.
  settings=$(ssh_do "$name" "docker exec postgres psql -U $DB_USER -d $DB_NAME -tAc \"select split_part(current_setting('server_version'), ' ', 1) || ' ' || current_setting('synchronous_commit') || ' ' || current_setting('shared_buffers') || ' ' || current_setting('max_connections') || ' ' || current_setting('shared_preload_libraries')\"" || true)
  if is_dry; then settings="17.0 on 32GB $max_conn pg_stat_statements"; fi
  read -r version sync_commit shared_buffers maxc preload <<<"$settings"
  [ "$sync_commit" = on ] || die "$name reports synchronous_commit='$sync_commit', not 'on' (settings: $settings)"
  case "$preload" in *pg_stat_statements*) ;; *) die "$name did not preload pg_stat_statements (settings: $settings)" ;; esac
  image_id=$(ssh_do "$name" "docker inspect postgres --format '{{.Image}}'" || true)
  if is_dry; then image_id="sha256:dryrun"; fi
  dsn=$(state_get ".postgres.instances[\"$name\"].dsn")
  if [ -z "$dsn" ]; then
    dsn="postgres://${DB_USER}:${RDS_PASSWORD}@${ip}:5432/${DB_NAME}?sslmode=disable"
  fi
  _state_update '.postgres.instances[$id] = {id:$id,instance_id:$iid,storage:$st,shard:$sh,type:$ty,storage_gb:$gb,iops:$iops,throughput_mbps:$tp,endpoint:$ep,private_ip:$ep,port:5432,dsn:$dsn,engine_version:$ver,image:$img,image_id:$imgid,settings:$set,running:true,managed:"ec2-docker"}' \
    --arg id "$name" --arg iid "$(inst_field "$name" id)" --arg st "$PG_STORAGE" --argjson sh "$shard" --arg ty "$PG_INSTANCE_TYPE" \
    --argjson gb "$PG_STORAGE_GB" --argjson iops "${PG_IOPS:-0}" --argjson tp "${PG_TP:-0}" --arg ep "$ip" --arg dsn "$dsn" \
    --arg ver "$version" --arg img "$PG_IMAGE" --arg imgid "$image_id" --arg set "$settings"
  log "$name ready at $ip: PostgreSQL $version, synchronous_commit=$sync_commit, shared_buffers=$shared_buffers, max_connections=$maxc"
done
if [ -n "${ACTIVE_STORAGE:-}" ]; then
  state_set '.postgres.active_storage' "$ACTIVE_STORAGE"
elif [ -z "$(state_get '.postgres.active_storage')" ]; then
  state_set '.postgres.active_storage' "$PG_STORAGE"
fi
state_set_json '.postgres.max_connections' "$max_conn"

cost_checkpoint
log_line 20-postgres "Postgres on EC2 ready: ${names[*]} ($PG_INSTANCE_TYPE, $PG_STORAGE ${PG_STORAGE_GB} GB, ${PG_IOPS} IOPS; $PG_IMAGE); active storage $(state_get '.postgres.active_storage'); est \$$(state_get '.budget.rate_usd_h')/h fleet" "25-pgdriver.sh (run 0a) or 30-nats.sh"
log "postgres complete"
