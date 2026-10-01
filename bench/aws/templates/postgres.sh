# Role: PostgreSQL in Docker (official image, host networking) with its data on
# a separate EBS volume formatted as xfs. Not RDS: the account's permission set
# denies RDS, so this is the same database on the same kind of EBS storage,
# without the managed layer (no automated failover or backups).
# The settings file and the client subnet are rendered by 20-postgres.sh.
mkdir -p /etc/postgresql /data/pg

# 1. Find the data volume (the RunInstances block-device mapping, /dev/sdf; on
# Nitro it is an NVMe disk that amazon-ec2-utils links as /dev/sdf) and format it once.
find_data_dev() {
  local d
  if [ -b @@DATA_DEVICE@@ ]; then
    readlink -f @@DATA_DEVICE@@
    return 0
  fi
  for d in $(lsblk -dnpo NAME,TYPE | awk '$2 == "disk" { print $1 }'); do
    if [ "$(lsblk -nro NAME "$d" | wc -l)" = 1 ] && ! grep -q "^$d " /proc/mounts; then
      echo "$d"
      return 0
    fi
  done
  return 1
}
dev=""
for _ in $(seq 1 90); do
  dev=$(find_data_dev || true)
  if [ -n "$dev" ]; then break; fi
  sleep 2
done
if [ -z "$dev" ]; then
  echo "the EBS data volume did not appear in 3 minutes" >&2
  lsblk >&2
  exit 1
fi
blkid "$dev" >/dev/null 2>&1 || mkfs.xfs -f "$dev"
echo "UUID=$(blkid -s UUID -o value "$dev") /data/pg xfs defaults,noatime,nofail 0 2" >>/etc/fstab
mount /data/pg
mkdir -p /data/pg/pgdata
# Docker must not start the container before the data volume is mounted.
mkdir -p /etc/systemd/system/docker.service.d
printf '[Unit]\nRequiresMountsFor=/data/pg\n' >/etc/systemd/system/docker.service.d/91-pgdata.conf
systemctl daemon-reload

# 2. Configuration. The private address comes from the metadata service.
TOKEN=$(curl -fsS -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
PRIVATE_IP=$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)
cat >/etc/postgresql/postgresql.conf <<'PGCONF'
@@POSTGRESQL_CONF@@
PGCONF
sed "s/PRIVATE_IP_AT_BOOT/$PRIVATE_IP/" /etc/postgresql/postgresql.conf >/etc/postgresql/postgresql.conf.new
mv /etc/postgresql/postgresql.conf.new /etc/postgresql/postgresql.conf
cat >/etc/postgresql/pg_hba.conf <<'HBA'
local all all trust
host  all all 127.0.0.1/32 scram-sha-256
host  all all @@CLIENT_CIDR@@ scram-sha-256
HBA
: >/etc/postgresql/pg_ident.conf
cat >/etc/postgresql/10-extensions.sql <<'SQL'
\connect template1
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
\connect postgres
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
\connect @@DB_NAME@@
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
SQL

# 3. Run it.
docker pull @@PG_IMAGE@@
docker rm -f postgres 2>/dev/null || true
# The database password arrives over SSH, not in user-data (templates/secrets.sh).
bench_load_secrets
set +x
POSTGRES_PASSWORD="$BENCH_PG_PASSWORD" \
  docker run -d --name postgres --restart unless-stopped --network host --shm-size=2g \
  --ulimit nofile=1048576:1048576 \
  -e POSTGRES_USER=@@DB_USER@@ -e POSTGRES_DB=@@DB_NAME@@ -e POSTGRES_PASSWORD \
  -v /data/pg/pgdata:/var/lib/postgresql/data -v /etc/postgresql:/etc/postgresql:ro \
  -v /etc/postgresql/10-extensions.sql:/docker-entrypoint-initdb.d/10-extensions.sql:ro \
  @@PG_IMAGE@@ postgres -c config_file=/etc/postgresql/postgresql.conf
unset BENCH_PG_PASSWORD
set -x
ready=0
for _ in $(seq 1 180); do
  if docker exec postgres pg_isready -h "$PRIVATE_IP" -p 5432 -U @@DB_USER@@ >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
if [ "$ready" != 1 ]; then
  docker logs postgres 2>&1 | tail -30
  echo "postgres did not become ready in 6 minutes" >&2
  exit 1
fi
touch /var/lib/bench-ready
