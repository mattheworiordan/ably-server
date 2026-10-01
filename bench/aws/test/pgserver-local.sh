#!/usr/bin/env bash
# Run the Postgres role's own steps (templates/postgres.sh, from "2. Configuration" on)
# against a local Docker network, with the settings rendered the way 20-postgres.sh
# renders them, and check what the plan asks of the database: the settings in force,
# pg_stat_statements loaded and collecting, slow statements logged, listening on one
# address only, scram-sha-256 from the client subnet. Only the EC2 specifics are swapped:
# the paths, the docker network, the private address. Needs Docker (pulls postgres:17).
# shellcheck disable=SC2015  # "test && ok || bad": ok never fails
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWS_DIR="$(cd "$HERE/.." && pwd)"
command -v docker >/dev/null || { echo "docker not found" >&2; exit 1; }
tmp=$(mktemp -d)
tmp=$(cd "$tmp" && pwd -P)
suffix=$$
net="pgserver-local-$suffix"
cn="pgserver-local-$suffix"
vol="pgserver-local-$suffix"
cleanup() {
  docker rm -f "$cn" >/dev/null 2>&1 || true
  docker volume rm -f "$vol" >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT
fails=0
ok() { printf 'ok   %s\n' "$1"; }
bad() { printf 'FAIL %s\n' "$1" >&2; fails=$((fails + 1)); }

export DRY_RUN=1 DRY_STATE_FILE="$tmp/state.json" PROJECT_TAG=pgserver-test AWS_REGION=test-region-1
# shellcheck source=lib.sh
source "$AWS_DIR/lib.sh"

ip=172.30.77.10
password=Secretpassword0123456789
docker network create --subnet 172.30.77.0/24 "$net" >/dev/null
conf=$(render_postgres_conf PRIVATE_IP_AT_BOOT 300 4096)
render_template "$AWS_DIR/templates/postgres.sh" "POSTGRESQL_CONF=$conf" CLIENT_CIDR=172.30.77.0/24 DATA_DEVICE=/dev/sdf \
  DB_USER=ably DB_NAME=ably PG_IMAGE=postgres:17 >"$tmp/role.sh"
# The password is delivered over SSH in the real flow (templates/secrets.sh); here the same loader reads a local file.
printf "BENCH_PG_PASSWORD='%s'\n" "$password" >"$tmp/secrets.env"

# From "2. Configuration" on; swap only the EC2 specifics.
mkdir -p "$tmp/etc"
{
  echo 'set -euo pipefail'
  echo "export BENCH_SECRETS_FILE='$tmp/secrets.env'"
  cat "$AWS_DIR/templates/secrets.sh"
  sed -n '/^# 2\. Configuration/,$p' "$tmp/role.sh" |
    grep -v -e '^TOKEN=' -e 'bench-ready' |
    sed -e "s#^PRIVATE_IP=.*#PRIVATE_IP=$ip#" \
      -e "s#/etc/postgresql#$tmp/etc#g" \
      -e "s#-v /data/pg/pgdata:#-v $vol:#" \
      -e "s#--network host#--network $net --ip $ip#" \
      -e "s#--name postgres#--name $cn#" \
      -e "s#docker rm -f postgres#docker rm -f $cn#" \
      -e "s#docker exec postgres#docker exec $cn#g" \
      -e "s#docker logs postgres#docker logs $cn#"
} >"$tmp/run.sh"
if bash "$tmp/run.sh" >"$tmp/run.log" 2>&1; then ok "the role's steps start Postgres and pg_isready succeeds on the private address"; else
  bad "the role's steps failed"
  tail -30 "$tmp/run.log" >&2
  exit 1
fi

psql_in() { docker exec "$cn" psql -X -U ably -d "${DB:-ably}" -qAt "$@"; }
client() { # <password> <db> <sql>: from another container in the client subnet
  docker run --rm --network "$net" -e "PGPASSWORD=$1" -e PGCONNECT_TIMEOUT=5 postgres:17 psql -X -qAt -h "$ip" -U ably -d "$2" -c "$3"
}
want() { # <description> <setting> <expected>
  local got
  got=$(psql_in -c "show $2")
  if [ "$got" = "$3" ]; then ok "$1 ($2 = $3)"; else bad "$1: $2 is '$got', expected '$3'"; fi
}
want "synchronous commit" synchronous_commit on
want "slow statements logged at 50 ms" log_min_duration_statement 50ms
want "WAL compression on (PostgreSQL 15 and later report 'on' as pglz)" wal_compression pglz
want "checkpoint interval" checkpoint_timeout 15min
want "WAL ceiling" max_wal_size 16GB
want "shared_buffers is 25 percent of RAM" shared_buffers 1GB
want "effective_cache_size is 75 percent of RAM" effective_cache_size 3GB
want "max_connections" max_connections 300
want "listen address" listen_addresses "$ip"
want "pg_stat_statements preloaded" shared_preload_libraries pg_stat_statements
want "scram passwords" password_encryption scram-sha-256

[ "$(psql_in -c "select count(*) from pg_extension where extname = 'pg_stat_statements'")" = 1 ] && ok "pg_stat_statements extension exists in the application database" || bad "pg_stat_statements extension missing in ably"
[ "$(DB=postgres psql_in -c "select count(*) from pg_extension where extname = 'pg_stat_statements'")" = 1 ] && ok "pg_stat_statements extension exists in the postgres database (run 0a connects there)" || bad "pg_stat_statements extension missing in postgres"
psql_in -c "create database fresh" >/dev/null
[ "$(DB=fresh psql_in -c "select count(*) from pg_extension where extname = 'pg_stat_statements'")" = 1 ] && ok "a new database gets the extension (template1)" || bad "template1 lacks the extension"

# Access: the client subnet with the password; nobody without it; nothing on loopback.
[ "$(client "$password" ably 'select 42')" = 42 ] && ok "a client in the subnet connects with the password (scram-sha-256)" || bad "client with the right password could not connect"
if client wrong-password-0123456789 ably 'select 1' >/dev/null 2>&1; then bad "a wrong password was accepted"; else ok "a wrong password is refused"; fi
if docker exec "$cn" pg_isready -q -h 127.0.0.1 -p 5432; then bad "Postgres answers on loopback; it must listen on the private address only"; else ok "nothing listens on loopback (private address only)"; fi

# Statistics and the slow-statement log.
psql_in -c "select pg_sleep(0.08)" >/dev/null
[ "$(psql_in -c "select count(*) from pg_stat_statements where query like '%pg_sleep%'")" -ge 1 ] && ok "pg_stat_statements records statements" || bad "pg_stat_statements recorded nothing"
docker logs "$cn" 2>&1 | grep -q 'duration: .*pg_sleep' && ok "a statement over 50 ms is logged with its duration" || bad "the slow statement was not logged"

# A restart keeps the data and the settings (the container has a restart policy on the box).
psql_in -c "create table survive(x int); insert into survive values (7)" >/dev/null
docker restart "$cn" >/dev/null
for _ in $(seq 1 60); do docker exec "$cn" pg_isready -q -h "$ip" -p 5432 && break; sleep 1; done
[ "$(psql_in -c 'select x from survive')" = 7 ] && ok "data survives a restart" || bad "data lost over a restart"
want "settings survive a restart" synchronous_commit on

if [ "$fails" -gt 0 ]; then
  echo "$fails check(s) failed" >&2
  exit 1
fi
echo "postgres server tests passed"
