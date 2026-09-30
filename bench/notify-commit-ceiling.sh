#!/usr/bin/env bash
# Measure Postgres' own ceiling for transactions that call pg_notify, with no
# ably-server node or ably-bench client involved. Each transaction is
# BEGIN; INSERT one 200 byte row; [SELECT pg_notify(...);] COMMIT, so the WAL
# volume is comparable to a publish. Runs the same load with and without the
# pg_notify call, for 1 client and for several, using pgbench inside the
# postgres container. PG 16 takes a database-wide exclusive lock in
# PreCommit_Notify and holds it through the WAL flush, so notifying commits
# should serialise while plain commits group-commit.
#
#   bench/notify-commit-ceiling.sh [pg-container] [seconds] [clients...]
#
# Creates and drops a scratch table `notify_probe` in the ably database.
set -euo pipefail
pg="${1:-ablyscale-postgres-1}"
secs="${2:-10}"
shift 2 2>/dev/null || true
clients=("${@:-1 8 32}")
[ "$#" -gt 0 ] || clients=(1 8 32)
psql_() { docker exec "$pg" psql -U ably -d ably -qAt "$@"; }
psql_ -c "DROP TABLE IF EXISTS notify_probe; CREATE TABLE notify_probe (id bigserial PRIMARY KEY, payload text NOT NULL)"
docker exec -i "$pg" sh -c 'cat > /tmp/probe_notify.sql' <<'SQL'
BEGIN;
INSERT INTO notify_probe (payload) VALUES (repeat('x', 200));
SELECT pg_notify('probe', 'channel=c serial=s');
COMMIT;
SQL
docker exec -i "$pg" sh -c 'cat > /tmp/probe_plain.sql' <<'SQL'
BEGIN;
INSERT INTO notify_probe (payload) VALUES (repeat('x', 200));
COMMIT;
SQL
run() { # run <script> <clients>
  docker exec -e PGPASSWORD=ably "$pg" pgbench -h localhost -U ably -d ably -n -M prepared \
    -c "$2" -j "$(( $2 < 4 ? $2 : 4 ))" -T "$secs" -f "/tmp/$1" 2>&1 \
    | grep -E 'tps =|latency average' | tr '\n' ' '
  echo
}
echo "# pg_notify commit ceiling on $pg (${secs}s per cell)"
for c in "${clients[@]}"; do
  echo "clients=$c  with pg_notify:    $(run probe_notify.sql "$c")"
  echo "clients=$c  without pg_notify: $(run probe_plain.sql "$c")"
done
psql_ -c "DROP TABLE IF EXISTS notify_probe"
