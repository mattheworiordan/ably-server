#!/usr/bin/env bash
# How does the notifying-commit ceiling change with the number of LISTENing
# backends? In ably-server every node holds one LISTEN connection, so the
# listener count is the node count. Runs the pgbench transaction from
# notify-commit-ceiling.sh (BEGIN; INSERT; pg_notify; COMMIT) with 32 clients
# while L psql sessions LISTEN on the channel and poll every 10 ms so they
# actively consume notifications, for each L given.
#
#   bench/notify-listener-scaling.sh <pg-container> [seconds] [listener counts...]
#   bench/notify-listener-scaling.sh ably-cluster-postgres-1 8 0 1 3 6 12
#
# Creates and drops a scratch table `notify_probe` in the ably database.
set -euo pipefail
pg="${1:?postgres container}"
secs="${2:-8}"
shift 2 2>/dev/null || true
counts=("$@"); [ "${#counts[@]}" -gt 0 ] || counts=(0 1 3 6 12)
psql_() { docker exec "$pg" psql -U ably -d ably -qAt "$@"; }
psql_ -c "DROP TABLE IF EXISTS notify_probe; CREATE TABLE notify_probe (id bigserial PRIMARY KEY, payload text NOT NULL)" 2>/dev/null
docker exec -i "$pg" sh -c 'cat > /tmp/probe_notify.sql' <<'SQL'
BEGIN;
INSERT INTO notify_probe (payload) VALUES (repeat('x', 200));
SELECT pg_notify('probe', 'channel=c serial=s');
COMMIT;
SQL
docker exec -i "$pg" sh -c 'cat > /tmp/listen.sql' <<'SQL'
LISTEN probe;
SELECT 1 \watch 0.01
SQL
stop_listeners() {
  docker exec "$pg" sh -c 'if [ -f /tmp/listeners.pid ]; then kill $(cat /tmp/listeners.pid) 2>/dev/null; rm -f /tmp/listeners.pid; fi' >/dev/null 2>&1 || true
}
trap stop_listeners EXIT
echo "# notify commit ceiling vs LISTENing backends on $pg ($(docker exec "$pg" psql -U ably -d ably -At -c 'show server_version')), 32 pgbench clients, ${secs}s per cell"
for l in "${counts[@]}"; do
  stop_listeners
  if [ "$l" -gt 0 ]; then
    docker exec "$pg" sh -c "rm -f /tmp/listeners.pid; for i in \$(seq 1 $l); do psql -U ably -d ably -qAt -f /tmp/listen.sql >/dev/null 2>&1 & echo \$! >> /tmp/listeners.pid; done"
    sleep 1
  fi
  out="$(docker exec -e PGPASSWORD=ably "$pg" pgbench -h localhost -U ably -d ably -n -M prepared -c 32 -j 4 -T "$secs" -f /tmp/probe_notify.sql 2>&1 | grep -E 'tps =|latency average' | tr '\n' ' ')"
  active="$(psql_ -c "select count(*) from pg_stat_activity where query like '%watch%' or query = 'SELECT 1'")"
  echo "listeners=$l  $out"
done
stop_listeners
psql_ -c "DROP TABLE IF EXISTS notify_probe" 2>/dev/null
