#!/usr/bin/env bash
# Measure the ceiling of ONE serial consumer: a single connection issuing,
# back to back, the exact query the LISTEN loop runs after every NOTIFY
# (postgres.go loadChannelMessage / sqlLoadCM) against a real row, over the
# same Docker network the nodes use. This is the upper bound on
# notifications per second one node can process if each costs one round trip,
# before msgpack decode and delivery are added.
#
#   bench/consumer-bound.sh [network] [pg-container] [seconds]
#
# Needs rows in channel_messages (run a benchmark first).
set -euo pipefail
net="${1:-ablyscale_default}"
pg="${2:-ablyscale-postgres-1}"
secs="${3:-10}"
tmp="$(mktemp -d)"
row="$(docker exec "$pg" psql -U ably -d ably -At -F '|' -c \
  "select channel, channel_serial from channel_messages order by channel_serial desc limit 1")"
ch="${row%%|*}"; cs="${row#*|}"
echo "probe row: channel=$ch channel_serial=$cs"
cat > "$tmp/load_cm.sql" <<SQL
SELECT idx, kind, payload, summary FROM channel_messages
WHERE channel = '$ch' AND channel_serial = '$cs'
ORDER BY idx;
SQL
echo 'SELECT 1;' > "$tmp/ping.sql"
run() {
  docker run --rm --network "$net" -e PGPASSWORD=ably -v "$tmp:/scripts:ro" postgres:16-alpine \
    pgbench -h postgres -U ably -d ably -n -M prepared -c 1 -j 1 -T "$secs" -f "/scripts/$1" 2>&1 \
    | grep -E 'number of transactions actually|latency average|tps ='
}
echo "--- SELECT 1 (pure round trip), 1 connection, ${secs}s"; run ping.sql
echo "--- loadChannelMessage query, 1 connection, ${secs}s"; run load_cm.sql
rm -rf "$tmp"
