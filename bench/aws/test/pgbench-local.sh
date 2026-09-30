#!/usr/bin/env bash
# Validate the run-0a SQL against a local Postgres 17 container: every
# variant runs, writes the rows it should, and the scripts still match the
# shipped publish code. Needs Docker (pulls postgres:17-alpine).
# shellcheck disable=SC2015  # "test && ok || bad": ok never fails
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWS_DIR="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$AWS_DIR/../.." && pwd)"
command -v docker >/dev/null || { echo "docker not found" >&2; exit 1; }
fails=0
ok() { printf 'ok   %s\n' "$1"; }
bad() { printf 'FAIL %s\n' "$1" >&2; fails=$((fails + 1)); }

# ---- 1. The SQL still matches the shipped code (textual drift guard)
schema_ref="90dfa15:internal/storage/postgres/migrations/0001_initial.sql"
if git -C "$REPO" cat-file -e "$schema_ref" 2>/dev/null; then
  if git -C "$REPO" show "$schema_ref" | diff -q - "$AWS_DIR/pgbench/schema.sql" >/dev/null; then ok "schema.sql is the shipped 0001_initial.sql (90dfa15)"; else bad "schema.sql differs from 90dfa15's 0001_initial.sql"; fi
else
  echo "skip schema drift check (90dfa15 not in this clone)"
fi
go_src=$(git -C "$REPO" show 90dfa15:internal/storage/postgres/postgres.go 2>/dev/null || true)
if [ -n "$go_src" ]; then
  for stmt in \
    'SELECT channel_serial FROM channel_messages' \
    'SELECT advance_channel_serial($1, $2)' \
    'INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, message_serial)' \
    'INSERT INTO messages (channel, message_serial, payload, deleted)' \
    'SELECT pg_notify($1, $2)'; do
    if grep -qF "$stmt" <<<"$go_src"; then ok "shipped code has: $stmt"; else bad "shipped code no longer has: $stmt"; fi
  done
  for frag in 'SELECT channel_serial FROM channel_messages' 'advance_channel_serial' 'INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, message_serial)' 'INSERT INTO messages (channel, message_serial, payload, deleted)' "pg_notify('ably_channel'"; do
    if grep -qF "$frag" "$AWS_DIR/pgbench/publish_shipped.sql"; then ok "publish_shipped.sql has: $frag"; else bad "publish_shipped.sql lacks: $frag"; fi
  done
fi

# ---- 2. Run every variant against Postgres 17. The tools run from the postgres image
# on the container's own network (the path the driver box uses), so nothing
# depends on Docker Desktop's port forwarding.
name="pgbench-local-$$"
out=$(mktemp -d)
out=$(cd "$out" && pwd -P)
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; rm -rf "$out"; }
trap cleanup EXIT
docker run -d --name "$name" -e POSTGRES_PASSWORD=pw postgres:17-alpine -c synchronous_commit=on >/dev/null
qsql() { docker exec -e PGPASSWORD=pw "$name" psql -X -qAt -U postgres "$@"; }
for _ in $(seq 1 60); do
  qsql -c 'select 1' >/dev/null 2>&1 && break
  sleep 1
done
version=$(qsql -c 'show server_version_num')
[ "${version%????}" -ge 17 ] && ok "Postgres $version" || bad "expected Postgres 17, got $version"

export PGPASSWORD=pw PGHOST=localhost PGPORT=5432 PGUSER=postgres PGDATABASE=postgres
export STORAGE_LABEL=local OUT_DIR="$out" DURATION_S=3 WARMUP_S=0 CLIENTS="1 4" CHANNELS=3000 PAYLOAD_BYTES=600
export PGTOOL=docker DOCKER_NETWORK="container:$name"
"$AWS_DIR/pgbench/run.sh" setup 2>&1 | sed 's/^/     /'
"$AWS_DIR/pgbench/run.sh" run 2>&1 | sed 's/^/     /'
csv="$out/results-local.csv"
rows=$(($(wc -l <"$csv") - 1))
[ "$rows" = 14 ] && ok "14 cells (7 variants x 2 client counts)" || bad "expected 14 cells, got $rows"
if awk -F, 'NR > 1 && ($7 + 0 <= 0 || $16 != "ok" || $14 != 0) { exit 1 }' "$csv"; then ok "every cell ran with tps > 0, no failed transactions"; else bad "a cell failed or had zero tps"; cat "$csv" >&2; fi

# ---- 3. What the last cells wrote (TRUNCATE ran before each cell, so these are batch100 at 4 clients)
tx=$(awk -F, '$2 == "batch100" && $4 == 4 {print $6}' "$csv")
rowsw=$(qsql -d ably_pgbench -c 'select count(*) from channel_messages')
proj=$(qsql -d ably_pgbench -c 'select count(*) from messages')
[ "$rowsw" = $((tx * 100)) ] && ok "batch100 wrote 100 rows per transaction ($rowsw rows, $tx transactions)" || bad "batch100 rows $rowsw != $tx x 100"
[ "$proj" = "$rowsw" ] && ok "projection rows match log rows" || bad "messages $proj != channel_messages $rowsw"
dups=$(qsql -d ably_pgbench -c 'select count(*) from (select channel, channel_serial from channel_messages group by 1,2 having count(*) > 1) x')
[ "$dups" = 0 ] && ok "no duplicate (channel, channel_serial)" || bad "$dups duplicate serials"
ids=$(qsql -d ably_pgbench -c 'select count(*) - count(distinct id) from channel_messages')
[ "$ids" = 0 ] && ok "idempotency ids are unique" || bad "$ids duplicate ids"

# ---- 4. A single-client shipped run: one row per transaction, serials strictly increasing per channel
export VARIANTS=shipped CLIENTS=1 CHANNELS=5
"$AWS_DIR/pgbench/run.sh" run >/dev/null 2>&1
tx=$(awk -F, '$2 == "shipped" && $4 == 1 {print $6}' "$csv" | tail -1)
rowsw=$(qsql -d ably_pgbench -c 'select count(*) from channel_messages')
[ "$rowsw" = "$tx" ] && ok "shipped wrote one row per transaction ($rowsw)" || bad "shipped rows $rowsw != transactions $tx"
order=$(qsql -d ably_pgbench -c "select count(*) from (select channel, channel_serial, lag(channel_serial) over (partition by channel order by channel_serial) p from channel_messages) x where p is not null and p >= channel_serial")
[ "$order" = 0 ] && ok "channel serials strictly increase" || bad "serial order violated $order times"

# ---- 5. The summary renders
"$AWS_DIR/pgbench/summarise.sh" "$csv" >"$out/summary.md"
if grep -q '^| shipped |' "$out/summary.md" && grep -q 'publishes (messages) per second' "$out/summary.md"; then ok "summary tables render"; else bad "summary tables missing"; cat "$out/summary.md" >&2; fi

[ "$fails" = 0 ] || { echo "$fails check(s) failed" >&2; exit 1; }
echo "pgbench SQL validated against Postgres $version"
