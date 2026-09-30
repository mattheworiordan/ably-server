#!/usr/bin/env bash
# Smoke-test one bench compose stack (DESIGN.md §7.2): bring it up, check
# every node is ready, publish over REST through node 1, read the message
# back through node 3's history, check node 1's /metrics reports the
# expected bus, and tear the stack down.
#
#   bench/compose-smoke.sh pgnotify|postgres|nats [--keep]
#
# --keep leaves the stack running afterwards. Needs docker and curl.
set -euo pipefail

bus=${1:?usage: compose-smoke.sh pgnotify|postgres|nats [--keep]}
keep=${2:-}
here=$(cd "$(dirname "$0")" && pwd)
file="$here/docker-compose.$bus.yml"
case $bus in
  pgnotify) base=8380 ;;
  postgres) base=8280 ;;
  nats) base=8180 ;;
  *) echo "unknown bus: $bus" >&2; exit 2 ;;
esac
node() { echo "http://localhost:$((base + $1))"; }
debug() { echo "http://localhost:$((base + 10 + $1))"; }

cleanup() {
  if [[ $keep != --keep ]]; then
    docker compose -f "$file" down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

docker compose -f "$file" up -d --build --wait

for i in 1 2 3; do
  curl -fsS "$(node "$i")/readyz" >/dev/null
done

channel="smoke-$bus-$RANDOM"
payload="hello from node1 on $bus"
curl -fsS -u app.key:secret -H 'Content-Type: application/json' \
  -d "{\"name\":\"smoke\",\"data\":\"$payload\"}" \
  "$(node 1)/channels/$channel/messages" >/dev/null

found=
for _ in $(seq 1 50); do
  history="$(curl -fsS -u app.key:secret "$(node 3)/channels/$channel/messages" || true)"
  if grep -q "$payload" <<<"$history"; then
    found=1
    break
  fi
  sleep 0.2
done
if [[ -z $found ]]; then
  echo "FAIL: the message published on node 1 is not in node 3's history" >&2
  exit 1
fi

# Read the whole body first: with pipefail, grep -q exiting at the first
# match makes curl fail writing the rest (exit 23) once /metrics outgrows
# the pipe buffer.
if ! metrics_body="$(curl -fsS "$(debug 1)/metrics")"; then
  echo "FAIL: node 1 /metrics did not answer" >&2
  exit 1
fi
if ! grep -q "ably_bus_info{bus=\"$bus\"" <<<"$metrics_body"; then
  echo "FAIL: node 1 /metrics does not report ably_bus_info{bus=\"$bus\"}" >&2
  exit 1
fi

echo "PASS: $bus stack ready on 3 nodes; REST publish on node 1 read back from node 3's history"
