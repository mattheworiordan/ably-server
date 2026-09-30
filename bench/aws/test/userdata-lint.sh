#!/usr/bin/env bash
# Render every user-data role with dummy values, run shellcheck over the result
# and check the EC2 size limit.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
export DRY_RUN=1 DRY_STATE_FILE="$tmp/state.json" PROJECT_TAG=test-scale AWS_REGION=test-region-1 AWS_ACCOUNT_ID=111111111111
# shellcheck source=lib.sh
source "$HERE/../lib.sh"

render() { # <role> KEY=VALUE...
  local role=$1
  shift
  render_userdata "$tmp/$role.sh" "$role" "$@"
  shellcheck -s bash "$tmp/$role.sh"
  printf 'ok   %s user-data renders and lints (%s bytes)\n' "$role" "$(wc -c <"$tmp/$role.sh")"
}
render nats SERVER_NAME=nats-1 CLIENT_PORT=4222 MONITOR_PORT=8222 ROUTE_PORT=6222 \
  'ROUTES="nats-route://10.0.0.11:6222", "nats-route://10.0.0.12:6222"' NATS_IMAGE=nats:2.11 \
  EXPORTER_IMAGE=natsio/prometheus-nats-exporter:0.15.0 EXPORTER_PORT=7777
render node IMAGE=example/ably-server:abc 'ENV_ARGS=-e GOMEMLIMIT=13GiB' API_KEY=bench.a:b \
  'DSN=postgres://u:p@h:5432/db?sslmode=disable' PORT=8080 DEBUG_PORT=6060 'FLAGS=--bus=nats --nats-url=nats://10.0.0.10:4222'
render loadgen IMAGE=example/ably-loadgen:abc 'CMD=ably-loadgen serve --listen=:9100'
INSTALL_COMPOSE=1 render conductor IMAGE=example/ably-loadgen:abc PGBENCH_IMAGE=postgres:17-alpine 'DSNS=postgres://u:p@h:5432/db,postgres://u:p@h2:5432/db'
render pgdriver PGBENCH_IMAGE=postgres:17-alpine
render postgres "POSTGRESQL_CONF=$(render_postgres_conf PRIVATE_IP_AT_BOOT 1300 131072)" CLIENT_CIDR=172.31.16.0/20 DATA_DEVICE=/dev/sdf \
  DB_USER=ably DB_NAME=ably PG_IMAGE=postgres:17 PG_PASSWORD=Secretpassword0123456789
# the operator's key is written to ec2-user's authorized_keys by every role
grep -q "ssh-ed25519 AAAAdryrunkey dry-run" "$tmp/postgres.sh" || { echo "FAIL: the SSH public key is not in the user-data"; exit 1; }
echo "ok   the SSH public key is in the user-data"
echo "user-data lint passed"
