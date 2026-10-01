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
# Dummy secrets, given to every render as if a template had regressed to using them: none may reach
# a user-data file, which EC2 keeps readable from the metadata service and in the console.
export GHCR_PULL_TOKEN=ghp_testtoken123
secret_values=(bench.a:b 'postgres://u:p@h:5432/db' Secretpassword0123456789 ghp_testtoken123)
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
# registry login in the common part: ghcr with a pull token, ghcr without, ecr, none
IMAGE_REGISTRY_KIND=ghcr render loadgen IMAGE=ghcr.io/o/ably-loadgen:abc 'CMD=ably-loadgen serve' >/dev/null
if ! grep -q "docker login ghcr.io -u 'token' --password-stdin" "$tmp/loadgen.sh" || ! grep -q "printf '%s' \"\$BENCH_REGISTRY_TOKEN\"" "$tmp/loadgen.sh"; then
  echo "FAIL: ghcr with a pull token does not log in"
  exit 1
fi
echo "ok   ghcr with a pull token logs in to ghcr.io (the token comes from the delivered env file)"
# the login runs before the first container starts, so a private base image can be pulled
login_line=$(grep -n 'docker login ghcr.io' "$tmp/loadgen.sh" | head -1 | cut -d: -f1)
first_run=$(grep -n 'docker run -d --name node-exporter' "$tmp/loadgen.sh" | head -1 | cut -d: -f1)
[ "$login_line" -lt "$first_run" ] || { echo "FAIL: the registry login comes after the first container"; exit 1; }
echo "ok   the registry login comes before the first container"
# the secrets are loaded (waited for) before they are used, and tracing is off while they are
load_line=$(grep -n '^      bench_load_secrets' "$tmp/loadgen.sh" | head -1 | cut -d: -f1)
off_line=$(grep -n 'set +x # keep the token out' "$tmp/loadgen.sh" | head -1 | cut -d: -f1)
[ -n "$load_line" ] && [ "$load_line" -lt "$login_line" ] || { echo "FAIL: the token is used before it is loaded"; exit 1; }
[ -n "$off_line" ] && [ "$off_line" -lt "$login_line" ] || { echo "FAIL: tracing is not switched off before the token is used"; exit 1; }
echo "ok   the token is loaded first and tracing is off while it is used"
# no secret reaches the user-data of any role (ghcr token set for all of them above and below)
for role_file in "$tmp"/*.sh; do
  [ "$role_file" = "$tmp/state.json" ] && continue
  for v in "${secret_values[@]}"; do
    if grep -qF -- "$v" "$role_file"; then echo "FAIL: $(basename "$role_file") user-data contains a secret value ($v)"; exit 1; fi
  done
  if grep -q 'ABLY_API_KEY\|GHCR_PULL_TOKEN' "$role_file"; then echo "FAIL: $(basename "$role_file") user-data names a secret variable (ABLY_API_KEY or GHCR_PULL_TOKEN)"; exit 1; fi
  if grep -Eq 'postgres(ql)?://[^[:space:]/:@]+:[^[:space:]@]+@' "$role_file"; then echo "FAIL: $(basename "$role_file") user-data contains a postgres:// URL with a password"; exit 1; fi
done
echo "ok   no user-data carries an API key, a database password or DSN, or a registry token"
# the roles that need secrets wait for them
for r in node postgres conductor; do
  grep -q '^bench_load_secrets\|^  *bench_load_secrets' "$tmp/$r.sh" || { echo "FAIL: $r user-data does not load delivered secrets"; exit 1; }
done
echo "ok   node, postgres and conductor load their secrets from the delivered env file"
unset GHCR_PULL_TOKEN
IMAGE_REGISTRY_KIND=ghcr render loadgen IMAGE=ghcr.io/o/ably-loadgen:abc 'CMD=ably-loadgen serve' >/dev/null
grep -q 'if \[ "0" = 1 \]; then' "$tmp/loadgen.sh" || { echo "FAIL: without a pull token the login branch is not disabled"; exit 1; }
if grep -q 'ghp_' "$tmp/loadgen.sh"; then echo "FAIL: a token appears without GHCR_PULL_TOKEN"; exit 1; fi
echo "ok   no token in the user-data when none is set"
IMAGE_REGISTRY_KIND=ecr REGISTRY_HOST=111111111111.dkr.ecr.test-region-1.amazonaws.com render loadgen IMAGE=x/y:abc 'CMD=ably-loadgen serve' >/dev/null
grep -q 'aws ecr get-login-password --region test-region-1' "$tmp/loadgen.sh" || { echo "FAIL: ecr user-data does not log in to ECR"; exit 1; }
echo "ok   ecr user-data logs in with the instance profile"
export GHCR_PULL_TOKEN=ghp_testtoken123
if (GHCR_PULL_TOKEN="bad'token" IMAGE_REGISTRY_KIND=ghcr render_userdata "$tmp/bad.sh" loadgen IMAGE=x 'CMD=y' 2>/dev/null); then
  echo "FAIL: a token with a quote was accepted"; exit 1
fi
echo "ok   a pull token with a quote is refused"
# the operator's key is written to ec2-user's authorized_keys by every role
grep -q "ssh-ed25519 AAAAdryrunkey dry-run" "$tmp/postgres.sh" || { echo "FAIL: the SSH public key is not in the user-data"; exit 1; }
echo "ok   the SSH public key is in the user-data"
echo "user-data lint passed"
