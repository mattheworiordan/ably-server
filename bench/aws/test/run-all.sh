#!/usr/bin/env bash
# Everything that runs without AWS credentials: shellcheck, unit tests, the
# dry-run call-sequence test, rendered user-data lint. SKIP_DOCKER=1 skips
# the tests that start containers (pgbench SQL, observability stack).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWS_DIR="$(cd "$HERE/.." && pwd)"
cd "$AWS_DIR"
shopt -s nullglob

echo "== shellcheck"
files=(./*.sh test/*.sh observability/*.sh)
if [ -d pgbench ]; then files+=(pgbench/*.sh); fi
shellcheck "${files[@]}"

echo "== rendered user-data lint"
test/userdata-lint.sh

echo "== lib unit tests"
test/lib-test.sh

echo "== dry-run call sequence"
test/dry-run.sh

if [ -x test/pgbench-local.sh ] && [ "${SKIP_DOCKER:-0}" != 1 ]; then
  echo "== pgbench SQL against a local Postgres 17"
  test/pgbench-local.sh
fi
if [ "${SKIP_DOCKER:-0}" != 1 ]; then
  echo "== observability stack"
  test/observability-local.sh
fi
echo "all checks passed"
