#!/usr/bin/env bash
# Block until every endpoint in a comma-separated host:port list answers
# 200 on /readyz, or fail after a timeout.
#
#   bench/wait-ready.sh localhost:8081,localhost:8082 [timeout-seconds]
set -euo pipefail
endpoints="$1"
timeout="${2:-120}"
deadline=$(( $(date +%s) + timeout ))
IFS=',' read -r -a eps <<< "$endpoints"
for ep in "${eps[@]}"; do
  until curl -fsS -o /dev/null "http://$ep/readyz" 2>/dev/null; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "timeout waiting for http://$ep/readyz" >&2
      exit 1
    fi
    sleep 0.5
  done
done
echo "all ${#eps[@]} endpoint(s) ready"
