#!/usr/bin/env bash
# Print the comma-separated endpoint list of every running node in the scale
# stack (bench/docker-compose.scale.yml), for ably-bench --endpoints.
#
#   bench/scale-endpoints.sh [project] [internal]
#
# Default: host ports (localhost:PORT), reached through Docker Desktop's port
# proxy. With `internal`: container IPs on the compose network (IP:8080), for a
# load generator running inside that network (bench/docker-bench.sh). IPs, not
# hostnames: ably-go treats a dotless endpoint as a routing-policy name.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project="${1:-ablyscale}"
mode="${2:-host}"
if [ "$mode" = internal ]; then
  docker ps --filter "label=com.docker.compose.project=$project" --filter "label=com.docker.compose.service=node" -q \
    | xargs docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}:8080' \
    | sort -t. -k4 -n | paste -sd, -
  exit 0
fi
docker compose -f "$here/docker-compose.scale.yml" -p "$project" ps --format json \
  | jq -rs '[ flatten[]
               | select(.Service == "node" and .State == "running")
               | .Publishers[]?
               | select(.TargetPort == 8080 and .PublishedPort > 0)
               | .PublishedPort ]
            | unique | sort | map("localhost:\(.)") | join(",")'
