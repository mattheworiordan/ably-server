#!/usr/bin/env bash
# Run the linux/arm64 build of ably-bench inside a container attached to a
# compose network, so the load generator talks to the nodes over the Docker
# bridge and not through Docker Desktop's macOS-side port proxy (which
# saturates several cores at high message rates).
#
#   DOCKER_BENCH_NET=ablyscale_default DOCKER_BENCH_BIN=/path/to/ably-bench-linux \
#     bench/docker-bench.sh --endpoints 172.19.0.3:8080,172.19.0.4:8080 --rate 1000 ...
#
# Use it as BENCH_BIN for bench/run-search.sh. Build the binary with:
#   GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o ably-bench-linux ./cmd/ably-bench
set -euo pipefail
: "${DOCKER_BENCH_NET:?set DOCKER_BENCH_NET}"
: "${DOCKER_BENCH_BIN:?set DOCKER_BENCH_BIN}"
exec docker run --rm --name "${DOCKER_BENCH_NAME:-ablybench}" --network "$DOCKER_BENCH_NET" \
  -v "$DOCKER_BENCH_BIN:/usr/local/bin/ably-bench:ro" alpine:3.20 ably-bench "$@"
