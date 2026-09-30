#!/usr/bin/env bash
#
# run.sh — run the connection-layer soak (internal/realtime/soak_test.go,
# DESIGN.md §5.2) inside a golang:1.26 Linux container.
#
# The soak holds tens of thousands of loopback WebSocket connections. On
# the host that exhausts the ephemeral port range and breaks every other
# loopback test on the machine, so it runs only here: the container has
# its own network namespace, port range and file-descriptor limit. The
# test itself skips unless SOAK_IN_CONTAINER=1, which this script sets.
#
# Usage (from anywhere in the repo):
#   bench/soak/run.sh                       # 50k connections, 60s hold
#   SOAK_CONNS=5000 SOAK_HOLD=10s bench/soak/run.sh
#
# Environment passed through: SOAK_CONNS (default 50000), SOAK_CHANNELS
# (1000), SOAK_HOLD (60s), SOAK_PUBLISH_EVERY (5s), SOAK_TIMEOUT (go test
# -timeout, default 40m). The JSON summary is written to
# bench/soak/results/soak-<UTC time>.json in the checkout.
#
# Needs Docker with enough memory for the server and client processes
# (about 3 GiB at 50k connections). Go modules are cached in the
# ably-server-soak-gomod volume between runs.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="${SOAK_IMAGE:-golang:1.26}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$ROOT/bench/soak/results"

exec docker run --rm \
	--ulimit nofile=1048576:1048576 \
	-v "$ROOT":/src \
	-v ably-server-soak-gomod:/go/pkg/mod \
	-w /src \
	-e SOAK_IN_CONTAINER=1 \
	-e SOAK_CONNS="${SOAK_CONNS:-50000}" \
	-e SOAK_CHANNELS="${SOAK_CHANNELS:-1000}" \
	-e SOAK_HOLD="${SOAK_HOLD:-60s}" \
	-e SOAK_PUBLISH_EVERY="${SOAK_PUBLISH_EVERY:-5s}" \
	-e SOAK_OUT="/src/bench/soak/results/soak-$STAMP.json" \
	-e GOFLAGS=-buildvcs=false \
	"$IMAGE" \
	go test -tags=soak -run '^TestSoakConnections$' -count=1 \
		-timeout "${SOAK_TIMEOUT:-40m}" -v ./internal/realtime/
