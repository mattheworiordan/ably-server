# Role: one ably-server node.
docker pull @@IMAGE@@
docker rm -f ably-server 2>/dev/null || true
set +x # the next command carries the database password and the API key
docker run -d --name ably-server --restart unless-stopped --network host \
  --ulimit nofile=1048576:1048576 @@ENV_ARGS@@ \
  -e 'ABLY_SERVER_KEYS=@@API_KEY@@' \
  -e 'ABLY_SERVER_POSTGRES_DSN=@@DSN@@' \
  @@IMAGE@@ --mode=cluster --listen=:@@PORT@@ --debug-listen=:@@DEBUG_PORT@@ \
  --log-level=info --log-format=json @@FLAGS@@
set -x
ready=0
for _ in $(seq 1 90); do
  if curl -sf "http://127.0.0.1:@@PORT@@/readyz" >/dev/null; then
    ready=1
    break
  fi
  sleep 2
done
if [ "$ready" != 1 ]; then
  docker logs ably-server 2>&1 | tail -30
  echo "ably-server did not become ready in 3 minutes" >&2
  exit 1
fi
touch /var/lib/bench-ready
