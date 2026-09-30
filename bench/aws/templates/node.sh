# Role: one ably-server node.
docker pull @@IMAGE@@
docker rm -f ably-server 2>/dev/null || true
docker run -d --name ably-server --restart unless-stopped --network host \
  --ulimit nofile=1048576:1048576 @@ENV_ARGS@@ \
  -e 'ABLY_SERVER_KEYS=@@API_KEY@@' \
  -e 'ABLY_SERVER_POSTGRES_DSN=@@DSN@@' \
  @@IMAGE@@ --mode=cluster --listen=:@@PORT@@ --debug-listen=:@@DEBUG_PORT@@ \
  --log-level=info --log-format=json @@FLAGS@@
for _ in $(seq 1 90); do
  if curl -sf "http://127.0.0.1:@@PORT@@/readyz" >/dev/null; then break; fi
  sleep 2
done
touch /var/lib/bench-ready
