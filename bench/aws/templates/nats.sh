# Role: one server of a three-server NATS core cluster (no JetStream).
mkdir -p /etc/nats
cat >/etc/nats/nats.conf <<'CONF'
server_name: @@SERVER_NAME@@
listen: 0.0.0.0:@@CLIENT_PORT@@
http: 0.0.0.0:@@MONITOR_PORT@@
cluster {
  name: bench
  listen: 0.0.0.0:@@ROUTE_PORT@@
  routes: [@@ROUTES@@]
}
CONF
docker run -d --name nats --restart unless-stopped --network host \
  -v /etc/nats:/etc/nats:ro @@NATS_IMAGE@@ -c /etc/nats/nats.conf
docker run -d --name nats-exporter --restart unless-stopped --network host \
  @@EXPORTER_IMAGE@@ -port @@EXPORTER_PORT@@ -varz -connz -routez -subz http://127.0.0.1:@@MONITOR_PORT@@
touch /var/lib/bench-ready
