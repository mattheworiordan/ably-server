# Role: a load generator or REST publisher box (the ably-loadgen image).
docker pull @@IMAGE@@
docker rm -f loadgen 2>/dev/null || true
docker run -d --name loadgen --restart unless-stopped --network host \
  --ulimit nofile=1048576:1048576 @@IMAGE@@ @@CMD@@
touch /var/lib/bench-ready
