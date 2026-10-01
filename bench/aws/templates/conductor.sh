# Role: the conductor, Prometheus and Grafana box.
mkdir -p /opt/bench/observability /opt/bench/run /opt/bench/results
chown -R ec2-user:ec2-user /opt/bench
# The DSNs (with the database password) arrive over SSH, not in user-data;
# loaded before the pulls so the delivered file is deleted first.
bench_load_secrets
docker pull @@IMAGE@@
docker pull @@PGBENCH_IMAGE@@
# pg_stat_statements needs CREATE EXTENSION once per database; retry while RDS settles.
set +x # the DSNs carry the database password
IFS=, read -r -a dsns <<<"$BENCH_DSNS"
unset BENCH_DSNS
for dsn in "${dsns[@]}"; do
  for _ in $(seq 1 30); do
    if docker run --rm @@PGBENCH_IMAGE@@ psql "$dsn" -c 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements'; then break; fi
    sleep 10
  done
done
set -x
touch /var/lib/bench-ready
