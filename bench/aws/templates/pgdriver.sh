# Role: the pgbench driver box for run 0a.
mkdir -p /opt/bench/pgbench
chown -R ec2-user:ec2-user /opt/bench
docker pull @@PGBENCH_IMAGE@@
touch /var/lib/bench-ready
