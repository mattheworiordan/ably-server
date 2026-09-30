#!/bin/bash
# bench/aws user-data, common part (Amazon Linux 2023). Rendered by
# lib.sh render_userdata; the role part follows it.
# shellcheck disable=SC2050,SC2157  # template markers are replaced before the script runs
set -euxo pipefail
exec > >(tee -a /var/log/bench-userdata.log) 2>&1

dnf install -y docker chrony jq

# Kernel limits for many connections and a wide ephemeral port range.
cat >/etc/sysctl.d/90-bench.conf <<'SYSCTL'
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_keepalive_time = 120
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
fs.file-max = 2097152
fs.nr_open = 2097152
SYSCTL
sysctl --system

# File descriptors: shells, systemd services and containers.
cat >/etc/security/limits.d/90-bench.conf <<'LIMITS'
* soft nofile 1048576
* hard nofile 1048576
root soft nofile 1048576
root hard nofile 1048576
LIMITS
mkdir -p /etc/systemd/system/docker.service.d /etc/docker
cat >/etc/systemd/system/docker.service.d/90-bench.conf <<'DROPIN'
[Service]
LimitNOFILE=2097152
LimitNPROC=infinity
DROPIN
cat >/etc/docker/daemon.json <<'DAEMON'
{
  "default-ulimits": {"nofile": {"Name": "nofile", "Hard": 1048576, "Soft": 1048576}},
  "log-driver": "local",
  "log-opts": {"max-size": "100m", "max-file": "5"}
}
DAEMON

# Clock: Amazon Time Sync through chrony (one-way latency needs about 1 ms).
systemctl enable --now chronyd
systemctl daemon-reload
systemctl enable --now docker
usermod -aG docker ec2-user

if [ "@@INSTALL_COMPOSE@@" = 1 ]; then
  mkdir -p /usr/local/lib/docker/cli-plugins
  curl -fsSL "https://github.com/docker/compose/releases/download/@@COMPOSE_VERSION@@/docker-compose-linux-x86_64" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
  chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
fi

# Host metrics for Prometheus.
docker run -d --name node-exporter --restart unless-stopped --network host --pid host \
  -v /:/host:ro,rslave @@NODE_EXPORTER_IMAGE@@ \
  --path.rootfs=/host --web.listen-address=:@@NODE_EXPORTER_PORT@@

# Pull from ECR with the instance profile (skipped when no registry is given).
# The instance profile can take a minute to become usable, so retry.
if [ -n "@@ECR_REGISTRY@@" ]; then
  for _ in $(seq 1 12); do
    if aws ecr get-login-password --region @@REGION@@ |
      docker login --username AWS --password-stdin @@ECR_REGISTRY@@; then break; fi
    sleep 10
  done
fi
