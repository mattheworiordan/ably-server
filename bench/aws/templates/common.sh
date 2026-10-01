#!/bin/bash
# bench/aws user-data, common part (Amazon Linux 2023). Rendered by
# lib.sh render_userdata; the role part follows it.
# shellcheck disable=SC2050,SC2157,SC2194  # template markers are replaced before the script runs
set -euxo pipefail
install -m 600 /dev/null /var/log/bench-userdata.log
exec > >(tee -a /var/log/bench-userdata.log) 2>&1

# SSH access. There is no EC2 key pair in this account: authorise the
# operator's public key for ec2-user here.
install -d -m 700 -o ec2-user -g ec2-user /home/ec2-user/.ssh
echo '@@SSH_PUBKEY@@' >>/home/ec2-user/.ssh/authorized_keys
chmod 600 /home/ec2-user/.ssh/authorized_keys
chown ec2-user:ec2-user /home/ec2-user/.ssh/authorized_keys
restorecon -R /home/ec2-user/.ssh 2>/dev/null || true

# Dead-man switch: the box powers itself off after this many minutes. It is
# launched with terminate-on-shutdown, so a forgotten fleet stops billing
# for compute and disks alike (nothing can be restarted afterwards).
shutdown -h +@@MAX_UPTIME_MIN@@

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
# Connection tracking: the default table (262,144) capped a load generator at
# 262k connections per box ("nf_conntrack: table full, dropping packet").
# Sized for 1M+ connections per box; the hashsize follows below.
net.netfilter.nf_conntrack_max = 4194304
SYSCTL
sysctl --system
# nf_conntrack is loaded lazily; load it so the sysctl applies and size its
# hash table (otherwise the default 16k buckets make lookups slow at 4M).
modprobe nf_conntrack 2>/dev/null || true
sysctl -w net.netfilter.nf_conntrack_max=4194304 >/dev/null 2>&1 || true
echo 524288 >/sys/module/nf_conntrack/parameters/hashsize 2>/dev/null || true

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
# The packaged unit on Amazon Linux 2023 starts dockerd with
# --default-ulimit nofile=32768:65536 on its command line. dockerd refuses to
# start when the same setting is also in daemon.json, so the container default
# is raised by rewriting the packaged ExecStart in a drop-in instead. The
# packaged line is read at boot so this stays correct across package versions
# and on distributions whose unit has no such flag (the flag is then appended).
docker_exec=$(systemctl cat docker 2>/dev/null | grep '^ExecStart=' | tail -n 1 \
  | sed -E 's/ --default-ulimit[= ]nofile=[0-9]+:[0-9]+//')
if [ -n "$docker_exec" ]; then
  cat >/etc/systemd/system/docker.service.d/91-bench-ulimit.conf <<DROPIN
[Service]
ExecStart=
${docker_exec} --default-ulimit nofile=1048576:1048576
DROPIN
fi
cat >/etc/docker/daemon.json <<'DAEMON'
{
  "log-driver": "local",
  "log-opts": {"max-size": "100m", "max-file": "5"}
}
DAEMON

# Clock: Amazon Time Sync through chrony (one-way latency needs about 1 ms).
systemctl enable --now chronyd
systemctl daemon-reload
systemctl enable --now docker
# The scripts drive Docker over SSH as ec2-user (pg_isready, inspect, the
# pgbench driver), so it needs the docker group; each SSH call is a new
# session, so the membership applies immediately.
usermod -aG docker ec2-user
usermod -aG docker ec2-user

if [ "@@INSTALL_COMPOSE@@" = 1 ]; then
  mkdir -p /usr/local/lib/docker/cli-plugins
  curl -fsSL "https://github.com/docker/compose/releases/download/@@COMPOSE_VERSION@@/docker-compose-linux-x86_64" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
  chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
fi

# Image registry login. ecr: the instance profile (it can take a minute to become
# usable, so retry). ghcr: public packages need no login; a pull token is used only
# when one was given (it is in this user-data: RUNBOOK section 3). none: nothing.
case "@@REGISTRY_KIND@@" in
  ecr)
    for _ in $(seq 1 12); do
      if aws ecr get-login-password --region @@REGION@@ |
        docker login --username AWS --password-stdin @@REGISTRY_HOST@@; then break; fi
      sleep 10
    done
    ;;
  ghcr)
    if [ -n "@@GHCR_PULL_TOKEN@@" ]; then
      set +x # keep the token out of the boot log
      printf '%s' '@@GHCR_PULL_TOKEN@@' | docker login ghcr.io -u '@@GHCR_PULL_USER@@' --password-stdin
      set -x
    fi
    ;;
esac

# Host metrics for Prometheus.
docker run -d --name node-exporter --restart unless-stopped --network host --pid host \
  -v /:/host:ro,rslave @@NODE_EXPORTER_IMAGE@@ \
  --path.rootfs=/host --web.listen-address=:@@NODE_EXPORTER_PORT@@
