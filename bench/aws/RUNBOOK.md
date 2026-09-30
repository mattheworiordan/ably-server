# Runbook: building the scale-proof fleet on AWS

This runbook is for someone with an AWS account who wants to stand up the
test fleet, run a scenario, collect the results and tear everything down.
The scripts are numbered shell scripts in this directory. There is no
Terraform and no orchestrator. Every script creates a resource or reuses the
one that already exists, tags it `Project=$PROJECT_TAG`, records its id in a
state file and appends one line to a log file.

Nothing in this repository names an account, a region or a person. You give
the scripts those values with environment variables (table below).

## 1. What it builds and what it costs

One Availability Zone in the default VPC. One security group (all traffic
inside the group, SSH from your address only). One cluster placement group.
Everything talks over private addresses. There is no load balancer.

| Part | Script | Size at 1x | Size at 2x |
|---|---|---|---|
| ably-server nodes | `40-nodes.sh` | 10 x c7i.2xlarge | 10 to 20 x c7i.2xlarge |
| NATS core cluster | `30-nats.sh` | 3 x c7i.2xlarge | 3 x c7i.2xlarge |
| Postgres | `20-postgres.sh` | RDS for PostgreSQL 17, db.r7g.4xlarge, io2 (one repeat on gp3), Single-AZ | same |
| Load generators | `50-loadgen.sh` | 3 x c7i.8xlarge | 6 x c7i.8xlarge |
| REST publishers | `50-loadgen.sh` | 2 x c7i.4xlarge | 3 x c7i.4xlarge |
| Conductor, Prometheus, Grafana | `50-loadgen.sh`, `55-observability.sh` | 1 x c7i.2xlarge | 1 x c7i.2xlarge |
| pgbench driver (run 0a only) | `25-pgdriver.sh` | 1 x c7i.4xlarge | - |

### Hourly cost (approximate on-demand list prices)

The prices are in `lib.sh` (`price_of`) and can be overridden with
`PRICE_<type>` (dots become underscores, for example `PRICE_c7i_2xlarge=0.36`).
Check them against the AWS pricing pages before you rely on them. The io2
line is the least certain: provisioned IOPS are billed per IOPS-month and
dominate the database price (`IO2_USD_PER_IOPS_MONTH`, `IO2_USD_PER_GB_MONTH`).

| Fleet | What is running | About USD per hour |
|---|---|---|
| Run 0a | RDS (db.r7g.4xlarge, 1000 GB, 20000 io2 IOPS) and the pgbench driver | 5.5 |
| Smoke | 2 nodes, 1 generator, 1 publisher, conductor, 3 NATS, RDS io2 | 9 |
| 1x | 10 nodes, 3 NATS, 3 generators, 2 publishers, conductor, RDS io2 | 15 |
| 2x | 20 nodes, 3 NATS, 6 generators, 3 publishers, conductor, RDS io2 | 25 |
| Run 8 (shards) | the 1x fleet plus up to 3 extra RDS instances | 15 plus 5 per shard |
| Stopped fleet | disks, RDS storage and provisioned IOPS | 0.2 to 4, depending on IOPS |

`bench/aws/cost-estimate.sh` prints the rate of what STATE says is running and
an estimate of the spend so far. The estimate is local arithmetic, not your
bill.

### Safety rails

- `00-preflight.sh` probes the permissions the proof needs, then checks that a
  billing alarm exists (it creates one) and refuses to go on without it.
  The default warning is USD 750 and the cap is USD 1500 (`BUDGET_ALARM_USD`,
  `BUDGET_CAP_USD`).
- `60-run.sh` and `65-run-0a.sh` refuse to start without `RUN_TIME_LIMIT`, and
  refuse when the estimated spend plus the run would pass the cap
  (`OVERRIDE_BUDGET_GUARD=1` exists; use it only after a decision by whoever owns the budget).
- Every run is detached on its box under `timeout(1)`, so a hung run ends. That
  ends the conductor (or pgbench), not the fleet: the generators and publishers
  keep running and billing until you stop them (`AUTO_STOP_AFTER_RUN=1` does it).
- `10` to `50`, `60` and `65` refuse to run until `00-preflight.sh` has passed,
  and `20` to `50` refuse once the spend estimate has reached the cap.
- Dead-man switch: every box is launched to stop (not terminate) itself
  `FLEET_MAX_UPTIME_H` hours (default 10) after it boots; `80-start.sh` arms it
  again. It stops compute only: the database needs `80-stop.sh`.
- `80-stop.sh` between runs, `90-teardown.sh --yes` at the end of every working day.

## 2. Prerequisites

On the machine that runs the scripts: bash 4.4 or newer (on macOS,
`brew install bash`), AWS CLI v2, `jq`, `ssh`, `scp`, `openssl`, `git`,
and Docker with buildx (for `build-push.sh`). `shellcheck` if you run the tests.

In the AWS account, the role you sign in with needs to be allowed to:
create and terminate EC2 instances, security groups, placement groups and key pairs;
create and delete RDS instances, parameter groups and subnet groups (and use Performance Insights);
create ECR repositories and push images;
create a Budget (or a CloudWatch alarm and an SNS topic);
create an IAM role and instance profile (or be given an existing instance
profile with ECR read access in `INSTANCE_PROFILE_NAME`).
`00-preflight.sh` tells you which of these is denied before anything is created.

The account also needs on-demand vCPU quota for the fleet. The 2x fleet is
about 430 vCPUs of the "Running On-Demand Standard instances" quota
(`L-1216C47A`); the 1x fleet is about 240. Preflight reads the quota and
refuses when it is too low. There must be a default VPC in the region, or set
`VPC_ID` and `SUBNET_ID`.

## 3. Environment variables

Copy `env.example` to a file outside the repository, fill it in and load it:

    cp bench/aws/env.example ~/ably-scale.env     # edit it
    set -a; . ~/ably-scale.env; set +a

Required before anything else:

| Variable | Meaning |
|---|---|
| `AWS_ACCOUNT_ID` | The 12 digit account the fleet may use. `00-preflight.sh` stops if your credentials belong to another account. |
| `AWS_REGION` | The region. One region, one zone. |
| `ADMIN_CIDR` | Your public address as a /32 (`$(curl -s https://checkip.amazonaws.com)/32`). The only source allowed to SSH in. `0.0.0.0/0` is refused. |
| `SSH_PUBLIC_KEY_PATH` | Public key to import as the EC2 key pair. The private key is the same path without `.pub` (or set `SSH_PRIVATE_KEY_PATH`). |
| `ALARM_EMAIL` | Receives the budget warnings. |
| `RDS_PASSWORD` | 16 or more characters, letters, digits, `_` and `-` only. Needed by `20-postgres.sh`. Other scripts read the DSN from STATE. |
| `RUN_TIME_LIMIT` | Hard limit for a run: `300`, `45m` or `2h`. Needed by `60-run.sh` and `65-run-0a.sh`. |

Optional, with defaults:

| Variable | Default | Meaning |
|---|---|---|
| `PROJECT_TAG` | `ably-server-scale` | Tag value on every resource and prefix of every name. Teardown deletes by it. |
| `AZ` | `${AWS_REGION}a` | The one Availability Zone. |
| `STATE_FILE` | `~/Workshop/work/research/ably-server-scale-proof-2026-10/STATE.json` | Resource ids, image tags, run records, spend estimate. Keep it out of the repository: it holds the database password and the API key. |
| `LOG_FILE` | `.../LOG.md` next to the state file | One line per completed step. |
| `RESULTS_DIR` | `.../results` next to the state file | Raw results per run. |
| `BUDGET_ALARM_USD`, `BUDGET_CAP_USD` | 750, 1500 | Warning and cap. |
| `BUDGET_SCOPE` | `account` | `account` counts everything in the account (other spend there can trip the alarm). `tag` counts only spend tagged `Project=$PROJECT_TAG`, but sees nothing until the `Project` cost allocation tag is activated in Billing (up to a day). Either way Budgets data lags by hours: the local spend estimate is the day-to-day guard. |
| `BUDGET_METHOD` | `auto` | `auto` uses Budgets when the role may create one, else CloudWatch billing alarms with SNS. `cloudwatch` forces the second. |
| `FLEET_PROFILE` | `2x` | Sizes the vCPU quota check (`2x` or `1x`); vCPUs already in use in the account are subtracted. |
| `FLEET_MAX_UPTIME_H` | 10 | Dead-man switch: each box stops itself this many hours after it boots. `SKIP_REARM=1` makes `80-start.sh` skip re-arming it. |
| `USE_PLACEMENT_GROUP` | 1 | `0` launches without the cluster placement group (for capacity errors). |
| `BILLING_REGION` | `us-east-1` | The one region that holds billing metrics; CloudWatch billing alarms and their SNS topic live there. |
| `ECR_REPO_SERVER`, `ECR_REPO_LOADGEN` | `$PROJECT_TAG/ably-server`, `$PROJECT_TAG/ably-loadgen` | ECR repository names. Teardown with `TEARDOWN_ALL=1` deletes only repositories tagged for the project. |
| `INSTANCE_PROFILE_NAME` | (created) | An existing instance profile with ECR read access. |
| `VPC_ID`, `SUBNET_ID` | (default VPC) | Use these instead of the default VPC. |
| `NODE_COUNT`, `NATS_COUNT`, `LOADGEN_COUNT`, `PUBLISHER_COUNT` | 10, 3, 3, 2 | Fleet sizes. |
| `NODE_INSTANCE_TYPE`, `NATS_INSTANCE_TYPE`, `LOADGEN_INSTANCE_TYPE`, `PUBLISHER_INSTANCE_TYPE`, `CONDUCTOR_INSTANCE_TYPE`, `PGDRIVER_INSTANCE_TYPE` | c7i.2xlarge, c7i.2xlarge, c7i.8xlarge, c7i.4xlarge, c7i.2xlarge, c7i.4xlarge | Instance types. |
| `RDS_INSTANCE_CLASS`, `RDS_STORAGE`, `RDS_STORAGE_GB`, `RDS_IOPS` | db.r7g.4xlarge, io2, 1000, 20000 (io2) | Database size. gp3 uses its baseline unless `RDS_IOPS` is set. |
| `RDS_ENGINE_VERSION` | `17` | Pin an exact minor version (for example `17.x`) before you quote a number. The engine version that was actually created is recorded in STATE. |
| `RDS_FORCE_SSL` | `0` | Plain connections inside the VPC (TLS is out of scope for the proof). `1` requires TLS and switches the DSN to `sslmode=require`. |
| `DB_POOL_SIZE`, `MAX_NODES` | 50, 20 | Size `max_connections` (`MAX_NODES x DB_POOL_SIZE + 300`). |
| `SHARDS` | 1 | Run 8: number of RDS instances per storage type. |
| `ACTIVE_STORAGE` | first created | Which storage type the nodes use (`io2` or `gp3`). |
| `BUS` | `nats` | `nats`, `postgres`, `pgnotify`, or `none` (no `--bus` flag, for the shipped image in run 1a). |
| `SERVER_TAG`, `LOADGEN_TAG` | from STATE | Image tags in ECR. |
| `ABLY_SERVER_EXTRA_FLAGS` | empty | Extra node flags (write path, connection layer). |
| `NODE_GOMAXPROCS`, `NODE_GOMEMLIMIT` | unset, `13GiB` | Go runtime settings on the nodes. |
| `NATS_IP_OFFSET`, `NATS_IMAGE` | 10, `nats:2.11` | NATS servers take the addresses from this offset in the subnet. |
| `LOADGEN_CMD`, `PUBLISHER_CMD`, `CONDUCTOR_CMD` | see section 7 | The commands run in the `ably-loadgen` image. |
| `NODE_VCPU`, `NODE_MEMORY_GB` | from `NODE_INSTANCE_TYPE` | Node size passed to the conductor (`--node-vcpu`, `--node-memory-gb`). |
| `SCENARIO_DIR` | `bench/scenarios` | Where scenario names are looked up. |
| `RECONFIGURE`, `SCALE_DOWN` | 0 | Re-apply the image and flags to live boxes (`40-nodes.sh`, `50-loadgen.sh`), or remove nodes above `NODE_COUNT`. |
| `DURATION_S`, `WARMUP_S`, `CLIENTS`, `VARIANTS`, `CHANNELS`, `PAYLOAD_BYTES` | 60, 10, `1 8 32 128`, all, 200000, 600 | Run 0a settings (`pgbench/run.sh`). |
| `IMAGE_TAG`, `IMAGE_PLATFORM`, `PUSH`, `ALLOW_DIRTY` | git sha, `linux/amd64`, 1, 0 | `build-push.sh`. |
| `KEEP_SNAPSHOT`, `KEEP_KEY_PAIR`, `TEARDOWN_ALL` | 0 | `90-teardown.sh` options. |
| `STOP_RDS` | 1 | `80-stop.sh`: set to 0 to leave Postgres running. |
| `AUTO_STOP_AFTER_RUN`, `OVERRIDE_BUDGET_GUARD`, `FORCE_NO_ALARM` | 0 | See the safety rails. |
| `SKIP_BOOT_WAIT`, `SKIP_OBSERVABILITY`, `KEEP_WORK_DIR` | 0 | Skip waiting for cloud-init, skip `55-observability.sh`, keep rendered user-data for inspection. |
| `DRY_RUN` | 0 | `1` prints every aws, ssh and scp call and runs nothing. |

## 4. Sign in

Export credentials for the account in `AWS_ACCOUNT_ID`, or set `AWS_PROFILE`.
Check them:

    aws sts get-caller-identity --query Account --output text

Credentials that expire (SSO sessions last hours) make the next script fail
with an expired-token error. Sign in again and re-run the script: every step
is safe to repeat.

> **Ably internal.** Install and set up `ablyctl`:
>
>     gh release download -R ably/infrastructure -p ablyctl-darwin-arm64 -O ~/.local/bin/ablyctl --clobber
>     chmod +x ~/.local/bin/ablyctl
>     ablyctl init          # writes the config and offers automatic_update; `ablyctl update` self-updates later
>
> Sign in and load credentials into the shell:
>
>     eval "$(ablyctl aws env --account dev)"
>
> The first use starts an SSO device-code login that you finish in a browser.
> `ablyctl aws env --unset` clears the credentials. The default role is
> `Operator`; `--aws-role` overrides it. `AWS_SSO_ROLE` and `ABLYCTL_ACCOUNT`
> override the role and the account name that the scripts pass.
>
> **Running under an AI agent.** `ablyctl` detects agents (in
> `go/tools/ablyctl/lib/sso/sso.go` of `ably/infrastructure`): when `CLAUDECODE`,
> `CODEX_CI`, `CODEX_SANDBOX`, `CURSOR_AGENT` or `GEMINI_CLI` is set it ignores
> `--aws-role` and chains the operator session into the `AgentOperator` IAM role
> of the target account. That role is not provisioned in the dev account today,
> so `ablyctl aws env --account dev` run by an agent fails with an STS
> AccessDenied. There are two supported paths: (a) infrastructure provisions
> `AgentOperator` in the dev account, or (b) the operator mints credentials in
> their own terminal with the command above and exports `AWS_ACCESS_KEY_ID`,
> `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` in the shell that runs the
> scripts. Do not unset the agent variables to get around the detection.
>
> `lib.sh` sources `ably-internal.sh`, which runs the `eval` line when `ablyctl`
> is on PATH and no `AWS_ACCESS_KEY_ID` or `AWS_PROFILE` is set. If it cannot
> mint credentials (expired sign-in, or the agent case above) the script prints
> both paths and exits. Do not put the account id or the permission set name in
> any file in this repository: take them from the environment.
>
> `ablyctl aws ecr env` prints the registry of ablyctl's default ECR account,
> which may not be the account the fleet runs in. `00-preflight.sh` creates the
> two repositories in the working account and `build-push.sh` derives
> `ECR_REGISTRY` from `AWS_ACCOUNT_ID` and `AWS_REGION`, so you do not need it.

## 5. Step by step

Every command is run from the repository root. Each script is idempotent:
run it again if it stops halfway.

### 5.0 Check the tooling without AWS

    bench/aws/test/run-all.sh           # shellcheck, unit tests, dry-run call sequence, local Postgres and Prometheus tests
    SKIP_DOCKER=1 bench/aws/test/run-all.sh

To see what a script would do in your account, without doing it:

    DRY_RUN=1 bench/aws/00-preflight.sh

A dry run writes a scratch state file (`$TMPDIR/<tag>-dryrun-STATE.json`,
`DRY_STATE_FILE` overrides it), never `STATE_FILE`, and does not touch `LOG_FILE`.

### 5.1 Preflight, network, images

    bench/aws/00-preflight.sh       # credentials, account, permissions, vCPU quota, ECR repositories, billing alarm
    bench/aws/10-network.sh         # security group, placement group, key pair, instance profile
    bench/aws/build-push.sh         # builds ably-server and ably-loadgen for linux/amd64 and pushes them

`build-push.sh` refuses to push from a dirty working tree, so a pushed tag
always names real commits. Without valid credentials it builds locally and
skips the push. To build the shipped baseline for run 1a:

    git checkout main
    IMAGE_TAG=main-$(git rev-parse --short=7 HEAD) bench/aws/build-push.sh server

Confirm the budget e-mail subscription that AWS sends to `ALARM_EMAIL`.

### 5.2 Run 0a: Postgres alone (first real numbers, before any node exists)

    RDS_STORAGE=io2 bench/aws/20-postgres.sh          # about 15 minutes to create
    bench/aws/25-pgdriver.sh
    RUN_TIME_LIMIT=1h bench/aws/65-run-0a.sh io2      # about 40 minutes
    RDS_STORAGE=gp3 bench/aws/20-postgres.sh          # a second instance; both exist side by side
    RUN_TIME_LIMIT=1h bench/aws/65-run-0a.sh gp3
    bench/aws/pgbench/summarise.sh "$RESULTS_DIR"/0a-io2-*/out/results-io2.csv "$RESULTS_DIR"/0a-gp3-*/out/results-gp3.csv > summary-0a.md

The variants, what each measures, and the pass check for the SQL itself are
in `pgbench/run.sh` and `pgbench/*.sql`. The numbers to read: the `trivial`
row at one client is the ACK latency floor; `shipped` is the current write
path; `batch30` and `batch100` show how far batching moves one primary. The
summary also prints WAL bytes per message.

Stop or delete what you do not need next: `bench/aws/80-stop.sh`, or
`bench/aws/90-teardown.sh --yes` to delete everything.

### 5.3 The fleet

    RDS_STORAGE=io2 bench/aws/20-postgres.sh          # skip if it already exists
    bench/aws/30-nats.sh                              # three NATS servers on fixed addresses
    BUS=nats bench/aws/40-nodes.sh                    # NODE_COUNT nodes
    bench/aws/50-loadgen.sh                           # generators, publishers, conductor, Prometheus, Grafana

`40-nodes.sh` needs a server image that knows `--bus` and `--nats-url` (the
integration branch). Boxes take about five minutes to finish cloud-init; the
scripts wait for it (`SKIP_BOOT_WAIT=1` to skip).

Check the fleet is up:

    jq '.instances | to_entries[] | {name: .key, role: .value.role, ip: .value.private_ip}' "$STATE_FILE"
    bench/aws/cost-estimate.sh

### 5.4 Smoke (run 0b)

Smoke runs the real stack at 1 percent of the 1x target (about 5000
connections and 500 publishes a second), then at 10 percent. It is scenario
files `smoke-1pct` and `smoke-10pct` run through `60-run.sh`. The scenario
files belong to the load generator branch (`bench/scenarios/*.toml`).

    RUN_TIME_LIMIT=20m bench/aws/60-run.sh smoke-1pct
    RUN_TIME_LIMIT=30m bench/aws/60-run.sh smoke-10pct

**Pass check.** Do not start a bigger run until each smoke run shows all of
these (the conductor's summary in `results/<run-id>/` has the numbers):

1. The conductor exited 0 and `60-run.sh` logged `ok`.
2. Every scrape target in Prometheus is up (`sum by (job) (up)` equals the number of boxes per job).
3. `ably_connections_open` reached the target count within one percent and stayed there.
4. No loss, duplicate or reorder on the serial-continuity sample, and no mismatch in the exact-once slice.
5. REST publish ACK p99 at or under 100 ms; delivery latency p50 at or under 50 ms and p99 at or under 250 ms.
6. No node restarted: `ssh` to a node and `docker ps` shows the same uptime as the box.
7. No ERROR lines in the node logs that the scenario did not cause (`70-collect.sh` copies them).
8. Memory is flat after the ramp (Grafana, "Node process memory").

Record the result in `LOG_FILE` and start the next run only when all eight hold.

### 5.5 Runs

The runs are defined by the plan section 8. Each one is a scenario file plus
these settings; change the settings, then run the scenario:

| Run | Settings before `60-run.sh <scenario>` |
|---|---|
| 1a shipped bus | build the `main` image (5.1), then `BUS=none SERVER_TAG=<main tag> RECONFIGURE=1 bench/aws/40-nodes.sh` |
| 1b small size | `BUS=postgres RECONFIGURE=1 bench/aws/40-nodes.sh` |
| 2, 3 large size | `BUS=nats RECONFIGURE=1 bench/aws/40-nodes.sh` (raise `NODE_COUNT` for 2x, and `LOADGEN_COUNT`, `PUBLISHER_COUNT` with `50-loadgen.sh`) |
| 4 storage | `RDS_STORAGE=gp3 ACTIVE_STORAGE=gp3 bench/aws/20-postgres.sh`, then `RECONFIGURE=1 bench/aws/40-nodes.sh` |
| 5 failure injection | the scenario kills a node or a NATS server through the conductor; to do it by hand, `aws ec2 stop-instances` on one tagged box |
| 6 presence | the presence scenario, `RUN_TIME_LIMIT=30m` |
| 7 node curve | `NODE_COUNT=5 SCALE_DOWN=1 bench/aws/40-nodes.sh`, then 10, then 20; re-run `55-observability.sh` after each change |
| 8 shard curve | `SHARDS=3 RDS_STORAGE=io2 bench/aws/20-postgres.sh` and `RECONFIGURE=1` with the sharding flag in `ABLY_SERVER_EXTRA_FLAGS` |

Watch a run from your machine through an SSH tunnel (the UIs listen on the
conductor's loopback only):

    ssh -i "${SSH_PRIVATE_KEY_PATH:-${SSH_PUBLIC_KEY_PATH%.pub}}" -L 3000:127.0.0.1:3000 -L 9090:127.0.0.1:9090 \
      ec2-user@"$(jq -r ".instances[\"${PROJECT_TAG:-ably-server-scale}-conductor-1\"].public_ip" "$STATE_FILE")"
    # Grafana:    http://localhost:3000  (user admin, password: jq -r .grafana_password "$STATE_FILE")
    # Prometheus: http://localhost:9090

### 5.6 Collect

    bench/aws/70-collect.sh <run-id>

It gathers a Prometheus snapshot and range exports of the series in
`observability/export-series.txt`, container logs and docker stats samples
from every box, `pg_stat_statements` from each database, RDS Performance
Insights load, and a copy of STATE with the secrets removed, into
`results/<run-id>/collect/`. Each step is best effort and logs a warning when
it cannot finish. Results and raw logs stay out of the repository.

### 5.7 Stop, start, teardown

    bench/aws/80-stop.sh            # stop every instance and the database (disks and storage still bill)
    bench/aws/80-start.sh           # start them again; private addresses stay, public ones change
    bench/aws/90-teardown.sh --yes  # delete everything tagged Project=$PROJECT_TAG, then verify

RDS restarts by itself after seven days stopped. The budget and the ECR
repositories survive a teardown on purpose (set `TEARDOWN_ALL=1` to remove
them). `KEEP_SNAPSHOT=1` takes a final database snapshot.

End every working day with `90-teardown.sh --yes`, or write in `LOG_FILE`
that the fleet is up, what it costs an hour and why.

## 6. If something is left running

1. Run `bench/aws/90-teardown.sh --yes`. It deletes by tag and by what STATE lists, so it works even when STATE is lost.
2. If it reports something still present, read the line: a security group that will not delete is usually waiting for a network interface to detach; wait a minute and run it again.
3. Without the scripts (or to double-check), list everything tagged for the project:

       aws resourcegroupstaggingapi get-resources --tag-filters Key=Project,Values="${PROJECT_TAG:-ably-server-scale}" \
         --query 'ResourceTagMappingList[].ResourceARN' --output text

   Terminated instances can stay in that list for about an hour.
4. To delete by hand: `aws ec2 terminate-instances`, `aws rds delete-db-instance --skip-final-snapshot --delete-automated-backups`, then the security group, placement group and key pair.
5. A resource with no tag is not found by any of this. The scripts tag every
   instance, volume, security group, placement group, key pair, RDS instance,
   parameter group, subnet group and IAM role they create.

## 7. The load generator commands

`50-loadgen.sh` and `60-run.sh` run commands inside the `ably-loadgen` image.
The defaults match the load generator's flags:

| Variable | Default |
|---|---|
| `LOADGEN_CMD` | `ably-loadgen serve --listen=:9200 --metrics-listen=:9101 --role=generator` |
| `PUBLISHER_CMD` | `ably-loadgen serve --listen=:9200 --metrics-listen=:9101 --role=publisher` |
| `CONDUCTOR_CMD` | `ably-conductor run --scenario /run-input/{SCENARIO} --inventory /run-input/inventory.json --results /results --run-id {RUN_ID} --node-vcpu $NODE_VCPU --node-memory-gb $NODE_MEMORY_GB` |

`--role` is `generator`, `publisher` or `all`. The conductor also takes
`--fault-hook CMD --fault-at D --time-limit D --log --state`; add them by
setting `CONDUCTOR_CMD` (the tokens `{SCENARIO}` and `{RUN_ID}` are replaced).
`NODE_VCPU` and `NODE_MEMORY_GB` default from `NODE_INSTANCE_TYPE` (vCPUs from
the size, memory at 2 GiB per vCPU, which holds for the c7i family); set them
for any other family. With an explicit `--run-id` the conductor writes straight
into `--results`, which is the mounted directory, so `60-run.sh` copies that
directory back as the run's results.

Scenarios are TOML files in `bench/scenarios/` (`shape-f`, `shape-m`,
`shape-d`, `presence-m`, `smoke-1pct`, `smoke-10pct`, `idle-connections`).
Pass the name with or without `.toml`.

Port 9100 is taken by node-exporter on every box (all containers use host
networking), so the agent listens on `LOADGEN_AGENT_PORT` (default 9200).
`lib.sh` stops with an error if two ports on one kind of box are the same.

`60-run.sh` writes `inventory.json` (nodes with their HTTP, WebSocket and
metrics addresses, NATS URLs, generator and publisher agent addresses, the
Postgres shards without credentials, the API key) and the scenario file into
`/run-input` on the conductor, and mounts an empty `/results`. The Prometheus
scrape config assumes the generators serve metrics on port 9101.

## 8. What the boxes run (for reproducing)

Amazon Linux 2023 (the latest AMI from the public SSM parameter, pinned in STATE
on first use), Docker from the distribution, chrony with Amazon Time Sync,
`fs.nr_open` and `nofile` at 1M, the ephemeral port range 1024 to 65535.
Images: `nats:2.11`, `natsio/prometheus-nats-exporter:0.15.0`,
`prom/node-exporter:v1.8.2`, `prom/prometheus:v2.55.1`, `grafana/grafana:11.3.0`,
`quay.io/prometheuscommunity/postgres-exporter:v0.15.0`, `postgres:17-alpine`
(pgbench and psql), plus the two images this repository builds. The Postgres
parameters (`synchronous_commit=on`, `log_min_duration_statement=50`,
`pg_stat_statements`, `max_connections`) are set in `20-postgres.sh`. The
instance types, versions and flags of every run are recorded in STATE
(`.deployment`, `.loadgen`, `.postgres`, `.runs`).

## 9. Troubleshooting

- **`InsufficientInstanceCapacity` in a cluster placement group.** Retry, or start the fleet in an order that launches the biggest boxes first, or remove the placement group from STATE and the scripts (the latency difference is small).
- **A box is not reachable over SSH.** Check `ADMIN_CIDR` still matches your address (it changes with the network you are on) and re-run `10-network.sh` after fixing it; the rule is added, not replaced, so remove the old rule by hand.
- **A box came up but the container is not running.** `ssh` in and read `/var/log/bench-userdata.log` and `docker logs`. `RECONFIGURE=1` re-applies the role part.
- **ECR pull denied.** The instance profile is missing or not yet propagated; wait a minute, then `RECONFIGURE=1 bench/aws/40-nodes.sh`.
- **`00-preflight.sh` reports a denied action.** Sign in with a role that has it, or ask for it. Nothing was created.
- **`10-network.sh` or `30-nats.sh` fails on an address.** NATS servers take fixed private addresses from `NATS_IP_OFFSET` in the subnet; if another instance already holds one, pick another offset.
- **The database password and the API key are in EC2 user-data.** Anyone who can describe instance attributes in the account can read them, and they appear in `docker inspect` on the box. The bench database is throwaway and only reachable inside the security group; do not reuse the password anywhere.
- **Teardown says a read failed.** Teardown refuses to treat a failed read as "nothing there". Fix the sign-in or permission and run it again; STATE is kept until it verifies clean.
- **STATE.json was lost.** `90-teardown.sh --yes` still works. To continue instead, re-run `10-network.sh`, `20-postgres.sh` (with `RDS_PASSWORD`), `30-nats.sh`, `40-nodes.sh` and `50-loadgen.sh`: they find existing resources by tag and name and refill STATE.
