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
inside the group, SSH from your address only). No placement group unless you
ask for one (`USE_PLACEMENT_GROUP=1`; some roles may not create it).
Everything talks over private addresses. There is no load balancer and there
is no EC2 key pair: each box gets your public key through its user-data.

| Part | Script | Size at 1x | Size at 2x |
|---|---|---|---|
| ably-server nodes | `40-nodes.sh` | 10 x c7i.2xlarge | 10 to 20 x c7i.2xlarge |
| NATS core cluster | `30-nats.sh` | 3 x c7i.2xlarge | 3 x c7i.2xlarge |
| Postgres | `20-postgres.sh` | 1 x r7i.4xlarge (16 vCPU, 128 GB) with a 1000 GB io2 data volume, PostgreSQL 17 in Docker (one repeat on gp3) | same |
| Load generators | `50-loadgen.sh` | 3 x c7i.8xlarge | 6 x c7i.8xlarge |
| REST publishers | `50-loadgen.sh` | 2 x c7i.4xlarge | 3 x c7i.4xlarge |
| Conductor, Prometheus, Grafana | `50-loadgen.sh`, `55-observability.sh` | 1 x c7i.2xlarge | 1 x c7i.2xlarge |
| pgbench driver (run 0a only) | `25-pgdriver.sh` | 1 x c7i.4xlarge | - |

| Count | 1x | 2x | What it is |
|---|---|---|---|
| Instances | 20 (plus 1 for run 0a) | 34 (plus 1) | 10 + 3 + 1 + 3 + 2 + 1 for 1x; 20 + 3 + 1 + 6 + 3 + 1 for 2x; the repeat on gp3 and run 8 add Postgres instances |
| vCPUs | about 290 | about 480 | including the driver and two Postgres instances |

### Postgres runs on EC2 here, not on RDS

The plan named RDS for PostgreSQL. The account's permission set for this work
denies every `rds:*` action (even the read-only ones), so `20-postgres.sh`
runs PostgreSQL 17 (the official `postgres:17` image, host networking) in
Docker on one r7i.4xlarge with its data on a separate EBS volume formatted as
xfs. The database listens on its private address only and accepts
scram-sha-256 logins from the subnet. Its settings are rendered from
`templates/postgresql.conf` (`synchronous_commit=on`, statements over 50 ms
logged, `pg_stat_statements` preloaded, `wal_compression=on`,
`checkpoint_timeout=15min`, `max_wal_size=16GB`, `shared_buffers` 25 percent
and `effective_cache_size` 75 percent of RAM, `max_connections` from
`MAX_NODES x DB_POOL_SIZE + 300`). The settings in force are read back from the
server and recorded in STATE for every database that is created.

What an operator should read across to RDS:

- Carries over. EBS io2 and gp3 are the storage RDS for PostgreSQL itself uses,
  so the commit latency and the IOPS ceiling measured here are those of EBS
  plus Postgres. The instance has the same vCPU and memory as the
  db.r7g.4xlarge the plan named (Intel, since the images and the AMI are amd64,
  instead of Graviton).
  Durability settings, WAL and checkpoint settings are what an RDS parameter
  group would carry.
- Does not carry over. There is no managed failover, backup or patching (the
  plan already states that a Multi-AZ failover is stated, not tested). RDS
  applies its own defaults to settings this file does not name; the file here
  lists every non-default value, and `70-collect.sh` copies it into the results.
  There is no Performance Insights: use `pg_stat_statements` and the
  postgres_exporter series instead.
- Two choices that matter for the comparison. gp3 is provisioned at 12000 IOPS
  and 500 MB/s (`PG_IOPS`, `PG_THROUGHPUT_MBPS`), the baseline RDS documents for a
  gp3 volume of 400 GB and over, not the EBS default of 3000 IOPS and 125 MB/s
  (which would make gp3 look far worse than it is on RDS). The r7i.4xlarge can
  drive at most 40000 IOPS and 10000 Mbps to EBS with a baseline of 20000 IOPS
  (`aws ec2 describe-instance-types`, read when this was written), so the
  20000 IOPS io2 volume sits at the instance baseline.

### No stop and start: terminate and re-create

The role may not stop or start instances, and it may not delete a standalone
volume. So every EBS volume is created inside the `RunInstances` call with
`DeleteOnTermination`, instances are launched with terminate-on-shutdown, and
"between runs" means terminate and re-create: `80-terminate.sh --yes`, then run
`20`, `25`, `30`, `40`, `50` again (about ten minutes). Everything on a box,
the Postgres data included, goes when its instance does: run `70-collect.sh`
first. This also means nothing bills while the fleet is down.

### Hourly cost (approximate on-demand list prices)

The prices are in `lib.sh` (`price_of`) and can be overridden with
`PRICE_<type>` (dots become underscores, for example `PRICE_c7i_2xlarge=0.36`).
They are us-east-1 list prices; eu-west-1 is usually about 10 percent higher
(`PRICE_FACTOR=1.1` scales every price). Check them against the AWS pricing
pages before you rely on them. The role cannot read Cost Explorer or the
pricing API, so these are estimates, not your bill. EBS: io2 bills provisioned
IOPS per month and dominates the database volume price
(`IO2_USD_PER_IOPS_MONTH`, `IO2_USD_PER_GB_MONTH`); gp3 bills IOPS above 3000 and
throughput above 125 MB/s.

| Fleet | What is running | About USD per hour |
|---|---|---|
| Run 0a | r7i.4xlarge with 1000 GB io2 at 20000 IOPS (3.0) and the pgbench driver | 3.7 |
| Run 0a, io2 and gp3 together | the above plus a second r7i.4xlarge with 1000 GB gp3 (12000 IOPS, 500 MB/s) | 5.0 |
| Smoke | 2 nodes, 1 generator, 1 publisher, conductor, 3 NATS, Postgres io2 | 7.3 |
| 1x | 10 nodes, 3 NATS, 3 generators, 2 publishers, conductor, Postgres io2 | 13.8 |
| 2x | 20 nodes, 3 NATS, 6 generators, 3 publishers, conductor, Postgres io2 | 22.4 |
| Run 8 (shards) | the 1x fleet plus up to 3 extra Postgres instances | 13.8 plus 3.0 per shard |
| Fleet terminated | nothing (each volume is deleted with its instance) | 0 |

Against the RDS plan, run 0a costs about 1.8 USD an hour less (the old figure
was 5.5) and the 1x fleet about 1.2 less (15 before), because an r7i.4xlarge
plus an EBS io2 volume is cheaper than db.r7g.4xlarge plus RDS io2 storage.
The 60 GB root volume of each box (about 0.007 USD an hour) is not counted.

`bench/aws/cost-estimate.sh` prints the rate of what is running (it asks EC2
which boxes still exist, because the dead-man switch terminates boxes behind
your back) and an estimate of the spend so far. The estimate is local
arithmetic, not your bill.

### Safety rails

The guards that act are local: the spend estimate with `spend_gate` and
`budget_guard`, and `FLEET_MAX_UPTIME_H`. The billing alarm is a late backstop.

- **The local estimate is the primary spend guard.** Every script that adds to
  the fleet refuses once the estimated spend has reached `BUDGET_CAP_USD`;
  `60-run.sh` and `65-run-0a.sh` refuse when the estimate plus the run would pass it
  (`OVERRIDE_BUDGET_GUARD=1` exists; use it only after a decision by whoever owns the budget).
- **Dead-man switch: `FLEET_MAX_UPTIME_H` (default 10).** Every box powers itself
  off that many hours after it boots. Boxes are launched to terminate on shutdown,
  so a forgotten fleet stops billing for compute and disks alike. It also ends a
  run that needed longer, Postgres included: set the value for the day you plan.
- **Billing alarm.** `00-preflight.sh` creates CloudWatch billing alarms (with an
  SNS topic) at `BUDGET_ALARM_USD` (default 750) and `BUDGET_CAP_USD` (1500) and refuses to go
  on without a way to make them (`BUDGET_METHOD=cloudwatch`, the default). The
  role can create an AWS Budget but cannot read one (no `budgets:ViewBudget`), so
  `BUDGET_METHOD=budgets` creates it and cannot confirm it; preflight says so. Billing
  data lags by hours and the metric needs "Receive Billing Alerts" switched on in the
  account's billing preferences, so treat the alarm as a backstop: until its first data
  point it shows INSUFFICIENT_DATA. Confirm the SNS subscription e-mail.
- Every run is detached on its box under `timeout(1)`, so a hung run ends. That
  ends the conductor (or pgbench), not the fleet: the generators and publishers
  keep running and billing until you terminate them (`AUTO_TERMINATE_AFTER_RUN=1` collects the run and then terminates everything).
- `10` to `50`, `60` and `65` refuse to run until `00-preflight.sh` has passed.
- `80-terminate.sh --yes` between runs, `90-teardown.sh --yes` at the end of every working day.

## 2. Prerequisites

On the machine that runs the scripts: bash 4.4 or newer (on macOS,
`brew install bash`), AWS CLI v2, `jq`, `ssh`, `scp`, `openssl`, `git`,
and Docker with buildx (for `build-push.sh`). `shellcheck` if you run the tests.

In the AWS account, the role you sign in with needs to be allowed to:
launch EC2 instances with EBS volumes (io2 and gp3) in the block-device mapping,
terminate them, and create security groups;
create CloudWatch alarms and an SNS topic (or a Budget).
It does not need RDS, EC2 key pairs, placement groups, stop or start,
standalone volume create and delete, or `tag:GetResources` (teardown and
`cost-estimate.sh` ask each service when that one is denied). Tagging on create
(`ec2:CreateTags` through the launch, and the IAM, ECR and SNS tag actions) is
used when allowed; where a tag is denied for IAM, ECR or SNS the resource is created untagged.
`00-preflight.sh` tells you which needed action is denied before anything is created.

Where the two images (`ably-server`, `ably-loadgen`) live is your choice
(`IMAGE_REGISTRY_KIND`, section 3; the default is `ghcr`). **Run 0a needs no registry at
all**: it runs only the official Postgres image, and preflight creates nothing for a registry
under `ghcr` or `none`. For the fleet runs:

- `ghcr` (the default): nothing in AWS. On the machine that builds, `gh` signed in with the
  `write:packages` scope (`gh auth refresh -h github.com -s write:packages`). The
  boxes pull anonymously, so set both packages to public once after the first
  push (section 5.1), or give the boxes a pull token.
- `ecr`: `ecr:CreateRepository` (or repositories created for you), the layer and
  image push actions, and an instance profile for the boxes to pull with (created when
  `iam:CreateRole` and friends are allowed, or an existing profile with ECR read access in
  `INSTANCE_PROFILE_NAME`; without any, the boxes launch but cannot pull, and the network
  step says so). The Operator role of the Ably dev account is denied `ecr:CreateRepository`
  and `ecr:TagResource`, which is why `ghcr` is the default; the `AgentOperator` role
  (section 4) may create repositories.
- `none` (`IMAGE_REGISTRY_KIND=none`): nothing; the run 0a path only.

The instance profile is optional everywhere: no script needs `iam:*` to succeed. IAM entities
are looked up by exact name (`iam get-role`, `iam get-instance-profile`), never by listing.

The account also needs on-demand vCPU quota for the fleet. The 2x fleet is
about 480 vCPUs of the "Running On-Demand Standard instances" quota
(`L-1216C47A`, which counts the r7i Postgres boxes too); the 1x fleet is about
290. Preflight reads the quota and refuses when it is too low; a role that may
not read quotas (`servicequotas:*`) gets a warning with the number instead, and a
short quota then shows as `VcpuLimitExceeded` when a script launches boxes
(use `FLEET_PROFILE=1x` or ask for an increase). There must be a default VPC
in the region, or set `VPC_ID` and `SUBNET_ID`.

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
| `SSH_PUBLIC_KEY_PATH` | An OpenSSH public key (`ssh-ed25519` or `ssh-rsa` line). There is no EC2 key pair: every box appends this key to `ec2-user`'s `authorized_keys` in its user-data. The private key is the same path without `.pub` (or set `SSH_PRIVATE_KEY_PATH`). |
| `ALARM_EMAIL` | Receives the budget warnings. |
| `PG_PASSWORD` (or `RDS_PASSWORD`) | 16 or more characters, letters, digits, `_` and `-` only. The Postgres superuser password. Needed by `20-postgres.sh`. Other scripts read the DSN from STATE. |
| `RUN_TIME_LIMIT` | Hard limit for a run: `300`, `45m` or `2h`. Needed by `60-run.sh` and `65-run-0a.sh`. |

Optional, with defaults:

| Variable | Default | Meaning |
|---|---|---|
| `PROJECT_TAG` | `ably-server-scale-<your user name>` | Tag value on every resource and prefix of every name. Teardown deletes by it, so the default carries your user name: two people in one account never delete each other's fleet. Set it explicitly to share one on purpose. |
| `AZ` | `${AWS_REGION}a` | The one Availability Zone. |
| `STATE_FILE` | `bench/aws/state/STATE.json` (git-ignored, directory mode 0700) | Resource ids, image tags, run records, spend estimate. **It holds the database password and the API key**: never commit it, paste it or put it where others read it. |
| `LOG_FILE` | `bench/aws/state/LOG.md` | One line per completed step. |
| `RESULTS_DIR` | `bench/aws/state/results` | Raw results per run. |
| `BUDGET_ALARM_USD`, `BUDGET_CAP_USD` | 750, 1500 | Warning and cap. |
| `BUDGET_METHOD` | `cloudwatch` | `cloudwatch`: CloudWatch billing alarms with an SNS topic (the role can create and read them). `budgets`: an AWS Budget (created, but the role cannot read it back; preflight warns). `auto`: CloudWatch, else Budgets. The local estimate and `FLEET_MAX_UPTIME_H` are the guards that act either way. |
| `BUDGET_SCOPE` | `account` | Budgets only. `account` counts everything in the account. `tag` counts only spend tagged `Project=$PROJECT_TAG`, but sees nothing until the `Project` cost allocation tag is activated in Billing (up to a day). |
| `PRICE_FACTOR` | 1 | Scales every price in the local cost estimate (`1.1` for eu-west-1). `PRICE_<type>` overrides one instance price. |
| `FLEET_PROFILE` | `2x` | Sizes the vCPU quota check (`2x` or `1x`); vCPUs already in use in the account are subtracted. |
| `FLEET_MAX_UPTIME_H` | 10 | Dead-man switch: each box powers itself off, and so terminates, this many hours after it boots. |
| `USE_PLACEMENT_GROUP` | 0 | `1` tries to create a cluster placement group and launches the fleet in it; a denial is a warning and the fleet launches without one. |
| `BILLING_REGION` | `us-east-1` | The one region that holds billing metrics; CloudWatch billing alarms and their SNS topic live there. |
| `IMAGE_REGISTRY` | `ghcr.io/<your GitHub login>` | Where the `ably-server` and `ably-loadgen` images are pushed and pulled: `ghcr.io/<github-owner>` (lower case), or `<account>.dkr.ecr.<region>.amazonaws.com[/<prefix>]`. No owner or account is written in a script: with `ghcr` and no value, the owner is your login from `gh api user` (or `GHCR_USER`), looked up only when an image reference is first needed, so run 0a never asks. Set it for an organisation. With `IMAGE_REGISTRY_KIND=ecr` and no value it becomes `<AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com/$PROJECT_TAG`. |
| `IMAGE_REGISTRY_KIND` | `ghcr` (an ECR host in `IMAGE_REGISTRY` gives `ecr`) | `ghcr`, `ecr` or `none`. `none` means no registry: enough for run 0a; `40-nodes.sh` and `50-loadgen.sh` stop and say so. |
| `GHCR_USER` | `gh api user --jq .login` | The name `build-push.sh` logs in to ghcr.io with (the token comes from `gh auth token`). Also the default for `GHCR_PULL_USER`. |
| `GHCR_PULL_TOKEN`, `GHCR_PULL_USER` | unset, `$GHCR_USER` or `token` | For private ghcr packages: a token with `read:packages` that the boxes log in with. Default is no token and public packages. See "Image registry" below for the trade-off. Letters, digits and `_` only. |
| `SKIP_REGISTRY_CHECK` | 0 | `40-nodes.sh` and `50-loadgen.sh` test, from your machine, that the package can be pulled anonymously (ghcr, no `GHCR_PULL_TOKEN`) and stop if not. `1` skips the test. |
| `BASE_IMAGE_REGISTRY` | `docker.io` | Where the third party images (postgres, nats, prometheus, grafana, exporters) are pulled from. Anything else is a mirror with a flat layout, `<registry>/<name>:<tag>`: `build-push.sh mirror` fills one under `IMAGE_REGISTRY` (section 5.1). |
| `ECR_REPO_SERVER`, `ECR_REPO_LOADGEN` | `<prefix>/ably-server`, `<prefix>/ably-loadgen` | ECR only. `<prefix>` is the path of `IMAGE_REGISTRY` (`$PROJECT_TAG` by default). Teardown with `TEARDOWN_ALL=1` deletes only repositories tagged for the project. |
| `INSTANCE_PROFILE_NAME` | (none; created for ecr) | An existing instance profile with ECR read access, looked up by exact name. Set: used. Unset: launch without one, except that ecr tries to create one. |
| `CREATE_INSTANCE_PROFILE` | 1 for `ecr`, else 0 | `10-network.sh` creates an IAM role and instance profile only when this is 1 (and no `INSTANCE_PROFILE_NAME`). `ghcr` and `none` need none; set 1 to get SSM Session Manager. Any IAM denial while creating it is a warning and the fleet launches without one. |
| `TAGGING_API` | `on` | `off` skips the tagging-API attempt in teardown and `cost-estimate.sh` and lists service by service straight away (saves one denied call when you know the role lacks `tag:GetResources`). |
| `VPC_ID`, `SUBNET_ID` | (default VPC) | Use these instead of the default VPC. |
| `NODE_COUNT`, `NATS_COUNT`, `LOADGEN_COUNT`, `PUBLISHER_COUNT` | 10, 3, 3, 2 | Fleet sizes. |
| `NODE_INSTANCE_TYPE`, `NATS_INSTANCE_TYPE`, `LOADGEN_INSTANCE_TYPE`, `PUBLISHER_INSTANCE_TYPE`, `CONDUCTOR_INSTANCE_TYPE`, `PGDRIVER_INSTANCE_TYPE` | c7i.2xlarge, c7i.2xlarge, c7i.8xlarge, c7i.4xlarge, c7i.2xlarge, c7i.4xlarge | Instance types. |
| `PG_INSTANCE_TYPE` | `r7i.4xlarge` | The Postgres box (x86_64; 16 vCPU, 128 GB). `shared_buffers` and `effective_cache_size` follow its memory. |
| `PG_STORAGE`, `PG_STORAGE_GB`, `PG_IOPS`, `PG_THROUGHPUT_MBPS` | io2, 1000, 20000 (io2) or 12000 (gp3), 500 (gp3 only) | The EBS data volume. The old names `RDS_STORAGE`, `RDS_STORAGE_GB`, `RDS_IOPS` still work; the `PG_` name wins when both are set. gp3 defaults to RDS's documented baseline for this size, not the EBS default of 3000 and 125. |
| `PG_IMAGE` | `postgres:17` | The database image (`RDS_ENGINE_VERSION=17.x` selects `postgres:17`). The server version and the image id that were actually started are recorded in STATE. |
| `DB_POOL_SIZE`, `MAX_NODES` | 50, 20 | Size `max_connections` (`MAX_NODES x DB_POOL_SIZE + 300`). |
| `SHARDS` | 1 | Run 8: number of Postgres instances per storage type. |
| `ACTIVE_STORAGE` | first created | Which storage type the nodes use (`io2` or `gp3`). |
| `BUS` | `nats` | `nats`, `postgres`, `pgnotify`, or `none` (no `--bus` flag, for the shipped image in run 1a). |
| `SERVER_TAG`, `LOADGEN_TAG` | from STATE | Image tags in the registry. |
| `ABLY_SERVER_EXTRA_FLAGS` | empty | Extra node flags (write path, connection layer). The proof ran with `--publish-lanes=2`: set `ABLY_SERVER_EXTRA_FLAGS="--publish-lanes=2"` to reproduce it. Empty means the code default, which differs (4 on this branch, DESIGN.md §6.3): with 10 to 20 nodes each lane finds little queued, so fewer lanes per node give deeper batches at the same write rate, while a smaller fleet may prefer the default. The run's `summary.md` prints the lane count the nodes reported (`ably_publish_lanes`); quote that, not this table. |
| `NODE_GOMAXPROCS`, `NODE_GOMEMLIMIT` | unset, `13GiB` | Go runtime settings on the nodes. |
| `NATS_IP_OFFSET`, `NATS_IMAGE` | 10, `nats:2.11` | NATS servers take the addresses from this offset in the subnet. Every third party image name (`PG_IMAGE`, `PGBENCH_IMAGE`, `NATS_IMAGE`, ...) is resolved through `BASE_IMAGE_REGISTRY`. |
| `LOADGEN_CMD`, `PUBLISHER_CMD`, `CONDUCTOR_CMD` | see section 7 | The commands run in the `ably-loadgen` image. |
| `NODE_VCPU`, `NODE_MEMORY_GB` | from `NODE_INSTANCE_TYPE` | Node size passed to the conductor (`--node-vcpu`, `--node-memory-gb`). |
| `SCENARIO_DIR` | `bench/scenarios` | Where scenario names are looked up. |
| `RECONFIGURE`, `SCALE_DOWN` | 0 | Re-apply the image and flags to live boxes (`40-nodes.sh`, `50-loadgen.sh`), or remove nodes above `NODE_COUNT`. |
| `DURATION_S`, `WARMUP_S`, `CLIENTS`, `VARIANTS`, `CHANNELS`, `PAYLOAD_BYTES` | 60, 10, `1 8 32 128`, all, 200000, 600 | Run 0a settings (`pgbench/run.sh`). |
| `IMAGE_TAG`, `IMAGE_PLATFORM`, `PUSH`, `ALLOW_DIRTY` | git sha, `linux/amd64`, 1, 0 | `build-push.sh`. With `IMAGE_REGISTRY_KIND=none` it builds and pushes nothing. |
| `TEARDOWN_ALL` | 0 | `90-teardown.sh`: also delete the ECR repositories (ecr only), the billing alarms and their SNS topic. ghcr packages are never touched. |
| `KEEP_POSTGRES` | 0 | `80-terminate.sh`: terminate everything except the Postgres instances. |
| `AUTO_TERMINATE_AFTER_RUN`, `OVERRIDE_BUDGET_GUARD`, `FORCE_NO_ALARM` | 0 | See the safety rails. |
| `SKIP_BOOT_WAIT`, `SKIP_OBSERVABILITY`, `KEEP_WORK_DIR` | 0 | Skip waiting for cloud-init (secrets are still delivered over SSH), skip `55-observability.sh`, keep rendered user-data for inspection (it carries no secret). |
| `DRY_RUN` | 0 | `1` prints every aws, ssh and scp call and runs nothing. |

### Image registry

Only the two images this repository builds need a registry of your own, and only
for the fleet runs (`40-nodes.sh`, `50-loadgen.sh`). Run 0a runs `postgres:17`
and `postgres:17-alpine`, nothing else.

    export IMAGE_REGISTRY=ghcr.io/<github-owner>      # the fork is public; the images hold nothing private
    export IMAGE_REGISTRY_KIND=ghcr                   # optional: inferred from the host

The GitHub owner and the AWS account never appear in a script: they come from the
environment.

- **Public packages (the default).** The boxes pull `ghcr.io/<owner>/ably-server`
  and `ghcr.io/<owner>/ably-loadgen` with no login. A package that `build-push.sh`
  creates is private, so set both to public once, after the first push:
  `https://github.com/users/<owner>/packages`, open the package, Package settings,
  Change visibility. For an organisation the address is
  `https://github.com/orgs/<owner>/packages`. `build-push.sh` says whether each
  package is public, and `40-nodes.sh` and `50-loadgen.sh` test it before they
  launch anything. The images contain the open source binaries and nothing that
  names a customer or an account, which is why public is acceptable here.
- **Private packages: `GHCR_PULL_TOKEN`.** Export a token with `read:packages`
  (a classic personal access token) before `40-nodes.sh` and `50-loadgen.sh`; every box
  then runs `docker login ghcr.io` at boot. The token is not written into user-data:
  the launching script copies it to the box over SSH (see "Secrets and user-data"
  below) and the boot script reads and deletes it. It still sits in root's Docker
  config on the box and is live until you revoke it. Use a token created for this
  run with nothing but `read:packages`, and revoke it at teardown. Public packages
  avoid all of that.
- **ECR.** `IMAGE_REGISTRY_KIND=ecr`: `00-preflight.sh` looks for each repository with
  `describe-repositories` and creates only a missing one (tagged, or untagged when the
  role may not tag). If `ecr:CreateRepository` is denied it stops and says so. It does not
  probe ECR with an invalid name: ECR answers `InvalidParameter` before it checks
  permission, so that probe reports "ok" for a role that is denied.
- **Docker Hub pull limits.** Every box pulls its role image and node-exporter from Docker
  Hub, which limits anonymous pulls per source address. Preflight warns when the fleet has
  more than 10 boxes and `BASE_IMAGE_REGISTRY` is `docker.io`. To avoid the risk, mirror
  the eight third party images once into the same registry and point the fleet at it:

      bench/aws/build-push.sh mirror                  # pulls linux/amd64, retags, pushes to $IMAGE_REGISTRY/<name>:<tag>
      export BASE_IMAGE_REGISTRY="$IMAGE_REGISTRY"    # 20-postgres.sh, 30-nats.sh, 55-observability.sh and the rest now pull from it

  The mirrored packages are private on ghcr too: set them public (they are the unmodified
  official images) or use `GHCR_PULL_TOKEN`. The mirror is flat, so `prom/prometheus:v2.55.1`
  becomes `<registry>/prometheus:v2.55.1`.

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
> of the target account. The dev account has that role, so
> `ablyctl aws env --account dev` works from an agent session (identity
> `assumed-role/AgentOperator/ablyctl-agent-<user>`). Its permissions differ from
> `Operator`: EC2 launch with io2 volumes, placement groups, security groups and
> ECR, tagging-API and service-quota calls are allowed; a scoped IAM grant allows
> `iam:CreateRole`, `CreateInstanceProfile`, `AddRoleToInstanceProfile`, `PassRole`,
> `AttachRolePolicy`, `GetRole`, `GetInstanceProfile`, `DeleteRole` and
> `DeleteInstanceProfile`, but not `ListInstanceProfiles`, `DetachRolePolicy` or
> `RemoveRoleFromInstanceProfile`; billing reads are denied. So these scripts look IAM entities up by
> exact name only, and a role or profile that cannot be detached is left in place
> (it costs nothing and is reused). If minting ever fails (expired sign-in, or
> `AgentOperator` missing), there are two paths: (a) infrastructure provisions
> `AgentOperator` in the dev account, or (b) the operator mints credentials in
> their own terminal with the command above and exports `AWS_ACCESS_KEY_ID`,
> `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` in the shell that runs the
> scripts. Do not unset the agent variables to get around the detection.
>
> `lib.sh` sources `ably-internal.sh`, which runs the `eval` line when `ablyctl`
> is on PATH and no `AWS_ACCESS_KEY_ID` or `AWS_PROFILE` is set, in every script
> that is not a dry run, so `aws.env` need hold no keys. If it cannot
> mint credentials (expired sign-in, or the agent case above) the script prints
> both paths and exits. Do not put the account id or the permission set name in
> any file in this repository: take them from the environment.
>
> `ablyctl aws ecr env` prints the registry of ablyctl's default ECR account,
> which may not be the account the fleet runs in. With `IMAGE_REGISTRY_KIND=ecr`,
> `00-preflight.sh` looks for the two repositories in the working account and
> `build-push.sh` derives the registry from `AWS_ACCOUNT_ID` and `AWS_REGION`, so you do
> not need it. The Operator role in the dev account cannot create repositories: use `ghcr`.

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

    bench/aws/00-preflight.sh       # credentials, account, permissions, vCPU quota, billing alarm; the ECR repositories only for IMAGE_REGISTRY_KIND=ecr
    bench/aws/10-network.sh         # security group (an instance profile only for ecr; a placement group only with USE_PLACEMENT_GROUP=1; no key pair)
    bench/aws/build-push.sh         # builds ably-server and ably-loadgen for linux/amd64 and pushes them to IMAGE_REGISTRY

Run 0a needs neither image: skip `build-push.sh` and go to 5.2 (set `IMAGE_REGISTRY_KIND=none` if you
want nothing registry-related at all). For the fleet, see section 3, "Image registry"; with no
`IMAGE_REGISTRY` the images go to `ghcr.io/<your GitHub login>`.

For ghcr the first push goes like this:

    gh auth status                                   # signed in; if the push is refused: gh auth refresh -h github.com -s write:packages
    export IMAGE_REGISTRY=ghcr.io/<github-owner>
    bench/aws/build-push.sh                          # logs in with gh auth token, pushes both images, says whether each is public
    # once, in a browser: https://github.com/users/<github-owner>/packages -> ably-server and ably-loadgen -> Package settings -> Change visibility -> Public
    bench/aws/build-push.sh mirror                   # optional, for a fleet of more than 10 boxes (Docker Hub pull limits); set those public too
    export BASE_IMAGE_REGISTRY="$IMAGE_REGISTRY"     # only after the mirror exists

`build-push.sh` refuses to push from a dirty working tree, so a pushed tag
always names real commits. Without credentials for the registry it builds locally and
skips the push. To build the shipped baseline for run 1a:

    git checkout main
    IMAGE_TAG=main-$(git rev-parse --short=7 HEAD) bench/aws/build-push.sh server

Confirm the budget e-mail subscription that AWS sends to `ALARM_EMAIL`.

### 5.2 Run 0a: Postgres alone (first real numbers, before any node exists)

    PG_STORAGE=io2 bench/aws/20-postgres.sh           # about 10 minutes: boot, Docker pull, initdb, pg_isready over SSH
    bench/aws/25-pgdriver.sh                          # the pgbench box
    RUN_TIME_LIMIT=1h bench/aws/65-run-0a.sh io2      # about 40 minutes
    PG_STORAGE=gp3 bench/aws/20-postgres.sh           # a second instance; both exist side by side (5.0 USD an hour together)
    RUN_TIME_LIMIT=1h bench/aws/65-run-0a.sh gp3
    bench/aws/pgbench/summarise.sh "$RESULTS_DIR"/0a-io2-*/out/results-io2.csv "$RESULTS_DIR"/0a-gp3-*/out/results-gp3.csv > summary-0a.md

`20-postgres.sh` does not finish until `pg_isready` answers over SSH and the
server reports `synchronous_commit=on` and `pg_stat_statements` preloaded; it
stops with an error otherwise. If you are not sure the gp3 run is needed this
hour, run io2 first, collect, and create the gp3 instance later: each instance
is 1.1 to 3.0 USD an hour (box plus volume).

The variants, what each measures, and the pass check for the SQL itself are
in `pgbench/run.sh` and `pgbench/*.sql`. The numbers to read: the `trivial`
row at one client is the ACK latency floor; `shipped` is the current write
path; `batch30` and `batch100` show how far batching moves one primary. The
summary also prints WAL bytes per message.

Nothing on the boxes survives a terminate: `65-run-0a.sh` has already copied
the CSV, raw output and tables into `results/`. Then delete what you do not
need next: `bench/aws/80-terminate.sh --yes` (keeps the security group), or
`bench/aws/90-teardown.sh --yes` to delete everything.

### 5.3 The fleet

    PG_STORAGE=io2 bench/aws/20-postgres.sh           # skip if it already exists
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
| 4 storage | `PG_STORAGE=gp3 ACTIVE_STORAGE=gp3 bench/aws/20-postgres.sh`, then `RECONFIGURE=1 bench/aws/40-nodes.sh` |
| 5 failure injection | the scenario kills a node or a NATS server through the conductor; to do it by hand, `ssh` to the box and `docker kill` the container (the role cannot stop instances) |
| 6 presence | the presence scenario, `RUN_TIME_LIMIT=30m` |
| 7 node curve | `NODE_COUNT=5 SCALE_DOWN=1 bench/aws/40-nodes.sh`, then 10, then 20; re-run `55-observability.sh` after each change |
| 8 shard curve | `SHARDS=3 PG_STORAGE=io2 bench/aws/20-postgres.sh` and `RECONFIGURE=1` with the sharding flag in `ABLY_SERVER_EXTRA_FLAGS` |

Watch a run from your machine through an SSH tunnel (the UIs listen on the
conductor's loopback only). The snippets here and in section 6 read `PROJECT_TAG` and
`STATE_FILE` from your shell; if you never set them, the scripts used
`ably-server-scale-$(id -un | tr A-Z a-z)` and `bench/aws/state/STATE.json`:

    ssh -i "${SSH_PRIVATE_KEY_PATH:-${SSH_PUBLIC_KEY_PATH%.pub}}" -L 3000:127.0.0.1:3000 -L 9090:127.0.0.1:9090 \
      ec2-user@"$(jq -r ".instances[\"${PROJECT_TAG:?set PROJECT_TAG to the value you ran with}-conductor-1\"].public_ip" "$STATE_FILE")"
    # Grafana:    http://localhost:3000  (user admin, password: jq -r .grafana_password "$STATE_FILE")
    # Prometheus: http://localhost:9090

### 5.6 Collect

    bench/aws/70-collect.sh <run-id>

It gathers a Prometheus snapshot and range exports of the series in
`observability/export-series.txt`, container logs and docker stats samples
from every box, `pg_stat_statements` from each database, the `postgresql.conf`
in force, and a copy of STATE with the secrets removed, into
`results/<run-id>/collect/`. Each step is best effort and logs a warning when
it cannot finish. Results and raw logs stay out of the repository.

### 5.7 Between runs, and teardown

The role cannot stop or start instances, so there is no stop and start. Between
runs, terminate and re-create:

    bench/aws/70-collect.sh <run-id>   # first: nothing on a box survives
    bench/aws/80-terminate.sh --yes    # every instance and its disks; keeps the security group, images and STATE
    # later: 20-postgres.sh, 25-pgdriver.sh (run 0a), 30-nats.sh, 40-nodes.sh, 50-loadgen.sh again (about ten minutes)
    bench/aws/90-teardown.sh --yes     # everything tagged Project=$PROJECT_TAG, then verify

`KEEP_POSTGRES=1 bench/aws/80-terminate.sh --yes` leaves the database boxes and
their data up (they keep billing, 3.0 USD an hour each). The billing alarms and
any ECR repositories survive a teardown on purpose (set `TEARDOWN_ALL=1` to remove
them, the SNS topic too; ghcr packages are never touched). The role cannot delete a standalone volume, so the scripts never create
one: every volume is deleted with its instance, and `90-teardown.sh` would report
any volume that is still there.

End every working day with `90-teardown.sh --yes`, or write in `LOG_FILE`
that the fleet is up, what it costs an hour and why. The dead-man switch
(`FLEET_MAX_UPTIME_H`) ends it after ten hours either way.

## 6. If something is left running

1. Run `bench/aws/90-teardown.sh --yes`. It deletes by tag and by what STATE lists, so it works even when STATE is lost.
2. If it reports something still present, read the line: a security group that will not delete is usually waiting for a network interface to detach; wait a minute and run it again.
3. Without the scripts (or to double-check), list everything tagged for the project. `bench/aws/cost-estimate.sh` does this too and warns about instances and volumes STATE does not know. The tagging API is the first attempt:

       aws resourcegroupstaggingapi get-resources --tag-filters Key=Project,Values="${PROJECT_TAG:?set PROJECT_TAG to the value you ran with}" \
         --query 'ResourceTagMappingList[].ResourceARN' --output text

   Terminated instances can stay in that list for about an hour. The Operator role is denied `tag:GetResources`
   (AccessDenied); teardown and `cost-estimate.sh` then ask each service, which you can do by hand too
   (add `--region "$AWS_REGION"`; the last two are in `BILLING_REGION`, us-east-1):

       P="${PROJECT_TAG:?set PROJECT_TAG to the value you ran with}"
       aws ec2 describe-instances --filters Name=tag:Project,Values=$P Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text
       aws ec2 describe-volumes --filters Name=tag:Project,Values=$P --query 'Volumes[].VolumeId' --output text
       aws ec2 describe-security-groups --filters Name=tag:Project,Values=$P --query 'SecurityGroups[].GroupId' --output text
       aws ec2 describe-network-interfaces --filters Name=tag:Project,Values=$P --query 'NetworkInterfaces[].NetworkInterfaceId' --output text
       aws iam list-roles --query "Roles[?starts_with(RoleName, '$P-')].RoleName" --output text
       aws cloudwatch describe-alarms --alarm-name-prefix "$P-" --query 'MetricAlarms[].AlarmName' --output text --region us-east-1
       aws sns list-topics --query "Topics[?contains(TopicArn, ':$P-')].TopicArn" --output text --region us-east-1
4. To delete by hand: `aws ec2 terminate-instances` (the volumes go with the instances), then the security group, then the IAM role and instance profile.
5. A resource with no tag is not found by the tag-based listings. The scripts tag every
   instance, volume, security group and placement group (if any) they create. IAM roles, the
   SNS topic and ECR repositories are tagged when the role may tag them and created untagged
   when it may not; the per-service listing finds those by their `$PROJECT_TAG-` name prefix.

## 7. The load generator commands

`50-loadgen.sh` and `60-run.sh` run commands inside the `ably-loadgen` image.
The defaults match the load generator's flags:

| Variable | Default |
|---|---|
| `LOADGEN_CMD` | `ably-loadgen serve --listen=:9200 --metrics-listen=:9101 --role=generator --ntp-server=169.254.169.123:123` |
| `PUBLISHER_CMD` | `ably-loadgen serve --listen=:9200 --metrics-listen=:9101 --role=publisher --ntp-server=169.254.169.123:123` |
| `CONDUCTOR_CMD` | `ably-conductor run --scenario /run-input/{SCENARIO} --inventory /run-input/inventory.json --results /results --run-id {RUN_ID} --node-vcpu $NODE_VCPU --node-memory-gb $NODE_MEMORY_GB` |

`--role` is `generator`, `publisher` or `all`. `--ntp-server` (set it with
`LOADGEN_NTP_SERVER`; the default is the Amazon Time Sync address chrony uses)
lets the conductor read each box's clock offset at the start and end of a run
(`GET /v1/clock`); the run record prints them and the run fails if one is above
5 ms. A box that cannot measure is recorded as "not measured" and only the
negative-latency check guards against skew on it. The conductor also takes
`--fault-hook CMD --fault-kind node-kill|bus-kill|other --fault-at D --time-limit D --log --state`; add them by
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

Amazon Linux 2023 (the latest AMI from the public SSM parameter, or from
`ec2 describe-images` when that parameter is not readable; pinned in STATE on first
use), Docker from the distribution, chrony with Amazon Time Sync, `fs.nr_open`
and `nofile` at 1M, the ephemeral port range 1024 to 65535, the operator's SSH
key in `ec2-user`'s `authorized_keys`. Third party images (pulled from `BASE_IMAGE_REGISTRY`,
Docker Hub by default): `nats:2.11`,
`natsio/prometheus-nats-exporter:0.15.0`, `prom/node-exporter:v1.8.2`,
`prom/prometheus:v2.55.1`, `grafana/grafana:11.3.0`,
`quay.io/prometheuscommunity/postgres-exporter:v0.15.0`, `postgres:17` (the
database), `postgres:17-alpine` (pgbench and psql), plus the two images this
repository builds. The Postgres box mounts its EBS data volume (xfs, `noatime`)
at `/data/pg`; the settings are in `templates/postgresql.conf` and
`templates/postgres.sh`, rendered by `20-postgres.sh` (nothing here is an RDS
default: see section 1). The instance types, versions and flags of every run
are recorded in STATE (`.deployment`, `.loadgen`, `.postgres` with the server
version, image id and settings read back from the database, `.runs`).

## 9. Troubleshooting

- **`InsufficientInstanceCapacity`.** Retry, or try another `AZ` (and re-run `10-network.sh`). With `USE_PLACEMENT_GROUP=1`, set it back to 0 (the latency difference is small).
- **`VcpuLimitExceeded` when launching.** The on-demand vCPU quota is lower than the fleet needs (preflight could not read it). Use `FLEET_PROFILE=1x` sizes, terminate what is not needed, or ask for an increase of quota `L-1216C47A`.
- **A box is not reachable over SSH.** Check `ADMIN_CIDR` still matches your address (it changes with the network you are on) and re-run `10-network.sh` after fixing it; the rule is added, not replaced, so remove the old rule by hand. The key is the one in `SSH_PUBLIC_KEY_PATH` when the box was launched, and the private key must match it (`SSH_PRIVATE_KEY_PATH`); there is no key pair to swap afterwards, only a new box.
- **`20-postgres.sh` times out waiting for Postgres.** `ssh` in (`ssh ec2-user@<ip>`), read `/var/log/bench-userdata.log`, then `docker logs postgres`, `lsblk` and `df -h /data/pg`. A data volume that never appears, a full disk, or a bad `postgresql.conf` are the usual causes. To start again, `80-terminate.sh --yes` (the volume goes with the instance) and re-run.
- **A box vanished.** The dead-man switch (`FLEET_MAX_UPTIME_H`) powers it off after that many hours and the box terminates with its disks, the database included. `cost-estimate.sh` notices; re-create what you need.
- **A box came up but the container is not running.** `ssh` in and read `/var/log/bench-userdata.log` and `docker logs`. `RECONFIGURE=1` re-applies the role part.
- **Image pull fails on a box.** Read `/var/log/bench-userdata.log`. ECR: the instance profile is missing or not yet propagated; wait a minute, then `RECONFIGURE=1 bench/aws/40-nodes.sh`. ghcr `denied` or `unauthorized`: the package is private; set it to public (section 3, "Image registry") or export `GHCR_PULL_TOKEN`, then `RECONFIGURE=1`. `429 Too Many Requests` from Docker Hub: mirror the third party images (`build-push.sh mirror`, `BASE_IMAGE_REGISTRY`), or wait and re-run the script that booted the box.
- **`40-nodes.sh` or `50-loadgen.sh` stops at "no registry".** `IMAGE_REGISTRY_KIND` is `none`. Unset it (the default is `ghcr`), or set `IMAGE_REGISTRY`, and run `build-push.sh`.
- **The network step warns "launching WITHOUT an instance profile".** The role may not create the IAM role or profile (or finish attaching the ECR policy). With `ghcr` that changes nothing except SSM Session Manager. With `ecr` the boxes cannot pull: use `ghcr`, or set `INSTANCE_PROFILE_NAME` to an existing profile.
- **Teardown leaves the IAM role and profile.** `DetachRolePolicy` and `RemoveRoleFromInstanceProfile` may be denied, so `DeleteRole` and `DeleteInstanceProfile` answer `DeleteConflict`. Teardown warns and goes on: they cost nothing and `10-network.sh` reuses them by name.
- **`build-push.sh` push refused on ghcr.** The `gh` token needs `write:packages` (`gh auth refresh -h github.com -s write:packages`), and the owner in `IMAGE_REGISTRY` must be your login or an organisation you can push packages to.
- **Preflight says "permission unproven".** The probe for that action (SNS, CloudWatch, Budgets, IAM) reached only parameter validation, which a service may run before it checks permission. It is a hint. The real call is the test; ECR has no probe for this reason.
- **`00-preflight.sh` reports a denied action.** Sign in with a role that has it, or ask for it. Nothing was created. Actions this design does not need (RDS, key pairs, placement groups, stop and start, standalone volumes, Cost Explorer, `budgets:ViewBudget`, service quotas) are not probed or are informational.
- **Preflight says the terminate permission is only weakly checked.** `ec2:TerminateInstances` cannot be proven without an instance; `80-terminate.sh` and `90-teardown.sh` are the real test, so try them on the smoke fleet first.
- **`10-network.sh` or `30-nats.sh` fails on an address.** NATS servers take fixed private addresses from `NATS_IP_OFFSET` in the subnet; if another instance already holds one, pick another offset.
- **Secrets and user-data.** No secret is written into EC2 user-data (which anyone who can describe instance attributes can read, and which stays on the box under `/var/lib/cloud`). The API key, the database password and DSNs, and the registry pull token go to a fresh box over SSH instead: the boot script (`templates/secrets.sh`) waits up to 20 minutes for `/home/ec2-user/.bench-secrets.env`, which `lib.sh deliver_boot_secrets` copies as soon as SSH answers (the scripts do this before `wait_boot`; it needs SSH even with `SKIP_BOOT_WAIT=1`). The boot script loads the file untraced and deletes it, and containers take the values from the environment by name, so none is on a command line or in the boot log. `RECONFIGURE=1` re-delivers what the role needs. Remaining exposure: the values are in the containers' environment (`docker inspect` on the box, which only `ec2-user` and root can run) and, for the registry token, in root's Docker config. `test/userdata-lint.sh` fails if a rendered user-data contains the API key, a `postgres://` URL with a password, the pull token or its variable name. The bench database is throwaway and only reachable inside the security group; do not reuse the password anywhere. The conductor box still passes a DSN to `psql` on its command line once, to create the `pg_stat_statements` extension.
- **Untagged IAM role, instance profile, SNS topic or ECR repository.** Tagging on create needs `iam:TagRole`, `iam:TagInstanceProfile`, `sns:TagResource` and `ecr:TagResource`. When the role lacks one, the script logs a warning and creates the resource untagged (every create goes through the same helper); `90-teardown.sh` still removes the IAM entities by name from STATE (a denied IAM delete is a warning: the role and profile cost nothing), and leaves an untagged ECR repository alone.
- **Teardown says a read failed.** Teardown refuses to treat a failed read as "nothing there". Fix the sign-in or permission and run it again; STATE is kept until it verifies clean. A denied `tag:GetResources` is not such a failure: the final listing then runs service by service, and a service whose list call is denied is named in a warning ("could not verify").
- **STATE.json was lost.** `90-teardown.sh --yes` still works. To continue instead, re-run `10-network.sh`, `20-postgres.sh` (with `PG_PASSWORD`), `30-nats.sh`, `40-nodes.sh` and `50-loadgen.sh`: they find existing resources by tag and name and refill STATE.
