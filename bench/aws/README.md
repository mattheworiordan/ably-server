# bench/aws

Scripts that stand up the scale-proof test fleet on AWS, run a scenario,
collect the results and tear the fleet down. Start with [RUNBOOK.md](RUNBOOK.md).
Anonymised results go in [RESULTS.md](RESULTS.md).

| File | What it does |
|---|---|
| `env.example` | Every environment variable, with placeholders. Copy it outside the repository. |
| `lib.sh` | Shared helpers: dry-run wrapper, tags, state file (jq), log, waits, cost estimate. |
| `00-preflight.sh` | Credentials, account, permission probes, vCPU quota, ECR repositories, billing alarm. |
| `10-network.sh` | Default VPC, one zone, security group, instance profile (a placement group only on request; no key pair: the SSH key goes in user-data). |
| `build-push.sh`, `Dockerfile.loadgen` | Build `ably-server` and `ably-loadgen` for linux/amd64, push to ECR, record tags. |
| `20-postgres.sh` | PostgreSQL 17 in Docker on an r7i.4xlarge with an EBS data volume (io2 or gp3), not RDS (see RUNBOOK section 1); optionally several instances for run 8. |
| `25-pgdriver.sh`, `65-run-0a.sh`, `pgbench/` | Run 0a: pgbench against Postgres alone. |
| `30-nats.sh` | Three-server NATS core cluster. |
| `40-nodes.sh` | The ably-server nodes. |
| `50-loadgen.sh`, `55-observability.sh`, `observability/` | Generators, publishers, conductor, Prometheus, Grafana, postgres_exporter. |
| `60-run.sh` | Run one scenario on the conductor under `RUN_TIME_LIMIT` and copy the results back. |
| `70-collect.sh` | Gather metrics, logs and database statistics for a run. |
| `80-terminate.sh` | Between runs: terminate every instance and its disks (there is no stop and start) and keep the network. |
| `90-teardown.sh` | Delete everything tagged for the project and verify. |
| `cost-estimate.sh` | Hourly rate of what is running and an estimate of the spend so far. |
| `templates/` | The user-data that boots each kind of box. |
| `test/` | Tests that need no AWS account: `test/run-all.sh`. |

Every script is safe to run again (create or reuse), tags what it creates with
`Project=$PROJECT_TAG`, records ids in `$STATE_FILE`, appends a line to
`$LOG_FILE`, and prints the calls it would make with `DRY_RUN=1`.
Nothing here names an account or a region: they come from the environment.

Needs bash 4.4 or newer, AWS CLI v2, `jq`, `ssh`, Docker with buildx.
