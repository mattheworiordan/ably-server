# bench/aws

Scripts that stand up the scale-proof test fleet on AWS, run a scenario,
collect the results and tear the fleet down. Start with [RUNBOOK.md](RUNBOOK.md).
Anonymised results go in [RESULTS.md](RESULTS.md).

| File | What it does |
|---|---|
| `env.example` | Every environment variable, with placeholders. Copy it outside the repository. |
| `lib.sh` | Shared helpers: dry-run wrapper, tags, state file (jq), log, waits, cost estimate. |
| `00-preflight.sh` | Credentials, account, permission probes, vCPU quota, billing alarm, image registry (ECR repositories only for `IMAGE_REGISTRY_KIND=ecr`). |
| `10-network.sh` | Default VPC, one zone, security group, instance profile (ecr only; a placement group only on request; no key pair: the SSH key goes in user-data). |
| `build-push.sh`, `Dockerfile.loadgen` | Build `ably-server` and `ably-loadgen` for linux/amd64, push to `IMAGE_REGISTRY` (ghcr or ECR; none builds only), record tags. `mirror` copies the third party images to the same registry. |
| `20-postgres.sh` | PostgreSQL 17 in Docker on an r7i.4xlarge with an EBS data volume (io2 or gp3), not RDS (see RUNBOOK section 1); optionally several instances for run 8. |
| `25-pgdriver.sh`, `65-run-0a.sh`, `pgbench/` | Run 0a: pgbench against Postgres alone. |
| `30-nats.sh` | Three-server NATS core cluster. |
| `40-nodes.sh` | The ably-server nodes. |
| `50-loadgen.sh`, `55-observability.sh`, `observability/` | Generators, publishers, conductor, Prometheus, Grafana, postgres_exporter. |
| `60-run.sh` | Run one scenario on the conductor under `RUN_TIME_LIMIT` and copy the results back. |
| `70-collect.sh` | Gather metrics, logs and database statistics for a run. |
| `80-terminate.sh` | Between runs: terminate every instance and its disks (there is no stop and start) and keep the network. |
| `90-teardown.sh` | Delete everything tagged for the project and verify. |
| `cost-estimate.sh` | Hourly rate of what is running, an estimate of the spend so far, and a warning about tagged instances and volumes STATE does not know. |
| `templates/` | The user-data that boots each kind of box. |
| `test/` | Tests that need no AWS account: `test/run-all.sh`. |

Every script is safe to run again (create or reuse), tags what it creates with
`Project=$PROJECT_TAG`, records ids in `$STATE_FILE`, appends a line to
`$LOG_FILE`, and prints the calls it would make with `DRY_RUN=1`.
Nothing here names an account or a region: they come from the environment.

Needs bash 4.4 or newer, AWS CLI v2, `jq`, `ssh`, Docker with buildx.

The load generator and conductor that the run scripts drive are described below.

## Load generation for the scale proof

Two binaries drive the cloud runs (plan §6 item 8, §7, §8):

- `ably-loadgen` generates load and checks correctness. It runs as an
  **agent** on each generator box and takes jobs over HTTP.
- `ably-conductor` reads a **scenario**, sends each agent its share of
  the work, ramps, holds, optionally injects a failure, collects every
  job's summary and the nodes' metrics, evaluates the pass criteria and
  writes a **run record**. It also prints plans and builds the report.

`cmd/ably-bench` stays for the exact-once slice (small, SDK-based,
same-process latency).

Nothing here names a customer: workloads are shapes F, M and D.

### Build

    go build -o bin/ ./cmd/ably-loadgen ./cmd/ably-conductor

Both are static Go binaries with no runtime dependencies.

### Local smoke

    bench/aws/local-smoke.sh     # shapes M, D, F at 1%, then M with a node killed

starts `bench/docker-compose.loadgen-smoke.yml` (three nodes, Postgres,
three NATS servers, two agents inside the compose network so the load
avoids Docker Desktop's port proxy), runs each through the conductor and
writes a report. `SMOKE_IMAGE` and `BUS` point it at another server
build. `SHAPES` picks the shape files (`SHAPES=""` runs none) and
`SCENARIOS` adds named scenario files, for instance
`SHAPES="" SCENARIOS=smoke-1pct FAULT=0 HOLD=3m bench/aws/local-smoke.sh`
for the run 0b smoke on the laptop (a scenario keeps its own scale unless
`SCALE` is set). The header of the script lists every variable.

### Quick start (laptop, against a local cluster)

    # three nodes on :8081-8083, debug listeners on :9091-9093
    ably-conductor run --scenario bench/scenarios/shape-d.toml \
      --scale 0.01 --ramp 30s --hold 60s --drain 10s \
      --endpoints localhost:8081,localhost:8082,localhost:8083 \
      --node-metrics http://localhost:9091/metrics,http://localhost:9092/metrics,http://localhost:9093/metrics \
      --local 2 --results results/

`--local N` spawns N agents on 127.0.0.1 with every role. In the cloud
the agents run on their own boxes and the conductor gets an inventory.

### What the generator does

- **Protocol.** A raw WebSocket client (gorilla/websocket, msgpack
  frames) that speaks only CONNECTED, ATTACH/ATTACHED, DETACH, MESSAGE,
  ACK/NACK, PRESENCE, HEARTBEAT, CLOSE, DISCONNECTED and ERROR
  (DESIGN.md §2.1). One goroutine per connection, small pooled buffers.
  A dead connection is detected by silence longer than the server's
  `maxIdleInterval` plus a margin.
- **Roles.** `subscriber` (connections, each holding one or more
  attachments, plus connection and channel churn), `rest-publisher`
  (keep-alive HTTP POSTs, open-loop schedule), `realtime-publisher`
  (MESSAGE frames on a small connection pool) and `presence` (members
  that enter, then leave and re-enter at a rate).
- **Assignment.** Every process builds the same plan from the scenario,
  multiplier, scale and run tag, and takes its slice by role index and
  count. Attachment *a* goes to connection *a mod connections*;
  connection *c* belongs to subscriber process *c mod count*; stream *l*
  of channel *ch* belongs to publisher process *(hash(ch)+l) mod count*.
  Connection *c* connects to node *c mod nodes*.
- **Streams.** A publish stream is sequential: one publish in flight,
  and a failure is retried with the same message id (the server's
  idempotency makes the retry safe, DESIGN.md §8) until it succeeds. A
  stream never skips a sequence number, so any missing one is loss.
  Publishes the schedule calls for while one is in flight queue in the
  stream's backlog; a slow server shows as backlog and an achieved rate
  below the offered rate.
- **Correctness** on a deterministic `sample_percent` of channels (plus
  the first channel of every class): each attachment checks that
  channelSerials strictly increase (starting after the ATTACHED attach
  point) and that each stream's sequence numbers arrive once, in order,
  with no gap. Counted kinds: `duplicate`, `gap`, `reorder`,
  `serial_regression`, `resume_gap` (a gap across a re-attach the server
  reported as RESUMED), `tail_loss` (a message a publisher saw
  acknowledged by the end of the hold that an attachment present
  throughout never received; the conductor computes it) and `attach_gap`
  (a message published after an attachment's attach point that never
  arrived before the first message the attachment did receive from that
  stream; the conductor computes it, see "What the correctness check
  sees"). The first ten of each process are kept verbatim. A resume the
  server declines (no RESUMED flag, DESIGN.md §4.3) is a signalled
  discontinuity: counted, not a violation.
- **Latency.** Each message carries its publisher's send time and node;
  the subscriber records one-way latency into log-linear histograms
  (within 1.6%, mergeable across processes) split into same-node and
  cross-node. Clocks must be synced (chrony). REST ACK latency is from
  the *scheduled* send time, retries included, so a saturated server
  cannot hide behind a slower schedule.
- **Churn is steady state.** Both kinds replace, never add, so after
  the ramp the generator's connections and attachments are flat. A
  connection drop is followed at once by a reconnect of the same
  connection. Channel churn uses churn slots: `channel_opens_per_sec` x
  `channel_lifetime` connections (spread evenly) each hold one churn
  channel from the ramp on, and each open detaches a slot's channel and
  attaches a never-used one, so opens/s equals detaches/s.
- **Resume.** A dropped connection (churn or failure) reconnects and
  re-attaches every channel with the last channelSerial it saw; the
  checker holds a RESUMED re-attach to continuity.

### `ably-loadgen`

    ably-loadgen serve [--listen :9200] [--metrics-listen :9101] [--role generator|publisher|all]
                       [--summary-dir DIR] [--addr-file FILE]

(`agent` is an alias.) On the cloud boxes (host networking; 9100 is
node-exporter): generator boxes run `ably-loadgen serve --listen=:9200
--metrics-listen=:9101`, REST publisher boxes the same with
`--role=publisher`. `--role` limits the jobs the agent accepts:
`generator` takes subscriber, realtime-publisher and presence jobs,
`publisher` takes rest-publisher jobs, `all` (the default) takes any.

| Endpoint | Purpose |
|---|---|
| `POST /v1/jobs` | start a job (body: JobSpec JSON) |
| `GET /v1/jobs`, `GET /v1/jobs/{id}` | status (connections, attached, acked, received, violations) |
| `GET /v1/jobs/{id}/summary` | the job's summary (202 while running) |
| `POST /v1/stop` | stop every running job (summaries are still written) |
| `DELETE /v1/jobs` | forget finished jobs |
| `GET /metrics` | Prometheus: `ably_loadgen_*` plus Go and process collectors |
| `GET /healthz` | liveness |

Environment: `ABLY_LOADGEN_LISTEN`, `ABLY_LOADGEN_METRICS_LISTEN`,
`ABLY_LOADGEN_ROLE`, `ABLY_LOADGEN_SUMMARY_DIR`. `GET /v1/info` returns
the accepted roles.

    ably-loadgen run --scenario FILE --role ROLE --index I --count N \
      --endpoints h:p,... [flags]      # one job, no conductor

| Flag | Default | Meaning |
|---|---|---|
| `--job FILE` | | a JobSpec JSON instead of the flags below |
| `--scenario FILE` | | scenario TOML |
| `--role` | subscriber | subscriber, rest-publisher, realtime-publisher, presence |
| `--index`, `--count` | 0, 1 | this process's slice of its role |
| `--endpoints` | localhost:8080 | nodes, same order everywhere (env `ABLY_LOADGEN_ENDPOINTS`) |
| `--key` | app.key:secret | API key (env `ABLY_SERVER_KEYS`) |
| `--multiplier`, `--scale` | scenario's | 1 or 2; 0.01 or 0.1 for smoke |
| `--run-tag` | random | shared by every process of a run; keeps channel names and message ids unique per run |
| `--start-at` / `--start-in` | now+2s | ramp start, the same on every process |
| `--ramp`, `--hold`, `--drain` | scenario's | overrides |
| `--workers` | 256 | REST publisher concurrency |
| `--realtime-conns` | 16 | realtime publisher connections |
| `--format` | msgpack | msgpack or json |
| `--summary FILE` | stdout | JSON summary |
| `--metrics-listen ADDR` | | serve /metrics during the run |

Exit status is 1 if the job saw any correctness violation.

#### Summary (`Summary`, one per job)

`connections` (target, opened, peak, open at the end of the hold,
connect failures, reconnects, churn and unplanned drops), `attachments`,
`publishes` (target, offered, dropped, sent, acked, retries, rejected,
unresolved, offered and achieved rates over the hold), `deliveries`
(received, in window, rate, negative latencies, foreign messages),
`presence`, `latency` (histograms: `delivery`, `delivery_cross_node`,
`delivery_same_node`, `rest_ack`, `rest_service`, `realtime_ack`,
`connect_attach`, `reconnect_attach`, `channel_open`, `presence_ack`;
each with count, min, max, mean, p50, p90, p99, p99.9 and the raw
buckets), `correctness` (checked messages, violations by kind, resumes,
discontinuities, first violations, per sampled channel records),
`streams` (publisher: last acknowledged sequence and the channelSerial
of every acknowledged sequence, per sampled stream; the serial log feeds
the attach-point check and is capped at 1.5M entries per process),
`correctness.attach_claims` (subscriber: each stream's first sequence
number on each attachment, with the attach point),
`resources` (the generator's own heap, stacks, RSS and goroutines every
10 s, and bytes per connection at the end of the ramp) and `errors`.

### `ably-conductor`

    ably-conductor plan --scenario FILE [--multiplier M] [--scale S] [--json]
    ably-conductor [run] --scenario FILE (--inventory FILE | --endpoints ... --agent ... | --local N) [flags]
    ably-conductor evaluate [--write] RUN_DIR
    ably-conductor report [--out FILE] RUN_DIR/summary.json ...

`run` is the default command, so bench/aws/60-run.sh's
`ably-conductor --scenario=/run-input/<file> --inventory=/run-input/inventory.json --results=/results --run-id=<id>`
is a run. `run` flags:

| Flag | Default | Meaning |
|---|---|---|
| `--scenario`, `--multiplier`, `--scale`, `--ramp`, `--hold`, `--drain` | | as for plan (`--hold 30m` for the headline) |
| `--inventory FILE` | | nodes and agents (below) |
| `--endpoints`, `--node-metrics` | | without an inventory: node host:port list and matching /metrics URLs |
| `--agent [roles=]URL` | | without an inventory: an agent, repeatable (roles comma-separated; default all) |
| `--local N` | 0 | spawn N local agents (`--loadgen-bin` to point at the binary) |
| `--key` | inventory, then `ABLY_SERVER_KEYS` | API key |
| `--run-id` | `<shape>-<mult>x-<UTC>` | with an explicit `--run-id` the run writes straight into `--results`; without one, into `<results>/<generated id>/` |
| `--run-tag` | random | see loadgen |
| `--results DIR` | results | the run directory (explicit `--run-id`) or the results root |
| `--node-vcpu`, `--node-memory-gb` | 0 | per-node size for the footprint when the inventory does not carry it |
| `--log FILE` | | append the one-line result (LOG.md) |
| `--state FILE` | | append the run to STATE.json's `runs` |
| `--start-delay` | 10s | job send to ramp start |
| `--poll` | 10s | status and node-metrics interval |
| `--fault-hook CMD`, `--fault-at D` | , 5m | shell command run D into the hold (kill a node, kill a NATS server); `RUN_ID` is in its environment |
| `--time-limit D` | planned length + 5m | hard limit: stops every agent, runs `--on-timeout`, verdict ABORTED |
| `--on-timeout CMD` | | for instance `bench/aws/90-teardown.sh` |
| `--env k=v` | | labels for the run record (bus, storage, instance types) |
| `--format` | msgpack | realtime wire format |
| `--server-idle-timeout D` | the scenario's `server_idle_timeout`, else 60s | the nodes' `--channel-idle-timeout`; node memory and goroutine growth are measured from hold start + D (0: from hold start) |

Exit status: 0 PASS, 1 FAIL or ABORTED, 2 usage.

#### Inventory

Two shapes are accepted. The one bench/aws/60-run.sh renders from STATE
(recognised by its `generators` or `api_key` keys):

    {"run_id": "...", "bus": "nats", "server_image": "...", "api_key": "app.key:secret",
     "nodes": [{"http": "http://10.0.1.11:8080", "ws": "ws://10.0.1.11:8080", "metrics": "http://10.0.1.11:6060/metrics"}],
     "nats": "nats://...,nats://...", 
     "generators": [{"agent": "10.0.2.31:9200", "metrics": "http://10.0.2.31:9101/metrics"}],
     "publishers": [{"agent": "10.0.3.41:9200", "metrics": "http://10.0.3.41:9101/metrics"}],
     "postgres": {"shards": [{"id": "...", "storage": "io2", "shard": 0, "endpoint": "..."}]},
     "prometheus": "http://127.0.0.1:9090"}

Generators take the subscriber, realtime-publisher and presence roles,
publishers the rest-publisher role (generators take it too when there
are no publishers). `bus`, the NATS server count, the shard count and
the storage go into the run record. Nodes may add `instance_type`,
`vcpu` and `memory_gb` for the footprint (or pass `--node-vcpu` and
`--node-memory-gb`).

The conductor's own shape, for anything else:

    {
      "key": "app.key:secret",
      "environment": {"bus": "nats", "storage": "rds-io2", "region": "eu-west-1"},
      "nodes": [
        {"name": "node-1", "endpoint": "10.0.1.11:8080", "metrics": "http://10.0.1.11:9090/metrics",
         "instance_type": "c7i.2xlarge", "vcpu": 8, "memory_gb": 16}
      ],
      "agents": [
        {"name": "gen-1", "url": "http://10.0.2.21:9200", "roles": ["subscriber", "realtime-publisher", "presence"]},
        {"name": "pub-1", "url": "http://10.0.3.31:9200", "roles": ["rest-publisher"], "workers": 2000}
      ]
    }

Node order is node identity: keep it the same for every run. The k
agents that list a role run jobs 0..k-1 of that role. `metrics` is the
node's `--debug-listen` address; without it memory and goroutine
flatness is reported as not measured. `vcpu` and `memory_gb` feed the
footprint.

#### Run record (`results/<run-id>/`)

- `plan.json`: scenario, multiplier, scale, derived totals, inventory
  (key removed), phase times.
- `agents/<job-id>.json`: every job's summary.
- `summary.json`: the merged result, node samples and stats, footprint,
  fault record, checks and verdict.
- `summary.md`: the same as tables.

Pass criteria (plan §8, overridable per scenario in `[pass]`): delivery
p50 <= 50 ms and p99 <= 250 ms, cross-node where there is such traffic
(p99 < 100 ms reported as stretch); REST ACK p99 <= 100 ms; connect plus
attach p99 <= 500 ms at the target churn; zero violations of every kind
on the sample (including `attach_gap`), with at least 90% of attach
claims settled (`min_attach_coverage`; reported, not gated, in a fault
run that succeeded); achieved publish rate >= 95% of offered and offered >=
95% of target (the generator kept up); no rejected publishes;
connections open at the end of the hold >= 99% of target; deliveries/s
>= 90% of the plan's; the generator's own connections and attachments
within ±3% from hold start to end (else node growth would measure the
generator); node RSS and goroutines grow <= 10% from the growth baseline
to the end of the hold (reported, not gated, in a fault run). The record sets the generator's connections
and attachments at hold start and end beside the nodes'
`ably_connections_open` and `ably_channels_bound`, and gives server
channels bound over generator attachments at the end of the hold. The footprint (vCPU and memory,
provisioned and used, per 100k connections, per 100k deliveries/s and
per 10k writes/s) is reported, not gated.

**Why the growth baseline is hold start plus the idle timeout.** A node
binds a channel when a subscriber attaches or a REST publish reaches it,
and releases it only after `--channel-idle-timeout` (default 60 s) with
no attachment and no publish. So the legitimate working set of bound
channels is the subscribers' channels plus one idle timeout's worth of
REST publish targets. At hold start that second part is still filling:
channels the ramp's publishers touched are bound, the hold's new targets
are being added, and the first evictions have not happened yet. Memory
and goroutines grow with it for one idle timeout and then plateau.
Measured from hold start, that fill reads as a leak (the 60 s smoke holds
showed node RSS up 30 to 50 percent). The conductor therefore scrapes the
nodes one second after hold start + idle timeout and measures growth
from the first sample at or after that point. The report prints
`ably_channels_bound` (summed over nodes) at hold start, at the baseline
and at the end, with RSS and goroutines at the baseline and the end, so
the plateau is visible. A hold not longer than the idle timeout has no
growth window: growth is reported as not measured and not gated, so a
run that must judge memory needs a hold of several idle timeouts (the
15-minute holds of plan §8 give fourteen minutes).

#### What the correctness check sees

"0 violations in N checked" is narrower than it reads. The check covers
the *sampled* channels only (the next section lists the rest), and within
them:

- **From the first message on.** An attachment learns a stream's sequence
  number from the first message it receives, so the per-attachment checks
  (gap, reorder, duplicate, serial regression) start there. Anything
  between the attach point and that first message used to be invisible.
  It is now settled by the **attach-point check**: each subscriber records
  `(channel, pubID, ATTACHED.channelSerial, first seq)` for every stream it
  sees on an attachment, each publisher records the channelSerial the
  server assigned to every acknowledged message of a sampled stream (from
  the REST response `serials` or the realtime ACK `res`), and the conductor
  finds the first sequence whose serial sorts after the attach point. An
  attachment whose first message is a later sequence lost the messages in
  between (`attach_gap`, counted per message). An earlier first sequence
  (a replay from before the attach point) is not a violation. Serials are
  compared as strings, the order the server uses (DESIGN.md §8), so no
  clock is involved. The attach point is the one from the last attach that
  was not a resume: an honoured resume keeps it, a declined one starts a
  new one.
- **What the attach-point check cannot settle** is reported as
  `unverifiable` and gated by coverage (at least 90% of claims settled
  outside a fault run, `min_attach_coverage`): a claim whose stream has no
  serial log (the publisher is not a generator of that stream, or the
  process reached the 1.5M-entry cap), whose first sequence lies beyond the
  log (the log ends with the last acknowledgement before the end of the
  hold, so an attachment made after that cannot be settled), or whose
  needed serial is unknown (an ACK that carried none). A run that settles
  none fails; it never passes on "0 of 0".
- **Residual blind spots.** A stream an attachment never received a single
  message from is invisible to the per-attachment and attach-point checks;
  only the tail check covers it, and only for streams still publishing at
  least `tail_margin` after the attachment. Messages are checked against
  the attach point the server reports: if the server reported an attach
  point later than where it really started delivering, messages in that
  stretch would not be seen as missing. A message published to a sampled
  channel by someone other than a generator stream is ignored.

`report` groups full-scale runs by bus, shape, multiplier, nodes and
shards: envelope with pass counts and run-to-run spread, footprint, and
the node and shard curves.

### Scenario format (`bench/scenarios/*.toml`)

Values are at 1x and full scale. `--multiplier` (1 or 2) and `--scale`
(0.01, 0.1) multiply them; each class says what absorbs the factor.

    name = "M"                 # short, no '|', ':' or spaces; part of channel names
    shape = "M"                # F, M or D
    description = "..."
    bus = "nats"               # recorded in the run record
    nodes = 10                 # node count (node curve parameter)
    shards = 1                 # Postgres shards (shard curve parameter)
    message_bytes = 470
    sample_percent = 5         # channels under the serial-continuity check
    server_idle_timeout = "60s" # the nodes' --channel-idle-timeout (growth baseline; absent or "0s" = 60s;
                                # to measure growth from hold start pass --server-idle-timeout 0)

    [timing]
    ramp = "10m"               # connections and publish rates rise linearly
    hold = "15m"               # the measurement window
    drain = "30s"              # subscribers keep reading after publishing stops

    [connections]
    count = 550000             # subscriber connections; attachments spread over them

    [churn]
    connects_per_sec = 1000    # drop and re-establish (half abrupt, half CLOSE)
    channel_opens_per_sec = 2400  # replace a churn channel with a never-used one
    channel_lifetime = "60s"   # mean churn-channel life: opens/s x lifetime churn slots
    resume = true              # re-attach with channelSerial, held to continuity

    [[class]]                  # repeat per class of channel
    name = "hot"               # no '-', '|', ':' or spaces
    channels = 1
    subscribers = 200000       # or subscribers_min/max + subscribers_dist
    # subscribers_dist = "uniform"   # each channel draws in [min, max]
    # subscribers_dist = "harmonic"  # channel i gets max/(i+1) clamped to [min, max]
    publish_rate = 0.5         # per channel; or publish_rate_total for the class
    publisher = "realtime"     # or "rest" (default)
    streams = 1                # sequential publishers per channel
    message_bytes = 470        # optional override
    scale_by = "subscribers"   # "channels" (default), "subscribers" or "rate"

    [presence]
    enabled = false
    channels = 500
    members_per_channel = 2000   # each member is its own connection
    events_per_sec = 1200        # leave+enter churn = 2 events
    subscribe = true             # members also receive presence

    [pass]                       # optional overrides of the plan §8 criteria
    delivery_p50 = "50ms"
    delivery_p99 = "250ms"
    rest_ack_p99 = "100ms"
    connect_attach_p99 = "500ms"
    min_achieved_ratio = 0.95
    max_memory_growth = 0.10
    max_goroutine_growth = 0.10
    max_connection_loss = 0.01
    tail_margin = "1s"
    min_delivery_ratio = 0.9     # deliveries/s measured over planned
    max_load_drift = 0.03        # generator connections and attachments, hold start to end

`ably-conductor plan` prints the derived connections, attachments,
channels, publishes/s, deliveries/s, fan-out, streams, churn and
presence for any multiplier and scale. At a smoke scale a fixed-size
channel's fan-out is clamped to the connection count (reported).

Committed scenarios: `idle-connections.toml` (memory per idle
connection), `shape-f.toml`, `shape-m.toml`, `shape-d.toml`
(plan §3 shapes; the 1x envelope is M for connections, channels and
deliveries and D for writes), `presence-m.toml` (run 6), and
`smoke-1pct.toml` and `smoke-10pct.toml` (run 0b: shapes M and D together
at 1% and 10%, about 5.5k connections and 590 publishes/s at 1%; a test
keeps their classes equal to the shape files'). The files are TOML, so
60-run.sh needs the extension: `60-run.sh smoke-1pct.toml`.

### Generator host tuning

At 200k connections per box: raise the open-file limit (`ulimit -n
1048576`, or `--ulimit nofile=1048576:1048576` for Docker), widen the
ephemeral range (`net.ipv4.ip_local_port_range = 1024 65535`; with ten
nodes that is 20k connections per node address, under the 64k limit per
destination), and raise `net.core.somaxconn` and
`net.ipv4.tcp_max_syn_backlog` on the nodes. Run with `--network host`
to avoid Docker NAT.
