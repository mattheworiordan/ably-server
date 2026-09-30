# Load generation for the scale proof

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

## Build

    go build -o bin/ ./cmd/ably-loadgen ./cmd/ably-conductor

Both are static Go binaries with no runtime dependencies.

## Quick start (laptop, against a local cluster)

    # three nodes on :8081-8083, debug listeners on :9091-9093
    ably-conductor run --scenario bench/aws/scenarios/shape-d.toml \
      --scale 0.01 --ramp 30s --hold 60s --drain 10s \
      --endpoints localhost:8081,localhost:8082,localhost:8083 \
      --node-metrics http://localhost:9091/metrics,http://localhost:9092/metrics,http://localhost:9093/metrics \
      --local 2 --results results/

`--local N` spawns N agents on 127.0.0.1 with every role. In the cloud
the agents run on their own boxes and the conductor gets an inventory.

## What the generator does

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
  reported as RESUMED) and `tail_loss` (a message a publisher saw
  acknowledged by the end of the hold that an attachment present
  throughout never received; the conductor computes it). The first ten
  of each process are kept verbatim. A resume the server declines (no
  RESUMED flag, DESIGN.md §4.3) is a signalled discontinuity: counted,
  not a violation.
- **Latency.** Each message carries its publisher's send time and node;
  the subscriber records one-way latency into log-linear histograms
  (within 1.6%, mergeable across processes) split into same-node and
  cross-node. Clocks must be synced (chrony). REST ACK latency is from
  the *scheduled* send time, retries included, so a saturated server
  cannot hide behind a slower schedule.
- **Resume.** A dropped connection (churn or failure) reconnects and
  re-attaches every channel with the last channelSerial it saw; the
  checker holds a RESUMED re-attach to continuity.

## `ably-loadgen`

    ably-loadgen agent [--listen :7070] [--summary-dir DIR] [--addr-file FILE]

| Endpoint | Purpose |
|---|---|
| `POST /v1/jobs` | start a job (body: JobSpec JSON) |
| `GET /v1/jobs`, `GET /v1/jobs/{id}` | status (connections, attached, acked, received, violations) |
| `GET /v1/jobs/{id}/summary` | the job's summary (202 while running) |
| `POST /v1/stop` | stop every running job (summaries are still written) |
| `DELETE /v1/jobs` | forget finished jobs |
| `GET /metrics` | Prometheus: `ably_loadgen_*` plus Go and process collectors |
| `GET /healthz` | liveness |

Environment: `ABLY_LOADGEN_LISTEN`, `ABLY_LOADGEN_SUMMARY_DIR`.

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

### Summary (`Summary`, one per job)

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
`streams` (publisher: last acknowledged sequence per sampled stream),
`resources` (the generator's own heap, stacks, RSS and goroutines every
10 s, and bytes per connection at the end of the ramp) and `errors`.

## `ably-conductor`

    ably-conductor plan --scenario FILE [--multiplier M] [--scale S] [--json]
    ably-conductor run  --scenario FILE (--inventory FILE | --endpoints ... --agent ... | --local N) [flags]
    ably-conductor evaluate [--write] RUN_DIR
    ably-conductor report [--out FILE] RUN_DIR/summary.json ...

`run` flags:

| Flag | Default | Meaning |
|---|---|---|
| `--scenario`, `--multiplier`, `--scale`, `--ramp`, `--hold`, `--drain` | | as for plan (`--hold 30m` for the headline) |
| `--inventory FILE` | | nodes and agents (below) |
| `--endpoints`, `--node-metrics` | | without an inventory: node host:port list and matching /metrics URLs |
| `--agent [roles=]URL` | | without an inventory: an agent, repeatable (roles comma-separated; default all) |
| `--local N` | 0 | spawn N local agents (`--loadgen-bin` to point at the binary) |
| `--key` | inventory, then `ABLY_SERVER_KEYS` | API key |
| `--run-id` | `<shape>-<mult>x-<UTC>` | results go to `<results>/<run-id>/` |
| `--run-tag` | random | see loadgen |
| `--results DIR` | results | results root |
| `--log FILE` | | append the one-line result (LOG.md) |
| `--state FILE` | | append the run to STATE.json's `runs` |
| `--start-delay` | 10s | job send to ramp start |
| `--poll` | 10s | status and node-metrics interval |
| `--fault-hook CMD`, `--fault-at D` | , 5m | shell command run D into the hold (kill a node, kill a NATS server); `RUN_ID` is in its environment |
| `--time-limit D` | planned length + 5m | hard limit: stops every agent, runs `--on-timeout`, verdict ABORTED |
| `--on-timeout CMD` | | for instance `bench/aws/90-teardown.sh` |
| `--env k=v` | | labels for the run record (bus, storage, instance types) |
| `--format` | msgpack | realtime wire format |

Exit status: 0 PASS, 1 FAIL or ABORTED, 2 usage.

### Inventory

    {
      "key": "app.key:secret",
      "environment": {"bus": "nats", "storage": "rds-io2", "region": "eu-west-1"},
      "nodes": [
        {"name": "node-1", "endpoint": "10.0.1.11:8080", "metrics": "http://10.0.1.11:9090/metrics",
         "instance_type": "c7i.2xlarge", "vcpu": 8, "memory_gb": 16}
      ],
      "agents": [
        {"name": "gen-1", "url": "http://10.0.2.21:7070", "roles": ["subscriber", "realtime-publisher", "presence"]},
        {"name": "pub-1", "url": "http://10.0.3.31:7070", "roles": ["rest-publisher"], "workers": 2000}
      ]
    }

Node order is node identity: keep it the same for every run. The k
agents that list a role run jobs 0..k-1 of that role. `metrics` is the
node's `--debug-listen` address; without it memory and goroutine
flatness is reported as not measured. `vcpu` and `memory_gb` feed the
footprint.

### Run record (`results/<run-id>/`)

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
on the sample; achieved publish rate >= 95% of offered and offered >=
95% of target (the generator kept up); no rejected publishes;
connections open at the end of the hold >= 99% of target; node RSS and
goroutines grow <= 10% across the hold. The footprint (vCPU and memory,
provisioned and used, per 100k connections, per 100k deliveries/s and
per 10k writes/s) is reported, not gated.

`report` groups full-scale runs by bus, shape, multiplier, nodes and
shards: envelope with pass counts and run-to-run spread, footprint, and
the node and shard curves.

## Scenario format (`scenarios/*.toml`)

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

    [timing]
    ramp = "10m"               # connections and publish rates rise linearly
    hold = "15m"               # the measurement window
    drain = "30s"              # subscribers keep reading after publishing stops

    [connections]
    count = 550000             # subscriber connections; attachments spread over them

    [churn]
    connects_per_sec = 1000    # drop and re-establish (half abrupt, half CLOSE)
    channel_opens_per_sec = 2400  # attach never-used channels
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

`ably-conductor plan` prints the derived connections, attachments,
channels, publishes/s, deliveries/s, fan-out, streams, churn and
presence for any multiplier and scale. At a smoke scale a fixed-size
channel's fan-out is clamped to the connection count (reported).

Committed scenarios: `shape-f.toml`, `shape-m.toml`, `shape-d.toml`
(plan §3 shapes; the 1x envelope is M for connections, channels and
deliveries and D for writes) and `presence-m.toml` (run 6).

## Generator host tuning

At 200k connections per box: raise the open-file limit (`ulimit -n
1048576`, or `--ulimit nofile=1048576:1048576` for Docker), widen the
ephemeral range (`net.ipv4.ip_local_port_range = 1024 65535`; with ten
nodes that is 20k connections per node address, under the 64k limit per
destination), and raise `net.core.somaxconn` and
`net.ipv4.tcp_max_syn_backlog` on the nodes. Run with `--network host`
to avoid Docker NAT.
