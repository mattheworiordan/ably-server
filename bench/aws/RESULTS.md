# Results

Anonymised results of the scale proof run on AWS on 30 September and
1 October 2026. Nothing here names a customer or an AWS account: workloads
are called shape F, shape M and shape D, and the target is "the largest
account on each axis". **1x** is that account's peak hour on the axis the
shape stresses; **2x** doubles it. The shapes are the scenario files in
[`bench/scenarios/`](../scenarios/); the reproduction steps are in
[RUNBOOK.md](RUNBOOK.md).

Every number below carries the run id it comes from. A run id names a
results folder (`run-<UTC start>-<scenario>`) with the conductor's
`summary.md`, `summary.json`, node metrics and `pg_stat_statements`;
those folders are kept outside the repository. A **gate** is one of the
pass criteria fixed before the first run (delivery p50 <= 50 ms and p99
<= 250 ms cross-node, REST ACK p99 <= 100 ms, connect+attach p99 <=
500 ms, zero loss, duplicates or reorders on the checked sample, node
memory growth <= 10% over the hold, and the rate checks). A **probe** is
a short run to find a ceiling; it is not quoted as a result.

Read "How these numbers were judged" at the end before quoting any of
them: the gates have been tightened since these runs, and none of the
runs was on the current code.

## Which code each run was on

| Code | What it is | Runs |
|---|---|---|
| `90dfa15` | `main`, the shipped code: `pg_notify` per publish, no other bus | run 1a |
| `6cb2713` | night-one integration: the `postgres` and `nats` buses, publish batching (4 lanes by default), write path before the day-two fixes | smoke, 1b, 2, 3, 4, 5, 6, 7 |
| `1e8ebff` | `6cb2713` plus channel sharding | run 8 (night one) |
| `4942d5c` | the write-path fixes (write-only REST publish, sweep scope), for their A/B | WS10 A/B |
| `d46d837` | the presence-path fixes (local member set, presence batching), for their A/B | WS11 A/B |
| `ffcb00a` | day-two merge of the two above | day-two shape D and M runs on fleets A and B |
| `c71ad76` | `ffcb00a` plus the per-node presence lease, for its A/B | WS12 A/B |
| `272012c` | day-two merged tip: `ffcb00a` plus the presence lease, the bounded per-attachment append set and the delivery-stage timings | fleet C: shape M 1x, 5x, 7x probe, presence |
| `7a1899d` | fleet D (running when this file was written): the hardening branch at `6535b12` (continuity, presence liveness, bus trust, bounds, harness gates) plus a bounded fan-out pool and presence attach timing. It does not have the retired settings, the inferred bus or the review fixes `2e03e05` and `96bc3f4`, and its lane default is still 4, so fleet D passes `--publish-lanes=2` | none reported yet |

**None of these is the hardened tip `96bc3f4`**, which adds to `7a1899d`'s
hardening the retirement of six A/B settings, the inferred bus, the lane
default of 2 and four review fixes, and does not have the fan-out pool.
Every run on an image older than `ffcb00a` predates the write-path fixes.

## Summary

The proof was organised around three messages.

1. **The objection was argued at a scale that does not exist.** The
   evaluation it answers planned capacity for 200 million deliveries a
   second. The largest account's peak hour is shape M's 1x envelope:
   550,000 connections, about 7,000 publishes and 120,000 deliveries a
   second ([shape-m.toml](../scenarios/shape-m.toml)). The proof uses that
   target, doubled, and then goes past it.
2. **The scale that is needed is reachable with a bounded amount of work
   on a small footprint.** The shipped code falls over at half the
   largest account's publish rate: 1,881 publishes a second achieved of
   3,513, delivery p99 8,061 ms, with the nodes using 3.9 of 80 cores
   (`run-20260930T184634Z-shape-m`, `90dfa15`). With the NATS bus and the
   day-two fixes, 20 nodes on one Postgres primary carried shape M at 2x
   (1.1 million connections, 14,013 publishes and 239,743 deliveries a
   second, 0 violations in 422,361,996 checked) and failed only latency
   and memory gates (`run-20261001T082046Z-shape-m`, `ffcb00a`). One
   primary took shape D's full 1x write load, 51,912 of 52,000 writes a
   second, at delivery p99 48.1 ms, failing the REST ACK p99 and memory
   gates (`run-20261001T071536Z-shape-d`, `ffcb00a`).
3. **There is a measured route to more.** Channel sharding: 8 Postgres
   shards carried shape D at 2x, 103,990 of 104,000 writes a second, each
   shard 50% to 57% busy (`run-20261001T082139Z-shape-d`, `ffcb00a`); 4
   shards and 28 nodes carried shape M at 5x, 2,749,992 connections,
   35,013 publishes and 599,658 deliveries a second, 0 violations in
   598,842,283 checked (`run-20261001T141856Z-shape-m`, `272012c`). A 7x
   probe was not carried (`run-20261001T150305Z-shape-m`), so on that
   footprint the ceiling is between 5x and 7x.

What still fails: the shape M delivery p99 (274.4 ms against 250 at 1x on
`272012c`, 507.9 ms at 5x), REST ACK p99 at scale (1,262 ms for shape D
1x on one primary), node memory growth on
several runs, and the presence attach tail (below). Presence at 2x, shape
F, Postgres failover and the fault runs on the fixed code were not run.

## Deployment sizes: the measured envelope at 1x and 2x

"Start with one Postgres, add NATS when you need it, add shards when one
primary's write rate is the limit." Run-to-run spread was measured once:
shape M 1x three times on `6cb2713` gave delivery p99 450.6, 434 and
462.8 ms (`run-20260930T195404Z-shape-m`, `run-20260930T223934Z-shape-m`,
`run-20260930T231121Z-shape-m`). Every other row is a single run.

### Small: nodes and one Postgres (`--bus=postgres`)

| Run | Code | Load | Result |
|---|---|---|---|
| `run-20260930T192139Z-shape-m` | `6cb2713` | shape M at 0.25x: 137,500 connections, 1,763 publishes/s, 10 nodes | Load carried (1,763 of 1,763 publishes/s, 29,995 deliveries/s), 0 violations of 30.0M; FAIL on delivery p50 54.8 ms and p99 438 ms, REST ACK p99 385 ms, memory +17.8%; Postgres 86% busy, its top statement the bus sweep |
| `run-20261001T001459Z-shape-m` | `6cb2713` | the same with `--bus-sweep-interval=60s` | 0 violations of 29.95M; delivery p50 44.5 ms, p99 290.8 ms, REST ACK p99 258.0 ms, memory +14.9%: FAIL on p99, REST ACK and memory; Postgres 66.8% |

This size was not run at 1x or 2x, and not on any code with the day-two
fixes. It is the least measured size and, with no `--bus` set and only
a DSN, the default.

### Large: nodes, one Postgres and three NATS servers (`--bus=nats`)

| Run | Code | Load | Result |
|---|---|---|---|
| `run-20261001T122159Z-shape-m` | `272012c` | shape M 1x, 10 nodes | 549,997 of 550,000 connections, 7,013 publishes/s, 119,973 deliveries/s, 0 violations of 119,863,249; delivery p50 41.0 ms, p99 274.4 ms (FAIL, gate 250); REST ACK p99 48.1 ms; connect+attach p99 143.4 ms; memory +5.9%; Postgres 54.4% |
| `run-20261001T074613Z-shape-m` | `ffcb00a` | shape M 1x, 10 nodes | 0 violations of 119,807,299; delivery p50 51.7 ms, p99 438.3 ms; REST ACK p99 186.4 ms; memory +13.4%; Postgres 51.5% |
| `run-20261001T082046Z-shape-m` | `ffcb00a` | shape M 2x, 20 nodes, 30-minute hold | 1,100,000 connections, 14,013 of 14,013 publishes/s, 239,743 deliveries/s, 0 violations of 422,361,996; delivery p50 54.3 ms, p99 483.3 ms; REST ACK p99 260.1 ms; memory +13.9%; Postgres 85.8%, nodes 13.8% |
| `run-20261001T071536Z-shape-d` | `ffcb00a` | shape D 1x, 10 nodes | 51,912 of 52,000 writes/s, 0 violations of 3,368,776; delivery p50 17.9 ms, p99 48.1 ms; REST ACK p50 19.2 ms, p99 1,262 ms (FAIL); memory +22.9% (FAIL); Postgres 78.5% |
| `run-20261001T091733Z-shape-d` | `ffcb00a` | shape D 2x, 20 nodes | Not carried: 43,053 of 104,000 writes/s, REST ACK p99 6,816 ms, Postgres 94.1%, 0 violations of 2,378,462 |
| `run-20261001T095851Z-shape-m` | `ffcb00a` | shape M 3x, 20 nodes | Not carried: 18,890 of 21,013 publishes/s, Postgres 93.2%; connections stopped at 1,572,840 because the load generators ran with the kernel's default connection-tracking table, so this step is harness-capped and not a clean measurement |

On one primary the ceiling for shape M lies between 2x and 3x, and for
shape D between 1x and 2x. The shape D 2x run committed fewer writes
than the 1x run because twice the nodes means twice the committers (7.34
messages per commit against 12.7, `run-20261001T091733Z-shape-d` and
`run-20261001T071536Z-shape-d`).

### Very large: Postgres sharded by channel

| Run | Code | Load | Result |
|---|---|---|---|
| `run-20261001T074224Z-shape-d` | `ffcb00a` | shape D 1x, 10 nodes, 4 shards | 52,000 of 52,000 writes/s, 0 violations of 3.39M; delivery p50 5.4 ms, p99 8.7 ms; REST ACK p99 11.5 ms; FAIL on memory only (+21.5%); shards 51.7% to 58.6% |
| `run-20261001T082139Z-shape-d` | `ffcb00a` | shape D 2x, 10 nodes, 8 shards | 103,990 of 104,000 writes/s, 0 violations of 6.78M; delivery p50 7.7 ms, p99 55.3 ms; REST ACK p99 190.5 ms (FAIL); memory +24.1% (FAIL); shards 50.0% to 56.9% |
| `run-20261001T141856Z-shape-m` | `272012c` | **shape M 5x**, 28 nodes, 4 shards, 14 generators | 2,749,992 of 2,750,000 connections, 35,013 of 35,013 publishes/s, 599,658 deliveries/s, 0 violations of 598,842,283; delivery p50 53.8 ms, p99 507.9 ms; REST ACK p99 380.9 ms; memory +10.5%: FAIL on those four; shards 60.6% to 70.4%, nodes 18.4% (25.8% the busiest), NATS 18.9% |
| `run-20261001T150305Z-shape-m` | `272012c` | shape M 7x probe (5-minute hold) | Not carried: 3,849,788 connections with 104,110 unplanned drops, 47,812 of 49,013 publishes/s, delivery p99 950.3 ms, shards 76% to 81% |
| `run-20261001T005237Z-shape-d` | `1e8ebff` | shape D 1x, 10 nodes, 8 shards at 2,500 IOPS each | PASS on every gate: 52,000 writes/s, delivery p50 4.0 ms, p99 14.5 ms, REST ACK p99 16.1 ms, 0 violations of 3.38M |

The 5x run's stage timings put the delivery tail inside each node: the
time from a cm's append to its frame being queued had p99 486.8 ms on a
channel with 35,737 local subscribers, while the socket write wait p99
was 0.78 ms and the bus append p99 0.099 ms
(`run-20261001T141856Z-shape-m`).

## The two scaling curves

**Node curve** (shape M 1x, one primary, `6cb2713`, default flags):

| Nodes | Run | Delivery p50 / p99 | REST ACK p99 | Cores used | Postgres busy |
|---|---|---|---|---|---|
| 5 | `run-20260930T231828Z-shape-m` | 85.0 / 1,016 ms | 901 ms | 10.9 | 70% |
| 10 | `run-20260930T195404Z-shape-m` | 63.5 / 450.6 ms | 299 ms | 11.4 | 94.7% |
| 20 | `run-20260930T224333Z-shape-m` | 41.5 / 237.6 ms | 147 ms | 11.6 | 98.6% |

More nodes lower the latency at the same total CPU; Postgres stays the
ceiling. All three carried the load with 0 violations of about 120M.

**Shard curve** (shape D 1x, 10 nodes, `1e8ebff`, default flags):

| Shards | Run | Writes/s of 52,000 | REST ACK p99 | Shard CPU |
|---|---|---|---|---|
| 1 | `run-20260930T235557Z-shape-d` | 19,305 | 7,274 ms | 97.6% |
| 2 | `run-20261001T003108Z-shape-d` | 32,875 | 4,260 ms | 98.2%, 98.2% |
| 4 | `run-20261001T010914Z-shape-d` | 52,000 | 33.3 ms | 92.7% to 97.4% |
| 8 | `run-20261001T005237Z-shape-d` | 52,000 | 16.1 ms | 55.1% to 60.6% |

On the fixed write path (`ffcb00a`) one primary carries what took 4 shards
on this code (`run-20261001T071536Z-shape-d`, above).

**Lanes on one primary** (shape D 1x, 10 nodes, 5-minute probes): 4 lanes
37,966 writes/s (`run-20261001T052440Z-shape-d`, `4942d5c`), 3 lanes
43,026 (`run-20261001T070031Z-shape-d`, `ffcb00a`), 2 lanes 51,863
(`run-20261001T064502Z-shape-d`, `ffcb00a`), 1 lane 46,556, capped per
node (`run-20261001T061109Z-shape-d`, `4942d5c`). Each is one probe.

## Postgres alone (run 0a)

`pgbench` against Postgres 17 on the same r7i.4xlarge and EBS volumes,
`synchronous_commit = on`, no ably-server involved (summary `summary-0a`,
raw runs `0a-io2-20260930T153824Z` and `0a-gp3-20260930T161707Z`).
Messages a second on io2 at 20,000 IOPS:

| Transaction | 1 client | 8 | 32 | 128 | p99 at 128 clients |
|---|---:|---:|---:|---:|---:|
| trivial commit | 940 | 5,658 | 31,180 | 111,430 | 2.36 ms |
| the shipped publish (seven statements) | 284 | 1,581 | 1,667 | 2,179 | 68.15 ms |
| pipelined publish | 824 | 5,342 | 26,557 | 58,480 | 8.90 ms |
| pipelined publish with `pg_notify` | 854 | 1,355 | 1,360 | 1,699 | 97.84 ms |
| batches of 10 | 6,984 | 41,711 | 99,193 | 120,895 | 31.44 ms |
| batches of 30 | 12,955 | 72,489 | 153,274 | 144,285 | 68.55 ms |
| batches of 100 | 21,932 | 116,738 | 167,129 | 153,069 | 199.53 ms |

A `pg_notify` per transaction caps the rate near 1,700 a second whatever
the client count; batching is what lifts one primary to 150,000 messages
a second (batches of 30 at 32 clients: p99 13.98 ms). gp3 at 12,000 IOPS
was 0.4 to 0.7 times io2 unbatched and 0.75 to 0.85 times batched; the
shipped publish managed 880 a second at 128 clients.

## Presence (run 6)

Shape M's presence load: 500 rooms of 2,000 members, 1,000,000
connections, one member per connection, 10 nodes, one primary, NATS.

| Run | Code | Result |
|---|---|---|
| `run-20260930T221246Z-presence-m` | `6cb2713` | FAIL: 786,422 connections (the generators' connection-tracking table was full), connect+attach p50 41.4 s, p99 134 s; presence ACK p99 148.9 s; nodes 71.8 of 80 cores |
| `run-20261001T055240Z-presence-m` | `d46d837` | 1,000,000 connections; connect+attach p50 49.7 ms, p99 2,850.8 ms (FAIL); presence ACK p50 3,768 ms, p99 23.9 s |
| `run-20261001T085947Z-presence-m` | `c71ad76` | Per-node lease: connect+attach p99 3,211 ms (FAIL); presence ACK p50 130 ms, p99 950 ms; blocked lock edges 1.0 per sample |
| `run-20261001T092800Z-presence-m` | `c71ad76` with the per-member lease | Same image, control: presence ACK p50 6,029 ms, p99 35.7 s; blocked lock edges 37.6 per sample |
| `run-20261001T125523Z-presence-m` | `272012c` | connect+attach p50 100.4 ms, p99 6,488 ms (FAIL); presence ACK p50 73.7 ms, p99 2,589 ms; 547 unplanned drops; generators 83% CPU |
| `run-20261001T132306Z-presence-m` | `272012c` | Repeat: connect+attach p99 5,374 ms (FAIL); presence ACK p50 79.9 ms, p99 975 ms; 851 unplanned drops; generators 82% CPU |

The write side is fixed: the per-node lease A/B on one image cut presence
ACK p50 from 6,029 ms to 130 ms (`run-20261001T092800Z-presence-m`,
`run-20261001T085947Z-presence-m`). **The attach tail regressed on the
merged image**: connect+attach p99 5,374 and 6,488 ms on `272012c`
against 3,211 ms on `c71ad76`, with nodes logging write timeouts (26 and
25) and skipped presence syncs (141 and 246). Its status: not
attributed. The generators were at 82% to 83% CPU, so a generator-side
share cannot be ruled out; the earlier presence runs did not record
generator CPU. A laptop could not reproduce the fleet's room-fill rate;
the fleet evidence points at the ramp (every drop happened during it, the
skipped syncs were on connections already closing, and the generators
stalled). `7a1899d` adds timings for each attach stage and one fix (a
shared member-set read is no longer cancelled by the attach that started
it), and fleet D is to repeat the run with six generators; no result
yet. These presence
runs checked no messages and no member set (the correctness row reads "0
of 0 checked"), so they say nothing about presence correctness.

## Failure injection (run 5)

Shape M 1x, 10 nodes, NATS, `6cb2713`, fault five minutes into the hold.

| Run | Fault | Result |
|---|---|---|
| `run-20260930T203111Z-shape-m` | one node's server killed | 0 violations of 120.0M (no gap, duplicate, reorder, resume gap or tail loss); 54,970 connections dropped and all reconnected; reconnect+attach p99 10.5 s |
| `run-20260930T213902Z-shape-m` | one of three NATS servers killed | 0 violations of 120.1M; nodes reconnected to NATS in 3 s and the delivery rate was back within about 10 s; 0 unplanned connection drops |
| `run-20260930T210527Z-shape-m` | one NATS server killed (an earlier attempt) | Not a clean run: the ramp degraded before the fault on a database that was not fresh (379,700 of 550,000 connections at hold start). One duplicate and one serial regression on one channel, cause not found. Not used as the verdict |

Both clean runs failed only the latency gates shape M 1x was already
failing without a fault on that code. No fault was injected on code with
the day-two fixes, and no presence run had a fault.

## Footprint

From the conductor's footprint table. "Provisioned" is the vCPU of the
node instances; "used" is the cores the nodes used on average over the
hold. Memory is node RSS at the end of the hold.

| Run | Code | Provisioned vCPU / used cores per 100k connections | Used cores per 100k deliveries/s | Used cores per 10k writes/s | RSS per 100k connections |
|---|---|---|---|---|---|
| `run-20261001T122159Z-shape-m` (M 1x) | `272012c` | 14.55 / 1.31 | 6.01 | 10.29 | 6.63 GB |
| `run-20261001T082046Z-shape-m` (M 2x) | `ffcb00a` | 14.55 / 1.80 | 8.27 | 14.15 | 7.15 GB |
| `run-20261001T141856Z-shape-m` (M 5x) | `272012c` | 8.15 / 1.33 | 6.09 | 10.44 | 6.59 GB |
| `run-20261001T071536Z-shape-d` (D 1x) | `ffcb00a` | 24.24 / 4.25 | 42.17 | 2.70 | 6.13 GB |
| `run-20261001T074224Z-shape-d` (D 1x, 4 shards) | `ffcb00a` | 24.24 / 5.06 | 50.05 | 3.21 | 6.33 GB |
| `run-20261001T082139Z-shape-d` (D 2x, 8 shards) | `ffcb00a` | 12.12 / 5.13 | 50.75 | 3.25 | 7.16 GB |
| `run-20261001T125523Z-presence-m` | `272012c` | 8.00 / 4.41 | n/a | n/a | 5.99 GB |

## Configuration

- **Instances.** Server nodes c7i.2xlarge (8 vCPU, 16 GiB), `GOMEMLIMIT=13GiB`.
  Postgres r7i.4xlarge on EBS io2, 1,000 GB at 20,000 IOPS (run 4 also
  gp3 at 12,000 IOPS); the night-one 8-shard runs at 2,500 IOPS a shard,
  the day-two 8-shard run at 10,000, the 4-shard runs at 20,000. Postgres
  17 (the `postgres:17` image) in Docker, `synchronous_commit = on`,
  `shared_buffers` a quarter of RAM (32 GB), the rest in
  `templates/postgresql.conf`. Three
  NATS 2.11 servers on c7i.2xlarge. Load generators c7i.8xlarge (3 or 6;
  14 at 5x and 7x), REST publishers c7i.4xlarge (2 or 3). One
  Availability Zone, one VPC.
- **Bus.** Every run passed `--bus` explicitly (`nats` unless the table
  says `postgres`), except run 1a, whose `main` image has only
  `pg_notify`.
- **Lanes.** Every image after `main` had a lane default of 4. The night-one runs
  (`6cb2713`, `1e8ebff`) used that default. Every quoted day-two write
  and shape M result (`ffcb00a`, `272012c`) ran with
  `--publish-lanes=2`; fleet D passes it too. The lane rows above say
  their own setting.
- **Other flags.** `--bus-sweep-interval=60s` only on the runs marked
  sweep-60 (`run-20260930T220627Z-shape-m`, `run-20260930T234303Z-shape-m`,
  `run-20261001T001459Z-shape-m`). The WS10 A/B rows set
  `--bus-sweep-scope=bound --bus-sweep-interval=5s`
  (`run-20261001T054102Z-shape-d`), `--publish-bind-on-write=true`
  (`run-20261001T055600Z-shape-d`) or `--publish-linger-min=3ms`
  (`run-20261001T062630Z-shape-d`); the WS11 A/B rows
  `--presence-sync-source=store` (`run-20261001T061827Z-presence-m`) or
  `--presence-batching=false` (`run-20261001T064259Z-presence-m`); the
  WS12 control `--presence-lease-mode=member`
  (`run-20261001T092800Z-presence-m`). Those six settings no longer exist
  (DESIGN.md §9 "Removed settings").
- **Record.** Each run's STATE snapshot records the instance types, image
  ids, the Postgres version and the settings read back from the database;
  `summary.md` names the server image. Runs before the harness changes do
  not record the node flags; newer runs print the flags the nodes report.

## What was not tested

- The hardened tip `96bc3f4` itself, and the inferred `--bus` default.
- Shape F. Presence at 2x, presence with a fault, and presence member-set
  correctness at scale.
- The small size (`--bus=postgres`) on any code with the day-two fixes.
- Failure injection on any code with the day-two fixes; a bus outage
  longer than the retention window at scale; a presence lease lapse at
  scale; reaper LEAVE throughput across 100,000 channels.
- Postgres failover, and RDS (Postgres ran on EC2 and EBS).
- gp3 storage on the fixed code.
- From the plan's "harden later" list: JetStream, rate limits and quotas,
  token revocation, TLS in the binary, auth beyond HS256, Helm and
  Kubernetes artefacts, mixed-version upgrades, an SDK CI matrix, delta,
  filtering, push and LiveObjects parity, a security review, resharding.

## How these numbers were judged

These runs were judged by the gates in force on 30 September and
1 October. The harness gates have since been tightened: deliveries must
reach 99% of plan outside a fault (it was 90%, which let up to 10% loss
on unsampled channels pass); load generator and publisher CPU must stay
under 70%; each box's clock offset against NTP must be within 5 ms;
unresolved publishes must be zero and retries under 1%; every attachment's
first message is now checked against the publishers' serial log, with a
coverage gate on those attach points; and presence runs compare the
member set over REST with each member's own record at the end of the
hold. Several runs above would not have been judged the same way: the
presence runs checked nothing, and the merged-image presence runs had
generators at 82% to 83% CPU. A re-run on the hardened tip with the
tightened gates is the next step.
