# Results

Anonymised results of the scale proof run on AWS from 30 September to
2 October 2026. Nothing here names a customer or an AWS account: workloads
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
them. The runs in "Final results" below are on `83721b1`, the code in
this branch, and were judged by the tightened gates. The runs on older
code in the later sections were judged by the gates in force at the
time.

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
| `7a1899d` | fleet D: the hardening branch at `6535b12` (continuity, presence liveness, bus trust, bounds, harness gates) plus a bounded fan-out pool and presence attach timing. It does not have the retired settings, the inferred bus or the later review fixes, and its lane default is 4, so fleet D passed `--publish-lanes=2` | fleet D: shape M 1x, 5x, 18x, two cut 10x attempts, presence |
| `83721b1` | **the final code, this branch**: `7a1899d` merged with the finished hardening branch (review fixes, the presence liveness rework, six A/B settings retired, the inferred bus, lane default 2) | fleets E1 and E2: shape M 1x, presence, a node kill, shape M 5x and 10x |

Every run on an image older than `ffcb00a` predates the write-path fixes.
No shape D (write) run exists on any image after `ffcb00a`.

## Final results on `83721b1`

Fleets E1 and E2, 2 October 2026. Every run: `--bus=nats`,
`--publish-lanes=2` (confirmed on every node), three NATS servers on
c7i.8xlarge, a fresh database, a 10-minute ramp (5 minutes for
presence) and a 15-minute hold, judged by the tightened gates with no
override. **Carried** means every throughput, connection and
correctness gate passed; it is not a pass.

| Run | Load and footprint | Result | Verdict |
|---|---|---|---|
| `run-20261002T081123Z-shape-m` | **shape M 1x**, the largest account: 550,000 connections, 7,013 publishes/s, about 120,000 deliveries/s; 10 nodes, one Postgres, 6 generators | 549,999 connections at the end of the hold, 0 unplanned drops; 0 violations of 120,013,190; delivery p50 31.0 ms, p99 153.6 ms; REST ACK p99 36.4 ms; connect+attach p99 58.9 ms; memory +5.5% | **PASS, every gate** |
| `run-20261002T085134Z-presence-m` | **presence 1x**: 500 rooms of 2,000 members, 1,000,000 connections; 10 nodes, 6 generators (53% mean CPU) | connect+attach p99 98.3 ms; presence ACK p99 294.9 ms; 0 NACKs; 0 member-set mismatches over 48,000 members compared over REST; memory +3.0% | **PASS, every gate** |
| `run-20261002T092020Z-shape-m` | shape M 1x with one node's server killed 5 minutes into the hold | 0 violations of 120,000,022; all 54,984 dropped connections re-established; 0 rejected or unresolved publishes; reconnect+attach p99 10,616.8 ms | **FAIL on one gate, publish retries 4.93% (limit 1%)**: a harness artefact (see "Unexplained and open") |
| `run-20261002T081515Z-shape-m` | **shape M 5x**: 2,750,000 connections, 35,013 publishes/s, about 600,000 deliveries/s; 28 nodes, 4 shards, 14 generators | 2,749,932 connections; 0 violations of 600,058,302; delivery p50 61.4 ms, p99 389.1 ms; REST ACK p99 274.4 ms; connect+attach p99 208.9 ms; memory +9.4% | **Carried with zero loss; FAIL on three latency gates** (delivery p50 by 11.4 ms, p99 by 139.1 ms, REST ACK p99 by 174.4 ms) |
| `run-20261002T091418Z-shape-m` | **shape M 10x**: 5,500,000 connections, 70,013 publishes/s, about 1,200,000 deliveries/s; 55 nodes, 8 shards, 14 generators | 5,499,528 connections, 0 unplanned drops; 70,013 publishes/s, 1,199,387 deliveries/s; 0 violations of 1,200,085,165; delivery p50 85.0 ms, p99 598.0 ms; REST ACK p99 335.9 ms; connect+attach p99 581.6 ms; reconnect+attach p99 1,007.6 ms; memory +12.5% | **Carried with zero loss; FAIL on six gates** (delivery p50 by 35 ms, p99 by 348 ms, REST ACK p99 by 236 ms, connect+attach p99 by 82 ms, reconnect+attach p99 by 508 ms, memory by 2.5 points) |

The same configurations on the previous code, `7a1899d` (fleet D, the
same tightened gates):

| Run | Load | Result |
|---|---|---|
| `run-20261002T015418Z-shape-m` | shape M 1x | PASS every gate: delivery p50 31.0 ms, p99 163.8 ms; REST ACK p99 40.4 ms; memory +5.2%; 0 violations of 120,011,364 |
| `run-20261002T012048Z-presence-m` | presence 1x, 6 generators | PASS every gate: connect+attach p99 90.1 ms; 0 member-set mismatches over 48,000 members |
| `run-20261002T023502Z-shape-m` | shape M 5x | Carried, 0 violations of 600,054,527; FAIL on delivery p50 60.9 ms, p99 315.4 ms and REST ACK p99 170.0 ms; memory +5.9% |

### Where the delivery time goes

Stage histograms summed over all nodes (the fan-out stage is one
append to every local attachment's frame queued; the write wait is
sampled on one connection in eight):

| Run | Fan-out p99 | Socket write wait p99 | Bus append p99 | Delivery p99 |
|---|---|---|---|---|
| 1x, `83721b1` | 48.2 ms | 48.9 ms | 0.099 ms | 153.6 ms |
| 5x, `7a1899d` | 97.5 ms | 99.2 ms | 0.099 ms | 315.4 ms |
| 5x, `83721b1` | 97.8 ms | 100.0 ms | 0.099 ms | 389.1 ms |
| 10x, `83721b1` | 99.3 ms | 101.6 ms | 0.099 ms | 598.0 ms |

The bounded fan-out pool cut the 5x fan-out p99 from 486.8 ms on
`272012c` (`run-20261001T141856Z-shape-m`) to about 98 ms. The stages
count one sample per event and the delivery histogram one per message,
so they cannot be summed into the delivery p99.

## The order of magnitude: where it stopped, and why

**10x carried** (`run-20261002T091418Z-shape-m`, `83721b1`). 1.2
million deliveries a second is about 3.8 times the peak-hour
deliveries summed across every Ably account in August 2026 (312,000),
and the total of about 1.27 million messages a second is about 2.6
times the platform's peak-hour total (496,000); in the same run the
delivery, REST ACK, connect, reconnect and memory gates above were
missed. Where each limit sat:

| Limit | At 10x |
|---|---|
| Postgres shard CPU | 68% to 77% mean per shard on 8 shards (79% one-minute max): **the nearest limit** |
| NATS | 9.0% to 9.8% CPU of 32 vCPU per server; 9.3 to 9.4 million subscriptions in 12.6 to 12.7 GB, at least 49 GB free; 0 slow consumers: not a limit on this box size |
| Nodes, generators | 18.7% and 22% mean CPU; node memory 7.0 GB per 100k connections |
| Disk | 1.3k to 2.3k write IOPS per shard of 10k provisioned |
| Account quota | 1,184 vCPU for the fleet, of 2,000 |

**18x stopped on NATS memory** (`run-20261002T034701Z-shape-m`,
`7a1899d`; 99 nodes, 15 shards, 25 generators, 1,928 of 2,000 vCPU).
The nodes and generators held 9,899,983 of 9,900,000 connections with
0 connect failures. Each of the three NATS servers, on 16 GB
c7i.2xlarge boxes, reached about 12.7 million subscriptions at the end
of the ramp, and Linux killed `nats-server` for out of memory twice.
Delivery then broke and the checker counted the loss (147 million gaps
after the kills): **FAIL**. Every NATS server holds every subscription
(one subject per channel, for each node that holds the channel), so
NATS memory has to grow with channels times nodes. The 10x run on
64 GB boxes holds 9.4 million subscriptions in about 13 GB. 18x was not
re-run on the larger boxes, and 19x would have needed 2,056 vCPU, over
the account quota.

**Two earlier 10x attempts** on `7a1899d` (`run-20261002T060615Z-shape-m`,
`run-20261002T064526Z-shape-m`) were cut by operator error (a box
power-off timer) 3.5 and 8 minutes into the hold, and have no verdict.
On 8-vCPU NATS boxes one server reached about 96% CPU while the other
two sat at about 25%.

## Unexplained and open

- **5x was slower on the final code than on the previous code, in one
  run each.** Delivery p99 389.1 against 315.4 ms, REST ACK p99 274.4
  against 170.0 ms, memory growth +9.4% against +5.9%
  (`run-20261002T081515Z-shape-m` against `run-20261002T023502Z-shape-m`),
  while the fan-out and write-wait histograms are the same. Run-to-run
  spread at 5x was not measured. Cause not found.
- **Most of the 10x delivery tail is in no histogram.** About 100 ms of
  the 598 ms p99 is in the fan-out and socket-write stages; the rest is
  not in any stage the server measures. Cause not found. At 10x the
  server-side attach p99 was 16.0 ms while the client's connect+attach
  p99 was 581.6 ms; that gap is not explained either.
- **The REST ACK tail at scale** (274.4 ms at 5x, 335.9 ms at 10x) is
  queueing in front of the server, whose own p99 was about 70 to 75 ms.
  Which stall causes it is not found.
- **The one duplicate and one serial regression** in the degraded
  night-one NATS-kill attempt (`run-20260930T210527Z-shape-m`) were
  never reproduced (seven targeted runs); cause not found.
- **The node-kill run's failed gate is the harness.** The rig has no
  load balancer, and the REST publishers address the nodes round robin
  without dropping a dead one, so 1 publish in 10 kept going to the
  killed node for the remaining 600 s, failed fast and was retried
  (411,261 retries of 8,346,850 sends; 7,013/s x 0.1 x 600 s is about
  421,000). Nothing was rejected or lost.
- **Reconnect after a node kill takes 10.6 s at p99**, as on night-one
  code (10.5 s): all 55,000 of the killed node's clients re-dialled one
  surviving node, whose attach p99 reached 8.1 s while the others stayed
  at 3.9 ms. A load balancer would spread them; the server does not
  limit attach concurrency per node.
- **Whether the 18x gaps were signalled to clients** as a discontinuity
  (error 80016) after the NATS failure was not analysed.

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
   gates (`run-20261001T071536Z-shape-d`, `ffcb00a`). On the final code,
   10 nodes and one primary pass every gate at shape M 1x
   (`run-20261002T081123Z-shape-m`, `83721b1`).
3. **There is a measured route to more.** Channel sharding: 8 Postgres
   shards carried shape D at 2x, 103,990 of 104,000 writes a second, each
   shard 50% to 57% busy (`run-20261001T082139Z-shape-d`, `ffcb00a`); 4
   shards and 28 nodes carried shape M at 5x, 2,749,992 connections,
   35,013 publishes and 599,658 deliveries a second, 0 violations in
   598,842,283 checked (`run-20261001T141856Z-shape-m`, `272012c`). A 7x
   probe was not carried (`run-20261001T150305Z-shape-m`), so on that
   footprint the ceiling was between 5x and 7x. On the final code, 55
   nodes and 8 shards carried shape M at 10x with 0 violations in
   1,200,085,165 checked, missing six gates
   (`run-20261002T091418Z-shape-m`, `83721b1`).

On the final code (`83721b1`, "Final results" above) the largest
account passes every gate, presence included, and 5x and 10x are
carried with zero loss while missing latency gates. What still fails:
the delivery and REST ACK tails from 5x upward, connect, reconnect and
memory at 10x, REST ACK p99 for shape D on one primary (1,262 ms, on
`ffcb00a`), and the publish-retries gate in the node-kill run (a
harness artefact). Presence at 2x, shape F, Postgres failover, a NATS
kill on the fixed code and any shape D run on the final code were not
run.

## Deployment sizes: the measured envelopes

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
a DSN, the default on `83721b1`.

### Large: nodes, one Postgres and three NATS servers (`--bus=nats`)

| Run | Code | Load | Result |
|---|---|---|---|
| `run-20261002T081123Z-shape-m` | `83721b1` | shape M 1x, 10 nodes | **PASS, every gate**: 0 violations of 120,013,190; delivery p50 31.0 ms, p99 153.6 ms; REST ACK p99 36.4 ms; memory +5.5%; Postgres 52.5% |
| `run-20261002T085134Z-presence-m` | `83721b1` | presence 1x, 10 nodes | **PASS, every gate**: connect+attach p99 98.3 ms; 0 member-set mismatches over 48,000 members |
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
| `run-20261002T091418Z-shape-m` | `83721b1` | **shape M 10x**, 55 nodes, 8 shards | Carried: 5,499,528 connections, 70,013 publishes/s, 1,199,387 deliveries/s, 0 violations of 1,200,085,165; FAIL on delivery p50 85.0 ms and p99 598.0 ms, REST ACK p99 335.9 ms, connect+attach p99 581.6 ms, reconnect+attach p99 1,007.6 ms, memory +12.5%; shards 68% to 77% |
| `run-20261002T081515Z-shape-m` | `83721b1` | **shape M 5x**, 28 nodes, 4 shards | Carried: 2,749,932 connections, 35,012 publishes/s, 599,743 deliveries/s, 0 violations of 600,058,302; FAIL on delivery p50 61.4 ms and p99 389.1 ms, REST ACK p99 274.4 ms; memory +9.4%; shards 63% to 67% |
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
stalled). `7a1899d` added timings for each attach stage and one fix (a shared
member-set read is no longer cancelled by the attach that started it).
With six generators the tail was gone on both later images:

| Run | Code | Result |
|---|---|---|
| `run-20261002T012048Z-presence-m` | `7a1899d` | PASS every gate, 6 generators at 52% mean CPU: connect+attach p99 90.1 ms; presence ACK p99 335.9 ms; 0 member-set mismatches over 48,000 members |
| `run-20261002T085134Z-presence-m` | `83721b1` | PASS every gate, 6 generators at 53% mean CPU: connect+attach p99 98.3 ms; presence ACK p99 294.9 ms; 0 member-set mismatches over 48,000 members (144 of 144 channels) |

Server-side, an attach and its SYNC completed in 63.1 ms at p99 on both
(SYNC queue p99 3.6 ms and write p99 3.8 ms on `83721b1`), so the
multi-second tail on `272012c` was the three saturated generators. The
code also changed between those runs, and no three-generator run was
made on a later image, so build and harness are not separated by an
A/B. The `272012c` and earlier presence runs checked no messages and no
member set (the correctness row reads "0 of 0 checked"); the two
six-generator runs compare every sampled room's member set over REST at
the end of the hold.

## Failure injection (run 5)

Shape M 1x, 10 nodes, NATS, `6cb2713`, fault five minutes into the hold.

| Run | Fault | Result |
|---|---|---|
| `run-20260930T203111Z-shape-m` | one node's server killed | 0 violations of 120.0M (no gap, duplicate, reorder, resume gap or tail loss); 54,970 connections dropped and all reconnected; reconnect+attach p99 10.5 s |
| `run-20260930T213902Z-shape-m` | one of three NATS servers killed | 0 violations of 120.1M; nodes reconnected to NATS in 3 s and the delivery rate was back within about 10 s; 0 unplanned connection drops |
| `run-20260930T210527Z-shape-m` | one NATS server killed (an earlier attempt) | Not a clean run: the ramp degraded before the fault on a database that was not fresh (379,700 of 550,000 connections at hold start). One duplicate and one serial regression on one channel, cause not found. Not used as the verdict |

Both clean runs failed only the latency gates shape M 1x was already
failing without a fault on that code.

On the final code, `run-20261002T092020Z-shape-m` (`83721b1`, shape M
1x, 10 nodes) killed one node's server five minutes into the hold: 0
violations of 120,000,022 (no gap, duplicate, reorder, resume gap, tail
loss or attach gap); all 54,984 dropped connections re-established; 0
rejected or unresolved publishes; delivery p99 174.1 ms. It fails one
gate, publish retries (4.93% against 1%), because the rig has no load
balancer and the publishers kept addressing the dead node ("Unexplained
and open" above). Reconnect+attach p99 was 10,616.8 ms. No NATS server
was killed on any code with the day-two fixes, and no presence run had
a fault.

## Footprint

From the conductor's footprint table. "Provisioned" is the vCPU of the
node instances; "used" is the cores the nodes used on average over the
hold. Memory is node RSS at the end of the hold.

| Run | Code | Provisioned vCPU / used cores per 100k connections | Used cores per 100k deliveries/s | Used cores per 10k writes/s | RSS per 100k connections |
|---|---|---|---|---|---|
| `run-20261002T081123Z-shape-m` (M 1x) | `83721b1` | 14.55 / 1.24 | 5.70 | 9.75 | 6.98 GB |
| `run-20261002T081515Z-shape-m` (M 5x) | `83721b1` | 8.15 / 1.28 | 5.87 | 10.05 | 6.96 GB |
| `run-20261002T091418Z-shape-m` (M 10x) | `83721b1` | 8.00 / 1.30 | 5.96 | 10.21 | 7.04 GB |
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
  NATS 2.11 servers on c7i.2xlarge up to fleet D (r7i.2xlarge for the
  two cut 10x attempts) and on c7i.8xlarge (32 vCPU, 64 GB) for every
  `83721b1` run. Load generators c7i.8xlarge (3 or 6; 6 for every
  1x run on `7a1899d` and `83721b1`; 14 at 5x, 7x and 10x; 25 at 18x),
  REST publishers c7i.4xlarge (2 to 4). The `83721b1` 10x run used 8
  shards at 10,000 IOPS each. One
  Availability Zone, one VPC.
- **Bus.** Every run passed `--bus` explicitly (`nats` unless the table
  says `postgres`), except run 1a, whose `main` image has only
  `pg_notify`.
- **Lanes.** Every image after `main` up to `7a1899d` had a lane default
  of 4; `83721b1` defaults to 2. The night-one runs
  (`6cb2713`, `1e8ebff`) used that default. Every quoted day-two write
  and shape M result (`ffcb00a`, `272012c`) ran with
  `--publish-lanes=2`; so did every `7a1899d` and `83721b1` run. The lane rows above say
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

- The inferred `--bus` default (every fleet run passed `--bus`).
- Any shape D (write) run on code after `ffcb00a`; 2x on the final code.
- A NATS server kill on any code with the day-two fixes.
- 18x with larger NATS servers.
- Repeatability on `7a1899d` and `83721b1`: each configuration ran once.
- Shape F. Presence at 2x, presence with a fault, and presence member-set
  correctness at scale.
- The small size (`--bus=postgres`) on any code with the day-two fixes.
- A bus outage longer than the retention window at scale; a presence lease lapse at
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
hold. Several runs on older code would not have been judged the same way: the
presence runs before `7a1899d` checked nothing, and the `272012c`
presence runs had generators at 82% to 83% CPU. The `7a1899d` and
`83721b1` runs were judged by the tightened gates, with no override.
Each was a single run.

The whole proof, over three days, used about USD 1,000 of on-demand
cloud capacity.
