# Results

Anonymised results of the scale proof. Nothing here names a customer or an AWS
account: workloads are called shape F, shape M and shape D, and accounts are
described by size ("the largest account"). This file is written after the
runs, from numbers that have passed an independent review. Until then every
section below is a heading only. The reproduction steps are in
[RUNBOOK.md](RUNBOOK.md).

## Summary

Not written yet. It carries three messages: the objection was argued at a scale
that does not exist; the scale that is needed is reachable with a bounded
amount of work on a small footprint; and there is a measured route to more.

## Deployment sizes: the measured envelope at 1x and 2x

Not written yet. One table per size (small: nodes and one Postgres; large:
nodes, one Postgres and a NATS cluster; very large: sharded Postgres), each
with the pass result at 1x and 2x, the ceiling reached, and the run-to-run
spread.

## The two scaling curves

Not written yet. Node curve (5, 10, 20 nodes at fixed write load) and shard
curve (1, 2, 4 Postgres primaries at 10 nodes).

## Postgres alone (run 0a)

Not written yet. The ACK latency floor and the single-primary write ceiling on
io2 and gp3, from `pgbench/`, for the trivial commit, the shipped publish
transaction, the pipelined publish, and batches of 10, 30 and 100.

## Presence (run 6)

Not written yet. Pass or fail, and exactly what happened.

## Failure injection (run 5)

Not written yet. Node kill and NATS server kill: recovery time and whether
any acknowledged message was lost.

## Footprint

Not written yet. vCPU and memory per 100k connections, per 100k deliveries a
second and per 10k writes a second.

## Configuration

Not written yet. Instance types, versions, flags and Postgres parameters of
every run, recorded in the run's STATE snapshot.

## What was not tested

Not written yet. Starts from the plan's "harden later" list: presence beyond
the short run, JetStream, rate limits and quotas, token revocation, TLS in the
binary, auth beyond HS256, Helm and Kubernetes artefacts, mixed-version
upgrades, SDK CI matrix, delta, filtering, push and LiveObjects parity,
security review, resharding.
