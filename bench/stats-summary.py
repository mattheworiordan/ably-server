#!/usr/bin/env python3
"""Summarise a docker-stats CSV written by sample-docker-stats.sh.

Per sample instant (the sampler prints one batch of containers per
`docker stats --no-stream` call) it computes the Postgres CPU% and the
summed and per-node CPU% of the node containers, then reports mean / p90 /
max over the selected window. CPU% is per core: 100 = one full core.

    stats-summary.py run.dockerstats.csv [--window HH:MM:SS HH:MM:SS] [--per-node]

Times are local (the same clock as the timestamps in the bench logs).
"""
import argparse
import csv
import datetime as dt
import statistics
from collections import defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("csv")
ap.add_argument("--window", nargs=2, metavar=("START", "END"))
ap.add_argument("--per-node", action="store_true")
ap.add_argument("--json", action="store_true", help="emit one compact key=value line")
a = ap.parse_args()


def local_hms(epoch):
    return dt.datetime.fromtimestamp(float(epoch)).strftime("%H:%M:%S")


def secs(hms):
    h, m, s = hms.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


batches = defaultdict(dict)  # epoch -> {name: cpu}
mem = defaultdict(list)
with open(a.csv) as f:
    for row in csv.DictReader(f):
        try:
            t = row["epoch"]
            cpu = float(row["cpu_pct"].rstrip("%"))
        except (ValueError, KeyError, AttributeError):
            continue
        if a.window:
            s = secs(local_hms(t))
            if not (secs(a.window[0]) <= s <= secs(a.window[1])):
                continue
        batches[t][row["name"]] = cpu
        # MemUsage like "123.4MiB / 7.75GiB"
        try:
            used = row["mem_usage"].split("/")[0].strip()
            unit = used.rstrip("0123456789.")
            val = float(used[: len(used) - len(unit)])
            mult = {"B": 1e-6, "KiB": 1 / 1024, "MiB": 1.0, "GiB": 1024.0, "kB": 1e-3, "MB": 1.0, "GB": 1000.0}[unit]
            mem[row["name"]].append(val * mult)
        except Exception:
            pass

if not batches:
    print("no samples in window")
    raise SystemExit(0)

pg, nodes_sum, bench_cpu = [], [], []
per_node = defaultdict(list)
for t, d in sorted(batches.items()):
    pg_v = [v for n, v in d.items() if "postgres" in n]
    bench_v = [v for n, v in d.items() if "bench" in n]
    node_v = {n: v for n, v in d.items() if "postgres" not in n and "bench" not in n}
    if pg_v:
        pg.append(sum(pg_v))
    if bench_v:
        bench_cpu.append(sum(bench_v))
    if node_v:
        nodes_sum.append(sum(node_v.values()))
        for n, v in node_v.items():
            per_node[n].append(v)


def q(vals, p):
    vals = sorted(vals)
    return vals[min(len(vals) - 1, int(round(p * (len(vals) - 1))))]


def line(label, vals):
    return f"{label:28s} n={len(vals):3d} mean={statistics.mean(vals):8.1f} p90={q(vals, .9):8.1f} max={max(vals):8.1f}"


if a.json:
    print(f"pg_cpu_mean={statistics.mean(pg):.0f} pg_cpu_max={max(pg):.0f} "
          f"nodes_cpu_sum_mean={statistics.mean(nodes_sum):.0f} nodes_cpu_sum_max={max(nodes_sum):.0f} samples={len(pg)}")
    raise SystemExit(0)

print("(CPU% of one core; Docker VM has 18 cores = 1800%)")
if pg:
    print(line("postgres", pg))
if nodes_sum:
    print(line("all nodes (sum)", nodes_sum))
if bench_cpu:
    print(line("bench container", bench_cpu))
if a.per_node:
    for n in sorted(per_node):
        print(line(n, per_node[n]))
for n in sorted(mem):
    if "postgres" in n or a.per_node:
        print(f"{n:28s} mem MiB mean={statistics.mean(mem[n]):7.0f} max={max(mem[n]):7.0f}")
