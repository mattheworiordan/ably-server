#!/usr/bin/env python3
"""Summarise one bench/diagnose.sh capture directory.

    diag-summary.py <capture-dir>

Prints: the bench result lines, notification-queue fill and connection counts
from pg-poll.csv, container CPU from dockerstats.csv, per-node publish rate
and mean publish-to-ACK latency from the /metrics scrapes, the top of the
CPU profile (if `go` is available) and the pg_stat_statements top list.
"""
import csv
import glob
import os
import re
import statistics
import subprocess
import sys

d = sys.argv[1]
here = os.path.dirname(os.path.abspath(__file__))

print("== bench")
for line in open(os.path.join(d, "run.log"), errors="replace"):
    if re.search(r"achieved|latency|correct|offered|#", line):
        print("  " + line.rstrip())

print("== pg-poll.csv (0.5 s samples during the run)")
rows = list(csv.DictReader(open(os.path.join(d, "pg-poll.csv"))))
def col(name):
    out = []
    for r in rows:
        try:
            out.append(float(r[name]))
        except (ValueError, KeyError, TypeError):
            pass
    return out
q = col("notify_queue_usage")
if q:
    print(f"  notification queue usage: max {max(q):.6f}  mean {statistics.mean(q):.6f}  ({len(q)} samples; 1.0 = full)")
for name in ("pg_conns", "pg_active", "pg_idle_in_tx"):
    v = col(name)
    if v:
        print(f"  {name}: mean {statistics.mean(v):.1f} max {max(v):.0f}")

print("== docker stats (CPU% of one core, whole run)")
out = subprocess.run([sys.executable, os.path.join(here, "stats-summary.py"), os.path.join(d, "dockerstats.csv"), "--per-node"],
                     capture_output=True, text=True).stdout
print("\n".join("  " + l for l in out.splitlines()))

print("== node /metrics deltas (before -> after)")
def read(path):
    vals = {}
    for line in open(path, errors="replace"):
        m = re.match(r"^([a-z_]+)(\{[^}]*\})? ([0-9.eE+-]+)$", line.strip())
        if m and not m.group(2):
            vals[m.group(1)] = float(m.group(3))
    return vals
tot_pub = 0
for before in sorted(glob.glob(os.path.join(d, "metrics-*-before.txt"))):
    after = before.replace("-before.txt", "-after.txt")
    if not os.path.exists(after):
        continue
    b, a = read(before), read(after)
    dp = a.get("ably_messages_published_total", 0) - b.get("ably_messages_published_total", 0)
    dd = a.get("ably_messages_delivered_total", 0) - b.get("ably_messages_delivered_total", 0)
    ds = a.get("ably_publish_latency_seconds_sum", 0) - b.get("ably_publish_latency_seconds_sum", 0)
    dc = a.get("ably_publish_latency_seconds_count", 0) - b.get("ably_publish_latency_seconds_count", 0)
    dcpu = a.get("process_cpu_seconds_total", 0) - b.get("process_cpu_seconds_total", 0)
    tot_pub += dp
    name = os.path.basename(before).replace("metrics-", "").replace("-before.txt", "")
    mean_ms = (ds / dc * 1000) if dc else float("nan")
    print(f"  {name}: published {dp:.0f}  delivered {dd:.0f}  mean publish-to-ACK {mean_ms:.2f} ms  node cpu-seconds {dcpu:.1f}  goroutines(after) {a.get('go_goroutines', 0):.0f}")
print(f"  total published across nodes: {tot_pub:.0f}")

print("== CPU profile of the first node (top by flat)")
for prof in sorted(glob.glob(os.path.join(d, "cpu-*.pprof"))):
    r = subprocess.run(["go", "tool", "pprof", "-top", "-nodecount=22", prof], capture_output=True, text=True)
    print("\n".join("  " + l for l in (r.stdout or r.stderr).splitlines()[:32]))

p = os.path.join(d, "pg_stat_statements.txt")
if os.path.exists(p):
    print("== pg_stat_statements / pg_stat_database")
    print("".join("  " + l for l in open(p, errors="replace")))
