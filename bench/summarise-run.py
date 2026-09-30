#!/usr/bin/env python3
"""Summarise one ably-bench --search run (written by run-search.sh).

    summarise-run.py <log-dir> <label> [--duration 15]

Reads <label>.log (timestamped bench output), <label>.dockerstats.csv and
<label>.top.log. Prints the trial table, the reported maximum, and the
resource use during the trial that produced it: Postgres and node CPU from
docker stats, plus the load generator and Docker VM CPU from `top`.

The bench prints a trial line when the trial ends. The measurement window
is taken as the `duration` seconds ending ~1.5 s before that line (publishers
stop at the window end, then the tool drains acks and deliveries).
"""
import argparse
import csv
import datetime as dt
import os
import re
import statistics
import subprocess
import sys
from collections import defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("logdir")
ap.add_argument("label")
ap.add_argument("--duration", type=float, default=15.0)
ap.add_argument("--drain", type=float, default=1.5)
ap.add_argument("--quiet", action="store_true")
ap.add_argument("--procs", nargs="*", default=["ably-bench", "ably-server-h", "com.apple.Virtua"],
                help="top command-name prefixes to report (host processes)")
a = ap.parse_args()

base = os.path.join(a.logdir, a.label)
here = os.path.dirname(os.path.abspath(__file__))


def hms_to_s(hms):
    h, m, s = hms.split(":")
    return int(h) * 3600 + int(m) * 60 + float(s)


trial_re = re.compile(r"^(\d\d:\d\d:\d\d\.\d+) (\d+)\s+(\d+)\s+([\d.]+ms)\s+([\d.]+ms)\s+([\d.]+ms)\s+(ok|FAIL) (✓|✗)")
trials = []
maxline = load = None
wall = None
with open(base + ".log", errors="replace") as f:
    for line in f:
        m = trial_re.match(line)
        if m:
            trials.append(dict(t=m.group(1), offered=int(m.group(2)), achieved=int(m.group(3)),
                               p50=m.group(4), p99=m.group(5), mx=m.group(6),
                               ok=m.group(7), passed=(m.group(8) == "✓")))
        elif "MAX SUSTAINED THROUGHPUT" in line:
            maxline = line.strip()
        elif "at that load" in line:
            load = line.strip()
        elif line.startswith("# wall clock"):
            wall = line.strip()

if not a.quiet:
    print(f"== {a.label}")
    print(f"{'ended':13s} {'offered':>8s} {'achieved':>9s} {'p50':>8s} {'p99':>9s} {'max':>9s}  verdict")
    for t in trials:
        print(f"{t['t']:13s} {t['offered']:8d} {t['achieved']:9d} {t['p50']:>8s} {t['p99']:>9s} {t['mx']:>9s}  {'PASS' if t['passed'] else 'fail'}")
    print(maxline or "(no MAX line)")
    print(load or "")
    print(wall or "")

m = re.search(r"THROUGHPUT: (\d+) msg/s", maxline or "")
if not m:
    sys.exit(0)
best_rate = int(m.group(1))
cands = [t for t in trials if t["passed"] and t["achieved"] == best_rate]
if not cands:
    sys.exit(0)
best = cands[-1]
end = hms_to_s(best["t"]) - a.drain
start = end - a.duration
w0 = (dt.datetime.min + dt.timedelta(seconds=start)).strftime("%H:%M:%S")
w1 = (dt.datetime.min + dt.timedelta(seconds=end)).strftime("%H:%M:%S")
print(f"best trial: offered {best['offered']} achieved {best['achieved']} p50 {best['p50']} p99 {best['p99']}  window {w0}-{w1}")

if os.path.exists(base + ".dockerstats.csv"):
    out = subprocess.run([sys.executable, os.path.join(here, "stats-summary.py"), base + ".dockerstats.csv",
                          "--window", w0, w1], capture_output=True, text=True).stdout
    print(out.rstrip())
if os.path.exists(base + ".top.log"):
    out = subprocess.run([sys.executable, os.path.join(here, "top-summary.py"), base + ".top.log",
                          *a.procs, "--window", w0, w1, "--stats", "--quiet"],
                         capture_output=True, text=True).stdout
    print(out.rstrip())
