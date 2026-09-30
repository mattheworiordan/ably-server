#!/usr/bin/env python3
"""Summarise a `top -l 0 -s 2 -o cpu -stats pid,command,cpu,mem` log.

Prints, per sample, the host CPU line and the CPU% of selected processes
(by command-name prefix). With --window START END (HH:MM:SS, local time) only
samples in that window are shown; --stats prints mean/max per process over
the selected samples.

    top-summary.py run.top.log ably-bench ably-server-h com.apple.Virtua --window 01:40:46 01:41:05 --stats
"""
import re
import sys
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("log")
ap.add_argument("procs", nargs="*")
ap.add_argument("--window", nargs=2, metavar=("START", "END"))
ap.add_argument("--stats", action="store_true")
ap.add_argument("--quiet", action="store_true", help="do not print each sample")
a = ap.parse_args()

def hms(s):
    h, m, sec = s.split(":")
    return int(h) * 3600 + int(m) * 60 + int(sec)

samples = []  # (time, idle, {proc: cpu})
cur = None
with open(a.log, errors="replace") as f:
    for line in f:
        line = line.rstrip("\n")
        if line.startswith("Processes:"):
            cur = {"t": None, "idle": None, "load": None, "p": {}}
            samples.append(cur)
        elif cur is None:
            continue
        elif re.match(r"\d{4}/\d\d/\d\d \d\d:\d\d:\d\d", line):
            cur["t"] = line.split()[1]
        elif line.startswith("CPU usage:"):
            m = re.search(r"([\d.]+)% idle", line)
            cur["idle"] = float(m.group(1)) if m else None
        elif line.startswith("Load Avg:"):
            cur["load"] = line.split(":")[1].strip().split(",")[0]
        else:
            m = re.match(r"\s*(\d+)\s+(.{1,16}?)\s+([\d.]+)\s+", line)
            if m:
                name = m.group(2).strip()
                cpu = float(m.group(3))
                for p in a.procs:
                    if name.startswith(p):
                        cur["p"][p] = cpu

sel = []
for s in samples[1:]:  # first sample is since-boot average; skip
    if s["t"] is None:
        continue
    if a.window and not (hms(a.window[0]) <= hms(s["t"]) <= hms(a.window[1])):
        continue
    sel.append(s)

if not a.quiet:
    print("time      hostIdle%  " + "  ".join(f"{p[:14]:>14}" for p in a.procs))
    for s in sel:
        print(f"{s['t']}  {s['idle'] if s['idle'] is not None else -1:8.1f}  " +
              "  ".join(f"{s['p'].get(p, 0.0):14.1f}" for p in a.procs))
if a.stats and sel:
    print("--- mean / max over %d samples (%%CPU of one core; processes not in top-8 count as 0) ---" % len(sel))
    idle = [s["idle"] for s in sel if s["idle"] is not None]
    print(f"host idle: mean {sum(idle)/len(idle):.1f}%  min {min(idle):.1f}%")
    for p in a.procs:
        vals = [s["p"].get(p, 0.0) for s in sel]
        print(f"{p:16s} mean {sum(vals)/len(vals):8.1f}  max {max(vals):8.1f}")
