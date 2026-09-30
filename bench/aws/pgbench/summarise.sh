#!/usr/bin/env bash
# Turn run-0a CSV files (one per storage type) into markdown tables.
#
#   summarise.sh results-io2.csv [results-gp3.csv ...] > summary.md
#
# Per storage type: publishes per second, transaction (commit) rate and p99 ACK
# latency by variant and client count. With two or more storage types: the
# publishes-per-second ratio between them.
set -euo pipefail
[ "$#" -gt 0 ] || { echo "usage: summarise.sh results-<storage>.csv..." >&2; exit 1; }

# pivot <metric column name> <title> <awk printf format> <csv...>
pivot() {
  local col=$1 title=$2 fmt=$3
  shift 3
  awk -F, -v col="$col" -v title="$title" -v fmt="$fmt" '
    FNR == 1 { for (i = 1; i <= NF; i++) idx[$i] = i; next }
    { st = $idx["storage"]; v = $idx["variant"]; c = $idx["clients"] + 0
      val[st, v, c] = $idx[col]; status[st, v, c] = $idx["status"]
      if (!(st in seenSt)) { seenSt[st] = 1; stOrder[++ns] = st }
      if (!((st, v) in seenV)) { seenV[st, v] = 1; vOrder[st, ++nv[st]] = v }
      if (!(c in seenC)) { seenC[c] = 1; cs[++nc] = c } }
    END {
      # sort client counts ascending
      for (i = 1; i <= nc; i++) for (j = i + 1; j <= nc; j++) if (cs[j] + 0 < cs[i] + 0) { t = cs[i]; cs[i] = cs[j]; cs[j] = t }
      for (s = 1; s <= ns; s++) {
        st = stOrder[s]
        printf "\n### %s: %s\n\n| variant |", title, st
        for (i = 1; i <= nc; i++) printf " %d clients |", cs[i]
        printf "\n|---|"
        for (i = 1; i <= nc; i++) printf "---:|"
        printf "\n"
        for (k = 1; k <= nv[st]; k++) {
          v = vOrder[st, k]
          printf "| %s |", v
          for (i = 1; i <= nc; i++) {
            x = val[st, v, cs[i]]
            if (x == "") printf " n/a |"
            else { printf " " fmt, x + 0; if (status[st, v, cs[i]] != "ok") printf " (FAILED)"; printf " |" }
          }
          printf "\n"
        }
      }
    }' "$@"
}

echo "# Run 0a: Postgres alone"
pivot msgs_per_s "publishes (messages) per second" "%.0f" "$@"
pivot tps "transactions (commits) per second" "%.0f" "$@"
pivot p99_ms "p99 transaction latency, ms (the ACK floor)" "%.2f" "$@"
pivot wal_bytes_per_msg "WAL bytes per message" "%.0f" "$@"

n=$(awk -F, 'FNR > 1 { s[$1] = 1 } END { print length(s) }' "$@")
if [ "$n" -ge 2 ]; then
  awk -F, '
    FNR == 1 { for (i = 1; i <= NF; i++) idx[$i] = i; next }
    { st = $idx["storage"]; v = $idx["variant"]; c = $idx["clients"] + 0
      m[st, v, c] = $idx["msgs_per_s"] + 0
      if (!(st in seenSt)) { seenSt[st] = 1; order[++ns] = st }
      if (!(v in seenV)) { seenV[v] = 1; vo[++nv] = v }
      if (!(c in seenC)) { seenC[c] = 1; cs[++nc] = c } }
    END {
      for (i = 1; i <= nc; i++) for (j = i + 1; j <= nc; j++) if (cs[j] + 0 < cs[i] + 0) { t = cs[i]; cs[i] = cs[j]; cs[j] = t }
      a = order[1]
      for (s = 2; s <= ns; s++) {
        b = order[s]
        printf "\n### publishes per second, %s / %s\n\n| variant |", b, a
        for (i = 1; i <= nc; i++) printf " %d clients |", cs[i]
        printf "\n|---|"
        for (i = 1; i <= nc; i++) printf "---:|"
        printf "\n"
        for (k = 1; k <= nv; k++) {
          v = vo[k]; printf "| %s |", v
          for (i = 1; i <= nc; i++) {
            if (m[a, v, cs[i]] > 0 && m[b, v, cs[i]] > 0) printf " %.2f |", m[b, v, cs[i]] / m[a, v, cs[i]]
            else printf " n/a |"
          }
          printf "\n"
        }
      }
    }' "$@"
fi
