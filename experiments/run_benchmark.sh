#!/usr/bin/env bash
# run_benchmark.sh — strong-scaling / Amdahl's-law benchmark for the distributed
# MapReduce system.
#
# Runs the SAME fixed workload while increasing the number of worker nodes
# (1, 2, 4, …, 128), times only the compute phases (map → reduce → collect),
# computes speedup S(N) = T(1)/T(N), writes a CSV, and (optionally) renders an
# Amdahl's-law plot with gnuplot.
#
# The workload is a fixed, deterministic set of Common Crawl WET URLs distributed
# round-robin across the active workers, so total work stays constant as N grows.
# Bound the workload with -files-limit and/or -chunks-limit.
#
# Usage:
#   experiments/run_benchmark.sh -job scripts/jobs/wordcount -chunks-limit 256
#   experiments/run_benchmark.sh -job scripts/jobs/wordcount -crawl CC-MAIN-2026-05 -files-limit 4 \
#       -nodes "1 2 4 8 16 32 64 128" -reps 3 -out experiments/results.csv
#
# Flags:
#   -job          PATH   Required Go job package directory
#   -crawl        ID     Override the Common Crawl ID (default: latest crawl)
#   -files-limit  N      Cap number of WET files used
#   -chunks-limit N      Second workload cap kept for compatibility
#   -nodes        LIST   Space-separated node counts   (default "1 2 4 8 16 32 64 128")
#   -reps         K      Timed runs per node count               (default 3)
#   -port         PORT   Worker HTTP port                        (default 9090)
#   -out          FILE   Output CSV path             (default experiments/results.csv)
#   -plot                Render experiments/amdahl.png with gnuplot after the run
#   -no-fetch            Reuse an existing master hosts file instead of make_hosts

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPTS="$ROOT/scripts"
EXP_DIR="$ROOT/experiments"

CRAWL=""
JOB=""
FILES_LIMIT=0
CHUNKS_LIMIT=0
NODES="1 2 4 8 16 32 64 128"
REPS=3
PORT=9090
OUT="$EXP_DIR/results.csv"
DO_PLOT=0
DO_FETCH=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    -job)          JOB="$2";          shift 2 ;;
    -input)        INPUT="$2";        shift 2 ;;
    -crawl)        CRAWL="$2";        shift 2 ;;
    -files-limit)  FILES_LIMIT="$2";  shift 2 ;;
    -chunks-limit) CHUNKS_LIMIT="$2"; shift 2 ;;
    -nodes)        NODES="$2";        shift 2 ;;
    -reps)         REPS="$2";         shift 2 ;;
    -port)         PORT="$2";         shift 2 ;;
    -out)          OUT="$2";          shift 2 ;;
    -plot)         DO_PLOT=1;         shift   ;;
    -no-fetch)     DO_FETCH=0;        shift   ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$JOB" ]]; then
  echo "Error: missing required -job <path> flag" >&2
  exit 1
fi

if [[ ! -d "$JOB" ]]; then
  echo "Error: job path must be an existing directory: $JOB" >&2
  exit 1
fi

JOB="$(cd "$JOB" && pwd)"

mkdir -p "$EXP_DIR"
WORK_DIR="$(mktemp -d)"
MASTER_HOSTS="$EXP_DIR/bench-hosts.txt"
trap 'rm -rf "$WORK_DIR"' EXIT

# Largest N requested → size of the master host pool.
MAX_N=0
for n in $NODES; do (( n > MAX_N )) && MAX_N=$n; done

# Warn if the chunk budget can't keep the largest node count busy: with fewer
# chunks than workers, high-N runs leave workers idle and speedup is meaningless.
if (( CHUNKS_LIMIT > 0 && CHUNKS_LIMIT < MAX_N )); then
  echo "WARNING: -chunks-limit=$CHUNKS_LIMIT < max N=$MAX_N — high-N runs will have idle workers." >&2
  echo "         Use at least a few chunks per worker (e.g. -chunks-limit $((MAX_N * 2)) or more)." >&2
fi

# ── Fetch a master pool of hosts once ────────────────────────────────────────
if [[ "$DO_FETCH" -eq 1 ]]; then
  FETCH_COUNT=$(( MAX_N * 2 ))
  echo "=== Fetching up to $FETCH_COUNT hosts ==="
  ( cd "$SCRIPTS" && go run ./make_hosts -n "$FETCH_COUNT" -f "$MASTER_HOSTS" )
fi
if [[ ! -s "$MASTER_HOSTS" ]]; then
  echo "Error: no master hosts file at $MASTER_HOSTS (run without -no-fetch)" >&2
  exit 1
fi
AVAIL=$(grep -cve '^[[:space:]]*$' "$MASTER_HOSTS")
echo "Master pool has $AVAIL hosts available"

# ── Run the sweep ────────────────────────────────────────────────────────────
# Collect per-N median compute seconds, then derive speedup vs the N=1 median.
declare -A MEDIAN
ORDERED_NODES=()

run_one() {
  # $1 = per-N hosts file, $2 = N → prints compute_seconds (or empty on failure).
  # Discards the run if the orchestrator reports a different active node count
  # than requested (e.g. fewer healthy workers than -n), so the CSV is never
  # mislabeled.
  local hosts_file="$1" n="$2" out_file="$WORK_DIR/result-n$2.txt" log got_nodes secs
  local extra=()
  if [[ -n "${INPUT:-}" ]]; then
    extra+=(-input "$INPUT")
  else
    extra+=(-commoncrawl)
    [[ -n "$CRAWL" ]] && extra+=(-crawl "$CRAWL")
  fi
  (( FILES_LIMIT  > 0 )) && extra+=(-files-limit  "$FILES_LIMIT")
  (( CHUNKS_LIMIT > 0 )) && extra+=(-chunks-limit "$CHUNKS_LIMIT")
  log="$( cd "$SCRIPTS" && go run ./mapreduce \
            -hosts "$hosts_file" \
            -job "$JOB" \
            -output "$out_file" \
            -n "$n" \
            -port "$PORT" \
            -max-attempts 64 \
            "${extra[@]}" 2>&1 )" || { echo "$log" >&2; return 1; }
  got_nodes="$(echo "$log" | sed -n 's/.*TIMING nodes=\([0-9][0-9]*\).*/\1/p' | tail -1)"
  secs="$(echo "$log" | sed -n 's/.*compute_seconds=\([0-9.][0-9.]*\).*/\1/p' | tail -1)"
  if [[ -z "$secs" ]]; then
    echo "$log" >&2
    return 1
  fi
  if [[ -n "$got_nodes" && "$got_nodes" != "$n" ]]; then
    echo "    discarding run: orchestrator used $got_nodes active nodes, requested $n" >&2
    return 1
  fi
  echo "$secs"
}

median() {
  # Reads numbers on stdin, prints the median (average of two middles if even).
  sort -n | awk '
    { a[NR] = $1 }
    END {
      if (NR == 0) { exit 1 }
      if (NR % 2) { printf "%.3f", a[(NR+1)/2] }
      else        { printf "%.3f", (a[NR/2] + a[NR/2+1]) / 2 }
    }'
}

for N in $NODES; do
  if (( N > AVAIL )); then
    echo ">>> Skipping N=$N (only $AVAIL hosts available)"
    continue
  fi
  head -n "$N" "$MASTER_HOSTS" > "$WORK_DIR/hosts-n$N.txt"

  echo ""
  echo "=== N=$N : $REPS run(s) ==="
  times=()
  for ((r = 1; r <= REPS; r++)); do
    echo "  run $r/$REPS …"
    if t="$(run_one "$MASTER_HOSTS" "$N")" && [[ -n "$t" ]]; then
      echo "    compute_seconds=$t"
      times+=("$t")
    else
      echo "    run failed (no timing captured)" >&2
    fi
  done

  if (( ${#times[@]} == 0 )); then
    echo "  !! all runs failed for N=$N; recording blank" >&2
    MEDIAN[$N]=""
    : > "$WORK_DIR/runs-n$N.txt"
  else
    MEDIAN[$N]="$(printf '%s\n' "${times[@]}" | median)"
    echo "  median=${MEDIAN[$N]}s"
    printf '%s\n' "${times[@]}" > "$WORK_DIR/runs-n$N.txt"
  fi
  ORDERED_NODES+=("$N")
done

# ── Write CSV ────────────────────────────────────────────────────────────────
BASE="${MEDIAN[1]:-}"
# If no N=1 baseline, use the smallest N that ran as the reference (speedup=1).
if [[ -z "$BASE" ]]; then
  for N in "${ORDERED_NODES[@]}"; do
    if [[ -n "${MEDIAN[$N]:-}" ]]; then BASE="${MEDIAN[$N]}"; break; fi
  done
fi

{
  echo "nodes,median_seconds,speedup,runs"
  for N in "${ORDERED_NODES[@]}"; do
    med="${MEDIAN[$N]:-}"
    runs="$(paste -sd';' "$WORK_DIR/runs-n$N.txt" 2>/dev/null || true)"
    if [[ -n "$med" && -n "$BASE" ]]; then
      sp="$(awk -v b="$BASE" -v m="$med" 'BEGIN { printf "%.4f", b/m }')"
    else
      sp=""
    fi
    echo "$N,$med,$sp,$runs"
  done
} > "$OUT"

echo ""
echo "=== Results written to $OUT ==="
cat "$OUT"

# ── Optional plot ────────────────────────────────────────────────────────────
if [[ "$DO_PLOT" -eq 1 ]]; then
  PNG="$EXP_DIR/amdahl.png"
  echo ""
  echo "=== Rendering $PNG ==="
  gnuplot -e "datafile='$OUT'; outfile='$PNG'" "$EXP_DIR/amdahl.gnuplot"
  echo "Plot written to $PNG"
fi
