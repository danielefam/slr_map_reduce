#!/usr/bin/env bash
# Runs all built-in MapReduce jobs against Common Crawl and records per-job timings.
#
# Usage:
#   ./run_all_commoncrawl_jobs.sh
#   ./run_all_commoncrawl_jobs.sh -crawl CC-MAIN-2026-21 -files-limit 4 -n 8
#
# Output layout:
#   run/commoncrawl_jobs/
#     wordcount_result.txt
#     langdetect_result.txt
#     domainpop_result.txt
#     docdensity_result.txt
#     logs/<timestamp>/<job>.log
#     timings_<timestamp>.csv

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CRAWL="CC-MAIN-2026-21"
FILES_LIMIT=1
CHUNKS_LIMIT=1
N=4
PORT=9090
OUT_DIR="$ROOT/run/commoncrawl_jobs"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -crawl)        CRAWL="$2";        shift 2 ;;
    -files-limit)  FILES_LIMIT="$2";  shift 2 ;;
    -chunks-limit) CHUNKS_LIMIT="$2"; shift 2 ;;
    -n)            N="$2";            shift 2 ;;
    -port)         PORT="$2";         shift 2 ;;
    -out-dir)      OUT_DIR="$2";      shift 2 ;;
    -h|--help)
      cat <<'EOF'
Usage: ./run_all_commoncrawl_jobs.sh [flags]

Flags:
  -crawl <id>         Common Crawl ID (default: CC-MAIN-2026-21)
  -files-limit <n>    Max number of WET files to resolve (default: 1)
  -chunks-limit <n>   Secondary URL cap for compatibility (default: 1)
  -n <workers>        Number of workers/hosts (default: 4)
  -port <port>        Worker HTTP port (default: 9090)
  -out-dir <path>     Directory for result files and logs
  -h, --help          Show this help
EOF
      exit 0
      ;;
    *)
      echo "Unknown flag: $1" >&2
      exit 1
      ;;
  esac
done

mkdir -p "$OUT_DIR"

TS="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="$OUT_DIR/logs/$TS"
CSV="$OUT_DIR/timings_$TS.csv"
mkdir -p "$LOG_DIR"

echo "job,status,nodes,map_seconds,reduce_seconds,collect_seconds,compute_seconds,output_file,log_file" > "$CSV"

run_job() {
  local job_name="$1"
  local job_path="$2"
  local output_file="$OUT_DIR/${job_name}_result.txt"
  local log_file="$LOG_DIR/${job_name}.log"

  echo "=== Running ${job_name} ==="

  if "$ROOT/mapreduce.sh" \
      -job "$job_path" \
      -commoncrawl \
      -crawl "$CRAWL" \
      -files-limit "$FILES_LIMIT" \
      -chunks-limit "$CHUNKS_LIMIT" \
      -n "$N" \
      -port "$PORT" \
      -output "$output_file" > "$log_file" 2>&1; then
    local timing
    timing="$(grep 'TIMING nodes=' "$log_file" | tail -n 1 || true)"
    if [[ -n "$timing" ]]; then
      local nodes map_s reduce_s collect_s compute_s
      nodes="$(echo "$timing" | sed -n 's/.*nodes=\([0-9][0-9]*\).*/\1/p')"
      map_s="$(echo "$timing" | sed -n 's/.*map_seconds=\([0-9.][0-9.]*\).*/\1/p')"
      reduce_s="$(echo "$timing" | sed -n 's/.*reduce_seconds=\([0-9.][0-9.]*\).*/\1/p')"
      collect_s="$(echo "$timing" | sed -n 's/.*collect_seconds=\([0-9.][0-9.]*\).*/\1/p')"
      compute_s="$(echo "$timing" | sed -n 's/.*compute_seconds=\([0-9.][0-9.]*\).*/\1/p')"
      echo "${job_name},ok,${nodes},${map_s},${reduce_s},${collect_s},${compute_s},${output_file},${log_file}" >> "$CSV"
      echo "${job_name}: OK (compute_seconds=${compute_s})"
    else
      echo "${job_name},ok_no_timing,,,,,,${output_file},${log_file}" >> "$CSV"
      echo "${job_name}: OK (timing line not found)"
    fi
  else
    echo "${job_name},failed,,,,,,${output_file},${log_file}" >> "$CSV"
    echo "${job_name}: FAILED (see $log_file)"
    return 1
  fi
}

FAIL=0
run_job "wordcount"  "$ROOT/scripts/jobs/wordcount"  || FAIL=1
run_job "langdetect" "$ROOT/scripts/jobs/langdetect" || FAIL=1
run_job "domainpop"  "$ROOT/scripts/jobs/domainpop"  || FAIL=1
run_job "docdensity" "$ROOT/scripts/jobs/docdensity" || FAIL=1

echo ""
echo "Timing summary: $CSV"
cat "$CSV"

if [[ "$FAIL" -ne 0 ]]; then
  exit 1
fi
