#!/usr/bin/env bash
# Runs the full MapReduce pipeline:
#   1. Fetch available hosts
#   2. Run the mapreduce client (builds worker, deploys, map→shuffle→reduce→collect)
#   3. Cleanup is handled by the mapreduce client itself
#
# Usage:
#   ./mapreduce.sh -job scripts/jobs/wordcount -input /path/to/data.txt -output result.txt [-n 10] [-port 9090]
#   ./mapreduce.sh -job scripts/jobs/wordcount -commoncrawl [-crawl CC-MAIN-2026-05] -output result.txt [-n 10] [-port 9090]

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")/scripts" && pwd)"
HOSTS="$ROOT/hosts.txt"

# Default values (can be overridden by passing the same flags)
INPUT=""
JOB=""
USE_COMMONCRAWL=0
CRAWL=""
FILES_LIMIT=0
CHUNKS_LIMIT=0
OUTPUT="result.txt"
N=10
PORT=9090

# Parse flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    -job)          JOB="$2";            shift 2 ;;
    -input)        INPUT="$2";          shift 2 ;;
    -commoncrawl)  USE_COMMONCRAWL=1;   shift   ;;
    -crawl)        CRAWL="$2";          shift 2 ;;
    -files-limit)  FILES_LIMIT="$2";    shift 2 ;;
    -chunks-limit) CHUNKS_LIMIT="$2";   shift 2 ;;
    -output)       OUTPUT="$2";         shift 2 ;;
    -n)            N="$2";              shift 2 ;;
    -port)         PORT="$2";           shift 2 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$JOB" ]]; then
  echo "Usage: missing required -job <path> flag" >&2
  exit 1
fi

if [[ ! -d "$JOB" ]]; then
  echo "Job path must be an existing directory: $JOB" >&2
  exit 1
fi

JOB="$(cd "$JOB" && pwd)"

if [[ -n "$INPUT" ]]; then
  if [[ ! -f "$INPUT" ]]; then
    echo "Input file does not exist: $INPUT" >&2
    exit 1
  fi
  INPUT="$(cd "$(dirname "$INPUT")" && pwd)/$(basename "$INPUT")"
fi

if [[ "$OUTPUT" != /* ]]; then
  OUTPUT="$ROOT/$OUTPUT"
fi

if [[ -n "$INPUT" && "$USE_COMMONCRAWL" -eq 1 ]]; then
  echo "Usage: choose exactly one input mode: -input <file> or -commoncrawl [-crawl <id>]" >&2
  exit 1
fi

if [[ -z "$INPUT" && "$USE_COMMONCRAWL" -eq 0 ]]; then
  echo "Usage: $0 -job <path> -input <file>|-commoncrawl [-crawl <id>] [-files-limit <n>] [-chunks-limit <n>] [-output <file>] [-n <workers>] [-port <port>]" >&2
  exit 1
fi

echo "=== Step 1: Fetching hosts ==="
(cd "$SCRIPTS" && go run ./make_hosts -n "$N" -f "$HOSTS")

echo ""
echo "=== Step 2: Running MapReduce ==="
EXTRA_FLAGS=()
[[ -n "$INPUT" ]] && EXTRA_FLAGS+=(-input "$INPUT")
if [[ "$USE_COMMONCRAWL" -eq 1 ]]; then
  EXTRA_FLAGS+=(-commoncrawl)
  [[ -n "$CRAWL" ]] && EXTRA_FLAGS+=(-crawl "$CRAWL")
  (( FILES_LIMIT > 0 )) && EXTRA_FLAGS+=(-files-limit "$FILES_LIMIT")
  (( CHUNKS_LIMIT > 0 )) && EXTRA_FLAGS+=(-chunks-limit "$CHUNKS_LIMIT")
fi

(cd "$SCRIPTS" && go run ./mapreduce \
  -hosts "$HOSTS" \
  -job "$JOB" \
  -output "$OUTPUT" \
  -n "$N" \
  -port "$PORT" \
  "${EXTRA_FLAGS[@]}")

echo ""
echo "All done. Results are in $OUTPUT"
