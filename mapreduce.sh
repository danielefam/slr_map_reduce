#!/usr/bin/env bash
# Runs the full MapReduce pipeline:
#   1. Fetch available hosts
#   2. Run the C++ mapreduce client (deploys workers, map→shuffle→reduce→collect)
#   3. Cleanup is handled by the mapreduce client itself
#
# Usage:
#   ./mapreduce.sh -job wordcount -input /path/to/data.txt -output result.txt [-n 10] [-port 9090]
#   ./mapreduce.sh -job wordcount -commoncrawl [-crawl CC-MAIN-2026-05] -output result.txt [-n 10] [-port 9090]

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS="$ROOT/scripts"
BIN_DIR="$ROOT/bin"
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
  echo "Usage: missing required -job <name> flag" >&2
  exit 1
fi

if [[ "$JOB" == */* ]]; then
  if [[ ! -d "$JOB" ]]; then
    echo "Job path must be an existing directory: $JOB" >&2
    exit 1
  fi
  JOB="$(basename "$JOB")"
fi

case "$JOB" in
  wordcount|langdetect|domainpop|docdensity) ;;
  *)
    echo "Unsupported job '$JOB'. Supported jobs: wordcount, langdetect, domainpop, docdensity" >&2
    exit 1
    ;;
esac

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
  echo "Usage: $0 -job <name> -input <file>|-commoncrawl [-crawl <id>] [-files-limit <n>] [-chunks-limit <n>] [-output <file>] [-n <workers>] [-port <port>]" >&2
  exit 1
fi

if [[ ! -x "$BIN_DIR/slr_make_hosts" || ! -x "$BIN_DIR/slr_mapreduce" || ! -x "$BIN_DIR/slr_worker" ]]; then
  "$SCRIPTS/build_cpp.sh"
fi

echo "=== Step 1: Fetching hosts ==="
"$BIN_DIR/slr_make_hosts" -n "$N" -f "$HOSTS"

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

"$BIN_DIR/slr_mapreduce" \
  -hosts "$HOSTS" \
  -job "$JOB" \
  -output "$OUTPUT" \
  -n "$N" \
  -port "$PORT" \
  "${EXTRA_FLAGS[@]}"

echo ""
echo "All done. Results are in $OUTPUT"
