#!/usr/bin/env bash
# Runs the full MapReduce pipeline:
#   1. Fetch available hosts
#   2. Run the mapreduce client (builds worker, deploys, map→shuffle→reduce→collect)
#   3. Cleanup is handled by the mapreduce client itself
#
# Usage:
#   ./mapreduce.sh -input /path/to/data.txt -output result.txt [-n 10] [-port 9090]
#   ./mapreduce.sh -input-dir /cal/commoncrawl  -output result.txt [-n 10] [-port 9090]

set -euo pipefail

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")/scripts" && pwd)"
HOSTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/hosts.txt"

# Default values (can be overridden by passing the same flags)
INPUT=""
INPUT_DIR=""
OUTPUT="result.txt"
N=10
PORT=9090

# Parse flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    -input)     INPUT="$2";     shift 2 ;;
    -input-dir) INPUT_DIR="$2"; shift 2 ;;
    -output)    OUTPUT="$2";    shift 2 ;;
    -n)         N="$2";         shift 2 ;;
    -port)      PORT="$2";      shift 2 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$INPUT" && -z "$INPUT_DIR" ]]; then
  echo "Usage: $0 -input <file>|-input-dir <dir> [-output <file>] [-n <workers>] [-port <port>]" >&2
  exit 1
fi

echo "=== Step 1: Fetching hosts ==="
(cd "$SCRIPTS" && go run ./make_hosts -n "$N" -f "$HOSTS")

echo ""
echo "=== Step 2: Running MapReduce ==="
EXTRA_FLAGS=()
[[ -n "$INPUT" ]]     && EXTRA_FLAGS+=(-input     "$INPUT")
[[ -n "$INPUT_DIR" ]] && EXTRA_FLAGS+=(-input-dir "$INPUT_DIR")

(cd "$SCRIPTS" && go run ./mapreduce \
  -hosts "$HOSTS" \
  -output "$OUTPUT" \
  -n "$N" \
  -port "$PORT" \
  "${EXTRA_FLAGS[@]}")

echo ""
echo "All done. Results are in $OUTPUT"
