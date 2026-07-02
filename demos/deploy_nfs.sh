#!/usr/bin/env bash
# deploy_nfs.sh — Deploys MapReduce workers using the rubric-specified pattern:
#
#   1 SCP  → copies the worker binary ONCE to the first reachable host's HOME
#             (HOME is on the lab NFS, so every machine can read ~/mr-worker)
#   N SSH  → each of the N workers starts from that shared NFS path
#
# This avoids N large SCP transfers and lets all nodes share a single binary copy.
# Equivalent to running mapreduce.sh with the -nfs-deploy flag.
#
# Usage:
#   ./deploy_nfs.sh -job scripts/jobs/wordcount -input /path/to/data.txt [-n 10] [-port 9090]
#   ./deploy_nfs.sh -job scripts/jobs/wordcount -commoncrawl [-crawl CC-MAIN-2026-21] [-n 10]
#
# All flags are forwarded verbatim to the mapreduce orchestrator.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS="$(cd "$ROOT/scripts" && pwd)"
HOSTS="$ROOT/hosts.txt"

INPUT=""
JOB=""
USE_COMMONCRAWL=0
CRAWL=""
FILES_LIMIT=0
CHUNKS_LIMIT=0
OUTPUT="result.txt"
N=10
PORT=9090

while [[ $# -gt 0 ]]; do
  case "$1" in
  -job)        JOB="$2";             shift 2 ;;
  -input)      INPUT="$2";           shift 2 ;;
  -commoncrawl) USE_COMMONCRAWL=1;   shift   ;;
  -crawl)      CRAWL="$2";           shift 2 ;;
  -files-limit) FILES_LIMIT="$2";    shift 2 ;;
  -chunks-limit) CHUNKS_LIMIT="$2";  shift 2 ;;
  -output)     OUTPUT="$2";          shift 2 ;;
  -n)          N="$2";               shift 2 ;;
  -port)       PORT="$2";            shift 2 ;;
  *)           echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$JOB" ]]; then
  echo "Usage: missing required -job <path>" >&2
  exit 1
fi

if [[ ! -d "$JOB" ]]; then
  echo "Job path must be an existing directory: $JOB" >&2
  exit 1
fi

JOB="$(cd "$JOB" && pwd)"

if [[ "$OUTPUT" != /* ]]; then
  OUTPUT="$ROOT/$OUTPUT"
fi

HOST_POOL_SIZE=$((N * 2))

echo "=== Step 1: Fetching $HOST_POOL_SIZE hosts for $N active workers ==="
(cd "$SCRIPTS" && go run ./make_hosts -n "$HOST_POOL_SIZE" -f "$HOSTS")

echo ""
echo "=== Step 2: Running MapReduce (NFS deploy: 1 SCP to HOME + $N SSH starts) ==="

EXTRA_FLAGS=(-nfs-deploy)
[[ -n "$INPUT" ]] && EXTRA_FLAGS+=(-input "$INPUT")
if [[ "$USE_COMMONCRAWL" -eq 1 ]]; then
  EXTRA_FLAGS+=(-commoncrawl)
  [[ -n "$CRAWL" ]] && EXTRA_FLAGS+=(-crawl "$CRAWL")
  ((FILES_LIMIT > 0)) && EXTRA_FLAGS+=(-files-limit "$FILES_LIMIT")
  ((CHUNKS_LIMIT > 0)) && EXTRA_FLAGS+=(-chunks-limit "$CHUNKS_LIMIT")
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
