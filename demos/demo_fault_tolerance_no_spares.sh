#!/usr/bin/env bash
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPTS="$ROOT/scripts"

echo "=== Starting Fault Tolerance Demo (NO SPARES) ==="
echo "Fetching exactly 4 hosts for 4 active workers (0 spare workers)..."

(cd "$SCRIPTS" && go run ./make_hosts -n 4 -f "$ROOT/hosts_no_spares.txt")

echo "Launching MapReduce job with Common Crawl data..."

(cd "$SCRIPTS" && go run ./mapreduce \
  -hosts "$ROOT/hosts_no_spares.txt" \
  -job "$SCRIPTS/jobs/wordcount" \
  -output "$ROOT/result_fault_demo_no_spares.txt" \
  -n 4 -commoncrawl -files-limit 15) > "$ROOT/fault_demo_no_spares.log" 2>&1 &
MR_PID=$!

echo "Waiting 20 seconds for workers to deploy and start processing..."
sleep 20

HOST=$(head -n 1 "$ROOT/hosts_no_spares.txt")
echo "Killing worker process on $HOST to trigger fault..."
ssh "$HOST" "pkill -9 worker" || true

echo "Waiting for MapReduce job to complete (or fail)..."
wait $MR_PID || true

echo "MapReduce job exited."
echo ""
echo "=== Relevant logs showing failure due to lack of spares ==="
grep -E -i "fail|timeout|error|replace|reassign|spare" "$ROOT/fault_demo_no_spares.log" || true
echo ""
echo "=== End of MapReduce log ==="
tail -n 15 "$ROOT/fault_demo_no_spares.log"
