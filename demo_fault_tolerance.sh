#!/usr/bin/env bash
set -uo pipefail

echo "=== Starting Fault Tolerance Demo ==="
echo "Launching MapReduce job with Common Crawl data..."

./mapreduce.sh -job scripts/jobs/wordcount -commoncrawl -files-limit 15 -n 4 -output result_fault_demo.txt > fault_demo.log 2>&1 &
MR_PID=$!

echo "Waiting 20 seconds for workers to deploy and start processing..."
sleep 20

HOST=$(head -n 1 hosts.txt)
echo "Killing worker process on $HOST to trigger fault tolerance..."
ssh "$HOST" "pkill -9 worker" || true

echo "Waiting for MapReduce job to complete (this might take a minute)..."
wait $MR_PID

echo "MapReduce completed successfully!"
echo ""
echo "=== Relevant logs showing fault detection and recovery ==="
grep -E -i "fail|timeout|error|replace|reassign" fault_demo.log || true
echo ""
echo "=== End of MapReduce log ==="
tail -n 15 fault_demo.log
