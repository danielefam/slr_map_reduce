#!/usr/bin/env bash
set -uo pipefail

echo "=== Starting Regular Demo ==="
echo "Launching MapReduce job with Common Crawl data..."

../mapreduce.sh -job scripts/jobs/wordcount -commoncrawl -files-limit 15 -n 4 -output result_regular_demo.txt > regular_demo.log 2>&1 &
MR_PID=$!

echo "Waiting for MapReduce job to complete..."
wait $MR_PID

echo "MapReduce completed successfully!"
echo ""
echo "=== End of MapReduce log ==="
tail -n 15 regular_demo.log
