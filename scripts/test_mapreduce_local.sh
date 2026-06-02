#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
RUN_DIR="$REPO_ROOT/run"
PORT=20123

echo "Starting local MapReduce test..."

# Create run directory
mkdir -p "$RUN_DIR"

# Create a local manifest for localhost
cat > "$RUN_DIR/mr_manifest_local.env" <<EOF
HOSTS_FILE=$RUN_DIR/hosts-local-mr.txt
PORT=$PORT
MR_SCRATCH_ROOT=/tmp/slr_map_reduce_test
EOF

# Create hosts file pointing to localhost
printf '127.0.0.1\n' > "$RUN_DIR/hosts-local-mr.txt"

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    # Kill worker if running
    if [[ -n "${worker_pid:-}" ]] && kill -0 "$worker_pid" 2>/dev/null; then
        kill "$worker_pid" 2>/dev/null || true
        wait "$worker_pid" 2>/dev/null || true
    fi
    # Remove scratch directory
    rm -rf /tmp/slr_map_reduce_test
}

trap cleanup EXIT

# Start the worker
echo "Starting mr_worker on port $PORT..."
"$REPO_ROOT/bin/mr_worker" --port "$PORT" --worker-id 0 --scratch-root /tmp/slr_map_reduce_test > "$RUN_DIR/mr_worker_local.log" 2>&1 &
worker_pid=$!

# Wait for worker to be ready
echo "Waiting for worker to be ready..."
for attempt in {1..50}; do
    if bash -lc "exec 3<>/dev/tcp/127.0.0.1/$PORT" 2>/dev/null; then
        echo "Worker is ready"
        break
    fi
    if (( attempt == 50 )); then
        echo "Worker failed to start"
        exit 1
    fi
    sleep 0.1
done

# Run the coordinator with local input
echo "Running MapReduce coordinator..."
"$REPO_ROOT/bin/mr_coordinator" \
    --hosts "$RUN_DIR/hosts-local-mr.txt" \
    --port "$PORT" \
    --input "$REPO_ROOT/examples/wordcount_input.txt" \
    --output "$RUN_DIR/mr_test_local_output.txt" \
    --chunk-lines 2 \
    --job-manifest "$RUN_DIR/mr_job_local.env"

# Check if output matches expected
echo "Comparing output..."
if diff -u "$REPO_ROOT/examples/wordcount_expected.txt" "$RUN_DIR/mr_test_local_output.txt"; then
    echo "✓ Test PASSED: Output matches expected"
    exit 0
else
    echo "✗ Test FAILED: Output does not match expected"
    exit 1
fi
