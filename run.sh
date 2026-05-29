#!/usr/bin/env bash
# Orchestrates the full workflow:
#   1. Fetch N available hosts
#   2. Start an HTTP server on each host
#   3. Collect memory + CPU load stats, saving them to stats.txt
#   4. Kill the HTTP servers and clean up

set -euo pipefail

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")/scripts" && pwd)"
MANIFEST="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/manifest.json"
HOSTS="hosts.txt"
STATS="stats.txt"
N=100  # number of cluster nodes; must match "n" in manifest.json

cleanup() {
  echo ""
  echo "=== Step 4: Cleaning up ==="
  (cd "$SCRIPTS" && go run ./cleanup -m "$MANIFEST" -h "../$HOSTS") || true
}
trap cleanup EXIT

echo "=== Step 1: Fetching $N hosts ==="
(cd "$SCRIPTS" && go run ./make_hosts -n "$N" -f "../$HOSTS")

echo ""
echo "=== Step 2: Starting HTTP servers ==="
(cd "$SCRIPTS" && go run ./deploy -m "$MANIFEST" -h "../$HOSTS") || true

echo ""
echo "=== Step 3: Collecting stats (1/5/15-min load + memory) ==="
(cd "$SCRIPTS" && go run ./collect -m "$MANIFEST" -h "../$HOSTS" -o "../$STATS") || true
echo "Stats written to $STATS"

echo ""
echo "All done. Stats are in $STATS"
