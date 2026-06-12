#!/usr/bin/env bash
# Orchestrates the full workflow:
#   1. Fetch N available hosts
#   2. Start an HTTP server on each host
#   3. Collect memory + CPU load stats, saving them to stats.txt
#   4. Kill the HTTP servers and clean up

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS="$ROOT/scripts"
BIN_DIR="$ROOT/bin"
MANIFEST="$ROOT/manifest.json"
HOSTS="hosts.txt"
STATS="stats.txt"
N=100  # number of cluster nodes; must match "n" in manifest.json

if [[ ! -x "$BIN_DIR/slr_make_hosts" || ! -x "$BIN_DIR/slr_deploy" || ! -x "$BIN_DIR/slr_collect" || ! -x "$BIN_DIR/slr_cleanup" ]]; then
  "$SCRIPTS/build_cpp.sh"
fi

cleanup() {
  echo ""
  echo "=== Step 4: Cleaning up ==="
  "$BIN_DIR/slr_cleanup" -m "$MANIFEST" -h "$HOSTS" || true
}
trap cleanup EXIT

echo "=== Step 1: Fetching $N hosts ==="
"$BIN_DIR/slr_make_hosts" -n "$N" -f "$HOSTS"

echo ""
echo "=== Step 2: Starting HTTP servers ==="
"$BIN_DIR/slr_deploy" -m "$MANIFEST" -h "$HOSTS"

echo ""
echo "=== Step 3: Collecting stats (1/5/15-min load + memory) ==="
"$BIN_DIR/slr_collect" -m "$MANIFEST" -h "$HOSTS" -o "$STATS"
echo "Stats written to $STATS"

echo ""
echo "All done. Stats are in $STATS"
