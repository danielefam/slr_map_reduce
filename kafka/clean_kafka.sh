#!/usr/bin/env bash
# Stops Kafka brokers and removes all Kafka files from the nodes previously
# deployed by deploy_kafka.sh (reads kafka_hosts.txt).
#
# Usage: ./clean_kafka.sh [-hosts kafka_hosts.txt]

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOSTS_FILE="$SCRIPT_DIR/kafka_hosts.txt"
REMOTE_ROOT="/tmp/kafka-bench"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -hosts) HOSTS_FILE="$2"; shift 2 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "No $HOSTS_FILE found — nothing to clean." >&2
  exit 0
fi

mapfile -t HOSTS < <(grep -v '^[[:space:]]*$' "$HOSTS_FILE")
echo "=== Cleaning Kafka from ${#HOSTS[@]} node(s) ==="

for h in "${HOSTS[@]}"; do
  echo "--- $h ---"
  ssh -o BatchMode=yes -o ConnectTimeout=10 "$h" "
    # Graceful stop via PID file, then hard-kill any leftover Kafka JVM.
    if [[ -f $REMOTE_ROOT/kafka.pid ]]; then
      kill \$(cat $REMOTE_ROOT/kafka.pid) 2>/dev/null || true
      sleep 3
      kill -9 \$(cat $REMOTE_ROOT/kafka.pid) 2>/dev/null || true
    fi
    pkill -u \$(id -un) -f 'kafka\.Kafka' 2>/dev/null || true
    pkill -u \$(id -un) -f 'kafka-streams' 2>/dev/null || true
    rm -rf $REMOTE_ROOT
  " && echo "  ✓ cleaned" || echo "  ✗ ssh failed (host unreachable?)"
done

rm -f "$HOSTS_FILE"
echo "Done."
