#!/usr/bin/env bash
# Stops the Kafka Streams WordCountDemo and the Kafka broker started by
# deploy_streams.sh, then removes all state from $STREAMS_ROOT.
#
# Usage: ./clean_streams.sh [-root /tmp/kafka-streams]

set -uo pipefail

STREAMS_ROOT="/tmp/kafka-streams"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -root) STREAMS_ROOT="$2"; shift 2 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

echo "=== Cleaning Kafka Streams local setup ==="

# ── Stop WordCountDemo ────────────────────────────────────────────────────────
WORDCOUNT_PID_FILE="$STREAMS_ROOT/wordcount.pid"
if [[ -f "$WORDCOUNT_PID_FILE" ]]; then
  WC_PID=$(cat "$WORDCOUNT_PID_FILE")
  if kill -0 "$WC_PID" 2>/dev/null; then
    echo "--- Stopping WordCountDemo (PID $WC_PID) ---"
    kill "$WC_PID" 2>/dev/null || true
    sleep 2
    kill -9 "$WC_PID" 2>/dev/null || true
  fi
  rm -f "$WORDCOUNT_PID_FILE"
fi
# Belt-and-suspenders: kill any leftover WordCountDemo processes owned by this user
pkill -u "$(id -un)" -f 'WordCountDemo' 2>/dev/null || true

# ── Stop Kafka broker ─────────────────────────────────────────────────────────
BROKER_PID_FILE="$STREAMS_ROOT/broker.pid"
if [[ -f "$BROKER_PID_FILE" ]]; then
  BROKER_PID=$(cat "$BROKER_PID_FILE")
  if kill -0 "$BROKER_PID" 2>/dev/null; then
    echo "--- Stopping Kafka broker (PID $BROKER_PID) ---"
    kill "$BROKER_PID" 2>/dev/null || true
    sleep 3
    kill -9 "$BROKER_PID" 2>/dev/null || true
  fi
  rm -f "$BROKER_PID_FILE"
fi
pkill -u "$(id -un)" -f 'kafka\.Kafka' 2>/dev/null || true

# ── Remove all Kafka Streams state ────────────────────────────────────────────
# quickstart teardown: rm -rf /tmp/kafka-logs /tmp/kraft-combined-logs
# We also remove our STREAMS_ROOT (contains extracted dist, logs, streams state)
if [[ -d "$STREAMS_ROOT" ]]; then
  echo "--- Removing $STREAMS_ROOT ---"
  rm -rf "$STREAMS_ROOT"
fi

# Clean up KRaft default dirs if the broker happened to write there
rm -rf /tmp/kafka-logs /tmp/kraft-combined-logs 2>/dev/null || true

echo "Done. To redeploy: ./deploy_streams.sh"
