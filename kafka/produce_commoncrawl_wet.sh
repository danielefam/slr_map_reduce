#!/usr/bin/env bash
# Streams a compressed Common Crawl WET file directly into a Kafka topic.
# This avoids staging a large decompressed file on shared NFS.
#
# Usage:
#   ./produce_commoncrawl_wet.sh -url https://data.commoncrawl.org/<WET-PATH>.wet.gz
#   ./produce_commoncrawl_wet.sh -url https://data.commoncrawl.org/<WET-PATH>.wet.gz -max-lines 10000
#
# Requires a running local Kafka quickstart deploy:
#   ./deploy_streams.sh

set -euo pipefail

KAFKA_VERSION="4.3.0"
SCALA_VERSION="2.13"
STREAMS_ROOT="/tmp/kafka-streams"
BROKER_PORT=9092
TOPIC="streams-plaintext-input"
URL=""
MAX_LINES=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -url)       URL="$2";          shift 2 ;;
    -topic)     TOPIC="$2";        shift 2 ;;
    -max-lines) MAX_LINES="$2";    shift 2 ;;
    -root)      STREAMS_ROOT="$2"; shift 2 ;;
    -version)   KAFKA_VERSION="$2"; shift 2 ;;
    -port)      BROKER_PORT="$2";  shift 2 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$URL" ]]; then
  echo "Usage: $0 -url <common-crawl-wet.gz-url> [-max-lines N]" >&2
  exit 1
fi

if ! [[ "$MAX_LINES" =~ ^[0-9]+$ ]]; then
  echo "ERROR: -max-lines must be a non-negative integer" >&2
  exit 1
fi

KAFKA_HOME="$STREAMS_ROOT/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
BOOTSTRAP="localhost:${BROKER_PORT}"

for cmd in curl gunzip; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: $cmd not found on PATH" >&2
    exit 1
  fi
done

if [[ ! -x "$KAFKA_HOME/bin/kafka-console-producer.sh" ]]; then
  echo "ERROR: Kafka distribution not found at $KAFKA_HOME. Run ./deploy_streams.sh first." >&2
  exit 1
fi

if ! bash -c "echo > /dev/tcp/127.0.0.1/$BROKER_PORT" 2>/dev/null; then
  echo "ERROR: Kafka broker not reachable on $BOOTSTRAP. Run ./deploy_streams.sh first." >&2
  exit 1
fi

echo "=== Streaming Common Crawl WET directly into Kafka ==="
echo "    URL:       $URL"
echo "    Bootstrap: $BOOTSTRAP"
echo "    Topic:     $TOPIC"
if (( MAX_LINES > 0 )); then
  echo "    Limit:     first $MAX_LINES lines"
else
  echo "    Limit:     full stream"
fi

if (( MAX_LINES > 0 )); then
  set +o pipefail
  curl -fsL "$URL" \
    | gunzip -c \
    | awk -v max_lines="$MAX_LINES" 'NR <= max_lines { print } NR >= max_lines { exit }' \
    | "$KAFKA_HOME/bin/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"
  status=$?
  set -o pipefail
  exit "$status"
fi

curl -fsL "$URL" \
  | gunzip -c \
  | "$KAFKA_HOME/bin/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"