#!/usr/bin/env bash
# Local single-node Kafka Streams deploy following the official quickstart:
#   https://kafka.apache.org/43/getting-started/quickstart/  (downloaded files, no Docker)
#   https://kafka.apache.org/43/streams/quickstart/
#
# Steps:
#   1. Download kafka_2.13-<version>.tgz (cached in ./dist/)
#   2. Extract to $STREAMS_ROOT (default /tmp/kafka-streams)
#   3. Format storage with KRaft standalone mode (no ZooKeeper)
#   4. Start the Kafka broker in the background
#   5. Create topics: streams-plaintext-input, streams-wordcount-output
#   6. Start the bundled WordCountDemo in the background
#      (bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo)
#
# Usage:
#   ./deploy_streams.sh [-version 4.3.0] [-root /tmp/kafka-streams]
#
# Prerequisites: Java 17+ on PATH.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

KAFKA_VERSION="4.3.0"
SCALA_VERSION="2.13"
STREAMS_ROOT="/tmp/kafka-streams"
BROKER_PORT=9092

while [[ $# -gt 0 ]]; do
  case "$1" in
    -version) KAFKA_VERSION="$2"; shift 2 ;;
    -root)    STREAMS_ROOT="$2";  shift 2 ;;
    -port)    BROKER_PORT="$2";   shift 2 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

KAFKA_DIST="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
TARBALL="${KAFKA_DIST}.tgz"
DIST_CACHE="$SCRIPT_DIR/dist"
PRIMARY_URL="https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/${TARBALL}"
ARCHIVE_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${TARBALL}"
KAFKA_HOME="$STREAMS_ROOT/$KAFKA_DIST"

echo "=== Kafka Streams local deploy (version $KAFKA_VERSION) ==="

# ── Prerequisite: Java 17+ ──────────────────────────────────────────────────
if ! command -v java &>/dev/null; then
  echo "ERROR: java not found on PATH. Install Java 17+." >&2; exit 1
fi
java_ver=$(java -version 2>&1 | sed -nE 's/.*version "([0-9]+).*/\1/p')
if [[ -z "$java_ver" || "$java_ver" -lt 17 ]]; then
  echo "ERROR: Java 17+ required, found: $(java -version 2>&1 | head -1)" >&2; exit 1
fi
echo "--- Java OK: $(java -version 2>&1 | head -1) ---"

# ── Step 1: download ────────────────────────────────────────────────────────
mkdir -p "$DIST_CACHE"
if [[ ! -f "$DIST_CACHE/$TARBALL" ]]; then
  echo "--- Downloading $TARBALL ---"
  curl -fL --retry 3 -o "$DIST_CACHE/$TARBALL.part" "$PRIMARY_URL" \
    || curl -fL --retry 3 -o "$DIST_CACHE/$TARBALL.part" "$ARCHIVE_URL"
  mv "$DIST_CACHE/$TARBALL.part" "$DIST_CACHE/$TARBALL"
else
  echo "--- Using cached $TARBALL ---"
fi

# ── Step 2: extract ─────────────────────────────────────────────────────────
mkdir -p "$STREAMS_ROOT"
if [[ ! -d "$KAFKA_HOME" ]]; then
  echo "--- Extracting to $KAFKA_HOME ---"
  tar -xzf "$DIST_CACHE/$TARBALL" -C "$STREAMS_ROOT"
else
  echo "--- Already extracted: $KAFKA_HOME ---"
fi

# ── Step 3: format storage (KRaft standalone) ────────────────────────────────
# quickstart step: kafka-storage.sh format --standalone -t <uuid> -c config/server.properties
# We write a custom server.properties so log.dirs points to STREAMS_ROOT (not /tmp default).
STORAGE_DIR="$STREAMS_ROOT/kafka-logs"
SERVER_PROPS="$STREAMS_ROOT/server.properties"
if [[ ! -d "$STORAGE_DIR" ]]; then
  echo "--- Formatting KRaft storage ---"
  mkdir -p "$STORAGE_DIR"
  # Copy the bundled config and override log.dirs
  cp "$KAFKA_HOME/config/server.properties" "$SERVER_PROPS"
  sed -i "s|^log\.dirs=.*|log.dirs=$STORAGE_DIR|" "$SERVER_PROPS"
  # If log.dirs wasn't in the file, append it
  grep -q "^log\.dirs=" "$SERVER_PROPS" || echo "log.dirs=$STORAGE_DIR" >> "$SERVER_PROPS"
  CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
  "$KAFKA_HOME/bin/kafka-storage.sh" format \
    --standalone \
    -t "$CLUSTER_ID" \
    -c "$SERVER_PROPS" \
    >/dev/null
  echo "    Cluster ID: $CLUSTER_ID"
else
  echo "--- Storage already formatted: $STORAGE_DIR ---"
fi

# ── Step 4: start broker ─────────────────────────────────────────────────────
BROKER_PID_FILE="$STREAMS_ROOT/broker.pid"
if [[ -f "$BROKER_PID_FILE" ]] && kill -0 "$(cat "$BROKER_PID_FILE")" 2>/dev/null; then
  echo "--- Broker already running (PID $(cat "$BROKER_PID_FILE")) ---"
else
  echo "--- Starting Kafka broker ---"
  # Override log.dirs so it doesn't land in /tmp/kraft-combined-logs default
  nohup "$KAFKA_HOME/bin/kafka-server-start.sh" \
    "$SERVER_PROPS" \
    > "$STREAMS_ROOT/broker.log" 2>&1 &
  echo $! > "$BROKER_PID_FILE"
  echo "    PID: $(cat "$BROKER_PID_FILE"), log: $STREAMS_ROOT/broker.log"

  echo "--- Waiting for broker on port $BROKER_PORT ---"
  for _ in $(seq 1 30); do
    if bash -c "echo > /dev/tcp/127.0.0.1/$BROKER_PORT" 2>/dev/null; then break; fi
    sleep 2
  done
  if ! bash -c "echo > /dev/tcp/127.0.0.1/$BROKER_PORT" 2>/dev/null; then
    echo "ERROR: broker did not start; see $STREAMS_ROOT/broker.log" >&2; exit 1
  fi
  echo "    ✓ broker is up"
fi

BOOTSTRAP="localhost:${BROKER_PORT}"

# ── Step 5: create topics ────────────────────────────────────────────────────
# quickstart: streams-plaintext-input (plain) + streams-wordcount-output (compacted changelog)
echo "--- Creating Streams topics ---"
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic streams-plaintext-input \
  --partitions 1 --replication-factor 1
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic streams-wordcount-output \
  --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact
echo "    Topics ready:"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --describe --exclude-internal 2>/dev/null | grep -E 'Topic:|streams-'

# ── Step 6: start WordCountDemo (bundled with Kafka, no compilation needed) ──
# quickstart: bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo
WORDCOUNT_PID_FILE="$STREAMS_ROOT/wordcount.pid"
if [[ -f "$WORDCOUNT_PID_FILE" ]] && kill -0 "$(cat "$WORDCOUNT_PID_FILE")" 2>/dev/null; then
  echo "--- WordCountDemo already running (PID $(cat "$WORDCOUNT_PID_FILE")) ---"
else
  echo "--- Starting WordCountDemo ---"
  # APP_ID used by WordCountDemo is "streams-wordcount"; state dir under STREAMS_ROOT
  KAFKA_OPTS="-Dstreams.state.dir=$STREAMS_ROOT/streams-state" \
  nohup "$KAFKA_HOME/bin/kafka-run-class.sh" \
    org.apache.kafka.streams.examples.wordcount.WordCountDemo \
    > "$STREAMS_ROOT/wordcount.log" 2>&1 &
  echo $! > "$WORDCOUNT_PID_FILE"
  echo "    PID: $(cat "$WORDCOUNT_PID_FILE"), log: $STREAMS_ROOT/wordcount.log"
  sleep 3   # give Streams time to initialise and join the group
fi

echo ""
echo "=== Kafka Streams ready ==="
echo "  Bootstrap:      $BOOTSTRAP"
echo "  Input topic:    streams-plaintext-input"
echo "  Output topic:   streams-wordcount-output"
echo "  Broker log:     $STREAMS_ROOT/broker.log"
echo "  WordCount log:  $STREAMS_ROOT/wordcount.log"
echo ""
echo "Next steps:"
echo "  Feed a file:     ./run_streams_wordcount.sh -input <file>"
echo "  Interactive:     $KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic streams-plaintext-input"
echo "  Watch output:    $KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic streams-wordcount-output --from-beginning --formatter-property print.key=true --formatter-property print.value=true --formatter-property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --formatter-property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer"
echo "  Clean up:        ./clean_streams.sh"
