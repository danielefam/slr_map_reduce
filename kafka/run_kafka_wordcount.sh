#!/usr/bin/env bash
# Runs the Kafka Streams word-count benchmark on the cluster deployed by
# deploy_kafka.sh, measures compute time, and downloads the final counts.
#
# Pipeline:
#   1. (Re)create bench-input / bench-output topics
#   2. Compile WordCountJob.java on the controller node (bundled Kafka jars,
#      no Maven required)
#   3. Produce the input file into bench-input        (NOT timed — mirrors
#      the Go orchestrator, which excludes /data uploads from compute time)
#   4. Start WordCountJob; it self-reports TIMING compute_seconds=… once the
#      app's consumer-group lag reaches zero on every assigned non-internal topic
#   5. Extract final state-store counts from WordCountJob RESULT lines
#
# Usage:
#   ./run_kafka_wordcount.sh -input ../test_input.txt [-output kafka_result.txt]
#
# Comparison with the custom framework:
#   ../mapreduce.sh -job scripts/jobs/wordcount -input <same file> ... prints
#   "TIMING ... compute_seconds=…"; this script prints the same metric.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOSTS_FILE="$SCRIPT_DIR/kafka_hosts.txt"
REMOTE_ROOT="/tmp/kafka-bench-${USER:-$(id -un)}"
KAFKA_VERSION="4.3.0"
SCALA_VERSION="2.13"
BROKER_PORT=9092
INPUT=""
OUTPUT="kafka_result.txt"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -input)   INPUT="$2";         shift 2 ;;
    -output)  OUTPUT="$2";        shift 2 ;;
    -version) KAFKA_VERSION="$2"; shift 2 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

[[ -n "$INPUT" && -f "$INPUT" ]] || { echo "Usage: $0 -input <file> [-output <file>]" >&2; exit 1; }
[[ -f "$HOSTS_FILE" ]] || { echo "Run ./deploy_kafka.sh first ($HOSTS_FILE missing)" >&2; exit 1; }

CONTROLLER_HOST="$(head -1 "$HOSTS_FILE")"
BOOTSTRAP="${CONTROLLER_HOST}:${BROKER_PORT}"
KAFKA_HOME="$REMOTE_ROOT/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
N_PARTITIONS="$(grep -cv '^[[:space:]]*$' "$HOSTS_FILE")"
STREAM_THREADS="${KAFKA_STREAM_THREADS:-$N_PARTITIONS}"
RUN_ID="${KAFKA_RUN_ID:-$(date +%Y%m%d%H%M%S)}"
INPUT_TOPIC="bench-input-${RUN_ID}"
OUTPUT_TOPIC="bench-output-${RUN_ID}"
APP_ID="bench-wordcount-${RUN_ID}"

kssh() { ssh -o BatchMode=yes "$CONTROLLER_HOST" "$@"; }

wait_topic_deleted() {
  local topic="$1"
  kssh "
    cd $KAFKA_HOME
    for _ in \$(seq 1 30); do
      if ! bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic $topic >/dev/null 2>&1; then
        exit 0
      fi
      sleep 2
    done
    echo 'ERROR: topic $topic was not deleted in time' >&2
    exit 1
  "
}

wait_app_internal_topics_deleted() {
  kssh "
    cd $KAFKA_HOME
    for _ in \$(seq 1 30); do
      if ! bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --list 2>/dev/null | grep -q '^$APP_ID-'; then
        exit 0
      fi
      sleep 2
    done
    echo 'ERROR: $APP_ID internal topics were not deleted in time' >&2
    bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --list | grep '^$APP_ID-' >&2 || true
    exit 1
  "
}

echo "=== Kafka Streams word-count benchmark (bootstrap: $BOOTSTRAP) ==="
echo "--- Run ID: $RUN_ID (input=$INPUT_TOPIC output=$OUTPUT_TOPIC app=$APP_ID) ---"
echo "--- Stream threads: $STREAM_THREADS ---"

# ── Step 1: topics ──────────────────────────────────────────────────────────
echo "--- Recreating topics ---"
kssh "
  cd $KAFKA_HOME
  pkill -u \$(id -un) -f '[W]ordCountJob' 2>/dev/null || true
  sleep 3
  bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic $INPUT_TOPIC  2>/dev/null || true
  bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic $OUTPUT_TOPIC 2>/dev/null || true
  for t in \$(bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --list 2>/dev/null | grep '^$APP_ID-' || true); do
    bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic \$t 2>/dev/null || true
  done
"
wait_topic_deleted "$INPUT_TOPIC"
wait_topic_deleted "$OUTPUT_TOPIC"
wait_app_internal_topics_deleted
kssh "
  cd $KAFKA_HOME
  bin/kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group $APP_ID 2>/dev/null || true
  rm -rf $REMOTE_ROOT/streams-state
  bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic $INPUT_TOPIC  --partitions $N_PARTITIONS --replication-factor 1
  bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic $OUTPUT_TOPIC --partitions $N_PARTITIONS --replication-factor 1
"

# ── Step 2: compile the Streams app remotely ───────────────────────────────
echo "--- Compiling WordCountJob on $CONTROLLER_HOST ---"
scp -q "$SCRIPT_DIR/WordCountJob.java" "$CONTROLLER_HOST:$REMOTE_ROOT/"
kssh "cd $REMOTE_ROOT && javac -cp '$KAFKA_HOME/libs/*' WordCountJob.java"

# ── Step 3: produce input (untimed, like the Go /data phase) ───────────────
echo "--- Producing input ($(wc -l < "$INPUT") lines) ---"
# Stream the local file through ssh into the console producer on the node.
ssh -o BatchMode=yes "$CONTROLLER_HOST" \
  "$KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic $INPUT_TOPIC" \
  < "$INPUT"

# ── Step 4: run the Streams job; it exits once total lag == 0 and prints TIMING ──
echo "--- Running Kafka Streams job ---"
kssh "cd $REMOTE_ROOT && KAFKA_BENCH_ROOT=$REMOTE_ROOT KAFKA_STREAM_THREADS=$STREAM_THREADS timeout 1800 java -cp '$KAFKA_HOME/libs/*:.' WordCountJob $BOOTSTRAP $INPUT_TOPIC $OUTPUT_TOPIC $APP_ID" \
  | tee /tmp/kafka_bench_run.log >/dev/null
TIMING_LINE="$(grep -E '^TIMING ' /tmp/kafka_bench_run.log | tail -1)"
[[ -n "$TIMING_LINE" ]] || { echo "ERROR: WordCountJob did not report TIMING" >&2; exit 1; }

# ── Step 5: collect final counts ───────────────────────────────────────────
echo "--- Writing final state-store counts into $OUTPUT ---"
awk -F'\t' '$1 == "RESULT" { print $2 "\t" $3 }' /tmp/kafka_bench_run.log \
  | sort -k2,2nr -k1,1 > "$OUTPUT"

echo ""
echo "Results: $OUTPUT ($(wc -l < "$OUTPUT") distinct words)"
echo "$TIMING_LINE"
echo "Compare with the Go framework's log line: 'TIMING nodes=… compute_seconds=…'"
