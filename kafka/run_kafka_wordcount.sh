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
#      consumer-group lag on bench-input reaches zero
#   5. Consume bench-output into a local result file (latest count per word)
#
# Usage:
#   ./run_kafka_wordcount.sh -input ../test_input.txt [-output kafka_result.txt]
#
# Comparison with the custom framework:
#   ../mapreduce.sh -job wordcount -input <same file> ... prints
#   "TIMING ... compute_seconds=…"; this script prints the same metric.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOSTS_FILE="$SCRIPT_DIR/kafka_hosts.txt"
REMOTE_ROOT="/tmp/kafka-bench"
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

kssh() { ssh -o BatchMode=yes "$CONTROLLER_HOST" "$@"; }

echo "=== Kafka Streams word-count benchmark (bootstrap: $BOOTSTRAP) ==="

# ── Step 1: topics ──────────────────────────────────────────────────────────
echo "--- Recreating topics ---"
kssh "
  cd $KAFKA_HOME
  bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic bench-input  2>/dev/null || true
  bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic bench-output 2>/dev/null || true
  sleep 2
  bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic bench-input  --partitions $N_PARTITIONS --replication-factor 1
  bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic bench-output --partitions $N_PARTITIONS --replication-factor 1
"

# ── Step 2: compile the Streams app remotely ───────────────────────────────
echo "--- Compiling WordCountJob on $CONTROLLER_HOST ---"
scp -q "$SCRIPT_DIR/WordCountJob.java" "$CONTROLLER_HOST:$REMOTE_ROOT/"
kssh "cd $REMOTE_ROOT && javac -cp '$KAFKA_HOME/libs/*' WordCountJob.java"

# ── Step 3: produce input (untimed, like the Go /data phase) ───────────────
echo "--- Producing input ($(wc -l < "$INPUT") lines) ---"
# Stream the local file through ssh into the console producer on the node.
ssh -o BatchMode=yes "$CONTROLLER_HOST" \
  "$KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic bench-input" \
  < "$INPUT"

# ── Step 4: run the Streams job; it exits once lag == 0 and prints TIMING ──
echo "--- Running Kafka Streams job ---"
kssh "cd $REMOTE_ROOT && java -cp '$KAFKA_HOME/libs/*:.' WordCountJob $BOOTSTRAP" \
  | tee /tmp/kafka_bench_run.log
TIMING_LINE="$(grep -E '^TIMING ' /tmp/kafka_bench_run.log | tail -1)"

# ── Step 5: collect final counts ───────────────────────────────────────────
echo "--- Collecting results into $OUTPUT ---"
kssh "
  cd $KAFKA_HOME
  timeout 30 bin/kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP \
    --topic bench-output --from-beginning \
    --property print.key=true --property key.separator=\$'\t' \
    --value-deserializer org.apache.kafka.common.serialization.LongDeserializer \
    2>/dev/null || true
" | awk -F'\t' '{count[$1]=$2} END {for (w in count) printf "%s\t%s\n", w, count[w]}' \
  | sort -k2,2nr -k1,1 > "$OUTPUT"

echo ""
echo "Results: $OUTPUT ($(wc -l < "$OUTPUT") distinct words)"
echo "$TIMING_LINE"
echo "Compare with the Go framework's log line: 'TIMING nodes=… compute_seconds=…'"
