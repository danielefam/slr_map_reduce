#!/usr/bin/env bash
# Feeds an input file into the Kafka Streams WordCountDemo deployed by
# deploy_streams.sh, waits for the lag to reach zero, then collects the final
# word counts from the output topic.
#
# Timing: mirrors the Go orchestrator metric — only the Streams processing
# time is measured; feeding the input is excluded (same as /data upload).
#
# Usage:
#   ./run_streams_wordcount.sh -input <file> [-output streams_result.txt] [-root /tmp/kafka-streams] [-version 4.3.0]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

KAFKA_VERSION="4.3.0"
SCALA_VERSION="2.13"
STREAMS_ROOT="/tmp/kafka-streams"
BROKER_PORT=9092
INPUT=""
OUTPUT="streams_result.txt"
LAG_POLL_INTERVAL=2   # seconds between consumer-group lag polls
LAG_ZERO_STREAK=2     # how many consecutive zero-lag reads before we declare done

while [[ $# -gt 0 ]]; do
  case "$1" in
    -input)   INPUT="$2";          shift 2 ;;
    -output)  OUTPUT="$2";         shift 2 ;;
    -root)    STREAMS_ROOT="$2";   shift 2 ;;
    -version) KAFKA_VERSION="$2";  shift 2 ;;
    -port)    BROKER_PORT="$2";    shift 2 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

[[ -n "$INPUT" && -f "$INPUT" ]] \
  || { echo "Usage: $0 -input <file> [-output <file>]" >&2; exit 1; }

KAFKA_HOME="$STREAMS_ROOT/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
BOOTSTRAP="localhost:${BROKER_PORT}"
WORDCOUNT_PID_FILE="$STREAMS_ROOT/wordcount.pid"

# ── Sanity checks ────────────────────────────────────────────────────────────
if ! bash -c "echo > /dev/tcp/127.0.0.1/$BROKER_PORT" 2>/dev/null; then
  echo "ERROR: broker not reachable on port $BROKER_PORT. Run ./deploy_streams.sh first." >&2
  exit 1
fi

if [[ ! -f "$WORDCOUNT_PID_FILE" ]] || ! kill -0 "$(cat "$WORDCOUNT_PID_FILE")" 2>/dev/null; then
  echo "ERROR: WordCountDemo is not running. Run ./deploy_streams.sh first." >&2
  exit 1
fi

echo "=== Kafka Streams WordCount (bootstrap: $BOOTSTRAP) ==="
echo "    Input: $INPUT ($(wc -l < "$INPUT") lines)"

# ── Step 1: stop WordCountDemo, reset topics + state, restart fresh ──────────
echo "--- Stopping WordCountDemo for a clean run ---"
if [[ -f "$WORDCOUNT_PID_FILE" ]]; then
  WC_PID=$(cat "$WORDCOUNT_PID_FILE")
  kill "$WC_PID" 2>/dev/null || true
  # Wait up to 15s for it to actually die so the consumer group becomes empty
  for _ in $(seq 1 15); do
    kill -0 "$WC_PID" 2>/dev/null || break
    sleep 1
  done
  kill -9 "$WC_PID" 2>/dev/null || true
  rm -f "$WORDCOUNT_PID_FILE"
fi
pkill -u "$(id -un)" -f 'WordCountDemo' 2>/dev/null || true
# Wait for the consumer group to become empty (broker needs to detect member left)
echo "    waiting for consumer group to go empty..."
for _ in $(seq 1 20); do
  state=$("$KAFKA_HOME/bin/kafka-consumer-groups.sh" \
    --bootstrap-server "$BOOTSTRAP" --describe --group streams-wordcount 2>/dev/null \
    | awk '/GROUP.*TOPIC/ {next} /streams-wordcount/ {print $NF; exit}')
  [[ "$state" == "Empty" || -z "$state" ]] && break
  sleep 2
done

echo "--- Resetting topics and Streams state ---"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --delete --topic streams-plaintext-input  2>/dev/null || true
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --delete --topic streams-wordcount-output 2>/dev/null || true
"$KAFKA_HOME/bin/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" \
  --delete --group streams-wordcount 2>/dev/null || true
rm -rf "$STREAMS_ROOT/streams-state"
sleep 3

"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic streams-plaintext-input \
  --partitions 1 --replication-factor 1
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic streams-wordcount-output \
  --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact

echo "--- Starting fresh WordCountDemo ---"
KAFKA_OPTS="-Dstreams.state.dir=$STREAMS_ROOT/streams-state" \
nohup "$KAFKA_HOME/bin/kafka-run-class.sh" \
  org.apache.kafka.streams.examples.wordcount.WordCountDemo \
  > "$STREAMS_ROOT/wordcount.log" 2>&1 &
echo $! > "$WORDCOUNT_PID_FILE"
echo "    PID: $(cat "$WORDCOUNT_PID_FILE")"

# Wait for WordCountDemo to join the group (first RUNNING state)
echo "--- Waiting for WordCountDemo to start up ---"
for _ in $(seq 1 30); do
  if grep -q 'stream-thread.*State transition.*RUNNING' "$STREAMS_ROOT/wordcount.log" 2>/dev/null; then
    break
  fi
  sleep 2
done
sleep 2   # extra buffer after RUNNING

# ── Step 2: produce input (untimed — analogous to the Go /data upload phase) ─
echo "--- Producing input into streams-plaintext-input ---"
"$KAFKA_HOME/bin/kafka-console-producer.sh" \
  --bootstrap-server "$BOOTSTRAP" \
  --topic streams-plaintext-input \
  < "$INPUT"
echo "    ✓ input produced"

# ── Step 3: start timer and poll lag ─────────────────────────────────────────
echo "--- Waiting for WordCountDemo to process all input (polling consumer-group lag) ---"
T_START=$(date +%s%N)   # nanoseconds

# Wait until the consumer group exists and has committed at least one offset
# (i.e. it actually started consuming) before we start the zero-streak check.
echo "    waiting for consumer group to appear..."
for _ in $(seq 1 60); do
  committed=$("$KAFKA_HOME/bin/kafka-consumer-groups.sh" \
    --bootstrap-server "$BOOTSTRAP" \
    --group streams-wordcount \
    --describe 2>/dev/null \
  | awk 'NR>1 && $5 ~ /^[0-9]/ {sum+=$5} END {print sum+0}')
  [[ "${committed:-0}" -gt 0 ]] && break
  sleep 2
done

ZERO_STREAK=0
MAX_WAIT=300  # seconds
elapsed=0
while (( ZERO_STREAK < LAG_ZERO_STREAK )); do
  sleep "$LAG_POLL_INTERVAL"
  elapsed=$((elapsed + LAG_POLL_INTERVAL))

  lag=$("$KAFKA_HOME/bin/kafka-consumer-groups.sh" \
        --bootstrap-server "$BOOTSTRAP" \
        --group streams-wordcount \
        --describe 2>/dev/null \
      | awk 'NR>1 && $6 ~ /^[0-9]/ {sum+=$6} END {print sum+0}')

  echo "    lag=${lag:-?}  (${elapsed}s elapsed)"

  if [[ "${lag:-1}" == "0" ]]; then
    ZERO_STREAK=$((ZERO_STREAK + 1))
  else
    ZERO_STREAK=0
  fi

  if (( elapsed >= MAX_WAIT )); then
    echo "WARNING: timed out after ${MAX_WAIT}s waiting for lag to reach 0; collecting partial results." >&2
    break
  fi
done

T_END=$(date +%s%N)
compute_ns=$(( T_END - T_START ))
compute_sec=$(awk "BEGIN {printf \"%.3f\", $compute_ns / 1e9}")

# ── Step 4: collect final word counts ────────────────────────────────────────
echo "--- Collecting results from streams-wordcount-output ---"
# Note: `|| true` must NOT appear mid-pipeline (lower precedence than |).
# The formatter outputs "word\t   count" (tab + optional spaces), so we strip
# leading whitespace from $2 with gsub before storing.
timeout 30 "$KAFKA_HOME/bin/kafka-console-consumer.sh" \
  --bootstrap-server "$BOOTSTRAP" \
  --topic streams-wordcount-output \
  --from-beginning \
  --formatter-property print.key=true \
  --formatter-property print.value=true \
  --formatter-property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --formatter-property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer \
  2>/dev/null \
| awk -F'\t' 'NF>=2 { gsub(/^[[:space:]]+/, "", $2); count[$1]=$2 }
              END    { for (w in count) printf "%s\t%s\n", w, count[w] }' \
| sort -k2,2nr -k1,1 \
> "$OUTPUT" || true

echo ""
echo "Results: $OUTPUT ($(wc -l < "$OUTPUT") distinct words)"
echo "TIMING compute_seconds=${compute_sec}"
echo "Compare with the Go framework: 'TIMING nodes=… compute_seconds=…'"
