#!/usr/bin/env bash
# test_distributed.sh — local end-to-end distributed test of the C++ MapReduce
# pipeline. Starts N slr_worker processes on localhost ports, distributes an
# input file round-robin, drives map → shuffle → reduce → result over real
# HTTP/TCP, merges the outputs, and diffs them against a reference word count
# computed with awk (same tokenization: lowercase + split on non-alphanumerics).
#
# Usage:
#   scripts/test_distributed.sh [-n 3] [-input test_input.txt] [-base-port 19100]

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin"
INPUT="$ROOT/test_input.txt"
N=3
BASE_PORT=19100

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n)         N="$2";         shift 2 ;;
    -input)     INPUT="$2";     shift 2 ;;
    -base-port) BASE_PORT="$2"; shift 2 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

[[ -f "$INPUT" ]] || { echo "input file not found: $INPUT" >&2; exit 1; }
[[ -x "$BIN/slr_worker" ]] || "$ROOT/scripts/build_cpp.sh"

WORK="$(mktemp -d)"
declare -a PIDS PEERS
cleanup() {
  kill "${PIDS[@]}" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

# ── 1. Start N workers ───────────────────────────────────────────────────────
echo "=== Starting $N workers ==="
for i in $(seq 0 $((N-1))); do
  PORT=$((BASE_PORT + i))
  "$BIN/slr_worker" -port "$PORT" -job wordcount >"$WORK/worker-$i.log" 2>&1 &
  PIDS+=($!)
  PEERS+=("127.0.0.1:$PORT")
done

# ── 2. Health checks ─────────────────────────────────────────────────────────
for peer in "${PEERS[@]}"; do
  ok=0
  for _ in $(seq 1 50); do
    if curl -fsS --max-time 2 "http://$peer/health" >/dev/null 2>&1; then ok=1; break; fi
    sleep 0.1
  done
  [[ $ok -eq 1 ]] || fail "worker $peer never became healthy"
  echo "  $peer healthy"
done

# ── 3. Distribute input round-robin by line ──────────────────────────────────
echo "=== Distributing input ($(wc -l < "$INPUT") lines across $N workers) ==="
for i in $(seq 0 $((N-1))); do
  awk -v n="$N" -v id="$i" 'NR % n == (id + 1) % n' "$INPUT" > "$WORK/chunk-$i.txt"
  curl -fsS --max-time 30 -X POST --data-binary "@$WORK/chunk-$i.txt" \
    "http://${PEERS[$i]}/data" >/dev/null
  echo "  chunk $i ($(wc -l < "$WORK/chunk-$i.txt") lines) -> ${PEERS[$i]}"
done

# ── 4. Map phase ─────────────────────────────────────────────────────────────
echo "=== Map phase ==="
PEER_BODY="$(printf '%s\n' "${PEERS[@]}")"
for i in $(seq 0 $((N-1))); do
  printf '%s\n%s\n' "$i" "$PEER_BODY" | \
    curl -fsS --max-time 60 -X POST --data-binary @- "http://${PEERS[$i]}/map" >/dev/null
  echo "  /map -> ${PEERS[$i]}"
done

# ── 5. Reduce phase (workers shuffle over TCP via GET /intermediate) ─────────
echo "=== Reduce phase (peer-to-peer shuffle) ==="
for peer in "${PEERS[@]}"; do
  printf '%s\n' "$PEER_BODY" | \
    curl -fsS --max-time 60 -X POST --data-binary @- "http://$peer/reduce" >/dev/null
  echo "  /reduce -> $peer"
done

# ── 6. Verify partitioning: no key may appear on two workers ─────────────────
echo "=== Collecting results ==="
: > "$WORK/all_results.tsv"
for i in $(seq 0 $((N-1))); do
  curl -fsS --max-time 30 "http://${PEERS[$i]}/result" > "$WORK/result-$i.tsv"
  echo "  ${PEERS[$i]}: $(wc -l < "$WORK/result-$i.tsv") keys"
  cat "$WORK/result-$i.tsv" >> "$WORK/all_results.tsv"
done

DUPES="$(cut -f1 "$WORK/all_results.tsv" | sort | uniq -d)"
[[ -z "$DUPES" ]] || fail "keys present on multiple workers (broken partitioning): $DUPES"
echo "  partitioning OK: every key owned by exactly one worker"

# ── 7. Merge and diff against an awk reference word count ────────────────────
sort -t$'\t' -k2,2rn -k1,1 "$WORK/all_results.tsv" > "$WORK/merged.tsv"

awk '{
  line = tolower($0)
  gsub(/[^a-z0-9]+/, " ", line)
  n = split(line, words, " ")
  for (i = 1; i <= n; i++) if (words[i] != "") count[words[i]]++
} END {
  for (w in count) printf "%s\t%d\n", w, count[w]
}' "$INPUT" | sort -t$'\t' -k2,2rn -k1,1 > "$WORK/reference.tsv"

echo ""
echo "=== Top 10 (system output) ==="
head -10 "$WORK/merged.tsv"

echo ""
if diff -u "$WORK/reference.tsv" "$WORK/merged.tsv" > "$WORK/diff.txt"; then
  echo "=== PASS: distributed output matches reference exactly ($(wc -l < "$WORK/merged.tsv") distinct words) ==="
else
  echo "=== FAIL: output differs from reference ===" >&2
  cat "$WORK/diff.txt" >&2
  exit 1
fi
