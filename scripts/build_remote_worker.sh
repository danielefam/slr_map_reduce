#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIR="$ROOT/cpp/src"
BIN_DIR="$ROOT/bin"
HOST=""
OUT="$BIN_DIR/slr_worker_remote"

usage() {
  echo "Usage: $0 -host <host> [-o bin/slr_worker_remote]" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -host) HOST="$2"; shift 2 ;;
    -o) OUT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown flag: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$HOST" ]]; then
  echo "missing -host <host>" >&2
  usage
  exit 1
fi

mkdir -p "$BIN_DIR" "$(dirname "$OUT")"

SSH_OPTS=(
  -o StrictHostKeyChecking=no
  -o BatchMode=yes
  -o ConnectTimeout=10
  -o ServerAliveInterval=5
  -o ServerAliveCountMax=3
  -o ControlMaster=auto
  -o ControlPersist=60s
  -o ControlPath=/tmp/slr-ssh-%C
)

TAG="${USER:-user}-$$-$(date +%s)"
REMOTE_DIR="/tmp/slr-worker-build-$TAG"
REMOTE_BIN="$REMOTE_DIR/slr_worker_remote"

cleanup() {
  ssh "${SSH_OPTS[@]}" "$HOST" "rm -rf '$REMOTE_DIR'" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Building remote-compatible worker on $HOST..."
ssh "${SSH_OPTS[@]}" "$HOST" "rm -rf '$REMOTE_DIR' && mkdir -p '$REMOTE_DIR'"
scp "${SSH_OPTS[@]}" \
  "$SRC_DIR/common.cpp" "$SRC_DIR/common.hpp" \
  "$SRC_DIR/job.cpp" "$SRC_DIR/job.hpp" \
  "$SRC_DIR/slr_worker.cpp" \
  "$HOST:$REMOTE_DIR/" >/dev/null

ssh "${SSH_OPTS[@]}" "$HOST" \
  "cd '$REMOTE_DIR' && g++ -std=c++20 -O2 -Wall -Wextra -pthread -static-libstdc++ -static-libgcc common.cpp job.cpp slr_worker.cpp -o '$REMOTE_BIN'"

scp "${SSH_OPTS[@]}" "$HOST:$REMOTE_BIN" "$OUT" >/dev/null
chmod +x "$OUT"

echo "Built remote-compatible worker into $OUT"
