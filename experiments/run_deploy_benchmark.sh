#!/usr/bin/env bash
# run_deploy_benchmark.sh — simple benchmark runner for the stats/deployment path.
#
# Reuses one host pool, then times deploy → collect → cleanup for each requested
# SSH fan-out level. Results are written as CSV so different -parallel settings
# can be compared directly.
#
# Usage:
#   experiments/run_deploy_benchmark.sh
#   experiments/run_deploy_benchmark.sh -n 100 -parallel-values "4 8 16 32" -reps 3
#   experiments/run_deploy_benchmark.sh -manifest manifest.json -hosts-file experiments/deploy-bench-hosts.txt -no-fetch

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPTS="$ROOT/scripts"
BIN_DIR="$ROOT/bin"
EXP_DIR="$ROOT/experiments"

MANIFEST="$ROOT/manifest.json"
NODES=100
PARALLELS="4 8 16 32"
REPS=3
OUT="$EXP_DIR/deploy-results.csv"
DO_FETCH=1
MASTER_HOSTS="$EXP_DIR/deploy-bench-hosts.txt"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -manifest)        MANIFEST="$2";   shift 2 ;;
    -n)               NODES="$2";      shift 2 ;;
    -parallel-values) PARALLELS="$2";  shift 2 ;;
    -reps)            REPS="$2";       shift 2 ;;
    -out)             OUT="$2";        shift 2 ;;
    -hosts-file)      MASTER_HOSTS="$2"; shift 2 ;;
    -no-fetch)        DO_FETCH=0;      shift   ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

mkdir -p "$EXP_DIR"
mkdir -p "$(dirname "$OUT")"
mkdir -p "$(dirname "$MASTER_HOSTS")"

if [[ ! -x "$BIN_DIR/slr_make_hosts" || ! -x "$BIN_DIR/slr_deploy" || ! -x "$BIN_DIR/slr_collect" || ! -x "$BIN_DIR/slr_cleanup" ]]; then
  "$SCRIPTS/build_cpp.sh"
fi

WORK_DIR="$(mktemp -d)"
RUN_HOSTS="$WORK_DIR/hosts.txt"
trap 'rm -rf "$WORK_DIR"' EXIT

if [[ "$DO_FETCH" -eq 1 ]]; then
  echo "=== Fetching up to $NODES hosts ==="
  "$BIN_DIR/slr_make_hosts" -n "$NODES" -f "$MASTER_HOSTS"
fi

if [[ ! -s "$MASTER_HOSTS" ]]; then
  echo "Error: no master hosts file at $MASTER_HOSTS (run without -no-fetch)" >&2
  exit 1
fi

AVAIL="$(grep -cve '^[[:space:]]*$' "$MASTER_HOSTS")"
if (( AVAIL < NODES )); then
  echo "Error: requested -n $NODES but only $AVAIL hosts are available in $MASTER_HOSTS" >&2
  exit 1
fi

head -n "$NODES" "$MASTER_HOSTS" > "$RUN_HOSTS"

timed_run() {
  local start_ns end_ns rc
  start_ns="$(date +%s%N)"
  set +e
  "$@"
  rc=$?
  set -e
  end_ns="$(date +%s%N)"
  TIMED_SECONDS="$(awk -v ns="$((end_ns - start_ns))" 'BEGIN { printf "%.3f", ns / 1000000000 }')"
  return "$rc"
}

run_phase() {
  local phase="$1" parallel="$2" stats_file="$3"
  case "$phase" in
    deploy)
      timed_run "$BIN_DIR/slr_deploy" -m "$MANIFEST" -h "$RUN_HOSTS" -parallel "$parallel"
      ;;
    collect)
      timed_run "$BIN_DIR/slr_collect" -m "$MANIFEST" -h "$RUN_HOSTS" -o "$stats_file" -parallel "$parallel"
      ;;
    cleanup)
      timed_run "$BIN_DIR/slr_cleanup" -m "$MANIFEST" -h "$RUN_HOSTS" -parallel "$parallel"
      ;;
    *)
      echo "unknown phase: $phase" >&2
      return 1
      ;;
  esac
}

echo "parallel,run,deploy_seconds,collect_seconds,cleanup_seconds,total_seconds,status" > "$OUT"

for parallel in $PARALLELS; do
  echo ""
  echo "=== parallel=$parallel : $REPS run(s) ==="
  for ((run = 1; run <= REPS; run++)); do
    stats_file="$WORK_DIR/stats-p${parallel}-r${run}.txt"
    deploy_seconds=""
    collect_seconds=""
    cleanup_seconds=""
    status="ok"

    echo "  run $run/$REPS …"

    if run_phase deploy "$parallel" "$stats_file"; then
      deploy_seconds="$TIMED_SECONDS"
    else
      deploy_seconds="$TIMED_SECONDS"
      status="deploy_failed"
    fi

    if [[ "$status" == "ok" ]]; then
      if run_phase collect "$parallel" "$stats_file"; then
        collect_seconds="$TIMED_SECONDS"
      else
        collect_seconds="$TIMED_SECONDS"
        status="collect_failed"
      fi
    fi

    if run_phase cleanup "$parallel" "$stats_file"; then
      cleanup_seconds="$TIMED_SECONDS"
    else
      cleanup_seconds="$TIMED_SECONDS"
      if [[ "$status" == "ok" ]]; then
        status="cleanup_failed"
      else
        status="${status}+cleanup_failed"
      fi
    fi

    total_seconds="$(awk \
      -v a="${deploy_seconds:-0}" \
      -v b="${collect_seconds:-0}" \
      -v c="${cleanup_seconds:-0}" \
      'BEGIN { printf "%.3f", a + b + c }')"

    echo "    deploy=${deploy_seconds:-n/a}s collect=${collect_seconds:-n/a}s cleanup=${cleanup_seconds:-n/a}s status=$status"
    echo "$parallel,$run,$deploy_seconds,$collect_seconds,$cleanup_seconds,$total_seconds,$status" >> "$OUT"
  done
done

echo ""
echo "=== Results written to $OUT ==="
cat "$OUT"
