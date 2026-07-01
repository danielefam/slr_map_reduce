#!/usr/bin/env bash

# Clean deployed MapReduce workers on a host set.
#
# Usage:
#   ./clean_mapreduce.sh [-hosts hosts.txt] [-parallel 16] [-port 9090]

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS="$ROOT/scripts"
HOSTS="$ROOT/hosts.txt"
PARALLEL=16
PORT=9090

while [[ $# -gt 0 ]]; do
  case "$1" in
  -hosts)
    HOSTS="$2"
    shift 2
    ;;
  -parallel)
    PARALLEL="$2"
    shift 2
    ;;
  -port)
    PORT="$2"
    shift 2
    ;;
  *)
    echo "Unknown flag: $1" >&2
    exit 1
    ;;
  esac
done

if [[ ! -f "$HOSTS" ]]; then
  echo "Hosts file does not exist: $HOSTS" >&2
  exit 1
fi

(cd "$SCRIPTS" && go run ./mapreduce -cleanup-only -hosts "$HOSTS" -parallel "$PARALLEL" -port "$PORT")
