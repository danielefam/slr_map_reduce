#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

legacy_manifest=""
mr_manifest=""

while (( $# > 0 )); do
    case "$1" in
        --manifest)
            legacy_manifest=$2
            shift 2
            ;;
        --mr-manifest)
            mr_manifest=$2
            shift 2
            ;;
        --help)
            cat <<'EOF'
Usage: clean_all_remote.sh [--manifest FILE] [--mr-manifest FILE]

Run both remote cleanup flows:
1) legacy load_server cleanup
2) MapReduce worker cleanup

The command exits with code 0 only if both cleanups succeed.
EOF
            exit 0
            ;;
        *)
            printf 'Unknown argument: %s\n' "$1" >&2
            exit 1
            ;;
    esac
done

legacy_cmd=("$SCRIPT_DIR/clean_remote.sh")
mr_cmd=("$SCRIPT_DIR/clean_mapreduce.sh")

if [[ -n "$legacy_manifest" ]]; then
    legacy_cmd+=(--manifest "$legacy_manifest")
fi
if [[ -n "$mr_manifest" ]]; then
    mr_cmd+=(--manifest "$mr_manifest")
fi

printf '=== Remote cleanup: legacy load_server ===\n'
legacy_rc=0
bash "${legacy_cmd[@]}" || legacy_rc=$?
printf '\n'

printf '=== Remote cleanup: MapReduce workers ===\n'
mr_rc=0
bash "${mr_cmd[@]}" || mr_rc=$?
printf '\n'

printf '=== Combined summary ===\n'
printf 'legacy_rc=%d mapreduce_rc=%d\n' "$legacy_rc" "$mr_rc"

if (( legacy_rc == 0 && mr_rc == 0 )); then
    printf 'Overall status: success\n'
    exit 0
fi

printf 'Overall status: failure\n' >&2
printf 'Hint: rerun single flows to debug details:\n' >&2
printf '  bash scripts/clean_remote.sh --manifest <legacy-manifest>\n' >&2
printf '  bash scripts/clean_mapreduce.sh --manifest <mr-manifest>\n' >&2
exit 1
