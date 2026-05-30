#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

manifest_file=$DEFAULT_MR_MANIFEST_FILE
job_manifest=$DEFAULT_MR_JOB_FILE
input_file=
output_file=$DEFAULT_MR_RESULTS_FILE
chunk_lines=200
timeout_ms=
job_id=
reducers=

while (( $# > 0 )); do
    case "$1" in
        --manifest)
            manifest_file=$2
            shift 2
            ;;
        --job-manifest)
            job_manifest=$2
            shift 2
            ;;
        --input)
            input_file=$2
            shift 2
            ;;
        --output)
            output_file=$2
            shift 2
            ;;
        --chunk-lines)
            chunk_lines=$2
            shift 2
            ;;
        --timeout-ms)
            timeout_ms=$2
            shift 2
            ;;
        --reducers)
            reducers=$2
            shift 2
            ;;
        --job-id)
            job_id=$2
            shift 2
            ;;
        --help)
            cat <<'EOF'
Usage: run_mapreduce.sh --input FILE [--manifest FILE] [--job-manifest FILE]
                        [--output FILE] [--chunk-lines N] [--reducers N]
                        [--timeout-ms MS] [--job-id ID]

Run the local MapReduce coordinator using the stored worker host list and port.
EOF
            exit 0
            ;;
        *)
            printf 'Unknown argument: %s\n' "$1" >&2
            exit 2
            ;;
    esac
done

if [[ -z "$input_file" ]]; then
    printf '--input is required\n' >&2
    exit 2
fi

if ! require_commands mkdir; then
    exit 5
fi
ensure_run_dir

manifest_file=$(normalize_path "$manifest_file")
job_manifest=$(normalize_path "$job_manifest")
input_file=$(normalize_path "$input_file")
output_file=$(normalize_path "$output_file")
output_dir=${output_file%/*}
mkdir -p "$output_dir"

if ! load_manifest "$manifest_file"; then
    printf 'Failed to load MapReduce manifest: %s\n' "$manifest_file" >&2
    exit 3
fi

command=(
    "$REPO_ROOT/bin/mr_coordinator"
    --hosts "$HOSTS_FILE"
    --port "$PORT"
    --input "$input_file"
    --output "$output_file"
    --chunk-lines "$chunk_lines"
    --job-manifest "$job_manifest"
)

if [[ -n "$timeout_ms" ]]; then
    command+=(--timeout-ms "$timeout_ms")
fi

if [[ -n "$reducers" ]]; then
    command+=(--reducers "$reducers")
fi

if [[ -n "$job_id" ]]; then
    command+=(--job-id "$job_id")
fi

set +e
"${command[@]}"
rc=$?
set -e

if (( rc == 0 )); then
    printf 'Saved MapReduce output to %s\n' "$output_file" >&2
    exit 0
fi

case "$rc" in
    2)
        reason="usage/config error"
        ;;
    3)
        reason="hosts/manifest error"
        ;;
    4)
        reason="input read/split error"
        ;;
    5)
        reason="job setup error"
        ;;
    6)
        reason="network/remote I/O error"
        ;;
    7)
        reason="protocol/response error"
        ;;
    8)
        reason="output write error"
        ;;
    9)
        reason="internal error"
        ;;
    *)
        reason="unknown error"
        ;;
esac

printf 'MapReduce coordinator failed (exit=%s: %s)\n' "$rc" "$reason" >&2
exit "$rc"