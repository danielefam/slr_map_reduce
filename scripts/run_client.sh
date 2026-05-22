#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

manifest_file=$DEFAULT_MANIFEST_FILE
output_file=$DEFAULT_RESULTS_FILE
client_args=()

while (( $# > 0 )); do
    case "$1" in
        --manifest)
            manifest_file=$2
            shift 2
            ;;
        --output)
            output_file=$2
            shift 2
            ;;
        --help)
            cat <<'EOF'
Usage: run_client.sh [--manifest FILE] [--output FILE] [extra load_client args...]

Run the local aggregator client using the stored host list and port.
Append the per-host results and final average to run/client_results.log by default.
EOF
            exit 0
            ;;
        *)
            client_args+=("$1")
            shift
            ;;
    esac
done

require_commands tee date mkdir
ensure_run_dir

manifest_file=$(normalize_path "$manifest_file")
output_file=$(normalize_path "$output_file")
output_dir=${output_file%/*}
mkdir -p "$output_dir"

load_manifest "$manifest_file"

printf '=== %s ===\n' "$(date -Is)" | tee -a "$output_file"

set +e
"$REPO_ROOT/bin/load_client" --hosts "$HOSTS_FILE" --port "$PORT" "${client_args[@]}" | tee -a "$output_file"
pipeline_status=("${PIPESTATUS[@]}")
set -e

client_status=${pipeline_status[0]:-1}
tee_status=${pipeline_status[1]:-1}

printf '\n' >> "$output_file"

if (( tee_status != 0 )); then
    exit "$tee_status"
fi

printf 'Saved results to %s\n' "$output_file" >&2
exit "$client_status"