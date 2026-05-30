#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
RUN_DIR="$REPO_ROOT/run"

DEFAULT_HOSTS_FILE="$RUN_DIR/hosts.txt"
DEFAULT_MANIFEST_FILE="$RUN_DIR/manifest.env"
DEFAULT_RESULTS_FILE="$RUN_DIR/client_results.log"
DEFAULT_MR_MANIFEST_FILE="$RUN_DIR/mr_manifest.env"
DEFAULT_MR_JOB_FILE="$RUN_DIR/mr_job.env"
DEFAULT_MR_RESULTS_FILE="$RUN_DIR/mr_wordcount.txt"
DEFAULT_API_BASE_URL="https://tp.telecom-paris.fr/ajax.php"
DEFAULT_REMOTE_DIRNAME="slr_map_reduce_bundle"
DEFAULT_MR_REMOTE_DIRNAME="slr_map_reduce_mr_bundle"
DEFAULT_MR_SCRATCH_ROOT="/tmp/slr_map_reduce"
DEFAULT_PORT_START=20000
DEFAULT_PORT_END=20099
DEFAULT_HOST_COUNT=100

SSH_OPTIONS=(
    -o BatchMode=yes
    -o ConnectTimeout=5
    -o ServerAliveInterval=15
    -o ServerAliveCountMax=2
    -o StrictHostKeyChecking=accept-new
)

ensure_run_dir() {
    mkdir -p "$RUN_DIR"
}

normalize_path() {
    local path=$1
    if [[ "$path" = /* ]]; then
        printf '%s\n' "$path"
    else
        printf '%s\n' "$REPO_ROOT/$path"
    fi
}

timestamp_ms() {
    date +%s%3N
}

require_commands() {
    local missing=()
    local command_name
    for command_name in "$@"; do
        if ! command -v "$command_name" >/dev/null 2>&1; then
            missing+=("$command_name")
        fi
    done

    if (( ${#missing[@]} > 0 )); then
        printf 'Missing required command(s): %s\n' "${missing[*]}" >&2
        return 1
    fi
}

list_hosts() {
    local hosts_file=${1:-$DEFAULT_HOSTS_FILE}
    grep -Ev '^[[:space:]]*(#|$)' "$hosts_file"
}

write_manifest() {
    local manifest_file=$1
    shift

    if (( $# % 2 != 0 )); then
        echo "write_manifest expects key/value pairs" >&2
        return 1
    fi

    : > "$manifest_file"
    while (( $# > 0 )); do
        local key=$1
        local value=$2
        printf '%s=%q\n' "$key" "$value" >> "$manifest_file"
        shift 2
    done
}

load_manifest() {
    local manifest_file=${1:-$DEFAULT_MANIFEST_FILE}
    if [[ ! -f "$manifest_file" ]]; then
        printf 'Manifest file not found: %s\n' "$manifest_file" >&2
        return 1
    fi

    # shellcheck disable=SC1090
    source "$manifest_file"
}