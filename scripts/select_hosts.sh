#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

host_count=$DEFAULT_HOST_COUNT
output_file=$DEFAULT_HOSTS_FILE
api_url=$DEFAULT_API_BASE_URL

while (( $# > 0 )); do
    case "$1" in
        --count)
            host_count=$2
            shift 2
            ;;
        --output)
            output_file=$2
            shift 2
            ;;
        --url)
            api_url=$2
            shift 2
            ;;
        --help)
            cat <<'EOF'
Usage: select_hosts.sh [--count N] [--output FILE] [--url URL]

Query the Telecom Paris availability endpoint, filter alive hosts,
append the canonical .enst.fr suffix, and write a deterministic host list.
EOF
            exit 0
            ;;
        *)
            printf 'Unknown argument: %s\n' "$1" >&2
            exit 1
            ;;
    esac
done

require_commands curl jq sed sort head mktemp wc
ensure_run_dir

output_file=$(normalize_path "$output_file")
tmp_file=$(mktemp)
trap 'rm -f "$tmp_file"' EXIT

curl -fsSL "$api_url" \
    | jq -r '.data[] | select(.[1] == true) | .[0]' \
    | sed -E '/^[[:space:]]*$/d; s/[[:space:]]+$//; /\.enst\.fr$/! s/$/.enst.fr/' \
    | sort -u \
    > "$tmp_file"

available_count=$(wc -l < "$tmp_file")
if (( available_count < host_count )); then
    printf 'Only %d available hosts found, but %d required\n' "$available_count" "$host_count" >&2
    exit 1
fi

head -n "$host_count" "$tmp_file" > "$output_file"

printf 'Selected %d hosts into %s\n' "$host_count" "$output_file"