#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

hosts_file=$DEFAULT_HOSTS_FILE
manifest_file=$DEFAULT_MANIFEST_FILE
start_port=$DEFAULT_PORT_START
end_port=$DEFAULT_PORT_END
required_host_count=0
minimum_host_count=0
selected_hosts_output=
allow_partial=0

while (( $# > 0 )); do
    case "$1" in
        --hosts)
            hosts_file=$2
            shift 2
            ;;
        --output)
            manifest_file=$2
            shift 2
            ;;
        --required-host-count)
            required_host_count=$2
            shift 2
            ;;
        --minimum-host-count)
            minimum_host_count=$2
            shift 2
            ;;
        --selected-hosts-output)
            selected_hosts_output=$2
            shift 2
            ;;
        --allow-partial)
            allow_partial=1
            shift
            ;;
        --start-port)
            start_port=$2
            shift 2
            ;;
        --end-port)
            end_port=$2
            shift 2
            ;;
        --help)
            cat <<'EOF'
Usage: select_port.sh [--hosts FILE] [--output FILE] [--required-host-count N]
                      [--minimum-host-count N] [--selected-hosts-output FILE]
                      [--start-port N] [--end-port N] [--allow-partial]

Probe the selected hosts over SSH, collect their listening TCP ports,
and persist the first common free port inside the manifest file.
EOF
            exit 0
            ;;
        *)
            printf 'Unknown argument: %s\n' "$1" >&2
            exit 1
            ;;
    esac
done

require_commands ssh awk cat cp sort grep mktemp wc
ensure_run_dir

hosts_file=$(normalize_path "$hosts_file")
manifest_file=$(normalize_path "$manifest_file")
if [[ -n "$selected_hosts_output" ]]; then
    selected_hosts_output=$(normalize_path "$selected_hosts_output")
else
    selected_hosts_output=$hosts_file
fi

if (( start_port > end_port )); then
    printf 'Invalid port range: %s-%s\n' "$start_port" "$end_port" >&2
    exit 1
fi

mapfile -t hosts < <(list_hosts "$hosts_file")
if (( ${#hosts[@]} == 0 )); then
    printf 'No hosts found in %s\n' "$hosts_file" >&2
    exit 1
fi

if (( required_host_count == 0 )); then
    required_host_count=${#hosts[@]}
fi

if (( required_host_count < 1 )); then
    printf 'Invalid required host count: %s\n' "$required_host_count" >&2
    exit 1
fi

if (( allow_partial )); then
    if (( minimum_host_count == 0 )); then
        minimum_host_count=1
    fi
else
    minimum_host_count=$required_host_count
fi

if (( minimum_host_count < 1 || minimum_host_count > required_host_count )); then
    printf 'Invalid minimum host count: %s\n' "$minimum_host_count" >&2
    exit 1
fi

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

selected_hosts_tmp="$tmp_dir/selected-hosts.txt"
in_use_union_file="$tmp_dir/in_use_union.txt"
reachable_count=0

: > "$selected_hosts_tmp"
: > "$in_use_union_file"

for host in "${hosts[@]}"; do
    host_ports_file="$tmp_dir/$host.ports"
    if ! ssh "${SSH_OPTIONS[@]}" "$host" \
        "awk 'NR > 1 && \$4 == \"0A\" { split(\$2, addr, \":\"); print toupper(addr[2]); }' /proc/net/tcp /proc/net/tcp6 2>/dev/null | sort -u" \
        > "$host_ports_file"; then
        printf 'Skipping unreachable host %s during port probe\n' "$host" >&2
        continue
    fi

    cat "$host_ports_file" >> "$in_use_union_file"
    printf '%s\n' "$host" >> "$selected_hosts_tmp"
    (( reachable_count += 1 ))

    if (( ! allow_partial && reachable_count >= required_host_count )); then
        break
    fi
done

if (( reachable_count < minimum_host_count )); then
    if (( allow_partial )); then
        printf 'Only %d reachable hosts found, but %d minimum required\n' "$reachable_count" "$minimum_host_count" >&2
    else
        printf 'Only %d reachable hosts found, but %d required\n' "$reachable_count" "$required_host_count" >&2
    fi
    exit 1
fi

sort -u "$in_use_union_file" -o "$in_use_union_file"
cp "$selected_hosts_tmp" "$selected_hosts_output"

chosen_port=
chosen_port_hex=
for (( port = start_port; port <= end_port; ++port )); do
    port_hex=$(printf '%04X' "$port")
    if ! grep -qx "$port_hex" "$in_use_union_file"; then
        chosen_port=$port
        chosen_port_hex=$port_hex
        break
    fi
done

if [[ -z "$chosen_port" ]]; then
    printf 'No common free port found in range %s-%s\n' "$start_port" "$end_port" >&2
    exit 1
fi

write_manifest "$manifest_file" \
    HOSTS_FILE "$selected_hosts_output" \
    HOST_COUNT "$reachable_count" \
    PORT "$chosen_port" \
    PORT_HEX "$chosen_port_hex" \
    PORT_RANGE_START "$start_port" \
    PORT_RANGE_END "$end_port"

printf 'Selected %d reachable hosts, common port %s, and wrote %s\n' "$reachable_count" "$chosen_port" "$manifest_file"