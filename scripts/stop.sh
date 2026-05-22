#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

manifest_file=$DEFAULT_MANIFEST_FILE

while (( $# > 0 )); do
    case "$1" in
        --manifest)
            manifest_file=$2
            shift 2
            ;;
        --help)
            cat <<'EOF'
Usage: stop.sh [--manifest FILE]

Stop the distributed load servers using the stored host list and manifest.
EOF
            exit 0
            ;;
        *)
            printf 'Unknown argument: %s\n' "$1" >&2
            exit 1
            ;;
    esac
done

require_commands ssh awk ps
manifest_file=$(normalize_path "$manifest_file")
load_manifest "$manifest_file"

failures=0

while IFS= read -r host; do
    result=$(ssh "${SSH_OPTIONS[@]}" "$host" bash -s -- "$REMOTE_DIRNAME" "$PORT_HEX" "$PORT" "$host" <<'REMOTE'
set -euo pipefail

remote_dirname=$1
port_hex=$2
port=$3
host_name=$4
app_dir="$HOME/$remote_dirname"
server_bin="$app_dir/bin/load_server"
pid_file="$app_dir/pids/$host_name.pid"

find_running_pids() {
    ps -u "$USER" -o pid= -o args= | awk -v server_bin="$server_bin" -v port="$port" '
        index($0, server_bin) && index($0, "--port " port) {
            print $1
        }
    '
}

port_is_listening() {
    awk -v port_hex="$port_hex" 'NR > 1 && $4 == "0A" {
        split($2, addr, ":")
        if (toupper(addr[2]) == port_hex) {
            found = 1
        }
    } END {
        exit found ? 0 : 1
    }' /proc/net/tcp /proc/net/tcp6
}

if [[ -f "$pid_file" ]]; then
    pid=$(<"$pid_file")
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
    fi
    rm -f "$pid_file"
fi

while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    kill "$pid" 2>/dev/null || true
done < <(find_running_pids || true)

if port_is_listening; then
    echo "still-listening"
    exit 1
fi

echo "stopped"
REMOTE
) || rc=$?
    rc=${rc:-0}
    printf '%s\t%s\n' "$host" "${result:-ssh-failed}"
    if (( rc != 0 )); then
        (( failures += 1 ))
    fi
    unset rc
done < "$HOSTS_FILE"

(( failures == 0 ))