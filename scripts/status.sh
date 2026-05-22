#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

manifest_file=$DEFAULT_MANIFEST_FILE
report_only=0

while (( $# > 0 )); do
    case "$1" in
        --manifest)
            manifest_file=$2
            shift 2
            ;;
        --report-only|--no-fail)
            report_only=1
            shift
            ;;
        --help)
            cat <<'EOF'
Usage: status.sh [--manifest FILE] [--report-only]

Check whether the distributed load servers are still running and listening.

By default, exit non-zero when any host is not healthy.
Use --report-only (alias: --no-fail) to always exit zero after printing status.
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
REMOTE_DIRNAME=${REMOTE_DIRNAME:-$DEFAULT_REMOTE_DIRNAME}

failures=0

while IFS= read -r host; do
    result=$(ssh "${SSH_OPTIONS[@]}" "$host" bash -s -- "$REMOTE_DIRNAME" "$PORT_HEX" "$host" <<'REMOTE'
set -euo pipefail

remote_dirname=$1
port_hex=$2
host_name=$3
app_dir="$HOME/$remote_dirname"
server_bin="$app_dir/bin/load_server"
pid_file="$app_dir/pids/$host_name.pid"

find_running_pid() {
    ps -u "$USER" -o pid= -o args= | awk -v server_bin="$server_bin" '
        index($0, server_bin) {
            print $1
            exit
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

pid=
if [[ -f "$pid_file" ]]; then
    pid=$(<"$pid_file")
fi

if [[ -z "$pid" ]]; then
    pid=$(find_running_pid || true)
fi

if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && port_is_listening; then
    echo "running:$pid"
    exit 0
fi

if port_is_listening; then
    echo "listening-without-pid"
    exit 2
fi

echo "stopped"
exit 1
REMOTE
) || rc=$?
    rc=${rc:-0}
    printf '%s\t%s\n' "$host" "${result:-ssh-failed}"
    if (( rc != 0 )); then
        (( failures += 1 ))
    fi
    unset rc
done < "$HOSTS_FILE"

if (( report_only )); then
    exit 0
fi

(( failures == 0 ))