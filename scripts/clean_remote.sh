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
Usage: clean_remote.sh [--manifest FILE]

Stop the distributed load servers for the current manifest and remove the
current remote deployment directory, pid files, and logs.
EOF
            exit 0
            ;;
        *)
            printf 'Unknown argument: %s\n' "$1" >&2
            exit 1
            ;;
    esac
done

require_commands ssh awk ps rm
manifest_file=$(normalize_path "$manifest_file")
load_manifest "$manifest_file"

failures=0

while IFS= read -r host; do
    result=$(ssh "${SSH_OPTIONS[@]}" "$host" bash -s -- "$REMOTE_DIRNAME" "$PORT" <<'REMOTE'
set -euo pipefail

remote_dirname=$1
port=$2
app_dir="$HOME/$remote_dirname"
server_bin="$app_dir/bin/load_server"
had_state=0

find_bundle_pids() {
    ps -u "$USER" -o pid= -o args= | awk -v server_bin="$server_bin" '
        index($0, server_bin) {
            print $1
        }
    '
}

find_port_pids() {
    ps -u "$USER" -o pid= -o args= | awk -v server_bin="$server_bin" -v port="$port" '
        index($0, server_bin) && index($0, "--port " port) {
            print $1
        }
    '
}

kill_matching_pids() {
    local pid
    while IFS= read -r pid; do
        [[ -n "$pid" ]] || continue
        had_state=1
        kill "$pid" 2>/dev/null || true
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
}

if [[ -d "$app_dir" ]]; then
    had_state=1
fi

kill_matching_pids < <(find_port_pids || true)
kill_matching_pids < <(find_bundle_pids || true)

rm -rf "$app_dir"

remaining_pid=$(find_bundle_pids | head -n 1 || true)
if [[ -n "$remaining_pid" ]]; then
    echo "still-running:$remaining_pid"
    exit 1
fi

if [[ -d "$app_dir" ]]; then
    echo "directory-remains"
    exit 2
fi

if (( had_state )); then
    echo "cleaned"
else
    echo "already-clean"
fi
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