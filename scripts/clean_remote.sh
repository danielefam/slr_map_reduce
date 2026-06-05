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

require_commands ssh awk ps rm printf mktemp
manifest_file=$(normalize_path "$manifest_file")
load_manifest "$manifest_file"

base_remote_dirname=${BASE_REMOTE_DIRNAME:-$REMOTE_DIRNAME}
hosts_registry_file="$RUN_DIR/legacy_deployed_hosts.txt"
hosts_tmp=$(mktemp)
trap 'rm -f "$hosts_tmp"' EXIT

{
    [[ -f "$HOSTS_FILE" ]] && list_hosts "$HOSTS_FILE" || true
    [[ -f "$hosts_registry_file" ]] && list_hosts "$hosts_registry_file" || true
} | awk 'NF > 0 && !seen[$0]++ { print }' > "$hosts_tmp"

if [[ ! -s "$hosts_tmp" ]]; then
    printf 'No hosts available for legacy cleanup (manifest/registry empty).\n' >&2
    exit 1
fi

failures=0
total_hosts=0
ok_hosts=0
already_clean_hosts=0
ssh_failed_hosts=0
still_running_hosts=0
state_remains_hosts=0
unknown_error_hosts=0

printf '%-28s %-3s %-14s %s\n' "HOST" "RC" "STATUS" "DETAILS"
printf '%-28s %-3s %-14s %s\n' "----------------------------" "---" "--------------" "------------------------------"

while IFS= read -r host; do
    (( total_hosts += 1 ))
    result=$(ssh "${SSH_OPTIONS[@]}" "$host" bash -s -- "$REMOTE_DIRNAME" "$base_remote_dirname" "$PORT" <<'REMOTE'
set -euo pipefail

remote_dirname=$1
base_remote_dirname=$2
port=$3
app_dir="$HOME/$remote_dirname"
server_bin="$app_dir/bin/load_server"
bundle_prefix="$HOME/$base_remote_dirname-"
had_state=0

find_bundle_pids() {
    ps -u "$USER" -o pid= -o args= | awk -v server_bin="$server_bin" -v bundle_prefix="$bundle_prefix" '
        {
            pid=$1
            $1=""
            sub(/^[[:space:]]+/, "", $0)
            argc=split($0, argv, /[[:space:]]+/)
            if (argc < 1) {
                next
            }
            if (argv[1] == server_bin || (index(argv[1], bundle_prefix) == 1 && argv[1] ~ /\/bin\/load_server$/)) {
                print pid
            }
        }
    '
}

find_port_pids() {
    ps -u "$USER" -o pid= -o args= | awk -v server_bin="$server_bin" -v bundle_prefix="$bundle_prefix" -v port="$port" '
        {
            pid=$1
            $1=""
            sub(/^[[:space:]]+/, "", $0)
            argc=split($0, argv, /[[:space:]]+/)
            if (argc < 1 || !(argv[1] == server_bin || (index(argv[1], bundle_prefix) == 1 && argv[1] ~ /\/bin\/load_server$/))) {
                next
            }
            for (i = 2; i < argc; i++) {
                if (argv[i] == "--port" && argv[i + 1] == port) {
                    print pid
                    break
                }
            }
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
if compgen -G "${bundle_prefix}*" > /dev/null; then
    had_state=1
fi

kill_matching_pids < <(find_port_pids || true)
kill_matching_pids < <(find_bundle_pids || true)

rm -rf "$app_dir" "${bundle_prefix}"*

remaining_pid=$(find_bundle_pids | head -n 1 || true)
if [[ -n "$remaining_pid" ]]; then
    echo "still-running:$remaining_pid"
    exit 1
fi

if [[ -d "$app_dir" ]] || compgen -G "${bundle_prefix}*" > /dev/null; then
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

    status="ok"
    details=${result:-ssh-failed}

    if (( rc == 0 )); then
        if [[ "$details" == "already-clean" ]]; then
            status="already-clean"
            (( already_clean_hosts += 1 ))
        else
            status="cleaned"
            (( ok_hosts += 1 ))
        fi
    else
        case "$details" in
            ssh-failed)
                status="ssh-failed"
                (( ssh_failed_hosts += 1 ))
                ;;
            still-running:*)
                status="still-running"
                (( still_running_hosts += 1 ))
                ;;
            directory-remains)
                status="state-remains"
                (( state_remains_hosts += 1 ))
                ;;
            *)
                status="error"
                (( unknown_error_hosts += 1 ))
                ;;
        esac
    fi

    printf '%-28s %-3s %-14s %s\n' "$host" "$rc" "$status" "$details"
    if (( rc != 0 )); then
        (( failures += 1 ))
    fi
    unset rc
done < "$hosts_tmp"

printf '\n'
printf 'Summary: total=%d cleaned=%d already-clean=%d failed=%d\n' \
    "$total_hosts" "$ok_hosts" "$already_clean_hosts" "$failures"
printf 'Failure details: ssh-failed=%d still-running=%d state-remains=%d other=%d\n' \
    "$ssh_failed_hosts" "$still_running_hosts" "$state_remains_hosts" "$unknown_error_hosts"

if (( failures > 0 )); then
    printf 'Hint: investigate hosts with rc!=0; common causes are stale processes (still-running) or unreachable SSH.\n' >&2
fi

(( failures == 0 ))