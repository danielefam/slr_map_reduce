#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

manifest_file=$DEFAULT_MANIFEST_FILE
strict_mode=1
repair_pid=1
retry_count=0
retry_delay=5

while (( $# > 0 )); do
    case "$1" in
        --manifest)
            manifest_file=$2
            shift 2
            ;;
        --report-only|--no-fail)
            strict_mode=0
            shift
            ;;
        --no-repair-pid)
            repair_pid=0
            shift
            ;;
        --retries)
            retry_count=$2
            shift 2
            ;;
        --retry-delay)
            retry_delay=$2
            shift 2
            ;;
        --help)
            cat <<'EOF'
Usage: status.sh [--manifest FILE] [--report-only|--no-fail] [--no-repair-pid]
                [--retries N] [--retry-delay SECONDS]

Check whether the distributed load servers are still running and listening.

By default, a host reported as listening-without-pid is treated as an unhealthy
state and the script exits non-zero. Use --report-only (alias --no-fail) to
always exit zero and treat status checks as informational.

By default, status tries to repair stale/missing pid files when it can match a
running load_server on the expected port. Disable this with --no-repair-pid.

In strict mode, if anomalies remain, the script can retry automatically with
--retries N and sleep between attempts with --retry-delay SECONDS.
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

if (( retry_count < 0 || retry_delay < 0 )); then
    echo "Invalid retry parameters: retries and retry-delay must be >= 0" >&2
    exit 1
fi

manifest_file=$(normalize_path "$manifest_file")
load_manifest "$manifest_file"

check_once() {
    local failures=0
    local running_count=0
    local stopped_count=0
    local anomaly_count=0

    while IFS= read -r host; do
        result=$(ssh "${SSH_OPTIONS[@]}" "$host" bash -s -- "$REMOTE_DIRNAME" "$PORT" "$PORT_HEX" "$host" "$repair_pid" <<'REMOTE'
set -euo pipefail

remote_dirname=$1
port=$2
port_hex=$3
host_name=$4
repair_pid=$5
app_dir="$HOME/$remote_dirname"
server_bin="$app_dir/bin/load_server"
pid_file="$app_dir/pids/$host_name.pid"

find_running_pid() {
    ps -u "$USER" -o pid= -o args= | awk -v server_bin="$server_bin" -v port="$port" '
        index($0, server_bin) && index($0, "--port " port) {
            print $1
            exit
        }
    '
}

find_compatible_pid() {
    ps -u "$USER" -o pid= -o args= | awk -v port="$port" '
        index($0, "load_server") && index($0, "--port " port) {
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

compatible_pid=$(find_compatible_pid || true)
if [[ -n "$compatible_pid" ]] && kill -0 "$compatible_pid" 2>/dev/null && port_is_listening; then
    if (( repair_pid )); then
        mkdir -p "$(dirname "$pid_file")"
        echo "$compatible_pid" > "$pid_file"
        echo "running:$compatible_pid:pid-repaired"
    else
        echo "running:$compatible_pid"
    fi
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
        status_value=${result:-ssh-failed}
        case "$status_value" in
            running:*)
                (( running_count += 1 ))
                ;;
            stopped)
                (( stopped_count += 1 ))
                ;;
            *)
                (( anomaly_count += 1 ))
                ;;
        esac
        # Exit code semantics from remote probe:
        # 0 = running, 1 = stopped, 2 = listening-without-pid, other = ssh/probe error.
        # Treat "stopped" as informational, not a failure.
        if (( strict_mode == 1 && rc != 0 && rc != 1 )); then
            (( failures += 1 ))
        fi
        unset rc
    done < "$HOSTS_FILE"

    printf 'Summary\trunning=%d\tstopped=%d\tanomaly=%d\n' "$running_count" "$stopped_count" "$anomaly_count"
    return $failures
}

for (( attempt = 0; attempt <= retry_count; ++attempt )); do
    if (( retry_count > 0 )); then
        printf 'Status attempt %d/%d\n' "$((attempt + 1))" "$((retry_count + 1))"
    fi

    if check_once; then
        exit 0
    fi

    if (( strict_mode == 0 )); then
        exit 0
    fi

    if (( attempt < retry_count )); then
        printf 'Strict status failed; retrying in %d second(s)...\n' "$retry_delay" >&2
        sleep "$retry_delay"
    fi
done

exit 1