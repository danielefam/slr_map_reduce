#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

host_count=$DEFAULT_HOST_COUNT
hosts_file=
manifest_file=$DEFAULT_MANIFEST_FILE
api_url=$DEFAULT_API_BASE_URL
start_port=$DEFAULT_PORT_START
end_port=$DEFAULT_PORT_END
remote_dirname=$DEFAULT_REMOTE_DIRNAME

while (( $# > 0 )); do
    case "$1" in
        --count)
            host_count=$2
            shift 2
            ;;
        --hosts)
            hosts_file=$2
            shift 2
            ;;
        --manifest)
            manifest_file=$2
            shift 2
            ;;
        --url)
            api_url=$2
            shift 2
            ;;
        --start-port)
            start_port=$2
            shift 2
            ;;
        --end-port)
            end_port=$2
            shift 2
            ;;
        --remote-dirname)
            remote_dirname=$2
            shift 2
            ;;
        --help)
            cat <<'EOF'
Usage: deploy.sh [--count N] [--hosts FILE] [--manifest FILE] [--url URL]
                 [--start-port N] [--end-port N] [--remote-dirname NAME]

Build locally, copy the shared bundle once to the NFS-backed home directory
through the first selected host, then issue one SSH start command per host.
EOF
            exit 0
            ;;
        *)
            printf 'Unknown argument: %s\n' "$1" >&2
            exit 1
            ;;
    esac
done

require_commands make ssh scp cp rm mkdir head awk ps
ensure_run_dir

manifest_file=$(normalize_path "$manifest_file")

if [[ -n "$hosts_file" ]]; then
    hosts_file=$(normalize_path "$hosts_file")
    candidate_hosts_file=$hosts_file
else
    candidate_hosts_file="$RUN_DIR/hosts-candidates.txt"
    hosts_file=$DEFAULT_HOSTS_FILE
    candidate_count=$(( host_count + 25 ))
    if ! bash "$SCRIPT_DIR/select_hosts.sh" --count "$candidate_count" --output "$candidate_hosts_file" --url "$api_url"; then
        bash "$SCRIPT_DIR/select_hosts.sh" --count "$host_count" --output "$candidate_hosts_file" --url "$api_url"
    fi
fi

bash "$SCRIPT_DIR/select_port.sh" --hosts "$candidate_hosts_file" --selected-hosts-output "$hosts_file" --required-host-count "$host_count" --output "$manifest_file" --start-port "$start_port" --end-port "$end_port"

load_manifest "$manifest_file"

make -C "$REPO_ROOT" clean all >/dev/null

bundle_dir="$RUN_DIR/$remote_dirname"
rm -rf "$bundle_dir"
mkdir -p "$bundle_dir/bin"
cp "$REPO_ROOT/bin/load_server" "$bundle_dir/bin/load_server"

staging_host=$(head -n 1 "$HOSTS_FILE")
launch_status_file="$RUN_DIR/launch_status.tsv"

scp -r "$bundle_dir" "$staging_host:~/" >/dev/null

write_manifest "$manifest_file" \
    HOSTS_FILE "$HOSTS_FILE" \
    HOST_COUNT "$HOST_COUNT" \
    PORT "$PORT" \
    PORT_HEX "$PORT_HEX" \
    PORT_RANGE_START "$PORT_RANGE_START" \
    PORT_RANGE_END "$PORT_RANGE_END" \
    STAGING_HOST "$staging_host" \
    REMOTE_DIRNAME "$remote_dirname" \
    LAUNCH_STATUS_FILE "$launch_status_file"

launch_remote() {
    local host=$1
    ssh "${SSH_OPTIONS[@]}" "$host" bash -s -- "$remote_dirname" "$PORT" "$PORT_HEX" "$host" <<'REMOTE'
set -euo pipefail

remote_dirname=$1
port=$2
port_hex=$3
host_name=$4
app_dir="$HOME/$remote_dirname"
server_bin="$app_dir/bin/load_server"
log_dir="$app_dir/logs"
pid_dir="$app_dir/pids"
log_file="$log_dir/$host_name.log"
pid_file="$pid_dir/$host_name.pid"

find_running_pid() {
    ps -u "$USER" -o pid= -o args= | awk -v server_bin="$server_bin" -v port="$port" '
        index($0, server_bin) && index($0, "--port " port) {
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

if [[ ! -x "$server_bin" ]]; then
    echo "missing-binary"
    exit 5
fi

mkdir -p "$log_dir" "$pid_dir"

if [[ -f "$pid_file" ]]; then
    existing_pid=$(<"$pid_file")
    if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null; then
        echo "already-running:$existing_pid"
        exit 0
    fi
    rm -f "$pid_file"
fi

existing_pid=$(find_running_pid || true)
if [[ -n "$existing_pid" ]]; then
    echo "$existing_pid" > "$pid_file"
    echo "already-running:$existing_pid"
    exit 0
fi

if port_is_listening; then
    echo "port-busy"
    exit 3
fi

nohup "$server_bin" --port "$port" > "$log_file" 2>&1 < /dev/null &
new_pid=$!
echo "$new_pid" > "$pid_file"

if kill -0 "$new_pid" 2>/dev/null; then
    echo "started:$new_pid"
    exit 0
fi

echo "failed-start"
exit 4
REMOTE
}

failures=0
: > "$launch_status_file"

while IFS= read -r host; do
    result=$(launch_remote "$host" 2>&1) || rc=$?
    rc=${rc:-0}
    printf '%s\t%s\t%s\n' "$host" "$rc" "${result:-}" >> "$launch_status_file"
    if (( rc != 0 )); then
        (( failures += 1 ))
    fi
    unset rc
done < "$HOSTS_FILE"

printf 'Launch results written to %s\n' "$launch_status_file"
(( failures == 0 ))