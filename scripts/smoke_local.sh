#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

port=${1:-23000}
hosts_file="$RUN_DIR/hosts-local.txt"
server_log="$RUN_DIR/smoke-local-server.log"

require_commands make bash
ensure_run_dir

make -C "$REPO_ROOT" >/dev/null
printf '127.0.0.1\n' > "$hosts_file"

"$REPO_ROOT/bin/load_server" --port "$port" > "$server_log" 2>&1 &
server_pid=$!

cleanup() {
    if kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid" 2>/dev/null || true
    fi
}

trap cleanup EXIT

for _attempt in {1..200}; do
    if bash -lc "exec 3<>/dev/tcp/127.0.0.1/$port" 2>/dev/null; then
        break
    fi
done

"$REPO_ROOT/bin/load_client" --hosts "$hosts_file" --port "$port" --timeout-ms 1000 --expect-host-count 1