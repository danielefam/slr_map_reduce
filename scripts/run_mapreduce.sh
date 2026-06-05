#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

manifest_file=$DEFAULT_MR_MANIFEST_FILE
job_manifest=$DEFAULT_MR_JOB_FILE
input_file=
output_file=$DEFAULT_MR_RESULTS_FILE
chunk_lines=200
timeout_ms=
job_id=
reducers=
rpc_retries=
rpc_retry_delay_ms=
run_attempts=6
run_retry_delay=1
preflight_timeout_sec=1

is_integer() {
    local value=$1
    [[ "$value" =~ ^[0-9]+$ ]]
}

require_positive_integer() {
    local name=$1
    local value=$2
    if ! is_integer "$value" || (( value <= 0 )); then
        printf '%s must be a positive integer (got: %s)\n' "$name" "$value" >&2
        exit 2
    fi
}

require_non_negative_integer() {
    local name=$1
    local value=$2
    if ! is_integer "$value"; then
        printf '%s must be a non-negative integer (got: %s)\n' "$name" "$value" >&2
        exit 2
    fi
}

while (( $# > 0 )); do
    case "$1" in
        --manifest)
            manifest_file=$2
            shift 2
            ;;
        --job-manifest)
            job_manifest=$2
            shift 2
            ;;
        --input)
            input_file=$2
            shift 2
            ;;
        --output)
            output_file=$2
            shift 2
            ;;
        --chunk-lines)
            chunk_lines=$2
            shift 2
            ;;
        --timeout-ms)
            timeout_ms=$2
            shift 2
            ;;
        --reducers)
            reducers=$2
            shift 2
            ;;
        --job-id)
            job_id=$2
            shift 2
            ;;
        --rpc-retries)
            rpc_retries=$2
            shift 2
            ;;
        --rpc-retry-delay-ms)
            rpc_retry_delay_ms=$2
            shift 2
            ;;
        --run-attempts)
            run_attempts=$2
            shift 2
            ;;
        --run-retry-delay)
            run_retry_delay=$2
            shift 2
            ;;
        --preflight-timeout-sec)
            preflight_timeout_sec=$2
            shift 2
            ;;
        --help)
            cat <<'EOF'
Usage: run_mapreduce.sh --input FILE [--manifest FILE] [--job-manifest FILE]
                        [--output FILE] [--chunk-lines N] [--reducers N]
                        [--timeout-ms MS] [--job-id ID]
                        [--rpc-retries N] [--rpc-retry-delay-ms MS]
                        [--run-attempts N] [--run-retry-delay SECONDS]
                        [--preflight-timeout-sec SECONDS]

Run the local MapReduce coordinator using the stored worker host list and port.
EOF
            exit 0
            ;;
        *)
            printf 'Unknown argument: %s\n' "$1" >&2
            exit 2
            ;;
    esac
done

if [[ -z "$input_file" ]]; then
    printf '--input is required\n' >&2
    exit 2
fi

require_positive_integer "--chunk-lines" "$chunk_lines"
require_positive_integer "--run-attempts" "$run_attempts"
require_non_negative_integer "--run-retry-delay" "$run_retry_delay"
require_positive_integer "--preflight-timeout-sec" "$preflight_timeout_sec"

if [[ -n "$reducers" ]]; then
    require_positive_integer "--reducers" "$reducers"
fi

if [[ -n "$timeout_ms" ]]; then
    require_positive_integer "--timeout-ms" "$timeout_ms"
fi

if [[ -n "$rpc_retries" ]]; then
    require_positive_integer "--rpc-retries" "$rpc_retries"
fi

if [[ -n "$rpc_retry_delay_ms" ]]; then
    require_positive_integer "--rpc-retry-delay-ms" "$rpc_retry_delay_ms"
fi

if ! require_commands mkdir cp grep tee awk timeout mktemp; then
    exit 5
fi
ensure_run_dir

manifest_file=$(normalize_path "$manifest_file")
job_manifest=$(normalize_path "$job_manifest")
input_file=$(normalize_path "$input_file")
output_file=$(normalize_path "$output_file")
output_dir=${output_file%/*}
mkdir -p "$output_dir"

if ! load_manifest "$manifest_file"; then
    printf 'Failed to load MapReduce manifest: %s\n' "$manifest_file" >&2
    exit 3
fi

command=(
    "$REPO_ROOT/bin/mr_coordinator"
    --hosts "$HOSTS_FILE"
    --port "$PORT"
    --input "$input_file"
    --output "$output_file"
    --chunk-lines "$chunk_lines"
    --job-manifest "$job_manifest"
)

if [[ -n "$timeout_ms" ]]; then
    command+=(--timeout-ms "$timeout_ms")
fi

if [[ -n "$reducers" ]]; then
    command+=(--reducers "$reducers")
fi

if [[ -n "$job_id" ]]; then
    command+=(--job-id "$job_id")
fi

if [[ -n "$rpc_retries" ]]; then
    command+=(--rpc-retries "$rpc_retries")
fi

if [[ -n "$rpc_retry_delay_ms" ]]; then
    command+=(--rpc-retry-delay-ms "$rpc_retry_delay_ms")
fi

runtime_hosts_file="$RUN_DIR/mr_hosts_runtime.txt"
source_hosts_file="$HOSTS_FILE"
if [[ "$source_hosts_file" == "$runtime_hosts_file" ]]; then
    if [[ -s "$DEFAULT_MR_HOSTS_FILE" ]]; then
        source_hosts_file="$DEFAULT_MR_HOSTS_FILE"
        printf 'MapReduce run: manifest HOSTS_FILE points to runtime hosts; using stable host list %s\n' "$source_hosts_file" >&2
    elif [[ -s "$RUN_DIR/mr_deployed_hosts.txt" ]]; then
        cp "$RUN_DIR/mr_deployed_hosts.txt" "$DEFAULT_MR_HOSTS_FILE"
        source_hosts_file="$DEFAULT_MR_HOSTS_FILE"
        printf 'MapReduce run: manifest HOSTS_FILE points to runtime hosts; recovered stable host list %s from deployed-hosts registry\n' "$source_hosts_file" >&2
    fi
fi

if [[ "$source_hosts_file" != "$runtime_hosts_file" ]]; then
    cp "$source_hosts_file" "$runtime_hosts_file"
fi

probe_host_port() {
    local host=$1
    local port=$2

    timeout "$preflight_timeout_sec" bash -c "exec 3<>/dev/tcp/${host}/${port}" >/dev/null 2>&1
}

preflight_reachability() {
    local hosts_file=$1
    local port=$2
    local total=0
    local reachable=0
    local first_unreachable=
    local reachable_hosts_tmp

    reachable_hosts_tmp=$(mktemp)
    : > "$reachable_hosts_tmp"

    while IFS= read -r host; do
        [[ -z "$host" ]] && continue
        (( total += 1 ))
        if probe_host_port "$host" "$port"; then
            (( reachable += 1 ))
            printf '%s\n' "$host" >> "$reachable_hosts_tmp"
        elif [[ -z "$first_unreachable" ]]; then
            first_unreachable=$host
        fi
    done < <(grep -Ev '^[[:space:]]*(#|$)' "$hosts_file" || true)

    if (( total == 0 )); then
        rm -f "$reachable_hosts_tmp"
        printf 'MapReduce preflight failed: no valid hosts in %s\n' "$hosts_file" >&2
        return 3
    fi

    if (( reachable == 0 )); then
        rm -f "$reachable_hosts_tmp"
        printf 'MapReduce preflight failed: 0/%d worker hosts reachable on tcp/%s from this coordinator. Example unreachable host: %s\n' \
            "$total" "$port" "${first_unreachable:-unknown}" >&2
        printf 'Action: redeploy workers (make deploy-mr) or use local fallback (make run-mr-local).\n' >&2
        return 6
    fi

    if (( reachable < total )); then
        mv "$reachable_hosts_tmp" "$hosts_file"
        printf 'MapReduce preflight pruned %d unreachable host(s); continuing with %d reachable host(s).\n' \
            "$((total - reachable))" "$reachable" >&2
    else
        rm -f "$reachable_hosts_tmp"
    fi

    printf 'MapReduce preflight: %d/%d worker hosts reachable on tcp/%s\n' "$reachable" "$total" "$port" >&2
    return 0
}

preflight_reachability "$runtime_hosts_file" "$PORT"
preflight_rc=$?
if (( preflight_rc != 0 )); then
    exit "$preflight_rc"
fi

attempt=1
rc=9
while (( attempt <= run_attempts )); do
    current_hosts=$(grep -Ec '^[[:space:]]*[^#[:space:]]' "$runtime_hosts_file" || true)
    printf 'MapReduce run attempt %d/%d using %d host(s)\n' "$attempt" "$run_attempts" "$current_hosts" >&2
    attempt_log="$RUN_DIR/mr_run_attempt_${attempt}.log"

    run_command=("${command[@]}")
    run_command[2]="$runtime_hosts_file"

    set +e
    "${run_command[@]}" 2>&1 | tee "$attempt_log"
    rc=${PIPESTATUS[0]}
    set -e

    if (( rc == 0 )); then
        break
    fi

    if (( (rc != 6 && rc != 7) || attempt >= run_attempts )); then
        break
    fi

    if (( current_hosts > 1 )); then
        bad_host=$(awk '
            /network error on / {
                for (i = 1; i <= NF; ++i) {
                    if ($i == "on" && i < NF) {
                        host = $(i + 1)
                        gsub(/[,)]/, "", host)
                        print host
                        exit
                    }
                }
            }
        ' "$attempt_log")

        tmp_hosts="$runtime_hosts_file.next"
        if [[ -n "$bad_host" ]] && grep -qxF "$bad_host" "$runtime_hosts_file"; then
            grep -vxF "$bad_host" "$runtime_hosts_file" > "$tmp_hosts"
            printf 'Retrying after excluding unstable host %s\n' "$bad_host" >&2
        else
            head -n -1 "$runtime_hosts_file" > "$tmp_hosts"
            printf 'Retrying with a smaller stable host set (%d host(s))\n' "$((current_hosts - 1))" >&2
        fi

        mv "$tmp_hosts" "$runtime_hosts_file"

        if ! bash "$SCRIPT_DIR/deploy_mapreduce.sh" \
            --hosts "$runtime_hosts_file" \
            --manifest "$manifest_file" \
            --start-port "${PORT_RANGE_START:-$DEFAULT_PORT_START}" \
            --end-port "${PORT_RANGE_END:-$DEFAULT_PORT_END}" \
            --remote-dirname "${REMOTE_DIRNAME:-$DEFAULT_MR_REMOTE_DIRNAME}"; then
            printf 'Failed to redeploy workers for retry host set\n' >&2
            break
        fi

    else
        printf 'Retry requested but only one host remains; keeping current host set\n' >&2
    fi

    if (( run_retry_delay > 0 )); then
        sleep "$run_retry_delay"
    fi

    attempt=$((attempt + 1))
done

if (( rc == 0 )); then
    printf 'Saved MapReduce output to %s\n' "$output_file" >&2
    exit 0
fi

case "$rc" in
    2)
        reason="usage/config error"
        ;;
    3)
        reason="hosts/manifest error"
        ;;
    4)
        reason="input read/split error"
        ;;
    5)
        reason="job setup error"
        ;;
    6)
        reason="network/remote I/O error"
        ;;
    7)
        reason="protocol/response error"
        ;;
    8)
        reason="output write error"
        ;;
    9)
        reason="internal error"
        ;;
    *)
        reason="unknown error"
        ;;
esac

printf 'MapReduce coordinator failed (exit=%s: %s)\n' "$rc" "$reason" >&2
exit "$rc"