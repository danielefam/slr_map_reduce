#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

manifest_file=$DEFAULT_MANIFEST_FILE
host_count_override=
retry_count=2
candidate_buffer=25
minimum_host_count=3
start_port_override=
end_port_override=
clean_all=0
dry_run=0

while (( $# > 0 )); do
    case "$1" in
        --manifest)
            manifest_file=$2
            shift 2
            ;;
        --host-count)
            host_count_override=$2
            shift 2
            ;;
        --retries)
            retry_count=$2
            shift 2
            ;;
        --candidate-buffer)
            candidate_buffer=$2
            shift 2
            ;;
        --minimum-host-count)
            minimum_host_count=$2
            shift 2
            ;;
        --start-port)
            start_port_override=$2
            shift 2
            ;;
        --end-port)
            end_port_override=$2
            shift 2
            ;;
        --clean-all)
            clean_all=1
            shift
            ;;
        --dry-run)
            dry_run=1
            shift
            ;;
        --help)
            cat <<'EOF'
Usage: remediate_status.sh [--manifest FILE] [--host-count N] [--retries N]
                           [--candidate-buffer N] [--minimum-host-count N]
                           [--start-port N] [--end-port N]
                           [--clean-all] [--dry-run]

Remediate legacy status anomalies (for example listening-without-pid) by:
1) health check
2) cleanup
3) reselection of candidate hosts and common port
4) redeploy
5) recheck

The script retries with alternative nodes up to N times.

Defaults:
- retries: 2 (for a total of 3 health checks)
- candidate-buffer: 25
- minimum-host-count: 3

Notes:
- This script targets the legacy load_server flow (manifest env with REMOTE_DIRNAME and PORT).
- Use --clean-all to also run MapReduce cleanup before retrying.
EOF
            exit 0
            ;;
        *)
            printf 'Unknown argument: %s\n' "$1" >&2
            exit 1
            ;;
    esac
done

require_commands bash awk grep sed tail wc
manifest_file=$(normalize_path "$manifest_file")
load_manifest "$manifest_file"

if [[ -n "$host_count_override" ]]; then
    desired_host_count=$host_count_override
else
    desired_host_count=${HOST_COUNT:-$DEFAULT_HOST_COUNT}
fi

if [[ -n "$start_port_override" ]]; then
    start_port=$start_port_override
else
    start_port=${PORT_RANGE_START:-$DEFAULT_PORT_START}
fi

if [[ -n "$end_port_override" ]]; then
    end_port=$end_port_override
else
    end_port=${PORT_RANGE_END:-$DEFAULT_PORT_END}
fi

if (( minimum_host_count > desired_host_count )); then
    minimum_host_count=$desired_host_count
fi

if (( desired_host_count < 1 || retry_count < 0 || candidate_buffer < 0 || minimum_host_count < 1 )); then
    echo "Invalid numeric parameter values" >&2
    exit 1
fi

ensure_run_dir
status_log="$RUN_DIR/remediation_status_report.txt"
remediation_log="$RUN_DIR/remediation.log"
candidate_hosts_file="$RUN_DIR/remediation_hosts_candidates.txt"
selected_hosts_file="$RUN_DIR/remediation_hosts_selected.txt"

echo "=== Remediation started $(date -Is) ===" | tee -a "$remediation_log"
echo "manifest=$manifest_file desired_host_count=$desired_host_count retries=$retry_count min_hosts=$minimum_host_count port_range=$start_port-$end_port" | tee -a "$remediation_log"

run_cmd() {
    if (( dry_run )); then
        printf '[dry-run] %s\n' "$*" | tee -a "$remediation_log"
        return 0
    fi
    printf '[run] %s\n' "$*" | tee -a "$remediation_log"
    "$@"
}

collect_status() {
    if (( dry_run )); then
        printf 'Summary\trunning=0\tstopped=0\tanomaly=1\n' > "$status_log"
        return 0
    fi

    if ! bash "$SCRIPT_DIR/status.sh" --manifest "$manifest_file" --report-only > "$status_log" 2>&1; then
        # report-only should not fail; keep output for diagnostics anyway.
        true
    fi
}

extract_anomaly_count() {
    local summary
    summary=$(grep -E '^Summary' "$status_log" | tail -n 1 || true)
    if [[ -z "$summary" ]]; then
        echo 999
        return
    fi
    sed -n 's/.*anomaly=\([0-9][0-9]*\).*/\1/p' <<<"$summary"
}

for (( attempt = 0; attempt <= retry_count; ++attempt )); do
    printf '\n== Health check attempt %d/%d ==\n' "$((attempt + 1))" "$((retry_count + 1))" | tee -a "$remediation_log"
    collect_status
    cat "$status_log" | tee -a "$remediation_log"

    anomaly_count=$(extract_anomaly_count)
    if [[ -z "$anomaly_count" ]]; then
        anomaly_count=999
    fi

    if (( anomaly_count == 0 )); then
        echo "Remediation complete: no anomalies detected." | tee -a "$remediation_log"
        exit 0
    fi

    if (( attempt == retry_count )); then
        if (( dry_run )); then
            echo "Dry-run complete: anomalies would still be present after configured retries." | tee -a "$remediation_log"
            exit 0
        fi
        echo "Remediation failed: anomalies still present after all retries." | tee -a "$remediation_log" >&2
        exit 1
    fi

    echo "Anomalies detected ($anomaly_count). Starting cleanup and retry with alternative nodes..." | tee -a "$remediation_log"

    if (( clean_all )); then
        run_cmd bash "$SCRIPT_DIR/clean_all_remote.sh" --manifest "$manifest_file" --mr-manifest "$DEFAULT_MR_MANIFEST_FILE"
    else
        run_cmd bash "$SCRIPT_DIR/clean_remote.sh" --manifest "$manifest_file"
    fi

    candidate_count=$(( desired_host_count + candidate_buffer ))
    run_cmd bash "$SCRIPT_DIR/select_hosts.sh" --count "$candidate_count" --output "$candidate_hosts_file"

    run_cmd bash "$SCRIPT_DIR/select_port.sh" \
        --hosts "$candidate_hosts_file" \
        --selected-hosts-output "$selected_hosts_file" \
        --required-host-count "$desired_host_count" \
        --minimum-host-count "$minimum_host_count" \
        --allow-partial \
        --output "$manifest_file" \
        --start-port "$start_port" \
        --end-port "$end_port"

    load_manifest "$manifest_file"

    run_cmd bash "$SCRIPT_DIR/deploy.sh" \
        --hosts "$selected_hosts_file" \
        --manifest "$manifest_file" \
        --start-port "$start_port" \
        --end-port "$end_port" \
        --remote-dirname "$REMOTE_DIRNAME"
done

echo "Unexpected control flow in remediation script." >&2
exit 1
