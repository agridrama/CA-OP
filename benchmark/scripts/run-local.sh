#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

usage() {
    cat <<'EOF'
Usage: run-local.sh <protocol> <clock_quality> <workload> [runs]

protocol:       caop | baseline
clock_quality:  high | medium | low
workload:       100 | 500 | 1000 | 5000 | 10000 | sweep
runs:           default 1
EOF
}

if [[ $# -lt 3 ]]; then
    usage
    exit 1
fi

PROTOCOL="$1"
CLOCK_QUALITY="$2"
WORKLOAD="$3"
NUM_RUNS="${4:-1}"

validate_clock_quality "$CLOCK_QUALITY"
validate_workload "$WORKLOAD"
protocol_features "$PROTOCOL" >/dev/null

SERVER_BIN=""
CLIENT_BIN=""
SERVER_PIDS=()
CLIENT_PIDS=()

cleanup() {
    for pid in "${CLIENT_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "${SERVER_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    if ((${#CLIENT_PIDS[@]} > 0)); then
        wait "${CLIENT_PIDS[@]}" 2>/dev/null || true
    fi
    if ((${#SERVER_PIDS[@]} > 0)); then
        wait "${SERVER_PIDS[@]}" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

run_cargo "$PROTOCOL" build --release --bin server --bin client >/dev/null
SERVER_BIN="$(binary_path server)"
CLIENT_BIN="$(binary_path client)"

run_local_once() {
    local rps="$1"
    local run="$2"
    local out
    out="$(prepare_run_dir local "$PROTOCOL" local "$CLOCK_QUALITY" "$rps" "$run" local)"
    SERVER_PIDS=()
    CLIENT_PIDS=()

    for id in "${SERVER_IDS[@]}"; do
        RUST_LOG="$RUST_LOG" \
        SERVER_CONFIG_FILE="$out/server-$id-config.toml" \
        CLUSTER_CONFIG_FILE="$out/cluster-config.toml" \
        "$SERVER_BIN" \
            >"$out/server-$id-stdout.log" \
            2>"$out/server-$id-stderr.log" &
        SERVER_PIDS+=($!)
    done

    sleep 3

    for client_id in $(seq 1 ${#CLIENT_SERVER_IDS[@]}); do
        RUST_LOG="$RUST_LOG" \
        CONFIG_FILE="$out/client-$client_id-config.toml" \
        "$CLIENT_BIN" \
            >"$out/client-$client_id-stdout.log" \
            2>"$out/client-$client_id-stderr.log" &
        CLIENT_PIDS+=($!)
    done

    wait "${CLIENT_PIDS[@]}"

    sleep 5
    for pid in "${SERVER_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait "${SERVER_PIDS[@]}" 2>/dev/null || true
    SERVER_PIDS=()
    CLIENT_PIDS=()
}

for rps in $(workload_values "$WORKLOAD"); do
    for run in $(seq 0 $((NUM_RUNS - 1))); do
        echo "Running local benchmark: protocol=$PROTOCOL scenario=$(scenario_name "$PROTOCOL" local "$CLOCK_QUALITY" "$rps") run=$run"
        run_local_once "$rps" "$run"
    done
done
