#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

usage() {
    cat <<'EOF'
Usage: run-clab.sh <protocol> <topology> <clock_quality> <workload> [runs]

protocol:       caop | baseline
topology:       small_jitter | large_jitter | imbalance
clock_quality:  high | medium | low
workload:       100 | 500 | 1000 | 5000 | 10000 | sweep
runs:           default 1
EOF
}

if [[ $# -lt 4 ]]; then
    usage
    exit 1
fi

PROTOCOL="$1"
TOPOLOGY="$2"
CLOCK_QUALITY="$3"
WORKLOAD="$4"
NUM_RUNS="${5:-1}"

protocol_features "$PROTOCOL" >/dev/null
validate_clock_quality "$CLOCK_QUALITY"
validate_workload "$WORKLOAD"
case "$TOPOLOGY" in
    small_jitter|large_jitter|imbalance) ;;
    *) echo "Unsupported topology: $TOPOLOGY" >&2; exit 1 ;;
esac

command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
command -v containerlab >/dev/null || { echo "containerlab is required" >&2; exit 1; }

CURRENT_TOPOLOGY_FILE=""

cleanup() {
    if [[ -n "$CURRENT_TOPOLOGY_FILE" ]]; then
        containerlab destroy -t "$CURRENT_TOPOLOGY_FILE" --cleanup >/dev/null 2>&1 || true
        CURRENT_TOPOLOGY_FILE=""
    fi
}

trap cleanup EXIT INT TERM

start_server_processes() {
    local lab_name="$1"
    for id in "${SERVER_IDS[@]}"; do
        docker exec \
            -w /app/run \
            -e "RUST_LOG=$RUST_LOG" \
            -e SERVER_CONFIG_FILE="/app/run/server-${id}-config.toml" \
            -e CLUSTER_CONFIG_FILE=/app/run/cluster-config.toml \
            "clab-${lab_name}-s${id}" \
            /bin/sh -lc "/usr/local/bin/server > /app/run/server-${id}-stdout.log 2> /app/run/server-${id}-stderr.log" &
    done
    sleep 3
}

start_client_processes() {
    local lab_name="$1"
    local client_pids=()
    for client_id in $(seq 1 ${#CLIENT_SERVER_IDS[@]}); do
        docker exec \
            -w /app/run \
            -e "RUST_LOG=$RUST_LOG" \
            -e CONFIG_FILE="/app/run/client-${client_id}-config.toml" \
            "clab-${lab_name}-c${client_id}" \
            /bin/sh -lc "/usr/local/bin/client > /app/run/client-${client_id}-stdout.log 2> /app/run/client-${client_id}-stderr.log" &
        client_pids+=($!)
    done
    wait "${client_pids[@]}"
}

run_clab_once() {
    local rps="$1"
    local run="$2"
    local out lab_name topology_file
    out="$(prepare_run_dir containerlab "$PROTOCOL" "$TOPOLOGY" "$CLOCK_QUALITY" "$rps" "$run" containerlab)"
    if [[ "$PROTOCOL" == "baseline" ]]; then
        lab_name="caop-bench-${PROTOCOL}-${TOPOLOGY}-rps${rps}-run${run}"
    else
        lab_name="caop-bench-${PROTOCOL}-${TOPOLOGY}-${CLOCK_QUALITY}-rps${rps}-run${run}"
    fi

    render_topology "$TOPOLOGY" "$out" "$lab_name" "$PROTOCOL"
    topology_file="$out/topology.clab.yml"
    CURRENT_TOPOLOGY_FILE="$topology_file"

    containerlab deploy -t "$topology_file"
    start_server_processes "$lab_name"
    start_client_processes "$lab_name"
    sleep 5
    cleanup
}

for rps in $(workload_values "$WORKLOAD"); do
    for run in $(seq 0 $((NUM_RUNS - 1))); do
        echo "Running containerlab benchmark: protocol=$PROTOCOL scenario=$(scenario_name "$PROTOCOL" "$TOPOLOGY" "$CLOCK_QUALITY" "$rps") run=$run"
        run_clab_once "$rps" "$run"
    done
done
