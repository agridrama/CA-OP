#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SETTINGS_DIR="$PROJECT_ROOT/benchmark/settings"
TEMPLATES_DIR="$SETTINGS_DIR/templates"
CLAB_DIR="$SETTINGS_DIR/clab"
RESULTS_ROOT="$PROJECT_ROOT/benchmark/results"
MANIFEST_PATH="$PROJECT_ROOT/benchmark/Cargo.toml"

RUST_LOG="${RUST_LOG:-info}"
CLIENT_DURATION_SEC="${CLIENT_DURATION_SEC:-20}"
CLIENT_READ_RATIO="${CLIENT_READ_RATIO:-0.5}"
CLUSTER_SIZE=5
SERVER_IDS=(1 2 3 4 5)
CLIENT_SERVER_IDS_CSV="${CLIENT_SERVER_IDS_CSV:-1,5}"
IFS=',' read -r -a CLIENT_SERVER_IDS <<< "$CLIENT_SERVER_IDS_CSV"
OWD_WINDOW_SIZE="${OWD_WINDOW_SIZE:-50}"
OWD_MAX_US="${OWD_MAX_US:-500000}"
OWD_UNCERTAINTY_BETA="${OWD_UNCERTAINTY_BETA:-3}"
OWD_PERCENTILE="${OWD_PERCENTILE:-0.8}"

protocol_features() {
    case "$1" in
        caop) echo "--no-default-features --features protocol-caop" ;;
        baseline) echo "--no-default-features --features protocol-baseline" ;;
        *) echo "Unsupported protocol: $1" >&2; exit 1 ;;
    esac
}

validate_clock_quality() {
    case "$1" in
        high|medium|low) ;;
        *) echo "Unsupported clock quality: $1" >&2; exit 1 ;;
    esac
}

validate_workload() {
    case "$1" in
        100|500|1000|5000|10000|sweep) ;;
        *) echo "Unsupported workload: $1" >&2; exit 1 ;;
    esac
}

validate_topology() {
    case "$1" in
        local|small_jitter|large_jitter|imbalance) ;;
        *) echo "Unsupported topology: $1" >&2; exit 1 ;;
    esac
}

clock_sync_uncertainty() {
    validate_clock_quality "$1"
    awk -F'= ' '/sync_uncertainty_us/ {print $2; exit}' "$SETTINGS_DIR/$1/server-1-config.toml"
}

clock_sync_period() {
    validate_clock_quality "$1"
    awk -F'= ' '/sync_period_us/ {print $2; exit}' "$SETTINGS_DIR/$1/server-1-config.toml"
}

server_drift() {
    case "$1" in
        1) echo 400 ;;
        2) echo 800 ;;
        3) echo -500 ;;
        4) echo -1000 ;;
        5) echo 100 ;;
        *) echo 0 ;;
    esac
}

server_seed() {
    echo $((41 + $1))
}

containerlab_server_ip() {
    case "$1" in
        1) echo "10.10.1.11" ;;
        2) echo "10.10.2.12" ;;
        3) echo "10.10.3.13" ;;
        4) echo "10.10.4.14" ;;
        5) echo "10.10.5.15" ;;
        *) echo "Unsupported server id: $1" >&2; exit 1 ;;
    esac
}

workload_values() {
    validate_workload "$1"
    case "$1" in
        sweep) echo "100 500 1000 5000 10000" ;;
        *) echo "$1" ;;
    esac
}

scenario_name() {
    local protocol="$1"
    local topology="$2"
    local clock_quality="$3"
    local rps="$4"
    if [[ "$protocol" == "baseline" ]]; then
        echo "${topology}-rps${rps}"
    else
        echo "${topology}-${clock_quality}-rps${rps}"
    fi
}

run_dir() {
    local experiment="$1"
    local protocol="$2"
    local topology="$3"
    local clock_quality="$4"
    local rps="$5"
    local run="$6"
    echo "$RESULTS_ROOT/$experiment/$protocol/$(scenario_name "$protocol" "$topology" "$clock_quality" "$rps")/run-$run"
}

render_template() {
    local template="$1"
    shift
    local sed_args=()
    for replacement in "$@"; do
        sed_args+=(-e "$replacement")
    done
    sed "${sed_args[@]}" "$template"
}

prepare_run_dir() {
    local experiment="$1"
    local protocol="$2"
    local topology="$3"
    local clock_quality="$4"
    local rps="$5"
    local run="$6"
    local mode="$7"
    local out cluster_template cluster_name
    local sync_uncertainty sync_period

    out="$(run_dir "$experiment" "$protocol" "$topology" "$clock_quality" "$rps" "$run")"
    mkdir -p "$out"

    if [[ "$mode" == "local" ]]; then
        cluster_template="$TEMPLATES_DIR/cluster-local.toml"
        cluster_name="local-cluster"
    else
        cluster_template="$TEMPLATES_DIR/cluster-containerlab.toml"
        cluster_name="containerlab-cluster"
    fi

    if [[ "$mode" == "local" ]]; then
        render_template "$cluster_template" \
            "s|__CLUSTER_NAME__|$cluster_name|g" \
            > "$out/cluster-config.toml"
    else
        render_template "$cluster_template" \
            "s|__CLUSTER_NAME__|$cluster_name|g" \
            "s|__NODE_ADDR_1__|$(containerlab_server_ip 1):8000|g" \
            "s|__NODE_ADDR_2__|$(containerlab_server_ip 2):8000|g" \
            "s|__NODE_ADDR_3__|$(containerlab_server_ip 3):8000|g" \
            "s|__NODE_ADDR_4__|$(containerlab_server_ip 4):8000|g" \
            "s|__NODE_ADDR_5__|$(containerlab_server_ip 5):8000|g" \
            > "$out/cluster-config.toml"
    fi

    sync_uncertainty="$(clock_sync_uncertainty "$clock_quality")"
    sync_period="$(clock_sync_period "$clock_quality")"

    for id in "${SERVER_IDS[@]}"; do
        local location listen_address listen_port num_clients drift seed
        if [[ "$mode" == "local" ]]; then
            location="local-$id"
            listen_address="127.0.0.1"
            listen_port="800$id"
        else
            location="${topology}-$id"
            listen_address="0.0.0.0"
            listen_port=8000
        fi

        num_clients=0
        for client_server_id in "${CLIENT_SERVER_IDS[@]}"; do
            if [[ "$id" -eq "$client_server_id" ]]; then
                num_clients=$((num_clients + 1))
            fi
        done

        drift="$(server_drift "$id")"
        seed="$(server_seed "$id")"

        render_template "$TEMPLATES_DIR/server-config.toml.in" \
            "s|__LOCATION__|$location|g" \
            "s|__PROTOCOL__|$protocol|g" \
            "s|__SERVER_ID__|$id|g" \
            "s|__NUM_CLIENTS__|$num_clients|g" \
            "s|__LISTEN_ADDRESS__|$listen_address|g" \
            "s|__LISTEN_PORT__|$listen_port|g" \
            "s|__OUTPUT_FILEPATH__|server-$id.json|g" \
            "s|__CLOCK_NODE_ID__|$id|g" \
            "s|__DRIFT_RATE__|$drift|g" \
            "s|__SYNC_UNCERTAINTY__|$sync_uncertainty|g" \
            "s|__SYNC_PERIOD__|$sync_period|g" \
            "s|__SEED__|$seed|g" \
            "s|__OWD_WINDOW_SIZE__|$OWD_WINDOW_SIZE|g" \
            "s|__OWD_MAX_US__|$OWD_MAX_US|g" \
            "s|__OWD_UNCERTAINTY_BETA__|$OWD_UNCERTAINTY_BETA|g" \
            "s|__OWD_PERCENTILE__|$OWD_PERCENTILE|g" \
            > "$out/server-$id-config.toml"
    done

    local client_index=1
    for server_id in "${CLIENT_SERVER_IDS[@]}"; do
        local location server_address
        if [[ "$mode" == "local" ]]; then
            location="local-client-$client_index"
            server_address="127.0.0.1:800${server_id}"
        else
            location="containerlab-client-$client_index"
            server_address="$(containerlab_server_ip "$server_id"):8000"
        fi

        render_template "$TEMPLATES_DIR/client-config.toml.in" \
            "s|__LOCATION__|$location|g" \
            "s|__CLIENT_ID__|$client_index|g" \
            "s|__SERVER_ID__|$server_id|g" \
            "s|__SERVER_ADDRESS__|$server_address|g" \
            "s|__SUMMARY_FILEPATH__|client-$client_index.json|g" \
            "s|__OUTPUT_FILEPATH__|client-$client_index.csv|g" \
            "s|__DURATION_SEC__|$CLIENT_DURATION_SEC|g" \
            "s|__REQUESTS_PER_SEC__|$rps|g" \
            "s|__READ_RATIO__|$CLIENT_READ_RATIO|g" \
            > "$out/client-$client_index-config.toml"

        client_index=$((client_index + 1))
    done

    echo "$out"
}

run_cargo() {
    local cargo_features
    cargo_features="$(protocol_features "$1")"
    shift
    cargo "$@" --manifest-path "$MANIFEST_PATH" $cargo_features
}

binary_path() {
    local name="$1"
    echo "$PROJECT_ROOT/benchmark/target/release/$name"
}

server_image_tag() {
    echo "omnipaxos-server-$1"
}

client_image_tag() {
    echo "omnipaxos-client-$1"
}

render_topology() {
    local topology="$1"
    local out_dir="$2"
    local lab_name="$3"
    local protocol="$4"
    local template="$CLAB_DIR/topology_${topology}.clab.yml"
    render_template "$template" \
        "s|__RUN_DIR__|$out_dir|g" \
        "s|__LAB_NAME__|$lab_name|g" \
        "s|__SERVER_IMAGE__|$(server_image_tag "$protocol")|g" \
        "s|__CLIENT_IMAGE__|$(client_image_tag "$protocol")|g" \
        > "$out_dir/topology.clab.yml"
}
