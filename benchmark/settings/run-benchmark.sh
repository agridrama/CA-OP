#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCRIPTS_DIR="$PROJECT_ROOT/benchmark/scripts"

if [[ $# -lt 4 ]]; then
    cat <<'EOF'
Usage: run-benchmark.sh <protocol> <topology> <clock_quality> <workload> [runs]

protocol:       caop | baseline
topology:       local | small_jitter | large_jitter | imbalance
clock_quality:  high | medium | low
workload:       100 | 500 | 1000 | 5000 | 10000 | sweep
runs:           default 1
EOF
    exit 1
fi

if [[ "$2" == "local" ]]; then
    exec "$SCRIPTS_DIR/run-local.sh" "$1" "$3" "$4" "${5:-1}"
fi

exec "$SCRIPTS_DIR/run-clab.sh" "$1" "$2" "$3" "$4" "${5:-1}"
