#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

NUM_RUNS="${1:-3}"
PROTOCOL="${2:-caop}"

if [[ "$PROTOCOL" == "baseline" ]]; then
    "$SCRIPT_DIR/run-local.sh" baseline medium 500 "$NUM_RUNS"
    exit 0
fi

for quality in high medium low; do
    "$SCRIPT_DIR/run-local.sh" "$PROTOCOL" "$quality" 500 "$NUM_RUNS"
done
