#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
exec "$PROJECT_ROOT/benchmark/scripts/run-local.sh" "${1:-caop}" "${2:-medium}" "${3:-1000}" "${4:-1}"
