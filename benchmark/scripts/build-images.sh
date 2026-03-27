#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

usage() {
    cat <<'EOF'
Usage: build-images.sh <protocol>

protocol: caop | baseline
EOF
}

if [[ $# -ne 1 ]]; then
    usage
    exit 1
fi

PROTOCOL="$1"
CARGO_FEATURES="$(protocol_features "$PROTOCOL")"

docker build --no-cache --build-arg "CARGO_FEATURES=$CARGO_FEATURES" -t "$(server_image_tag "$PROTOCOL")" -f "$PROJECT_ROOT/benchmark/server.dockerfile" "$PROJECT_ROOT"
docker build --no-cache --build-arg "CARGO_FEATURES=$CARGO_FEATURES" -t "$(client_image_tag "$PROTOCOL")" -f "$PROJECT_ROOT/benchmark/client.dockerfile" "$PROJECT_ROOT"
