#!/usr/bin/env bash
set -euo pipefail
DAEMON="${1:-http://127.0.0.1:19085}"
BIND="${2:-127.0.0.1:11001}"
cargo run -- --bind "$BIND" --daemon "$DAEMON"
