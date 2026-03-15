#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build --release --bin duta-stratumd
echo
echo "Release binary is in target/release/duta-stratumd"
