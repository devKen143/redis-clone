#!/usr/bin/env bash
# Compiles the command engine (no networking) to WebAssembly for the browser redis-cli.
set -e
cd "$(dirname "$0")"
cargo build --lib --release --target wasm32-unknown-unknown
T="${CARGO_TARGET_DIR:-target}"
mkdir -p web/public
cp "$T/wasm32-unknown-unknown/release/redis_clone.wasm" web/public/redis.wasm
echo "wrote web/public/redis.wasm ($(wc -c < web/public/redis.wasm) bytes)"
