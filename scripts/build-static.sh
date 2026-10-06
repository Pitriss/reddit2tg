#!/bin/sh
set -eu
TARGET=${TARGET:-x86_64-unknown-linux-musl}
command -v cargo >/dev/null 2>&1 || { echo "cargo not found" >&2; exit 1; }
command -v rustup >/dev/null 2>&1 || { echo "rustup not found" >&2; exit 1; }
command -v musl-gcc >/dev/null 2>&1 || { echo "musl-gcc not found; install musl-tools" >&2; exit 1; }
rustup target add "$TARGET"
cargo build --release --target "$TARGET"
BIN="target/$TARGET/release/reddit2tg"
file "$BIN"
ldd "$BIN" 2>&1 || true
printf '%s\n' "$BIN"
