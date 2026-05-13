#!/usr/bin/env sh
set -eu

WORKER_BUILD_VERSION="0.8.3"
WORKER_BUILD="$HOME/.cargo/bin/worker-build"

if command -v rustup >/dev/null 2>&1; then
    RUSTUP_CARGO="$(rustup which cargo)"
    export PATH="$(dirname "$RUSTUP_CARGO"):$PATH"
    export RUSTC="$(rustup which rustc)"
fi

installed_version=""
if [ -x "$WORKER_BUILD" ]; then
    installed_version="$("$WORKER_BUILD" --version 2>/dev/null || true)"
fi

if [ "$installed_version" != "$WORKER_BUILD_VERSION" ]; then
    cargo install worker-build --version "$WORKER_BUILD_VERSION" --locked --force
fi

"$WORKER_BUILD" --release --no-panic-recovery
