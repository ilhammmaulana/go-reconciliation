#!/bin/bash
# Build script for Go Reconciliation (Server & Reconcile Worker)

set -e

GOOS="${1:-linux}"
GOARCH="${2:-amd64}"

echo "==> Building Go Reconciliation Binaries"
echo "    OS: $GOOS | Arch: $GOARCH"

mkdir -p bin

# 1. Build Ingestion & Scanner Server
echo "==> Building server (Ingestion daemon & Scanner)..."
GOOS=$GOOS GOARCH=$GOARCH go build \
  -ldflags="-s -w" \
  -o bin/server \
  ./cmd/server/

# 2. Build Reconcile Queue Worker
echo "==> Building reconcile-worker (Reconciliation Queue Consumer)..."
GOOS=$GOOS GOARCH=$GOARCH go build \
  -ldflags="-s -w" \
  -o bin/reconcile-worker \
  ./cmd/worker/

echo "==> Build complete in ./bin/"
ls -lh bin/
