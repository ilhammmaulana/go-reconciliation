# ==========================================
# Multi-Stage Dockerfile for Go Reconciliation
# ==========================================

# 1. Build Stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Cache go modules
COPY go.mod go.sum* ./
RUN go mod download || true

# Copy source code
COPY . .

# Ensure dependencies are tidy
RUN go mod tidy

# Build static binary with optimizations
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o /app/bin/go-reconciliation \
    ./cmd/server/main.go

# 2. Production Runtime Stage
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies (tzdata for Asia/Jakarta timezone support)
RUN apk --no-cache add ca-certificates tzdata

# Copy compiled binary from builder
COPY --from=builder /app/bin/go-reconciliation /app/go-reconciliation

# Expose HTTP API port
EXPOSE 8080

# Run binary
ENTRYPOINT ["/app/go-reconciliation"]
