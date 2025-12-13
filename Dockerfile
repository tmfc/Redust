# Build stage
FROM rust:1.75-alpine AS builder

# Install build dependencies
RUN apk add --no-cache musl-dev

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy main.rs to cache dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies (this layer will be cached)
RUN cargo build --release && rm -rf src

# Copy actual source code
COPY src ./src

# Build the application
RUN touch src/main.rs && cargo build --release

# Runtime stage
FROM alpine:3.19

# Install runtime dependencies
RUN apk add --no-cache ca-certificates netcat-openbsd

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/target/release/redust /usr/local/bin/redust

# Create data directory
RUN mkdir -p /data

# Default environment variables
ENV REDUST_ADDR=0.0.0.0:6379
ENV REDUST_RDB_PATH=/data/redust.rdb

# Expose Redis port
EXPOSE 6379

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD echo "PING" | nc -w 1 localhost 6379 | grep -q "PONG" || exit 1

# Run as non-root user
RUN adduser -D -u 1000 redust && chown -R redust:redust /data
USER redust

# Start the server
CMD ["redust"]
