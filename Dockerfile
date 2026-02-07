FROM rust:1.91 as builder
WORKDIR /app

RUN apt update && apt install -y clang llvm-dev libclang-dev protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy src to build dependencies
RUN mkdir -p src && echo "fn main(){}" > src/main.rs

# BuildKit cache mounts for dependencies
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release

# Copy actual source code
COPY build.rs ./
COPY src ./src
COPY proto ./proto

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --bin arustio \
    && cp /app/target/release/arustio /tmp/arustio

############################
# Runtime
############################
# FROM debian:bookworm-slim AS runtime
FROM debian:trixie-slim AS runtime
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /tmp/arustio /usr/local/bin/arustio

# Default command (start gRPC server)
CMD ["arustio-server", "--mode", "server", "--addr", "0.0.0.0:50051"]