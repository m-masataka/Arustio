FROM rust:1.91 as builder
WORKDIR /app

RUN apt update && apt install -y clang llvm-dev libclang-dev protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY common/Cargo.toml common/Cargo.toml
COPY ufs/Cargo.toml ufs/Cargo.toml
COPY vfs/Cargo.toml vfs/Cargo.toml
COPY metadata/Cargo.toml metadata/Cargo.toml
COPY server/Cargo.toml server/Cargo.toml
COPY command/Cargo.toml command/Cargo.toml

# Copy build scripts and proto files
COPY common/build.rs common/build.rs
COPY common/proto common/proto

# Copy source code
COPY common/src common/src
COPY ufs/src ufs/src
COPY vfs/src vfs/src
COPY metadata/src metadata/src
COPY server/src server/src
COPY command/src command/src

RUN cargo build --release --bin server
RUN cargo build --release -p command

# Create data directory
RUN mkdir -p /data
RUN cp -r /app/target/release/server /usr/local/bin/arustio-server
RUN cp -r /app/target/release/arustio /usr/local/bin/arustio

WORKDIR /app

# Default command (start gRPC server)
CMD ["arustio-server", "--mode", "server", "--addr", "0.0.0.0:50051"]