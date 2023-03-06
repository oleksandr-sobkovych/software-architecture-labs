# syntax=docker/dockerfile:1

FROM lukemathwalker/cargo-chef:latest-rust-1.67.1 AS chef
WORKDIR /logging-service

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /logging-service/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN apt-get update && apt-get install -y protobuf-compiler
RUN cargo build --release --bin logging-service

FROM debian:bullseye-slim AS runtime
WORKDIR /logging-service
COPY --from=builder /logging-service/target/release/logging-service /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/logging-service"]
