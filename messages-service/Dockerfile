# syntax=docker/dockerfile:1

FROM lukemathwalker/cargo-chef:latest-rust-1.67.1 AS chef
WORKDIR /messages-service

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /messages-service/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN apt-get update && apt-get install -y protobuf-compiler
RUN cargo build --release --bin messages-service

FROM debian:bullseye-slim AS runtime
WORKDIR /messages-service
COPY --from=builder /messages-service/target/release/messages-service /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/messages-service"]
