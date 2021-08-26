FROM chillfish8/rust-builder:latest as builder

WORKDIR /home/rust/

# Avoid having to install/build all dependencies by copying
# the Cargo files and making a dummy src/main.rs
COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl

# Size optimization
# RUN strip target/x86_64-unknown-linux-musl/release/lnx

# Start building the final image
FROM scratch
WORKDIR /etc/lnx

COPY --from=builder /home/rust/target/x86_64-unknown-linux-musl/release/lnx .
ENTRYPOINT ["./lnx"]