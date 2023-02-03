FROM ubuntu:22.04 as builder

RUN apt-get update && apt-get install -y git libpq-dev curl build-essential libgpgme-dev pkg-config libssl-dev libglib2.0-dev libostree-dev
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > rustup.sh && \
    sh rustup.sh -y -q

ADD . /src
RUN cd /src && /root/.cargo/bin/cargo build --release

RUN git clone https://github.com/jameswestman/flathub-hooks.git
RUN cd flathub-hooks && /root/.cargo/bin/cargo build --release

FROM ubuntu:22.04

RUN apt-get update && apt-get install -y flatpak ostree libpq5 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ADD https://github.com/openSUSE/catatonit/releases/download/v0.1.7/catatonit.x86_64 /usr/local/bin/catatonit
RUN chmod +x /usr/local/bin/catatonit

COPY --from=builder /src/target/release/flat-manager /usr/local/bin/flat-manager
COPY --from=builder /src/target/release/delta-generator-client /usr/local/bin/delta-generator-client
COPY --from=builder /flathub-hooks/target/release/flathub-hooks /usr/local/bin/flathub-hooks

ENV RUST_BACKTRACE=1

ENTRYPOINT ["/usr/local/bin/catatonit", "--"]
CMD ["/usr/local/bin/flat-manager"]
