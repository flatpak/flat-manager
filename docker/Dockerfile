# Two-staged build
# 1. Use a container with rust to build the executable
# 2. Copy the executable from that container to actual one.
#
# alpine is used, since it's small.
# alpine/edge is used, since it's flatpak and rust are modern. 

FROM alpine:edge AS builder
WORKDIR /root
RUN apk add cargo rust
RUN apk add postgresql-dev
RUN cargo --quiet install --git https://github.com/flatpak/flat-manager.git --tag 0.3.4 --root=/root
RUN cd /root/.cargo/git/checkouts/flat-manager*/* && \
    install -m 644 -D -t /root/etc example-config.json

#################################################################################

FROM alpine:edge

# Run time dependencies
RUN apk add libpq # postgres runtime dependency
RUN apk add flatpak

# Copy the executable(s) from builder
COPY --from=builder /root/bin/ /usr/bin
COPY --from=builder /root/etc /etc/flat-manager


# A setup script
ADD entrypoint.sh /usr/bin
RUN chown root:root /usr/bin/entrypoint.sh && chmod 755 /usr/bin/entrypoint.sh

# Create needed directories
RUN mkdir -p /var/run/postgresql

ENV HOME /var/run/flat-manager
ENV REPO_CONFIG $HOME/config.json
ENV RUST_LOG info

# Add a user
RUN addgroup flatmanager &&\
    adduser -D -G flatmanager -h $HOME -s /sbin/nologin flatmanager

USER flatmanager
# from default config; may change, if config changes
EXPOSE 8080

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
CMD ["flat-manager"]

