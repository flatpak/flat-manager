FROM quay.io/fedora/fedora:latest

# Install Rust, Cargo, and run-tests.py's dependencies
RUN dnf install -y rust cargo flatpak flatpak-builder python3-pip python3-gobject ostree-devel libpq-devel
RUN flatpak remote-add flathub https://flathub.org/repo/flathub.flatpakrepo
RUN flatpak install --noninteractive flathub org.freedesktop.Platform//21.08 org.freedesktop.Sdk//21.08
RUN pip install asyncio aiohttp tenacity

# Use the test config.json and configure it with a GPG key
COPY ./tests/config.json ./test-config.json
COPY ./tests/gen-key.sh ./gen-key.sh
RUN ./gen-key.sh

# Initialize the OSTree repo
RUN ostree --repo=repo init --mode=archive-z2
RUN mkdir build-repo

# Copy the files
COPY ./ ./

# Make sure our test config.json is used, not one that was in the source directory
RUN cp --force ./test-config.json ./config.json

# Build flat-manager
RUN cargo build

# Wait 5 seconds for the database to start, then run flat-manager
CMD ["sh", "-c", "sleep 5 ; ./target/debug/flat-manager"]
