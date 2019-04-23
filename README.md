# flat-manager

flat-manager serves and maintains a Flatpak repository. You point it
at an ostree repository and it will allow Flatpak clients to install
apps from the repository over HTTP. Additionally, it has an HTTP API
that lets you upload new builds and manage the repository.

## Building the server

The server is written in Rust, so you need to have Rust and Cargo
installed. Everything works with the stable version of Rust,
so you can get it from [rustup](https://github.com/rust-lang/rustup.rs)
or your distribution. On Fedora:

    sudo dnf install cargo

PostgreSQL is used for the database, so the Postgres client libraries
need to be installed first. On Fedora, this is done with:

    sudo dnf install postgresql-devel

Then build the server by running:

    cargo build

## Building the client

flat-manager contains a Python-based client that can be used
to talk to the server. To run this, you need Python 3 as
well as the aiohttp packages, installed via pip or the
distribution packages. On Fedora, this can be installed using:

    sudo dnf install python3-aiohttp

## Configuration

flat-manager reads the `config.json` file on startup in the
current directory, although the `REPO_CONFIG` environment variable
can be set to a different file. If you have a `.env` file in the
current directory or one of its parents, it will be read and used
to initialize environment variables.

The source repository contains an `example.env` and an
`example-config.json` that can be used as a basis:

    cp example.env .env
    cp example-config.json config.json
    # edit config.json

## Database

flat-manager uses a PostgreSQL database to store information, and
requires you to specify its address in the configuration file.
The default `example-config.json` points this at:

    "database-url": "postgres://%2Fvar%2Frun%2Fpostgresql/repo",

This is a database called `repo` accessed via the default (at
least on Fedora) UNIX domain socket. To install and start
PostgreSQL, do something like:

    sudo dnf install postgresql-server postgresql-contrib
    sudo systemctl enable postgresql
    sudo postgresql-setup --initdb --unit postgresql
    sudo systemctl start postgresql

And create the `repo` database owned by your user:

    sudo -u postgres createuser $(whoami)
    sudo -u postgres createdb --owner=$(whoami) repo

Note that if you're doing development work, it is important to also
have `DATABASE_URL=...` set in the `.env` file for the Diesel
command-line application to work. This is not required in production
though.

## Repositories

flat-manager maintains a set of repositories specified in the
configuration, as well as a set of dynamically generated repositories
beneath the configured `build-repo-base` path. For testing with
the example configuration, these can be initialized by doing:

    ostree --repo=repo init --mode=archive-z2
    ostree --repo=beta-repo init --mode=archive-z2
    mkdir build-repo

On a deployed system, these should be stored elsewhere, but make sure
they are on the same filesystem so that hardlinks work between them as
otherwise performance will be degraded.

## Tokens

All requests to the API require a token. Token are signed with a secret
that has to be stored on the server. The default configuration contains:

    "secret": "c2VjcmV0"

This is base64 of "secret", so don't use this in production, but it
works for local testing. Otherwise, you can generate one based on
some random data:

    dd bs=256 count=1 if=/dev/random of=/dev/stdout | base64 -w 0

Each token can have various levels of privileges. For example one
could let you do everything, while another would only allow you to
upload builds to a particular build. There is an API to subset
your token for sharing with others (for example sending the above
upload-only token to a builder), but you can also generate a
token with the gentoken command:

    echo -n "secret" | base64 | cargo run --bin gentoken -- --base64 --secret-file - --name testtoken

The above matches the default secret, so can be used for testing.

The client takes tokens via either the `--token` argument or in the
`REPO_TOKEN` environment variable.

## Running

To start the server, run:

    cargo run --bin flat-manager

It will listen on port 8080 by default.

To test adding something to the repository, you can try building a simple
app and exporting it to a repository. This would normally happen on a
different machine than the one serving the repository, but for testing
we can just do it in a subdirectory:

    mkdir test-build
    cd test-build
    wget https://raw.githubusercontent.com/flathub/org.gnome.eog/master/org.gnome.eog.yml
    flatpak-builder --install-deps-from=flathub --repo=local-repo builddir org.gnome.eog.yml
    cd ..

Then we can upload it to the repository by doing (assuming the default secret):

    export REPO_TOKEN=$(echo -n "secret" | base64 | cargo run --bin gentoken -- --base64 --secret-file - --name test)
    ./flat-manager-client push --commit $(./flat-manager-client create http://127.0.0.1:8080 stable) test-build/local-repo

This will create a new "build", upload the build to it and then "commit" the build.

## License

Licensed under either of

- Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license
   ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
