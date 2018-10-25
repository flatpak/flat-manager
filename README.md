repo-manager
============

repo-manager serves and maintains a flatpak repostitory. You point it
at an ostree repository and it will allow flatpak clients to install
apps from the repo over http, and additionally it has a http API that
lets you upload new builds and otherwise manage the repository.

Building the server
===================

The server is written in rust, so you need rust and cargo. Everything
works with the stable version of rust, so you can get it from rustup
or your distribution. On fedora:

    sudo dnf install cargo

Postgres is used for the database, so the postgres client libraries
need to be installed first. On fedora this is done with:

    sudo dnf install postgresql-devel

The build the server by running:

    cargo build

Building client
==================

repo-manager contains a python3 based client that can be used
to talk to the server. To run this you need python 3 as
well as the aiohttp packages, installed via pip or the
distribution packages. On fedora this can be installed by:

    sudo dnf install python3-aiohttp

Configuration
=============

All configuration is done via environment variables, but it will
also look for a '.env' file in the current directory or one
of its parents. The source repository contains a version
that should work fine for testing.

Database
--------

repo-manager uses a postgresql database to store information, and
requires you to specity the address to it in the DATABASE_URL
environment variable. The default .env points this at:

    DATABASE_URL=postgres://%2Fvar%2Frun%2Fpostgresql/repo

This is a database called 'repo' accessed via the default (at
least on fedora) unix domain socket. To install and start
postgresql, do something like:

    sudo dnf install postgresql-server postgresql-contrib
    sudo systemctl enable postgresql
    sudo postgresql-setup --initdb --unit postgresql
    sudo systemctl start postgresql

And create the 'repo' database owned by your user:

    sudo -u postgres createuser $(whoami)
    sudo -u postgres createdb --owner=$(whoami) repo

You must the initialize the tables and resources needed
by the service. This is done using the diesel cli app:

    cargo install diesel_cli --no-default-features --features postgres
    # At this point, ensure $HOME/.cargo/bin is in $PATH
    diesel setup

Repos
-----

repo-manager maintains a main repository in a path specified by
$REPO_PATH, and a set of dynamically generated repos beneath
$BUILD_REPO_BASE_PATH.  The default values for these are 'repo' and
'build-repo', which for testing can be initialized by doing:

    ostree --repo=repo init --mode=archive-z2
    mkdir build-repo

On a deployed system these should be stored elsewhere, but make sure
they are on the same filesystem so that hardlinks work between them as
otherwise performance will be degraded.

Running
=======

To start the server run:

    cargo run --bin repo-manager

Which will listen to port 8080 by default.

To test adding something to the repo, you can try building a simple
app and exporting it to a repo. This would normally happen on a
different machine than the one serving the repo, but for testing
we can just do it in a subdirectory:

    mkdir test-build
    cd test-build
    wget https://raw.githubusercontent.com/flathub/org.gnome.eog/master/org.gnome.eog.yml
    flatpak-builder --install-deps-from=flathub --repo=local-repo builddir org.gnome.eog.yml
    cd ..

Then we can upload this to the repo by doing:

    ./repoclient push --commit $(./repoclient create http://127.0.0.1:8080) test-build/local-repo

This will create a new "build" upload the build to it and then "commit" the build.
