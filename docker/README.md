# Forewords

In this simple setup, both flat-manager and postgres are alpine based to keep things small.
A socket is shared between the containers for simplicity. This, however, won't work across machines.

# Set up storage
    POSTGRES_RUNDIR=/path/to/some/storage/postgres-run
    POSTGRES_DB=/path/to/some/storage/postgres-db
    FLATMAN_REPO=/path/to/some/storage/flatman-repo

    mkdir -p $POSTGRES_RUNDIR
    mkdir -p $POSTGRES_DB
    mkdir -p $FLATMAN_REPO

# Run postgres in one terminal
    docker run --rm \
        -v $POSTGRES_RUNDIR:/var/run/postgresql \
        -e POSTGRES_PASSWORD=mysecretpasspassword \
        -e POSTGRES_USER=flatmanager \
        -e POSTGRES_DB=repo \
        --name flat-manager-postgres postgres:alpine

# Flat-manager
## Build docker image
    docker build --tag yourname/flat-manager:latest .
## Run image
    docker run --rm \
        -v $FLATMAN_REPO:/var/run/flat-manager \
        -v $POSTGRES_RUNDIR:/var/run/postgresql \
        -p 8080:8080  yourname/flat-manager:latest

At this point, you'll need to stop the container again and edit `$FLATMAN_REPO/config.js`. This is the unmodified default config. It would seem you need to tell `flat-manager` to listen on all interfaces to make forwarding work, namely:

```json
"host": "0.0.0.0",
```

When running `yourname/flat-manager:latest` again, you should be able to access http://localhost:8080/status from your browser.
