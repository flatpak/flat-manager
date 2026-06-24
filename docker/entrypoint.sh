#!/bin/sh

if [ ! -f $REPO_CONFIG ]; then
    echo No config found, copying example config.
    cp /etc/flat-manager/example-config.json "$REPO_CONFIG"
fi

exec $*
