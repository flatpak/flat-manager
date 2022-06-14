#!/bin/sh

# Generate a GPG key
gpg --quick-gen-key --batch --yes --passphrase="" "flat-manager CI"

# Find the key
KEY=$(gpg --list-signatures --with-colons | grep "sig" | grep "flat-manager CI" | head -n 1 | awk -F":" '{print $5}')

# Export the key so we can import it into flatpak
gpg --export --armor > key.gpg

# Set the key in the config file
sed --in-place "s/@GPG_KEY@/$KEY/g" test-config.json