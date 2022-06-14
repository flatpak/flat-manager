#!/usr/bin/env python3

import os, subprocess, sys, time
from subprocess import PIPE

def sleep(seconds):
    print(f"Waiting {seconds} seconds")
    time.sleep(seconds)

def exec(cmd):
    print("Executing", cmd)

    p = subprocess.run(cmd, stdout=PIPE, stderr=sys.stderr)

    if p.returncode != 0:
        raise AssertionError(f"Command `{cmd}` failed with exit code {p.returncode}")

    return p.stdout.decode().strip()

REPO_DIR = "_repo"

# Build the flatpak app
exec(["flatpak-builder", "--force-clean", "--repo", REPO_DIR, "_flatpak", "tests/org.flatpak.FlatManagerCI.yml"])

# Generate a flat-manager token
os.environ["REPO_TOKEN"] =  exec(["cargo", "run", "--bin=gentoken", "--", "--secret=secret", "--repo=stable"])

# Create a new build and save the repo URL
build_repo = exec(["./flat-manager-client", "create", "http://127.0.0.1:8080", "stable"])

# Push to the upload repo
exec(["./flat-manager-client", "push", build_repo, REPO_DIR])

# Commit to the build repo
exec(["./flat-manager-client", "commit", build_repo])

# Wait for that job to finish
sleep(10)

# Publish to the main repo
exec(["./flat-manager-client", "publish", build_repo])

# Wait for the repository to be updated
sleep(15)

# Make sure the app installs successfully
exec(["flatpak", "remote-add", "flat-manager", "http://127.0.0.1:8080/repo/stable", "--gpg-import=key.gpg"])
exec(["flatpak", "install", "-y", "flat-manager", "org.flatpak.FlatManagerCI"])