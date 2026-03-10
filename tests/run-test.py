#!/usr/bin/env python3

import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from subprocess import PIPE


def exec(cmd):
    print("Executing", cmd)

    p = subprocess.run(cmd, stdout=PIPE, stderr=sys.stderr)

    if p.returncode != 0:
        raise AssertionError(f"Command `{cmd}` failed with exit code {p.returncode}")

    return p.stdout.decode().strip()


def wait_for_repo_summary(repo_url, timeout_secs=30, poll_interval_secs=0.5):
    deadline = time.time() + timeout_secs
    last_error = None

    while time.time() < deadline:
        all_ready = True
        for path in ("summary", "summary.sig"):
            try:
                with urllib.request.urlopen(f"{repo_url}/{path}", timeout=5) as resp:
                    if resp.status != 200:
                        all_ready = False
                        last_error = f"{path} returned HTTP {resp.status}"
            except (urllib.error.URLError, TimeoutError, OSError) as err:
                all_ready = False
                last_error = err

        if all_ready:
            return
        time.sleep(poll_interval_secs)

    raise AssertionError(
        f"Timed out waiting for repo summary at {repo_url}: {last_error}"
    )


REPO_DIR = "_repo"

# Build the flatpak app
exec(
    [
        "flatpak-builder",
        "--force-clean",
        "--repo",
        REPO_DIR,
        "_flatpak",
        "tests/org.flatpak.FlatManagerCI.yml",
    ]
)

# Generate a flat-manager token
os.environ["REPO_TOKEN"] = exec(
    ["./target/debug/gentoken", "--secret=secret", "--repo=stable"]
)

# Create a new build and save the repo URL
build_repo = exec(
    ["./target/debug/flat-manager-client", "create", "http://127.0.0.1:8080", "stable"]
)

# Push to the upload repo
exec(["./target/debug/flat-manager-client", "push", build_repo, REPO_DIR])

# Commit to the build repo
exec(["./target/debug/flat-manager-client", "commit", "--wait", build_repo])

# Publish to the main repo
exec(["./target/debug/flat-manager-client", "publish", "--wait", build_repo])

# Publish completion can race delayed update-repo jobs that generate summary files.
wait_for_repo_summary("http://127.0.0.1:8080/repo/stable")

# Make sure the app installs successfully
exec(
    [
        "flatpak",
        "remote-add",
        "flat-manager",
        "http://127.0.0.1:8080/repo/stable",
        "--gpg-import=key.gpg",
    ]
)
exec(["flatpak", "update", "-y"])
exec(["flatpak", "install", "-y", "flat-manager", "org.flatpak.FlatManagerCI"])

os.environ["REPO_TOKEN"] = exec(
    [
        "./target/debug/gentoken",
        "--secret=secret",
        "--repo=stable",
        "--scope=tokenmanagement",
    ]
)
exec(["./target/debug/flat-manager-client", "prune", "http://127.0.0.1:8080", "stable"])
