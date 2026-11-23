#!/usr/bin/env python3

import base64
import hashlib
import hmac
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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


WEBHOOK_ADDRESS = ("127.0.0.1", 9099)
WEBHOOK_KEY_FILE = "webhook-hmac-key"
REDELIVER_URL = "http://127.0.0.1:8080/api/v1/admin/webhooks/redeliver"

JOB_COMMIT = 0
JOB_PUBLISH = 1
JOB_UPDATE_REPO = 2
JOB_PRUNE = 5
JOB_ENDED = 2
REPO_READY = 2
PUBLISHED = 2

PAYLOAD_FIELDS = ["id", "type", "created_at", "data"]
JOB_FIELDS = ["id", "kind", "status", "repo", "results", "start_after"]
BUILD_FIELDS = [
    "id",
    "repo",
    "app_id",
    "repo_state",
    "repo_state_reason",
    "published_state",
    "published_state_reason",
    "commit_job_id",
    "publish_job_id",
]
TIMESTAMP_FORMAT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class Delivery:
    def __init__(self, headers, body):
        self.headers = headers
        self.body = body
        self.status = 200
        self.payload = json.loads(body)

    @property
    def job(self):
        return self.payload["data"]["job"]

    @property
    def build(self):
        return self.payload["data"]["build"]

    @property
    def kind(self):
        return self.job["kind"]


class WebhookSink:
    def __init__(self, address, key):
        self.key = key
        self.deliveries = []
        self.reject_once = set()
        self.changed = threading.Condition()
        sink = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                delivery = Delivery(self.headers, body)
                with sink.changed:
                    if delivery.kind in sink.reject_once:
                        sink.reject_once.remove(delivery.kind)
                        delivery.status = 400
                self.send_response(delivery.status)
                self.send_header("Content-Length", "0")
                self.end_headers()
                with sink.changed:
                    sink.deliveries.append(delivery)
                    sink.changed.notify_all()

        self.server = ThreadingHTTPServer(address, Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def wait_for(self, description, predicate, timeout_secs=60):
        deadline = time.time() + timeout_secs
        with self.changed:
            while True:
                matches = [d for d in self.deliveries if predicate(d)]
                if matches:
                    return matches
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise AssertionError(f"Timed out waiting for {description}")
                self.changed.wait(remaining)


def load_webhook_key():
    with open(WEBHOOK_KEY_FILE) as key_file:
        return base64.b64decode(key_file.read().strip(), validate=True)


def parse_timestamp(value):
    assert TIMESTAMP_FORMAT.match(value), f"Unexpected timestamp format: {value}"
    return datetime.fromisoformat(value)


def verify_delivery(delivery, key):
    expected = "sha256=" + hmac.new(key, delivery.body, hashlib.sha256).hexdigest()
    signature = delivery.headers["X-Flatmanager-Signature"]
    assert hmac.compare_digest(signature, expected), f"Invalid signature {signature}"
    assert delivery.headers["Content-Type"] == "application/json"
    assert delivery.headers["X-Flatmanager-Event"] == "job.finished"

    payload = delivery.payload
    assert list(payload) == PAYLOAD_FIELDS, list(payload)
    assert payload["type"] == "job.finished"
    assert delivery.headers["X-Flatmanager-Delivery"] == str(payload["id"])
    assert abs(time.time() - int(delivery.headers["X-Flatmanager-Timestamp"])) < 300

    created_at = parse_timestamp(payload["created_at"])
    assert abs((datetime.now(UTC) - created_at).total_seconds()) < 300

    assert list(delivery.job) == JOB_FIELDS, list(delivery.job)
    assert delivery.build is None or list(delivery.build) == BUILD_FIELDS


def post_json(url, body, token):
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def redeliver(token):
    before = datetime.now(UTC) + timedelta(hours=1)
    body = {"before": before.strftime("%Y-%m-%dT%H:%M:%SZ")}
    return post_json(REDELIVER_URL, body, token)["queued"]


def check_pipeline_webhooks(sink):
    deliveries = {}
    for name, kind in (
        ("commit", JOB_COMMIT),
        ("publish", JOB_PUBLISH),
        ("update-repo", JOB_UPDATE_REPO),
    ):
        matches = sink.wait_for(f"the {name} webhook", lambda d: d.kind == kind)
        assert len(matches) == 1, f"Expected one {name} webhook, got {len(matches)}"
        verify_delivery(matches[0], sink.key)
        assert matches[0].job["status"] == JOB_ENDED
        deliveries[name] = matches[0]
    commit, publish, update_repo = deliveries.values()

    assert commit.build["repo"] == "stable"
    assert commit.build["commit_job_id"] == commit.job["id"]
    assert commit.build["repo_state"] == REPO_READY
    assert commit.build["published_state"] == 0
    assert json.loads(commit.job["results"])["refs"]

    assert publish.build["id"] == commit.build["id"]
    assert publish.build["publish_job_id"] == publish.job["id"]
    assert publish.build["published_state"] == PUBLISHED
    update_repo_job = json.loads(publish.job["results"])["update-repo-job"]
    assert update_repo_job == update_repo.job["id"]

    assert update_repo.build is None
    assert update_repo.job["repo"] == "stable"
    parse_timestamp(update_repo.job["start_after"])

    assert commit.payload["id"] < publish.payload["id"] < update_repo.payload["id"]
    return commit


REPO_DIR = "_repo"

webhook_key = load_webhook_key()
sink = WebhookSink(WEBHOOK_ADDRESS, webhook_key)
# The first commit webhook is rejected so the redeliver endpoint has work to do.
sink.reject_once.add(JOB_COMMIT)

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
    ["./target/debug/flat-manager-client", "gentoken", "--secret=secret", "--repo=stable"]
)
build_token = os.environ["REPO_TOKEN"]

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

# Each job must have been announced once with a valid signature
rejected_commit = check_pipeline_webhooks(sink)

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
        "./target/debug/flat-manager-client",
        "gentoken",
        "--secret=secret",
        "--repo=stable",
        "--scope=tokenmanagement",
    ]
)

admin_token = os.environ["REPO_TOKEN"]

# Redelivery needs the token management scope
try:
    redeliver(build_token)
    raise AssertionError("Redelivery was allowed without the token management scope")
except urllib.error.HTTPError as err:
    assert err.code == 403, err.code

# The rejected commit webhook is sent again with a new creation time
assert redeliver(admin_token) >= 1
redelivered = sink.wait_for(
    "the redelivered commit webhook",
    lambda d: d.payload["id"] == rejected_commit.payload["id"] and d.status == 200,
)[0]
verify_delivery(redelivered, webhook_key)
assert redelivered.payload["data"] == rejected_commit.payload["data"]
assert parse_timestamp(redelivered.payload["created_at"]) >= parse_timestamp(
    rejected_commit.payload["created_at"]
)

# Once every webhook is accepted there is nothing left to redeliver
for _ in range(20):
    if redeliver(admin_token) == 0:
        break
    time.sleep(0.5)
else:
    raise AssertionError("Redelivery keeps queueing webhooks that were accepted")

exec(["./target/debug/flat-manager-client", "prune", "http://127.0.0.1:8080", "stable"])

prune = sink.wait_for("the prune webhook", lambda d: d.kind == JOB_PRUNE)[0]
verify_delivery(prune, webhook_key)
assert prune.build is None
