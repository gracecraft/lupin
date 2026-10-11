"""Shared pytest fixtures for the `redis` backend tests (issue #210).

A real `redis-server` subprocess, not a mock -- per #210's test plan. One
server for the whole session (starting it has a real cost); each test gets
a clean keyspace via `flush_redis` instead of a fresh server.
"""

from __future__ import annotations

import socket
import subprocess
import time

import pytest
import redis as redis_lib


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="session")
def redis_port():
    port = _free_port()
    proc = subprocess.Popen(
        [
            "redis-server",
            "--port", str(port),
            "--bind", "127.0.0.1",
            "--save", "",
            "--appendonly", "no",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = redis_lib.Redis(host="127.0.0.1", port=port)
    deadline = time.monotonic() + 10
    up = False
    while time.monotonic() < deadline:
        try:
            up = client.ping()
            break
        except redis_lib.exceptions.ConnectionError:
            time.sleep(0.05)
    if not up:
        proc.terminate()
        proc.wait(timeout=5)
        raise RuntimeError("redis-server did not come up in time")
    try:
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture
def flush_redis(redis_port):
    client = redis_lib.Redis(host="127.0.0.1", port=redis_port)
    client.flushall()
    yield
    client.flushall()


@pytest.fixture
def no_eval_kw(auth_redis_port):
    """Connection kwargs for a user that can only GET, SET and PING. EVAL and SCAN are denied."""
    admin = redis_lib.Redis(host="127.0.0.1", port=auth_redis_port, password="test-pass")
    admin.execute_command("ACL", "SETUSER", "no-eval", "on", ">no-eval-pw", "~lupin:*", "+get", "+set", "+ping")
    yield {"redis_username": "no-eval", "redis_password": "no-eval-pw"}
    admin.execute_command("ACL", "DELUSER", "no-eval")


@pytest.fixture
def no_set_kw(auth_redis_port):
    """Connection kwargs for a user that can only GET and PING. SET and EVAL are denied."""
    admin = redis_lib.Redis(host="127.0.0.1", port=auth_redis_port, password="test-pass")
    admin.execute_command("ACL", "SETUSER", "no-set", "on", ">no-set-pw", "~lupin:*", "+get", "+ping")
    yield {"redis_username": "no-set", "redis_password": "no-set-pw"}
    admin.execute_command("ACL", "DELUSER", "no-set")


@pytest.fixture
def no_scan_kw(auth_redis_port):
    """Connection kwargs for a user that can run everything except SCAN."""
    admin = redis_lib.Redis(host="127.0.0.1", port=auth_redis_port, password="test-pass")
    admin.execute_command("ACL", "SETUSER", "no-scan", "on", ">no-scan-pw", "~*", "+@all", "-scan")
    yield {"redis_username": "no-scan", "redis_password": "no-scan-pw"}
    admin.execute_command("ACL", "DELUSER", "no-scan")


@pytest.fixture
def closed_port() -> int:
    """A TCP port nothing listens on -- exercises the unreachable-redis path."""
    return _free_port()


class CountingRedis:
    """Wraps a real client. Records each method the code under test calls.

    A pipeline's `execute()` is recorded as `execute`. Each `execute()` is
    one round trip, so `calls` shows the round trips.
    """

    def __init__(self, client):
        self.client = client
        self.calls: list[str] = []

    def __getattr__(self, name):
        attr = getattr(self.client, name)

        def counted(*args, **kwargs):
            self.calls.append(name)
            result = attr(*args, **kwargs)
            if name == "pipeline":
                return _CountingPipeline(result, self.calls)
            return result

        return counted


class _CountingPipeline:
    def __init__(self, pipe, calls: list[str]):
        self.pipe = pipe
        self.calls = calls

    def __getattr__(self, name):
        attr = getattr(self.pipe, name)
        if name != "execute":
            return attr

        def execute(*args, **kwargs):
            self.calls.append("execute")
            return attr(*args, **kwargs)

        return execute


@pytest.fixture
def counting_redis():
    """The `CountingRedis` class. Call it with a client to wrap that client."""
    return CountingRedis


@pytest.fixture
def no_client_retry(monkeypatch):
    """Make `slots_redis._client` return a client that does not retry.

    A default client took about four seconds to report a refused login in a local run.
    Use this fixture in tests that only check a refusal.
    """
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    from lupin import slots_redis

    def client(redis_host, redis_port, redis_username=None, redis_password=None):
        return redis_lib.Redis(
            host=redis_host or "localhost",
            port=redis_port or 6379,
            username=redis_username,
            password=redis_password,
            socket_connect_timeout=slots_redis.CONNECT_TIMEOUT,
            socket_timeout=slots_redis.CONNECT_TIMEOUT,
            decode_responses=True,
            retry=Retry(NoBackoff(), 0),
        )

    monkeypatch.setattr(slots_redis, "_client", client)


@pytest.fixture(scope="session")
def auth_redis_port():
    """A separate server from `redis_port`, with `requirepass` set -- tests
    the username/password plumbing (jesus's real Redis needs ACL auth, see
    docs/redis-schema.md), without adding auth to the shared no-auth server
    every other test in this file relies on.
    """
    port = _free_port()
    proc = subprocess.Popen(
        [
            "redis-server",
            "--port", str(port),
            "--bind", "127.0.0.1",
            "--save", "",
            "--appendonly", "no",
            "--requirepass", "test-pass",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = redis_lib.Redis(host="127.0.0.1", port=port, password="test-pass")
    deadline = time.monotonic() + 10
    up = False
    while time.monotonic() < deadline:
        try:
            up = client.ping()
            break
        except redis_lib.exceptions.ConnectionError:
            time.sleep(0.05)
    if not up:
        proc.terminate()
        proc.wait(timeout=5)
        raise RuntimeError("redis-server did not come up in time")
    try:
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture
def make_checkout(tmp_path):
    """Return a function that makes a real git checkout with an `origin` remote.

    The checkout has one commit on `main`, and `origin/HEAD` points to
    `origin/main`. The bare `origin` repo is under `tmp_path/origins`.
    """

    def git(cwd, *args):
        subprocess.run(
            [
                "git", "-c", "user.name=test", "-c", "user.email=test@example.com",
                "-c", "commit.gpgsign=false", *args,
            ],
            cwd=cwd, check=True, capture_output=True,
        )

    def make(path):
        origin = tmp_path / "origins" / f"{path.name}.git"
        origin.parent.mkdir(parents=True, exist_ok=True)
        path.parent.mkdir(parents=True, exist_ok=True)
        git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
        git(tmp_path, "clone", "-q", str(origin), str(path))
        (path / "README.md").write_text("hello\n", encoding="utf-8")
        git(path, "add", "README.md")
        git(path, "commit", "-q", "-m", "first commit")
        git(path, "push", "-q", "origin", "main")
        git(path, "remote", "set-head", "origin", "--auto")
        return path

    return make
