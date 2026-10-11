"""End-to-end tests for the command-queue poll loop (issue #28, aligned to
#27's full design): enqueue via `commands.py`, then
poll/claim/execute(mocked)/result via `agent.py`, against the real
`redis-server` fixtures in `conftest.py`.

`subprocess.run` is mocked. These tests do not start Herdr or systemd units.
They check signed queue actions and results against Redis.
"""

from __future__ import annotations

import contextlib
import json
import re
import socket
import threading
import time
from types import SimpleNamespace

import pytest
import redis as redis_lib

from lupin import agent, commands, debrief, loop_runtime, slots, slots_redis

KEY = "secret"
ACTOR_KW = {"actor": "grace", "issuer": "test-host"}


def _fake_run(returncode=0, stdout="ok", stderr=""):
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)

    run.calls = calls
    return run


def test_valid_command_runs_as_agent_user_and_produces_ok_result(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.stop", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == [["lupin", "loop", "local-action", "stop", "lupin"]]
    assert touched == [{"id": cmd_id, "state": "ok"}]
    status = commands.get_status(cmd_id, **kw)
    assert status["state"] == "ok"
    assert status["host"] == "jesus"
    assert status["exit_code"] == 0
    assert status["output"] == "ok"
    assert status["truncated"] is False
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    assert raw.zrange("lupin:v1:cmdq:jesus", 0, -1) == []


def test_loop_run_returns_after_starting_lupin_worker(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.run", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == [["lupin", "run", "lupin"]]
    assert touched == [{"id": cmd_id, "state": "ok"}]
    status = commands.get_status(cmd_id, **kw)
    assert status["state"] == "ok"
    assert status["output"] == "ok"



@pytest.mark.parametrize(
    "action, params, expected",
    [
        (
            "loop.run",
            {"repo": "lupin", "platform": "omp", "note": "review\nhandoff", "resume": True},
            ["lupin", "run", "lupin", "--platform", "omp", "--note", "review\nhandoff", "--resume"],
        ),
        (
            "loop.run",
            {
                "repo": "lupin",
                "platform": "omp",
                "provider": "openai",
                "model": "openai/gpt-5.2",
            },
            [
                "lupin", "run", "lupin", "--platform", "omp",
                "--provider", "openai", "--model", "openai/gpt-5.2",
            ],
        ),
        (
            "loop.run-all",
            {"note": "review"},
            ["lupin", "run", "--all", "--note", "review"],
        ),
        (
            "loop.run-all",
            {
                "platform": "omp",
                "provider": "opencode-go",
                "model": "opencode-go/step-5-preview-free:xhigh",
            },
            [
                "lupin", "run", "--all", "--platform", "omp", "--provider", "opencode-go",
                "--model", "opencode-go/step-5-preview-free:xhigh",
            ],
        ),
        (
            "repo.enable",
            {"repo": "lupin"},
            ["lupin", "enable", "lupin"],
        ),
    ],
)
def test_supported_remote_actions_build_validated_argv(
    redis_port, flush_redis, monkeypatch, action, params, expected
):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    commands.enqueue("jesus", action, params, key=KEY, **ACTOR_KW, **kw)
    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)

    agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == [expected]


def test_failed_run_reports_nonzero_exit_code(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.stop", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run(returncode=1, stdout="", stderr="boom")
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert touched == [{"id": cmd_id, "state": "failed"}]
    status = commands.get_status(cmd_id, **kw)
    assert status["exit_code"] == 1
    assert status["output"] == "boom"


def test_bad_signature_is_rejected_without_executing(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue(
        "jesus", "loop.stop", {"repo": "lupin"}, key="right-secret", **ACTOR_KW, **kw
    )

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", "wrong-secret", **kw)

    assert fake.calls == []
    assert touched == [{"id": cmd_id, "state": "rejected"}]
    status = commands.get_status(cmd_id, **kw)
    assert status["state"] == "rejected"
    assert "bad signature" in status["reason"]


def test_forged_target_field_is_rejected_without_executing(redis_port, flush_redis, monkeypatch):
    """Defense in depth: even if a command somehow ends up in this machine's
    queue with a `target` field naming a different machine, the explicit
    field check must still catch it -- not just the key-based routing.
    """
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    now = time.time()
    fields = {
        "v": 1, "id": "forged1", "target": "someone-else", "action": "loop.stop",
        "params": {"repo": "lupin"}, "actor": "grace", "issuer": "test-host",
        "issued_at": now, "expires_at": now + 120,
    }
    cmd = {**fields, "sig": commands.sign(fields, KEY)}
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    raw.set("lupin:v1:cmd:forged1", json.dumps(cmd), px=3600000)
    raw.zadd("lupin:v1:cmdq:jesus", {"forged1": commands.now_ms()})

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == []
    assert touched == [{"id": "forged1", "state": "rejected"}]


def test_invalid_repo_format_is_rejected_without_executing(redis_port, flush_redis, monkeypatch):
    """Defense in depth required by the design even though nothing upstream
    validates `repo` yet: anything not matching
    `^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$` must never reach Lupin's argv.
    """
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.stop", {"repo": "; rm -rf /"}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == []
    assert touched == [{"id": cmd_id, "state": "rejected"}]
    status = commands.get_status(cmd_id, **kw)
    assert "invalid repo" in status["reason"]


def test_valid_repo_format_is_passed_to_lupin(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.stop", {"repo": "field-trip_2.0"}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert touched == [{"id": cmd_id, "state": "ok"}]
    assert fake.calls == [["lupin", "loop", "local-action", "stop", "field-trip_2.0"]]


def test_loop_stop_force_is_passed_to_lupin(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    commands.enqueue("jesus", "loop.stop", {"repo": "lupin", "force": True}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == [["lupin", "loop", "local-action", "stop", "lupin", "--force"]]


def test_loop_stop_rejects_a_force_that_is_not_a_boolean(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.stop", {"repo": "lupin", "force": "yes"}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == []
    assert touched == [{"id": cmd_id, "state": "rejected"}]
    assert "force must be true or false" in commands.get_status(cmd_id, **kw)["reason"]


def test_loop_stop_gets_more_time_than_other_actions(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    commands.enqueue("jesus", "loop.stop", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)
    timeouts = []

    def run(argv, **kwargs):
        timeouts.append(kwargs["timeout"])
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(agent.subprocess, "run", run)

    agent.poll_once("jesus", KEY, **kw)

    assert timeouts == [agent.SUBPROCESS_TIMEOUT_S["loop.stop"]]
    assert timeouts[0] > agent.EXEC_TIMEOUT_S


# Worst case for one debrief_client call, in seconds. It is
# agent.DEBRIEF_CALL_WORST_S. docs/redis-schema.md has the derivation.
DEBRIEF_CALL_S = agent.DEBRIEF_CALL_WORST_S
POLL_S = 0.25  # loop_runtime._wait_for_server, time.sleep(0.25)


def test_stop_subprocess_cap_leaves_room_for_agent_redis_calls(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    commands.enqueue("jesus", "loop.stop", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)
    timeouts = []

    def run(argv, **kwargs):
        timeouts.append(kwargs["timeout"])
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(agent.subprocess, "run", run)

    agent.poll_once("jesus", KEY, **kw)

    time_limit = agent.ACTION_TIMEOUT_S["loop.stop"]
    agent_s = agent.COMMAND_READ_WORST_S + agent.AGENT_REDIS_CALLS_ON_STOP * DEBRIEF_CALL_S
    worst = timeouts[0] + agent_s
    assert worst <= time_limit, (
        f"subprocess {timeouts[0]:g}s + agent Redis calls {agent_s:g}s = {worst:g}s. "
        f"time limit is {time_limit:g}s"
    )


def test_stop_time_limit_covers_the_listed_timeouts():
    # Each term is one worst-case wait, in seconds, on the non-forced stop path.
    # Subprocess terms run inside the stop subprocess, one after another.
    # Agent terms run in agent.py, outside the subprocess.
    # The test cannot see a timeout that is not listed here. Add new ones here.
    subprocess_terms = [
        # Ensure the server (loop_runtime.py:784, 772, 775, 776, 758-763, 791, 794)
        ("server: running check", loop_runtime.HERDR_TIMEOUT),
        ("server: running check under the machine lock", loop_runtime.HERDR_TIMEOUT),
        ("server: systemd-run start", loop_runtime.HERDR_TIMEOUT),
        ("server: running check after failed start", loop_runtime.HERDR_TIMEOUT),
        ("server: wait for server", loop_runtime.SERVER_START_TIMEOUT + loop_runtime.HERDR_TIMEOUT + POLL_S),
        # Find the workspace (loop_runtime.py:288, 293)
        ("workspace list", loop_runtime.HERDR_TIMEOUT),
        ("pane list fallback", loop_runtime.HERDR_TIMEOUT),
        # Ask for a handoff (loop_runtime.py:305, 1093-1096)
        ("agent list", loop_runtime.HERDR_TIMEOUT),
        ("agent prompt --wait", loop_runtime.HANDOFF_GRACE_S + loop_runtime.HERDR_TIMEOUT),
        # Save report, close, stop worker (loop_runtime.py:1062, 1127, 1128, 1131-1132)
        ("pane read for report", loop_runtime.PANE_READ_TIMEOUT),
        ("workspace close", loop_runtime.HERDR_TIMEOUT),
        ("systemctl is-active", loop_runtime.SYSTEMCTL_CHECK_TIMEOUT),
        ("systemctl stop", loop_runtime.SYSTEMCTL_STOP_TIMEOUT),
        # Stop a session other than the shared one (loop_runtime.py:1136, 1137)
        ("session: workspace list", loop_runtime.HERDR_TIMEOUT),
        ("session: pane list fallback", loop_runtime.HERDR_TIMEOUT),
        ("session: session stop", loop_runtime.HERDR_TIMEOUT),
        # Debrief, inside the subprocess. All gh calls share one time limit.
        ("debrief: all gh calls (repo view and lists)", debrief.DEBRIEF_TIME_LIMIT_S),
        ("debrief: ledger read (Redis)", DEBRIEF_CALL_S),
        # Known limit: this term assumes one SCAN request and reply. See docs/redis-schema.md.
        ("debrief: claims scan (Redis)", DEBRIEF_CALL_S),
        ("debrief: claims read (Redis)", DEBRIEF_CALL_S),
    ]
    # The command read uses _client. The five calls after it use debrief_client.
    # The claim read runs only when SET NX returns nil. It is listed even when it does not run.
    agent_terms = [
        ("agent: write claim", DEBRIEF_CALL_S),
        ("agent: read claim back", DEBRIEF_CALL_S),
        ("agent: write result", DEBRIEF_CALL_S),
        ("agent: dequeue", DEBRIEF_CALL_S),
        ("agent: audit line", DEBRIEF_CALL_S),
    ]
    assert len(agent_terms) == agent.AGENT_REDIS_CALLS_ON_STOP
    agent_s = agent.COMMAND_READ_WORST_S + sum(seconds for _, seconds in agent_terms)
    subprocess_s = sum(seconds for _, seconds in subprocess_terms)
    total = subprocess_s + agent_s
    time_limit = agent.ACTION_TIMEOUT_S["loop.stop"]
    assert total <= time_limit, (
        f"stop can take {total:.2f}s (subprocess {subprocess_s:.2f}s + agent {agent_s:.2f}s). "
        f"time limit is {time_limit:g}s"
    )


def test_debrief_redis_calls_fit_the_listed_redis_terms(
    tmp_path, monkeypatch, redis_port, flush_redis, counting_redis
):
    checkout = tmp_path / "widgets"
    checkout.mkdir()
    client = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    for number in range(1, 6):
        client.set(f"lupin:v1:claim:acme/widgets#{number}", json.dumps({"host": "jesus", "session": "s", "since": 0}))
    counting = counting_redis(client)
    monkeypatch.setattr(debrief, "_gh", lambda args, cwd=None, timeout=None: (
        {"nameWithOwner": "acme/widgets"} if args[0] == "repo" else []
    ))
    monkeypatch.setattr(debrief.slots_redis, "debrief_client", lambda *_args: counting)

    debrief.write_debrief(tmp_path, "widgets", checkout, "2026-10-02T09:00:00Z")

    # Listed debrief Redis terms: ledger read, claims scan, claims read. Each is one request and reply.
    # Building a batch sends nothing. Its execute() sends the batch.
    listed_s = 3 * DEBRIEF_CALL_S
    round_trips = [name for name in counting.calls if name != "pipeline"]
    assert round_trips == ["xrange", "scan_iter", "execute"], counting.calls
    assert len(round_trips) * DEBRIEF_CALL_S <= listed_s


def test_debrief_client_has_short_timeouts_and_no_redis_retry():
    client = slots_redis.debrief_client(None, None)
    kwargs = client.connection_pool.connection_kwargs
    assert kwargs["socket_timeout"] == slots_redis.DEBRIEF_TIMEOUT_S
    assert kwargs["socket_connect_timeout"] == slots_redis.DEBRIEF_TIMEOUT_S
    # No redis-py retry. _call_with_retry gives the one retry.
    assert client.get_retry().get_retries() == 0


def test_command_takes_five_round_trips(redis_port):
    # A new connection sends the handshake, then the command. The relay
    # counts each command on the wire. agent.REDIS_ROUND_TRIPS sets the
    # worst-case bound. A redis-py change that adds a handshake command fails
    # this test. (INFO commandstats misses one handshake command, so it is not used.)
    commands_seen: list[bytes] = []
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(10)
    threading.Thread(target=_relay, args=(listener, redis_port, 0.0, commands_seen), daemon=True).start()
    try:
        client = slots_redis.debrief_client("127.0.0.1", listener.getsockname()[1])
        client.get("lupin-test:round-trips")
    finally:
        listener.close()
    assert len(commands_seen) == agent.REDIS_ROUND_TRIPS


def test_default_client_retries_match_the_read_bound():
    # agent.COMMAND_READ_WORST_S assumes redis-py's default retry count.
    assert slots_redis._client("127.0.0.1", 1).get_retry().get_retries() == agent.DEFAULT_CLIENT_RETRIES


def _forward(src, dst, delay_s, commands_seen=None):
    with contextlib.suppress(OSError):
        while data := src.recv(65536):
            if commands_seen is not None:
                commands_seen.extend(re.findall(rb"\*\d+\r\n", data))  # one array header per command
            time.sleep(delay_s)
            dst.sendall(data)


def _relay(listener, upstream_port, reply_delay_s, commands_seen=None):
    # Forwards each connection to the server. Each reply waits reply_delay_s first.
    while True:
        try:
            front, _ = listener.accept()
        except OSError:
            return  # the listener was closed
        back = socket.create_connection(("127.0.0.1", upstream_port))
        threading.Thread(target=_forward, args=(front, back, 0.0, commands_seen), daemon=True).start()
        threading.Thread(target=_forward, args=(back, front, reply_delay_s), daemon=True).start()


def test_non_stop_command_survives_a_reply_slower_than_the_stop_bound(redis_port, flush_redis, monkeypatch):
    # Each reply takes 1.2 s. That is over the 1 s bound on loop.stop calls.
    # It is under the 2 s bound on other calls. A loop.peek must still run.
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(10)
    threading.Thread(target=_relay, args=(listener, redis_port, 1.2), daemon=True).start()
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.peek", {"repo": "lupin", "lines": "20"}, key=KEY, **ACTOR_KW, **kw)
    monkeypatch.setattr(agent.subprocess, "run", lambda argv, **_kwargs: SimpleNamespace(returncode=0, stdout="ok", stderr=""))
    try:
        touched = agent.poll_once("jesus", KEY, redis_host="127.0.0.1", redis_port=listener.getsockname()[1])
    finally:
        listener.close()
    assert touched == [{"id": cmd_id, "state": "ok"}]


def test_stop_result_write_failure_leaves_the_claim_until_restart(redis_port, flush_redis, monkeypatch):
    # Known limit: the stop ran, but its result write failed. The result stays
    # "running" until the next start marks it failed as orphaned.
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.stop", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)
    monkeypatch.setattr(agent.subprocess, "run", lambda argv, **_kwargs: SimpleNamespace(returncode=0, stdout="", stderr=""))
    real_write = agent._write_result

    def failing_write(*_args, **_kwargs):
        raise redis_lib.exceptions.ConnectionError("write failed")

    monkeypatch.setattr(agent, "_write_result", failing_write)
    with pytest.raises(slots.CoordinatorUnreachable):
        agent.poll_once("jesus", KEY, **kw)
    monkeypatch.setattr(agent, "_write_result", real_write)

    assert commands.get_status(cmd_id, **kw)["state"] == "running"
    assert agent.startup_scan("jesus", **kw) == [cmd_id]
    assert commands.get_status(cmd_id, **kw)["state"] == "failed"


def _hold_connections(server, held):
    while True:
        try:
            conn, _ = server.accept()
        except OSError:
            return  # the server socket was closed
        held.append(conn)


def test_debrief_call_on_a_stalled_server_ends_inside_its_budget():
    # Stall: the server accepts each connection and never replies.
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(10)
    held: list[socket.socket] = []
    threading.Thread(target=_hold_connections, args=(server, held), daemon=True).start()
    client = slots_redis.debrief_client("127.0.0.1", server.getsockname()[1])
    try:
        start = time.monotonic()
        with pytest.raises((redis_lib.exceptions.ConnectionError, redis_lib.exceptions.TimeoutError)):
            slots_redis._call_with_retry(lambda: client.get("k"))
        elapsed = time.monotonic() - start
    finally:
        with contextlib.suppress(OSError):
            server.shutdown(socket.SHUT_RDWR)
        server.close()
        for conn in held:
            conn.close()
    # The stall is real: two read timeouts, one per attempt. The budget bounds it.
    assert 2 * slots_redis.DEBRIEF_TIMEOUT_S - 0.5 <= elapsed <= agent.DEBRIEF_CALL_WORST_S


def test_command_for_a_different_machine_is_never_picked_up(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    commands.enqueue("ralpha", "loop.stop", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert touched == []
    assert fake.calls == []


def test_expired_command_is_marked_expired_and_never_executes(redis_port, flush_redis, monkeypatch):
    """Past `expires_at` by more than the clock-skew allowance (30s) -- a
    short `pickup_window` alone isn't enough to prove this, since the skew
    allowance covers a few seconds of staleness on purpose.
    """
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    now = time.time()
    fields = {
        "v": 1, "id": "expired1", "target": "jesus", "action": "loop.stop",
        "params": {"repo": "lupin"}, "actor": "grace", "issuer": "test-host",
        "issued_at": now - 200, "expires_at": now - 80,  # 80s past expiry, well beyond the 30s skew
    }
    cmd = {**fields, "sig": commands.sign(fields, KEY)}
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    raw.set("lupin:v1:cmd:expired1", json.dumps(cmd), px=3600000)
    raw.zadd("lupin:v1:cmdq:jesus", {"expired1": commands.now_ms()})
    cmd_id = "expired1"

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == []
    assert touched == [{"id": cmd_id, "state": "expired"}]
    status = commands.get_status(cmd_id, **kw)
    assert status["state"] == "expired"


def test_clock_skew_allowance_lets_a_just_expired_command_still_run(redis_port, flush_redis, monkeypatch):
    """A command past its nominal `expires_at` but within the 30s skew
    allowance must still execute -- the allowance exists precisely so a
    small clock difference between sender and executor doesn't reject a
    command that's actually still fresh.
    """
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    now = time.time()
    fields = {
        "v": 1, "id": "skew1", "target": "jesus", "action": "loop.stop",
        "params": {"repo": "lupin"}, "actor": "grace", "issuer": "test-host",
        "issued_at": now - 130, "expires_at": now - 10,  # 10s past nominal expiry, well within 30s skew
    }
    cmd = {**fields, "sig": commands.sign(fields, KEY)}
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    raw.set("lupin:v1:cmd:skew1", json.dumps(cmd), px=3600000)
    raw.zadd("lupin:v1:cmdq:jesus", {"skew1": commands.now_ms()})

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert touched == [{"id": "skew1", "state": "ok"}]
    assert len(fake.calls) == 1


def test_double_claim_only_runs_once(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.stop", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)

    calls = []
    lock = threading.Lock()

    def slow_run(argv, **kwargs):
        with lock:
            calls.append(argv)
        time.sleep(0.2)  # widen the race window between claim and finish
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(agent.subprocess, "run", slow_run)

    results = []

    def poll():
        results.append(agent.poll_once("jesus", KEY, **kw))

    t1 = threading.Thread(target=poll)
    t2 = threading.Thread(target=poll)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert len(calls) == 1
    states = [r["state"] for batch in results for r in batch if r["id"] == cmd_id]
    assert states.count("ok") == 1
    final = commands.get_status(cmd_id, **kw)
    assert final["state"] == "ok"


class _ReplyLostOnce:
    # Wraps a redis client. The first claim (SET NX on the result key)
    # applies on the server. Its reply is then lost as a timeout.
    def __init__(self, client, res_key):
        self._client = client
        self._res_key = res_key
        self.lost = False

    def set(self, name, value, *args, **kwargs):
        applied = self._client.set(name, value, *args, **kwargs)
        if not self.lost and name == self._res_key and kwargs.get("nx"):
            self.lost = True
            raise redis_lib.exceptions.TimeoutError("reply lost after the claim applied")
        return applied

    def __getattr__(self, attr):
        return getattr(self._client, attr)


def test_claim_whose_reply_was_lost_still_runs_once(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.stop", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)
    wrapper = _ReplyLostOnce(agent.debrief_client("127.0.0.1", redis_port), commands.res_key(cmd_id))
    monkeypatch.setattr(agent, "debrief_client", lambda *_args, **_kwargs: wrapper)
    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert wrapper.lost
    assert len(fake.calls) == 1
    assert touched == [{"id": cmd_id, "state": "ok"}]
    assert commands.get_status(cmd_id, **kw)["state"] == "ok"


def test_claim_held_by_another_token_is_lost_race_and_not_run(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.stop", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    raw.set(commands.res_key(cmd_id), json.dumps({"id": cmd_id, "state": "running", "claim": "another-process"}))
    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)

    result = agent._process_one(
        agent._client("127.0.0.1", redis_port), agent.debrief_client("127.0.0.1", redis_port), "jesus", KEY, cmd_id
    )

    assert result == {"id": cmd_id, "state": "lost-race"}
    assert fake.calls == []


def test_startup_scan_marks_orphaned_running_entry_as_failed(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.stop", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)

    # Simulate a previous `lupin agent` process that claimed this command
    # and crashed before finishing: cmdres says "running", but the id is
    # still sitting in the queue (never reached the dequeue step).
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    raw.set(
        f"lupin:v1:cmdres:{cmd_id}",
        json.dumps({"id": cmd_id, "state": "running", "host": "jesus"}),
    )

    marked = agent.startup_scan("jesus", **kw)
    assert marked == [cmd_id]

    status = commands.get_status(cmd_id, **kw)
    assert status["state"] == "failed"
    assert raw.zrange("lupin:v1:cmdq:jesus", 0, -1) == []

    # Not silently re-run: a poll after the scan sees nothing left to do.
    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)
    touched = agent.poll_once("jesus", KEY, **kw)
    assert touched == []
    assert fake.calls == []


def test_startup_scan_ignores_running_entries_for_other_machines(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.stop", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    raw.set(
        f"lupin:v1:cmdres:{cmd_id}",
        json.dumps({"id": cmd_id, "state": "running", "host": "some-other-host"}),
    )

    marked = agent.startup_scan("jesus", **kw)
    assert marked == []
    status = commands.get_status(cmd_id, **kw)
    assert status["state"] == "running"


def test_run_forever_polls_while_a_periodic_check_is_slow(monkeypatch):
    class Stop(Exception):
        pass

    started = threading.Event()
    finished = threading.Event()
    release = threading.Event()
    polls = []
    seen = []

    def slow_check():
        started.set()
        release.wait(10)
        finished.set()

    def poll_once(machine, key, **kwargs):
        polls.append(machine)
        if len(polls) == 3:
            # The check must be running now. Polls do not wait for it.
            seen.append(started.wait(10) and not finished.is_set())
            raise Stop()
        return []

    monkeypatch.setattr(agent, "startup_scan", lambda machine, **kwargs: [])
    monkeypatch.setattr(agent, "poll_once", poll_once)
    monkeypatch.setattr(loop_runtime, "write_due_periodic_debriefs", slow_check)

    try:
        with pytest.raises(Stop):
            agent.run_forever("jesus", KEY, poll_interval=0)
    finally:
        release.set()

    assert len(polls) == 3
    assert seen == [True]


def test_periodic_check_failure_does_not_stop_the_loop(capsys):
    class Stop(BaseException):
        pass

    calls = []

    def check():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("boom")
        raise Stop()

    with pytest.raises(Stop):
        agent._run_periodic_debriefs(check, 0)

    assert len(calls) == 2
    assert "lupin agent: periodic debrief check failed: boom" in capsys.readouterr().err


def test_poll_once_unreachable_redis_raises(closed_port):
    kw = {"redis_host": "127.0.0.1", "redis_port": closed_port}
    with pytest.raises(slots.CoordinatorUnreachable):
        agent.poll_once("jesus", KEY, **kw)


def test_startup_scan_unreachable_redis_raises(closed_port):
    kw = {"redis_host": "127.0.0.1", "redis_port": closed_port}
    with pytest.raises(slots.CoordinatorUnreachable):
        agent.startup_scan("jesus", **kw)


def test_unknown_action_is_rejected_without_executing(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.frobnicate", {}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == []
    assert touched == [{"id": cmd_id, "state": "rejected"}]


def test_missing_required_param_is_rejected_without_executing(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.stop", {}, key=KEY, **ACTOR_KW, **kw)  # no 'repo'

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == []
    assert touched == [{"id": cmd_id, "state": "rejected"}]
    status = commands.get_status(cmd_id, **kw)
    assert status["state"] == "rejected"



def testvalidate_cal_expr_rejects_newline():
    with pytest.raises(agent.RejectedCommand):
        agent.validate_cal_expr("*-*-* 00:00:00\n[Service]\nExecStart=rm -rf /")


def testvalidate_cal_expr_rejects_control_char():
    with pytest.raises(agent.RejectedCommand):
        agent.validate_cal_expr("*-*-* 00\x0000:00")


def testvalidate_cal_expr_rejects_overlength():
    with pytest.raises(agent.RejectedCommand):
        agent.validate_cal_expr("x" * (agent._CAL_EXPR_MAX_LEN + 1))


def testvalidate_cal_expr_accepts_a_normal_expression():
    assert agent.validate_cal_expr("*-*-* 00/5:00:00") == "*-*-* 00/5:00:00"


def test_loop_peek_runs_local_action_directly(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.peek", {"repo": "lupin", "lines": "20"}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run(returncode=0, stdout="pane text")
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == [["lupin", "loop", "local-action", "peek", "lupin", "20"]]
    assert touched == [{"id": cmd_id, "state": "ok"}]


def test_loop_peek_defaults_lines_to_sixty(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    commands.enqueue("jesus", "loop.peek", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)

    agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == [["lupin", "loop", "local-action", "peek", "lupin", "60"]]

def test_loop_send_runs_local_action_with_the_text(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue(
        "jesus", "loop.send", {"repo": "lupin", "text": "yes, keep going"}, key=KEY, **ACTOR_KW, **kw
    )
    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == [["lupin", "loop", "local-action", "send", "lupin", "yes, keep going"]]
    assert touched == [{"id": cmd_id, "state": "ok"}]


@pytest.mark.parametrize("text", ["", "   ", "one\ntwo", "tab\there", "x" * 2001, None, 5])
def test_loop_send_rejects_bad_text(text):
    with pytest.raises(agent.RejectedCommand):
        agent.ACTIONS["loop.send"]({"repo": "lupin", "text": text}, "id")


def test_loop_send_is_not_allowed_while_draining():
    assert "loop.send" not in agent.DRAIN_ALLOWED


def test_loop_state_uses_herdr_reported_state(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.state", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)
    herdr_state = '{"state":"running","backend":"herdr","session":"lupin"}'
    fake = _fake_run(returncode=0, stdout=herdr_state)
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == [["lupin", "loop", "local-action", "state", "lupin"]]
    assert touched == [{"id": cmd_id, "state": "ok"}]
    assert commands.get_status(cmd_id, **kw)["output"] == herdr_state
    assert "loop.state" in agent.DRAIN_ALLOWED


def test_schedule_show_runs_lupin_directly_without_systemd_run(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    commands.enqueue("jesus", "schedule.show", {}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)

    agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == [["lupin", "schedule"]]


def test_schedule_set_cal_runs_lupin_after_validation(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue(
        "jesus", "schedule.set", {"mode": "cal", "expr": "*-*-* 00/5:00:00"}, key=KEY, **ACTOR_KW, **kw
    )

    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)
    monkeypatch.setattr(agent.shutil, "which", lambda name: None)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == [["lupin", "schedule", "cal", "*-*-* 00/5:00:00"]]
    assert touched == [{"id": cmd_id, "state": "ok"}]


def test_schedule_set_cal_with_newline_is_rejected_without_executing(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue(
        "jesus", "schedule.set",
        {"mode": "cal", "expr": "*-*-* 00:00:00\n[Service]\nExecStart=rm -rf /"},
        key=KEY, **ACTOR_KW, **kw,
    )

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == []
    assert touched == [{"id": cmd_id, "state": "rejected"}]


def test_schedule_set_cal_rejected_when_systemd_analyze_says_invalid(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue(
        "jesus", "schedule.set", {"mode": "cal", "expr": "not a real expression"}, key=KEY, **ACTOR_KW, **kw
    )

    fake = _fake_run()
    monkeypatch.setattr(agent.shutil, "which", lambda name: "/usr/bin/systemd-analyze")
    monkeypatch.setattr(
        agent.subprocess, "run",
        lambda argv, **kwargs: SimpleNamespace(returncode=1, stdout="", stderr="bad")
        if argv[:2] == ["/usr/bin/systemd-analyze", "calendar"] else fake(argv, **kwargs),
    )

    touched = agent.poll_once("jesus", KEY, **kw)

    assert touched == [{"id": cmd_id, "state": "rejected"}]
    assert fake.calls == []


def test_schedule_set_first_runs_lupin(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue(
        "jesus", "schedule.set", {"mode": "first", "when": "+2h5m", "interval": "5h15m"},
        key=KEY, **ACTOR_KW, **kw,
    )

    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == [["lupin", "schedule", "first", "+2h5m", "every", "5h15m"]]
    assert touched == [{"id": cmd_id, "state": "ok"}]


def test_schedule_pause_and_resume_run_as_agent_user(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    pause_id = commands.enqueue("jesus", "schedule.pause", {}, key=KEY, **ACTOR_KW, **kw)
    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)
    pause_touched = agent.poll_once("jesus", KEY, batch=1, **kw)

    resume_id = commands.enqueue("jesus", "schedule.resume", {}, key=KEY, **ACTOR_KW, **kw)
    resume_touched = agent.poll_once("jesus", KEY, batch=1, **kw)

    assert fake.calls == [["lupin", "pause"], ["lupin", "resume"]]
    assert pause_touched == [{"id": pause_id, "state": "ok"}]
    assert resume_touched == [{"id": resume_id, "state": "ok"}]


@pytest.mark.parametrize(
    "action, params",
    [
        ("loop.run", {"repo": "lupin"}),
        ("loop.run-all", {}),
    ],
)
def test_draining_machine_rejects_loop_start_without_executing(
    redis_port, flush_redis, monkeypatch, action, params
):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    raw.set("lupin:v1:machine:jesus", json.dumps({"state": "draining"}))
    cmd_id = commands.enqueue("jesus", action, params, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == []
    assert touched == [{"id": cmd_id, "state": "rejected"}]
    status = commands.get_status(cmd_id, **kw)
    assert "draining" in status["reason"]

def test_draining_machine_rejects_schedule_set_without_executing(redis_port, flush_redis, monkeypatch):
    # schedule.set arms a timer to start a loop later -- draining blocks it
    # the same as loop.run (see DRAIN_ALLOWED's comment in agent.py).
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    raw.set("lupin:v1:machine:jesus", json.dumps({"state": "draining"}))
    cmd_id = commands.enqueue(
        "jesus", "schedule.set", {"mode": "first", "when": "+1h", "interval": "1h"}, key=KEY, **ACTOR_KW, **kw
    )

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == []
    assert touched == [{"id": cmd_id, "state": "rejected"}]


def test_draining_machine_rejects_schedule_resume_without_executing(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    raw.set("lupin:v1:machine:jesus", json.dumps({"state": "draining"}))
    cmd_id = commands.enqueue("jesus", "schedule.resume", {}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run()
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert fake.calls == []
    assert touched == [{"id": cmd_id, "state": "rejected"}]


def test_draining_machine_still_runs_loop_stop(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    raw.set("lupin:v1:machine:jesus", json.dumps({"state": "draining"}))
    cmd_id = commands.enqueue("jesus", "loop.stop", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert touched == [{"id": cmd_id, "state": "ok"}]
    assert len(fake.calls) == 1


def test_draining_machine_still_runs_peek_show_and_pause(redis_port, flush_redis, monkeypatch):
    # Read-only actions, and schedule.pause (winds a timer down, same
    # shape as loop.stop) -- all three are in DRAIN_ALLOWED.
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    raw.set("lupin:v1:machine:jesus", json.dumps({"state": "draining"}))

    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)

    for action, params in (("loop.peek", {"repo": "lupin"}), ("schedule.show", {}), ("schedule.pause", {})):
        cmd_id = commands.enqueue("jesus", action, params, key=KEY, **ACTOR_KW, **kw)
        touched = agent.poll_once("jesus", KEY, **kw)
        assert touched == [{"id": cmd_id, "state": "ok"}], action


def test_non_draining_machine_runs_loop_run_normally(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    raw.set("lupin:v1:machine:jesus", json.dumps({"state": "online"}))
    cmd_id = commands.enqueue("jesus", "loop.run", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)

    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)

    touched = agent.poll_once("jesus", KEY, **kw)

    assert touched == [{"id": cmd_id, "state": "ok"}]
    assert len(fake.calls) == 1


def test_poll_once_writes_cmdlog_entries_for_terminal_outcomes(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    cmd_id = commands.enqueue("jesus", "loop.stop", {"repo": "lupin"}, key=KEY, **ACTOR_KW, **kw)

    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    before = raw.xrange("lupin:v1:cmdlog")
    assert len(before) == 1  # the enqueue event
    assert before[0][1]["event"] == "enqueued"

    fake = _fake_run(returncode=0)
    monkeypatch.setattr(agent.subprocess, "run", fake)
    agent.poll_once("jesus", KEY, **kw)

    after = raw.xrange("lupin:v1:cmdlog")
    assert len(after) == 2  # enqueue + terminal outcome
    assert after[1][1]["id"] == cmd_id
    assert after[1][1]["state"] == "ok"
