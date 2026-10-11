"""Tests for the `local` slot-lease backend (issue #205).

Pytest-style (not unittest), unlike test_route.py/test_classify.py -- these
are new tests, not ported ones, and the signal-handling cases below need
real subprocesses and real `os.kill`, which reads more plainly as plain
functions with `tmp_path` than as TestCase methods.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time

import pytest

from lupin import cli, slots


def test_acquire_respects_max_third_call_is_full(tmp_path, clean_lupin_env):
    root = str(tmp_path)
    assert cli.main(["acquire", "bmo", "--holder", "a", "--max", "2", "--state-root", root]) == 0
    assert cli.main(["acquire", "bmo", "--holder", "b", "--state-root", root]) == 0
    # Third holder on a max-2 slot: full, exit 2.
    assert cli.main(["acquire", "bmo", "--holder", "c", "--state-root", root]) == 2


def test_holder_past_ttl_is_pruned_and_its_spot_freed(tmp_path):
    root = str(tmp_path)
    first = slots.acquire("bmo", "a", ttl=0.05, max_holders=1, state_root=root)
    with pytest.raises(slots.SlotFull):
        slots.acquire("bmo", "b", ttl=10, state_root=root)
    time.sleep(0.15)
    second = slots.acquire("bmo", "b", ttl=10, state_root=root)
    assert second != first
    status = slots.status(state_root=root)
    assert status["bmo"]["holders"] == 1


def test_acquire_wait_succeeds_once_a_holder_expires(tmp_path):
    root = str(tmp_path)
    slots.acquire("bmo", "a", ttl=0.1, max_holders=1, state_root=root)
    start = time.monotonic()
    lease = slots.acquire("bmo", "b", wait=3.0, ttl=5, state_root=root)
    elapsed = time.monotonic() - start
    assert lease.startswith("bmo:")
    assert elapsed < 3.0  # found room once the first holder's TTL passed, not by exhausting wait


def test_release_frees_the_slot_and_is_idempotent(tmp_path):
    root = str(tmp_path)
    lease = slots.acquire("bmo", "a", max_holders=1, state_root=root)
    assert slots.status(state_root=root)["bmo"]["holders"] == 1
    assert slots.release(lease, state_root=root) is True
    assert slots.status(state_root=root)["bmo"]["holders"] == 0
    assert slots.release(lease, state_root=root) is False  # already gone, not an error


def test_release_malformed_lease_id_is_an_error(tmp_path):
    code = cli.main(["release", "--lease", "not-a-lease", "--state-root", str(tmp_path)])
    assert code == 1


def test_status_json_reports_accurate_counts(tmp_path, capsys, clean_lupin_env):
    root = str(tmp_path)
    cli.main(["acquire", "bmo", "--holder", "a", "--max", "2", "--state-root", root])
    cli.main(["acquire", "bmo", "--holder", "b", "--state-root", root])
    capsys.readouterr()

    code = cli.main(["status", "--json", "--state-root", root])
    captured = capsys.readouterr()

    assert code == 0
    assert json.loads(captured.out) == {"bmo": {"holders": 2, "max": 2}}


def test_hold_releases_on_normal_exit(tmp_path):
    root = str(tmp_path)
    code = slots.hold(
        [sys.executable, "-c", "pass"], slot="bmo", holder="a", max_holders=1, ttl=5, state_root=root
    )
    assert code == 0
    assert slots.status(state_root=root)["bmo"]["holders"] == 0


def test_hold_releases_on_nonzero_exit(tmp_path):
    root = str(tmp_path)
    code = slots.hold(
        [sys.executable, "-c", "import sys; sys.exit(3)"],
        slot="bmo",
        holder="a",
        max_holders=1,
        ttl=5,
        state_root=root,
    )
    assert code == 3
    assert slots.status(state_root=root)["bmo"]["holders"] == 0


def test_hold_keeps_renewing_after_repeated_renew_errors(tmp_path, monkeypatch, capsys):
    root = str(tmp_path)
    real_renew = slots.renew
    calls = []

    def renew_fails_twice(lease, **kwargs):
        calls.append(lease)
        if len(calls) in (1, 2):
            raise OSError("renew broke")
        return real_renew(lease, **kwargs)

    monkeypatch.setattr(slots, "renew", renew_fails_twice)
    code = slots.hold(
        [sys.executable, "-c", "import time; time.sleep(0.5)"],
        slot="bmo",
        holder="a",
        max_holders=1,
        ttl=0.3,
        state_root=root,
    )
    assert code == 0
    assert len(calls) >= 3, "renew was not called again after the errors"
    assert capsys.readouterr().err.count("renew broke") == 1
    assert slots.status(state_root=root)["bmo"]["holders"] == 0


def test_hold_reports_a_new_failure_after_a_success(tmp_path, monkeypatch, capsys):
    root = str(tmp_path)
    real_renew = slots.renew
    calls = []

    def renew_fails_first_and_third(lease, **kwargs):
        calls.append(lease)
        if len(calls) in (1, 3):
            raise OSError("renew broke")
        return real_renew(lease, **kwargs)

    monkeypatch.setattr(slots, "renew", renew_fails_first_and_third)
    code = slots.hold(
        [sys.executable, "-c", "import time; time.sleep(0.5)"],
        slot="bmo",
        holder="a",
        max_holders=1,
        ttl=0.3,
        state_root=root,
    )
    assert code == 0
    assert len(calls) >= 3, "renew was not called a third time"
    assert capsys.readouterr().err.count("renew broke") == 2
    assert slots.status(state_root=root)["bmo"]["holders"] == 0


def _run_hold_and_signal_child(tmp_path, root, sig):
    """Start `hold` running a child that reports its own pid, then send it
    `sig` directly (not through `hold`, to prove release runs no matter how
    the child dies -- real SIGTERM/SIGKILL, not an assumption that `finally`
    always fires)."""
    pidfile = tmp_path / "child.pid"
    command = [
        sys.executable,
        "-c",
        f"import os,time\nopen({str(pidfile)!r}, 'w').write(str(os.getpid()))\ntime.sleep(30)\n",
    ]
    outcome: dict = {}

    def runner():
        outcome["code"] = slots.hold(
            command, slot="bmo", holder="a", max_holders=1, ttl=2, state_root=root
        )

    thread = threading.Thread(target=runner)
    thread.start()

    deadline = time.time() + 5
    while not pidfile.exists() and time.time() < deadline:
        time.sleep(0.05)
    assert pidfile.exists(), "child process never reported its pid"

    child_pid = int(pidfile.read_text())
    os.kill(child_pid, sig)
    thread.join(timeout=10)
    assert not thread.is_alive(), "hold() did not return after the child died"
    return outcome["code"]


def test_hold_releases_when_command_is_sigtermed(tmp_path):
    root = str(tmp_path)
    code = _run_hold_and_signal_child(tmp_path, root, signal.SIGTERM)
    assert code == 128 + signal.SIGTERM
    assert slots.status(state_root=root)["bmo"]["holders"] == 0


def test_hold_releases_when_command_is_sigkilled(tmp_path):
    root = str(tmp_path)
    code = _run_hold_and_signal_child(tmp_path, root, signal.SIGKILL)
    assert code == 128 + signal.SIGKILL
    assert slots.status(state_root=root)["bmo"]["holders"] == 0
