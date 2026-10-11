"""Tests for the `redis` slot-lease backend (issue #210).

Uses the real `redis-server` fixtures in `conftest.py`, not a mock -- per
#210's test plan. `redis_port`/`flush_redis`/`closed_port` are defined
there and shared with any future redis-backed test module.
"""

from __future__ import annotations

import json
import socket
import threading
import time

import pytest
import redis as redis_lib

from lupin import cli, slots, slots_redis


def test_acquire_respects_max_third_call_is_full(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    slots_redis.acquire("bmo", "a", max_holders=2, **kw)
    slots_redis.acquire("bmo", "b", **kw)
    # Third holder on a max-2 slot: full.
    with pytest.raises(slots.SlotFull):
        slots_redis.acquire("bmo", "c", **kw)


def test_holder_past_ttl_is_pruned_and_its_spot_freed(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    first = slots_redis.acquire("bmo", "a", ttl=0.05, max_holders=1, **kw)
    with pytest.raises(slots.SlotFull):
        slots_redis.acquire("bmo", "b", ttl=10, **kw)
    time.sleep(0.2)
    second = slots_redis.acquire("bmo", "b", ttl=10, **kw)
    assert second != first
    status = slots_redis.status(**kw)
    assert status["bmo"]["holders"] == 1


def test_release_is_compare_and_delete(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    real_lease = slots_redis.acquire("bmo", "real-holder", max_holders=2, **kw)

    # A non-holder's release attempt (same slot, a holder name that never
    # acquired) must not touch the real holder's entry.
    assert slots_redis.release("bmo:impostor", **kw) is False
    assert slots_redis.status(**kw)["bmo"]["holders"] == 1

    assert slots_redis.release(real_lease, **kw) is True
    assert slots_redis.status(**kw)["bmo"]["holders"] == 0
    # Releasing twice is not an error -- already gone is the end state anyway.
    assert slots_redis.release(real_lease, **kw) is False


def test_status_json_matches_real_sorted_set_contents(redis_port, flush_redis, capsys, clean_lupin_env):
    common = ["--backend", "redis", "--redis-host", "127.0.0.1", "--redis-port", str(redis_port)]
    cli.main(["acquire", "bmo", "--holder", "a", "--max", "2", *common])
    cli.main(["acquire", "bmo", "--holder", "b", *common])
    capsys.readouterr()

    code = cli.main(["status", "--json", *common])
    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(captured.out) == {"bmo": {"holders": 2, "max": 2}}

    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    assert raw.zcard("lupin:v1:slot:bmo") == 2
    assert set(raw.zrange("lupin:v1:slot:bmo", 0, -1)) == {"a", "b"}


def test_set_max_overwrites_an_already_set_max(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    slots_redis.acquire("bmo", "a", max_holders=2, **kw)
    assert slots_redis.status(**kw)["bmo"]["max"] == 2

    slots_redis.set_max("bmo", 5, **kw)

    assert slots_redis.status(**kw)["bmo"]["max"] == 5


def test_set_max_does_not_evict_holders_above_the_new_max(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    slots_redis.acquire("bmo", "a", max_holders=2, **kw)
    slots_redis.acquire("bmo", "b", **kw)

    slots_redis.set_max("bmo", 1, **kw)

    # Both existing holders are still counted -- lowering the max doesn't
    # touch the holder sorted set, only the separate `:max` key.
    assert slots_redis.status(**kw)["bmo"] == {"holders": 2, "max": 1}
    # A third, brand new acquire is turned away by the lowered max.
    with pytest.raises(slots.SlotFull):
        slots_redis.acquire("bmo", "c", **kw)
    # A fourth acquire frees up once a holder's lease is released.
    assert slots_redis.release("bmo:a", **kw) is True
    with pytest.raises(slots.SlotFull):
        slots_redis.acquire("bmo", "c", **kw)  # still 1 holder ("b"), max 1
    assert slots_redis.release("bmo:b", **kw) is True
    slots_redis.acquire("bmo", "c", **kw)  # now under the max


def test_set_max_unreachable_redis_raises_coordinator_unreachable(closed_port):
    with pytest.raises(slots.CoordinatorUnreachable):
        slots_redis.set_max("bmo", 3, redis_host="127.0.0.1", redis_port=closed_port)


def test_unreachable_redis_falls_back_to_local_for_bmo(closed_port, tmp_path, capsys):
    root = str(tmp_path)
    lease = slots_redis.acquire(
        "bmo", "a", max_holders=1, redis_host="127.0.0.1", redis_port=closed_port, state_root=root
    )
    captured = capsys.readouterr()

    assert "redis unreachable" in captured.err
    assert "'bmo'" in captured.err
    # The fallback actually acquired the slot, via the local backend's lock file.
    assert lease.startswith("bmo:")
    assert slots.status(state_root=root)["bmo"]["holders"] == 1


def test_unreachable_redis_raises_for_a_non_bmo_slot(closed_port):
    with pytest.raises(slots.CoordinatorUnreachable):
        slots_redis.acquire("not-bmo", "a", redis_host="127.0.0.1", redis_port=closed_port)


def test_missing_password_against_a_requirepass_server_raises_for_bmo(
    auth_redis_port, tmp_path, capsys, no_client_retry
):
    # redis-py's AuthenticationError is a ConnectionError subclass. A refused
    # login raises for bmo too. It never falls back to the local backend.
    root = str(tmp_path)
    with pytest.raises(slots_redis.CoordinatorAuthFailed) as caught:
        slots_redis.acquire(
            "bmo", "a", max_holders=1, redis_host="127.0.0.1", redis_port=auth_redis_port, state_root=root
        )
    assert "Check the Redis password" in str(caught.value)
    assert "unreachable" not in str(caught.value)
    assert "Falling back" not in capsys.readouterr().err
    assert slots.status(state_root=root).get("bmo", {}).get("holders", 0) == 0


def test_acquire_with_the_right_password_uses_redis_not_the_fallback(auth_redis_port):
    kw = {"redis_host": "127.0.0.1", "redis_port": auth_redis_port, "redis_password": "test-pass"}
    lease = slots_redis.acquire("bmo", "auth-holder", **kw)
    assert lease == "bmo:auth-holder"
    raw = redis_lib.Redis(host="127.0.0.1", port=auth_redis_port, password="test-pass", decode_responses=True)
    assert raw.zcard("lupin:v1:slot:bmo") == 1
    slots_redis.release(lease, **kw)


# A refused login is not an unreachable server. `auth_redis_port` is a real
# server with a password. The tests below connect without one.


@pytest.mark.parametrize(
    "call",
    [
        lambda kw: slots_redis.acquire("not-bmo", "a", **kw),
        lambda kw: slots_redis.release("not-bmo:a", **kw),
        lambda kw: slots_redis.set_max("not-bmo", 3, **kw),
    ],
    ids=["acquire", "release", "set_max"],
)
def test_refused_login_on_a_non_bmo_slot_raises_and_is_not_unreachable(call, auth_redis_port, no_client_retry):
    with pytest.raises(slots_redis.CoordinatorAuthFailed) as caught:
        call({"redis_host": "127.0.0.1", "redis_port": auth_redis_port})

    message = str(caught.value)
    assert "unreachable" not in message
    assert "Check the Redis password" in message


def test_refused_login_on_bmo_status_raises(auth_redis_port, tmp_path, capsys, no_client_retry):
    with pytest.raises(slots_redis.CoordinatorAuthFailed) as caught:
        slots_redis.status(redis_host="127.0.0.1", redis_port=auth_redis_port, state_root=str(tmp_path))

    assert "Check the Redis password" in str(caught.value)
    assert "Falling back" not in capsys.readouterr().err


def test_refused_login_makes_one_failed_login_per_call(auth_redis_port):
    # Production client, real server. The server counts each failed AUTH in
    # INFO stats. redis-py's own retry must not send the password again.
    admin = redis_lib.Redis(host="127.0.0.1", port=auth_redis_port, password="test-pass")
    before = admin.info("stats")["acl_access_denied_auth"]

    with pytest.raises(slots_redis.CoordinatorAuthFailed):
        slots_redis.acquire(
            "not-bmo", "a", redis_host="127.0.0.1", redis_port=auth_redis_port, redis_password="wrong-pass"
        )

    assert admin.info("stats")["acl_access_denied_auth"] - before == 1


def test_refused_login_is_not_retried_by_the_lupin_layer(auth_redis_port, no_client_retry):
    # `no_client_retry` turns off redis-py's own retries, so this checks
    # `_call_with_retry` alone.
    admin = redis_lib.Redis(host="127.0.0.1", port=auth_redis_port, password="test-pass")
    before = admin.info("stats")["acl_access_denied_auth"]

    with pytest.raises(slots_redis.CoordinatorAuthFailed):
        slots_redis.acquire(
            "not-bmo", "a", redis_host="127.0.0.1", redis_port=auth_redis_port, redis_password="wrong-pass"
        )

    assert admin.info("stats")["acl_access_denied_auth"] - before == 1


def test_dropped_connection_is_still_retried():
    # A real socket. Every connection is closed at once, which redis-py sees
    # as a transient ConnectionError. The lupin layer makes two tries on its
    # own, so more than two connections means the client retried too.
    listener = socket.create_server(("127.0.0.1", 0))
    listener.settimeout(0.1)
    port = listener.getsockname()[1]
    accepted = []
    stop = threading.Event()

    def hang_up_on_every_connection():
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except socket.timeout:
                continue
            accepted.append(conn)
            conn.close()

    thread = threading.Thread(target=hang_up_on_every_connection, daemon=True)
    thread.start()
    try:
        with pytest.raises(slots.CoordinatorUnreachable):
            slots_redis.acquire("not-bmo", "a", redis_host="127.0.0.1", redis_port=port)
    finally:
        stop.set()
        thread.join()
        listener.close()

    assert len(accepted) > 2


@pytest.mark.parametrize(
    "call, kw_fixture",
    [
        (lambda kw: slots_redis.acquire("not-bmo", "a", **kw), "no_eval_kw"),
        (lambda kw: slots_redis.release("not-bmo:a", **kw), "no_eval_kw"),
        (lambda kw: slots_redis.set_max("bmo", 3, **kw), "no_set_kw"),
    ],
    ids=["acquire-not-bmo", "release-not-bmo", "set_max-bmo"],
)
def test_acl_denial_is_raised_as_is_not_wrapped(call, kw_fixture, auth_redis_port, request):
    kw = {"redis_host": "127.0.0.1", "redis_port": auth_redis_port, **request.getfixturevalue(kw_fixture)}
    with pytest.raises(redis_lib.exceptions.NoPermissionError) as caught:
        call(kw)

    assert "no-eval-pw" not in str(caught.value)
    assert "no-set-pw" not in str(caught.value)


def test_renew_re_raises_an_acl_denial_for_a_non_bmo_slot(auth_redis_port):
    admin = redis_lib.Redis(host="127.0.0.1", port=auth_redis_port, password="test-pass")
    admin.execute_command("ACL", "SETUSER", "no-eval", "on", ">no-eval-pw", "~lupin:*", "+get", "+set", "+ping")
    try:
        with pytest.raises(redis_lib.exceptions.NoPermissionError):
            slots_redis.renew(
                "not-bmo:a", redis_username="no-eval", redis_password="no-eval-pw",
                redis_host="127.0.0.1", redis_port=auth_redis_port,
            )
    finally:
        admin.execute_command("ACL", "DELUSER", "no-eval")


def test_renew_refused_login_on_a_non_bmo_slot_raises(auth_redis_port, no_client_retry):
    with pytest.raises(slots_redis.CoordinatorAuthFailed):
        slots_redis.renew("not-bmo:a", redis_host="127.0.0.1", redis_port=auth_redis_port)


@pytest.mark.parametrize(
    "call",
    [
        lambda kw: slots_redis.acquire("bmo", "a", max_holders=1, **kw),
        lambda kw: slots_redis.renew("bmo:a", **kw),
        lambda kw: slots_redis.release("bmo:a", **kw),
        lambda kw: slots_redis.status(**kw),
    ],
    ids=["acquire", "renew", "release", "status"],
)
def test_bmo_acl_denial_raises_and_does_not_fall_back(call, auth_redis_port, no_eval_kw, tmp_path, capsys):
    kw = {"redis_host": "127.0.0.1", "redis_port": auth_redis_port, "state_root": str(tmp_path), **no_eval_kw}
    with pytest.raises(redis_lib.exceptions.NoPermissionError):
        call(kw)
    assert "Falling back" not in capsys.readouterr().err


@pytest.mark.parametrize(
    "call",
    [
        lambda kw: slots_redis.acquire("bmo", "a", max_holders=1, **kw),
        lambda kw: slots_redis.renew("bmo:a", **kw),
        lambda kw: slots_redis.release("bmo:a", **kw),
        lambda kw: slots_redis.status(**kw),
    ],
    ids=["acquire", "renew", "release", "status"],
)
def test_bmo_refused_login_raises_and_does_not_fall_back(call, auth_redis_port, tmp_path, no_client_retry, capsys):
    root = str(tmp_path)
    kw = {"redis_host": "127.0.0.1", "redis_port": auth_redis_port, "state_root": root}
    with pytest.raises(slots_redis.CoordinatorAuthFailed):
        call(kw)
    assert "Falling back" not in capsys.readouterr().err
    assert slots.status(state_root=root).get("bmo", {}).get("holders", 0) == 0


REDIS_ARGS = ["--redis-host", "127.0.0.1", "--redis-port", "{port}"]


@pytest.mark.parametrize(
    "argv, where",
    [
        (["acquire", "not-bmo", "--holder", "a", "--backend", "redis", *REDIS_ARGS], "For slot 'not-bmo'"),
        (["hold", "not-bmo", "--holder", "a", "--backend", "redis", *REDIS_ARGS, "--", "true"], "For slot 'not-bmo'"),
        (["release", "--lease", "not-bmo:a", "--backend", "redis", *REDIS_ARGS], "For lease 'not-bmo:a'"),
        (["reconcile", *REDIS_ARGS], "For the reconcile slot"),
        (["claim", "gracecraft/lupin#6", "--holder", "a", *REDIS_ARGS], "For claim 'gracecraft/lupin#6'"),
        (["status", "--backend", "redis", *REDIS_ARGS], "For the slot status"),
    ],
    ids=["acquire", "hold", "release", "reconcile", "claim", "status"],
)
def test_cli_refused_login_exits_three_and_names_the_password_setting(
    argv, where, auth_redis_port, no_client_retry, monkeypatch, capsys
):
    monkeypatch.setattr(cli.serve, "enabled_repos", lambda: [])
    code = cli.main([arg.format(port=auth_redis_port) for arg in argv])

    err = capsys.readouterr().err
    assert code == 3
    assert "cannot reach" not in err
    assert "unreachable" not in err
    assert "Set --redis-password or LUPIN_REDIS_PASSWORD." in err
    assert where in err


@pytest.mark.parametrize(
    "slot, where",
    [("bmo", "For slot 'bmo'"), ("not-bmo", "For slot 'not-bmo'")],
    ids=["bmo", "not-bmo"],
)
def test_cli_acl_denial_exits_three_and_names_the_acl(slot, where, auth_redis_port, no_eval_kw, capsys):
    argv = ["acquire", slot, "--holder", "a", "--backend", "redis", *REDIS_ARGS]
    login = ["--redis-username", no_eval_kw["redis_username"], "--redis-password", no_eval_kw["redis_password"]]
    code = cli.main([arg.format(port=auth_redis_port) for arg in argv] + login)

    err = capsys.readouterr().err
    assert code == 3
    assert "redis denied the command" in err
    assert "User no-eval" in err
    assert "Falling back" not in err
    assert "no-eval-pw" not in err
    assert where in err


@pytest.mark.parametrize(
    "argv, fixture, where",
    [
        (["hold", "not-bmo", "--holder", "a", "--backend", "redis", *REDIS_ARGS, "--", "true"], "no_eval_kw", "For slot 'not-bmo'"),
        (["release", "--lease", "not-bmo:a", "--backend", "redis", *REDIS_ARGS], "no_eval_kw", "For lease 'not-bmo:a'"),
        (["claim", "gracecraft/lupin#6", "--holder", "a", *REDIS_ARGS], "no_eval_kw", "For claim 'gracecraft/lupin#6'"),
        (["status", "--backend", "redis", *REDIS_ARGS], "no_scan_kw", "For the slot status"),
        (["reconcile", *REDIS_ARGS], "no_scan_kw", "For the reconcile run"),
    ],
    ids=["hold", "release", "claim", "status", "reconcile"],
)
def test_cli_acl_denial_exits_three_for_each_command(
    argv, fixture, where, auth_redis_port, request, monkeypatch, capsys
):
    monkeypatch.setattr(cli.serve, "enabled_repos", lambda: [])
    login = request.getfixturevalue(fixture)
    flags = ["--redis-username", login["redis_username"], "--redis-password", login["redis_password"]]
    args = [arg.format(port=auth_redis_port) for arg in argv]
    split = args.index("--") if "--" in args else len(args)
    code = cli.main(args[:split] + flags + args[split:])

    err = capsys.readouterr().err
    assert code == 3
    assert "Traceback" not in err
    assert "redis denied the command" in err
    assert "cannot reach" not in err
    assert where in err
    assert "no-eval-pw" not in err
    assert "no-scan-pw" not in err


def test_hold_keeps_the_command_exit_code_when_release_is_denied(redis_port, flush_redis, monkeypatch, capsys):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    lease = slots_redis.acquire("not-bmo", "a", max_holders=1, **kw)

    def denied(lease_id, **_kwargs):
        raise redis_lib.exceptions.NoPermissionError("User x has no permissions to run the 'eval' command.")

    monkeypatch.setattr(slots_redis, "release", denied)
    code = slots_redis.hold(["sh", "-c", "exit 7"], lease=lease, **kw)

    err = capsys.readouterr().err
    assert code == 7
    assert f"lease {lease} not released" in err
    assert "Traceback" not in err


def test_hold_prints_one_line_when_renew_is_denied_mid_run(redis_port, flush_redis, monkeypatch, capsys):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    lease = slots_redis.acquire("not-bmo", "a", max_holders=1, **kw)
    calls = []

    def denied(lease_id, **_kwargs):
        calls.append(lease_id)
        raise redis_lib.exceptions.NoPermissionError("User x has no permissions to run the 'eval' command.")

    monkeypatch.setattr(slots_redis, "renew", denied)
    code = slots_redis.hold(["sh", "-c", "sleep 1; exit 3"], lease=lease, ttl=0.3, **kw)

    err = capsys.readouterr().err
    assert code == 3
    assert len(calls) == 1
    assert f"lease {lease} not renewed" in err
    assert "Traceback" not in err


def test_hold_prints_one_line_when_renew_reports_the_lease_gone(redis_port, flush_redis, monkeypatch, capsys):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    lease = slots_redis.acquire("not-bmo", "a", max_holders=1, **kw)
    calls = []

    def gone(lease_id, **_kwargs):
        calls.append(lease_id)
        return False

    monkeypatch.setattr(slots_redis, "renew", gone)
    code = slots_redis.hold(["sh", "-c", "sleep 1; exit 3"], lease=lease, ttl=0.3, **kw)

    err = capsys.readouterr().err
    assert code == 3
    assert err.count("not renewed") == 1
    assert f"lease {lease} not renewed" in err
    # Renewal keeps running after a False return. Redis may recover.
    assert len(calls) > 1
