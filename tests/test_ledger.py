"""Tests for repository ledger events in Redis."""

from __future__ import annotations

import json

import redis
import pytest

from lupin import cli, ledger, machines, slots


@pytest.fixture
def connection(redis_port, flush_redis):
    return {"redis_host": "127.0.0.1", "redis_port": redis_port}


@pytest.fixture
def isolated_cli_args(tmp_path, monkeypatch):
    monkeypatch.delenv("LUPIN_REDIS_USERNAME", raising=False)
    monkeypatch.delenv("LUPIN_REDIS_PASSWORD", raising=False)
    config_path = tmp_path / "fleet.json"
    config_path.write_text("{}", encoding="utf-8")
    return ["--config-path", str(config_path)]


def test_append_and_read_round_trip_event_fields(connection, monkeypatch):
    monkeypatch.setattr(machines, "hostname", lambda: "machine-a")
    event = {
        "event": "handoff",
        "issue": 42,
        "status": "ready",
        "branch": "issue/42-ledger",
        "summary": "Shared ledger added.",
        "highlights": ["Redis stream"],
        "evidence": ["tests pass"],
        "decisions": ["No local fallback."],
        "next": ["Review the pull request."],
        "children": [43],
    }

    first = ledger.append_event(
        "Acme/Repo", {"event": "dispatch", "issue": 42}, **connection
    )
    appended = ledger.append_event("Acme/Repo", event, **connection)
    events = ledger.read_events("acme/repo", **connection)

    assert events == [first, appended]
    assert appended["host"] == "machine-a"
    assert appended["timestamp"].endswith("Z")
    assert appended["issue"] == 42
    assert appended["children"] == [43]
    assert appended["highlights"] == ["Redis stream"]


def test_append_does_not_retry_after_ambiguous_timeout(connection, monkeypatch):
    real_client_factory = ledger._client
    real_client = real_client_factory(**connection)
    attempts = 0

    class CommitThenTimeout:
        def xadd(self, key, fields):
            nonlocal attempts
            attempts += 1
            real_client.xadd(key, fields)
            raise redis.exceptions.TimeoutError("reply lost after commit")

    monkeypatch.setattr(ledger, "_client", lambda *_args: CommitThenTimeout())

    with pytest.raises(slots.CoordinatorUnreachable):
        ledger.append_event(
            "acme/repo", {"event": "dispatch", "issue": 42}, **connection
        )

    monkeypatch.setattr(ledger, "_client", real_client_factory)
    events = ledger.read_events("acme/repo", **connection)

    assert attempts == 1
    assert len(events) == 1


def test_read_returns_latest_ten_and_honors_custom_limit(connection):
    for number in range(1, 13):
        ledger.append_event(
            "acme/repo", {"event": "work", "issue": number}, **connection
        )

    recent = ledger.read_events("acme/repo", **connection)
    smaller = ledger.read_events("acme/repo", limit=3, **connection)
    full_stream = ledger.read_events("acme/repo", limit=None, **connection)

    assert [event["issue"] for event in recent] == list(range(3, 13))
    assert [event["issue"] for event in smaller] == [10, 11, 12]
    assert [event["issue"] for event in full_stream] == list(range(1, 13))


@pytest.mark.parametrize("limit", [0, -1, True, "2"])
def test_read_rejects_invalid_limit(limit):
    with pytest.raises(ValueError, match="limit must be a positive integer"):
        ledger.read_events("acme/repo", limit=limit)


def test_read_of_empty_ledger_returns_no_events(connection):
    assert ledger.read_events("acme/empty", **connection) == []


@pytest.mark.parametrize(
    ("bad_fields", "error_name"),
    [
        (
            {
                "ts": "2026-10-10T00:00:00Z",
                "host": "machine-a",
                "event": "work",
                "highlights": "not json",
            },
            "JSONDecodeError",
        ),
        ({"host": "machine-a", "event": "work"}, "KeyError"),
        ({"ts": "2026-10-10T00:00:00Z", "event": "work"}, "KeyError"),
        (
            {
                "ts": "2026-10-10T00:00:00Z",
                "host": "machine-a",
                "event": "work",
                "issue": "abc",
            },
            "ValueError",
        ),
        (
            {
                "ts": "2026-10-10T00:00:00Z",
                "host": "machine-a",
                "event": "work",
                "highlights": "[" * 100_000 + "]" * 100_000,
            },
            "RecursionError",
        ),
    ],
    ids=["bad-json", "missing-ts", "missing-host", "bad-issue", "deep-json"],
)
def test_read_skips_bad_entry_and_reports_it_once(
    connection, capsys, bad_fields, error_name
):
    first = ledger.append_event(
        "acme/repo", {"event": "work", "issue": 1}, **connection
    )
    bad_id = ledger._client(**connection).xadd(
        ledger._stream_key("acme/repo"), bad_fields
    )
    last = ledger.append_event("acme/repo", {"event": "work", "issue": 2}, **connection)

    events = ledger.read_events("acme/repo", **connection)

    err = capsys.readouterr().err
    assert events == [first, last]
    assert err.count(bad_id) == 1
    assert f"{error_name}:" in err


def test_read_counts_bad_entry_toward_limit(connection, capsys):
    ledger.append_event("acme/repo", {"event": "work", "issue": 1}, **connection)
    bad_id = ledger._client(**connection).xadd(
        ledger._stream_key("acme/repo"),
        {
            "ts": "2026-10-10T00:00:00Z",
            "host": "machine-a",
            "event": "work",
            "highlights": "not json",
        },
    )
    last = ledger.append_event("acme/repo", {"event": "work", "issue": 2}, **connection)

    events = ledger.read_events("acme/repo", limit=2, **connection)

    assert events == [last]
    assert capsys.readouterr().err.count(bad_id) == 1


def test_read_skips_non_utf8_entry_and_reports_it_once(connection, capsys):
    bad_id = ledger._client(**connection).xadd(
        ledger._stream_key("acme/repo"),
        {
            "ts": "2026-10-10T00:00:00Z",
            "host": "machine-a",
            "event": "work",
            "summary": b"\xff\xfe",
        },
    )
    good = ledger.append_event("acme/repo", {"event": "work", "issue": 2}, **connection)

    events = ledger.read_events("acme/repo", **connection)

    err = capsys.readouterr().err
    assert events == [good]
    assert err.count(bad_id) == 1
    assert "UnicodeDecodeError:" in err


def test_append_and_read_report_unavailable_coordinator(closed_port):
    connection = {"redis_host": "127.0.0.1", "redis_port": closed_port}

    with pytest.raises(slots.CoordinatorUnreachable):
        ledger.append_event("acme/repo", {"event": "dispatch"}, **connection)
    with pytest.raises(slots.CoordinatorUnreachable):
        ledger.read_events("acme/repo", **connection)


def test_cli_appends_and_reads_shared_event(connection, capsys, isolated_cli_args):
    append_args = [
        "ledger", "append", "acme/repo", "--event", "handoff", "--issue", "42",
        "--status", "ready", "--summary", "Shared handoff.", "--highlights", "Redis",
        "--child", "43", "--redis-host", connection["redis_host"],
        "--redis-port", str(connection["redis_port"]), "--json",
    ]
    append_args.extend(isolated_cli_args)

    assert cli.main(append_args) == 0
    appended = json.loads(capsys.readouterr().out)
    assert appended["issue"] == 42
    assert appended["children"] == [43]

    read_args = [
        "ledger", "read", "acme/repo", "--redis-host", connection["redis_host"],
        "--redis-port", str(connection["redis_port"]), "--json",
    ]
    read_args.extend(isolated_cli_args)
    assert cli.main(read_args) == 0
    assert json.loads(capsys.readouterr().out) == [appended]


def test_cli_read_uses_default_and_custom_limits(
    connection, capsys, isolated_cli_args
):
    for number in range(1, 13):
        ledger.append_event(
            "acme/repo", {"event": "work", "issue": number}, **connection
        )

    read_args = [
        "ledger", "read", "acme/repo", "--redis-host", connection["redis_host"],
        "--redis-port", str(connection["redis_port"]), "--json", *isolated_cli_args,
    ]
    assert cli.main(read_args) == 0
    recent = json.loads(capsys.readouterr().out)
    assert [event["issue"] for event in recent] == list(range(3, 13))

    limited_args = [
        "ledger", "read", "acme/repo", "--limit", "3",
        "--redis-host", connection["redis_host"],
        "--redis-port", str(connection["redis_port"]), "--json", *isolated_cli_args,
    ]
    assert cli.main(limited_args) == 0
    limited = json.loads(capsys.readouterr().out)
    assert [event["issue"] for event in limited] == [10, 11, 12]


def test_cli_reports_unavailable_coordinator(closed_port, capsys, isolated_cli_args):
    result = cli.main([
        "ledger", "read", "acme/repo", "--redis-host", "127.0.0.1",
        "--redis-port", str(closed_port),
        *isolated_cli_args,
    ])

    assert result == 3
    assert "cannot reach the redis coordinator" in capsys.readouterr().err
