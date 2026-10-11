from __future__ import annotations

import json
from datetime import datetime, timezone

import redis as redis_lib

from lupin import cli, quota, quota_cache, usage_cache


def _kw(redis_port):
    return {"redis_host": "127.0.0.1", "redis_port": redis_port}


def _raw_client(redis_port):
    return redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)


def test_refresh_publishes_local_rows_for_this_machine(redis_port, flush_redis, monkeypatch):
    rows = [{
        "provider": "openai-codex",
        "input_tokens": 15,
        "output_tokens": 10,
        "cost": 0.5,
        "period": "last 7 days",
        "source": "/omp/stats.db",
        "last_update": "2026-10-08 09:00:00",
    }]
    monkeypatch.setattr(usage_cache.socket, "gethostname", lambda: "jesus")
    monkeypatch.setattr(quota, "claude_usage", lambda: [])
    monkeypatch.setattr(quota, "omp_usage", lambda: rows)

    snapshots = usage_cache.refresh_snapshot(**_kw(redis_port))

    assert snapshots["jesus"]["rows"] == rows
    assert usage_cache.is_fresh(snapshots["jesus"])
    assert _raw_client(redis_port).ttl(f"{usage_cache.REDIS_KEY_PREFIX}jesus") > 0


def test_read_aggregates_distinct_machine_totals(redis_port, flush_redis):
    client = _raw_client(redis_port)
    for machine, tokens, cost in (("jesus", 15, 0.5), ("ralpha", 6, 0.25)):
        entry = {
            "rows": [{
                "provider": "openai-codex",
                "input_tokens": tokens,
                "output_tokens": tokens - 5,
                "cost": cost,
                "period": "last 7 days",
                "source": "/omp/stats.db",
                "last_update": "2026-10-08 09:00:00",
            }],
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "fetched_by": machine,
        }
        client.set(
            f"{usage_cache.REDIS_KEY_PREFIX}{machine}",
            json.dumps(entry),
            ex=usage_cache.REDIS_KEY_TTL,
        )

    snapshots = usage_cache.read_snapshot(**_kw(redis_port))
    rows = usage_cache.aggregate_rows(snapshots)

    assert set(snapshots) == {"jesus", "ralpha"}
    assert rows == [{
        "provider": "openai-codex",
        "period": "last 7 days",
        "last_update": "2026-10-08 09:00:00",
        "input_tokens": 21,
        "output_tokens": 11,
        "cost": 0.75,
        "source": "/omp/stats.db on jesus, /omp/stats.db on ralpha",
    }]


def test_missing_corrupt_or_unreachable_snapshot_is_empty(redis_port, flush_redis, closed_port):
    assert usage_cache.read_snapshot(**_kw(redis_port)) == {}

    _raw_client(redis_port).set(f"{usage_cache.REDIS_KEY_PREFIX}jesus", "{not json")
    assert usage_cache.read_snapshot(**_kw(redis_port)) == {}
    assert usage_cache.read_snapshot(redis_host="127.0.0.1", redis_port=closed_port) == {}


def test_is_fresh_uses_the_five_minute_window():
    now = datetime.now(timezone.utc).timestamp()
    fresh_at = datetime.fromtimestamp(now - usage_cache.CACHE_TTL + 1, timezone.utc)
    stale_at = datetime.fromtimestamp(now - usage_cache.CACHE_TTL - 1, timezone.utc)

    assert usage_cache.is_fresh({"fetched_at": fresh_at.isoformat()}, now=now)
    assert not usage_cache.is_fresh({"fetched_at": stale_at.isoformat()}, now=now)


def test_quota_command_publishes_quota_and_usage(
    redis_port, flush_redis, monkeypatch, capsys, tmp_path, clean_lupin_env
):
    quota_rows = [{
        "provider": "openai",
        "duration": quota.QuotaDuration.FIVE_HOURS,
        "used_pct": 42,
        "resets_at": 1_800_000_000_000,
    }]
    usage_rows = [{
        "provider": "openai-codex",
        "input_tokens": 15,
        "output_tokens": 10,
        "cost": 0.5,
        "period": "last 7 days",
        "source": "/omp/stats.db",
        "last_update": "2026-10-08 09:00:00",
    }]
    monkeypatch.setattr(quota, "quota_usage", lambda: quota_rows)
    monkeypatch.setattr(quota, "claude_usage", lambda: [])
    monkeypatch.setattr(quota, "omp_usage", lambda: usage_rows)
    monkeypatch.setattr(usage_cache.socket, "gethostname", lambda: "jesus")

    code = cli.main([
        "quota",
        "--json",
        "--redis-host", "127.0.0.1",
        "--redis-port", str(redis_port),
        "--config-path", str(tmp_path / "fleet.json"),
    ])
    capsys.readouterr()

    assert code == 0
    assert quota_cache.read_snapshot(**_kw(redis_port))["openai"]["rows"] == quota_rows
    assert usage_cache.aggregate_rows(usage_cache.read_snapshot(**_kw(redis_port))) == [{
        **usage_rows[0],
        "source": "/omp/stats.db on jesus",
    }]
