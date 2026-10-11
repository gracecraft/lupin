"""Append and read repository events in the shared Redis ledger."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone

import redis

from . import machines
from .slots import CoordinatorUnreachable
from .slots_redis import PREFIX, _call_with_retry, _client

_REPO_RE = re.compile(r"^[^/#\s]+/[^/#\s]+$")
_EVENT_FIELDS = (
    "event", "issue", "status", "branch", "summary", "highlights",
    "evidence", "decisions", "next", "children",
)
_LIST_FIELDS = {"highlights", "evidence", "decisions", "next", "children"}


def _stream_key(repo: str) -> str:
    if not isinstance(repo, str) or not _REPO_RE.fullmatch(repo):
        raise ValueError(f"expected OWNER/REPO, got {repo!r}")
    return f"{PREFIX}ledger:{repo.lower()}"


def _validate_event(event: dict) -> dict:
    if (
        not isinstance(event, dict)
        or not isinstance(event.get("event"), str)
        or not event["event"].strip()
    ):
        raise ValueError("ledger event needs a non-empty event name")

    record = {key: event[key] for key in _EVENT_FIELDS if key in event}
    record["event"] = record["event"].strip()
    if "issue" in record and (
        not isinstance(record["issue"], int)
        or isinstance(record["issue"], bool)
        or record["issue"] < 1
    ):
        raise ValueError("issue must be a positive integer")
    if record.get("children") and "issue" not in record:
        raise ValueError("children need an issue number")
    for key in ("event", "status", "branch", "summary"):
        if key in record and not isinstance(record[key], str):
            raise ValueError(f"{key} must be text")
    for key in _LIST_FIELDS:
        if key not in record:
            continue
        value = record[key]
        if not isinstance(value, list):
            raise ValueError(f"{key} must be a list")
        if key == "children":
            if any(
                not isinstance(number, int)
                or isinstance(number, bool)
                or number < 1
                for number in value
            ):
                raise ValueError("children must contain positive issue numbers")
        elif any(not isinstance(item, str) for item in value):
            raise ValueError(f"{key} must contain text")
    return record


def _encode(record: dict) -> dict[str, str]:
    fields = {"ts": record["timestamp"], "host": record["host"]}
    for key in _EVENT_FIELDS:
        if key not in record:
            continue
        value = record[key]
        fields[key] = (
            json.dumps(value, ensure_ascii=False)
            if key in _LIST_FIELDS else str(value)
        )
    return fields


def _decode(stream_id: str, fields: dict[str, str]) -> dict:
    record = {
        "id": stream_id, "timestamp": fields["ts"], "host": fields["host"]
    }
    for key in _EVENT_FIELDS:
        if key not in fields:
            continue
        if key in _LIST_FIELDS:
            record[key] = json.loads(fields[key])
        elif key == "issue":
            record[key] = int(fields[key])
        else:
            record[key] = fields[key]
    return record


def append_event(
    repo: str,
    event: dict,
    *,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
) -> dict:
    """Append one event and return it with its Redis stream ID.

    Raises `ValueError` for an invalid repo or event, or
    `CoordinatorUnreachable` when Redis cannot be reached.
    """
    key = _stream_key(repo)
    record = _validate_event(event)
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    record["timestamp"] = timestamp.replace("+00:00", "Z")
    record["host"] = machines.hostname()
    client = _client(redis_host, redis_port, redis_username, redis_password)
    try:
        stream_id = client.xadd(key, _encode(record))
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise CoordinatorUnreachable(repo) from exc
    return {"id": stream_id, **record}


def read_events(
    repo: str,
    *,
    limit: int | None = 10,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
    client: "redis.Redis | None" = None,
) -> list[dict]:
    """Return the latest `limit` events, oldest first.

    `limit` defaults to 10 and must be positive. Pass `None` to read the full
    stream. An empty stream returns an empty list. Raises
    `CoordinatorUnreachable` when Redis cannot be reached. Pass `client` to
    use a ready client, as the debrief path does. Without it, a client is made
    from the connection arguments.
    """
    key = _stream_key(repo)
    if limit is not None and (
        not isinstance(limit, int) or isinstance(limit, bool) or limit < 1
    ):
        raise ValueError("limit must be a positive integer")
    if client is None:
        client = _client(redis_host, redis_port, redis_username, redis_password)
    try:
        if limit is None:
            entries = _call_with_retry(lambda: client.xrange(key))
        else:
            entries = _call_with_retry(lambda: client.xrevrange(key, count=limit))
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise CoordinatorUnreachable(repo) from exc
    if limit is not None:
        entries = reversed(entries)
    return [_decode(stream_id, fields) for stream_id, fields in entries]
