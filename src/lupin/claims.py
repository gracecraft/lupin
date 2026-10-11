"""GitHub-issue claims: one loop marks an issue as its own so two loops
never work the same task (issue #6, the `lupin` half of
`gracecraft/nix#212`). See `docs/redis-schema.md` for the key shape this
module implements.

A claim is a single Redis string at `claim:<owner>/<repo>#<n>`: JSON
`{"host", "session", "since"}`, with a TTL Redis enforces natively. Unlike
`slots_redis.py`'s slots (a sorted set, lazily pruned by whoever next calls
`acquire`/`renew`), a claim has exactly one holder, so `SET ... PX` and
Redis's own expiry are enough -- no pruning code needed here.

Reuses `slots_redis._client` and `slots_redis._call_with_retry` as-is (same
"2s connect timeout, one retry" rule every other Redis call in this project
follows) -- not redefined here, per issue #6.

Judgment call -- what `--holder H` means: the schema's JSON has three
fields, not one, so the single `--holder` string from the CLI becomes the
`session` field (the identity that must match for a renew or release to
succeed). `host` is filled in automatically from `socket.gethostname()`.
This keeps the CLI surface the same shape as `slots.py`/`slots_redis.py`'s
single `holder` string -- `host` is metadata the schema asks for, not a
second identity the caller has to pass.

Claims have no local fallback (`docs/redis-schema.md`'s fallback table: "the
orchestrator starts no new issue"), so every function here raises
`CoordinatorUnreachable` (imported from `slots.py`, same exception every
other backend failure uses) when Redis can't be reached -- there is no
`local` backend to fall back to, unlike the `bmo` slot.
A refused login raises `CoordinatorAuthFailed`. A denied command raises
`NoPermissionError` as-is.
"""

from __future__ import annotations

import json
import re
import socket
import time

import redis

from .slots import CoordinatorUnreachable
from .slots_redis import _auth_failed, _call_with_retry, _client

PREFIX = "lupin:v1:"
DEFAULT_TTL = 600.0  # 10 minutes, per docs/redis-schema.md

_TARGET_RE = re.compile(r"^(?P<owner>[^/#]+)/(?P<repo>[^/#]+)#(?P<number>\d+)$")


class ClaimHeld(Exception):
    """Raised by `claim` when someone else already holds this issue."""

    def __init__(self, target: str, current_raw: str | None):
        detail = current_raw or "unknown holder"
        super().__init__(f"{target} is already claimed: {detail}")


def parse_target(target: str) -> str:
    """Validate `OWNER/REPO#N` and return it unchanged -- it's also the key
    suffix, since `docs/redis-schema.md`'s key is literally `claim:<that
    string>`.
    """
    if not _TARGET_RE.match(target):
        raise ValueError(f"expected OWNER/REPO#N, got {target!r}")
    return target


def _key(target: str) -> str:
    return f"{PREFIX}claim:{target}"


def _value(session: str) -> str:
    return json.dumps({"host": socket.gethostname(), "session": session, "since": time.time()})


# KEYS[1] = claim:<target>, ARGV[1] = session, ARGV[2] = value (JSON),
# ARGV[3] = ttl_ms. Claiming is idempotent for the same session (a retry
# renews instead of failing). Returns 1 (claimed or renewed) or 0 (someone
# else holds it).
_CLAIM_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if current then
    local ok, decoded = pcall(cjson.decode, current)
    if not ok or decoded.session ~= ARGV[1] then
        return 0
    end
end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1
"""

# KEYS[1] = claim:<target>, ARGV[1] = session, ARGV[2] = value (JSON),
# ARGV[3] = ttl_ms. Unlike the claim script, never creates a new claim --
# only pushes the TTL out if `session` is the current holder. Returns 1
# (renewed) or 0 (not held by this session).
_RENEW_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if not current then
    return 0
end
local ok, decoded = pcall(cjson.decode, current)
if not ok or decoded.session ~= ARGV[1] then
    return 0
end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1
"""

# KEYS[1] = claim:<target>, ARGV[1] = session. Compare-and-delete: only
# removes the claim if `session` is the current holder. Returns 1
# (released) or 0 (not held by this session).
_RELEASE_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if not current then
    return 0
end
local ok, decoded = pcall(cjson.decode, current)
if not ok or decoded.session ~= ARGV[1] then
    return 0
end
redis.call('DEL', KEYS[1])
return 1
"""


def claim(
    target: str,
    holder: str,
    *,
    ttl: float = DEFAULT_TTL,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
) -> None:
    """Atomically take `target` (an `OWNER/REPO#N` string) for `holder`.

    Idempotent for the same holder -- a retry renews rather than failing.
    Raises `ClaimHeld` if another holder already has it, or
    `CoordinatorUnreachable` if Redis can't be reached.
    """
    target = parse_target(target)
    client = _client(redis_host, redis_port, redis_username, redis_password)
    value = _value(holder)
    try:
        result = _call_with_retry(
            lambda: client.eval(_CLAIM_SCRIPT, 1, _key(target), holder, value, int(ttl * 1000))
        )
        if not result:
            current = _call_with_retry(lambda: client.get(_key(target)))
    except redis.exceptions.AuthenticationError as exc:
        raise _auth_failed(exc) from exc
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise CoordinatorUnreachable(target) from exc
    if not result:
        raise ClaimHeld(target, current)


def renew_claim(
    target: str,
    holder: str,
    *,
    ttl: float = DEFAULT_TTL,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
) -> bool:
    """Push `target`'s claim TTL back out. Returns False if `holder` is not
    the current holder (claim expired, released, or never theirs). Raises
    `CoordinatorUnreachable` if Redis can't be reached.
    """
    target = parse_target(target)
    client = _client(redis_host, redis_port, redis_username, redis_password)
    value = _value(holder)
    try:
        result = _call_with_retry(
            lambda: client.eval(_RENEW_SCRIPT, 1, _key(target), holder, value, int(ttl * 1000))
        )
    except redis.exceptions.AuthenticationError as exc:
        raise _auth_failed(exc) from exc
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise CoordinatorUnreachable(target) from exc
    return bool(result)


def release_claim(
    target: str,
    holder: str,
    *,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
) -> bool:
    """Compare-and-delete release. Returns False if `holder` is not the
    current holder (including "no one holds it"). Raises
    `CoordinatorUnreachable` if Redis can't be reached.
    """
    target = parse_target(target)
    client = _client(redis_host, redis_port, redis_username, redis_password)
    try:
        result = _call_with_retry(lambda: client.eval(_RELEASE_SCRIPT, 1, _key(target), holder))
    except redis.exceptions.AuthenticationError as exc:
        raise _auth_failed(exc) from exc
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise CoordinatorUnreachable(target) from exc
    return bool(result)


def claims_for(
    repos: list[str],
    *,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
    client: "redis.Redis | None" = None,
) -> dict[str, dict]:
    """Return `{"<owner>/<repo>#<n>": {"host", "session", "since"}}` for
    every currently-claimed issue in `repos` (each an `"<owner>/<repo>"`
    string, no issue number).

    This is the integration point that future `roadmap` (#10) and `quest`
    (#11) commands import. They use it to find out which of their issues are
    off-limits. Pass the repos you already know about. The function returns
    the claimed subset. Raises
    `CoordinatorUnreachable` if Redis cannot be reached. There is no local
    fallback for claims. Callers must handle `CoordinatorUnreachable`.
    Start no new issue. Do not disturb anything
    already in progress. Pass `client` to use a ready client, as the debrief
    path does. Without it, a client is made from the connection arguments.
    """
    if client is None:
        client = _client(redis_host, redis_port, redis_username, redis_password)
    prefix = f"{PREFIX}claim:"
    try:
        keys = _call_with_retry(lambda: list(client.scan_iter(match=f"{prefix}*")))
        wanted = []
        for key in keys:
            target = key[len(prefix) :]
            owner_repo, _sep, _number = target.rpartition("#")
            if owner_repo in repos:
                wanted.append(key)
        result: dict[str, dict] = {}
        if not wanted:
            return result
        # One batch of GET commands for all keys, sent together. GET, not MGET,
        # so a key of the wrong type raises an error. MGET would return None.
        # The batch is built inside the retry, because execute() clears it.
        def read_claims():
            pipe = client.pipeline(transaction=False)
            for key in wanted:
                pipe.get(key)
            return pipe.execute(raise_on_error=True)

        raws = _call_with_retry(read_claims)
        for key, raw in zip(wanted, raws):
            if raw is None:
                continue  # expired between the scan and the read
            result[key[len(prefix) :]] = json.loads(raw)
        return result
    except redis.exceptions.AuthenticationError as exc:
        raise _auth_failed(exc) from exc
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise CoordinatorUnreachable("claims_for") from exc
