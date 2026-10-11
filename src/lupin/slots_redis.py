"""Slot leases backed by Redis: the `redis` backend (issue #210, part of
#198's plan). See `docs/redis-schema.md` for the key shapes, the two Lua
scripts, TTLs, and the fallback rules this module implements -- that doc is
the spec, this module is just that spec in code.

Same shape as `slots.py`'s `local` backend (`acquire`, `renew`, `release`,
`status`, `hold`), same lease-id convention (`"<slot>:<token>"`, split with
`_lease_runtime.split_lease`), same `SlotFull`/`CoordinatorUnreachable`
exceptions (imported from `slots.py`, not redefined -- one exit-code
contract, see `cli.py`). `hold`'s subprocess/renew-timer plumbing is the
same shared `_lease_runtime.run_with_lease` the `local` backend uses.

Key difference from `local`: the sorted set's member *is* the holder's name,
not a random token (`docs/redis-schema.md`'s acquire script renews in place
when "the caller already holds the slot"). So a lease id here is
`"<slot>:<holder>"` -- there is no secret token, by design: the schema's
trust boundary is the tailnet plus a Redis ACL (#198 section 4), not a
guessable lease id.

Judgment call -- where the slot's `max` lives: the schema's key table does
not list one. Reusing the `local` backend's own judgment call (a slot's
config is read once and fixed at creation), this module stores it in a
plain string key, `lupin:v1:slot:<name>:max`, set with `SET ... NX` on the
first `acquire`/`hold` call and read on every later one. `GET`/`SET` are
already on the ACL command list in `docs/redis-schema.md`, so this adds no
new permission, just a second use of an already-allowed command pair.

A dashboard or operator can still change that number later, with
`set_max()` -- a plain `SET`, no `NX`. It does not touch `slot:<name>`
itself, so a holder already past the new, lower max keeps its lease; only
a later `acquire` sees the new number and can be turned away by it.

Fallback: only the `bmo` slot falls back to the `local` backend when Redis
is unreachable (`docs/redis-schema.md`'s fallback table; a connect timeout
counts as unreachable too). Every other slot name raises
`CoordinatorUnreachable` instead -- the schema does not describe fallback
for a hypothetical second fleet slot, v1 only has `bmo`, so this module does
not invent a rule for a slot that does not exist yet.

Refusals never fall back to the local backend.

A refused login raises `CoordinatorAuthFailed` for every slot. It is a
subclass of `CoordinatorUnreachable`, so existing handlers still catch it.
An ACL-denied command raises redis-py's `NoPermissionError` as-is.
redis-py's `AuthenticationError` is a subclass of `ConnectionError`. Catch
it first. The client's own retry skips it too (`_RetryUnlessRefused`).
`docs/redis-schema.md` lists the result of each call.

Claims and ledger streams are fleet keys. `claims.py` and `ledger.py`
implement them separately. Neither uses a local fallback when Redis is
unreachable.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import redis
from redis.backoff import ExponentialWithJitterBackoff
from redis.retry import Retry

from . import slots as local_slots
from ._lease_runtime import run_with_lease, split_lease

SlotFull = local_slots.SlotFull
CoordinatorUnreachable = local_slots.CoordinatorUnreachable


class CoordinatorAuthFailed(CoordinatorUnreachable):
    """Redis answered, but refused the login.

    The message never contains the password.
    """

    def for_user(self, setting: str) -> str:
        """Return the message for a user. It names `setting`."""
        return f"{self}. Set {setting}."


PREFIX = "lupin:v1:"
FALLBACK_SLOTS = {"bmo"}

# Where the Redis login comes from. Used in messages. The fleet settings
# also cover the `lupin join` config and the systemd credential that
# `machines.resolve_connection` reads.
FLAG_SETTING = "--redis-password or LUPIN_REDIS_PASSWORD"
FLAG_USER_SETTING = "--redis-username or LUPIN_REDIS_USERNAME"
FLEET_PASSWORD_SETTING = (
    "--redis-password, LUPIN_REDIS_PASSWORD, or the systemd credential redis-password"
)
FLEET_USER_SETTING = "--redis-username, LUPIN_REDIS_USERNAME, or the lupin join config"

CONNECT_TIMEOUT = 2.0

# KEYS[1] = slot:<name>, ARGV[1] = holder, ARGV[2] = now_ms, ARGV[3] = ttl_ms,
# ARGV[4] = max holders. Returns 1 (added or renewed) or 0 (busy).
_ACQUIRE_SCRIPT = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[2])
local expiry = tonumber(ARGV[2]) + tonumber(ARGV[3])
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
    redis.call('ZADD', KEYS[1], expiry, ARGV[1])
    return 1
end
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[4]) then
    redis.call('ZADD', KEYS[1], expiry, ARGV[1])
    return 1
end
return 0
"""

# KEYS[1] = slot:<name>, ARGV[1] = holder, ARGV[2] = now_ms, ARGV[3] = ttl_ms.
# Only pushes the deadline out if the holder is still live -- unlike
# acquire, never takes a new spot. Returns 1 (renewed) or 0 (lease is gone).
_RENEW_SCRIPT = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[2])
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
    redis.call('ZADD', KEYS[1], tonumber(ARGV[2]) + tonumber(ARGV[3]), ARGV[1])
    return 1
end
return 0
"""

# KEYS[1] = slot:<name>, ARGV[1] = holder. Compare-and-delete: only removes
# the caller's own entry. Returns 1 (released) or 0 (already gone).
_RELEASE_SCRIPT = """
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
    redis.call('ZREM', KEYS[1], ARGV[1])
    return 1
end
return 0
"""


class _RetryUnlessRefused(Retry):
    """redis-py's default retry, except a refused login is not retried.

    redis-py's AuthenticationError is a ConnectionError, so the default
    Retry tries again. Each try sends the password again.
    """

    def call_with_retry(self, do, fail, is_retryable=None, with_failure_count=False):
        def retryable(exc):
            if isinstance(exc, redis.exceptions.AuthenticationError):
                return False
            return is_retryable is None or is_retryable(exc)

        return super().call_with_retry(do, fail, retryable, with_failure_count)


def _client(
    redis_host: str | None,
    redis_port: int | None,
    redis_username: str | None = None,
    redis_password: str | None = None,
    *,
    decode_responses: bool = True,
) -> "redis.Redis":
    return redis.Redis(
        host=redis_host or "localhost",
        port=redis_port or 6379,
        username=redis_username,
        password=redis_password,
        socket_connect_timeout=CONNECT_TIMEOUT,
        socket_timeout=CONNECT_TIMEOUT,
        decode_responses=decode_responses,
        # Same backoff and count as redis-py's default Retry.
        retry=_RetryUnlessRefused(ExponentialWithJitterBackoff(base=0.01, cap=1), retries=10),
    )


def _call_with_retry(func):
    """Try `func()`, retrying once on a connect/timeout error (the schema's
    "2s connect timeout, 1 retry"), then let the second failure propagate.
    A refused login (`AuthenticationError`) is not retried.
    """
    try:
        return func()
    except redis.exceptions.AuthenticationError:
        raise
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
        return func()


def _auth_failed(exc: Exception) -> CoordinatorAuthFailed:
    """Build the exception for a refused login.
    The message never contains the password.
    """
    reply = str(exc).rstrip(".")
    return CoordinatorAuthFailed(
        f"redis refused the login. Check the Redis password. Redis said: {reply}"
    )


def refusal_message(exc: Exception, what: str, *, fleet: bool = False) -> str:
    """Return the one-line text for a refusal or an ACL denial. `what` names
    the failed action, for example `slot 'bmo'`. Never contains the password.
    `fleet` also names the `lupin join` config and the systemd credential.
    """
    password_setting = FLEET_PASSWORD_SETTING if fleet else FLAG_SETTING
    user_setting = FLEET_USER_SETTING if fleet else FLAG_USER_SETTING
    if isinstance(exc, redis.exceptions.NoPermissionError):
        return (
            "redis denied the command. The redis user's ACL does not allow it. "
            f"Check the ACL in docs/redis-schema.md. The user is set by {user_setting}. "
            f"Redis said: {str(exc).rstrip('.')}. For {what}."
        )
    return f"{exc.for_user(password_setting)} For {what}."


def _warn_fallback(slot: str, exc: Exception) -> None:
    print(
        f"lupin: redis unreachable ({exc}); falling back to the local backend "
        f"for slot {slot!r}",
        file=sys.stderr,
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _get_or_set_max(client: "redis.Redis", slot: str, max_holders: int | None) -> int:
    key = f"{PREFIX}slot:{slot}:max"
    chosen = max_holders if max_holders is not None else 1
    client.set(key, chosen, nx=True)
    return int(client.get(key))


def set_max(
    slot: str,
    max_holders: int,
    *,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
) -> int:
    """Change a slot's max, overwriting whatever `_get_or_set_max` set it to.

    A plain `SET`, not `NX` -- unlike `acquire`'s one-time bootstrap, this
    call means to replace the stored number. Does not touch `slot:<name>`
    (the holder sorted set): a lowered max does not evict anyone already
    holding a lease, since `_ACQUIRE_SCRIPT` only checks the max on a new
    acquire, never on an existing holder's renew. New acquires are turned
    away until enough holders release or expire to bring the count back
    under the new max.

    Raises `CoordinatorUnreachable` if Redis can't be reached -- there is no
    local-backend equivalent of this call to fall back to, for `bmo` or any
    other slot. A refused login raises `CoordinatorAuthFailed` for every slot,
    with no fallback. An ACL-denied command raises `NoPermissionError`.
    """
    client = _client(redis_host, redis_port, redis_username, redis_password)
    key = f"{PREFIX}slot:{slot}:max"
    try:
        _call_with_retry(lambda: client.set(key, max_holders))
    except redis.exceptions.AuthenticationError as exc:
        raise _auth_failed(exc) from exc
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise CoordinatorUnreachable(slot) from exc
    return max_holders


def acquire(
    slot: str,
    holder: str,
    *,
    wait: float = 0.0,
    ttl: float = local_slots.DEFAULT_TTL,
    max_holders: int | None = None,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
    state_root: str | Path | None = None,
) -> str:
    """Acquire a lease on `slot`, same contract as `slots.acquire` (blocks up
    to `wait` seconds, polling; raises `SlotFull` once `wait` elapses).

    Falls back to the `local` backend for `slot == "bmo"` if Redis is
    unreachable. Raises `CoordinatorUnreachable` for any other slot.
    A refused login raises `CoordinatorAuthFailed` for every slot, with no
    fallback. An ACL-denied command raises `NoPermissionError`.
    """
    client = _client(redis_host, redis_port, redis_username, redis_password)
    key = f"{PREFIX}slot:{slot}"
    ttl_ms = int(ttl * 1000)
    deadline = time.monotonic() + wait
    poll_interval = 0.2
    while True:
        try:
            max_n = _call_with_retry(lambda: _get_or_set_max(client, slot, max_holders))
            result = _call_with_retry(
                lambda: client.eval(_ACQUIRE_SCRIPT, 1, key, holder, _now_ms(), ttl_ms, max_n)
            )
        except redis.exceptions.AuthenticationError as exc:
            raise _auth_failed(exc) from exc
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
            if slot not in FALLBACK_SLOTS:
                raise CoordinatorUnreachable(slot) from exc
            _warn_fallback(slot, exc)
            return local_slots.acquire(
                slot, holder, wait=wait, ttl=ttl, max_holders=max_holders, state_root=state_root
            )
        if result:
            return f"{slot}:{holder}"
        if time.monotonic() >= deadline:
            raise SlotFull(slot)
        time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))


def renew(
    lease: str,
    *,
    ttl: float = local_slots.DEFAULT_TTL,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
    state_root: str | Path | None = None,
) -> bool:
    """Extend `lease`'s deadline. Return False if the lease is gone.

    A slot with no local fallback returns False if Redis is unreachable.
    `bmo` uses the local result instead.
    A refused login raises `CoordinatorAuthFailed` for every slot.
    An ACL-denied command raises `NoPermissionError`.
    """
    slot, holder = split_lease(lease)
    client = _client(redis_host, redis_port, redis_username, redis_password)
    key = f"{PREFIX}slot:{slot}"
    try:
        result = _call_with_retry(
            lambda: client.eval(_RENEW_SCRIPT, 1, key, holder, _now_ms(), int(ttl * 1000))
        )
        return bool(result)
    except redis.exceptions.AuthenticationError as exc:
        raise _auth_failed(exc) from exc
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        if slot not in FALLBACK_SLOTS:
            return False
        _warn_fallback(slot, exc)
        return local_slots.renew(lease, ttl=ttl, state_root=state_root)


def release(
    lease: str,
    *,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
    state_root: str | Path | None = None,
) -> bool:
    """Compare-and-delete release, same contract as `slots.release` (not an
    error to release twice). Falls back to the `local` backend for the
    `bmo` slot if Redis is unreachable. A refused login raises
    `CoordinatorAuthFailed` for every slot. An ACL-denied command raises
    `NoPermissionError`.
    """
    slot, holder = split_lease(lease)
    client = _client(redis_host, redis_port, redis_username, redis_password)
    key = f"{PREFIX}slot:{slot}"
    try:
        result = _call_with_retry(lambda: client.eval(_RELEASE_SCRIPT, 1, key, holder))
        return bool(result)
    except redis.exceptions.AuthenticationError as exc:
        raise _auth_failed(exc) from exc
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        if slot not in FALLBACK_SLOTS:
            raise CoordinatorUnreachable(slot) from exc
        _warn_fallback(slot, exc)
        return local_slots.release(lease, state_root=state_root)


def status(
    *,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
    state_root: str | Path | None = None,
) -> dict[str, dict]:
    """Return `{slot_name: {"holders": live_count, "max": max_or_None}}`,
    read from the sorted sets without pruning them (`ZREMRANGEBYSCORE` is an
    `acquire`/`renew` job, not this one's).

    Listed by `:max` key, not by the sorted set itself -- Redis deletes a
    sorted set once its last member is removed, but a slot that has gone
    back to zero holders still exists (same as the `local` backend, whose
    slot directory outlives its last holder file) and should still show up
    with `holders: 0`, not disappear from the report.

    Falls back to the `local` backend's status if Redis is unreachable.
    A refused login raises `CoordinatorAuthFailed`, with no fallback.
    An ACL-denied command raises `NoPermissionError`.
    """
    client = _client(redis_host, redis_port, redis_username, redis_password)
    try:
        now = _now_ms()
        result: dict[str, dict] = {}
        max_keys = _call_with_retry(lambda: list(client.scan_iter(match=f"{PREFIX}slot:*:max")))
        for max_key in max_keys:
            slot = max_key[len(f"{PREFIX}slot:") : -len(":max")]
            members = client.zrange(f"{PREFIX}slot:{slot}", 0, -1, withscores=True)
            live = sum(1 for _holder, score in members if score >= now)
            max_raw = client.get(max_key)
            result[slot] = {"holders": live, "max": int(max_raw) if max_raw is not None else None}
        return result
    except redis.exceptions.AuthenticationError as exc:
        raise _auth_failed(exc) from exc
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        _warn_fallback("status", exc)
        return local_slots.status(state_root=state_root)


def hold(
    command: list[str],
    *,
    lease: str | None = None,
    slot: str | None = None,
    holder: str | None = None,
    wait: float = 0.0,
    ttl: float = local_slots.DEFAULT_TTL,
    max_holders: int | None = None,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
    state_root: str | Path | None = None,
) -> int:
    """Same contract as `slots.hold`: acquire (unless `lease` is already
    held), run `command`, renew while it runs, release on exit.

    A renew or release error prints one line. The command's exit code is
    still returned.
    """
    if lease is None:
        if slot is None or holder is None:
            raise ValueError("hold needs either lease=, or slot= and holder=")
        lease = acquire(
            slot,
            holder,
            wait=wait,
            ttl=ttl,
            max_holders=max_holders,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_username=redis_username,
            redis_password=redis_password,
            state_root=state_root,
        )

    renew_failed = False
    renew_lost_reported = False

    def renew_during_run(lease_id: str) -> bool:
        # Report the first error only. The command keeps running.
        nonlocal renew_failed, renew_lost_reported
        if renew_failed:
            return False
        try:
            renewed = renew(
                lease_id,
                ttl=ttl,
                redis_host=redis_host,
                redis_port=redis_port,
                redis_username=redis_username,
                redis_password=redis_password,
                state_root=state_root,
            )
        except (CoordinatorUnreachable, redis.exceptions.NoPermissionError) as exc:
            renew_failed = True
            _report_lease_error("renewed", lease_id, exc)
            return False
        # False also means an unreachable non-bmo Redis. Keep renewing. Print once.
        if not renewed and not renew_lost_reported:
            renew_lost_reported = True
            print(f"lupin: lease {lease_id} not renewed. It may no longer be held.", file=sys.stderr)
        return renewed

    def release_after_run(lease_id: str) -> bool:
        # A failed release does not replace the command's exit code.
        try:
            return release(
                lease_id,
                redis_host=redis_host,
                redis_port=redis_port,
                redis_username=redis_username,
                redis_password=redis_password,
                state_root=state_root,
            )
        except (CoordinatorUnreachable, redis.exceptions.NoPermissionError) as exc:
            _report_lease_error("released", lease_id, exc)
            return False

    return run_with_lease(command, lease, ttl=ttl, renew=renew_during_run, release=release_after_run)


def _report_lease_error(action: str, lease: str, exc: Exception) -> None:
    """Print one line for a failed renew or release during `hold`.
    The message never contains the password.
    """
    if isinstance(exc, CoordinatorAuthFailed):
        detail = exc.for_user(FLAG_SETTING)
    elif isinstance(exc, redis.exceptions.NoPermissionError):
        reply = str(exc).rstrip(".")
        detail = (
            "redis denied the command. Check the user's ACL in docs/redis-schema.md. "
            f"Redis said: {reply}."
        )
    else:
        detail = "redis is unreachable."
    print(f"lupin: lease {lease} not {action}. {detail}", file=sys.stderr)
