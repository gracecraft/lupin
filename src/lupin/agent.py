"""The per-machine poll loop: claims and runs commands from `commands.py`'s
queue. Implements #27's design -- see that issue's design comment for the
full spec. `cli.py`'s `lupin agent` subcommand is the long-running process;
this module is what it runs.

Security model, checked in this order, and nothing after a failed check
ever reaches `subprocess.run`:
1. HMAC signature over the canonical payload (`commands.verify`).
2. `target` must equal this machine -- belt and suspenders. A command
   only reaches this machine's queue by key already, but a forged payload
   naming the wrong `target` field should still be caught, not trusted.
3. Not past `expires_at` (plus `commands.CLOCK_SKEW_S` grace).
4. `action` must be in `ACTIONS`, a fixed, explicit table. An action not
   in it is rejected, never run as a best-effort guess.
5. If this machine's own record (`machines.py`) says `draining`, only
   `DRAIN_ALLOWED` actions run -- a draining machine can still wind work
   down (`loop.stop`) but refuses to start anything new.
6. Per-action parameter validation (e.g. `repo` against a strict regex,
   defense in depth independent of whatever a future dashboard checks).

`ACTIONS` calls Lupin commands rather than reimplementing loop behavior.
Every handler returns a list of argv, never a shell string. Execution is
always `subprocess.run(argv, shell=False)`.

- Loop actions run Lupin directly as this machine's service user. `run`
  starts a separate systemd worker, so the queue action can return at once.
  Stop and schedule commands manage their own systemd units.
- Read-only actions (`loop.peek`, `loop.state`, `schedule.show`) also run
  Lupin directly.

`schedule.set` values are checked before Lupin writes a timer drop-in.
Lupin checks them again before writing. `systemd-analyze` checks calendar
syntax when it is available.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable

import redis

from . import commands, loop_runtime, machines
from .slots import CoordinatorUnreachable
from .slots_redis import CONNECT_TIMEOUT, DEBRIEF_TIMEOUT_S, _call_with_retry, _client, debrief_client

DEFAULT_BATCH = 20  # design: "ZRANGE the oldest 20"
DEFAULT_POLL_INTERVAL = 2.0
PERIODIC_CHECK_S = 300.0  # Seconds between checks for due periodic debriefs.
EXEC_TIMEOUT_S = 120.0
# Budget for loop.stop, in seconds. The stop subprocess gets what is left
# after the command read and the Redis calls after it. See docs/redis-schema.md.
ACTION_TIMEOUT_S = {"loop.stop": 1740.0}
REDIS_ADDRESSES = 2  # IPv6 and IPv4 for localhost
# One command on a new connection takes five Redis requests. The client
# first sends HELLO 3, CLIENT MAINT_NOTIFICATIONS ON, CLIENT SETINFO
# LIB-NAME, and CLIENT SETINFO LIB-VER. Then it sends the command.
# The test test_command_takes_five_round_trips checks this.
REDIS_ROUND_TRIPS = 5
ATTEMPTS_PER_CALL = 2  # _call_with_retry makes two attempts
# Worst case for one debrief_client call, in seconds. Each attempt waits
# for one connect per address, then for each round trip. Each wait is up to DEBRIEF_TIMEOUT_S.
DEBRIEF_CALL_WORST_S = ATTEMPTS_PER_CALL * (REDIS_ADDRESSES + REDIS_ROUND_TRIPS) * DEBRIEF_TIMEOUT_S
# Worst case for the command read, in seconds. The read uses _client, with
# redis-py's default of 10 retries and a backoff of up to 1 s between tries.
# Each try waits for one connect per address and for each round trip. Each
# wait is up to CONNECT_TIMEOUT.
DEFAULT_CLIENT_RETRIES = 10
DEFAULT_CLIENT_BACKOFF_CAP_S = 1.0
COMMAND_READ_WORST_S = ATTEMPTS_PER_CALL * (
    (DEFAULT_CLIENT_RETRIES + 1) * (REDIS_ADDRESSES + REDIS_ROUND_TRIPS) * CONNECT_TIMEOUT
    + DEFAULT_CLIENT_RETRIES * DEFAULT_CLIENT_BACKOFF_CAP_S
)
# The agent makes up to five Redis calls on the loop.stop path after the read.
# The calls are: claim, claim read, write result, dequeue, and audit line.
# The claim read runs only when the claim returns nil. They use debrief_client.
AGENT_REDIS_CALLS_ON_STOP = 5
# Cap on the stop subprocess. The read and the agent's Redis calls use the rest of the limit.
SUBPROCESS_TIMEOUT_S = {
    "loop.stop": ACTION_TIMEOUT_S["loop.stop"] - COMMAND_READ_WORST_S - AGENT_REDIS_CALLS_ON_STOP * DEBRIEF_CALL_WORST_S,
}
OUTPUT_CAP = 8192  # 8 KiB, combined stdout+stderr -- design's "last 8 KiB combined"

# Defense in depth: queue commands must pass this strict repo-name check.
_REPO_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
# These limits are big enough for a real value. They are small enough to
# stop someone from hiding something else inside a long string.
_CAL_EXPR_MAX_LEN = 256
_SCHEDULE_TOKEN_MAX_LEN = 64
_DEFAULT_PEEK_LINES = 60
_MAX_PEEK_LINES = 5000


class RejectedCommand(Exception):
    """A command whose params don't fit its action -- caught before
    `subprocess.run`, turned into a `rejected` result, never executed."""


def _validate_repo(repo) -> str:
    if not isinstance(repo, str) or not _REPO_RE.match(repo):
        raise RejectedCommand(f"invalid repo {repo!r}")
    return repo


def _validate_peek_lines(value) -> int:
    if value is None:
        return _DEFAULT_PEEK_LINES
    try:
        lines = int(value)
    except (TypeError, ValueError):
        raise RejectedCommand(f"invalid lines {value!r}")
    if not (1 <= lines <= _MAX_PEEK_LINES):
        raise RejectedCommand(f"lines must be 1-{_MAX_PEEK_LINES}, got {lines}")
    return lines


def _reject_unsafe_text(label: str, value, max_len: int) -> str:
    """Check one string field before it is written into a config file or
    passed as a Lupin argument. Several fields share this check. A
    newline or other control character in any of them is the same kind
    of injection risk as the `cal` expression described in the module
    docstring."""
    if not isinstance(value, str) or not value:
        raise RejectedCommand(f"{label} must be a non-empty string")
    if len(value) > max_len:
        raise RejectedCommand(f"{label} longer than {max_len} characters")
    if any(ord(c) < 0x20 or ord(c) == 0x7F for c in value):
        raise RejectedCommand(f"{label} contains a newline or control character")
    return value


def validate_cal_expr(expr) -> str:
    """Check characters and length only. No subprocess call, so this
    check (and its tests) still run where `systemd-analyze` is not
    installed. See `_check_cal_expr_with_systemd_analyze` for the real
    syntax check on top."""
    return _reject_unsafe_text("cal expression", expr, _CAL_EXPR_MAX_LEN)


def validate_schedule_token(label: str, value) -> str:
    return _reject_unsafe_text(f"schedule {label}", value, _SCHEDULE_TOKEN_MAX_LEN)


def _check_cal_expr_with_systemd_analyze(expr: str) -> None:
    """Ask systemd whether `expr` is a valid calendar expression. This is
    the same check a person would run by hand before trusting one.

    If `systemd-analyze` is not on PATH, this function does nothing --
    it does not pretend the check passed. That way, a sandbox without
    the binary still runs `validate_cal_expr` above, instead of skipping
    all checks.
    """
    binary = shutil.which("systemd-analyze")
    if binary is None:
        return
    proc = subprocess.run([binary, "calendar", expr], capture_output=True, text=True, timeout=5.0)
    if proc.returncode != 0:
        raise RejectedCommand(f"not a valid calendar expression: {expr!r}")


def _handle_repo_enable(params: dict, cmd_id: str) -> list[str]:
    repo = _validate_repo(params.get("repo"))
    return ["lupin", "enable", repo]


def _handle_loop_stop(params: dict, cmd_id: str) -> list[str]:
    repo = _validate_repo(params.get("repo"))
    force = params.get("force", False)
    if not isinstance(force, bool):
        raise RejectedCommand("force must be true or false")
    return ["lupin", "loop", "local-action", "stop", repo, *(["--force"] if force else [])]


def _validate_platform(value) -> str:
    if not isinstance(value, str) or value not in {"claude", "omp"}:
        raise RejectedCommand("platform must be claude or omp")
    return value

def _validate_omp_options(params: dict, platform: str | None) -> list[str]:
    provider = params.get("provider")
    model = params.get("model")
    if provider is not None and (
        not isinstance(provider, str) or provider not in {"openai", "opencode-go"}
    ):
        raise RejectedCommand("provider must be openai or opencode-go")
    if model is not None and (
        not isinstance(model, str)
        or not model
        or len(model) > 256
        or any(ord(character) < 32 or ord(character) == 127 for character in model)
    ):
        raise RejectedCommand("invalid OMP model")
    if (provider is not None or model is not None) and platform != "omp":
        raise RejectedCommand("provider and model require platform omp")
    flags = []
    if provider is not None:
        flags.extend(["--provider", provider])
    if model is not None:
        flags.extend(["--model", model])
    return flags


def _validate_note(value) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > 8000 or "\x00" in value:
        raise RejectedCommand("note is invalid or too long")
    return value


def _handle_loop_run(params: dict, cmd_id: str) -> list[str]:
    repo = _validate_repo(params.get("repo"))
    argv = ["lupin", "run", repo]
    platform = params.get("platform")
    if platform is not None:
        platform = _validate_platform(platform)
        argv.extend(["--platform", platform])
    argv.extend(_validate_omp_options(params, platform))
    note = _validate_note(params.get("note"))
    if note is not None:
        argv.extend(["--note", note])
    resume = params.get("resume", False)
    if not isinstance(resume, bool):
        raise RejectedCommand("resume must be a boolean")
    if resume:
        argv.append("--resume")
    return argv


def _handle_loop_run_all(params: dict, cmd_id: str) -> list[str]:
    argv = ["lupin", "run", "--all"]
    platform = params.get("platform")
    if platform is not None:
        platform = _validate_platform(platform)
        argv.extend(["--platform", platform])
    argv.extend(_validate_omp_options(params, platform))
    note = _validate_note(params.get("note"))
    if note is not None:
        argv.extend(["--note", note])
    return argv


def _handle_loop_peek(params: dict, cmd_id: str) -> list[str]:
    repo = _validate_repo(params.get("repo"))
    lines = _validate_peek_lines(params.get("lines"))
    return ["lupin", "loop", "local-action", "peek", repo, str(lines)]


_MAX_SEND_CHARS = 2000


def _validate_send_text(value) -> str:
    # One printable line. A newline would submit before the user is done typing.
    if not isinstance(value, str) or not value.strip():
        raise RejectedCommand("text must be a non-empty string")
    if len(value) > _MAX_SEND_CHARS or not value.isprintable():
        raise RejectedCommand(f"text must be one printable line of at most {_MAX_SEND_CHARS} characters")
    return value


def _handle_loop_send(params: dict, cmd_id: str) -> list[str]:
    repo = _validate_repo(params.get("repo"))
    text = _validate_send_text(params.get("text"))
    return ["lupin", "loop", "local-action", "send", repo, text]


def _handle_loop_state(params: dict, cmd_id: str) -> list[str]:
    repo = _validate_repo(params.get("repo"))
    return ["lupin", "loop", "local-action", "state", repo]


def _handle_schedule_show(params: dict, cmd_id: str) -> list[str]:
    return ["lupin", "schedule"]


def _handle_schedule_set(params: dict, cmd_id: str) -> list[str]:
    mode = params.get("mode")
    if mode == "cal":
        expr = validate_cal_expr(params.get("expr"))
        _check_cal_expr_with_systemd_analyze(expr)
        return ["lupin", "schedule", "cal", expr]
    if mode == "first":
        when = validate_schedule_token("when", params.get("when"))
        interval = validate_schedule_token("interval", params.get("interval"))
        return ["lupin", "schedule", "first", when, "every", interval]
    raise RejectedCommand(f"unknown schedule mode {mode!r}")


def _handle_schedule_pause(params: dict, cmd_id: str) -> list[str]:
    return ["lupin", "pause"]


def _handle_schedule_resume(params: dict, cmd_id: str) -> list[str]:
    return ["lupin", "resume"]


# Fixed, explicit allowlist -- the only actions this process will ever run.
# An action not in this table is rejected, not attempted.
ACTIONS = {
    "loop.stop": _handle_loop_stop,
    "repo.enable": _handle_repo_enable,
    "loop.run": _handle_loop_run,
    "loop.run-all": _handle_loop_run_all,
    "loop.peek": _handle_loop_peek,
    "loop.send": _handle_loop_send,
    "loop.state": _handle_loop_state,
    "schedule.show": _handle_schedule_show,
    "schedule.set": _handle_schedule_set,
    "schedule.pause": _handle_schedule_pause,
    "schedule.resume": _handle_schedule_resume,
}

# Draining hosts accept actions that stop work or read its state.
DRAIN_ALLOWED = {"loop.stop", "loop.peek", "loop.state", "schedule.show", "schedule.pause"}


def _write_result(client, cmd_id: str, payload: dict, *, overwrite: bool = False) -> bool:
    value = json.dumps(payload)
    key = commands.res_key(cmd_id)
    px = int(commands.RESULT_TTL_S * 1000)
    if overwrite:
        return bool(_call_with_retry(lambda: client.set(key, value, px=px)))
    return bool(_call_with_retry(lambda: client.set(key, value, nx=True, px=px)))


def _reject(client, machine: str, cmd_id: str, action: str | None, reason: str) -> dict:
    _write_result(client, cmd_id, {"id": cmd_id, "state": "rejected", "host": machine, "reason": reason})
    _call_with_retry(lambda: client.zrem(commands.queue_key(machine), cmd_id))
    commands.log_event(
        client, {"id": cmd_id, "machine": machine, "state": "rejected", "action": action, "reason": reason}
    )
    return {"id": cmd_id, "state": "rejected"}


def _is_draining(client, machine: str) -> bool:
    """Read this machine's own `machine:<name>` record (written by
    `machines.join`/`heartbeat`) and check its `state`. Reuses that
    module's key format directly instead of a second copy of it."""
    raw = _call_with_retry(lambda: client.get(machines._record_key(machine)))
    if raw is None:
        return False
    return json.loads(raw).get("state") == "draining"


def _mark_expired(client, machine: str, cmd_id: str) -> dict:
    _write_result(client, cmd_id, {"id": cmd_id, "state": "expired", "host": machine})
    _call_with_retry(lambda: client.zrem(commands.queue_key(machine), cmd_id))
    commands.log_event(client, {"id": cmd_id, "machine": machine, "state": "expired", "reason": "ttl"})
    return {"id": cmd_id, "state": "expired"}


def _process_one(client, stop_client, machine: str, key: str, cmd_id: str) -> dict:
    # The read uses `client` for every action, because the action is
    # not known until the command is read. loop.stop has a time limit, so
    # its later Redis calls use `stop_client`, which is bounded.
    raw = _call_with_retry(lambda: client.get(commands.cmd_key(cmd_id)))
    if raw is None:
        # Gone from Redis already (its own retention TTL, or evicted under
        # memory pressure) -- same outcome either way.
        return _mark_expired(client, machine, cmd_id)
    cmd = json.loads(raw)
    action = cmd.get("action")
    if action == "loop.stop":
        client = stop_client

    if not commands.verify(cmd, key):
        return _reject(client, machine, cmd_id, action, "bad signature")
    if cmd.get("target") != machine:
        return _reject(client, machine, cmd_id, action, f"addressed to {cmd.get('target')!r}")
    if time.time() >= cmd.get("expires_at", 0) + commands.CLOCK_SKEW_S:
        return _mark_expired(client, machine, cmd_id)
    if action not in ACTIONS:
        return _reject(client, machine, cmd_id, action, f"unknown action {action!r}")
    if action not in DRAIN_ALLOWED and _is_draining(client, machine):
        return _reject(client, machine, cmd_id, action, f"{machine} is draining")

    # Build the argv (and so validate the params) before claiming -- a
    # rejection has to land while no `cmdres` exists yet, so `_reject`'s
    # `SET ... NX` actually writes "rejected" instead of silently losing to
    # a "running" claim already sitting there.
    params = cmd.get("params") or {}
    try:
        argv = ACTIONS[action](params, cmd_id)
    except RejectedCommand as exc:
        return _reject(client, machine, cmd_id, action, str(exc))

    started_at = time.time()
    # New for each attempt. After a retry, the poll checks for this token.
    claim = uuid.uuid4().hex
    claimed = _call_with_retry(
        lambda: client.set(
            commands.res_key(cmd_id),
            json.dumps(
                {
                    "id": cmd_id,
                    "state": "running",
                    "host": machine,
                    "action": action,
                    "started_at": started_at,
                    "claim": claim,
                }
            ),
            nx=True,
            px=int(commands.RESULT_TTL_S * 1000),
        )
    )
    if not claimed:
        # SET NX returns nil when the key exists. That can be this attempt's
        # own claim, after a lost reply, or another poller's. The claim token
        # shows which.
        raw = _call_with_retry(lambda: client.get(commands.res_key(cmd_id)))
        held = json.loads(raw) if raw else {}
        if held.get("state") != "running" or held.get("claim") != claim:
            # Another poller racing on the same id claimed it first. Do not
            # touch the queue or run anything. The poller that made the
            # claim finishes the job, including the dequeue.
            return {"id": cmd_id, "state": "lost-race"}

    try:
        proc = subprocess.run(argv, shell=False, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S.get(action, EXEC_TIMEOUT_S))
        combined = (proc.stdout or "") + (proc.stderr or "")
        payload = {
            "id": cmd_id,
            "state": "ok" if proc.returncode == 0 else "failed",
            "host": machine,
            "action": action,
            "started_at": started_at,
            "finished_at": time.time(),
            "exit_code": proc.returncode,
            "output": combined[-OUTPUT_CAP:],
            "truncated": len(combined) > OUTPUT_CAP,
        }
    except Exception as exc:  # subprocess failed to even start, or timed out
        payload = {
            "id": cmd_id,
            "state": "failed",
            "host": machine,
            "action": action,
            "started_at": started_at,
            "finished_at": time.time(),
            "reason": str(exc),
        }

    _write_result(client, cmd_id, payload, overwrite=True)
    _call_with_retry(lambda: client.zrem(commands.queue_key(machine), cmd_id))
    commands.log_event(client, {"id": cmd_id, "machine": machine, "state": payload["state"], "action": action})
    return {"id": cmd_id, "state": payload["state"]}


def startup_scan(
    machine: str,
    *,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
) -> list[str]:
    """Run once before polling starts. A command this host claimed (its own
    `cmdres` still says `running`) but never finished means the previous
    process crashed mid-run -- mark it `failed` rather than silently
    re-running it on restart. Returns the ids it marked failed.
    """
    client = _client(redis_host, redis_port, redis_username, redis_password)
    try:
        ids = _call_with_retry(lambda: client.zrange(commands.queue_key(machine), 0, -1))
        marked = []
        for cmd_id in ids:
            raw = _call_with_retry(lambda c=cmd_id: client.get(commands.res_key(c)))
            if raw is None:
                continue
            result = json.loads(raw)
            if result.get("state") != "running" or result.get("host") != machine:
                continue
            result["state"] = "failed"
            result["reason"] = "orphaned: still running when the agent restarted"
            result["finished_at"] = time.time()
            _write_result(client, cmd_id, result, overwrite=True)
            _call_with_retry(lambda c=cmd_id: client.zrem(commands.queue_key(machine), c))
            commands.log_event(client, {"id": cmd_id, "machine": machine, "state": "failed", "reason": "startup-scan"})
            marked.append(cmd_id)
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise CoordinatorUnreachable(machine) from exc
    return marked


def poll_once(
    machine: str,
    key: str,
    *,
    batch: int = DEFAULT_BATCH,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
) -> list[dict]:
    """One pass: prune queue entries past the retention window, then claim
    and run up to `batch` of the oldest remaining pending commands. Returns
    a summary per id touched, in the order handled.
    """
    client = _client(redis_host, redis_port, redis_username, redis_password)
    qkey = commands.queue_key(machine)
    touched: list[dict] = []
    try:
        # Safety-net prune: entries older than the retention window are
        # long past `expires_at` too (120s default vs. 1h here) -- this
        # just stops the queue growing forever if something is never
        # polled. The real "is this still runnable" check is `expires_at`,
        # done per-item below.
        cutoff_ms = commands.now_ms() - int(commands.CMD_RETENTION_S * 1000)
        _call_with_retry(lambda: client.zremrangebyscore(qkey, "-inf", cutoff_ms))

        pending = _call_with_retry(lambda: client.zrange(qkey, 0, batch - 1))
        stop_client = debrief_client(redis_host, redis_port, redis_username, redis_password)
        for cmd_id in pending:
            touched.append(_process_one(client, stop_client, machine, key, cmd_id))
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise CoordinatorUnreachable(machine) from exc
    return touched


def _run_periodic_debriefs(check: Callable[[], None], interval: float) -> None:
    """Call `check` now, then every `interval` seconds. Runs on a daemon thread.

    A failed call prints a warning. The loop keeps running.
    """
    while True:
        try:
            check()
        except Exception as exc:
            print(f"lupin agent: periodic debrief check failed: {exc}", file=sys.stderr)
        time.sleep(interval)


def run_forever(
    machine: str,
    key: str,
    *,
    batch: int = DEFAULT_BATCH,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
) -> None:
    """The `lupin agent` process: one crash-recovery pass, then poll forever.
    Doesn't return under normal operation."""
    conn = dict(redis_host=redis_host, redis_port=redis_port, redis_username=redis_username, redis_password=redis_password)
    startup_scan(machine, **conn)
    # Periodic debriefs run on their own thread. A slow debrief does not delay command polls.
    threading.Thread(
        target=_run_periodic_debriefs,
        args=(loop_runtime.write_due_periodic_debriefs, PERIODIC_CHECK_S),
        daemon=True,
    ).start()
    while True:
        poll_once(machine, key, batch=batch, **conn)
        time.sleep(poll_interval)
