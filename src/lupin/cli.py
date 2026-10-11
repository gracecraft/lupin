"""The `lupin` command-line entry point: one executable, subcommands.

`route` and `classify` are the model-routing calls, moved out of
ghostbook.nix in issue #203. `fetch-models` fetches a daily snapshot of
which model IDs each subscription can call today and what they cost
(issue #16; see `model_fetch.py`). `fetch-benchmarks` fetches (or reads the
fleet-shared cache of) a daily benchmark/quality score per model, via
sharded free-model `omp` agent calls, not a local file (issue #17's
reopen; see `benchmark_fetch.py`). `quota` publishes provider quota and
this machine's `7-day usage totals to shared Redis for the dashboard.
`acquire`/`hold`/`release`/`status` are the
slot-lease commands (issue #205 for the `local` backend, #210 for
`redis`). `ledger` appends and reads shared repository events in Redis
(see `ledger.py`). `claim`/`renew-claim`/`release-claim` mark a GitHub issue
as one loop's own, so two loops never work the same task (issue #6; Redis only,
no `--backend` choice -- see `claims.py`). `review-route` picks which lock a
routed model needs (issue #185), `roadmap` prints prioritized open tasks
from `roadmap.py`'s data plus claims (issue #10), and `serve` runs the
read-only dashboard (issue #204). `join`/`heartbeat`/`drain`/`undrain`/
`machines` are the fleet machine registry (issue #7); see `machines.py` for
the Redis record they read and write. `place` picks which registered
machine should run a task already routed to a model (issue #9; see
`place.py`). `quest` lists quests (GitHub issues labeled `quest`) and their
progress (issue #11, read-only); `quest focus`/`quest release` pin a quest
to a machine (issue #12); `start`/`stop` claim and release a quest's issues
(issue #13). `reconcile` applies the automatic release rules -- a claim
with no heartbeat, a quest focus that is done/closed/idle/down, a started
quest that is done/down (issue #14; see `reconcile.py`). `cmd` sends,
checks, or lists signed cross-machine commands; `agent` is the
long-running process that polls its own queue and runs them through a
fixed action table (issue #28, implementing #27's design; see
`run` starts a loop, `once` starts or schedules one, and `enable`/`disable`
edit the local repo list. `loops` reads Herdr's agent state. `stop`/`peek`/
`schedule`/`pause`/`resume` can target any fleet machine. Local actions run
through Lupin. Remote actions use the signed Redis queue and `agent.py`.
`attach` is direct: it runs the Herdr terminal here or connects to the
remote Herdr server over SSH. It never uses the queue. `loops.py` owns the
shared local-or-remote dispatch.

Slot commands use `--backend local|redis`, or `LUPIN_BACKEND` if the flag is
not set. A host can set the default once in its systemd environment.

Exit codes, by design (see #198's architecture plan):
  0  done
  2  busy/full (acquire, hold) -- skip and try again later, or a usage error
     from argparse itself (its own default for a bad flag; both meanings are
     "this invocation didn't produce a result", so sharing the code is fine).
     `place` reuses this code too: no online machine runs the routed
     provider right now, which is the same "try again later" shape.
  3  cannot reach the coordinator, and this slot has no local fallback. The
     `local` backend's coordinator is the filesystem, which it always
     reaches once the state root is writable, so it never returns 3. The
     `redis` backend returns 3 for any slot other than `bmo` when Redis is
     unreachable -- `bmo` falls back to the `local` backend instead (see
     `slots_redis.py`), so it does not reach this exit code. Claims have no
     local fallback at all, so `claim`/`renew-claim`/`release-claim` return
     3 for every unreachable-Redis case. For every slot, `acquire` and
     `release` return 3 when Redis refuses a login. `bmo` does not fall back
     for this. They also return 3 when the ACL denies a command. The message
     names the cause.
  1  any other error (malformed lease id, bad JSON input, hold with neither
     --lease nor <slot>/--holder, etc.) -- also `renew-claim`/`release-claim`
     when the caller isn't the claim's current holder.

`claim` returns 2 when another holder already has the issue -- same "busy,
skip and try again later" meaning as a full slot. `quest start` reuses the
same two codes for its own validation failures: 2 for "claimed by another
loop" or "target machine draining" (both "try again later"), 1 for
anything else (closed, missing, or blocked by an issue outside the quest).

`stop`/`peek`/`schedule`/`pause`/`resume` (issue #2 phase A) add two more
exit codes. Both apply only to a remote target, sent through the signed
command queue:
  4  sent, but the result is unknown. The command was queued, but no
     final state (`ok`/`failed`/`rejected`/`expired`) arrived within
     `--wait` seconds. It may still be running -- check
     `lupin cmd status <id>` later.
  5  the repo's loop matched zero machines, or more than one, in the
     fleet registry's `loops` field (`stop`/`peek` only, and only when
     `--machine` isn't given). Pass `--machine` instead of guessing.
"""

from __future__ import annotations

import argparse
import functools
import json
import os
import shlex
import sys
import time
from datetime import datetime

import redis

from . import agent as agent_mod
from . import benchmark_fetch
from . import claims
from . import classify as classify_mod
from . import commands
from . import ledger
from . import loops as loops_mod
from . import machines
from . import model_fetch
from . import place as place_mod
from . import quest as quest_mod
from . import quota
from . import quota_cache
from . import usage_cache
from . import reconcile as reconcile_mod
from . import review_dispatch
from . import roadmap
from . import roadmap_cli
from . import route as route_mod
from . import serve
from . import slots
from . import slots_redis
from . import loop_runtime

DEFAULT_RESULT_WAIT_S = 20.0  # how long stop/peek/schedule/pause/resume
# wait, by default, for a remote result before they report exit code 4
# ("sent, result unknown"). This is not the time limit for the action
# itself. agent.py sets that. Other actions use EXEC_TIMEOUT_S. For
# loop.stop on the queue, the stop subprocess uses SUBPROCESS_TIMEOUT_S,
# which is derived from ACTION_TIMEOUT_S. A local loop.stop uses
# ACTION_TIMEOUT_S as its limit. A caller who wants to wait longer than
# 20 seconds passes --wait.


def _route_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("category")
    parser.add_argument("size")
    parser.add_argument(
        "--no-bmo",
        dest="bmo_available",
        action="store_false",
        default=True,
        help="bmo's lock already timed out -- skip a bmo-dependent tier0 pick",
    )
    parser.add_argument("--primary-effort", default=None)
    parser.add_argument("--json", action="store_true")


def _classify_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--issue-json", required=True, help="path to a `gh issue view --json ...` file")
    parser.add_argument("--diff-stat", default=None, help="path to a `git diff --stat` file")
    parser.add_argument("--json", action="store_true")


def _fetch_models_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--snapshot-file", default=model_fetch.SNAPSHOT_FILE,
        help="where to save the fetched snapshot (default: ~/.local/state/lupin/model-snapshot.json)",
    )
    parser.add_argument("--no-write", action="store_true", help="print the snapshot without saving it")
    parser.add_argument("--json", action="store_true")
    _fleet_connection_args(parser)


def _fetch_benchmarks_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--force", action="store_true",
        help="skip the freshness cache and pull now, even if a fresh snapshot already exists",
    )
    parser.add_argument("--json", action="store_true")
    # Fleet-shared, not per-machine (issue #17's reopen) -- same connection
    # resolution as `machines`/`place`, so a machine that already ran
    # `lupin join` doesn't need to repeat its Redis location here.
    _fleet_connection_args(parser)


def _quota_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true")
    # Same fleet-shared connection resolution as `fetch-benchmarks` -- this
    # command both reads and (if this machine has real credentials for a
    # provider) publishes to the shared quota cache (issue #38).
    _fleet_connection_args(parser)


def _state_root_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--state-root", default=None, help="default: $LUPIN_STATE_ROOT or ~/.lupin/slots")


def _redis_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--redis-host", default=os.environ.get("LUPIN_REDIS_HOST", "localhost"),
        help="default: $LUPIN_REDIS_HOST or localhost",
    )
    parser.add_argument(
        "--redis-port", type=int, default=int(os.environ.get("LUPIN_REDIS_PORT", "6379")),
        help="default: $LUPIN_REDIS_PORT or 6379",
    )
    parser.add_argument(
        "--redis-username", default=os.environ.get("LUPIN_REDIS_USERNAME"),
        help="default: $LUPIN_REDIS_USERNAME, no auth if unset",
    )
    parser.add_argument(
        "--redis-password", default=os.environ.get("LUPIN_REDIS_PASSWORD"),
        help="default: $LUPIN_REDIS_PASSWORD, no auth if unset",
    )


def _backend_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--backend", choices=["local", "redis"], default=os.environ.get("LUPIN_BACKEND", "local"),
        help="slot-lease backend (default: $LUPIN_BACKEND or local)",
    )
    _redis_args(parser)


def _quest_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "mode", nargs="?", choices=["status", "start", "stop", "focus", "release"], default=None,
        help=(
            "omit to list every quest; 'status' for one quest's task breakdown; "
            "'start' to claim --issue N as a quest, 'stop' to release one; "
            "'focus'/'release' to pin or unpin a quest's focus machine"
        ),
    )
    parser.add_argument(
        "id", nargs="?", default=None,
        help="quest name or issue number (status, focus, release); quest id (stop)",
    )
    parser.add_argument(
        "--issue", dest="issues", type=int, action="append", default=None,
        help="an issue to ship; repeat for each (start only)",
    )
    parser.add_argument(
        "--machine", default=None,
        help="run on this machine (start); target machine (focus, default: most free slots)",
    )
    parser.add_argument("--platform", default=None, help="force a provider (start only)")
    parser.add_argument("--note", default=None, help="extra instruction for the loop (start only)")
    parser.add_argument(
        "--pin", action="store_true", help="with focus: keep the focus until explicitly released"
    )
    parser.add_argument("--json", action="store_true")
    _redis_args(parser)


def _reconcile_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--ttl", type=float, default=300.0,
        help="how long this run may hold the fleet-wide reconcile slot, in seconds",
    )
    parser.add_argument("--json", action="store_true")
    _redis_args(parser)


def _slot_common_args(parser: argparse.ArgumentParser) -> None:
    _state_root_arg(parser)
    _backend_args(parser)
    parser.add_argument("--ttl", type=float, default=slots.DEFAULT_TTL, help="lease TTL in seconds")


def _acquire_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("slot")
    parser.add_argument("--holder", required=True)
    parser.add_argument("--wait", type=float, default=0.0, help="seconds to poll before giving up")
    parser.add_argument(
        "--max", dest="max_holders", type=int, default=None,
        help="slot capacity, used only the first time this slot is created",
    )
    _slot_common_args(parser)


def _hold_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("slot", nargs="?", default=None)
    parser.add_argument("--holder", default=None)
    parser.add_argument("--lease", default=None, help="reuse a lease an earlier `acquire` returned")
    parser.add_argument("--wait", type=float, default=0.0)
    parser.add_argument("--max", dest="max_holders", type=int, default=None)
    _slot_common_args(parser)


def _release_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--lease", required=True)
    _state_root_arg(parser)
    _backend_args(parser)


def _status_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true")
    _state_root_arg(parser)
    _backend_args(parser)


def _redis_conn_args(parser: argparse.ArgumentParser) -> None:
    """Connection flags for the claim subcommands -- same env-var defaults
    as `_backend_args`, but no `--backend` choice: claims only ever live in
    Redis, there is no `local` backend for them to pick (see `claims.py`).
    """
    parser.add_argument(
        "--redis-host", default=os.environ.get("LUPIN_REDIS_HOST", "localhost"),
        help="default: $LUPIN_REDIS_HOST or localhost",
    )
    parser.add_argument(
        "--redis-port", type=int, default=int(os.environ.get("LUPIN_REDIS_PORT", "6379")),
        help="default: $LUPIN_REDIS_PORT or 6379",
    )
    parser.add_argument(
        "--redis-username", default=os.environ.get("LUPIN_REDIS_USERNAME"),
        help="default: $LUPIN_REDIS_USERNAME, no auth if unset",
    )
    parser.add_argument(
        "--redis-password", default=os.environ.get("LUPIN_REDIS_PASSWORD"),
        help="default: $LUPIN_REDIS_PASSWORD, no auth if unset",
    )


def _claim_args(parser: argparse.ArgumentParser) -> None:
    """Shared by `claim` and `renew-claim` -- both take a TTL."""
    parser.add_argument("target", help="OWNER/REPO#N, e.g. gracecraft/lupin#6")
    parser.add_argument("--holder", required=True)
    parser.add_argument("--ttl", type=float, default=claims.DEFAULT_TTL, help="claim TTL in seconds")
    _redis_conn_args(parser)


def _release_claim_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("target", help="OWNER/REPO#N, e.g. gracecraft/lupin#6")
    parser.add_argument("--holder", required=True)
    _redis_conn_args(parser)


def _ledger_args(parser: argparse.ArgumentParser) -> None:
    modes = parser.add_subparsers(dest="ledger_action", required=True)

    append = modes.add_parser("append", help="append one event to a repository ledger")
    append.add_argument("repo", help="OWNER/REPO")
    append.add_argument("--event", required=True)
    append.add_argument("--issue", type=int)
    append.add_argument("--status")
    append.add_argument("--branch")
    append.add_argument("--summary")
    for field in ("highlights", "evidence", "decisions", "next"):
        append.add_argument(f"--{field}", action="append", default=[])
    append.add_argument("--child", action="append", type=int, default=[])
    append.add_argument("--json", action="store_true")
    _fleet_connection_args(append)

    read = modes.add_parser("read", help="read a repository ledger")
    read.add_argument("repo", help="OWNER/REPO")
    read.add_argument(
        "--limit", type=int, default=10, metavar="N",
        help="number of latest events to return (default: 10)",
    )
    read.add_argument("--json", action="store_true")
    _fleet_connection_args(read)


def _roadmap_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repo", default=None, help="only this repo (default: every enabled repo)")
    parser.add_argument("--limit", type=int, default=10, help="show the top N (default: 10)")
    parser.add_argument("--stage", choices=["ready", "blocked", "all"], default="ready")
    parser.add_argument("--dag", action="store_true", help="draw dependencies between tasks")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--refresh", action="store_true", help="bypass the GitHub issue cache")
    # `roadmap` marks claimed issues via claims.py's claims_for(), which needs
    # the same Redis connection info as `claim`/`renew-claim`/`release-claim`
    # -- without these, claims_for() always connects to localhost:6379 with
    # no auth, so it silently misses every claim on a real deployment.
    _redis_conn_args(parser)


def _serve_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bind", default="127.0.0.1", help="loopback address only")
    parser.add_argument("--port", type=int, default=8788)
    parser.add_argument("--peek-lines", type=int, default=25, help="tail lines shown per loop")
    parser.add_argument("--roadmap", metavar="REPO", help="print a repository roadmap and exit")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--json", action="store_true")
    # Lets `lupin serve --redis-host ...` parse at this top level (issue
    # #15's dashboard reads the fleet registry) -- `main()` still hands
    # `serve`'s own argv to `serve.main()` rather than this parsed
    # Namespace (see `main()` below), so serve.py's own parser repeats
    # these same flags; this call only keeps `--help` and top-level
    # parsing in sync with what serve.py actually accepts. The Machines
    # page (#20) reads machines.machines(), which needs this same
    # fleet-config-aware resolution, not _redis_conn_args's hardcoded
    # localhost default -- see _machines_args, which uses the same helper.
    _fleet_connection_args(parser)


def _review_route_args(parser: argparse.ArgumentParser) -> None:
    """`lupin review-route` — route a pair and report which lock it needs.

    Or, with `--prefetch`, fetch issue/PR text instead of routing (#1) --
    see review_dispatch.py's own `_parse_args` for this command's real
    argument parsing; this copy is only for `--help` output, since `main()`
    hands `review-route`'s raw argv to `review_dispatch.main()` directly.
    """
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--category", help="task category, with --size")
    source.add_argument(
        "--issue-json", help="path to a `gh issue view --json ...` file to classify"
    )
    parser.add_argument("--size", help="task size label, with --category")
    parser.add_argument(
        "--diff-stat", help="path to a `git diff --stat` file (refines --issue-json size)"
    )
    parser.add_argument(
        "--mode",
        choices=("same-unit", "separate"),
        default="same-unit",
        help="same-unit (default): caller is already inside a loop-claude-* unit",
    )
    parser.add_argument(
        "--bmo-unavailable",
        action="store_true",
        help="bmo's lock already timed out -- skip a bmo-dependent tier0 pick",
    )
    parser.add_argument("--primary-effort", default=None)
    parser.add_argument(
        "--prefetch",
        default=None,
        help="comma-separated issue/PR numbers to fetch instead of routing",
    )
    parser.add_argument(
        "--repo", default=None, help="OWNER/REPO for --prefetch (default: gh's own resolution)"
    )


def _fleet_connection_args(parser: argparse.ArgumentParser) -> None:
    """Redis location overrides for `heartbeat`/`drain`/`undrain`/`machines`.

    Unlike `_backend_args`, the default is `None`, not `"localhost"` --
    these commands fall back to what `lupin join` already wrote to the
    fleet config (`machines.resolve_connection`), and a `"localhost"`
    default would mask that file every time.
    """
    parser.add_argument("--redis-host", default=os.environ.get("LUPIN_REDIS_HOST"))
    parser.add_argument(
        "--redis-port", type=int,
        default=int(os.environ["LUPIN_REDIS_PORT"]) if os.environ.get("LUPIN_REDIS_PORT") else None,
    )
    parser.add_argument("--redis-username", default=os.environ.get("LUPIN_REDIS_USERNAME"))
    parser.add_argument("--redis-password", default=os.environ.get("LUPIN_REDIS_PASSWORD"))
    parser.add_argument(
        "--config-path", default=os.environ.get("LUPIN_FLEET_CONFIG"),
        help="fleet config file (default: $LUPIN_FLEET_CONFIG or ~/.config/lupin/fleet.json)",
    )


def _join_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("coordinator", help="redis host[:port] for this machine group")
    parser.add_argument("--redis-username", default=os.environ.get("LUPIN_REDIS_USERNAME"))
    parser.add_argument("--redis-password", default=os.environ.get("LUPIN_REDIS_PASSWORD"))
    parser.add_argument(
        "--config-path", default=os.environ.get("LUPIN_FLEET_CONFIG"),
        help="fleet config file (default: $LUPIN_FLEET_CONFIG or ~/.config/lupin/fleet.json)",
    )


def _machines_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true")
    _fleet_connection_args(parser)


def _place_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("task", help="a GitHub issue number (e.g. 418 or #418) or free text")
    parser.add_argument("--explain", action="store_true", help="show every candidate machine and why")
    parser.add_argument("--json", action="store_true")
    _fleet_connection_args(parser)


def _signing_key_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--signing-key", default=os.environ.get("LUPIN_CMD_SIGNING_KEY"),
        help="default: $LUPIN_CMD_SIGNING_KEY -- shared secret for this target machine",
    )


def _cmd_group_args(parser: argparse.ArgumentParser) -> None:
    """`lupin cmd send|status|queue` -- nested subcommands, since each mode
    takes a different shape of positional args (unlike `quest`, which reuses
    one flat set of optional fields across modes).
    """
    sub = parser.add_subparsers(dest="cmd_action", required=True)

    send = sub.add_parser("send", help="enqueue a signed command for another machine")
    send.add_argument("machine", help="target machine")
    send.add_argument("action", help="e.g. loop.stop, loop.run")
    send.add_argument("params", nargs="*", help="key=value pairs, e.g. repo=owner/name")
    send.add_argument(
        "--pickup-window", type=float, default=commands.DEFAULT_PICKUP_S,
        help="seconds this command stays valid to run, not stored (default: 120)",
    )
    send.add_argument("--actor", default=os.environ.get("USER", "unknown"), help="audit only, not authorization")
    send.add_argument("--issuer", default=None, help="default: this host's hostname")
    send.add_argument("--json", action="store_true")
    _signing_key_arg(send)
    _redis_conn_args(send)

    status = sub.add_parser("status", help="look up one command's result")
    status.add_argument("id")
    status.add_argument("--json", action="store_true")
    _redis_conn_args(status)

    queue = sub.add_parser("queue", help="list a machine's pending commands")
    queue.add_argument("machine")
    queue.add_argument("--json", action="store_true")
    _redis_conn_args(queue)


def _agent_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--machine", default=None, help="default: this host's hostname")
    parser.add_argument(
        "--batch", type=int, default=agent_mod.DEFAULT_BATCH, help="commands handled per poll"
    )
    parser.add_argument(
        "--poll-interval", type=float, default=agent_mod.DEFAULT_POLL_INTERVAL,
        help="seconds between polls",
    )
    _signing_key_arg(parser)
    _redis_conn_args(parser)


def _wait_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--wait", type=float, default=DEFAULT_RESULT_WAIT_S,
        help=f"seconds to wait for a remote result before exit code 4 (default: {DEFAULT_RESULT_WAIT_S:g})",
    )


def _stop_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("repo")
    parser.add_argument("--machine", default=None, help="skip resolving which machine runs repo's loop")
    parser.add_argument(
        "--force", action="store_true",
        help="stop now; do not ask the agent to run /handoff first",
    )
    parser.add_argument("--json", action="store_true")
    _wait_arg(parser)
    _signing_key_arg(parser)
    _fleet_connection_args(parser)


def _peek_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("repo")
    parser.add_argument("lines", nargs="?", type=int, default=60)
    parser.add_argument("--machine", default=None, help="skip resolving which machine runs repo's loop")
    parser.add_argument("--json", action="store_true")
    _wait_arg(parser)
    _signing_key_arg(parser)
    _fleet_connection_args(parser)


def _attach_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("repo")
    parser.add_argument("--machine", default=None, help="skip resolving which machine runs repo's loop")
    parser.add_argument("--print", dest="print_only", action="store_true", help="print the argv, don't exec it")
    _fleet_connection_args(parser)


def _schedule_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--machine", default=None, help="default: this host")
    parser.add_argument("--json", action="store_true")
    _wait_arg(parser)
    _signing_key_arg(parser)
    _fleet_connection_args(parser)


def _schedule_args(parser: argparse.ArgumentParser) -> None:
    """Build the parser for:
    `lupin schedule [--machine M] [show | first <when> every <interval> | cal "<expr>"]`

    This uses nested subcommands, for the same reason as
    `_cmd_group_args`: each mode takes different positional arguments.
    Plain `lupin schedule`, with no mode given, means `show`. That is why
    the flags are added to the parent parser too, not only to the `show`
    subparser.
    """
    sub = parser.add_subparsers(dest="schedule_mode", required=False)

    _schedule_common_args(sub.add_parser("show", help="print the current cadence (default)"))

    first = sub.add_parser("first", help="set an anchored cadence: first <when> every <interval>")
    first.add_argument("when")
    first.add_argument("every_literal", metavar="every")
    first.add_argument("interval")
    _schedule_common_args(first)

    cal = sub.add_parser("cal", help='set a clock-aligned cadence: cal "<expr>"')
    cal.add_argument("expr")
    _schedule_common_args(cal)

    _schedule_common_args(parser)


def _pause_resume_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--machine", default=None, help="default: this host")
    parser.add_argument("--all", action="store_true", help="every registered machine, not just one")
    parser.add_argument("--json", action="store_true")
    _wait_arg(parser)
    _signing_key_arg(parser)
    _fleet_connection_args(parser)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lupin", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    _route_args(sub.add_parser("route", help="pick a {model, effort} for a (category, size) pair"))
    _classify_args(sub.add_parser("classify", help="sort an issue into (category, size)"))
    _fetch_models_args(
        sub.add_parser("fetch-models", help="fetch today's live model list and prices per subscription")
    )
    _fetch_benchmarks_args(
        sub.add_parser("fetch-benchmarks", help="fetch (or read the fleet-shared cache of) today's benchmark scores")
    )
    _quota_args(sub.add_parser("quota", help="show quota per provider -- real data if this machine has credentials, fleet cache otherwise"))
    _acquire_args(sub.add_parser("acquire", help="take a lease on a slot"))
    _hold_args(sub.add_parser("hold", help="acquire (or reuse a lease), run a command, release on exit"))
    _release_args(sub.add_parser("release", help="give up a lease"))
    _status_args(sub.add_parser("status", help="list every slot's holder count and max"))
    _claim_args(sub.add_parser("claim", help="atomically take a GitHub issue, so no other loop works it"))
    _claim_args(sub.add_parser("renew-claim", help="push a claim's TTL back out"))
    _release_claim_args(sub.add_parser("release-claim", help="give up a claim (compare-and-delete)"))
    _ledger_args(sub.add_parser("ledger", help="append or read shared repository events"))
    _review_route_args(
        sub.add_parser("review-route", help="route a pair and report which lock it needs")
    )
    _roadmap_args(sub.add_parser("roadmap", help="prioritized open tasks, across repos"))
    _serve_args(sub.add_parser("serve", help="run the loopback fleet dashboard"))
    _join_args(sub.add_parser("join", help="add this machine to the fleet"))
    _fleet_connection_args(sub.add_parser("heartbeat", help="refresh this machine's fleet record"))
    _fleet_connection_args(sub.add_parser("drain", help="mark this machine as draining"))
    _fleet_connection_args(sub.add_parser("undrain", help="mark this machine as online again"))
    _machines_args(sub.add_parser("machines", help="list every registered machine's state"))
    _place_args(sub.add_parser("place", help="pick which machine should run a task"))
    _quest_args(sub.add_parser("quest", help="list quests, their progress, and their focus machine"))
    _reconcile_args(sub.add_parser("reconcile", help="apply the automatic release rules once"))
    _cmd_group_args(sub.add_parser("cmd", help="send, check, or list cross-machine commands"))
    _agent_args(sub.add_parser("agent", help="run the command-queue poll loop for this machine"))
    run_parser = sub.add_parser("run", help="start one loop or all enabled loops")
    run_parser.add_argument("repo", nargs="?")
    run_parser.add_argument("--all", action="store_true")
    run_parser.add_argument("--platform", choices=("claude", "omp"))
    run_parser.add_argument("--provider", choices=("openai", "opencode-go"))
    run_parser.add_argument("--model")
    run_parser.add_argument("--note")
    run_parser.add_argument("--resume", action="store_true")
    run_parser.add_argument("--machine", help="run on a fleet machine")
    run_parser.add_argument("--json", action="store_true")
    _wait_arg(run_parser)
    _signing_key_arg(run_parser)
    _fleet_connection_args(run_parser)
    fleet_run_parser = sub.add_parser("fleet-run", help="dispatch enabled loops to online fleet workers")
    fleet_run_parser.add_argument("--json", action="store_true")
    _fleet_connection_args(fleet_run_parser)
    once_parser = sub.add_parser("once", help="start now or schedule a one-off loop run")
    once_parser.add_argument("when")
    once_parser.add_argument("repos", nargs="*")
    once_parser.add_argument("--platform", choices=("claude", "omp"))
    once_parser.add_argument("--provider", choices=("openai", "opencode-go"))
    once_parser.add_argument("--model")
    once_parser.add_argument("--note")
    once_parser.add_argument("--resume", action="store_true")
    once_parser.add_argument("--json", action="store_true")
    enable_parser = sub.add_parser("enable", help="add a repo to the loop schedule")
    enable_parser.add_argument("repo")
    enable_parser.add_argument("--platform", choices=("claude", "omp"), default="claude")
    enable_parser.add_argument(
        "--orchestrator",
        action="append",
        help="eligible model: claude, openai/MODEL, or opencode-go/MODEL; repeat to add choices",
    )
    enable_parser.add_argument(
        "--clear-orchestrators",
        action="store_true",
        help="remove this repo's orchestrator profile",
    )
    disable_parser = sub.add_parser("disable", help="remove a repo from the loop schedule")
    disable_parser.add_argument("repo")
    loops_parser = sub.add_parser("loops", help="show Herdr loop state")
    loops_parser.add_argument("repo", nargs="?")
    loops_parser.add_argument("--machine")
    loops_parser.add_argument("--json", action="store_true")
    _wait_arg(loops_parser)
    _signing_key_arg(loops_parser)
    _fleet_connection_args(loops_parser)
    loop_parser = sub.add_parser("loop", help=argparse.SUPPRESS)
    loop_parser.add_argument("loop_args", nargs=argparse.REMAINDER)
    _stop_args(sub.add_parser("stop", help="stop a repo's loop, on whichever machine runs it"))
    _peek_args(sub.add_parser("peek", help="show recent pane output for a repo's loop"))
    _attach_args(sub.add_parser("attach", help="attach a terminal to a repo's loop session"))
    _schedule_args(sub.add_parser("schedule", help="show or set a machine's loop-run cadence"))
    _pause_resume_args(sub.add_parser("pause", help="pause a machine's scheduled loop runs"))
    _pause_resume_args(sub.add_parser("resume", help="resume a machine's scheduled loop runs"))
    return parser


def _cmd_route(args: argparse.Namespace) -> int:
    result = route_mod.route(
        args.category,
        args.size,
        bmo_available=args.bmo_available,
        primary_effort=args.primary_effort,
    )
    if args.json:
        print(json.dumps(result))
    else:
        print(f"{result['model']} {result['effort']}")
    return 0


def _cmd_classify(args: argparse.Namespace) -> int:
    with open(args.issue_json, encoding="utf-8") as handle:
        issue = json.load(handle)
    diff_stat = None
    if args.diff_stat:
        with open(args.diff_stat, encoding="utf-8") as handle:
            diff_stat = handle.read()
    category, size = classify_mod.classify(issue, diff_stat)
    if args.json:
        print(json.dumps({"category": category, "size": size}))
    else:
        print(f"{category} {size}")
    return 0


def _cmd_fetch_models(args: argparse.Namespace) -> int:
    data = model_fetch.snapshot()
    if not args.no_write:
        model_fetch.save_snapshot(data, path=args.snapshot_file)
        if not model_fetch.publish_snapshot(data, **_fleet_connection(args)):
            print("warning: snapshot saved locally but not shared with the fleet (Redis unavailable)", file=sys.stderr)
    if args.json:
        print(json.dumps(data))
    else:
        for name, sub in data["subscriptions"].items():
            tag = "live" if sub.get("live") else "stale/unavailable"
            print(f"{name}: {len(sub['models'])} models ({tag})")
    return 0


def _cmd_fetch_benchmarks(args: argparse.Namespace) -> int:
    try:
        data = benchmark_fetch.refresh_snapshot(force=args.force, **_fleet_connection(args))
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, "the benchmark refresh", fleet=True)
        return 3
    if args.json:
        print(json.dumps(data))
    else:
        scores = data.get("scores", [])
        scored = sum(
            1 for score in scores
            if isinstance(score, dict) and score.get("score") is not None
        )
        tag = "live" if data.get("live") else f"stale/unavailable ({data.get('stale_reason')})"
        if data.get("live"):
            tag += f"; fetched {_ago(data.get('fetched_at'))}"
        print(f"benchmarks: {scored}/{len(scores)} scored ({tag})")
        for score in scores:
            if isinstance(score, dict) and score.get("score") is None:
                reason = score.get("reason") or "no reason provided"
                print(f"  unscored: {score.get('id', '<unknown model>')} — {reason}")
    return 0


def _ago(fetched_at: str | None) -> str:
    """"N ago" for a CLI line, from an ISO timestamp. Plain-text cousin of
    `serve._snapshot_age()`, which does the same job as an HTML span for
    the dashboard -- that version can't be reused here since it returns
    markup, not text."""
    if not fetched_at:
        return "unknown"
    try:
        epoch = datetime.fromisoformat(fetched_at).timestamp()
    except ValueError:
        return "unknown"
    seconds = max(0, time.time() - epoch)
    if seconds < 60:
        return "<1m ago"
    minutes = int(seconds // 60)
    if minutes < 60:
        return f"{minutes}m ago"
    return f"{minutes // 60}h ago"


def _cmd_quota(args: argparse.Namespace) -> int:
    """Publish quota and this machine's 7-day totals, then print shared quota."""
    connection = _fleet_connection(args)
    merged = quota_cache.refresh_snapshot(**connection)
    usage_cache.refresh_snapshot(**connection)
    if args.json:
        print(json.dumps(merged))
        return 0
    if not merged:
        print("no quota data available yet -- run this on a machine with real provider credentials")
        return 0
    for provider in sorted(merged):
        entry = merged[provider]
        real_rows = [row for row in entry.get("rows", []) if isinstance(row.get("used_pct"), (int, float))]
        if not real_rows:
            print(f"{provider}: no quota data cached yet")
            continue
        age = _ago(entry.get("fetched_at"))
        fetched_by = entry.get("fetched_by", "-")
        for row in real_rows:
            duration = row.get("duration")
            label = duration.label if isinstance(duration, quota.QuotaDuration) else str(duration)
            left = max(0.0, 100 - row["used_pct"])
            resets = serve.time_until_reset(row.get("resets_at"))
            print(f"{provider:<12} {label:<9} {left:.0f}% left   resets in {resets:<8} (fetched {age} via {fetched_by})")
    return 0


def _backend_module(args: argparse.Namespace):
    return slots_redis if args.backend == "redis" else slots


def _backend_kwargs(args: argparse.Namespace) -> dict:
    """Extra kwargs the `redis` backend needs that `local` doesn't take."""
    if args.backend == "redis":
        return {
            "redis_host": args.redis_host,
            "redis_port": args.redis_port,
            "redis_username": args.redis_username,
            "redis_password": args.redis_password,
        }
    return {}


_REDIS_REFUSED = (slots_redis.CoordinatorAuthFailed, redis.exceptions.NoPermissionError)

def _print_auth_failed(exc: Exception, what: str, *, fleet: bool = False) -> None:
    """Print a Redis refusal or ACL denial. Never prints the password."""
    print(f"lupin: {slots_redis.refusal_message(exc, what, fleet=fleet)}", file=sys.stderr)


def _cmd_acquire(args: argparse.Namespace) -> int:
    backend = _backend_module(args)
    try:
        lease = backend.acquire(
            args.slot,
            args.holder,
            wait=args.wait,
            ttl=args.ttl,
            max_holders=args.max_holders,
            state_root=args.state_root,
            **_backend_kwargs(args),
        )
    except slots.SlotFull:
        print(f"slot {args.slot!r} is full", file=sys.stderr)
        return 2
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, f"slot {args.slot!r}")
        return 3
    except slots.CoordinatorUnreachable:
        print(f"cannot reach the {args.backend} coordinator for slot {args.slot!r}", file=sys.stderr)
        return 3
    print(lease)
    return 0


def _cmd_hold(args: argparse.Namespace, command: list[str]) -> int:
    if not command:
        print("hold needs -- <command>", file=sys.stderr)
        return 1
    if args.lease and (args.slot or args.holder):
        print("--lease is exclusive with <slot>/--holder", file=sys.stderr)
        return 1
    if not args.lease and not (args.slot and args.holder):
        print("hold needs either --lease ID, or <slot> --holder H", file=sys.stderr)
        return 1
    backend = _backend_module(args)
    try:
        return backend.hold(
            command,
            lease=args.lease,
            slot=args.slot,
            holder=args.holder,
            wait=args.wait,
            ttl=args.ttl,
            max_holders=args.max_holders,
            state_root=args.state_root,
            **_backend_kwargs(args),
        )
    except slots.SlotFull:
        print(f"slot {args.slot!r} is full", file=sys.stderr)
        return 2
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, f"slot {args.slot!r}")
        return 3
    except slots.CoordinatorUnreachable:
        print(f"cannot reach the {args.backend} coordinator for slot {args.slot!r}", file=sys.stderr)
        return 3


def _cmd_release(args: argparse.Namespace) -> int:
    backend = _backend_module(args)
    try:
        backend.release(args.lease, state_root=args.state_root, **_backend_kwargs(args))
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, f"lease {args.lease!r}")
        return 3
    except slots.CoordinatorUnreachable:
        print(f"cannot reach the {args.backend} coordinator for lease {args.lease!r}", file=sys.stderr)
        return 3
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    backend = _backend_module(args)
    try:
        result = backend.status(state_root=args.state_root, **_backend_kwargs(args))
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, "the slot status")
        return 3
    if args.json:
        print(json.dumps(result))
    else:
        for name in sorted(result):
            info = result[name]
            max_display = info["max"] if info["max"] is not None else "?"
            print(f"{name}: {info['holders']}/{max_display}")
    return 0


def _claim_kwargs(args: argparse.Namespace) -> dict:
    return {
        "redis_host": args.redis_host,
        "redis_port": args.redis_port,
        "redis_username": args.redis_username,
        "redis_password": args.redis_password,
    }


def _cmd_claim(args: argparse.Namespace) -> int:
    try:
        claims.claim(args.target, args.holder, ttl=args.ttl, **_claim_kwargs(args))
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1
    except claims.ClaimHeld as exc:
        print(exc, file=sys.stderr)
        return 2
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, f"claim {args.target!r}")
        return 3
    except slots.CoordinatorUnreachable:
        print(f"cannot reach the redis coordinator for claim {args.target!r}", file=sys.stderr)
        return 3
    return 0


def _cmd_renew_claim(args: argparse.Namespace) -> int:
    try:
        renewed = claims.renew_claim(args.target, args.holder, ttl=args.ttl, **_claim_kwargs(args))
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, f"claim {args.target!r}")
        return 3
    except slots.CoordinatorUnreachable:
        print(f"cannot reach the redis coordinator for claim {args.target!r}", file=sys.stderr)
        return 3
    if not renewed:
        print(f"{args.target!r} is not held by {args.holder!r}", file=sys.stderr)
        return 1
    return 0


def _cmd_release_claim(args: argparse.Namespace) -> int:
    try:
        released = claims.release_claim(args.target, args.holder, **_claim_kwargs(args))
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, f"claim {args.target!r}")
        return 3
    except slots.CoordinatorUnreachable:
        print(f"cannot reach the redis coordinator for claim {args.target!r}", file=sys.stderr)
        return 3
    if not released:
        print(f"{args.target!r} is not held by {args.holder!r}", file=sys.stderr)
        return 1
    return 0


def _fleet_connection(args: argparse.Namespace) -> dict:
    return machines.resolve_connection(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_username=args.redis_username,
        redis_password=args.redis_password,
        config_path=args.config_path,
    )


def _cmd_join(args: argparse.Namespace) -> int:
    try:
        record = machines.join(
            args.coordinator,
            redis_username=args.redis_username,
            redis_password=args.redis_password,
            config_path=args.config_path,
            loops=loop_runtime.local_loops(),
            repos=serve.local_repo_inventory(),
        )
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, "the machine join")
        return 3
    except machines.CoordinatorUnreachable as exc:
        print(f"cannot reach the {exc}", file=sys.stderr)
        return 3
    print(json.dumps(record))
    return 0


def _cmd_heartbeat(args: argparse.Namespace) -> int:
    try:
        record = machines.heartbeat(
            _fleet_connection(args),
            loops=loop_runtime.local_loops(),
            repos=serve.local_repo_inventory(),
        )
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, "the machine heartbeat", fleet=True)
        return 3
    except machines.CoordinatorUnreachable as exc:
        print(f"cannot reach the {exc}", file=sys.stderr)
        return 3
    print(json.dumps(record))
    return 0


def _cmd_drain(args: argparse.Namespace) -> int:
    try:
        record = machines.drain(_fleet_connection(args))
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, "the machine drain", fleet=True)
        return 3
    except machines.CoordinatorUnreachable as exc:
        print(f"cannot reach the {exc}", file=sys.stderr)
        return 3
    print(json.dumps(record))
    return 0


def _cmd_undrain(args: argparse.Namespace) -> int:
    try:
        record = machines.undrain(_fleet_connection(args))
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, "the machine undrain", fleet=True)
        return 3
    except machines.CoordinatorUnreachable as exc:
        print(f"cannot reach the {exc}", file=sys.stderr)
        return 3
    print(json.dumps(record))
    return 0


def _cmd_machines(args: argparse.Namespace) -> int:
    try:
        result = machines.machines(_fleet_connection(args))
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, "the machine list", fleet=True)
        return 3
    except machines.CoordinatorUnreachable as exc:
        print(f"cannot reach the {exc}", file=sys.stderr)
        return 3
    if args.json:
        print(json.dumps(result))
    else:
        for record in result:
            note = " (version mismatch)" if record["version_mismatch"] else ""
            print(f"{record['name']}: {record['state']}{note}")
    return 0


def _format_place_table(result: dict) -> str:
    headers = ["machine", "state", "slots", "quest focus", "lupin", "result"]
    rows = [
        [
            c["name"],
            c["state"],
            f"{c['slots_used']}/{c['slots_max']}",
            c["quest_focus"] or "-",
            c["version"] or "-",
            c["result"],
        ]
        for c in result["candidates"]
    ]
    widths = [len(h) for h in headers]
    for row in rows:
        for i, value in enumerate(row):
            widths[i] = max(widths[i], len(value))

    def fmt(cells: list[str]) -> str:
        return "   ".join(cell.ljust(widths[i]) for i, cell in enumerate(cells)).rstrip()

    return "\n".join([fmt(headers)] + [fmt(row) for row in rows])


def _format_place_explain(result: dict) -> str:
    lines = [
        f"{result['task_label']} · {result['category_label']}, {result['size_label']}"
        f" → {result['model']} {result['effort']} ({result['provider']})"
    ]
    quota = result["quota"]
    if quota and quota.get("pct_left") is not None:
        pct = f"{quota['pct_left']:.0f}%"
        resets_at = quota.get("resets_at")
        if resets_at:
            resets_in = place_mod.format_duration(resets_at / 1000 - time.time())
            lines.append(f"{result['provider']} quota {pct}, resets in {resets_in}")
        else:
            lines.append(f"{result['provider']} quota {pct}")
    else:
        lines.append(f"{result['provider']} quota: unknown")
    if result.get("downgraded_from"):
        prior = result["downgraded_from"]
        lines.append(
            f"{prior['model']} {prior['effort']} was out of quota everywhere -- "
            f"downgraded to {result['model']} {result['effort']} ({result['provider']})"
        )
    elif result.get("wait_seconds") is not None:
        wait = place_mod.format_duration(result["wait_seconds"])
        lines.append(f"every candidate is out of quota -- wait {wait}, same model")
    lines.append("")
    if result["candidates"]:
        lines.append(_format_place_table(result))
    skipped = result["skipped"]
    total_skipped = sum(skipped.values())
    if total_skipped:
        reasons = []
        if skipped["other_provider"]:
            reasons.append(f"{skipped['other_provider']} run a different provider")
        if skipped["no_key"]:
            reasons.append(f"{skipped['no_key']} have no credentials for {result['provider']}")
        if skipped["read_failed"]:
            reasons.append(f"{skipped['read_failed']} cannot read {result['provider']} quota")
        if skipped["offline"]:
            reasons.append(f"{skipped['offline']} offline")
        lines.append(f"{total_skipped} machine(s) skipped: {', '.join(reasons)}")
    return "\n".join(lines)


def _cmd_place(args: argparse.Namespace) -> int:
    try:
        result = place_mod.place(args.task, _fleet_connection(args))
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, "the placement", fleet=True)
        return 3
    except place_mod.CoordinatorUnreachable as exc:
        print(f"cannot reach the {exc}", file=sys.stderr)
        return 3
    if args.json:
        print(json.dumps(result))
        return 0 if result["pick"] else 2
    if args.explain:
        print(_format_place_explain(result))
        return 0 if result["pick"] else 2
    if not result["pick"]:
        if result["wait_seconds"] is not None:
            wait = place_mod.format_duration(result["wait_seconds"])
            print(
                f"every {result['provider']!r} machine is out of quota -- "
                f"wait {wait}, same model ({result['model']})",
                file=sys.stderr,
            )
        else:
            print(f"no online machine runs provider {result['provider']!r}", file=sys.stderr)
        return 2
    print(result["pick"])
    return 0


def _cmd_quest_focus(args: argparse.Namespace, repos: list[str], redis_kwargs: dict) -> int:
    if not args.id:
        print("quest focus needs a quest name", file=sys.stderr)
        return 1
    try:
        result = quest_mod.focus(args.id, redis_kwargs, repos, machine=args.machine, pin=args.pin)
    except quest_mod.QuestNotFound as exc:
        print(exc, file=sys.stderr)
        return 1
    except (quest_mod.MachineDraining, quest_mod.MachineNotFound, quest_mod.NoReadyTasks) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except quest_mod.NoMachineAvailable as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, "quest focus")
        return 3
    except quest_mod.CoordinatorUnreachable as exc:
        print(f"cannot reach the {exc}", file=sys.stderr)
        return 3
    print(
        f"focused {result['quest']} on {result['machine']} · "
        f"{result['ready_count']} ready tasks will route there in order"
    )
    return 0


def _cmd_quest_release(args: argparse.Namespace, repos: list[str], redis_kwargs: dict) -> int:
    if not args.id:
        print("quest release needs a quest name", file=sys.stderr)
        return 1
    try:
        result = quest_mod.release(args.id, redis_kwargs, repos)
    except quest_mod.QuestNotFound as exc:
        print(exc, file=sys.stderr)
        return 1
    except quest_mod.NoFocus as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, "quest release")
        return 3
    except quest_mod.CoordinatorUnreachable as exc:
        print(f"cannot reach the {exc}", file=sys.stderr)
        return 3
    print(f"released {result['quest']} · {result['machine']} returns to normal routing")
    return 0


def _cmd_quest(args: argparse.Namespace) -> int:
    repos = serve.enabled_repos()
    redis_kwargs = {
        "redis_host": args.redis_host,
        "redis_port": args.redis_port,
        "redis_username": args.redis_username,
        "redis_password": args.redis_password,
    }

    if args.mode == "start":
        if not args.issues:
            print("quest start needs at least one --issue", file=sys.stderr)
            return 1
        try:
            result = quest_mod.start(
                args.issues,
                repos,
                connection=redis_kwargs,
                machine=args.machine,
                platform=args.platform,
                note=args.note,
            )
        except quest_mod.QuestError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return exc.exit_code
        except _REDIS_REFUSED as exc:
            _print_auth_failed(exc, "quest start")
            return 3
        except slots.CoordinatorUnreachable as exc:
            print(f"cannot reach the redis coordinator for quest start: {exc}", file=sys.stderr)
            return 3
        if args.json:
            print(json.dumps(result))
        else:
            print(quest_mod.render_start(result))
        return 0

    if args.mode == "stop":
        if not args.id:
            print("quest stop needs an id", file=sys.stderr)
            return 1
        try:
            message = quest_mod.stop(args.id, connection=redis_kwargs)
        except quest_mod.QuestError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return exc.exit_code
        except _REDIS_REFUSED as exc:
            _print_auth_failed(exc, "quest stop")
            return 3
        except slots.CoordinatorUnreachable as exc:
            print(f"cannot reach the redis coordinator for quest stop: {exc}", file=sys.stderr)
            return 3
        if args.json:
            print(json.dumps({"id": args.id, "message": message}))
        else:
            print(message)
        return 0

    if args.mode == "focus":
        return _cmd_quest_focus(args, repos, redis_kwargs)
    if args.mode == "release":
        return _cmd_quest_release(args, repos, redis_kwargs)

    quests, warnings = quest_mod.load_quests(repos)
    for warning in warnings:
        print(f"lupin: {warning}", file=sys.stderr)

    if args.mode == "status":
        if args.id:
            match = quest_mod.find_quest(quests, args.id)
            if match is None:
                print(f"no quest matches {args.id!r}", file=sys.stderr)
                return 1
            selected = [match]
        else:
            selected = quests
        dag = roadmap.cached_dependency_dag(repos)
        results = [
            quest_mod.status_to_json(
                one, dag, quest_mod.read_focus(one["name"], **redis_kwargs)
            )
            for one in selected
        ]
        if args.json:
            print(json.dumps(results))
        elif not results:
            print("No quests found.")
        else:
            print("\n\n".join(quest_mod.render_status(result) for result in results))
        return 0

    focuses = {
        one["name"]: quest_mod.read_focus(one["name"], **redis_kwargs) for one in quests
    }
    if args.json:
        print(json.dumps([quest_mod.to_json(one, focuses[one["name"]]) for one in quests]))
    else:
        print(quest_mod.render_list(quests, focuses))
    return 0


def _cmd_reconcile(args: argparse.Namespace) -> int:
    repos = serve.enabled_repos()
    redis_kwargs = {
        "redis_host": args.redis_host,
        "redis_port": args.redis_port,
        "redis_username": args.redis_username,
        "redis_password": args.redis_password,
    }
    try:
        lease = slots_redis.acquire("reconcile", machines.hostname(), wait=0.0, ttl=args.ttl, **redis_kwargs)
    except slots.SlotFull:
        print("reconcile is already running on another machine", file=sys.stderr)
        return 2
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, "the reconcile slot")
        return 3
    except slots.CoordinatorUnreachable:
        print("cannot reach the redis coordinator for the reconcile slot", file=sys.stderr)
        return 3

    try:
        lines, warnings = reconcile_mod.reconcile(repos, redis_kwargs)
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, "the reconcile run")
        return 3
    except reconcile_mod.CoordinatorUnreachable as exc:
        print(f"cannot reach the {exc}", file=sys.stderr)
        return 3
    finally:
        try:
            slots_redis.release(lease, **redis_kwargs)
        except slots_redis.CoordinatorAuthFailed:
            raise
        except slots.CoordinatorUnreachable:
            pass

    for warning in warnings:
        print(f"lupin: {warning}", file=sys.stderr)
    if args.json:
        print(json.dumps({"released": lines}))
        return 0
    if not lines:
        print("Nothing to release.")
        return 0
    for line in lines:
        print(line)
    return 0


def _cmd_roadmap(args: argparse.Namespace) -> int:
    # One resolved connection for both the claim marks and the shared
    # `gh` cache behind `roadmap_cli.build_roadmap` -- the roadmap's own
    # GitHub reads used to resolve nothing at all, so on a fleet whose
    # Redis needs auth every repo came back unreadable. `--config-path` is
    # deliberately absent here: `_roadmap_args` uses `_redis_conn_args`,
    # which has no such flag, and `resolve_connection` falls back to the
    # same default fleet config every other reader uses.
    connection = machines.resolve_connection(**_claim_kwargs(args))
    claims_lookup = functools.partial(claims.claims_for, **connection)
    text, code = roadmap_cli.run(
        args.repo, args.limit, args.stage, args.dag, args.json, args.refresh,
        claims_lookup=claims_lookup,
        connection=connection,
    )
    print(text)
    return code



def _cmd_ledger(args: argparse.Namespace) -> int:
    connection = _fleet_connection(args)
    try:
        if args.ledger_action == "append":
            event = {"event": args.event}
            for field in ("issue", "status", "branch", "summary"):
                value = getattr(args, field)
                if value is not None:
                    event[field] = value
            for field in ("highlights", "evidence", "decisions", "next"):
                if getattr(args, field):
                    event[field] = getattr(args, field)
            if args.child:
                event["children"] = args.child
            result = ledger.append_event(args.repo, event, **connection)
        else:
            result = ledger.read_events(args.repo, limit=args.limit, **connection)
    except ValueError as exc:
        print(f"invalid ledger request: {exc}", file=sys.stderr)
        return 1
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, f"ledger {args.repo!r}", fleet=True)
        return 3
    except slots.CoordinatorUnreachable:
        print(f"cannot reach the redis coordinator for ledger {args.repo!r}", file=sys.stderr)
        return 3

    if args.json:
        print(json.dumps(result))
    elif args.ledger_action == "append":
        print(result["id"])
    else:
        for entry in result:
            print(json.dumps(entry, ensure_ascii=False))
    return 0


def _redis_kwargs(args: argparse.Namespace) -> dict:
    return {
        "redis_host": args.redis_host,
        "redis_port": args.redis_port,
        "redis_username": args.redis_username,
        "redis_password": args.redis_password,
    }


def _cmd_cmd(args: argparse.Namespace) -> int:
    if args.cmd_action == "send":
        if not args.signing_key:
            print("cmd send needs --signing-key or $LUPIN_CMD_SIGNING_KEY", file=sys.stderr)
            return 1
        try:
            params = commands.parse_params(args.params)
        except ValueError as exc:
            print(exc, file=sys.stderr)
            return 1
        try:
            cmd_id = commands.enqueue(
                args.machine, args.action, params,
                key=args.signing_key, actor=args.actor, issuer=args.issuer or machines.hostname(),
                pickup_window=args.pickup_window, **_redis_kwargs(args),
            )
        except _REDIS_REFUSED as exc:
            _print_auth_failed(exc, f"send to {args.machine!r}")
            return 3
        except slots.CoordinatorUnreachable:
            print(f"cannot reach the redis coordinator to send to {args.machine!r}", file=sys.stderr)
            return 3
        if args.json:
            print(json.dumps({"id": cmd_id}))
        else:
            print(cmd_id)
        return 0

    if args.cmd_action == "status":
        try:
            result = commands.get_status(args.id, **_redis_kwargs(args))
        except _REDIS_REFUSED as exc:
            _print_auth_failed(exc, f"command {args.id!r}")
            return 3
        except slots.CoordinatorUnreachable:
            print(f"cannot reach the redis coordinator for command {args.id!r}", file=sys.stderr)
            return 3
        if result is None:
            print(f"no command {args.id!r}", file=sys.stderr)
            return 1
        if args.json:
            print(json.dumps(result))
        else:
            print(f"{result['id']}: {result['state']}")
        return 0

    # args.cmd_action == "queue"
    try:
        entries = commands.get_queue(args.machine, **_redis_kwargs(args))
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, f"queue {args.machine!r}")
        return 3
    except slots.CoordinatorUnreachable:
        print(f"cannot reach the redis coordinator for queue {args.machine!r}", file=sys.stderr)
        return 3
    if args.json:
        print(json.dumps(entries))
    else:
        for entry in entries:
            print(f"{entry['id']}: {entry.get('action', '?')} {entry.get('params', {})}")
    return 0


def _cmd_agent(args: argparse.Namespace) -> int:
    if not args.signing_key:
        print("agent needs --signing-key or $LUPIN_CMD_SIGNING_KEY", file=sys.stderr)
        return 1
    machine = args.machine or machines.hostname()
    try:
        agent_mod.run_forever(
            machine, args.signing_key,
            batch=args.batch, poll_interval=args.poll_interval, **_redis_kwargs(args),
        )
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, f"agent {machine!r}")
        return 3
    except slots.CoordinatorUnreachable:
        print(f"cannot reach the redis coordinator for agent {machine!r}", file=sys.stderr)
        return 3
    return 0


def _start_loop_with_result(repo: str, args: argparse.Namespace) -> tuple[bool, str]:
    try:
        return loop_runtime.start_loop(
            repo,
            platform=args.platform,
            provider=args.provider,
            model=args.model,
            note=args.note,
            resume=args.resume,
        )
    except loop_runtime.LoopError as exc:
        return False, f"could not start loop for {repo}: {exc}"


def _cmd_run(args: argparse.Namespace) -> int:
    if args.all == bool(args.repo):
        print("run needs one repo or --all", file=sys.stderr)
        return 1
    if args.all and args.resume:
        print("run --all cannot resume", file=sys.stderr)
        return 1
    if (args.provider or args.model) and args.platform != "omp":
        print("--provider and --model require --platform omp", file=sys.stderr)
        return 1
    local_host = machines.hostname()
    machine = args.machine or local_host
    if machine != local_host:
        queue_action = "loop.run-all" if args.all else "loop.run"
        queue_params = {"note": args.note} if args.all else {
            "repo": args.repo,
            "platform": args.platform,
            "note": args.note,
            "resume": args.resume,
        }
        if args.platform is not None:
            queue_params["platform"] = args.platform
        if args.provider is not None:
            queue_params["provider"] = args.provider
        if args.model is not None:
            queue_params["model"] = args.model
        local_argv = ["lupin", "run", "--all"] if args.all else ["lupin", "run", args.repo]
        if args.note is not None:
            local_argv.extend(["--note", args.note])
        if args.platform:
            local_argv.extend(["--platform", args.platform])
        if args.provider:
            local_argv.extend(["--provider", args.provider])
        if args.model:
            local_argv.extend(["--model", args.model])
        if args.resume:
            local_argv.append("--resume")
        try:
            result = loops_mod.dispatch_loop_action(
                machine=machine,
                local_host=local_host,
                local_argv=local_argv,
                queue_action=queue_action,
                queue_params=queue_params,
                connection=_fleet_connection(args),
                signing_key=args.signing_key,
                actor=os.environ.get("USER", "lupin"),
                issuer=local_host,
                wait_s=args.wait,
            )
        except _REDIS_REFUSED as exc:
            _print_auth_failed(exc, f"the loop start on {machine!r}", fleet=True)
            return 3
        except slots.CoordinatorUnreachable as exc:
            print(f"cannot reach the redis coordinator to start a loop on {machine!r}: {exc}", file=sys.stderr)
            return 3
        except loops_mod.MissingSigningKey:
            print("run needs --signing-key or $LUPIN_CMD_SIGNING_KEY to reach another machine", file=sys.stderr)
            return 1
        code, payload = _exit_for_dispatch(result)
        if args.json:
            _print_dispatch_result(payload, as_json=True)
        elif payload.get("output"):
            print(payload["output"].rstrip())
        else:
            print(f"{payload.get('state', 'queued')} ({payload['id']})")
        return code
    try:
        targets = list(loop_runtime.enabled_repos()) if args.all else [args.repo]
        results = []
        failed = False
        for repo in targets:
            ok, message = _start_loop_with_result(repo, args)
            results.append({"repo": repo, "started": ok, "message": message})
            failed |= not ok and not message.startswith("skip ")
    except loop_runtime.LoopError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(results))
    else:
        for item in results:
            print(item["message"])
    if failed:
        return 1
    if not args.all and results and not results[0]["started"]:
        return 2 if results[0]["message"].startswith("skip ") else 1
    return 0


def _cmd_fleet_run(args: argparse.Namespace) -> int:
    """Dispatch the local enabled-repo list to eligible fleet workers."""
    try:
        repos = list(loop_runtime.enabled_repos())
        if not repos:
            print("no enabled repos to dispatch", file=sys.stderr)
            return 1
        connection = _fleet_connection(args)
        records = machines.machines(connection)
        signing_keys = {}
        key_dir = os.environ.get("LUPIN_CMD_SIGNING_KEYS_DIR")
        for record in records:
            machine = record.get("name")
            if not isinstance(machine, str):
                continue
            key = commands.signing_key_for(machine, directory=key_dir)
            if key is not None:
                signing_keys[machine] = key
        results = loops_mod.dispatch_fleet_runs(
            repos,
            records,
            local_host=machines.hostname(),
            signing_keys=signing_keys,
            connection=connection,
        )
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, "fleet-run", fleet=True)
        return 3
    except slots.CoordinatorUnreachable as exc:
        print(f"cannot reach the redis coordinator: {exc}", file=sys.stderr)
        return 3
    except loop_runtime.LoopError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(results))
    else:
        for result in results:
            if result["queued"]:
                print(f"queued {result['repo']}@{result['machine']} ({result['id']})")
            else:
                print(f"skip {result['repo']}: {result['message']}")
    return 1 if any(not result["queued"] for result in results) else 0


def _cmd_once(args: argparse.Namespace) -> int:
    try:
        if args.when == "now":
            targets = args.repos or list(loop_runtime.enabled_repos())
            results = []
            failed = False
            for repo in targets:
                ok, message = _start_loop_with_result(repo, args)
                results.append({"repo": repo, "started": ok, "message": message})
                failed |= not ok and not message.startswith("skip ")
            if args.json:
                print(json.dumps(results))
            else:
                for item in results:
                    print(item["message"])
            return 1 if failed else 0
        targets = args.repos or list(loop_runtime.enabled_repos())
        message = loop_runtime.schedule_once(
            args.when, targets, platform=args.platform, provider=args.provider,
            model=args.model, note=args.note, resume=args.resume
        )
    except loop_runtime.LoopError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps({"scheduled": True, "message": message}))
    else:
        print(message)
    return 0


def _cmd_enable(args: argparse.Namespace) -> int:
    try:
        loop_runtime.enable_repo(
            args.repo,
            args.platform,
            args.orchestrator,
            clear_orchestrators=args.clear_orchestrators,
        )
    except loop_runtime.LoopError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(f"enabled {args.repo} ({args.platform})")
    return 0


def _cmd_disable(args: argparse.Namespace) -> int:
    try:
        loop_runtime.disable_repo(args.repo)
    except loop_runtime.LoopError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(f"disabled {args.repo}")
    return 0


def _cmd_loop_internal(args: argparse.Namespace) -> int:
    return loop_runtime.main(args.loop_args)


def _cmd_loops(args: argparse.Namespace) -> int:
    local_host = machines.hostname()
    machine = args.machine or local_host
    if machine == local_host:
        try:
            if args.repo:
                rows = [loop_runtime.loop_state(args.repo)]
            else:
                rows = loop_runtime.status_rows()
        except loop_runtime.LoopError as exc:
            print(str(exc), file=sys.stderr)
            return 1
    elif args.repo:
        try:
            result = loops_mod.dispatch_loop_action(
                machine=machine,
                local_host=local_host,
                local_argv=["lupin", "loop", "local-action", "state", args.repo],
                queue_action="loop.state",
                queue_params={"repo": args.repo},
                connection=_fleet_connection(args),
                signing_key=args.signing_key,
                actor=os.environ.get("USER", "lupin"),
                issuer=local_host,
                wait_s=args.wait,
            )
        except _REDIS_REFUSED as exc:
            _print_auth_failed(exc, f"loops on {machine!r}", fleet=True)
            return 3
        except (slots.CoordinatorUnreachable, loops_mod.MissingSigningKey) as exc:
            print(str(exc), file=sys.stderr)
            return 3 if isinstance(exc, slots.CoordinatorUnreachable) else 1
        code, payload = _exit_for_dispatch(result)
        if code:
            _print_dispatch_result(payload, as_json=args.json)
            return code
        output = payload.get("output") or ""
        if result["mode"] == "queued":
            output = result["result"].get("output") or ""
        try:
            rows = [json.loads(output)]
        except json.JSONDecodeError:
            print("remote loop state returned invalid JSON", file=sys.stderr)
            return 1
    else:
        try:
            records = machines.machines(_fleet_connection(args))
        except _REDIS_REFUSED as exc:
            _print_auth_failed(exc, "the machine list", fleet=True)
            return 3
        except machines.CoordinatorUnreachable as exc:
            print(f"cannot reach the {exc}", file=sys.stderr)
            return 3
        record = next((item for item in records if item["name"] == machine), None)
        if record is None:
            print(f"no machine named {machine!r}", file=sys.stderr)
            return 1
        rows = record.get("loops", [])
    if args.json:
        print(json.dumps(rows))
    else:
        for row in rows:
            print(f"{row.get('repo', '?')}: {row.get('state', 'unknown')} ({row.get('backend', 'unknown')})")
    return 0


def _exit_for_dispatch(result: dict) -> tuple[int, dict]:
    """Turn a `loops.dispatch_loop_action()` result into an exit code and
    a result dict. Shared by `stop`/`peek`/`schedule`/`pause`/`resume`.

    See cli.py's module docstring for what 0, 1, and 4 mean. Exit code 5
    (`AmbiguousMachine`) and `attach` are handled elsewhere, before
    dispatch is even tried.
    """
    if result["mode"] == "local":
        rc = result["returncode"]
        return (0 if rc == 0 else 1), {"mode": "local", "returncode": rc, "output": result["output"]}
    queued = result["result"]
    if queued is None or queued.get("state") in ("queued", "running"):
        return 4, {"mode": "queued", "id": result["id"], "state": (queued or {}).get("state", "unknown")}
    if queued.get("state") == "ok":
        return 0, {"mode": "queued", "id": result["id"], **queued}
    return 1, {"mode": "queued", "id": result["id"], **queued}


def _print_dispatch_result(payload: dict, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload))
        return
    if payload["mode"] == "local":
        if payload["output"]:
            print(payload["output"].rstrip())
        return
    print(f"{payload.get('state', 'queued')} ({payload['id']})")


def _cmd_stop(args: argparse.Namespace) -> int:
    connection = _fleet_connection(args)
    local_host = machines.hostname()
    if args.machine:
        machine = args.machine
    else:
        try:
            machine = loops_mod.resolve_machine_for_repo(args.repo, connection)
        except loops_mod.AmbiguousMachine as exc:
            print(str(exc), file=sys.stderr)
            return 5
        except _REDIS_REFUSED as exc:
            _print_auth_failed(exc, "the machine lookup", fleet=True)
            return 3
        except machines.CoordinatorUnreachable as exc:
            print(f"cannot reach the {exc}", file=sys.stderr)
            return 3
    try:
        result = loops_mod.dispatch_loop_action(
            machine=machine, local_host=local_host,
            local_argv=["lupin", "loop", "local-action", "stop", args.repo, *(["--force"] if args.force else [])],
            queue_action="loop.stop", queue_params={"repo": args.repo, "force": args.force},
            connection=connection, signing_key=args.signing_key,
            actor=os.environ.get("USER", "lupin"), issuer=local_host, wait_s=args.wait,
            run_local=lambda argv: loops_mod.run_subprocess(argv, timeout=agent_mod.ACTION_TIMEOUT_S["loop.stop"]),
        )
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, f"stop {args.repo!r} on {machine!r}", fleet=True)
        return 3
    except slots.CoordinatorUnreachable:
        print(f"cannot reach the redis coordinator to stop {args.repo!r} on {machine!r}", file=sys.stderr)
        return 3
    except loops_mod.MissingSigningKey:
        print("stop needs --signing-key or $LUPIN_CMD_SIGNING_KEY to reach another machine", file=sys.stderr)
        return 1
    code, payload = _exit_for_dispatch(result)
    _print_dispatch_result(payload, as_json=args.json)
    return code


def _cmd_peek(args: argparse.Namespace) -> int:
    connection = _fleet_connection(args)
    local_host = machines.hostname()
    if args.machine:
        machine = args.machine
    else:
        try:
            machine = loops_mod.resolve_machine_for_repo(args.repo, connection)
        except loops_mod.AmbiguousMachine as exc:
            print(str(exc), file=sys.stderr)
            return 5
        except _REDIS_REFUSED as exc:
            _print_auth_failed(exc, "the machine lookup", fleet=True)
            return 3
        except machines.CoordinatorUnreachable as exc:
            print(f"cannot reach the {exc}", file=sys.stderr)
            return 3
    try:
        result = loops_mod.dispatch_loop_action(
            machine=machine, local_host=local_host,
            local_argv=["lupin", "loop", "local-action", "peek", args.repo, str(args.lines)],
            queue_action="loop.peek", queue_params={"repo": args.repo, "lines": args.lines},
            connection=connection, signing_key=args.signing_key,
            actor=os.environ.get("USER", "lupin"), issuer=local_host, wait_s=args.wait,
        )
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, f"peek {args.repo!r} on {machine!r}", fleet=True)
        return 3
    except slots.CoordinatorUnreachable:
        print(f"cannot reach the redis coordinator to peek {args.repo!r} on {machine!r}", file=sys.stderr)
        return 3
    except loops_mod.MissingSigningKey:
        print("peek needs --signing-key or $LUPIN_CMD_SIGNING_KEY to reach another machine", file=sys.stderr)
        return 1
    code, payload = _exit_for_dispatch(result)
    if args.json:
        print(json.dumps(payload))
    elif payload["mode"] == "local":
        print(payload["output"].rstrip())
    else:
        print((payload.get("output") or f"{payload.get('state', 'queued')} ({payload['id']})").rstrip())
    return code


def _cmd_attach(args: argparse.Namespace) -> int:
    local_host = machines.hostname()
    if args.machine:
        machine = args.machine
    else:
        try:
            machine = loops_mod.resolve_machine_for_repo(args.repo, _fleet_connection(args))
        except loops_mod.AmbiguousMachine as exc:
            print(str(exc), file=sys.stderr)
            return 5
        except _REDIS_REFUSED as exc:
            _print_auth_failed(exc, "the machine lookup", fleet=True)
            return 3
        except machines.CoordinatorUnreachable as exc:
            print(f"cannot reach the {exc}", file=sys.stderr)
            return 3

    target = loops_mod.ssh_target_for(machine) if machine != local_host else None
    try:
        argv = loop_runtime.attach_argv(args.repo, machine, local_host, target)
    except loop_runtime.LoopError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.print_only:
        print(" ".join(shlex.quote(a) for a in argv))
        return 0
    os.execvp(argv[0], argv)  # pragma: no cover -- replaces this process


def _cmd_schedule(args: argparse.Namespace) -> int:
    mode = args.schedule_mode or "show"
    connection = _fleet_connection(args)
    local_host = machines.hostname()
    machine = args.machine or local_host

    if mode == "show":
        local_argv = ["lupin", "loop", "local-action", "schedule"]
        queue_action, queue_params = "schedule.show", {}
    elif mode == "first":
        if args.every_literal != "every":
            print(f"schedule first: expected the word 'every', got {args.every_literal!r}", file=sys.stderr)
            return 1
        try:
            when = agent_mod.validate_schedule_token("when", args.when)
            interval = agent_mod.validate_schedule_token("interval", args.interval)
        except agent_mod.RejectedCommand as exc:
            print(str(exc), file=sys.stderr)
            return 1
        local_argv = ["lupin", "loop", "local-action", "schedule", "first", when, "every", interval]
        queue_action, queue_params = "schedule.set", {"mode": "first", "when": when, "interval": interval}
    else:  # cal
        try:
            expr = agent_mod.validate_cal_expr(args.expr)
        except agent_mod.RejectedCommand as exc:
            print(str(exc), file=sys.stderr)
            return 1
        local_argv = ["lupin", "loop", "local-action", "schedule", "cal", expr]
        queue_action, queue_params = "schedule.set", {"mode": "cal", "expr": expr}

    try:
        result = loops_mod.dispatch_loop_action(
            machine=machine, local_host=local_host, local_argv=local_argv,
            queue_action=queue_action, queue_params=queue_params,
            connection=connection, signing_key=args.signing_key,
            actor=os.environ.get("USER", "lupin"), issuer=local_host, wait_s=args.wait,
        )
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, f"schedule on {machine!r}", fleet=True)
        return 3
    except slots.CoordinatorUnreachable:
        print(f"cannot reach the redis coordinator to reach {machine!r}", file=sys.stderr)
        return 3
    except loops_mod.MissingSigningKey:
        print("schedule needs --signing-key or $LUPIN_CMD_SIGNING_KEY to reach another machine", file=sys.stderr)
        return 1
    code, payload = _exit_for_dispatch(result)
    _print_dispatch_result(payload, as_json=args.json)
    return code


# How bad each per-machine result is, worst last. This ranks the exit
# codes `_cmd_pause_resume` can see for one machine: 0 (ok) is least bad.
# 4 (sent, result unknown) is next -- the action may still turn out fine.
# 1 (a real failure happened) is worse than an unknown result. 3 (could
# not even reach the coordinator for that machine) is the worst, since it
# means this command has no information at all about that machine.
# `--all`'s overall exit code is the worst of these, by this order -- not
# "whichever machine came first" -- so the same set of per-machine results
# always gives the same overall code, no matter what order they ran in.
_PAUSE_RESUME_SEVERITY = {0: 0, 4: 1, 1: 2, 3: 3}


def _cmd_pause_resume(args: argparse.Namespace, verb: str) -> int:
    """Shared by `_cmd_pause` and `_cmd_resume`. Both take the same
    `--machine`/`--all` shape, and both send a `schedule.<verb>` queue
    action."""
    connection = _fleet_connection(args)
    local_host = machines.hostname()
    if args.all:
        try:
            targets = sorted(record["name"] for record in machines.machines(connection))
        except _REDIS_REFUSED as exc:
            _print_auth_failed(exc, f"{verb} --all", fleet=True)
            return 3
        except machines.CoordinatorUnreachable as exc:
            print(f"cannot reach the {exc}", file=sys.stderr)
            return 3
        if not targets:
            targets = [local_host]
    else:
        targets = [args.machine or local_host]

    results: dict[str, dict] = {}
    worst = 0
    for machine in targets:
        try:
            result = loops_mod.dispatch_loop_action(
                machine=machine, local_host=local_host,
                local_argv=["lupin", "loop", "local-action", verb],
                queue_action=f"schedule.{verb}", queue_params={},
                connection=connection, signing_key=args.signing_key,
                actor=os.environ.get("USER", "lupin"), issuer=local_host, wait_s=args.wait,
            )
        except _REDIS_REFUSED as exc:
            _print_auth_failed(exc, f"{verb} {machine!r}", fleet=True)
            results[machine] = {"machine": machine, "error": "redis refused or denied the command"}
            code = 3
        except slots.CoordinatorUnreachable:
            print(f"cannot reach the redis coordinator to {verb} {machine!r}", file=sys.stderr)
            results[machine] = {"machine": machine, "error": "cannot reach the redis coordinator"}
            code = 3
        except loops_mod.MissingSigningKey:
            print(f"{verb} needs --signing-key or $LUPIN_CMD_SIGNING_KEY to reach another machine", file=sys.stderr)
            results[machine] = {"machine": machine, "error": "missing signing key"}
            code = 1
        else:
            code, payload = _exit_for_dispatch(result)
            results[machine] = payload
        if _PAUSE_RESUME_SEVERITY[code] > _PAUSE_RESUME_SEVERITY[worst]:
            worst = code

    if args.json:
        print(json.dumps(results))
    else:
        for machine, payload in results.items():
            if "error" in payload:
                state = f"error: {payload['error']}"
            elif payload["mode"] == "local":
                state = "ok" if payload["returncode"] == 0 else "failed"
            else:
                state = payload.get("state", "queued")
            print(f"{machine}: {state}")
    return worst


def _cmd_pause(args: argparse.Namespace) -> int:
    return _cmd_pause_resume(args, "pause")


def _cmd_resume(args: argparse.Namespace) -> int:
    return _cmd_pause_resume(args, "resume")


def main(argv: list[str] | None = None) -> int:
    """Run `_main`. A Redis refusal or ACL denial that no command caught
    prints one line and exits 3. It never prints a traceback.
    """
    try:
        return _main(argv)
    except _REDIS_REFUSED as exc:
        _print_auth_failed(exc, "a Redis step in this command", fleet=True)
        return 3


def _main(argv: list[str] | None = None) -> int:
    raw = sys.argv[1:] if argv is None else argv
    if "--" in raw:
        split = raw.index("--")
        lupin_argv, command = raw[:split], raw[split + 1 :]
    else:
        lupin_argv, command = raw, []

    parser = _build_parser()
    args = parser.parse_args(lupin_argv)

    if args.cmd == "serve":
        return serve.main(lupin_argv[1:])

    if args.cmd == "review-route":
        return review_dispatch.main(lupin_argv[1:])

    if args.cmd == "route":
        return _cmd_route(args)
    if args.cmd == "classify":
        return _cmd_classify(args)
    if args.cmd == "fetch-models":
        return _cmd_fetch_models(args)
    if args.cmd == "fetch-benchmarks":
        return _cmd_fetch_benchmarks(args)
    if args.cmd == "quota":
        return _cmd_quota(args)
    if args.cmd == "acquire":
        return _cmd_acquire(args)
    if args.cmd == "hold":
        return _cmd_hold(args, command)
    if args.cmd == "release":
        return _cmd_release(args)
    if args.cmd == "status":
        return _cmd_status(args)
    if args.cmd == "claim":
        return _cmd_claim(args)
    if args.cmd == "renew-claim":
        return _cmd_renew_claim(args)
    if args.cmd == "release-claim":
        return _cmd_release_claim(args)
    if args.cmd == "ledger":
        return _cmd_ledger(args)
    if args.cmd == "join":
        return _cmd_join(args)
    if args.cmd == "heartbeat":
        return _cmd_heartbeat(args)
    if args.cmd == "drain":
        return _cmd_drain(args)
    if args.cmd == "undrain":
        return _cmd_undrain(args)
    if args.cmd == "machines":
        return _cmd_machines(args)
    if args.cmd == "place":
        return _cmd_place(args)
    if args.cmd == "quest":
        return _cmd_quest(args)
    if args.cmd == "reconcile":
        return _cmd_reconcile(args)
    if args.cmd == "roadmap":
        return _cmd_roadmap(args)
    if args.cmd == "cmd":
        return _cmd_cmd(args)
    if args.cmd == "agent":
        return _cmd_agent(args)
    if args.cmd == "loop":
        return _cmd_loop_internal(args)
    if args.cmd == "fleet-run":
        return _cmd_fleet_run(args)
    if args.cmd == "run":
        return _cmd_run(args)
    if args.cmd == "once":
        return _cmd_once(args)
    if args.cmd == "enable":
        return _cmd_enable(args)
    if args.cmd == "disable":
        return _cmd_disable(args)
    if args.cmd == "loops":
        return _cmd_loops(args)
    if args.cmd == "stop":
        return _cmd_stop(args)
    if args.cmd == "peek":
        return _cmd_peek(args)
    if args.cmd == "attach":
        return _cmd_attach(args)
    if args.cmd == "schedule":
        return _cmd_schedule(args)
    if args.cmd == "pause":
        return _cmd_pause(args)
    if args.cmd == "resume":
        return _cmd_resume(args)
    parser.error(f"unknown command {args.cmd!r}")  # pragma: no cover
    return 1


if __name__ == "__main__":
    sys.exit(main())
