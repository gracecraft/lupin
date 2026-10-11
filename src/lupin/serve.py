#!/usr/bin/env python3
"""Serve the Lupin dashboard.

The dashboard reads local loop state from Herdr. It reads remote loop state
from machine heartbeats. It sends remote loop actions through signed commands.
It runs local loop actions through Lupin.

The server binds to loopback or a tailnet address. A reverse proxy must
control access to write routes.
"""

from __future__ import annotations

import html
import ipaddress
import json
import os
import concurrent.futures
import re
import shlex
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from importlib import resources
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse, urlsplit

import redis

from . import benchmark_catalog, benchmark_fetch, claims, commands, debrief, loop_runtime, loops, machines, model_fetch, quest, quota_cache, roadmap, slots_redis
from . import usage_cache
from .slots import CoordinatorUnreachable
from .quota import (
    QuotaDuration,
    epoch_ms_to_local,
    quota_source_label,
)

ATTACHMENT_ID = roadmap.ATTACHMENT_ID
ATTACHMENT_REDIRECT_HOST = re.compile(
    r"github-production-user-asset-[a-z0-9-]+\.s3\.amazonaws\.com"
)
MAX_IMAGE_BYTES = 10 * 1024 * 1024
MAX_FORM_BYTES = 8 * 1024
IMAGE_TYPES = {"image/gif", "image/jpeg", "image/png", "image/webp"}
FAVICON = b"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><path fill="#e11d48" d="M16 28S3 20.4 3 11.5A7.5 7.5 0 0 1 16 7.4a7.5 7.5 0 0 1 13 4.1C29 20.4 16 28 16 28Z"/></svg>"""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def github_attachment(attachment_id: str) -> tuple[bytes, str] | None:
    try:
        auth = subprocess.run(
            ["gh", "auth", "token"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    token = auth.stdout.strip()
    auth.stdout = ""
    if auth.returncode != 0 or not token:
        return None

    opener = urllib.request.build_opener(NoRedirect())
    request = urllib.request.Request(
        f"https://github.com/user-attachments/assets/{attachment_id}",
        headers={"Authorization": f"token {token}", "Accept": "image/*"},
    )
    token = ""
    try:
        try:
            response = opener.open(request, timeout=15)
        except urllib.error.HTTPError as redirect:
            if redirect.code not in (301, 302, 303, 307, 308):
                redirect.close()
                return None
            location = redirect.headers.get("Location")
            redirect.close()
            try:
                parsed = urlsplit(location or "")
            except ValueError:
                return None
            try:
                port = parsed.port
            except ValueError:
                return None
            if (
                parsed.scheme != "https"
                or parsed.username is not None
                or parsed.password is not None
                or port not in (None, 443)
                or not ATTACHMENT_REDIRECT_HOST.fullmatch(parsed.hostname or "")
            ):
                return None
            response = opener.open(
                urllib.request.Request(location, headers={"Accept": "image/*"}),
                timeout=15,
            )
        with response:
            content_type = response.headers.get_content_type()
            if content_type not in IMAGE_TYPES:
                return None
            data = response.read(MAX_IMAGE_BYTES + 1)
            if len(data) > MAX_IMAGE_BYTES:
                return None
            return data, content_type
    except (OSError, TimeoutError, urllib.error.URLError):
        return None


STATE_DIR = str(loop_runtime.STATE_DIR)
REPOS_FILE = str(loop_runtime.REPOS_FILE)
CODE_DIR = str(loop_runtime.CODE_DIR)
LOOP_DOC = "docs/delegation-loop.md"

# Tailscale's CGNAT range (100.64.0.0/10). A bind address in this range is a
# tailnet interface, gated by the headscale ACL and the host firewall -- the
# same trust boundary this project's Redis deployment already relies on
# (docs/redis-schema.md). Any other non-loopback address is still refused.
TAILNET_RANGE = ipaddress.ip_network("100.64.0.0/10")


def bind_allowed(addr: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Loopback, or a tailnet address -- see TAILNET_RANGE above."""
    return addr.is_loopback or (addr.version == 4 and addr in TAILNET_RANGE)

# model-tiers.json -- the same file route.py routes a (category, size) pair
# with. It ships as package data, so read it the same way route.py does.
MODEL_TIERS_PATH = str(resources.files("lupin").joinpath("model-tiers.json"))
# The tier keys the data file uses, cheapest first. A category may be
# missing any of them; route.py escalates a missing tier to the next one up.
MODEL_TIER_ORDER = ("tier0", "tier1", "tier2")

# --------------------------------------------------------------------------
# running read-only probes
# --------------------------------------------------------------------------


def run(argv: list[str], timeout: float = 10.0) -> tuple[int, str]:
    """Run a fixed argv command without a shell."""
    return loops.run_subprocess(argv, timeout=timeout)


# --------------------------------------------------------------------------
# reading state
# --------------------------------------------------------------------------


def enabled_repos() -> list[str]:
    """Repo names enabled for the schedule, from `REPOS_FILE`."""
    return list(_repo_platforms().keys())


def write_enabled_repos(names: list[str]) -> None:
    """Write enabled repo names in the legacy one-name-per-line format.

    The loop runtime also writes `REPOS_FILE` with repo platforms.
    """
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(REPOS_FILE, "w", encoding="utf-8") as fh:
        fh.writelines(f"{name}\n" for name in names)


# A per-repo loop concurrency cap uses the existing Redis slot schema.
REPO_SLOT_PREFIX = "repo:"


def _repo_slot_name(repo: str) -> str:
    return f"{REPO_SLOT_PREFIX}{repo}"


def _delegation_doc_template(repo: str) -> str:
    """A minimal starting doc for a repo that has none yet ("Generate docs
    and add", issue #23). No existing doc generator was found in this repo
    (grepped for "generate"/"scaffold"/LOOP_DOC outside serve.py and
    docs/) -- this is a plain fill-in-the-blanks skeleton, not a smart
    generator.
    """
    return (
        f"# {repo} -- the delegation loop\n\n"
        "This file gives repo-specific guidance. Fill it in with useful details;\n"
        "a loop can run without it.\n\n"
        "## What this repo is for\n\n"
        "TODO: say what this repo does, and what the first loops should build.\n\n"
        "## Rules\n\n"
        "TODO: anything a loop must know before it starts -- how to test,\n"
        "what not to touch, where to push its work.\n"
    )


def code_repos() -> list[dict]:
    """Every directory under /code, with whether it can run a loop."""
    out = []
    enabled = set(enabled_repos())
    try:
        names = sorted(
            d.name for d in os.scandir(CODE_DIR) if d.is_dir(follow_symlinks=True)
        )
    except OSError:
        names = []
    for name in names:
        has_doc = os.path.isfile(os.path.join(CODE_DIR, name, LOOP_DOC))
        state = "enabled" if name in enabled else "disabled"
        out.append({
            "repo": name,
            "state": state,
            "loopable": True,
            "has_doc": has_doc,
        })
    return out


def local_repo_inventory() -> list[dict]:
    """Return repo names and loop readiness for this machine's heartbeat."""
    enabled = set(enabled_repos())
    return [
        {
            "repo": repo["repo"],
            "enabled": repo["repo"] in enabled,
            "loopable": repo["loopable"],
        }
        for repo in code_repos()
    ]


# The Roadmap page is a fleet view: one model per repo, and each model can
# wait on Redis, the ledger, or `gh`. Build them side by side so the page
# takes as long as the slowest repo, not the sum of all of them.
_ROADMAP_WORKERS = 8


def _parallel(repos: list[str], build):
    """Run `build(repo)` for every repo, side by side."""
    if len(repos) < 2:
        return [build(repo) for repo in repos]
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(len(repos), _ROADMAP_WORKERS)
    ) as pool:
        return list(pool.map(build, repos))


def _roadmap_models(repos: list[str], connection: dict) -> dict:
    """One combined model per repo, built in parallel."""

    def build(repo: str):
        return roadmap.cached_combined_model(
            repo, os.path.join(CODE_DIR, repo), connection=connection
        )

    return dict(zip(repos, _parallel(repos, build)))


# Rendered board/list HTML. The key holds the repos shown and the whole
# query, because every filter changes the output. The lifetime stays short
# so ledger events and GitHub data remain near-live, while a reload inside
# the window costs nothing -- including no GitHub call.
_FRAGMENT_TTL = float(os.environ.get("LUPIN_ROADMAP_FRAGMENT_TTL", "30"))
_FRAGMENT_MAX = 64
_FRAGMENT_CACHE: dict[tuple, tuple[float, bytes]] = {}
_FRAGMENT_LOCK = threading.Lock()


def _fragment_key(repos: list[str], query: dict) -> tuple:
    return (tuple(repos), tuple(sorted(query.items())))


def _cached_fragment(key: tuple, build) -> bytes:
    now = time.monotonic()
    with _FRAGMENT_LOCK:
        cached = _FRAGMENT_CACHE.get(key)
        if cached and now - cached[0] < _FRAGMENT_TTL:
            return cached[1]
    body = build()
    with _FRAGMENT_LOCK:
        _FRAGMENT_CACHE[key] = (time.monotonic(), body)
        if len(_FRAGMENT_CACHE) > _FRAGMENT_MAX:
            # The key holds the query, so a crawl of filter URLs would grow
            # this without a bound. Drop the oldest entry.
            oldest = min(_FRAGMENT_CACHE, key=lambda item: _FRAGMENT_CACHE[item][0])
            del _FRAGMENT_CACHE[oldest]
    return body


def roadmap_fragment(query: dict, connection: dict, quest_state) -> tuple[bytes, int]:
    """Render the board or list view as a bare HTML fragment (no page)."""
    repos = roadmap.repository_names(code_repos())
    selected = query.get("repo", "").strip()
    if selected and selected not in repos:
        return render_error("unknown repository"), 404

    def build() -> bytes:
        models = _roadmap_models(repos, connection)
        issue_count = sum(
            len(model.get("nodes", []))
            for repo, model in models.items()
            if not selected or repo == selected
        )
        if query.get("view") == "list" or (
            query.get("view") != "board"
            and issue_count > roadmap.BOARD_MAX_ISSUES
        ):
            body, _css = roadmap.list_fragment(repos, models, query, quest_state)
        else:
            body, _css = roadmap.board_fragment(repos, models, query, quest_state)
        return body.encode("utf-8")

    return _cached_fragment(_fragment_key(repos, query), build), 200


def _roadmap_loader_script() -> str:
    """Swap the placeholder from `/roadmap/board` after first paint."""
    return """
(function(){
  var box=document.getElementById('roadmap-fragment');
  if(!box)return;
  fetch(box.dataset.src,{credentials:'same-origin'}).then(function(response){
    if(!response.ok)throw new Error('status '+response.status);
    return response.text();
  }).then(function(html){
    var holder=document.createElement('template');
    holder.innerHTML=html;
    box.replaceWith(holder.content);
  }).catch(function(){location.href=box.dataset.full;});
})();
"""



# How long a page waits for a remote machine's answer, in seconds.
REMOTE_WAIT_S = 15.0


def loop_tail(repo: str, machine: str, lines: int, connection: dict, signing_key: str | None) -> str:
    local_host = machines.hostname()
    result = loops.dispatch_loop_action(
        machine=machine,
        local_host=local_host,
        local_argv=["lupin", "loop", "local-action", "peek", repo, str(lines)],
        queue_action="loop.peek",
        queue_params={"repo": repo, "lines": lines},
        connection=connection,
        signing_key=signing_key,
        actor="lupin-dashboard",
        issuer=local_host,
        run_local=lambda argv: run(argv, timeout=20.0),
        wait_s=REMOTE_WAIT_S,
    )
    if result["mode"] == "queued":
        finished = result.get("result")
        if not finished or finished.get("state") not in ("ok", "failed"):
            return f"(the request to {machine} has not finished; reload to try again)"
        return finished.get("output", "")
    return result.get("output", "")


def _repo_platforms() -> dict[str, str]:
    """Repo to platform, read from `REPOS_FILE`.
    Lines use `repo` or `repo platform`.
    """
    result: dict[str, str] = {}
    try:
        with open(REPOS_FILE, encoding="utf-8") as fh:
            for line in fh:
                parts = line.split()
                if parts:
                    result[parts[0]] = parts[1] if len(parts) > 1 else "claude"
    except OSError:
        pass
    return result



def timers() -> list[dict]:
    """Delegation timers, from systemd's own JSON. Times are epoch seconds."""
    rc, out = run(["systemctl", "list-timers", "--all", "--no-pager", "--output=json"])
    if rc != 0:
        return []
    try:
        rows = json.loads(out)
    except json.JSONDecodeError:
        return []
    result = []
    for row in rows:
        unit = row.get("unit") or ""
        if unit != "delegation-loop.timer" and not re.fullmatch(
            r"lupin-once-[a-f0-9]{16}\.timer", unit
        ):
            continue
        nxt = row.get("next")
        last = row.get("last")
        result.append(
            {
                "unit": unit,
                "activates": row.get("activates") or "",
                "next": nxt / 1e6 if isinstance(nxt, (int, float)) and nxt else None,
                "last": last / 1e6 if isinstance(last, (int, float)) and last else None,
            }
        )
    return sorted(result, key=lambda t: (t["next"] is None, t["next"] or 0))


def oneoff_repositories(unit: str) -> str:
    """Read repo names from Lupin's one-off schedule file."""
    match = re.fullmatch(r"lupin-once-([a-f0-9]{16})\.timer", unit)
    if not match:
        return unit
    path = loop_runtime.STATE_DIR / "once" / f"{match.group(1)}.json"
    try:
        entry = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return unit
    repos = entry.get("repos") if isinstance(entry, dict) else None
    if not isinstance(repos, list) or not repos:
        return unit
    try:
        names = [loop_runtime.validate_repo(repo) for repo in repos]
    except loop_runtime.LoopError:
        return unit
    return ", ".join(names)


def timer_repository(unit: str) -> str:
    if unit == "delegation-loop.timer":
        return "all enabled repos"
    return oneoff_repositories(unit)


def timer_active() -> bool:
    rc, _ = run(["systemctl", "is-active", "--quiet", "delegation-loop.timer"])
    return rc == 0


def fleet_state(connection: dict) -> dict:
    """Machines and claims from the cross-machine Redis registry (issue
    #15). Degrades the same way the local readers above do: a Redis outage
    returns empty data and an error string instead of raising -- claims has
    no local fallback either way (see claims.py), so a claims-only failure
    just leaves that part empty without blanking the machines list.

    `repos` for `claims.claims_for` is built the same way
    `roadmap_cli.build_roadmap` already does: `enabled_repos()` for the
    short names, `roadmap._repo_identity()` to resolve each one's GitHub
    owner from its local checkout. A repo with no local checkout (or no
    `gh` access) is silently skipped, same as `/roadmap` already tolerates.
    """
    try:
        machine_list = machines.machines(connection)
    except machines.CoordinatorUnreachable as exc:
        return {"machines": [], "claims": {}, "fleet_error": str(exc)}

    full_names = {}
    for repo in enabled_repos():
        owner, _name, _warning = roadmap._repo_identity(os.path.join(CODE_DIR, repo))
        if owner:
            full_names[repo] = f"{owner}/{repo}"

    claims_data: dict = {}
    if full_names:
        try:
            claims_data = claims.claims_for(
                list(full_names.values()),
                redis_host=connection.get("redis_host"),
                redis_port=connection.get("redis_port"),
                redis_username=connection.get("redis_username"),
                redis_password=connection.get("redis_password"),
            )
        except claims.CoordinatorUnreachable:
            pass
    return {"machines": machine_list, "claims": claims_data, "fleet_error": None}


# Check repo names before local or remote loop actions.
REPO_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")


def _valid_repo_name(repo: str) -> bool:
    return bool(REPO_NAME_RE.match(repo))


def merge_repo_inventory(
    local_repos: list[dict], machine_records: list[dict], local_host: str
) -> list[dict]:
    """Add repo inventories from other machines to this machine's repo list."""
    repos = {repo["repo"]: {**repo, "local": True} for repo in local_repos}
    remote = {}
    for record in machine_records:
        host = record.get("name")
        if not isinstance(host, str) or host == local_host:
            continue
        reported = record.get("repos", [])
        if not isinstance(reported, list):
            continue
        loops = record.get("loops", [])
        running_repos = (
            {
                loop.get("repo")
                for loop in loops
                if isinstance(loop, dict) and isinstance(loop.get("repo"), str)
            }
            if isinstance(loops, list)
            else set()
        )
        active = record.get("state") in ("online", "draining")
        for item in reported:
            if not isinstance(item, dict):
                continue
            repo = item.get("repo")
            if not isinstance(repo, str) or not _valid_repo_name(repo):
                continue
            entry = remote.setdefault(
                repo,
                {
                    "enabled": False,
                    "loopable": False,
                    "hosts": set(),
                    "running_hosts": set(),
                    "enabled_hosts": set(),
                    "enable_hosts": set(),
                },
            )
            is_enabled = item.get("enabled") is True
            entry["enabled"] |= is_enabled
            entry["loopable"] |= item.get("loopable") is True
            label = f"{host} (offline)" if record.get("state") == "offline" else host
            entry["hosts"].add(label)
            if record.get("state") == "online":
                actions = record.get("actions", [])
                if is_enabled and "loop.run" in actions:
                    entry["enabled_hosts"].add(host)
                if not is_enabled and "repo.enable" in actions:
                    entry["enable_hosts"].add(host)
            if active and repo in running_repos:
                entry["running_hosts"].add(host)

    for repo, entry in remote.items():
        if repo in repos:
            repos[repo]["fleet_enabled"] = entry["enabled"]
            repos[repo]["enabled_hosts"] = sorted(entry["enabled_hosts"])
            repos[repo]["enable_hosts"] = sorted(entry["enable_hosts"])
            continue
        state = "enabled" if entry["enabled"] else "disabled"
        repos[repo] = {
            "repo": repo,
            "state": state,
            "enabled": entry["enabled"],
            "loopable": entry["loopable"],
            "local": False,
            "running": bool(entry["running_hosts"]),
            "running_machines": ", ".join(sorted(entry["running_hosts"])),
            "machine": ", ".join(sorted(entry["hosts"])),
            "enabled_hosts": sorted(entry["enabled_hosts"]),
            "enable_hosts": sorted(entry["enable_hosts"]),
        }
    return sorted(repos.values(), key=lambda repo: repo["repo"])


def _repo_full_names() -> dict[str, str]:
    """short repo name -> "owner/repo", for each enabled repo this host can
    resolve from its own local checkout. Same mapping `fleet_state()` builds
    for its own claims lookup -- built again here, not shared, to keep each
    function a small, self-contained read.
    """
    full_names = {}
    for repo in enabled_repos():
        owner, _name, _warning = roadmap._repo_identity(os.path.join(CODE_DIR, repo))
        if owner:
            full_names[repo] = f"{owner}/{repo}"
    return full_names


def _loop_hosts_from_heartbeat(machine_records: list[dict], local_host: str) -> dict[str, dict]:
    """Map each remote repo to its Herdr state and host."""
    hosts = {}
    for record in machine_records:
        if record.get("name") == local_host:
            continue
        for loop in record.get("loops", []):
            repo = loop.get("repo")
            if repo:
                state = loop.get("state") or "unknown"
                if record.get("state") == "offline":
                    state = "unknown"
                hosts[repo] = {**loop, "state": state, "machine": record["name"]}
    return hosts


def gather_loops(connection: dict) -> dict:
    """Return local Herdr state and remote Herdr heartbeat state."""
    local_host = machines.hostname()
    local_states = {row["repo"]: row for row in loop_runtime.local_loops()}
    enabled = set(enabled_repos())
    state = fleet_state(connection)
    remote_states = _loop_hosts_from_heartbeat(state.get("machines", []), local_host)
    entries = []
    # The server may have no /code. Loops that other machines report still count.
    repo_names = {repo_info["repo"] for repo_info in code_repos()} | set(remote_states)
    for repo in sorted(repo_names):
        entry = local_states.get(repo)
        machine = local_host
        if entry is None:
            entry = remote_states.get(repo)
            if entry is not None:
                machine = entry["machine"]
        entry = entry or {"repo": repo, "backend": "herdr", "state": "stopped"}
        status = entry.get("state") or "unknown"
        entries.append({
            "repo": repo,
            "enabled": repo in enabled,
            "status": status,
            "agent_status": status,
            "machine": machine,
            "backend": entry.get("backend", "herdr"),
            "session": entry.get("session"),
            "workspace_id": entry.get("workspace_id"),
            "pane_id": entry.get("pane_id"),
            "platform": entry.get("platform"),
            "since": entry.get("since"),
        })
    entries.sort(key=lambda entry: entry["repo"])
    return {
        "entries": entries,
        "machines": state.get("machines", []),
        "claims": state.get("claims", {}),
        "fleet_error": state.get("fleet_error"),
        "local_host": local_host,
    }


def gather(peek_lines: int, connection: dict | None = None) -> dict:
    del peek_lines
    connection = connection or {}
    loop_state = gather_loops(connection)
    state = {
        "now": time.time(),
        "enabled": enabled_repos(),
        "repos": code_repos(),
        "loops": loop_state["entries"],
        "timers": timers(),
        "timer_active": timer_active(),
    }
    state.update({
        "machines": loop_state["machines"],
        "claims": loop_state["claims"],
        "fleet_error": loop_state["fleet_error"],
    })
    state["repos"] = merge_repo_inventory(
        state["repos"], state["machines"], machines.hostname()
    )
    state["enabled"] = sorted(
        set(state["enabled"])
        | {
            repo["repo"]
            for repo in state["repos"]
            if repo.get("enabled") is True or repo.get("fleet_enabled") is True
        }
    )
    return state


def gather_repos(connection: dict) -> dict:
    """Return local repos and repos reported by other fleet machines."""
    loops = gather_loops(connection)
    by_repo = {entry["repo"]: entry for entry in loops["entries"]}
    slot_status = slots_redis.status(**connection)
    repos = []
    inventory = merge_repo_inventory(
        code_repos(), loops.get("machines", []), loops["local_host"]
    )
    for repo in inventory:
        entry = by_repo.get(repo["repo"])
        slot = slot_status.get(_repo_slot_name(repo["repo"]), {})
        if repo["local"]:
            running = entry is not None and entry["status"] not in ("stopped", "unknown")
            machine = entry["machine"] if entry else loops["local_host"]
        else:
            running = repo["running"]
            machine = repo["machine"]
        repos.append(
            {
                **repo,
                "running": running,
                "machine": machine,
                "max": slot.get("max"),
            }
        )
    return {
        "repos": repos,
        "machines": loops["machines"],
        "fleet_error": loops["fleet_error"],
        "local_host": loops["local_host"],
    }


def gather_schedule(connection: dict) -> dict:
    """Everything the Schedule page (issue #22) reads: the timer list (same
    `timers()` the Overview page already uses), and the fleet machine list
    for the "Run now" placement picker (same `fleet_state()` /machines and
    /loops already read -- one reading of the registry, not a new one)."""
    state = fleet_state(connection)
    return {
        "now": time.time(),
        "enabled": enabled_repos(),
        "timers": timers(),
        "timer_active": timer_active(),
        "machines": state.get("machines", []),
        "fleet_error": state.get("fleet_error"),
        "local_host": machines.hostname(),
    }


def _coordinator_only() -> bool:
    return os.environ.get("LUPIN_LOOP_COORDINATOR_ONLY") == "1"


def _rank_candidates(records: list[dict], local_host: str) -> list[dict]:
    """Online machines for "Run now" placement, most free loop capacity
    first (ties broken by name, for a deterministic "spread out" rotation).

    Judgment call -- not `place.py`'s `_rank_key`: that ranking scores one
    quota-routed task against a provider's remaining quota (claude/opencode
    usage windows). Starting a recurring loop has no such task to classify
    or provider to route to -- a loop picks its own model per-task once it
    is running. The only dimension that still applies here is free slot
    capacity (`machines._slot_totals`, the same helper `place.py` itself
    uses for its own free-slots tiebreak), so that is all this uses.

    The local machine is a candidate only when this service also runs loops.
    Coordinator-only mode excludes it, even if its registry record says
    `online`.
    """
    local_worker = not _coordinator_only()
    online = [
        record
        for record in records
        if record["state"] == "online" and (local_worker or record["name"] != local_host)
    ]
    if local_worker and local_host not in {record["name"] for record in records}:
        online.append({"name": local_host, "state": "online", "slots": {}})

    def free_slots(record: dict) -> int:
        used, max_ = machines._slot_totals(record.get("slots"))
        return max_ - used

    return sorted(online, key=lambda r: (-free_slots(r), r["name"]))


def _machine_available(name: str, records: list[dict], local_host: str) -> bool:
    """Whether `name` is a legal target for "Run now"."""
    if _coordinator_only() and name == local_host:
        return False
    record = next((r for r in records if r["name"] == name), None)
    if record is None:
        return name == local_host and not _coordinator_only()
    return record["state"] == "online"


def _timer_loop_count(unit: str, repo_label: str, enabled: list[str]) -> int:
    """Count runs scheduled by a timer. The recurring timer starts every
    enabled repo. A one-off timer uses its saved repo list. If the saved
    list is missing, show a count of one rather than guess.
    """
    if unit == "delegation-loop.timer":
        return len(enabled)
    if repo_label == unit:
        return 1
    return len([part for part in repo_label.split(", ") if part])


# --------------------------------------------------------------------------
# html
# --------------------------------------------------------------------------

CSS = """
:root{
--ok:#1f7a6a;--warn:#d2512e;--ink:#14201e;--bg:#f2f6f5;--surface:#ffffff;
--side:#e4eeec;--ink2:#4a5b58;--ink3:#5f706d;--line:#d3e0dd;--line2:#e3ecea;
--track:#dae6e3;--warnbg:#fff0ea;--warnline:#f6cdbd;--warnink:#a03c1a;
--term:#10201e;--termink:#dfece9;--frame:#c3d3cf;--idle:#9fb0ac;
--lav:#26636b;--lavbg:#dcebe8;--okbg:#e1f1ee;
/* The mockup loads these two from Google Fonts. We don't load that file
(CSP blocks it), so the names below are unused and every browser falls
through to the system font right after them. */
--sans:Nunito,"Segoe UI Rounded",ui-rounded,-apple-system,"Segoe UI",system-ui,sans-serif;
--mono:"Geist Mono",ui-monospace,"SF Mono","Cascadia Code","Roboto Mono",monospace;
/* legacy names: src/lupin/roadmap.py's own CSS still refers to these */
--fg:var(--ink);--dim:var(--ink3);--card:var(--surface);--accent:var(--ok);--code:var(--track);
}
:root[data-theme="dark"]{
--ok:#4cc2ad;--warn:#ef8a5c;--ink:#e8f1ef;--bg:#101615;--surface:#172120;
--side:#0c1110;--ink2:#a9bcb8;--ink3:#8da29d;--line:#273532;--line2:#1e2927;
--track:#222f2c;--warnbg:#35211a;--warnline:#5e3626;--warnink:#f5b394;
--term:#0a100f;--termink:#dbe8e5;--frame:#2f3f3b;--idle:#72857f;
--lav:#7fc7cf;--lavbg:#1c2c2c;--okbg:#18291f;
}
*{box-sizing:border-box}
html,body{margin:0}
body{background:var(--bg);color:var(--ink);font:15px/1.5 var(--sans)}
a{color:var(--ok);text-decoration:none}
a:hover{color:var(--ink);text-decoration:underline}
.shell{display:flex;min-height:100vh}
.side{width:200px;flex:none;background:var(--side);border-right:1px solid var(--line);
padding:22px 14px;display:flex;flex-direction:column;position:sticky;top:0;
height:100vh;overflow:auto}
.brand{font:700 19px var(--mono);padding:0 8px 22px 8px}
.navlinks{display:grid;gap:2px}
.navlink{display:flex;align-items:center;gap:9px;padding:8px 10px;border-radius:10px;
font-size:14px;color:var(--ink)}
.navlink svg{color:var(--ink2)}
.navlink:hover{background:var(--line2);text-decoration:none}
.navlink.active{background:var(--lavbg);font-weight:600}
.content{flex:1;min-width:0;display:flex;flex-direction:column}
.topbar{display:flex;align-items:center;gap:14px;padding:12px 28px;
border-bottom:1px solid var(--line);font-size:13px;color:var(--ink2)}
.topbar .sp{flex:1}
.autolabel{display:flex;align-items:center;gap:6px;cursor:pointer}
.iconbtn{width:32px;height:32px;flex:none;padding:0;display:flex;align-items:center;
justify-content:center;border-radius:10px;border:1px solid var(--line);
background:var(--surface);color:var(--ink);cursor:pointer}
.iconbtn:hover{filter:brightness(.94)}
.iconbtn:focus-visible{outline:2px solid var(--ok);outline-offset:2px}
.iconbtn .icon-sun{display:none}
:root[data-theme="dark"] .iconbtn .icon-sun{display:inline-flex}
:root[data-theme="dark"] .iconbtn .icon-moon{display:none}
main{max-width:1500px;padding:1.5rem 1.75rem 4rem;flex:1;min-width:0}
h1{font-size:1.25rem;margin:0}
h2{font-size:.8rem;margin:2rem 0 .6rem;color:var(--ink2);font-family:var(--mono);
text-transform:uppercase;letter-spacing:.06em}
header{display:flex;gap:1rem;align-items:center;flex-wrap:wrap;
border-bottom:1px solid var(--line);padding-bottom:.8rem;margin-bottom:.2rem}
header h1{display:flex;align-items:center;gap:10px;font:600 21px var(--mono)}
header h1 svg{color:var(--ok)}
header .sp{flex:1}
header a{font-size:13px}
.dim{color:var(--ink3)}
.stale{color:var(--warn)}
.mono{font-family:var(--mono)}
.card{background:var(--surface);border:1px solid var(--line);border-radius:18px;
box-shadow:0 3px 0 var(--line);padding:.9rem 1.1rem;margin-bottom:.7rem}
.row{display:flex;gap:.8rem;align-items:center;flex-wrap:wrap}
.pill{font-size:.75rem;padding:.15rem .6rem;border-radius:99px;
border:1px solid var(--line);color:var(--ink2)}
.pill.on{color:var(--ok);border-color:var(--ok)}
.pill.off{color:var(--warn);border-color:var(--warn)}
.big{font-size:1.05rem;font-weight:600}
pre{background:var(--term);color:var(--termink);border-radius:14px;
padding:.6rem .7rem;overflow-x:auto;font:400 12px/1.5 var(--mono);margin:.6rem 0 0;
max-height:16rem;white-space:pre}
table{border-collapse:collapse;width:100%;font-size:14px}
td,th{text-align:left;padding:.5rem .6rem;border-bottom:1px solid var(--line2)}
th{color:var(--ink3);font-weight:500;font-size:.72rem;letter-spacing:.04em;
text-transform:uppercase}
.section-head{display:flex;align-items:center;gap:7px;margin:1.8rem 0 .7rem}
.section-head svg{color:var(--ink2)}
.section-head h2{margin:0}
.stat-row{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}
.stat{display:flex;flex-direction:column;gap:5px}
.stat svg{color:var(--ink2)}
.stat-label{font-size:.72rem;letter-spacing:.06em;text-transform:uppercase;
color:var(--ink3);font-family:var(--mono);display:flex;align-items:center;gap:6px}
.stat-value{font-size:26px;font-weight:600}
.stat-value.mono{font-family:var(--mono)}
.stat-note{font-size:.8rem;color:var(--ink2)}
.stat.warn{background:var(--warnbg);border-color:var(--warnline);box-shadow:none}
.stat.warn .stat-label,.stat.warn .stat-value{color:var(--warnink)}
.stat.ok .stat-value{color:var(--ok)}
.loop-grid{display:grid;gap:10px}
.loop-card{display:block;color:inherit}
.loop-card:hover{border-color:var(--ink3);text-decoration:none}
.loop-head{display:flex;align-items:center;gap:10px;font-size:13px;
color:var(--ink2);flex-wrap:wrap}
.loop-head b{font-size:15px;color:var(--ink);font-weight:600}
.dot{width:8px;height:8px;border-radius:2px;background:var(--ok);flex:none}
.dot.idle{background:var(--idle)}
.loop-tail{margin:10px 0 0;max-height:4.6em}
.loop-empty{display:flex;gap:14px;align-items:center}
@media(max-width:860px){
.shell{flex-direction:column}
.side{width:auto;height:auto;position:static;flex-direction:row;align-items:center;
gap:14px;padding:12px 16px;overflow-x:auto}
.navlinks{display:flex;flex-direction:row;gap:4px}
.navlink span{display:none}
main{padding:1rem 1rem 3rem}
.stat-row{grid-template-columns:1fr}
}
.quota-heading{display:flex;justify-content:space-between;align-items:baseline;
gap:1rem;flex-wrap:wrap}
.scroll{overflow-x:auto}
.quota-summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));
gap:1rem;margin:1.5rem 0}
.quota-summary-key{font-size:.75rem;color:var(--dim);letter-spacing:.06em;
text-transform:uppercase}
.quota-summary-value{font-size:1.15rem;font-weight:600;margin-top:.35rem}
.quota-summary-note{font-size:.8rem;color:var(--dim);margin-top:.15rem}
.quota-legend{display:flex;gap:1.25rem;flex-wrap:wrap;color:var(--dim);
font-size:.85rem;margin:1.25rem 0}
.quota-legend-item{display:flex;align-items:center;gap:.5rem}
.quota-legend-used{width:28px;height:8px;border-radius:4px;background:var(--accent)}
.quota-legend-time{width:2px;height:14px;background:var(--fg)}
.quota-legend-ahead{width:28px;height:8px;border-radius:4px;background:var(--warn)}
.quota-groups{display:grid;gap:.8rem}
.quota-group{padding:1.2rem 1.4rem .4rem}
.quota-group-heading{display:flex;justify-content:space-between;align-items:baseline;
gap:.75rem;flex-wrap:wrap;margin-bottom:.4rem}
.quota-group-heading h3{margin:0;font-size:1.1rem}
.quota-row{display:grid;grid-template-columns:minmax(90px,120px) minmax(0,1fr)
minmax(130px,170px);gap:1.5rem;align-items:center;padding:1rem 0;
border-top:1px solid var(--line)}
.quota-window{font-weight:500}
.quota-status{font-size:.8rem;color:var(--dim);margin-top:.2rem}
.quota-status.ahead{color:var(--warn)}
.quota-status.under{color:var(--accent)}
.quota-values{display:flex;justify-content:space-between;gap:1rem;
font-size:.85rem;color:var(--dim);margin-bottom:.45rem}
.quota-values strong{font-size:1.2rem;color:var(--fg)}
.quota-meter{position:relative;height:9px;border-radius:5px;background:var(--line)}
.quota-meter-used{position:absolute;inset:0 auto 0 0;border-radius:5px;
background:var(--accent)}
.quota-meter-used.ahead{background:var(--warn)}
.quota-meter-elapsed{position:absolute;top:-4px;bottom:-4px;width:2px;
border-radius:1px;background:var(--fg)}
.quota-reset{text-align:right}
.quota-reset-left{font:500 1rem ui-monospace,monospace}
.quota-reset-at{font:400 .75rem ui-monospace,monospace;color:var(--dim);margin-top:.2rem}
@media(max-width:700px){.quota-row{grid-template-columns:1fr;gap:.5rem}
.quota-reset{text-align:left}}
.tier-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:.8rem}
.tier-card{margin-bottom:0;display:flex;flex-direction:column;gap:.45rem}
.tier-heading{display:flex;justify-content:space-between;align-items:baseline;gap:.75rem}
.tier-heading h3{margin:0;font-size:1.05rem}
.tier-row{display:grid;grid-template-columns:3.5rem minmax(0,1fr);gap:.75rem;
align-items:baseline;padding-top:.5rem;border-top:1px solid var(--line)}
.tier-name{font-size:.75rem;letter-spacing:.06em;text-transform:uppercase;
color:var(--dim)}
.tier-picks{display:flex;flex-wrap:wrap;gap:.4rem;align-items:baseline}
.tier-pick{display:inline-flex;gap:.4rem;align-items:baseline;
border:1px solid var(--line);border-radius:6px;padding:.1rem .45rem;
background:var(--code)}
.tier-model{font:500 .9rem ui-monospace,monospace}
.tier-arrow{color:var(--dim)}
.tier-note{margin:.15rem 0 0;font-size:.85rem}
.issue-details{border-top:1px solid var(--line);margin-top:.5rem;padding-top:.35rem}
.issue-details summary{cursor:pointer}
.issue-body{white-space:pre-wrap;overflow-wrap:anywhere;margin:.35rem 0}
.activity-comments{padding-left:1.5rem}
.loops-shell{display:flex;gap:1rem;align-items:flex-start}
.loops-side{width:260px;flex:none;padding:.6rem}
.loops-main{flex:1;min-width:0}
.loops-row{display:block;padding:.3rem .2rem;color:inherit}
.loops-row.sel{font-weight:700}
.loops-group{color:var(--ink3);margin-top:.6rem;font-size:.8rem}
.err{color:#9b2226}
"""

JS = """
// Live-tick the relative times, and reload on a timer if the box is ticked.
function fmt(s){s=Math.max(0,Math.round(s));
 var d=Math.floor(s/86400),h=Math.floor(s%86400/3600),
     m=Math.floor(s%3600/60),x=s%60;
 if(d)return d+"d "+h+"h"; if(h)return h+"h "+m+"m";
 if(m)return m+"m "+x+"s"; return x+"s";}
function tick(){var now=Date.now()/1000;
 document.querySelectorAll("[data-since]").forEach(function(e){
   e.textContent=fmt(now-parseFloat(e.dataset.since))+" ago";});
 document.querySelectorAll("[data-until]").forEach(function(e){
   var d=parseFloat(e.dataset.until)-now;
   e.textContent=d>0?("in "+fmt(d)):("overdue by "+fmt(-d));});}
setInterval(tick,1000);tick();
var box=document.getElementById("auto");
if(box){box.checked=localStorage.getItem("lupin-auto")==="1";
 box.addEventListener("change",function(){
   localStorage.setItem("lupin-auto",box.checked?"1":"0");});
 setInterval(function(){if(box.checked)location.reload();},10000);}
document.querySelectorAll("[data-once-repo]").forEach(function(row){
 var when=row.querySelector("input"),command=row.querySelector("code"),
     status=row.querySelector("[data-copy-status]");
 function update(){command.textContent="lupin once "+when.value+" "+row.dataset.onceRepo;}
 when.addEventListener("input",update);
 row.querySelector("button").addEventListener("click",function(){
   Promise.resolve().then(function(){
     return navigator.clipboard.writeText(command.textContent);
   }).then(function(){status.textContent="copied";},function(){
     status.textContent="copy failed";
   }).then(function(){setTimeout(function(){status.textContent="";},2000);});
 });
});
var themeBtn=document.getElementById("theme-toggle");
if(themeBtn){themeBtn.addEventListener("click",function(){
  var root=document.documentElement;
  var next=root.getAttribute("data-theme")==="dark"?"light":"dark";
  root.setAttribute("data-theme",next);
  try{localStorage.setItem("lupin-theme",next);}catch(e){}});}
"""

# Runs before the stylesheet paints, so the page never flashes the wrong
# theme. No network access, no state beyond one localStorage key.
THEME_BOOTSTRAP = """<script>(function(){try{
var t=localStorage.getItem("lupin-theme");
if(!t){t=matchMedia("(prefers-color-scheme: dark)").matches?"dark":"light";}
document.documentElement.setAttribute("data-theme",t);
}catch(e){}})();</script>"""

# (nav key, path, label, icon path data) -- icon paths are the same ones the
# design mockup uses for these pages, so the sidebar and page headers agree.
NAV_ITEMS = [
    ("overview", "/", "Overview", "M3 11l9-8 9 8M5 10v10h14V10"),
    (
        "machines",
        "/machines",
        "Machines",
        "M4 4h16v6H4zM4 14h16v6H4zM8 7h.01M8 17h.01",
    ),
    (
        "loops",
        "/loops",
        "Loops",
        "M17 2l4 4-4 4M3 11V9a3 3 0 013-3h15M7 22l-4-4 4-4M21 13v2a3 3 0 01-3 3H3",
    ),
    (
        "schedule",
        "/schedule",
        "Schedule",
        "M4 5h16v16H4zM4 10h16M8 3v4M16 3v4",
    ),
    (
        "repos",
        "/repos",
        "Repos",
        "M6 3v12M18 9a3 3 0 100-6 3 3 0 000 6zM6 21a3 3 0 100-6 3 3 0 000 6zM18 9a9 9 0 01-9 9",
    ),
    ("roadmap", "/roadmap", "Roadmap", "M5 21V4M5 4h12l-2 4 2 4H5"),
    ("usage", "/usage", "Usage", "M5 20V10M12 20V4M19 20v-7"),
    (
        "models",
        "/model-tiers",
        "Models",
        "M7 7h10v10H7zM9 2v3M15 2v3M9 19v3M15 19v3M2 9h3M2 15h3M19 9h3M19 15h3",
    ),
]
SUN_ICON = "M12 8a4 4 0 100 8 4 4 0 000-8zM12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"
MOON_ICON = "M20 13.5A8.5 8.5 0 1110.5 4a6.5 6.5 0 009.5 9.5zM18 2v3M16.5 3.5h3"


def esc(value) -> str:
    return html.escape(str(value), quote=True)


def icon(d: str, size: int = 16) -> str:
    """A stroke-style icon, matching the mockup's svg icons. `d` is always
    one of the fixed path strings above, never caller-supplied text."""
    return (
        f'<svg aria-hidden="true" width="{size}" height="{size}" viewBox="0 0 24 24" '
        'fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" '
        f'stroke-linejoin="round" style="flex:none"><path d="{d}"></path></svg>'
    )


def render_nav(active: str) -> str:
    links = "".join(
        f"<a class='navlink{' active' if key == active else ''}' href='{href}'>"
        f"{icon(path)}<span>{esc(label)}</span></a>"
        for key, href, label, path in NAV_ITEMS
    )
    return (
        "<nav class=side><div class=brand>lupin</div>"
        f"<div class=navlinks>{links}</div><div style='flex:1'></div></nav>"
    )


def render_topbar(extra_html: str = "") -> str:
    return (
        "<div class=topbar>"
        "<label class=autolabel><input type=checkbox id=auto> auto-refresh</label>"
        f"{extra_html}"
        "<span class=sp></span>"
        "<button type=button id=theme-toggle class=iconbtn aria-label='Toggle dark mode' "
        "title='Toggle dark mode'>"
        f"<span class=icon-sun>{icon(SUN_ICON, 17)}</span>"
        f"<span class=icon-moon>{icon(MOON_ICON, 17)}</span>"
        "</button></div>"
    )


def page(
    title: str,
    body: str,
    extra_css: str = "",
    extra_js: str = "",
    active: str = "",
    topbar_extra: str = "",
) -> bytes:
    return (
        "<!doctype html><html lang=en><head><meta charset=utf-8>"
        f"{THEME_BOOTSTRAP}"
        "<meta name=viewport content='width=device-width,initial-scale=1'>"
        "<link rel=icon href='/favicon.ico' type='image/svg+xml'>"
        f"<title>{esc(title)}</title><style>{CSS}{extra_css}</style></head>"
        f"<body><div class=shell>{render_nav(active)}<div class=content>"
        f"{render_topbar(topbar_extra)}<main>{body}</main></div></div>"
        f"<script>{JS}{extra_js}</script></body></html>"
    ).encode("utf-8")


def render_dashboard(state: dict) -> bytes:
    loops = [entry for entry in state.get("loops", []) if entry["status"] not in ("stopped", "unknown")]
    enabled = set(state["enabled"])

    recurring = [t for t in state["timers"] if t["unit"] == "delegation-loop.timer"]
    oneoffs = [t for t in state["timers"] if t["unit"] != "delegation-loop.timer"]
    nxt = recurring[0]["next"] if recurring and recurring[0]["next"] else None

    attention = []
    if not state["timer_active"]:
        attention.append("timer paused")

    body = [f'<header><h1>{icon("M3 11l9-8 9 8M5 10v10h14V10")}Overview</h1></header>']

    # ---- the three questions the overview exists to answer ---------------
    body.append('<div class="stat-row">')
    body.append(
        '<div class="card stat">'
        f'<div class="stat-label">{icon("M17 2l4 4-4 4M3 11V9a3 3 0 013-3h15M7 22l-4-4 4-4M21 13v2a3 3 0 01-3 3H3", 13)}Live loops</div>'
        f'<div class="stat-value">{esc(len(loops))}</div>'
        f'<div class="stat-note">{esc(len(enabled))} repos enabled</div></div>'
    )
    if nxt:
        body.append(
            '<div class="card stat">'
            f'<div class="stat-label">{icon("M12 7v5l3 2M12 3a9 9 0 100 18 9 9 0 000-18z", 13)}Next run</div>'
            f'<div class="stat-value mono" data-until="{nxt:.0f}"></div>'
            f'<div class="stat-note">{esc(time.strftime("%a %H:%M:%S %Z", time.localtime(nxt)))}</div></div>'
        )
    else:
        body.append(
            '<div class="card stat">'
            f'<div class="stat-label">{icon("M12 7v5l3 2M12 3a9 9 0 100 18 9 9 0 000-18z", 13)}Next run</div>'
            '<div class="stat-value">None scheduled</div></div>'
        )
    body.append(
        f'<div class="card stat {"warn" if attention else "ok"}">'
        f'<div class="stat-label">{icon("M12 3l10 18H2zM12 10v5M12 18h.01", 13)}Needs attention</div>'
        f'<div class="stat-value">{esc("; ".join(attention)) if attention else "All clear"}</div></div>'
    )
    body.append("</div>")

    # ---- live loops ----------------------------------------------------
    body.append(
        f'<div class="section-head">{icon("M17 2l4 4-4 4M3 11V9a3 3 0 013-3h15M7 22l-4-4 4-4M21 13v2a3 3 0 01-3 3H3", 15)}<h2>Live loops</h2></div>'
    )
    if not loops:
        body.append(
            "<div class='card loop-empty dim'>No live loops."
            + (
                f" Next run <span data-until='{nxt:.0f}'></span>."
                if nxt
                else " No next run scheduled."
            )
            + "</div>"
        )
    body.append('<div class="loop-grid">')
    for entry in loops:
        body.append(
            f"<a class='card loop-card' href='/loops?repo={quote(entry['repo'], safe='')}&lines=400'>"
        )
        body.append('<div class="loop-head">')
        body.append(f"<span class='dot {LOOP_STATUS_DOT.get(entry['status'], 'idle')}'></span>")
        body.append(f"<b>{esc(entry['repo'])}</b>")
        body.append(f"<span class=dim>{esc(entry['status'])}</span>")
        body.append(f"<span class=dim>{esc(entry.get('backend') or 'herdr')}</span>")
        body.append(f"<span class=dim>platform {esc(entry.get('platform') or '-')}</span>")
        if entry.get("workspace_id"):
            body.append(f"<span class=dim>workspace {esc(entry['workspace_id'])}</span>")
        if entry.get("pane_id"):
            body.append(f"<span class=dim>pane {esc(entry['pane_id'])}</span>")
        if entry["machine"] != machines.hostname():
            body.append(f"<span class=dim>on {esc(entry['machine'])}</span>")
        body.append("</div></a>")
    body.append("</div>")
    # ---- coming up --------------------------------------------------------
    body.append(
        f'<div class="section-head">{icon("M12 7v5l3 2M12 3a9 9 0 100 18 9 9 0 000-18z", 15)}<h2>Coming up</h2></div>'
    )
    body.append("<div class='card scroll'><table>")
    body.append("<tr><th>repository</th><th>next</th><th>at</th><th>last</th></tr>")
    for t in recurring + oneoffs:
        when = f"<span data-until='{t['next']:.0f}'></span>" if t["next"] else "<span class=dim>-</span>"
        at = time.strftime("%a %H:%M:%S %Z", time.localtime(t["next"])) if t["next"] else "-"
        last = f"<span data-since='{t['last']:.0f}'></span>" if t["last"] else "<span class=dim>never</span>"
        body.append(
            f"<tr><td>{esc(timer_repository(t['unit']))}</td><td>{when}</td>"
            f"<td class=dim>{esc(at)}</td><td class=dim>{last}</td></tr>"
        )
    body.append("</table></div>")

    # ---- fleet (issue #15) -------------------------------------------
    body.append(
        f'<div class="section-head">{icon("M17 2l4 4-4 4M3 11V9a3 3 0 013-3h15M7 22l-4-4 4-4M21 13v2a3 3 0 01-3 3H3", 15)}<h2>Fleet</h2></div>'
    )
    fleet_error = state.get("fleet_error")
    if fleet_error:
        body.append(f"<div class='card dim'>Fleet registry unreachable: {esc(fleet_error)}</div>")
    else:
        fleet_machines = state.get("machines", [])
        if not fleet_machines:
            body.append("<div class='card dim'>No machines registered.</div>")
        else:
            body.append("<div class='card scroll'><table>")
            body.append("<tr><th>machine</th><th>state</th><th>version</th><th>heartbeat</th></tr>")
            for m in fleet_machines:
                pill = {
                    "online": "<span class='pill on'>online</span>",
                    "offline": "<span class='pill off'>offline</span>",
                }.get(m["state"], f"<span class=pill>{esc(m['state'])}</span>")
                version = esc(m.get("version") or "-")
                if m.get("version_mismatch"):
                    version += " <span class=pill>mismatch</span>"
                body.append(
                    f"<tr><td>{esc(m['name'])}</td><td>{pill}</td>"
                    f"<td class=dim>{version}</td><td class=dim>{esc(m.get('heartbeat') or '-')}</td></tr>"
                )
            body.append("</table></div>")

        fleet_claims = state.get("claims", {})
        if not fleet_claims:
            body.append("<div class='card dim'>No claimed issues.</div>")
        else:
            body.append("<div class='card scroll'><table>")
            body.append("<tr><th>issue</th><th>claimed by</th><th>host</th></tr>")
            for target, info in sorted(fleet_claims.items()):
                body.append(
                    f"<tr><td>{esc(target)}</td><td>{esc(info.get('session', '-'))}</td>"
                    f"<td class=dim>{esc(info.get('host', '-'))}</td></tr>"
                )
            body.append("</table></div>")

    # ---- repos --------------------------------------------------------
    body.append(
        f'<div class="section-head">{icon("M6 3v12M18 9a3 3 0 100-6 3 3 0 000 6zM6 21a3 3 0 100-6 3 3 0 000 6zM18 9a9 9 0 01-9 9", 15)}<h2>Repos</h2></div>'
    )
    body.append("<div class='card scroll'><table>")
    body.append(
        "<tr><th>repo</th><th>state</th><th>session</th><th>roadmap</th>"
        "<th>one-off command</th></tr>"
    )
    live = {s["repo"] for s in loops}
    for r in state["repos"]:
        pill = {
            "enabled": "<span class='pill on'>enabled</span>",
            "disabled": "<span class='pill off'>disabled</span>",
        }[r["state"]]
        if r.get("local", True):
            sess = "live" if r["repo"] in live else "<span class=dim>-</span>"
            queue = f"<a href='/roadmap?repo={quote(r['repo'], safe='')}'>open</a>"
            once = ""
            if r["state"] == "enabled":
                command = f"lupin once now {r['repo']}"
                once = (
                    f"<span data-once-repo='{esc(r['repo'])}'>"
                    f"<code>{esc(command)}</code> "
                    "<label>when <input value=now aria-label='one-off loop time'></label> "
                    "<button type=button>copy</button> "
                    "<span class=dim data-copy-status aria-live=polite></span></span>"
                )
        else:
            sess = (
                f"running on {esc(r['running_machines'])}"
                if r["running"]
                else f"available on {esc(r['machine'])}"
            )
            queue = once = "<span class=dim>-</span>"
        body.append(
            f"<tr><td>{esc(r['repo'])}</td><td>{pill}</td><td>{sess}</td>"
            f"<td>{queue}</td><td>{once}</td></tr>"
        )
    body.append("</table></div>")

    return page("Overview", "".join(body), active="overview")


def time_until_reset(reset_at_ms, now_ms=None) -> str:
    if not isinstance(reset_at_ms, (int, float)):
        return "-"
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    remaining_ms = reset_at_ms - now_ms
    if remaining_ms <= 0:
        return "now"
    if remaining_ms < 60_000:
        return "<1m"
    remaining_minutes = (remaining_ms + 59_999) // 60_000
    days, remainder = divmod(remaining_minutes, 24 * 60)
    hours, minutes = divmod(remainder, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    return " ".join(parts)


def time_remaining_pct(row: dict, now_ms=None) -> float | None:
    duration = row.get("duration", QuotaDuration.OTHER)
    if not isinstance(duration, QuotaDuration):
        try:
            duration = QuotaDuration(duration)
        except (TypeError, ValueError):
            return None
    reset_at_ms = row.get("resets_at")
    if duration.milliseconds is None or not isinstance(reset_at_ms, (int, float)):
        return None
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    return min(100, max(0, (reset_at_ms - now_ms) / duration.milliseconds * 100))


def quota_duration_label(row: dict) -> str:
    duration = row.get("duration", QuotaDuration.OTHER)
    if isinstance(duration, QuotaDuration) and duration is not QuotaDuration.OTHER:
        return duration.label
    return row.get("label", "Other")


def quota_progress_pct(row: dict) -> float | None:
    used = row.get("used_pct")
    return min(100, max(0, used)) if isinstance(used, (int, float)) else None


def quota_elapsed_pct(row: dict, now_ms=None) -> float | None:
    remaining = time_remaining_pct(row, now_ms)
    return None if remaining is None else 100 - remaining


def quota_status(row: dict, now_ms=None) -> tuple[str, str]:
    used = quota_progress_pct(row)
    elapsed = quota_elapsed_pct(row, now_ms)
    if used is None or elapsed is None:
        return "Status unavailable", ""
    if used > elapsed + 5:
        return "Ahead of pace", "ahead"
    if used < elapsed - 15:
        return "Under pace", "under"
    return "On pace", ""


def render_quota_summary(rows: list[dict], now_ms: int) -> str:
    measured = [row for row in rows if quota_progress_pct(row) is not None]
    ahead = [row for row in measured if quota_status(row, now_ms)[1] == "ahead"]
    most_used = max(measured, key=quota_progress_pct, default=None)
    resets = [
        row for row in rows
        if isinstance(row.get("resets_at"), (int, float))
    ]
    next_reset = min(resets, key=lambda row: row["resets_at"], default=None)
    attention = (
        ", ".join(f"{row['provider']} {quota_duration_label(row)}" for row in ahead)
        if ahead else "None"
    )
    most_value = (
        f"{most_used['provider']} · {quota_duration_label(most_used)}"
        if most_used else "Not available"
    )
    most_note = (
        f"{quota_progress_pct(most_used):.0f}% used" if most_used else "No quota data"
    )
    next_value = (
        f"{next_reset['provider']} · {quota_duration_label(next_reset)}"
        if next_reset else "Not available"
    )
    next_note = (
        f"Resets in {time_until_reset(next_reset['resets_at'], now_ms)}"
        if next_reset else "No reset time"
    )
    items = (
        ("Needs attention", attention, "Usage is ahead of time elapsed" if ahead else "No quota is ahead of pace"),
        ("Most used", most_value, most_note),
        ("Next reset", next_value, next_note),
    )
    return "<div class=quota-summary>" + "".join(
        "<div class=quota-summary-item>"
        f"<div class=quota-summary-key>{esc(key)}</div>"
        f"<div class=quota-summary-value>{esc(value)}</div>"
        f"<div class=quota-summary-note>{esc(note)}</div></div>"
        for key, value, note in items
    ) + "</div>"


def render_quota_row(row: dict, now_ms: int) -> str:
    duration = row.get("duration", QuotaDuration.OTHER)
    duration_value = duration.value if isinstance(duration, QuotaDuration) else str(duration)
    reset_at_ms = row.get("resets_at")
    reset_timestamp = "" if reset_at_ms is None else str(reset_at_ms)
    used = quota_progress_pct(row)
    elapsed = quota_elapsed_pct(row, now_ms)
    status, status_class = quota_status(row, now_ms)
    available_text = "--" if used is None else f"{max(0, 100 - used):.0f}%"
    used_text = "--" if used is None else f"{used:.0f}%"
    used_width = "0%" if used is None else f"{used:.1f}%"
    elapsed_marker = (
        "" if elapsed is None
        else f"<div class=quota-meter-elapsed style='left:{elapsed:.1f}%' "
        "title='time elapsed in window'></div>"
    )
    aria_label = "quota usage unavailable"
    if used is not None and elapsed is not None:
        aria_label = f"{used:.0f}% used, {elapsed:.0f}% of window elapsed"
    elif used is not None:
        aria_label = f"{used:.0f}% used"
    return (
        f"<div class=quota-row data-window-duration='{esc(duration_value)}' "
        f"data-resets-at-ms='{esc(reset_timestamp)}'>"
        "<div><div class=quota-window>"
        f"{esc(quota_duration_label(row))}</div>"
        f"<div class='quota-status {status_class}'>{esc(status)}</div></div>"
        "<div>"
        f"<div class=quota-values><span><strong>{available_text}</strong> available</span>"
        f"<span>{used_text} used</span></div>"
        f"<div class=quota-meter role=img aria-label='{esc(aria_label)}'>"
        f"<div class='quota-meter-used {status_class}' style='width:{used_width}'></div>"
        f"{elapsed_marker}</div></div>"
        f"<div class=quota-reset><div class=quota-reset-left>"
        f"{esc(time_until_reset(reset_at_ms, now_ms))}</div>"
        f"<div class=quota-reset-at>{esc(epoch_ms_to_local(reset_at_ms))}</div></div>"
        "</div>"
    )


def render_usage(*, connection: dict | None = None) -> bytes:
    """Read quota and 7-day usage snapshots from Redis on each page render.

    Scheduled `lupin quota` runs publish the data; this page does not call
    provider APIs or read machine-local usage files.
    """
    connection = connection or {}
    snapshot = quota_cache.read_snapshot(**connection)
    usage_snapshots = usage_cache.read_snapshot(**connection)
    quota_rows: list[dict] = []
    fetched_by_provider: dict[str, dict] = {}
    for provider, entry in snapshot.items():
        rows = entry.get("rows") or []
        quota_rows.extend(rows)
        fetched_by_provider[provider] = entry
    now_ms = int(time.time() * 1000)
    fetched = next((row["generated_at"] for row in quota_rows if "generated_at" in row), None)
    body = [
        f'<header><h1>{icon("M5 20V10M12 20V4M19 20v-7")}Usage</h1></header>',
        "<div class=quota-heading><h2>Quota</h2>"
        f"<span class=dim>Data timestamp: {esc(fetched) if fetched else 'not available'}</span></div>",
        render_quota_summary(quota_rows, now_ms),
        "<div class=quota-legend>"
        "<span class=quota-legend-item><span class=quota-legend-used></span>used</span>"
        "<span class=quota-legend-item><span class=quota-legend-time></span>"
        "time elapsed in window</span>"
        "<span class=quota-legend-item><span class=quota-legend-ahead></span>"
        "used faster than time</span></div><div class=quota-groups>",
    ]
    if not quota_rows:
        body.append(
            "<p class=dim>No quota data cached yet -- run <code>lupin quota</code> "
            "on a machine with real provider credentials.</p>"
        )
    groups: dict[str, list[dict]] = {}
    for row in quota_rows:
        groups.setdefault(row["provider"], []).append(row)
    for provider, rows in groups.items():
        entry = fetched_by_provider.get(provider) or {}
        age = _snapshot_age(entry.get("fetched_at"))
        fetched_by = entry.get("fetched_by", "-")
        stale = "" if quota_cache.is_fresh(entry) else " &middot; <span class=stale>stale</span>"
        body.append(
            f"<section class='card quota-group'><div class=quota-group-heading>"
            f"<h3>{esc(provider)}</h3><span class=dim>"
            f"{esc(quota_source_label(provider))} &middot; fetched {age} via {esc(fetched_by)}{stale}"
            "</span></div>"
        )
        for row in rows:
            if "error" in row or "note" in row:
                message = row.get("error", row.get("note"))
                body.append(f"<p class=dim>{esc(message)}</p>")
            else:
                body.append(render_quota_row(row, now_ms))
        body.append("</section>")
    body.append(
        "</div><p class=dim>Bars show quota used; the marker shows time elapsed "
        "in the window. The reset time appears at the right. OpenCode Go uses "
        "omp or Orca's usage API; Claude uses Anthropic's OAuth usage API. "
        "OpenAI uses omp or Codex's latest local snapshot, which only updates "
        "when Codex writes a session event.</p>"
    )
    body.append("<h2>7-day totals</h2>")
    if usage_snapshots:
        sources = "; ".join(
            f"{esc(machine)} fetched {_snapshot_age(entry.get('fetched_at'))}"
            + ("" if usage_cache.is_fresh(entry, now=now_ms / 1000) else " &middot; <span class=stale>stale</span>")
            for machine, entry in sorted(usage_snapshots.items())
        )
        body.append(f"<p class=dim>Usage snapshots: {sources}</p>")
    else:
        body.append(
            "<p class=dim>No shared 7-day usage data cached yet -- run "
            "<code>lupin quota</code> on a machine with local usage data.</p>"
        )
    body.append(
        "<p class=dim>Totals combine the last 7 days from each machine's local "
        "Claude and OMP statistics. Claude's local cache reports one combined "
        "token total per day, not an input/output split, and no daily cost.</p>"
        "<div class='card scroll'><table><tr><th>provider</th>"
        "<th>input tokens</th><th>output tokens</th><th>cost</th>"
        "<th>period</th><th>source</th><th>last update</th></tr>"
    )
    rows = usage_cache.aggregate_rows(usage_snapshots)
    for row in rows:
        if "error" in row:
            body.append(
                f"<tr><td>{esc(row['provider'])}</td><td colspan=3>"
                f"{esc(row['error'])}</td><td>last 7 days</td>"
                f"<td>{esc(row['source'])}</td><td>-</td></tr>"
            )
            continue
        input_tokens = "-" if row["input_tokens"] is None else esc(row["input_tokens"])
        output_tokens = "-" if row["output_tokens"] is None else esc(row["output_tokens"])
        cost = "not tracked" if row["cost"] is None else f"${row['cost']:.2f}"
        body.append(
            f"<tr><td>{esc(row['provider'])}</td>"
            f"<td>{input_tokens}</td>"
            f"<td>{output_tokens}</td>"
            f"<td>{cost}</td><td>{esc(row['period'])}</td>"
            f"<td>{esc(row['source'])}</td><td>{esc(row['last_update'])}</td></tr>"
        )
    body.append("</table></div>")
    return page(
        "Agent usage", "".join(body), extra_css="main{max-width:none}", active="usage"
    )


def unavailable_tiers(error: Exception) -> list[dict]:
    return [{
        "error": f"unavailable ({type(error).__name__})",
        "source": MODEL_TIERS_PATH,
    }]


def model_tiers() -> list[dict]:
    """Read MODEL_TIERS_PATH, one row per task category."""
    try:
        with open(MODEL_TIERS_PATH, encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, ValueError) as error:
        return unavailable_tiers(error)
    if not isinstance(raw, dict):
        return [{"error": "no categories in the file", "source": MODEL_TIERS_PATH}]
    rows = []
    for category, entry in raw.items():
        # Keys starting with "_" are file comments, not categories.
        if category.startswith("_") or not isinstance(entry, dict):
            continue
        tiers = entry.get("tiers")
        rows.append({
            "category": category,
            "source": entry.get("source", "not recorded"),
            "last_verified": entry.get("last_verified", "-"),
            "tiers": tiers if isinstance(tiers, dict) else {},
            "note": entry.get("note", ""),
        })
    return rows


_ALIAS_PREFIX = re.compile(r"^(?:bmo|local):|^(?:opencode-go|openai)/")


def load_model_snapshot(connection: dict | None = None) -> dict | None:
    """Read the fleet snapshot, then this host's file as a fallback."""
    shared = model_fetch.read_shared_snapshot(**(connection or {}))
    if shared is not None:
        return shared
    try:
        with open(model_fetch.SNAPSHOT_FILE, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def snapshot_models(snapshot: dict | None) -> list[dict]:
    """Flatten a snapshot's per-subscription model lists into one list,
    each row tagged with which subscription it came from and whether that
    subscription's fetch was live that day."""
    rows = []
    for subscription, info in (snapshot or {}).get("subscriptions", {}).items():
        if not isinstance(info, dict):
            continue
        for model in info.get("models") or []:
            if not isinstance(model, dict) or not model.get("id"):
                continue
            rows.append({
                "subscription": subscription,
                "id": model["id"],
                "display_name": model.get("display_name") or model["id"],
                "price": model.get("price"),
                "promo": model.get("promo"),
                "live": bool(info.get("live")),
                "stale_reason": info.get("stale_reason") or info.get("error"),
            })
    return rows


def match_live_model(alias: str, models: list[dict]) -> dict | None:
    """Match a model-tiers.json alias (short hand names like "sonnet" or
    "opencode-go/glm-5.3") to a snapshot model (full API IDs like
    "claude-sonnet-4-5-..."). It is not a 1:1 lookup, so this is a
    heuristic, not a resolver:

    - A "bmo:" or "local:" prefix is stripped before matching, but those
      two are never found -- `model_fetch` only covers the claude,
      opencode-go, and codex subscriptions, not bmo's or a local model
      server's own catalog. Those aliases always report "no live data".
    - An "opencode-go/" or "openai/" prefix is stripped too, because the
      snapshot's own `subscription` field already says which service a
      model id belongs to -- the tier file repeats it in the alias, and
      the badge would otherwise never match.
    - What remains is looked up as a case-insensitive substring of a
      snapshot model's id or display name, first match wins. Good enough
      to flag "known reachable today" without pretending to be a precise
      ID resolver -- a short alias like "opus" could in principle match
      more than one real id (e.g. a future "opus-mini"), but today's
      catalogs don't have that collision.
    """
    bare = _ALIAS_PREFIX.sub("", alias).strip().lower()
    if not bare:
        return None
    for model in models:
        haystack = f"{model['id']} {model.get('display_name', '')}".lower()
        if bare in haystack:
            return model
    return None


def _format_price(price: dict | None) -> str:
    if not price:
        return "no price data"
    input_price, output_price = price.get("input"), price.get("output")
    if input_price is None and output_price is None:
        return "no price data"
    def fmt(value):
        return f"${value:.2f}" if isinstance(value, (int, float)) else "-"

    return f"{fmt(input_price)} / {fmt(output_price)} per Mtok"


def _live_badge(alias: str, models: list[dict] | None) -> str:
    """A tier pick's live-match note: today's price if `match_live_model`
    finds one, "no live data" if not, or nothing at all when the caller
    (e.g. an existing test of the static chain) passed no snapshot."""
    if models is None:
        return ""
    match = match_live_model(alias, models)
    if not match:
        return "<span class=dim> &middot; no live data</span>"
    return f"<span class=dim> &middot; {esc(_format_price(match['price']))}</span>"


def render_tier_picks(tiers: dict, models: list[dict] | None = None) -> str:
    """One row per tier: its ordered fallback chain, or that it has none.

    `models` is the day's snapshot (issue #16), optional for callers that
    only want the static chain (e.g. existing tests). When given, each
    pick gets a live-match badge from `match_live_model` -- today's price
    if matched, "no live data" if not.
    """
    rows = []
    for tier in MODEL_TIER_ORDER:
        entries = tiers.get(tier)
        picks = [
            pick
            for pick in (entries if isinstance(entries, list) else [])
            if isinstance(pick, dict)
        ]
        chain = "<span class=tier-arrow>&rarr;</span>".join(
            "<span class=tier-pick>"
            f"<span class=tier-model>{esc(pick.get('model', '-'))}</span>"
            f"<span class=dim>{esc(pick.get('effort', '-'))}</span>"
            f"{_live_badge(pick.get('model', ''), models)}"
            "</span>"
            for pick in picks
        ) or "<span class='tier-pick dim'>none</span>"
        rows.append(
            f"<div class=tier-row><span class=tier-name>{esc(tier)}</span>"
            f"<div class=tier-picks>{chain}</div></div>"
        )
    return "".join(rows)


def _snapshot_age(fetched_at: str | None) -> str:
    if not fetched_at:
        return "never pulled"
    try:
        epoch = datetime.fromisoformat(fetched_at).timestamp()
    except ValueError:
        return "never pulled"
    return f"<span data-since='{epoch:.0f}'></span>"


def _format_score(scores: list[dict]) -> str:
    if not scores:
        return "<span title='No source-verified score for this model'>—</span>"
    cells = []
    for score in scores:
        title = f"{score['benchmark']} · {score['metric']} · {score['source']}"
        cells.append(f"<span title='{esc(title)}'>{score['score']:g}</span>")
    return "<br>".join(cells)


def _format_research_note(note: dict | None) -> str:
    if not isinstance(note, dict):
        return "—"
    text = note.get("text")
    source = note.get("source")
    if (
        not isinstance(text, str)
        or not text.strip()
        or not isinstance(source, str)
        or not source.startswith(("https://", "http://"))
    ):
        return "—"
    return (
        f"{esc(text)}<div class=dim>"
        f"<a href='{esc(source)}' target='_blank' rel='noopener'>Source</a></div>"
    )


def _format_value(performance: float | None, price: dict | None) -> str:
    if performance is None or not price:
        return "—"
    prices = [value for value in (price.get("input"), price.get("output")) if isinstance(value, (int, float))]
    blended = sum(prices) / len(prices) if prices else 0
    return f"{performance / blended:.1f} pts/$" if blended else "—"


def render_benchmarks_by_category(categories: list[dict], models: list[dict]) -> str:
    names = {model["id"]: model.get("display_name") or model["id"] for model in models}
    rows = []
    for category in categories:
        benchmarks = category["benchmarks"]
        if not benchmarks:
            note = category.get("note") or "No source-verified model scores."
            source = category.get("source")
            source_link = (
                f" <a href='{esc(source)}' target='_blank' rel='noopener'>Source</a>"
                if source else ""
            )
            rows.append(
                f"<tr><td>{esc(category['name'])}</td>"
                f"<td colspan='2' class=dim>{esc(note)}{source_link}</td></tr>"
            )
            continue
        for benchmark in benchmarks:
            scores = sorted(benchmark["scores"], key=lambda item: item["score"], reverse=True)[:3]
            leaders = ", ".join(
                f"{esc(score.get('label') or names.get(score['model'], score['model']))}: {score['score']:g}"
                for score in scores
            ) or "No matching models in today's snapshot"
            source = (
                f"<a href='{esc(benchmark['source'])}' target='_blank' rel='noopener'>"
                f"{esc(benchmark['name'])}</a>"
            )
            rows.append(
                f"<tr><td>{esc(category['name'])}</td>"
                f"<td>{source}<div class=dim>{esc(benchmark['metric'])}</div></td>"
                f"<td>{leaders}<div class=dim>source checked {esc(str(benchmark.get('retrieved_on') or 'date unknown'))}</div></td></tr>"
            )
        if category.get("note"):
            rows.append(
                f"<tr><td></td><td colspan='2' class=dim>{esc(category['note'])}</td></tr>"
            )
    return (
        "<div class='card scroll'><table><thead><tr>"
        "<th>category</th><th>benchmark</th><th>top source scores</th>"
        f"</tr></thead><tbody>{''.join(rows)}</tbody></table></div>"
    )


def render_snapshot_models_table(
    models: list[dict],
    categories: list[dict],
    by_model: dict,
    performance: dict,
    notes_by_model: dict[str, dict] | None = None,
) -> str:
    if not models:
        return (
            "<p class=dim>No model snapshot yet. Run <code>lupin fetch-models</code> "
            "or click \"Pull models\" above.</p>"
        )
    category_heads = "".join(f"<th>{esc(category['name'])}</th>" for category in categories)
    notes_by_model = notes_by_model or {}
    rows = []
    for model in sorted(models, key=lambda item: (item["subscription"], item["id"])):
        model_id = model["id"]
        cells = "".join(
            f"<td>{_format_score(by_model.get(model_id, {}).get(category['id'], []))}</td>"
            for category in categories
        )
        perf = performance.get(model_id)
        perf_text = f"{perf:.1f}%" if perf is not None else "—"
        model_meta = f"<div class=dim>{esc(model['subscription'])}</div>"
        if model.get("stale_reason"):
            model_meta += f"<div class=dim>{esc(model['stale_reason'])}</div>"
        rows.append(
            "<tr>"
            f"<td>{esc(model.get('display_name') or model_id)}{model_meta}</td>"
            f"{cells}"
            f"<td>{esc(_format_price(model.get('price')))}</td>"
            f"<td>{perf_text}</td>"
            f"<td>{esc(_format_value(perf, model.get('price')))}</td>"
            f"<td>{_format_research_note(notes_by_model.get(model_id))}</td>"
            "</tr>"
        )
    return (
        "<div class='card scroll'><table><thead><tr><th>model</th>"
        f"{category_heads}<th>$ input / output</th><th>perf</th><th>value</th><th>notes</th>"
        f"</tr></thead><tbody>{''.join(rows)}</tbody></table></div>"
    )


def render_model_tiers(*, sent: str | None = None, connection: dict | None = None) -> bytes:
    rows = model_tiers()
    snapshot = load_model_snapshot(connection)
    models = snapshot_models(snapshot)
    benchmark_snapshot = benchmark_fetch.read_snapshot(**(connection or {}))
    benchmark_categories, scores_by_model, performance = benchmark_catalog.matrix(models, benchmark_snapshot)
    notes_by_model = benchmark_catalog.notes_by_model(benchmark_snapshot)
    benchmark_scores = (benchmark_snapshot or {}).get("scores") or []
    scored_benchmarks = sum(
        1 for score in benchmark_scores
        if isinstance(score, dict) and score.get("score") is not None
    )
    body = [
        f'<header><h1>{icon("M7 7h10v10H7zM9 2v3M15 2v3M9 19v3M15 19v3M2 9h3M2 15h3M19 9h3M19 15h3")}Models</h1></header>',
    ]
    if sent:
        body.append(f"<p class=dim>{esc(sent)}</p>")
    body.append(
        "<div class='card' style='display:flex;align-items:center;gap:1rem'>"
        f"<span class=dim>Last pulled: {_snapshot_age(snapshot.get('fetched_at') if snapshot else None)}</span>"
        f"<span class=dim>General scores: {_snapshot_age(benchmark_snapshot.get('fetched_at') if benchmark_snapshot else None)} ({scored_benchmarks}/{len(benchmark_scores)} scored)</span>"
        "<span style='flex:1'></span>"
        "<form method=post action='/model-tiers/refresh'>"
        "<button type=submit>Pull models</button></form>"
        "<form method=post action='/model-tiers/refresh-benchmarks'>"
        "<button type=submit>Pull benchmarks</button></form></div>"
    )
    body.extend([
        "<h2>Routing by task category</h2>",
        "<p class=dim>Read from <code>"
        f"{esc(MODEL_TIERS_PATH)}</code>. Each tier is an ordered fallback "
        "chain: the first entry is tried first, then the next. A category "
        "with no entry for a tier escalates to the next tier up. The "
        "&middot; note after each pick is today's live match (issue #16's "
        "fetch), when one is found.</p>",
        "<div class=tier-grid>",
    ])
    for row in rows:
        if "error" in row:
            body.append(
                "<section class='card tier-card'>"
                f"<p class=dim>{esc(row['source'])}</p>"
                f"<p class=dim>{esc(row['error'])}</p></section>"
            )
            continue
        note = row["note"]
        body.append(
            "<section class='card tier-card'>"
            "<div class=tier-heading>"
            f"<h3>{esc(row['category'])}</h3>"
            f"<span class=dim>verified {esc(row['last_verified'])}</span></div>"
            f"<div class=dim>{esc(row['source'])}</div>"
            f"{render_tier_picks(row['tiers'], models)}"
            + (f"<p class='tier-note dim'>{esc(note)}</p>" if note else "")
            + "</section>"
        )
    if not rows:
        body.append("<div class='card dim'>No task categories.</div>")
    body.append("</div>")
    body.extend([
        "<h2>Benchmarks by category</h2>",
        "<p class=dim>Scores link to their sources. We show a score only for an exact model ID; "
        "provider-family scores do not stand in for model scores. Empty cells mean no verified "
        "public score is available. We may add our own tests later, one model at a time. "
        "No private model tests were run for this page.</p>",
        render_benchmarks_by_category(benchmark_categories, models),
        "<h2>All models</h2>",
        "<p class=dim>Perf is each model's average score as a percent of the best result in each "
        "available benchmark, averaged equally across categories. Value is Perf divided by the "
        "blended input/output price per million tokens. Rows with no matching score show —. "
        "Notes show source-backed public observations. Older snapshots may not include them.</p>",
        render_snapshot_models_table(
            models, benchmark_categories, scores_by_model, performance, notes_by_model
        ),
    ])
    return page("Model tiers", "".join(body), active="models")


def _heartbeat_epoch(stamp: str | None) -> float | None:
    """Return epoch seconds for a machine heartbeat timestamp."""
    if not stamp:
        return None
    try:
        return datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None


def _slot_controls(slot: str, current_max: int) -> str:
    """Fleet-wide slot limit controls with race-free next values."""
    fewer = max(1, current_max - 1)
    more = current_max + 1
    fewer_disabled = " disabled" if current_max <= 1 else ""
    return (
        "<form method=post action='/machines/slot-max' style='display:inline'>"
        f"<input type=hidden name=slot value='{esc(slot)}'>"
        f"<input type=hidden name=max value='{fewer}'>"
        f"<button type=submit aria-label='Fewer slots'{fewer_disabled}>−</button></form>"
        "<form method=post action='/machines/slot-max' style='display:inline'>"
        f"<input type=hidden name=slot value='{esc(slot)}'>"
        f"<input type=hidden name=max value='{more}'>"
        "<button type=submit aria-label='More slots'>+</button></form>"
    )


def render_machines(
    records: list[dict],
    slot_status: dict,
    *,
    recurring_timer: dict | None = None,
    timer_running: bool | None = None,
) -> bytes:
    """Render machine health separately from the fleet-wide slot limits."""
    local_host = machines.hostname()
    online = sum(record["state"] == "online" for record in records)
    timer_html = ""
    if timer_running is not None:
        timer_state = "running" if timer_running else "paused"
        timer_dot = "online" if timer_running else "offline"
        timer_html = (
            f"<span class=machine-timer><span class='machine-dot {timer_dot}'></span>"
            f"Timer {timer_state}</span>"
        )
        next_run = recurring_timer.get("next") if recurring_timer else None
        if next_run:
            timer_html += (
                f"<span class=machine-timer>Next run "
                f"<b class=mono data-until='{next_run:.0f}'></b></span>"
            )
    body = [
        f'<header><h1>{icon("M4 4h16v6H4zM4 14h16v6H4zM8 7h.01M8 17h.01")}Machines</h1>'
        f'<span class=sp></span>{timer_html}'
        f'<span class="dim machine-count">{online} of {len(records)} machines online</span></header>'
    ]
    if slot_status:
        body.append("<section class=machine-slots><h2>Fleet-wide capacity</h2>")
        body.append(
            "<p class=dim>Each slot has one shared limit across all machines.</p>"
            "<div class=machine-slot-list>"
        )
        for slot_name, info in sorted(slot_status.items()):
            used = info.get("holders", 0)
            slot_max = info.get("max")
            controls = _slot_controls(slot_name, slot_max if slot_max is not None else 1)
            body.append(
                f"<div class=machine-slot><b>{esc(slot_name)}</b>"
                f"<span class=mono>{esc(used)} / {esc(slot_max) if slot_max is not None else '-'}</span>"
                f"<span class=slot-controls>{controls}</span></div>"
            )
        body.append("</div></section>")

    if records:
        body.append("<div class=machine-grid>")
        for record in sorted(records, key=lambda r: r["name"]):
            state = record["state"]
            state_class = {"online": "online", "draining": "draining"}.get(state, "offline")
            body.append("<article class=machine-card>")
            body.append("<div class=machine-heading>")
            body.append(f"<span class='machine-dot {state_class}'></span>")
            body.append(f"<b>{esc(record['name'])}</b>")
            if record["name"] == local_host:
                body.append("<span class=machine-local>This machine</span>")
            body.append(f"<span class=machine-state>{esc(state)}</span></div>")
            detail = [f"lupin {esc(record.get('version') or '-')}"]
            hb = _heartbeat_epoch(record.get("heartbeat"))
            if hb is not None:
                detail.append(f"<span data-since='{hb:.0f}'></span> since heartbeat")
            body.append(f"<div class='machine-meta mono'>{' · '.join(detail)}</div>")

            loops = record.get("loops") or []
            body.append("<div class=machine-loops><b>Loops</b>")
            if loops:
                for loop in loops:
                    label = loop.get("repo") or "Unknown repo"
                    if loop.get("platform"):
                        label += f" · {loop['platform']}"
                    body.append(
                        f"<div class=machine-loop><span>{esc(label)}</span>"
                        f"<span class=mono>{esc(loop.get('state') or 'unknown')}</span></div>"
                    )
            else:
                body.append("<div class=dim>No active loops.</div>")
            body.append("</div>")

            if state == "draining":
                body.append(
                    "<p class=machine-notice>Draining. Running loops can finish, "
                    "but Lupin will not place new work here.</p>"
                )
            if record.get("version_mismatch"):
                body.append(
                    f"<p class=machine-notice>Version mismatch. This machine runs "
                    f"{esc(record.get('version') or 'an unknown version')}.</p>"
                )
            providers = record.get("providers") or []
            if providers:
                body.append("<div class=machine-providers><span class=dim>Signed in</span>")
                for provider in providers:
                    body.append(f"<span class=machine-provider>{esc(provider)}</span>")
                body.append("</div>")
            body.append("</article>")
        body.append("</div>")
    else:
        body.append("<div class='card dim'>No machine has joined the fleet yet.</div>")

    body.append(
        "<p class='dim machine-join'>To add a machine, run "
        "<code>lupin join &lt;redis-host&gt;[:port]</code> on it.</p>"
    )
    extra_css = """
.machine-timer{display:flex;align-items:center;gap:6px;white-space:nowrap;font-size:13px;color:var(--ink2)}
.machine-timer .machine-dot{width:7px;height:7px}
.machine-timer b{font-weight:500;color:var(--ink);font-size:12px}
.machine-count{font-size:13px}
.machine-slots{margin:1rem 0 1.2rem}
.machine-slots h2{margin:0 0 .25rem}
.machine-slots>p{margin:.2rem 0 .65rem;font-size:13px}
.machine-slot-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(245px,1fr));gap:.6rem}
.machine-slot{display:flex;align-items:center;gap:.8rem;background:var(--surface);
border:1px solid var(--line);border-radius:12px;padding:.65rem .8rem}
.slot-controls{display:flex;gap:4px}
.slot-controls button{width:24px;height:24px;padding:0;border:1px solid var(--line);
border-radius:6px;background:var(--surface);color:var(--ink);cursor:pointer;font:500 14px var(--sans)}
.slot-controls button:disabled{opacity:.45;cursor:default}
.slot-controls button:focus-visible{outline:2px solid var(--ok);outline-offset:2px}
.machine-slot>b{flex:1;overflow-wrap:anywhere}
.machine-slot .mono{font-size:13px;white-space:nowrap}
.slot-controls{white-space:nowrap}
.machine-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:1rem}
.machine-card{display:grid;gap:.7rem;align-content:start;background:var(--surface);
border:1px solid var(--line);border-radius:18px;box-shadow:0 3px 0 var(--line);padding:1rem 1.1rem}
.machine-heading{display:flex;align-items:center;gap:.6rem;min-width:0}
.machine-heading>b{font-size:16px;overflow-wrap:anywhere}
.machine-dot{width:8px;height:8px;flex:none;border-radius:2px;background:var(--warn)}
.machine-dot.online{background:var(--ok)}
.machine-dot.draining{background:var(--warn)}
.machine-local,.machine-provider{font-size:12px;padding:1px 8px;border-radius:6px;
background:var(--lavbg);color:var(--lav)}
.machine-state{margin-left:auto;font-size:13px;color:var(--ink2)}
.machine-meta{margin-top:-.6rem;color:var(--ink3);font-size:12px}
.machine-loops{display:grid;gap:.35rem;font-size:13px}
.machine-loops>b{margin-bottom:.1rem}
.machine-loop{display:flex;justify-content:space-between;gap:.6rem}
.machine-notice{margin:0;padding:.55rem .7rem;background:var(--warnbg);
border:1px solid var(--warnline);border-radius:10px;color:var(--warnink);font-size:13px}
.machine-providers{display:flex;align-items:center;gap:.4rem;flex-wrap:wrap;font-size:12px}
.machine-join{margin-top:1.1rem;font-size:13px}
@media(max-width:760px){.machine-grid{grid-template-columns:1fr}}
"""
    return page("Machines", "".join(body), extra_css=extra_css, active="machines")


STATE_PILL = {
    "enabled": "<span class='pill on'>enabled</span>",
    "disabled": "<span class=pill>disabled</span>",
}
REPOS_ICON = "M6 3v12M18 9a3 3 0 100-6 3 3 0 000 6zM6 21a3 3 0 100-6 3 3 0 000 6zM18 9a9 9 0 01-9 9"


def _repo_slot_controls(repo: str, current_max: int) -> str:
    """Same fewer/more button-pair shape `_slot_controls` uses on the
    Machines page, but the form only ever posts `repo` -- `do_repos_slot_max`
    derives the actual slot name (`_repo_slot_name`) itself, so a request
    can only ever change the one slot that belongs to the repo it named,
    never an arbitrary slot string chosen by the browser.
    """
    fewer = max(1, current_max - 1)
    more = current_max + 1
    fewer_disabled = " disabled" if current_max <= 1 else ""
    return (
        "<form method=post action=/repos/slot-max style='display:inline'>"
        f"<input type=hidden name=repo value='{esc(repo)}'>"
        f"<input type=hidden name=max value='{fewer}'>"
        f"<button type=submit{fewer_disabled} aria-label='Lower max'>&minus;</button></form> "
        f"<span class=mono>{current_max}</span> "
        "<form method=post action=/repos/slot-max style='display:inline'>"
        f"<input type=hidden name=repo value='{esc(repo)}'>"
        f"<input type=hidden name=max value='{more}'>"
        "<button type=submit aria-label='Raise max'>+</button></form>"
    )


def _render_add_panel(tab: str, repos: list[dict]) -> str:
    """The "Add repo" card. "Existing repo" (issue #23) is fully wired:
    a repo with a doc gets an "add to schedule" button, one without gets
    "generate docs and add". "Clone from Git" and "Create new" are shown
    but inert -- see this issue's report for why: both are real
    filesystem/git-network operations (an arbitrary clone URL, process
    spawning, file generation) with a bigger security surface than
    anything else on this page, and landing them safely didn't fit this
    pass.
    """
    tab = "new" if tab == "new" else "existing"

    def tab_link(value: str, label: str) -> str:
        active = value == tab
        text = f"<b>{esc(label)}</b>" if active else esc(label)
        return f"<a href='/repos?add={value}'>{text}</a>"

    tabs = f"<div class=row style='margin-bottom:.6rem'>{tab_link('existing', 'Existing repo')} {tab_link('new', 'Create new')}</div>"

    if tab == "new":
        body = (
            "<input type=text disabled placeholder='repo name, e.g. payments-sync' "
            "style='display:block;width:100%;max-width:420px;margin-bottom:.5rem'>"
            "<textarea disabled placeholder='message for the loop (optional)' "
            "style='display:block;width:100%;max-width:420px;height:80px;margin-bottom:.5rem'></textarea>"
            "<button type=button disabled>Create repo</button>"
            "<p class=dim>Not implemented yet -- scaffolding a new repo and generating its "
            "delegation doc is a bigger surface than this pass takes on.</p>"
        )
        return f"<div class=card>{tabs}{body}</div>"

    clone = (
        "<div style='font-size:12px;color:var(--ink3);margin-bottom:6px'>Clone from Git</div>"
        "<input type=text disabled placeholder='git@github.com:owner/name.git or https URL' "
        "style='width:100%;max-width:420px'> "
        "<button type=button disabled>Clone and add</button>"
        "<p class=dim>Not implemented yet.</p>"
    )
    rows = []
    local_repos = [repo for repo in repos if repo.get("local", True)]
    for r in local_repos:
        if r["state"] == "enabled":
            continue
        repo = r["repo"]
        doc_status = (
            f"has {esc(LOOP_DOC)}"
            if r.get("has_doc")
            else "no delegation doc (optional)"
        )
        add_form = (
            "<form method=post action=/repos/add style='display:inline'>"
            f"<input type=hidden name=repo value='{esc(repo)}'>"
            "<button type=submit>Add to schedule</button></form>"
        )
        generate_form = (
            "<form method=post action=/repos/generate-docs style='display:inline'>"
            f"<input type=hidden name=repo value='{esc(repo)}'>"
            "<button type=submit>Generate docs and add</button></form>"
            if not r.get("has_doc")
            else ""
        )
        rows.append(
            "<div class=row style='justify-content:space-between;border-top:1px solid var(--line2);padding:.4rem 0'>"
            f"<span><b>{esc(repo)}</b> <span class=dim>{doc_status}</span></span>"
            f"{add_form}{generate_form}</div>"
        )
    if rows:
        picker = "".join(rows)
    elif local_repos:
        picker = "<p class=dim>Every repo under /code is already on the schedule.</p>"
    elif repos:
        picker = "<p class=dim>No local repos under /code. Fleet repos are listed below.</p>"
    else:
        picker = "<p class=dim>No repos found under /code.</p>"
    body = (
        f"{clone}"
        f"<div style='font-size:12px;color:var(--ink3);margin:14px 0 6px'>Or pick a directory in /code</div>"
        f"{picker}"
    )
    return f"<div class=card>{tabs}{body}</div>"


def _render_doc_panel(repo: str, text: str, edit: bool) -> str:
    view_href = f"/repos?doc={quote(repo, safe='')}"
    edit_href = f"/repos?doc={quote(repo, safe='')}&edit=1"
    if edit:
        inner = (
            "<form method=post action=/repos/doc/save>"
            f"<input type=hidden name=repo value='{esc(repo)}'>"
            f"<textarea name=text spellcheck=false "
            "style='width:100%;height:230px;font-family:monospace;box-sizing:border-box'>"
            f"{esc(text)}</textarea>"
            "<div class=row style='margin-top:.4rem'><button type=submit>Save</button>"
            f"<a href='{view_href}'>cancel</a></div></form>"
        )
    else:
        inner = f"<pre style='max-height:230px'>{esc(text)}</pre>"
    view_label = "View" if edit else "<b>View</b>"
    edit_label = "<b>Edit</b>" if edit else "Edit"
    return (
        "<div class=card>"
        f"<div class=row><b class=mono>{esc(repo)}/{esc(LOOP_DOC)}</b><span class=sp></span>"
        f"<a href='{view_href}'>{view_label}</a> <a href='{edit_href}'>{edit_label}</a> "
        "<a href='/repos'>Close</a></div>"
        f"{inner}"
        "<p class=dim>Loops pick up changes at the start of their next run.</p>"
        "</div>"
    )


def _render_schedule_panel(repo: str) -> str:
    """Render the local Lupin command for a one-off run."""
    return (
        "<div class=card>"
        "<form method=post action=/repos/schedule class=row>"
        f"<b>Schedule a one-off run for {esc(repo)}</b>"
        f"<input type=hidden name=repo value='{esc(repo)}'>"
        "<input type=text name=when placeholder='15:00, +2h, tomorrow 09:00' required>"
        "<button type=submit>Schedule</button>"
        "<a href='/repos'>Cancel</a>"
        "</form>"
        "<p class=dim>Runs once, in addition to the recurring schedule. Same as "
        f"<code>lupin once &lt;when&gt; {esc(repo)}</code>.</p>"
        "</div>"
    )


def _render_remove_panel(repo: str) -> str:
    return (
        "<div class=card style='border-color:var(--warnline);background:var(--warnbg)'>"
        "<form method=post action=/repos/remove class=row>"
        f"<div><b>Remove {esc(repo)} from the schedule?</b>"
        "<div class=dim>Stops scheduled and one-off runs. A live session keeps "
        "running. Files in /code are untouched. Type the repo name to confirm.</div></div>"
        f"<input type=hidden name=repo value='{esc(repo)}'>"
        f"<input type=text name=confirm placeholder='{esc(repo)}' required>"
        "<button type=submit>Remove repo</button>"
        "<a href='/repos'>Cancel</a>"
        "</form></div>"
    )


def _remote_repo_controls(repo: str, entry: dict) -> str:
    controls = []
    for machine in entry.get("enabled_hosts", []):
        controls.append(
            "<form method=post action=/loops/start style='display:inline'>"
            f"<input type=hidden name=repo value='{esc(repo)}'>"
            f"<input type=hidden name=machine value='{esc(machine)}'>"
            f"<button type=submit>Start on {esc(machine)}</button></form>"
        )
    for machine in entry.get("enable_hosts", []):
        controls.append(
            "<form method=post action=/repos/enable style='display:inline'>"
            f"<input type=hidden name=repo value='{esc(repo)}'>"
            f"<input type=hidden name=machine value='{esc(machine)}'>"
            f"<button type=submit>Enable on {esc(machine)}</button></form>"
        )
    return " ".join(controls)


def _render_repo_table(repos: list[dict], local_host: str) -> str:
    rows = [
        "<tr><th>repo</th><th>state</th><th>loops &middot; max</th>"
        "<th>machine</th><th>roadmap</th><th>actions</th></tr>"
    ]
    for r in repos:
        repo = r["repo"]
        pill = STATE_PILL[r["state"]]
        if not r.get("local", True):
            dot = "ok" if r["running"] else "idle"
            loop_status = (
                f"running on {r['running_machines']}" if r["running"] else "stopped"
            )
            actions = _remote_repo_controls(repo, r) or (
                "<span class=dim>Remote actions unavailable</span>"
            )
            rows.append(
                f"<tr><td><b>{esc(repo)}</b></td><td>{pill}</td>"
                f"<td><span class='dot {dot}'></span> {esc(loop_status)}</td>"
                f"<td>{esc(r['machine'])}</td><td class=dim>-</td>"
                f"<td>{actions}</td></tr>"
            )
            continue
        dot = "ok" if r["running"] else "idle"
        sess_label = "running" if r["running"] else "stopped"
        current_max = r["max"] if r["max"] is not None else 1
        stepper = _repo_slot_controls(repo, current_max)
        roadmap_link = f"<a href='/roadmap?repo={quote(repo, safe='')}'>Roadmap</a>"
        doc_action = (
            f"<a href='/repos?doc={quote(repo, safe='')}'>doc</a>"
            if r.get("has_doc")
            else "<span class=dim>no doc (optional)</span>"
        )
        schedule_link = f"<a href='/repos?schedule={quote(repo, safe='')}'>Schedule&hellip;</a>"
        if r["state"] == "enabled":
            place_choice = "spread" if _coordinator_only() else local_host
            run_form = (
                "<form method=post action=/schedule/run style='display:inline'>"
                f"<input type=hidden name=repo value='{esc(repo)}'>"
                "<input type=hidden name=cnt value=1>"
                f"<input type=hidden name=place value='{esc(place_choice)}'>"
                "<button type=submit>Run now</button></form>"
            )
            remove_link = f"<a href='/repos?remove={quote(repo, safe='')}'>Remove</a>"
            actions = f"{run_form} {schedule_link} &middot; {doc_action} &middot; {remove_link}"
        else:
            add_btn = (
                "<form method=post action=/repos/add style='display:inline'>"
                f"<input type=hidden name=repo value='{esc(repo)}'>"
                "<button type=submit>Add to schedule</button></form>"
            )
            actions = f"{add_btn} {schedule_link} &middot; {doc_action}"
        remote_controls = _remote_repo_controls(repo, r)
        if remote_controls:
            actions += f" &middot; {remote_controls}"
        rows.append(
            f"<tr><td><b>{esc(repo)}</b></td><td>{pill}</td>"
            f"<td><span class='dot {dot}'></span> {esc(sess_label)} {stepper}</td>"
            f"<td>{esc(r['machine'])}</td><td>{roadmap_link}</td><td>{actions}</td></tr>"
        )
    return "<div class=card><table>" + "".join(rows) + "</table></div>"


def render_repos(
    data: dict,
    *,
    add: str | None = None,
    doc_repo: str | None = None,
    doc_text: str | None = None,
    doc_edit: bool = False,
    schedule_repo: str | None = None,
    remove_repo: str | None = None,
    sent: str | None = None,
) -> bytes:
    """Show local repos and repos reported by other machines.

    Local rows support local actions. Remote rows support signed enable and
    start actions when the machine reports those capabilities.
    `data` comes from `gather_repos()`. Query flags open a panel.
    """
    repos = data["repos"]
    local_host = data["local_host"]
    body = [f'<header><h1>{icon(REPOS_ICON)}Repos</h1></header>']

    if sent:
        body.append(f"<div class=card><span class='pill on'>{esc(sent)}</span></div>")

    add_href = "/repos" if add else "/repos?add=existing"
    add_label = "Close" if add else "Add repo"
    body.append(f"<div class=row style='margin-bottom:.6rem'><span class=sp></span><a href='{add_href}'>{esc(add_label)}</a></div>")

    if add:
        body.append(_render_add_panel(add, repos))
    if doc_repo:
        body.append(_render_doc_panel(doc_repo, doc_text or "", doc_edit))
    if schedule_repo:
        body.append(_render_schedule_panel(schedule_repo))
    if remove_repo:
        body.append(_render_remove_panel(remove_repo))

    if not repos:
        body.append(
            "<div class=card><p class=dim>No repos yet. Add one to start scheduling loops.</p>"
            "<a href='/repos?add=existing'>Add your first repo</a></div>"
        )
    else:
        body.append(_render_repo_table(repos, local_host))

    fleet_error = data.get("fleet_error")
    if fleet_error:
        body.append(
            f"<p class=dim>Fleet registry unreachable: {esc(fleet_error)} "
            "(loop status and concurrency caps below are limited to this machine).</p>"
        )

    return page("Repos", "".join(body), active="repos")


TIMER_WINDOW_S = 4 * 3600  # the mockup's 4-hour timeline strip


def render_schedule(data: dict, *, sent: str | None = None) -> bytes:
    """The Schedule page (issue #22): the `delegation-loop.timer` status
    card, a table of every timer (recurring + one-off), and a "Run now"
    form that dispatches `loop.run` on demand.

    `data` is `gather_schedule()`'s output -- same read-then-render split
    the rest of this module uses.
    """
    now = data["now"]
    enabled = data["enabled"]
    timers_list = data["timers"]
    recurring = [t for t in timers_list if t["unit"] == "delegation-loop.timer"]
    fleet_machines = data.get("machines", [])
    local_host = data["local_host"]

    body = [f'<header><h1>{icon("M4 5h16v16H4zM4 10h16M8 3v4M16 3v4")}Schedule</h1></header>']

    if sent:
        body.append(f"<div class=card><span class='pill on'>{esc(sent)}</span></div>")

    # ---- recurring timer card ---------------------------------------------
    active = data["timer_active"]
    recurring_next = recurring[0]["next"] if recurring and recurring[0]["next"] else None
    status_text = (
        f"Running - next at {esc(time.strftime('%a %H:%M', time.localtime(recurring_next)))}"
        if active and recurring_next
        else "Running - no upcoming run scheduled" if active else "Stopped - no automatic runs"
    )
    toggle_action, toggle_label = ("stop", "Stop") if active else ("start", "Start")

    due = sorted(
        (t for t in timers_list if t["next"] is not None and t["next"] - now <= TIMER_WINDOW_S),
        key=lambda t: t["next"],
    )
    if due:
        marks = "".join(
            "<li>"
            f"{esc(timer_repository(t['unit']))} "
            f"<span class=dim>({'recurring' if t['unit'] == 'delegation-loop.timer' else 'one-off'})</span> "
            f"<span class=mono data-until='{t['next']:.0f}'></span> "
            f"<span class=dim>{esc(time.strftime('%H:%M', time.localtime(t['next'])))}</span></li>"
            for t in due
        )
        marks_html = f"<ul style='margin:0;padding-left:1.1rem'>{marks}</ul>"
    else:
        marks_html = "<p class=dim style='margin:0'>No runs in the next 4 hours.</p>"

    overall_next = next((t["next"] for t in timers_list if t["next"] is not None), None)
    next_block = (
        f"<div class=dim>Next</div><div class='big mono' data-until='{overall_next:.0f}'></div>"
        f"<div class=dim>{esc(time.strftime('%a %H:%M', time.localtime(overall_next)))}</div>"
        if overall_next
        else "<div class=dim>Next</div><div class=big>None scheduled</div>"
    )

    body.append(
        "<div class=card><div class=row style='justify-content:space-between;align-items:flex-start'>"
        "<div><div class=dim>Recurring timer</div><div class=big>delegation-loop.timer</div>"
        f"<div class=dim>{status_text}</div>"
        f"<form method=post action=/schedule/timer style='margin-top:.5rem'>"
        f"<input type=hidden name=action value={toggle_action}>"
        f"<button type=submit>{toggle_label}</button></form></div>"
        f"<div style='flex:1;min-width:220px'><div class=dim style='margin-bottom:.3rem'>Next 4 hours</div>{marks_html}</div>"
        f"<div style='text-align:right'>{next_block}</div>"
        "</div></div>"
    )

    # ---- timer table --------------------------------------------------
    body.append("<div class=card><table>")
    body.append("<tr><th>repository</th><th>loops</th><th>next</th><th>at</th><th>last run</th></tr>")
    if not timers_list:
        body.append("<tr><td colspan=5 class=dim>No timer found.</td></tr>")
    for t in timers_list:
        repo_label = timer_repository(t["unit"])
        kind = "recurring" if t["unit"] == "delegation-loop.timer" else "one-off"
        loops = _timer_loop_count(t["unit"], repo_label, enabled)
        next_cell = (
            f"<span class=mono data-until='{t['next']:.0f}'></span>" if t["next"] else "<span class=dim>-</span>"
        )
        at_cell = (
            esc(time.strftime("%a %H:%M", time.localtime(t["next"]))) if t["next"] else "<span class=dim>-</span>"
        )
        last_cell = (
            f"<span data-since='{t['last']:.0f}'></span>" if t["last"] else "<span class=dim>never</span>"
        )
        body.append(
            f"<tr><td>{esc(repo_label)} <span class=pill>{esc(kind)}</span></td>"
            f"<td>{esc(loops)}</td><td>{next_cell}</td><td>{at_cell}</td><td>{last_cell}</td></tr>"
        )
    body.append("</table></div>")

    # ---- run now --------------------------------------------------------
    repo_options = "".join(f"<option value='{esc(r)}'>{esc(r)}</option>" for r in sorted(enabled))
    cnt_options = "".join(f"<option value={n}>{n} loop{'s' if n != 1 else ''}</option>" for n in range(1, 5))
    place_options = ["<option value=spread>spread out</option>", "<option value=any>any machine</option>"]
    coordinator_only = _coordinator_only()
    by_name = {m["name"]: m for m in fleet_machines}
    if coordinator_only:
        by_name.pop(local_host, None)
    else:
        # A registered local machine keeps its reported state.
        by_name.setdefault(local_host, {"name": local_host, "state": "online"})
    for m in sorted(by_name.values(), key=lambda m: m["name"]):
        if m["state"] == "offline":
            continue
        disabled = " disabled" if m["state"] == "draining" else ""
        label = f"{m['name']} (draining)" if m["state"] == "draining" else m["name"]
        place_options.append(f"<option value='{esc(m['name'])}'{disabled}>{esc(label)}</option>")

    fleet_error = data.get("fleet_error")
    placement_note = (
        "No worker choices are available until the registry responds."
        if coordinator_only
        else "Placement is limited to this machine."
    )
    error_note = (
        f"<p class=dim>Fleet registry unreachable: {esc(fleet_error)} {placement_note}</p>"
        if fleet_error
        else ""
    )

    body.append(
        '<div class=card><div class=row style="margin-bottom:.4rem">'
        "<b>Run now</b><span class=dim>Starts loops on free machines. "
        "The note is kept with the request but is not passed into the loop.</span></div>"
        f"{error_note}"
        "<form method=post action=/schedule/run class=row>"
        f"<select name=repo><option value=all>all enabled repos</option>{repo_options}</select>"
        f"<select name=cnt>{cnt_options}</select>"
        f"<select name=place>{''.join(place_options)}</select>"
        "<input type=text name=note placeholder='note for the loop (optional)' style='flex:1;min-width:180px'>"
        "<button type=submit>Start run</button>"
        "</form></div>"
    )

    body.append(
        "<p class=dim>To schedule a one-off run, use "
        "<code>lupin once &lt;when&gt; [repo...] [--note TEXT] [--platform P]</code>.</p>"
    )

    return page("Schedule", "".join(body), active="schedule")


LOOP_STATUS_DOT = {"running": "ok", "working": "ok", "needs_attention": "warn", "starting": "warn", "blocked": "warn", "stopped": "idle", "done": "idle", "idle": "idle", "unknown": "idle"}
LOOP_STATUS_LABEL = {"running": "running", "working": "working", "needs_attention": "needs attention", "starting": "starting", "blocked": "blocked", "stopped": "stopped", "done": "done", "idle": "idle", "unknown": "unknown"}


def _loops_url(repo: str, group: str, lines: int) -> str:
    return f"/loops?repo={quote(repo, safe='')}&group={esc(group)}&lines={lines}"


def render_loops(
    data: dict, *, group: str, selected_repo: str, selected_tail: str | None, lines: int
) -> bytes:
    """The Loops page (issue #21): a sidebar grouped by repo or by machine,
    and a tail pane for whichever loop is selected.

    `data` is `gather_loops()`'s output, with `selected_tail` fetched
    separately by the caller (same split `gather()`/`render_dashboard()`
    already use: this function renders a already-read state, it doesn't go
    read anything itself).
    """
    entries = data["entries"]
    local_host = data["local_host"]
    fleet_machines = data.get("machines", [])
    selected = next((e for e in entries if e["repo"] == selected_repo), None)

    def row(entry: dict) -> str:
        sel = " sel" if entry["repo"] == selected_repo else ""
        dot = LOOP_STATUS_DOT.get(entry["status"], "idle")
        label = LOOP_STATUS_LABEL.get(entry["status"], entry["status"])
        return (
            f"<a class='loops-row{sel}' href='{_loops_url(entry['repo'], group, lines)}'>"
            f"<span class='dot {dot}'></span> {esc(entry['repo'])} "
            f"<span class=dim>{esc(label)}</span></a>"
        )

    side = [
        "<div class=row style='margin-bottom:.4rem'>",
        f"<a href='/loops?group=repo&repo={quote(selected_repo, safe='')}&lines={lines}'>by repo</a>",
        " &middot; ",
        f"<a href='/loops?group=machine&repo={quote(selected_repo, safe='')}&lines={lines}'>by machine</a>",
        "</div>",
    ]
    if not entries:
        side.append("<p class=dim>No loopable repos found.</p>")
    elif group == "machine":
        by_machine: dict[str, list[dict]] = {}
        for entry in entries:
            by_machine.setdefault(entry["machine"], []).append(entry)
        # Include machines named by loop state, even when they are not in
        # the registry.
        known = sorted({m["name"] for m in fleet_machines} | {local_host} | {e["machine"] for e in entries})
        for name in known:
            label = "this machine" if name == local_host else name
            side.append(f"<div class=loops-group>{esc(label)}</div>")
            rows = by_machine.get(name, [])
            if not rows:
                side.append("<p class=dim style='margin:.2rem 0 0 .8rem'>no loop visible from here</p>")
            side.extend(row(entry) for entry in rows)
    else:
        side.extend(row(entry) for entry in entries)

    main = ["<div class=card>"]
    if selected is None:
        main.append("<p class=dim>Select a loop.</p>")
    else:
        main.append(
            "<div class=row>"
            f"<span class=big>{esc(selected['repo'])}</span>"
            f"<span class=dim>on {esc(selected['machine'])}</span>"
            "</div>"
        )
        active = selected["status"] not in ("stopped", "unknown")
        main.append(f"<div class=dim>Herdr state: {esc(selected['agent_status'])}</div>")
        main.append(f"<div class=dim>backend: {esc(selected['backend'])}; platform: {esc(selected.get('platform') or '-')}</div>")
        for label, key in (("session", "session"), ("workspace", "workspace_id"), ("pane", "pane_id")):
            if selected.get(key):
                main.append(f"<div class=dim>{label}: {esc(selected[key])}</div>")
        if active:
            if selected["machine"] == local_host:
                main.append(f"<pre style='max-height:none'>{esc((selected_tail or '').rstrip() or '(no output)')}</pre>")
            else:
                main.append("<p class=dim>Use peek output to request the remote log.</p>")
            main.append(
                f"<a href='/peek?repo={quote(selected_repo, safe='')}&machine={quote(selected['machine'], safe='')}&lines={lines}'>peek output</a>"
            )
            presets = sorted({25, 400, 2000, lines})
            picker = " ".join(
                f"<a href='{_loops_url(selected_repo, group, n)}'>"
                f"{'<b>' if n == lines else ''}{n} lines{'</b>' if n == lines else ''}</a>"
                for n in presets
            )
            main.append(f"<div style='margin:.6rem 0'>{picker}</div>")
            if selected.get("session"):
                if selected["machine"] == local_host:
                    attach_argv = ["herdr", "--session", selected["session"]]
                else:
                    target = loops.ssh_target_for(selected["machine"])
                    attach_argv = (
                        ["herdr", "--remote", target, "--session", selected["session"]]
                        if target
                        else None
                    )
                if attach_argv:
                    main.append(f"<p>Attach: <code>{esc(shlex.join(attach_argv))}</code></p>")
                    if selected["machine"] != local_host:
                        # `herdr --remote` asks to restart the far server if the Herdr versions differ.
                        ssh_argv = ["ssh", "-t", target, "herdr", "--session", selected["session"]]
                        main.append(
                            f"<p>Or, with only ssh: <code>{esc(shlex.join(ssh_argv))}</code></p>"
                        )
                else:
                    main.append(f"<p class=dim>No SSH target is set for {esc(selected['machine'])}.</p>")
        elif selected["status"] == "stopped":
            main.append("<p class=dim>Not running.</p>")
        else:
            main.append("<p class=dim>State cannot be verified; actions are disabled.</p>")
        main.append("<div class=row style='margin-top:1rem'>")
        if active:
            main.append(
                "<form method=post action=/loops/state style='display:inline'>"
                f"<input type=hidden name=repo value='{esc(selected_repo)}'>"
                f"<input type=hidden name=machine value='{esc(selected['machine'])}'>"
                "<button type=submit>refresh state</button></form>"
                "<form method=post action=/loops/send style='display:inline'>"
                f"<input type=hidden name=repo value='{esc(selected_repo)}'>"
                f"<input type=hidden name=machine value='{esc(selected['machine'])}'>"
                "<input name=text maxlength=2000 size=40 required placeholder='type a line to send to the agent'> "
                "<button type=submit>send</button></form>"
                "<form method=post action=/loops/close style='display:inline'>"
                f"<input type=hidden name=repo value='{esc(selected_repo)}'>"
                f"<input type=hidden name=machine value='{esc(selected['machine'])}'>"
                "<select name=scope><option value=repo>this repo</option>"
                "<option value=all>every loop on this machine</option></select> "
                "<button type=submit>stop loop</button></form>"
            )
        if selected["status"] == "stopped":
            targets = _rank_candidates(fleet_machines, local_host)
            if targets:
                options = []
                for target in targets:
                    name = target["name"]
                    label = "this machine" if name == local_host else name
                    selected_attr = " selected" if name == selected["machine"] else ""
                    options.append(
                        f"<option value='{esc(name)}'{selected_attr}>{esc(label)}</option>"
                    )
                main.append(
                    "<form method=post action=/loops/start style='display:inline'>"
                    f"<input type=hidden name=repo value='{esc(selected_repo)}'>"
                    "<label>Start on <select name=machine>"
                    f"{''.join(options)}</select></label> "
                    "<button type=submit>start loop</button></form>"
                )
            else:
                main.append("<p class=dim>No online machine can start a loop.</p>")
        main.append("</div>")
    main.append("</div>")

    fleet_error = data.get("fleet_error")
    error_note = (
        f"<p class=dim>Fleet registry unreachable: {esc(fleet_error)}</p>" if fleet_error else ""
    )
    body = (
        f'<header><h1>{icon("M17 2l4 4-4 4M3 11V9a3 3 0 013-3h15M7 22l-4-4 4-4M21 13v2a3 3 0 01-3 3H3")}Loops</h1></header>'
        f"{error_note}"
        "<div class=loops-shell>"
        f"<div class='card loops-side'>{''.join(side)}</div>"
        f"<div class=loops-main>{''.join(main)}</div>"
        "</div>"
    )
    return page("Loops", body, active="loops")


def render_loop_fullscreen(entry: dict, tail: str | None, lines: int, group: str) -> bytes:
    """A bare tail view: no sidebar, no nav, no topbar -- the mockup's
    fullscreen mode. A plain link takes the reader back to the normal page;
    nothing here needs JavaScript.
    """
    exit_url = _loops_url(entry["repo"], group, lines)
    tail_text = esc((tail or "").rstrip() or "(no output)")
    presets = sorted({25, 400, 2000, lines})
    picker = " ".join(
        f"<a href='{_loops_url(entry['repo'], group, n)}&fullscreen=1' style='color:inherit'>"
        f"{'<b>' if n == lines else ''}{n} lines{'</b>' if n == lines else ''}</a>"
        for n in presets
    )
    return (
        "<!doctype html><html lang=en><head><meta charset=utf-8>"
        f"{THEME_BOOTSTRAP}"
        "<meta name=viewport content='width=device-width,initial-scale=1'>"
        f"<title>{esc(entry['repo'])} - fullscreen</title>"
        f"<style>{CSS}body{{padding:0}}.full{{padding:1rem}}"
        "pre{max-height:none;height:calc(100vh - 5rem)}</style></head>"
        f"<body><div class=full><header><h1>{esc(entry['repo'])}</h1>"
        f"<span class=dim>{esc(entry['machine'])}</span><span class=sp></span>"
        f"<span style='font-size:13px'>{picker}</span>"
        f"<a href='{exit_url}'>exit fullscreen</a></header>"
        f"<pre>{tail_text}</pre></div></body></html>"
    ).encode("utf-8")


def render_debrief_index(items: list[tuple[str, str]]) -> bytes:
    rows = "".join(
        f"<li><a href='/debrief?repo={quote(repo, safe='')}&amp;file={quote(name, safe='')}'>"
        f"{esc(repo)}</a> <span class=dim>{esc(_debrief_stamp(name))}</span></li>"
        for repo, name in items
    )
    listing = f"<ul>{rows}</ul>" if rows else "<p class=dim>No debriefs yet.</p>"
    return page(
        "Debriefs",
        f"<header><h1>Debriefs</h1></header><div class=card>{listing}</div>",
        debrief.CSS,
    )


def _debrief_stamp(name: str) -> str:
    """`20261010-120000.md` -> `2026-10-10 12:00:00 UTC`. Periodic files keep the suffix. Name is pre-checked."""
    stamp = name[:15]
    label = f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]} {stamp[9:11]}:{stamp[11:13]}:{stamp[13:15]} UTC"
    period = name[15:-3]
    return f"{label} {period}" if period else label


def render_error(msg: str) -> bytes:
    return page(
        "error",
        "<header><h1>Rejected</h1><span class=sp></span>"
        "<a href='/'>back</a></header>"
        f"<div class=card><p class=err>{esc(msg)}</p></div>",
        active="overview",
    )


# --------------------------------------------------------------------------
# server
# --------------------------------------------------------------------------


class Handler(BaseHTTPRequestHandler):
    server_version = "lupin"
    sys_version = ""
    peek_lines = 25
    allowed_hosts: set = set()
    # Named `fleet_connection`, not `connection` -- `socketserver`'s own
    # `BaseRequestHandler` already sets `self.connection` to the live
    # client socket, which would otherwise shadow this class attribute on
    # every real request (a bug caught by the real-HTTP tests, not the
    # mocked-Handler ones, since those never call setup()). Used by issue
    # #15's fleet data, issue #19's quest POST routes, and issue #20's
    # Machines page alike.
    fleet_connection: dict = {}
    cmd_signing_key: str | None = None
    cmd_signing_keys_dir: str | None = None

    def _signing_key_for(self, machine: str) -> str | None:
        return commands.signing_key_for(
            machine,
            default=self.cmd_signing_key,
            directory=self.cmd_signing_keys_dir,
        )

    def reply(self, body: bytes, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        # Images load from the same-origin attachment route.
        self.send_header("Cache-Control", "no-store")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; style-src 'unsafe-inline'; "
            "script-src 'unsafe-inline'; img-src 'self'; connect-src 'self'; "
            "base-uri 'none'",
        )
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def reply_json(self, obj) -> None:
        body = json.dumps(obj, indent=2).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def redirect(self, location: str) -> None:
        """303: the browser re-GETs `location` instead of re-submitting
        the form that landed here (standard post/redirect/get)."""
        self.send_response(303)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def host_ok(self) -> bool:
        """Block DNS rebinding: only the names we bound to are accepted."""
        host = (self.headers.get("Host") or "").strip().lower()
        return host in self.allowed_hosts

    def log_message(self, fmt, *args):
        sys.stderr.write("%s %s\n" % (time.strftime("%H:%M:%S"), fmt % args))

    def render_roadmap_page(self, query, repos, selected, state, roadmap_page) -> bytes:
        """Render a whole Roadmap page: closed, detail, or `full=1`."""
        if query.get("view") == "list" and state == "open":
            models = dict(
                zip(
                    repos,
                    _parallel(
                        repos,
                        lambda repo: roadmap.cached_model(
                            repo,
                            os.path.join(CODE_DIR, repo),
                            connection=self.fleet_connection,
                        ),
                    ),
                )
            )
            quest_state = self.quest_state(query.get("quest", "").strip()) if selected else None
            return roadmap.render_list_page(repos, models, roadmap_page, query, quest_state)
        if state == "closed":
            names = [selected] if selected else repos
            issues_by_repo = dict(
                zip(
                    names,
                    _parallel(
                        names,
                        lambda name: roadmap.cached_github(
                            name,
                            os.path.join(CODE_DIR, name),
                            "closed",
                            connection=self.fleet_connection,
                        ),
                    ),
                )
            )
            return roadmap.render_completed_page(
                selected, repos, issues_by_repo, roadmap_page
            )
        if query.get("view") == "detail" and selected:
            model = roadmap.cached_model(
                selected, os.path.join(CODE_DIR, selected), connection=self.fleet_connection
            )
            quest_state = self.quest_state(query.get("quest", "").strip())
            return roadmap.render_page(selected, repos, model, roadmap_page, quest_state)
        models = _roadmap_models(repos, self.fleet_connection)
        quest_state = self.quest_state(query.get("quest", "").strip()) if selected else None
        issue_count = sum(
            len(model.get("nodes", []))
            for repo, model in models.items()
            if not selected or repo == selected
        )
        if query.get("view") != "board" and issue_count > roadmap.BOARD_MAX_ISSUES:
            return roadmap.render_list_page(repos, models, roadmap_page, query, quest_state)
        return roadmap.render_combined_page(repos, models, roadmap_page, query, quest_state)

    def do_GET(self):  # noqa: N802
        if not self.host_ok():
            self.reply(render_error("bad Host header"), 421)
            return
        url = urlparse(self.path)
        query = {k: v[0] for k, v in parse_qs(url.query).items()}

        if url.path == "/favicon.ico":
            self.send_response(200)
            self.send_header("Content-Type", "image/svg+xml")
            self.send_header("Content-Length", str(len(FAVICON)))
            self.send_header("Cache-Control", "public, max-age=86400")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(FAVICON)
        elif url.path == "/":
            self.reply(render_dashboard(gather(self.peek_lines, self.fleet_connection)))
        elif url.path == "/roadmap":
            try:
                machine_records = machines.machines(self.fleet_connection)
                online = sum(record.get("state") == "online" for record in machine_records)
                fleet_html = (
                    "<a class='machine-count' href='/machines' style='display:flex;align-items:center;gap:6px'>"
                    f"<span class='machine-dot {'online' if online == len(machine_records) else ''}'></span>"
                    f"{online} of {len(machine_records)} machines</a>"
                )
            except machines.CoordinatorUnreachable:
                fleet_html = "<span class='machine-count'>Machine status unavailable</span>"
            recurring = next((row for row in timers() if row["unit"] == "delegation-loop.timer"), None)
            next_run = recurring.get("next") if recurring else None
            if next_run:
                timer_html = (
                    f"<span class=machine-timer>{icon('M12 7v5l3 2M12 3a9 9 0 100 18 9 9 0 000-18z', 14)}"
                    f"Next run <b data-until='{next_run:.0f}'></b></span>"
                )
            elif not timer_active():
                timer_html = "<span class=machine-timer>Timer paused</span>"
            else:
                timer_html = ""
            roadmap_topbar = (
                f"{fleet_html}{timer_html}<span class='updated-label'>Updated "
                f"{time.strftime('%H:%M:%S', time.localtime())}</span>"
            )

            def roadmap_page(title, body, extra_css="", extra_js=""):
                return page(
                    title,
                    body,
                    extra_css,
                    extra_js,
                    active="roadmap",
                    topbar_extra=roadmap_topbar,
                )

            repos = roadmap.repository_names(code_repos())
            selected = query.get("repo", "").strip()
            if selected and selected not in repos:
                self.reply(render_error("unknown repository"), 404)
                return
            state = "closed" if query.get("state") == "closed" else "open"
            if query.get("full") == "1" or state == "closed" or query.get("view") == "detail":
                self.reply(
                    self.render_roadmap_page(query, repos, selected, state, roadmap_page)
                )
                return
            # Board and list fill in after first paint, so the shell paints
            # at once. `full=1` renders the same page without the extra
            # request, for clients without JavaScript.
            suffix = f"?{url.query}" if url.query else ""
            full_query = f"{url.query}&full=1" if url.query else "full=1"
            shell = (
                "<div id='roadmap-fragment' "
                f"data-src='/roadmap/board{suffix}' data-full='/roadmap?{full_query}'>"
                "<p class='dim'>Loading the roadmap&hellip;</p></div>"
                "<noscript><p class='dim'>The Roadmap fills in with JavaScript. "
                f"<a href='/roadmap?{full_query}'>Open it whole</a> instead.</p></noscript>"
            )
            self.reply(
                roadmap_page(
                    "Roadmap",
                    shell,
                    roadmap.BOARD_CSS + roadmap.LIST_CSS,
                    _roadmap_loader_script(),
                )
            )
        elif url.path == "/roadmap/board":
            quest_state = (
                self.quest_state(query.get("quest", "").strip())
                if query.get("repo", "").strip()
                else None
            )
            fragment, status = roadmap_fragment(query, self.fleet_connection, quest_state)
            self.reply(fragment, status)
        elif url.path == "/usage":
            self.reply(render_usage(connection=self.fleet_connection))
        elif url.path == "/model-tiers":
            self.reply(render_model_tiers(sent=query.get("sent"), connection=self.fleet_connection))
        elif url.path == "/machines":
            try:
                records = machines.machines(self.fleet_connection)
                slot_status = slots_redis.status(**self.fleet_connection)
                timer_list = timers()
                recurring_timer = next(
                    (timer for timer in timer_list if timer["unit"] == "delegation-loop.timer"),
                    None,
                )
                timer_is_running = timer_active()
            except machines.CoordinatorUnreachable:
                self.reply(render_error("cannot reach the machine registry"), 502)
                return
            self.reply(
                render_machines(
                    records,
                    slot_status,
                    recurring_timer=recurring_timer,
                    timer_running=timer_is_running,
                )
            )
        elif url.path == "/api/state":
            self.reply_json(gather(self.peek_lines, self.fleet_connection))
        elif url.path == "/repos":
            self.do_repos(query)
        elif url.path == "/loops":
            self.do_loops(query)
        elif url.path == "/schedule":
            self.reply(render_schedule(gather_schedule(self.fleet_connection), sent=query.get("sent")))
        elif url.path == "/peek":
            self.do_peek(query)
        elif url.path == "/image":
            self.do_image(query)
        elif url.path == "/debrief":
            self.do_debrief(query)
        elif url.path == "/debrief/shot":
            self.do_debrief_shot(query)
        elif url.path == "/evidence":
            self.do_evidence(query)
        elif url.path == "/healthz":
            self.reply(b"ok")
        else:
            self.reply(render_error("no such page"), 404)

    def do_image(self, query: dict) -> None:
        attachment_id = query.get("id", "")
        if not ATTACHMENT_ID.fullmatch(attachment_id):
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        image = github_attachment(attachment_id)
        if image is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        data, content_type = image
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "private, max-age=300")
        self.send_header("Content-Security-Policy", "default-src 'none'; sandbox")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(data)

    def do_debrief(self, query: dict) -> None:
        root = Path(STATE_DIR)
        repo = (query.get("repo") or "").strip()
        name = (query.get("file") or "").strip()
        if not repo and not name:
            self.reply(render_debrief_index(debrief.list_debriefs(root)))
            return
        try:
            markdown = debrief.read_debrief(root, repo, name)
        except debrief.DebriefError:
            self.reply(render_error("no such debrief"), 404)
            return
        self.reply(
            page(
                f"Debrief {repo}",
                "<header><h1>Debrief</h1><span class=sp></span>"
                "<a href='/debrief'>all debriefs</a></header>"
                f"<div class=card><div class=debrief>{debrief.render_html(markdown, repo)}</div></div>",
                debrief.CSS,
            )
        )

    def do_debrief_shot(self, query: dict) -> None:
        found = debrief.read_screenshot(
            Path(STATE_DIR), (query.get("repo") or "").strip(), query.get("path") or "",
        )
        if found is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        data, content_type = found
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "private, max-age=300")
        self.send_header("Content-Security-Policy", "default-src 'none'; sandbox")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(data)

    def do_evidence(self, query: dict) -> None:
        repo = (query.get("repo") or "").strip()
        found = None
        if _valid_repo_name(repo) and repo in enabled_repos():
            found = debrief.read_evidence(Path(CODE_DIR) / repo, query.get("path") or "")
        if found is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        data, content_type = found
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "private, max-age=300")
        self.send_header("Content-Security-Policy", "default-src 'none'; sandbox")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(data)

    def _repo_exists(self, repo: str) -> bool:
        """A valid repo name with a directory on this machine."""
        return (
            bool(repo)
            and _valid_repo_name(repo)
            and os.path.isdir(os.path.join(CODE_DIR, repo))
        )

    def _repo_has_doc(self, repo: str) -> bool:
        return self._repo_exists(repo) and os.path.isfile(
            os.path.join(CODE_DIR, repo, LOOP_DOC)
        )

    def do_repos(self, query: dict) -> None:
        doc_repo = (query.get("doc") or "").strip()
        doc_text = None
        if doc_repo:
            if not self._repo_has_doc(doc_repo):
                self.reply(render_error("unknown repository or missing delegation doc"), 404)
                return
            try:
                with open(os.path.join(CODE_DIR, doc_repo, LOOP_DOC), encoding="utf-8") as fh:
                    doc_text = fh.read()
            except OSError as exc:
                self.reply(render_error(f"could not read doc: {exc}"), 500)
                return

        data = gather_repos(self.fleet_connection)
        self.reply(
            render_repos(
                data,
                add=query.get("add"),
                doc_repo=doc_repo or None,
                doc_text=doc_text,
                doc_edit=query.get("edit") == "1",
                schedule_repo=(query.get("schedule") or "").strip() or None,
                remove_repo=(query.get("remove") or "").strip() or None,
                sent=query.get("sent"),
            )
        )

    def do_repos_enable(self, form: dict) -> None:
        repo = form.get("repo", [""])[0].strip()
        machine = form.get("machine", [""])[0].strip()
        if not _valid_repo_name(repo) or not machine or machine == machines.hostname():
            self.reply(render_error("bad remote repo enable request"), 400)
            return
        try:
            records = machines.machines(self.fleet_connection)
        except CoordinatorUnreachable:
            self.reply(render_error("cannot reach the redis coordinator"), 502)
            return
        record = next((row for row in records if row.get("name") == machine), None)
        known_repo = next(
            (
                item for item in (record or {}).get("repos", [])
                if isinstance(item, dict) and item.get("repo") == repo
            ),
            None,
        )
        if (
            not record
            or record.get("state") != "online"
            or not known_repo
            or known_repo.get("enabled") is True
            or known_repo.get("loopable") is not True
            or "repo.enable" not in record.get("actions", [])
        ):
            self.reply(render_error("remote repository cannot be enabled on that machine"), 400)
            return
        signing_key = self._signing_key_for(machine)
        if not signing_key:
            self.reply(render_error(f"no signing key is configured for {machine!r}"), 400)
            return
        try:
            result = loops.dispatch_loop_action(
                machine=machine,
                local_host=machines.hostname(),
                local_argv=["lupin", "enable", repo],
                queue_action="repo.enable",
                queue_params={"repo": repo},
                connection=self.fleet_connection,
                signing_key=signing_key,
                actor="lupin-dashboard",
                issuer=machines.hostname(),
            )
        except CoordinatorUnreachable:
            self.reply(render_error("cannot reach the redis coordinator"), 502)
            return
        if result.get("mode") == "local" or result.get("state") == "failed":
            self.reply(render_error(f"enable failed: {result.get('output', 'remote command failed')}"), 502)
            return
        self.redirect(f"/repos?sent={quote(f'enable request sent for {repo} to {machine}', safe='')}")

    def do_repos_add(self, form: dict) -> None:
        repo = form.get("repo", [""])[0].strip()
        if not self._repo_exists(repo):
            self.reply(render_error("unknown repository"), 400)
            return
        enabled = enabled_repos()
        if repo not in enabled:
            write_enabled_repos(sorted(set(enabled) | {repo}))
        self.redirect(f"/repos?sent={quote(f'{repo} added to the schedule', safe='')}")

    def do_repos_generate_docs(self, form: dict) -> None:
        repo = form.get("repo", [""])[0].strip()
        if not repo or not _valid_repo_name(repo) or not os.path.isdir(os.path.join(CODE_DIR, repo)):
            self.reply(render_error("unknown repository"), 400)
            return
        doc_path = os.path.join(CODE_DIR, repo, LOOP_DOC)
        if os.path.exists(doc_path):
            self.reply(render_error(f"{repo} already has {LOOP_DOC}"), 400)
            return
        try:
            os.makedirs(os.path.dirname(doc_path), exist_ok=True)
            with open(doc_path, "w", encoding="utf-8") as fh:
                fh.write(_delegation_doc_template(repo))
        except OSError as exc:
            self.reply(render_error(f"could not write doc: {exc}"), 500)
            return
        write_enabled_repos(sorted(set(enabled_repos()) | {repo}))
        self.redirect(f"/repos?sent={quote(f'generated docs and added {repo}', safe='')}")

    def do_repos_remove(self, form: dict) -> None:
        repo = form.get("repo", [""])[0].strip()
        confirm = form.get("confirm", [""])[0].strip()
        enabled = enabled_repos()
        if not repo or repo not in enabled:
            self.reply(render_error("unknown or not-scheduled repository"), 400)
            return
        if confirm != repo:
            self.reply(render_error("type the repo name to confirm removal"), 400)
            return
        try:
            loop_runtime.disable_repo(repo)
        except loop_runtime.LoopError as exc:
            self.reply(render_error(f"remove failed: {exc}"), 500)
            return
        self.redirect(f"/repos?sent={quote(f'{repo} removed from the schedule', safe='')}")

    def do_repos_doc_save(self, form: dict) -> None:
        repo = form.get("repo", [""])[0].strip()
        text = form.get("text", [""])[0]
        if not self._repo_has_doc(repo):
            self.reply(render_error("unknown repository or missing delegation doc"), 400)
            return
        if len(text) > 200_000:
            self.reply(render_error("doc is too long"), 400)
            return
        try:
            with open(os.path.join(CODE_DIR, repo, LOOP_DOC), "w", encoding="utf-8") as fh:
                fh.write(text)
        except OSError as exc:
            self.reply(render_error(f"could not save doc: {exc}"), 500)
            return
        self.redirect(f"/repos?doc={quote(repo, safe='')}&sent=saved")

    def do_repos_slot_max(self, form: dict) -> None:
        repo = form.get("repo", [""])[0].strip()
        raw_max = form.get("max", [""])[0].strip()
        try:
            max_value = int(raw_max)
        except ValueError:
            max_value = None
        if not self._repo_exists(repo) or max_value is None or max_value < 1:
            self.reply(render_error("bad slot-max request"), 400)
            return
        slot = _repo_slot_name(repo)
        try:
            slots_redis.set_max(slot, max_value, **self.fleet_connection)
        except (slots_redis.CoordinatorAuthFailed, redis.exceptions.NoPermissionError) as exc:
            self.reply(render_error(slots_redis.refusal_message(exc, f"slot {slot!r}", fleet=True)), 502)
            return
        except CoordinatorUnreachable:
            self.reply(render_error("cannot reach the machine registry"), 502)
            return
        self.redirect("/repos")

    def do_repos_schedule(self, form: dict) -> None:
        """Schedule one local one-off run through Lupin."""
        repo = form.get("repo", [""])[0].strip()
        when = form.get("when", [""])[0].strip()
        if not self._repo_exists(repo):
            self.reply(render_error("unknown repository"), 400)
            return
        if not when or len(when) > 200:
            self.reply(render_error("missing or too-long schedule time"), 400)
            return
        rc, out = run(["lupin", "once", when, repo], timeout=20.0)
        if rc != 0:
            self.reply(render_error(f"lupin once failed: {out.strip()}"), 502)
            return
        sent = f"scheduled a one-off run for {repo} at {when}"
        self.redirect(f"/repos?sent={quote(sent, safe='')}")

    def do_model_tiers_refresh(self, form: dict) -> None:
        """Pull models now and publish the snapshot for the fleet dashboard."""
        try:
            snapshot = model_fetch.snapshot()
            model_fetch.save_snapshot(snapshot)
            shared = model_fetch.publish_snapshot(snapshot, **self.fleet_connection)
            sent = "pulled today's model list and prices"
            if not shared:
                sent += "; fleet cache unavailable, saved on this machine only"
        except Exception as exc:  # keep the dashboard usable if a pull or write fails
            sent = f"pull failed: {exc}"
        self.redirect(f"/model-tiers?sent={quote(sent, safe='')}")

    def do_model_tiers_refresh_benchmarks(self, form: dict) -> None:
        """"Pull benchmarks": force a fresh `benchmark_fetch` dispatch,
        right now. `force=True` because a human clicking this button has
        already decided they want a fresh pull -- it still goes through
        `refresh_snapshot`'s single-fetcher lock (`benchmark_fetch.py`'s
        docstring), so if another machine is mid-fetch this click just
        reports whatever is cached instead of starting a second, paying
        dispatch. `refresh_snapshot` turns most failures (timed out, bad
        output, Redis unreachable) into a `live: False` result. A refused
        login or an ACL denial raises instead, and the first except below
        names it. The broad except is a last-resort guard.
        """
        try:
            data = benchmark_fetch.refresh_snapshot(force=True, **self.fleet_connection)
            if data.get("live"):
                sent = f"pulled today's benchmark scores ({len(data.get('scores', []))} models)"
            else:
                sent = f"benchmark pull did not complete: {data.get('stale_reason', 'unknown reason')}"
        except (slots_redis.CoordinatorAuthFailed, redis.exceptions.NoPermissionError) as exc:
            sent = slots_redis.refusal_message(exc, "the benchmark refresh", fleet=True)
        except Exception as exc:  # best-effort by design, see docstring above
            sent = f"pull failed: {exc}"
        self.redirect(f"/model-tiers?sent={quote(sent, safe='')}")

    def do_loops(self, query: dict) -> None:
        group = "machine" if query.get("group") == "machine" else "repo"
        lines_raw = (query.get("lines") or "60").strip() or "60"
        if not lines_raw.isdigit() or not (1 <= int(lines_raw) <= 5000):
            self.reply(render_error("lines must be a number from 1 to 5000"), 400)
            return
        lines = int(lines_raw)

        data = gather_loops(self.fleet_connection)
        entries = data["entries"]
        selected_repo = (query.get("repo") or "").strip()
        selected = next((e for e in entries if e["repo"] == selected_repo), None)
        if selected is None and entries and not selected_repo:
            selected = entries[0]
            selected_repo = selected["repo"]

        tail = None
        if (
            selected is not None
            and selected["machine"] == data["local_host"]
            and selected["status"] not in ("stopped", "unknown")
        ):
            try:
                tail = loop_tail(selected["repo"], selected["machine"], lines, self.fleet_connection, self.cmd_signing_key)
            except CoordinatorUnreachable:
                self.reply(render_error("cannot reach the redis coordinator"), 502)
                return

        if query.get("fullscreen") == "1":
            if selected is None:
                self.reply(render_error("no such loop"), 404)
                return
            self.reply(render_loop_fullscreen(selected, tail, lines, group))
            return

        self.reply(render_loops(data, group=group, selected_repo=selected_repo, selected_tail=tail, lines=lines))

    def _loop_targets(self, machine: str, scope: str, repo: str) -> list[str]:
        """Return active Herdr loops on a machine for an all-stop request."""
        if scope != "all":
            return [repo]
        entries = gather_loops(self.fleet_connection)["entries"]
        targets = [
            e["repo"] for e in entries if e["machine"] == machine and e["status"] not in ("stopped", "unknown")
        ]
        return targets or [repo]

    def do_loops_close(self, form: dict) -> None:
        repo = form.get("repo", [""])[0].strip()
        machine = form.get("machine", [""])[0].strip()
        scope = form.get("scope", ["repo"])[0].strip()
        if not repo or not _valid_repo_name(repo) or not machine:
            self.reply(render_error("bad close request"), 400)
            return
        targets = self._loop_targets(machine, scope, repo)
        if any(not _valid_repo_name(t) for t in targets):
            self.reply(render_error("bad repo name"), 400)
            return
        local_host = machines.hostname()
        signing_key = self._signing_key_for(machine)
        if machine != local_host and not signing_key:
            self.reply(
                render_error(
                    f"no signing key is configured for {machine!r}"
                ),
                400,
            )
            return
        errors = []
        for target in targets:
            try:
                result = loops.dispatch_loop_action(
                    machine=machine, local_host=local_host,
                    local_argv=["lupin", "loop", "local-action", "stop", target, "--force"],
                    queue_action="loop.stop", queue_params={"repo": target, "force": True},
                    connection=self.fleet_connection, signing_key=signing_key,
                    actor="lupin-dashboard", issuer=local_host,
                    run_local=lambda argv: run(argv, timeout=20.0),
                )
            except CoordinatorUnreachable:
                self.reply(render_error("cannot reach the redis coordinator"), 502)
                return
            if result["mode"] == "local" and result["returncode"] != 0:
                errors.append(f"{target}: {result['output'].strip()}")
        if errors:
            self.reply(render_error("stop failed:\n" + "\n".join(errors)), 502)
            return
        self.redirect(f"/loops?repo={quote(repo, safe='')}")

    def do_loops_send(self, form: dict) -> None:
        repo = form.get("repo", [""])[0].strip()
        machine = form.get("machine", [""])[0].strip()
        text = form.get("text", [""])[0].strip()
        if not repo or not _valid_repo_name(repo) or not machine or not text:
            self.reply(render_error("bad send request"), 400)
            return
        local_host = machines.hostname()
        signing_key = self._signing_key_for(machine)
        if machine != local_host and not signing_key:
            self.reply(render_error(f"no signing key is configured for {machine!r}"), 400)
            return
        try:
            result = loops.dispatch_loop_action(
                machine=machine, local_host=local_host,
                local_argv=["lupin", "loop", "local-action", "send", repo, text],
                queue_action="loop.send", queue_params={"repo": repo, "text": text},
                connection=self.fleet_connection, signing_key=signing_key,
                actor="lupin-dashboard", issuer=local_host,
                run_local=lambda argv: run(argv, timeout=20.0),
            )
        except CoordinatorUnreachable:
            self.reply(render_error("cannot reach the redis coordinator"), 502)
            return
        if result["mode"] == "local" and result["returncode"] != 0:
            self.reply(render_error("send failed:\n" + result["output"].strip()), 502)
            return
        self.redirect(f"/loops?repo={quote(repo, safe='')}")

    def do_loops_start(self, form: dict) -> None:
        repo = form.get("repo", [""])[0].strip()
        machine = form.get("machine", [""])[0].strip()
        if not repo or not _valid_repo_name(repo) or not machine:
            self.reply(render_error("bad start request"), 400)
            return
        local_host = machines.hostname()
        if _coordinator_only() and machine == local_host:
            self.reply(render_error("the coordinator cannot run loops"), 400)
            return
        signing_key = self._signing_key_for(machine)
        if machine != local_host and not signing_key:
            self.reply(render_error(f"no signing key is configured for {machine!r}"), 400)
            return
        local_argv = ["lupin", "run", repo]
        queue_params = {"repo": repo}
        try:
            result = loops.dispatch_loop_action(
                machine=machine, local_host=local_host,
                local_argv=local_argv,
                queue_action="loop.run", queue_params=queue_params,
                connection=self.fleet_connection, signing_key=signing_key,
                actor="lupin-dashboard", issuer=local_host,
                run_local=lambda argv: run(argv, timeout=20.0),
            )
        except CoordinatorUnreachable:
            self.reply(render_error("cannot reach the redis coordinator"), 502)
            return
        if result["mode"] == "local" and result["returncode"] != 0:
            self.reply(render_error(f"run failed: {result['output'].strip()}"), 502)
            return
        self.redirect(f"/loops?repo={quote(repo, safe='')}")

    def do_schedule_timer(self, form: dict) -> None:
        """Start or stop `delegation-loop.timer` -- always on this machine.
        Issue #22's scope call: the timer card controls the timer on
        whatever host `lupin serve` itself runs on, same as `timers()`/
        `timer_active()` already read it; there is no cross-machine timer
        control to wire up here.
        """
        action = (form.get("action") or [None])[0]
        if action not in ("start", "stop"):
            self.reply(render_error("bad timer action"), 400)
            return
        rc, out = run(["systemctl", action, "delegation-loop.timer"], timeout=10.0)
        if rc != 0:
            self.reply(render_error(f"systemctl {action} failed: {out.strip()}"), 502)
            return
        self.redirect("/schedule")

    def do_schedule_run(self, form: dict) -> None:
        """"Run now": start one `loop.run` per target repo, on a machine
        chosen by `place` -- "spread" (round-robin across ranked online
        machines), "any" (the single best-ranked one), or a user-pinned
        machine name. See `_rank_candidates`'s docstring for why this does
        not call `place.py`'s `place()`: there is no task here to classify
        or quota-routed provider to pick a machine for, just free loop
        capacity.
        """
        enabled = enabled_repos()
        repo_choice = (form.get("repo") or [None])[0]
        if not repo_choice:
            self.reply(render_error("missing repo"), 400)
            return
        if repo_choice == "all":
            if not enabled:
                self.reply(render_error("no enabled repos to run"), 400)
                return
            cnt_raw = (form.get("cnt") or ["1"])[0].strip()
            try:
                cnt = int(cnt_raw)
            except ValueError:
                cnt = None
            if cnt is None or not (1 <= cnt <= 4):
                self.reply(render_error("loop count must be 1-4"), 400)
                return
            targets_repos = sorted(enabled)[:cnt]
        else:
            # The count applies when "all enabled repos" is selected.
            if not _valid_repo_name(repo_choice) or repo_choice not in enabled:
                self.reply(render_error("unknown or non-enabled repository"), 400)
                return
            targets_repos = [repo_choice]

        place_choice = (form.get("place") or [None])[0]
        if not place_choice:
            self.reply(render_error("missing placement"), 400)
            return

        note = (form.get("note") or [""])[0].strip()
        if len(note) > 500:
            self.reply(render_error("note is too long"), 400)
            return

        local_host = machines.hostname()
        try:
            records = machines.machines(self.fleet_connection)
        except CoordinatorUnreachable:
            records = []

        if place_choice in ("spread", "any"):
            ranked = _rank_candidates(records, local_host)
            if not ranked:
                self.reply(render_error("no machine is available to place a run on"), 400)
                return
            if place_choice == "spread":
                assigned = [ranked[i % len(ranked)]["name"] for i in range(len(targets_repos))]
            else:
                assigned = [ranked[0]["name"]] * len(targets_repos)
        elif _machine_available(place_choice, records, local_host):
            assigned = [place_choice] * len(targets_repos)
        else:
            self.reply(render_error("bad placement"), 400)
            return

        missing_key = next(
            (machine for machine in assigned if machine != local_host and not self._signing_key_for(machine)),
            None,
        )
        if missing_key:
            self.reply(render_error(f"no signing key is configured for {missing_key!r}"), 400)
            return

        errors = []
        for repo, machine in zip(targets_repos, assigned):
            local_argv = ["lupin", "run", repo]
            queue_params = {"repo": repo}
            try:
                result = loops.dispatch_loop_action(
                    machine=machine, local_host=local_host,
                    local_argv=local_argv, queue_action="loop.run",
                    queue_params=queue_params,
                    connection=self.fleet_connection, signing_key=self._signing_key_for(machine),
                    actor="lupin-dashboard", issuer=local_host,
                    run_local=lambda argv: run(argv, timeout=20.0),
                )
            except CoordinatorUnreachable:
                errors.append(f"{repo}@{machine}: cannot reach the redis coordinator")
                continue
            if result["mode"] == "local" and result["returncode"] != 0:
                errors.append(f"{repo}@{machine}: {result['output'].strip()}")
        if errors:
            self.reply(render_error("run now failed:\n" + "\n".join(errors)), 502)
            return

        summary = ", ".join(f"{r}@{m}" for r, m in zip(targets_repos, assigned))
        sent = f"Started {len(targets_repos)} loop(s): {summary}"
        self.redirect(f"/schedule?sent={quote(sent, safe='')}")

    def do_peek(self, query: dict) -> None:
        repo = query.get("repo", "").strip()
        machine = query.get("machine", "").strip()
        if not repo or not _valid_repo_name(repo):
            self.reply(render_error("bad repo name"), 400)
            return
        lines = query.get("lines", "60").strip() or "60"
        if not lines.isdigit() or not (1 <= int(lines) <= 5000):
            self.reply(render_error("lines must be a number from 1 to 5000"), 400)
            return
        if not machine:
            entry = next((row for row in gather_loops(self.fleet_connection)["entries"] if row["repo"] == repo), None)
            if entry is None:
                self.reply(render_error("unknown loop"), 404)
                return
            machine = entry["machine"]
        signing_key = self._signing_key_for(machine)
        if machine != machines.hostname() and not signing_key:
            self.reply(render_error(f"no signing key is configured for {machine!r}"), 400)
            return
        try:
            out = loop_tail(repo, machine, int(lines), self.fleet_connection, signing_key)
        except CoordinatorUnreachable:
            self.reply(render_error("cannot reach the redis coordinator"), 502)
            return
        except loops.MissingSigningKey:
            self.reply(render_error("a signing key is required for a remote action"), 400)
            return
        body = (
            f"<header><h1>{esc(repo)}</h1><span class=sp></span>"
            "<a href='/loops'>back to loops</a></header>"
            f"<pre style='max-height:none'>{esc(out.rstrip() or '(request sent; no output returned)')}</pre>"
        )
        self.reply(page(f"peek {repo}", body, active="loops"))


    def do_loops_state(self, form: dict) -> None:
        repo = form.get("repo", [""])[0].strip()
        machine = form.get("machine", [""])[0].strip()
        if not repo or not _valid_repo_name(repo) or not machine:
            self.reply(render_error("bad state request"), 400)
            return
        local_host = machines.hostname()
        signing_key = self._signing_key_for(machine)
        if machine != local_host and not signing_key:
            self.reply(render_error(f"no signing key is configured for {machine!r}"), 400)
            return
        try:
            result = loops.dispatch_loop_action(
                machine=machine,
                local_host=local_host,
                local_argv=["lupin", "loop", "local-action", "state", repo],
                queue_action="loop.state",
                queue_params={"repo": repo},
                connection=self.fleet_connection,
                signing_key=signing_key,
                actor="lupin-dashboard",
                issuer=local_host,
                run_local=lambda argv: run(argv, timeout=20.0),
            )
        except CoordinatorUnreachable:
            self.reply(render_error("cannot reach the redis coordinator"), 502)
            return
        output = result.get("output") if result["mode"] == "local" else "state request sent"
        if result["mode"] == "local" and result["returncode"] != 0:
            self.reply(render_error(f"state request failed: {output.strip()}"), 502)
            return
        self.reply(page(f"state {repo}", f"<div class=card><pre>{esc(output)}</pre></div>", active="loops"))

    def quest_state(self, quest_id: str) -> dict | None:
        """Read `quest:<quest_id>` and work out which of its issues are
        still claimed (in progress) versus released (done, closed, or
        merged). Returns `None` if there's no id, no such quest, or Redis
        can't be reached -- the roadmap page just skips the progress card
        in that case rather than failing the whole (read-only) page.
        """
        if not quest_id:
            return None
        try:
            record = quest.read_quest(quest_id, self.fleet_connection)
        except CoordinatorUnreachable:
            return None
        if record is None:
            return None
        targets = record.get("targets", [])
        owner_repos = sorted({target.rpartition("#")[0] for target in targets})
        try:
            held = claims.claims_for(owner_repos, **self.fleet_connection) if owner_repos else {}
        except CoordinatorUnreachable:
            held = {}
        holder = f"quest:{quest_id}"
        pending, done = [], []
        for number, target in zip(record.get("issues", []), targets):
            if held.get(target, {}).get("session") == holder:
                pending.append(number)
            else:
                done.append(number)
        return {
            "id": quest_id,
            "machine": record.get("machine"),
            "state": record.get("state"),
            "pending": pending,
            "done": done,
            "total": len(record.get("issues", [])),
        }

    def do_POST(self):  # noqa: N802
        if not self.host_ok():
            self.reply(render_error("bad Host header"), 421)
            return
        url = urlparse(self.path)
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            self.reply(render_error("bad Content-Length header"), 400)
            return
        length = max(0, min(length, MAX_FORM_BYTES))
        raw = self.rfile.read(length) if length else b""
        try:
            form = parse_qs(raw.decode("utf-8"))
        except UnicodeDecodeError:
            self.reply(render_error("request body must be utf-8"), 400)
            return

        try:
            if url.path == "/quest/start":
                self.do_quest_start(form)
            elif url.path == "/quest/stop":
                self.do_quest_stop(form)
            elif url.path == "/machines/slot-max":
                self.do_set_slot_max(form)
            elif url.path == "/loops/close":
                self.do_loops_close(form)
            elif url.path == "/loops/send":
                self.do_loops_send(form)
            elif url.path == "/loops/state":
                self.do_loops_state(form)
            elif url.path == "/loops/start":
                self.do_loops_start(form)
            elif url.path == "/schedule/timer":
                self.do_schedule_timer(form)
            elif url.path == "/schedule/run":
                self.do_schedule_run(form)
            elif url.path == "/repos/enable":
                self.do_repos_enable(form)
            elif url.path == "/repos/add":
                self.do_repos_add(form)
            elif url.path == "/repos/generate-docs":
                self.do_repos_generate_docs(form)
            elif url.path == "/repos/remove":
                self.do_repos_remove(form)
            elif url.path == "/repos/doc/save":
                self.do_repos_doc_save(form)
            elif url.path == "/repos/slot-max":
                self.do_repos_slot_max(form)
            elif url.path == "/repos/schedule":
                self.do_repos_schedule(form)
            elif url.path == "/model-tiers/refresh":
                self.do_model_tiers_refresh(form)
            elif url.path == "/model-tiers/refresh-benchmarks":
                self.do_model_tiers_refresh_benchmarks(form)
            else:
                self.reply(render_error("no such page"), 404)
        except (slots_redis.CoordinatorAuthFailed, redis.exceptions.NoPermissionError) as exc:
            self.reply(render_error(slots_redis.refusal_message(exc, f"POST {url.path}", fleet=True)), 502)

    def do_quest_start(self, form: dict) -> None:
        repo = form.get("repo", [""])[0].strip()
        try:
            issue_numbers = [int(value) for value in form.get("issue", [])]
        except ValueError:
            self.reply(render_error("bad issue number"), 400)
            return
        if not issue_numbers:
            self.reply(render_error("select at least one issue to start a quest"), 400)
            return
        try:
            result = quest.start(issue_numbers, enabled_repos(), connection=self.fleet_connection)
        except quest.QuestError as exc:
            self.reply(render_error(f"quest start failed: {exc}"), 400)
            return
        except CoordinatorUnreachable as exc:
            self.reply(render_error(f"cannot reach the redis coordinator: {exc}"), 502)
            return
        query = f"quest={quote(result['id'], safe='')}"
        if repo:
            query = f"repo={quote(repo, safe='')}&{query}"
        self.redirect(f"/roadmap?{query}")

    def do_quest_stop(self, form: dict) -> None:
        quest_id = form.get("id", [""])[0].strip()
        repo = form.get("repo", [""])[0].strip()
        if not quest_id:
            self.reply(render_error("missing quest id"), 400)
            return
        try:
            quest.stop(quest_id, connection=self.fleet_connection)
        except quest.QuestError as exc:
            self.reply(render_error(f"quest stop failed: {exc}"), 400)
            return
        except CoordinatorUnreachable as exc:
            self.reply(render_error(f"cannot reach the redis coordinator: {exc}"), 502)
            return
        self.redirect(f"/roadmap?repo={quote(repo, safe='')}" if repo else "/roadmap")

    def do_set_slot_max(self, form: dict) -> None:
        slot = form.get("slot", [""])[0].strip()
        raw_max = form.get("max", [""])[0].strip()
        try:
            # int(), not raw_max.isdigit(): isdigit() also accepts Unicode
            # digits like superscript two ('²') that int() then
            # can't parse, which used to crash this handler.
            max_value = int(raw_max)
        except ValueError:
            max_value = None
        if not slot or max_value is None or max_value < 1:
            self.reply(render_error("bad slot-max request"), 400)
            return
        try:
            slots_redis.set_max(slot, max_value, **self.fleet_connection)
        except (slots_redis.CoordinatorAuthFailed, redis.exceptions.NoPermissionError) as exc:
            self.reply(render_error(slots_redis.refusal_message(exc, f"slot {slot!r}", fleet=True)), 502)
            return
        except machines.CoordinatorUnreachable:
            self.reply(render_error("cannot reach the machine registry"), 502)
            return
        self.send_response(303)
        self.send_header("Location", "/machines")
        self.send_header("Content-Length", "0")
        self.end_headers()


def _roadmap_rows(model: dict) -> list[dict]:
    buckets = {
        number: stage["name"]
        for stage in model["stages"]
        for number in stage["numbers"]
    }
    rows = []
    for node in model["nodes"]:
        number = node["number"]
        incoming = [
            edge for edge in model["edges"]
            if edge["to"] == number and edge["kind"] in {"depends", "parent", "split"}
        ]
        rows.append(
            {
                "number": number,
                "priority": node["priority"],
                "size": node["size"],
                "bucket": buckets.get(number, ""),
                "comments": len(node["comments"]),
                "deps": sorted(
                    {edge["from"] for edge in incoming if edge["kind"] == "depends"}
                ),
                "parents": sorted(
                    {edge["from"] for edge in incoming if edge["kind"] in {"parent", "split"}}
                ),
                "title": node["title"],
                "body": node["body"],
                "commentText": [comment["body"] for comment in node["comments"]],
            }
        )
    return rows


def _print_roadmap(repo: str, model: dict, verbose: bool, as_json: bool) -> None:
    rows = _roadmap_rows(model)
    if as_json:
        if not verbose:
            for row in rows:
                del row["title"]
                del row["body"]
                del row["commentText"]
        print(json.dumps({"repo": repo, "issues": rows}, ensure_ascii=False, indent=2))
        return
    for row in rows:
        refs = []
        if row["deps"]:
            refs.append("deps:" + ",".join(f"#{number}" for number in row["deps"]))
        if row["parents"]:
            refs.append("parent:" + ",".join(f"#{number}" for number in row["parents"]))
        suffix = " " + " ".join(refs) if refs else ""
        print(
            f"#{row['number']} {row['bucket']} {row['priority']} {row['size']} "
            f"{row['comments']} comments{suffix}"
        )
        if verbose:
            node = next(node for node in model["nodes"] if node["number"] == row["number"])
            print(f"  {row['title']}")
            print(f"  {row['body']}")
            for index, comment in enumerate(node["comments"], 1):
                print(f"  Comment {index}: {comment['body']}")


def main(argv: list[str] | None = None) -> int:
    """Serve the dashboard, or print one repository's roadmap.

    `argv` is the argument list without the leading subcommand name. The
    caller owns the argument surface (see `lupin.cli`), so this only parses
    what it needs and ignores `None` to read sys.argv.
    """
    import argparse

    ap = argparse.ArgumentParser(prog="lupin serve", add_help=False)
    ap.add_argument("--bind", default="127.0.0.1", help="loopback or a tailnet address")
    ap.add_argument("--port", type=int, default=8788)
    ap.add_argument("--peek-lines", type=int, default=25, help="tail lines shown per loop")
    ap.add_argument("--roadmap", metavar="REPO", help="print a repository roadmap and exit")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--json", action="store_true")
    # Fleet data (issue #15, also used by issue #19's quest POST routes and
    # issue #20's Machines page): same flags and env-var fallback as
    # cli.py's `_fleet_connection_args`, kept in sync by hand since serve.py
    # parses its own argv independently of cli.py (see this function's
    # docstring) and importing cli.py here would be circular.
    ap.add_argument("--redis-host", default=os.environ.get("LUPIN_REDIS_HOST"))
    ap.add_argument(
        "--redis-port", type=int,
        default=int(os.environ["LUPIN_REDIS_PORT"]) if os.environ.get("LUPIN_REDIS_PORT") else None,
    )
    ap.add_argument("--redis-username", default=os.environ.get("LUPIN_REDIS_USERNAME"))
    ap.add_argument("--redis-password", default=os.environ.get("LUPIN_REDIS_PASSWORD"))
    ap.add_argument(
        "--config-path", default=os.environ.get("LUPIN_FLEET_CONFIG"),
        help="fleet config file (default: $LUPIN_FLEET_CONFIG or ~/.config/lupin/fleet.json)",
    )
    ap.add_argument(
        "--cmd-signing-key", default=os.environ.get("LUPIN_CMD_SIGNING_KEY"),
        help="legacy shared signing key for remote loop actions",
    )
    ap.add_argument(
        "--cmd-signing-key-dir", default=os.environ.get("LUPIN_CMD_SIGNING_KEYS_DIR"),
        help="directory with one file per target machine",
    )
    args, _unknown = ap.parse_known_args(argv)

    if args.roadmap:
        repos = roadmap.repository_names(code_repos())
        if args.roadmap not in repos:
            print(f"lupin: unknown repository {args.roadmap!r}", file=sys.stderr)
            return 2
        connection = machines.resolve_connection(
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_username=args.redis_username,
            redis_password=args.redis_password,
            config_path=args.config_path,
        )
        model = roadmap.cached_model(
            args.roadmap, os.path.join(CODE_DIR, args.roadmap), connection=connection
        )
        _print_roadmap(args.roadmap, model, args.verbose, args.json)
        return 0


    try:
        addr = ipaddress.ip_address(args.bind)
    except ValueError:
        print(f"lupin: --bind must be an IP address, got {args.bind!r}", file=sys.stderr)
        return 2
    if not bind_allowed(addr):
        print(
            f"lupin: refusing to bind {args.bind}. This dashboard is meant "
            "for loopback or a tailnet address (100.64.0.0/10) only. Use an "
            "SSH forward, or a tailnet ACL, to reach it from another machine.",
            file=sys.stderr,
        )
        return 2

    class Server(ThreadingHTTPServer):
        address_family = socket.AF_INET6 if addr.version == 6 else socket.AF_INET
        daemon_threads = True

    Handler.peek_lines = args.peek_lines
    Handler.allowed_hosts = {
        f"127.0.0.1:{args.port}",
        f"localhost:{args.port}",
        f"[::1]:{args.port}",
        f"{args.bind}:{args.port}",
    }
    Handler.fleet_connection = machines.resolve_connection(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_username=args.redis_username,
        redis_password=args.redis_password,
        config_path=args.config_path,
    )
    Handler.cmd_signing_key = args.cmd_signing_key
    Handler.cmd_signing_keys_dir = args.cmd_signing_key_dir

    httpd = Server((args.bind, args.port), Handler)
    print(f"lupin on http://{args.bind}:{args.port}", file=sys.stderr)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
