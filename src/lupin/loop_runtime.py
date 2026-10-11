"""Run and inspect delegation loops in Herdr sessions."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import slots, slots_redis

REPO_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
PLATFORMS = {"claude", "omp"}
AGENT_NAME = "lupin-loop"
# One Herdr session per machine. Each loop is one workspace in it.
SESSION_NAME = "lupin-loops"
STATE_DIR = Path(os.environ.get("LUPIN_LOOP_STATE_DIR", "/var/lib/delegation-loop"))
CODE_DIR = Path(os.environ.get("LUPIN_LOOP_CODE_DIR", "/code"))
REPOS_FILE = STATE_DIR / "repos"
ORCHESTRATORS_FILE = STATE_DIR / "orchestrators.json"
LOOPS_DIR = STATE_DIR / "herdr-loops"
REPORTS_DIR = STATE_DIR / "reports"
HERDR = os.environ.get("LUPIN_HERDR_BIN", "herdr")
NIXOS_SUDO_WRAPPER = Path("/run/wrappers/bin/sudo")
DEFAULT_PROMPT = (
    "You are the delegation-loop orchestrator for this repository. Read "
    "AGENTS.md and /delegation-loop. If docs/delegation-loop.md exists, read "
    "it; otherwise continue without repo-specific delegation notes."
)
HERDR_TIMEOUT = 20.0
GIT_TIMEOUT = 60.0
# Files in the git directory that show an unfinished git operation.
UNFINISHED_GIT_STATES = (
    ("MERGE_HEAD", "merge"),
    ("rebase-merge", "rebase"),
    ("rebase-apply", "rebase"),
    ("CHERRY_PICK_HEAD", "cherry-pick"),
    ("REVERT_HEAD", "revert"),
)
SERVER_START_TIMEOUT = 30.0
PANE_READ_TIMEOUT = 60.0
SYSTEMCTL_CHECK_TIMEOUT = 3.0
SYSTEMCTL_STOP_TIMEOUT = 30.0
LEASE_TTL = 60.0
HANDOFF_GRACE_S = 600.0
HANDOFF_TEXT = (
    "Lupin will stop this loop shortly. Run /handoff now: write an entry to the "
    ".loop/loop-state.json ledger (issue number if any, repo, branch, last known "
    "status) and update the relevant GitHub issue. Lupin keeps a copy of that "
    "ledger when it stops. Lupin deletes other ignored files in the worktree, so "
    "keep anything you need in the ledger or the issue. Lupin stops this loop when "
    "you finish, or after the wait time ends."
)

def validate_orchestrator(selector: str) -> str:
    if not isinstance(selector, str):
        raise LoopError("orchestrator selector must be text")
    if selector == "claude":
        return selector
    provider, separator, model = selector.partition("/")
    if not separator or provider not in {"openai", "opencode-go"}:
        raise LoopError("orchestrator must be claude, openai/MODEL, or opencode-go/MODEL")
    validate_omp_options("omp", provider, model)
    return selector


def orchestrator_profiles() -> dict[str, list[str]]:
    if not ORCHESTRATORS_FILE.exists():
        return {}
    try:
        values = json.loads(ORCHESTRATORS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LoopError(f"could not read {ORCHESTRATORS_FILE}: {exc}") from exc
    if not isinstance(values, dict):
        raise LoopError(f"invalid orchestrator profiles in {ORCHESTRATORS_FILE}")
    profiles: dict[str, list[str]] = {}
    for repo, selectors in values.items():
        repo = validate_repo(repo)
        if not isinstance(selectors, list) or not selectors:
            raise LoopError(f"invalid orchestrator profile for {repo}")
        profiles[repo] = list(dict.fromkeys(validate_orchestrator(item) for item in selectors))
    return profiles


def _write_orchestrator_profiles(profiles: dict[str, list[str]]) -> None:
    ORCHESTRATORS_FILE.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    content = json.dumps(profiles, indent=2, sort_keys=True) + "\n"
    fd, temporary = tempfile.mkstemp(prefix=".orchestrators.", dir=ORCHESTRATORS_FILE.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o640)
        os.replace(temporary, ORCHESTRATORS_FILE)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def select_orchestrator(
    candidates: list[str], snapshot: dict, *, now: float | None = None
) -> dict[str, str | None]:
    from . import pace, quota_cache

    now = time.time() if now is None else now
    now_ms = now * 1000
    choices: list[tuple[float, int, dict[str, str | None]]] = []
    for index, raw in enumerate(candidates):
        selector = validate_orchestrator(raw)
        if selector == "claude":
            provider, model = "claude", None
            platform = "claude"
        else:
            provider, model = selector.split("/", 1)[0], selector
            platform = "omp"
        entry = snapshot.get(provider)
        if not isinstance(entry, dict) or not quota_cache.is_fresh(entry, now):
            continue
        rows = entry.get("rows", [])
        if not isinstance(rows, list):
            continue
        rows = [row for row in rows if isinstance(row, dict)]
        usable = [
            row for row in rows
            if row.get("provider") == provider
            and isinstance(row.get("used_pct"), (int, float))
            and isinstance(row.get("resets_at"), (int, float))
        ]
        if not usable or pace.blocked(rows, provider, now_ms):
            continue
        headroom = min(100 - row["used_pct"] for row in usable)
        choices.append((headroom, -index, {
            "platform": platform,
            "provider": provider if platform == "omp" else None,
            "model": model,
        }))
    if not choices:
        raise LoopError("no configured orchestrator has fresh, usable quota")
    return max(choices, key=lambda choice: (choice[0], choice[1]))[2]


class LoopError(Exception):
    """A loop action cannot continue safely."""


class HerdrError(LoopError):
    """Herdr returned an error or an unsupported response."""


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def validate_repo(repo: Any) -> str:
    if not isinstance(repo, str) or not REPO_RE.fullmatch(repo):
        raise LoopError(f"invalid repo name {repo!r}")
    return repo


def validate_platform(platform: Any) -> str:
    if platform not in PLATFORMS:
        raise LoopError(f"invalid platform {platform!r}; use claude or omp")
    return platform


def session_name(repo: str) -> str:
    validate_repo(repo)
    return SESSION_NAME


def legacy_session_name(repo: str) -> str:
    """The session name used before loops shared one session per machine."""
    repo = validate_repo(repo)
    slug = re.sub(r"[^A-Za-z0-9._-]", "-", repo)[:38].strip(".-") or "repo"
    digest = hashlib.sha256(repo.encode()).hexdigest()[:10]
    return f"lupin-{slug}-{digest}"


def agent_name(repo: str) -> str:
    """A Herdr agent name for one repo: lowercase, digits, - and _, up to 32 characters."""
    repo = validate_repo(repo)
    slug = re.sub(r"[^a-z0-9_-]", "-", repo.lower())[:15].strip("-_") or "repo"
    digest = hashlib.sha256(repo.encode()).hexdigest()[:8]
    return f"lupin-{slug}-{digest}"


def _owns_session(repo: str, session: Any) -> bool:
    return session in (SESSION_NAME, legacy_session_name(repo))


def _agent_name_for(repo: str, session: str) -> str:
    """Loops in the shared session need their own agent name. Old loops keep AGENT_NAME."""
    return agent_name(repo) if session == SESSION_NAME else AGENT_NAME


def unit_name(repo: str, kind: str = "loop") -> str:
    return f"lupin-{kind}-{hashlib.sha256(validate_repo(repo).encode()).hexdigest()[:16]}"


def _lupin_command(*args: str) -> list[str]:
    executable = os.environ.get("LUPIN_EXECUTABLE") or str(Path(sys.argv[0]).resolve())
    return [executable, "loop", *args]


def _run(argv: list[str], *, timeout: float = HERDR_TIMEOUT, env: dict | None = None) -> tuple[int, str]:
    try:
        proc = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
            env=env,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LoopError(f"could not run {argv[0]}: {exc}") from exc
    output = proc.stdout
    if proc.stderr:
        output += proc.stderr
    return proc.returncode, output.strip()


def _sudo_argv(*args: str) -> list[str]:
    """Use NixOS's sudo wrapper when the service PATH omits it."""
    executable = (
        str(NIXOS_SUDO_WRAPPER)
        if NIXOS_SUDO_WRAPPER.is_file() and os.access(NIXOS_SUDO_WRAPPER, os.X_OK)
        else "sudo"
    )
    return [executable, "-n", *args]


def _herdr_env() -> dict[str, str]:
    env = os.environ.copy()
    env["HERDR_ENV"] = "1"
    return env


def _herdr(session: str | None, *args: str, timeout: float = HERDR_TIMEOUT) -> str:
    argv = [HERDR]
    if session:
        argv.extend(["--session", session])
    argv.extend(args)
    rc, output = _run(argv, timeout=timeout, env=_herdr_env())
    if rc:
        raise HerdrError(output or f"Herdr exited with status {rc}")
    return output


def _herdr_json(session: str | None, *args: str, timeout: float = HERDR_TIMEOUT) -> dict:
    output = _herdr(session, *args, timeout=timeout)
    try:
        value = json.loads(output)
    except json.JSONDecodeError as exc:
        raise HerdrError("Herdr returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise HerdrError("Herdr returned an unexpected JSON value")
    if "error" in value:
        error = value["error"]
        message = error.get("message", str(error)) if isinstance(error, dict) else str(error)
        raise HerdrError(message)
    result = value.get("result", value)
    if not isinstance(result, dict):
        raise HerdrError("Herdr returned an unexpected result")
    return result


def herdr_sessions() -> list[dict]:
    result = _herdr_json(None, "session", "list", "--json")
    sessions = result.get("sessions", [])
    return [row for row in sessions if isinstance(row, dict)]


def _server_running(session: str) -> bool:
    try:
        result = _herdr_json(session, "status", "server", "--json")
    except LoopError:
        return False
    return result.get("running") is True or result.get("status") == "running"


def _workspaces(session: str) -> list[dict]:
    result = _herdr_json(session, "workspace", "list")
    rows = [row for row in result.get("workspaces", []) if isinstance(row, dict)]
    if any("root_pane" not in row for row in rows):
        # Some Herdr versions list workspaces without their root pane.
        panes = [
            pane for pane in _herdr_json(session, "pane", "list").get("panes", [])
            if isinstance(pane, dict)
        ]
        for row in rows:
            if "root_pane" not in row:
                pane = next((p for p in panes if p.get("workspace_id") == _workspace_id(row)), None)
                if pane:
                    row["root_pane"] = pane
    return rows


def _agents(session: str) -> list[dict]:
    result = _herdr_json(session, "agent", "list")
    rows = result.get("agents", [])
    return [row for row in rows if isinstance(row, dict)]


def _agent_for_workspace(session: str, workspace_id: str) -> dict | None:
    for agent in _agents(session):
        name = agent.get("name")
        if agent.get("workspace_id") == workspace_id and isinstance(name, str) and name.startswith("lupin-"):
            return agent
    return None


def _workspace_by_id(session: str, workspace_id: str | None) -> dict | None:
    if not workspace_id:
        return None
    return next((row for row in _workspaces(session) if _workspace_id(row) == workspace_id), None)


def _workspace_root(workspace: dict) -> dict:
    pane = workspace.get("root_pane")
    return pane if isinstance(pane, dict) else {}


def _workspace_id(workspace: dict) -> str | None:
    value = workspace.get("id") or workspace.get("workspace_id")
    if isinstance(value, str):
        return value
    return _workspace_root(workspace).get("workspace_id")


def _pane_id(workspace: dict) -> str | None:
    value = _workspace_root(workspace).get("pane_id")
    return value if isinstance(value, str) else None


def _agent_state(session: str, workspace: dict) -> str:
    workspace_id = _workspace_id(workspace)
    if workspace_id:
        agent = _agent_for_workspace(session, workspace_id)
        if agent and agent.get("agent_status") in {"idle", "working", "blocked", "done", "unknown"}:
            return agent["agent_status"]
    state = _workspace_root(workspace).get("agent_status")
    return state if state in {"idle", "working", "blocked", "done", "unknown"} else "unknown"


def _metadata_path(repo: str) -> Path:
    return LOOPS_DIR / f"{validate_repo(repo)}.json"


def _read_metadata(repo: str) -> dict | None:
    path = _metadata_path(repo)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        raise LoopError(f"could not read loop state for {repo}: {exc}") from exc
    if not isinstance(value, dict) or value.get("repo") != repo:
        raise LoopError(f"invalid loop state for {repo}")
    return value


def _write_metadata(repo: str, value: dict) -> None:
    path = _metadata_path(repo)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    fd, temporary = tempfile.mkstemp(prefix=f".{repo}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o640)
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _default_repos() -> list[tuple[str, str]]:
    raw = os.environ.get("LUPIN_LOOP_DEFAULT_REPOS", "")
    return [(validate_repo(item), "claude") for item in raw.split(",") if item.strip()]


def enabled_repos() -> dict[str, str]:
    if not REPOS_FILE.exists():
        defaults = _default_repos()
        write_repos(defaults)
    values: dict[str, str] = {}
    try:
        lines = REPOS_FILE.read_text(encoding="utf-8").splitlines()
    except (OSError, ValueError) as exc:
        raise LoopError(f"could not read {REPOS_FILE}: {exc}") from exc
    for line in lines:
        fields = line.split()
        if not fields or fields[0].startswith("#"):
            continue
        repo = validate_repo(fields[0])
        platform = fields[1] if len(fields) > 1 else "claude"
        values[repo] = validate_platform(platform)
    return values


def write_repos(values: list[tuple[str, str]]) -> None:
    REPOS_FILE.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    content = "".join(f"{validate_repo(repo)} {validate_platform(platform)}\n" for repo, platform in values)
    fd, temporary = tempfile.mkstemp(prefix=".repos.", dir=REPOS_FILE.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o640)
        os.replace(temporary, REPOS_FILE)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def enable_repo(
    repo: str,
    platform: str = "claude",
    orchestrators: list[str] | None = None,
    *,
    clear_orchestrators: bool = False,
) -> None:
    repo = validate_repo(repo)
    platform = validate_platform(platform)
    if orchestrators is not None and clear_orchestrators:
        raise LoopError("use --orchestrator or --clear-orchestrators, not both")
    profiles = None
    if orchestrators is not None or clear_orchestrators:
        profiles = orchestrator_profiles()
        if clear_orchestrators:
            profiles.pop(repo, None)
        else:
            candidates = list(dict.fromkeys(validate_orchestrator(item) for item in orchestrators))
            if not candidates:
                raise LoopError("at least one orchestrator is required")
            profiles[repo] = candidates
    values = enabled_repos()
    values[repo] = platform
    write_repos(sorted(values.items()))
    if profiles is not None:
        _write_orchestrator_profiles(profiles)


def disable_repo(repo: str) -> None:
    repo = validate_repo(repo)
    values = enabled_repos()
    values.pop(repo, None)
    profiles = orchestrator_profiles()
    profiles.pop(repo, None)
    write_repos(sorted(values.items()))
    if ORCHESTRATORS_FILE.exists():
        _write_orchestrator_profiles(profiles)


def repo_catalog() -> list[dict]:
    enabled = enabled_repos()
    rows = []
    if not CODE_DIR.is_dir():
        return rows
    for path in sorted(CODE_DIR.iterdir(), key=lambda item: item.name.casefold()):
        if not path.is_dir() or not REPO_RE.fullmatch(path.name):
            continue
        has_doc = (path / "docs" / "delegation-loop.md").is_file()
        rows.append({
            "repo": path.name,
            "loopable": True,
            "has_doc": has_doc,
            "state": "enabled" if path.name in enabled else "disabled",
            "platform": enabled.get(path.name, "claude"),
        })
    return rows


def _runtime_paths(repo: str) -> dict:
    path = os.environ.get("LUPIN_LOOP_CREDENTIALS_FILE")
    if not path:
        return {}
    try:
        values = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LoopError(f"could not read loop credential map: {exc}") from exc
    if not isinstance(values, dict):
        raise LoopError("loop credential map must be a JSON object")
    value = values.get(repo)
    if value is None:
        return {}
    if not isinstance(value, str) or not Path(value).is_absolute():
        raise LoopError(f"invalid credential path for {repo}")
    if not Path(value).is_file():
        raise LoopError(f"credential file for {repo} is not available")
    return {"gh-token": value}


def _password_source() -> str | None:
    value = os.environ.get("LUPIN_LOOP_REDIS_PASSWORD_FILE")
    if value and Path(value).is_file():
        return value
    return None


def _credential_properties(values: dict[str, str]) -> list[str]:
    return [f"--property=LoadCredential={name}:{source}" for name, source in values.items()]


def _service_environment() -> list[str]:
    names = (
        "HOME", "LUPIN_BACKEND", "LUPIN_REDIS_HOST", "LUPIN_REDIS_PORT",
        "LUPIN_REDIS_USERNAME", "LUPIN_STATE_ROOT", "LUPIN_EXECUTABLE", "LUPIN_HERDR_BIN",
        "LUPIN_LOOP_STATE_DIR", "LUPIN_LOOP_CODE_DIR", "LUPIN_LOOP_USER", "LUPIN_LOOP_GROUP",
        "LUPIN_LOOP_DEFAULT_REPOS", "LUPIN_LOOP_CLAUDE_MAX_CONCURRENT",
        "LUPIN_LOOP_CREDENTIALS_FILE", "LUPIN_LOOP_REDIS_PASSWORD_FILE",
        "LUPIN_LOOP_PROMPT_FILE", "LUPIN_LOOP_PATH", "ANTHROPIC_BASE_URL",
        "HEADROOM_TELEMETRY_DISABLED", "RTK_TELEMETRY_DISABLED", "CODEGRAPH_TELEMETRY",
        "DO_NOT_TRACK",
    )
    properties = []
    path = os.environ.get("LUPIN_LOOP_PATH")
    if path:
        properties.append(f"--setenv=PATH={path}")
    for name in names:
        value = os.environ.get(name)
        if value is not None:
            properties.append(f"--setenv={name}={value}")
    return properties


def _systemd_run(
    repo: str | None,
    kind: str,
    command: list[str],
    *,
    credentials: dict[str, str] | None = None,
    claude_limits: bool = False,
) -> tuple[int, str]:
    user = os.environ.get("LUPIN_LOOP_USER", os.environ.get("USER", "ghosta"))
    group = os.environ.get("LUPIN_LOOP_GROUP", "users")
    unit = unit_name(repo, kind) if repo else f"lupin-{kind}-machine"
    argv = _sudo_argv(
        "systemd-run", f"--unit={unit}", "--collect",
        f"--uid={user}", f"--gid={group}", "--property=KillMode=control-group",
        "--property=TimeoutStopSec=20s",
    )
    if kind == "loop":
        argv.extend([
            "--property=Restart=on-failure",
            "--property=RestartSec=10s",
            "--property=StartLimitIntervalSec=5min",
            "--property=StartLimitBurst=3",
        ])
    if claude_limits:
        argv.extend(["--property=OOMPolicy=kill", "--property=MemoryMax=5G"])
    argv.extend(_service_environment())
    argv.extend(_credential_properties(credentials or {}))
    directory = CODE_DIR / validate_repo(repo) if repo else CODE_DIR
    argv.extend(["--working-directory", str(directory), "--", *command])
    return _run(argv, timeout=HERDR_TIMEOUT)


def _lock_file(repo: str):
    locks = STATE_DIR / "locks"
    locks.mkdir(parents=True, exist_ok=True, mode=0o750)
    return (locks / f"{validate_repo(repo)}.lock").open("a+")


def _machine_lock():
    locks = STATE_DIR / "locks"
    locks.mkdir(parents=True, exist_ok=True, mode=0o750)
    return (locks / "herdr-server.lock").open("a+")


def _legacy_tmux_exists(repo: str) -> bool:
    rc, _ = _run(["tmux", "has-session", "-t", f"=loop-{validate_repo(repo)}:"], timeout=2.0)
    return rc == 0


def _session_info(session: str) -> dict | None:
    return next((row for row in herdr_sessions() if row.get("name") == session), None)


def _session_running(session: str) -> bool:
    info = _session_info(session)
    return bool(info and info.get("running"))


def _write_prompt(repo: str, note: str | None, resume: bool) -> str:
    base_path = os.environ.get("LUPIN_LOOP_PROMPT_FILE")
    base = Path(base_path).read_text(encoding="utf-8") if base_path else DEFAULT_PROMPT
    if resume:
        base = (
            "You were cut off, not finished. Check for work in progress and continue it.\n\n"
            + base
        )
    handoff = STATE_DIR / "handoffs" / f"{validate_repo(repo)}.json"
    if handoff.is_file():
        base = (
            f"The last run kept its ledger at {handoff}. Read it before you start.\n\n"
            + base
        )
    handoff_doc = STATE_DIR / "handoffs" / f"{validate_repo(repo)}.HANDOFF.md"
    if handoff_doc.is_file():
        base = (
            f"The last run kept its HANDOFF.md at {handoff_doc}. Read it before you start.\n\n"
            + base
        )
    if note:
        base += f"\n\nExtra instructions for this run:\n{note}\n"
    notes = STATE_DIR / "notes"
    notes.mkdir(parents=True, exist_ok=True, mode=0o750)
    path = notes / f"prompt-{validate_repo(repo)}-{time.time_ns()}.md"
    path.write_text(base, encoding="utf-8")
    os.chmod(path, 0o640)
    return str(path)


def validate_omp_options(
    platform: str, provider: str | None, model: str | None
) -> tuple[str | None, str | None]:
    if provider is not None and (
        not isinstance(provider, str) or provider not in {"openai", "opencode-go"}
    ):
        raise LoopError("OMP provider must be openai or opencode-go")
    if model is not None and (
        not isinstance(model, str)
        or not model
        or len(model) > 256
        or any(ord(character) < 32 or ord(character) == 127 for character in model)
    ):
        raise LoopError("invalid OMP model")
    if platform != "omp" and (provider is not None or model is not None):
        raise LoopError("--provider and --model require --platform omp")
    return provider, model


def _git(directory: Path, *args: str) -> str:
    """Run git in `directory`. Return its output. Raise LoopError if git fails."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(directory), *args],
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LoopError(f"could not run git in {directory}: {exc}") from exc
    if proc.returncode:
        detail = (proc.stderr or proc.stdout).strip()
        raise LoopError(f"git {args[0]} failed in {directory}: {detail}")
    return proc.stdout


def _refuse_unfinished_git_work(checkout: Path) -> None:
    """Raise LoopError if the checkout has an unfinished git operation or unmerged files.

    This function only reads. It never aborts, resets, stashes, or cleans.
    """
    git_dir = Path(_git(checkout, "rev-parse", "--absolute-git-dir").strip())
    for name, label in UNFINISHED_GIT_STATES:
        marker = git_dir / name
        if marker.exists():
            raise LoopError(
                f"{checkout} has a {label} in progress ({marker}). "
                "Lupin does not change it. Finish or cancel it, then start the loop again."
            )
    entries = _git(checkout, "ls-files", "-u", "-z").split("\0")
    unmerged = sorted({entry.split("\t", 1)[1] for entry in entries if "\t" in entry})
    if unmerged:
        shown = ", ".join(unmerged[:5])
        if len(unmerged) > 5:
            shown += f" and {len(unmerged) - 5} more"
        raise LoopError(
            f"{checkout} has unmerged files: {shown}. "
            "Lupin does not change them. Resolve them, then start the loop again."
        )


def _default_base(checkout: Path) -> str:
    """Return the remote branch that a run starts from, for example `origin/main`."""
    try:
        base = _git(checkout, "symbolic-ref", "--short", "refs/remotes/origin/HEAD").strip()
    except LoopError:
        base = ""
    if base:
        return base
    for candidate in ("origin/main", "origin/master"):
        try:
            _git(checkout, "rev-parse", "--verify", "--quiet", f"refs/remotes/{candidate}")
        except LoopError:
            continue
        return candidate
    raise LoopError(
        f"cannot find the default branch of {checkout}. "
        "Run `git remote set-head origin --auto` in it."
    )


def _create_worktree(repo: str, checkout: Path) -> tuple[Path, str]:
    """Add a worktree for one run, on a new branch from the fetched default branch.

    Return the worktree path and the branch name. The worktree is under the
    loop state directory, not under CODE_DIR. Raise LoopError if the fetch fails.
    """
    _git(checkout, "fetch", "origin")
    base = _default_base(checkout)
    run_id = f"{_stamp()}-{time.time_ns() % 1000000:06d}"
    branch = f"lupin-loop/{run_id}"
    path = STATE_DIR / "worktrees" / repo / run_id
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    _git(checkout, "worktree", "add", "--no-track", "-b", branch, str(path), base)
    return path, branch


def _resumed_worktree(repo: str, metadata: dict | None) -> tuple[Path, str]:
    """Return the worktree and branch that the last run recorded. Never create one."""
    info = metadata or {}
    recorded = info.get("worktree")
    if not recorded:
        raise LoopError(f"cannot resume {repo}: no worktree is recorded. Start it without --resume.")
    path = Path(recorded)
    if not path.is_dir():
        raise LoopError(f"cannot resume {repo}: worktree {path} is missing. Start it without --resume.")
    branch = _git(path, "branch", "--show-current").strip()
    if branch != info.get("branch"):
        raise LoopError(
            f"cannot resume {repo}: worktree {path} is on branch {branch or 'none'}, "
            f"not {info.get('branch')}."
        )
    return path, branch


def _keep_handoff(repo: str, worktree: Path) -> None:
    """Copy handoff files out of the worktree before git deletes them.

    git deletes ignored files with the worktree. A file missing from this
    worktree keeps its earlier copy.
    """
    copies = [
        (worktree / ".loop" / "loop-state.json", f"{repo}.json"),
        (worktree / "HANDOFF.md", f"{repo}.HANDOFF.md"),
    ]
    found = [(source, name) for source, name in copies if source.is_file()]
    if not found:
        return
    target = STATE_DIR / "handoffs"
    target.mkdir(parents=True, exist_ok=True, mode=0o750)
    for source, name in found:
        shutil.copyfile(source, target / name)


def _remove_worktree(repo: str, metadata: dict) -> tuple[bool, str]:
    """Remove the run's worktree, then its branch. Never use `--force`.

    Return (gone, note). `gone` is False when git keeps the worktree. The note
    names what was kept and starts with "; " so a caller can append it. Raise
    LoopError when git fails for another reason. A branch that git will not
    delete only adds a note. "worktree" leaves `metadata` only when the worktree
    is gone. "branch" leaves it only when the branch is deleted.
    """
    value = metadata.get("worktree")
    if value:
        path = Path(value)
        if path.is_dir():
            if not _git(path, "for-each-ref", "--contains", "HEAD", "refs/heads", "refs/remotes").strip():
                return False, f"; worktree kept at {path}: its commit is on no branch"
            _keep_handoff(repo, path)
            try:
                _git(CODE_DIR / repo, "worktree", "remove", str(path))
            except LoopError as exc:
                try:
                    changed = _git(path, "status", "--porcelain").strip()
                except LoopError:
                    changed = ""
                if changed:
                    return False, f"; worktree kept at {path}: it has changes"
                raise LoopError(f"worktree kept at {path}: {exc}") from exc
        metadata.pop("worktree", None)
    branch = metadata.get("branch")
    if branch:
        try:
            _git(CODE_DIR / repo, "branch", "-d", branch)
        except LoopError:
            return True, f"; branch {branch} kept: git branch -d refused it"
        metadata.pop("branch", None)
    return True, ""


def _clear_previous_worktree(repo: str, metadata: dict | None) -> str:
    """Remove the last run's worktree before a new run starts. Return a note.

    Raise LoopError if git keeps that worktree.
    """
    if not metadata:
        return ""
    gone, note = _remove_worktree(repo, metadata)
    if not gone:
        raise LoopError(f"cannot start {repo}{note}. Save or remove that work, then start again.")
    return note


def _finish_failed_start(repo: str, value: dict, resume: bool) -> str:
    """Record a failed start and remove what it made. Return a note for the message.

    A resume keeps the recorded worktree, so the next resume can reuse it.
    Otherwise remove the worktree and its branch. When git keeps the worktree,
    the note names its path. Metadata is written even if removal raises.
    """
    value["state"] = "failed"
    removed_note = ""
    try:
        if not resume:
            try:
                _, removed_note = _remove_worktree(repo, value)
            except LoopError as exc:
                removed_note = f"; {exc}"
    finally:
        _write_metadata(repo, value)
        if value["prompt_file"]:
            Path(value["prompt_file"]).unlink(missing_ok=True)
    return removed_note


def start_loop(
    repo: str,
    *,
    platform: str | None = None,
    provider: str | None = None,
    model: str | None = None,
    note: str | None = None,
    resume: bool = False,
) -> tuple[bool, str]:
    repo = validate_repo(repo)
    selected = validate_platform(platform or enabled_repos().get(repo, "claude"))
    provider, model = validate_omp_options(selected, provider, model)
    checkout = CODE_DIR / repo
    if not checkout.is_dir():
        return False, f"skip {repo}: no checkout at {checkout}"
    if note is not None and (
        not isinstance(note, str) or len(note) > 8000 or "\x00" in note
    ):
        raise LoopError("note is invalid or too long")
    session = session_name(repo)
    worker_unit = unit_name(repo)
    lock = _lock_file(repo)
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        return False, f"skip {repo}: another start request is active"
    try:
        rc, _ = _run(
            ["systemctl", "is-active", "--quiet", f"{worker_unit}.service"],
            timeout=3.0,
        )
        if rc == 0:
            return False, f"skip {repo}: Lupin loop is already active"
        if _legacy_tmux_exists(repo):
            return False, (
                f"skip {repo}: legacy tmux loop-{repo} is open; "
                "drain it before starting Herdr"
            )
        metadata = _read_metadata(repo)
        if metadata and metadata.get("state") in {"running", "starting", "needs_attention"}:
            if _session_running(metadata.get("session") or session):
                return False, f"skip {repo}: Herdr loop is already running; use lupin stop first"
            return False, f"skip {repo}: saved Herdr state needs review; use lupin stop before starting again"
        legacy = _session_info(legacy_session_name(repo))
        if legacy and legacy.get("running"):
            return False, f"skip {repo}: an old Herdr session is still running; use lupin stop first"
        if platform is None and provider is None and model is None:
            candidates = orchestrator_profiles().get(repo)
            if candidates:
                from . import quota_cache

                choice = select_orchestrator(candidates, quota_cache.read_snapshot(**_redis_kwargs()))
                selected = choice["platform"]
                provider = choice["provider"]
                model = choice["model"]
        provider, model = validate_omp_options(selected, provider, model)
        _refuse_unfinished_git_work(checkout)
        if resume:
            worktree, branch = _resumed_worktree(repo, metadata)
            previous_note = ""
        else:
            previous_note = _clear_previous_worktree(repo, metadata)
            worktree, branch = _create_worktree(repo, checkout)
        value = {
            "version": 1,
            "repo": repo,
            "platform": selected,
            "provider": provider,
            "model": model,
            "session": session,
            "state": "starting",
            "started_at": _now(),
            "workspace_id": None,
            "pane_id": None,
            "prompt_file": None,
            "resume": bool(resume),
            "worktree": str(worktree),
            "branch": branch,
        }
        try:
            prompt_file = _write_prompt(repo, note, resume)
            value["prompt_file"] = prompt_file
            _write_metadata(repo, value)
            worker = _lupin_command(
                "worker", "--repo", repo, "--platform", selected, "--session", session,
                "--prompt-file", prompt_file,
            )
            if resume:
                worker.append("--resume")
            password_source = _password_source()
            worker_credentials = {"redis-password": password_source} if password_source else {}
            rc, output = _systemd_run(
                repo,
                "loop",
                worker,
                credentials=worker_credentials,
                claude_limits=selected == "claude",
            )
        except BaseException as exc:
            removed_note = _finish_failed_start(repo, value, resume)
            if not isinstance(exc, Exception):
                raise
            return False, (str(exc) or f"could not start loop service for {repo}") + removed_note
        if rc:
            removed_note = _finish_failed_start(repo, value, resume)
            return False, (output or f"could not start loop service for {repo}") + removed_note
        return True, (
            f"started {repo} ({selected}) in Herdr session {session}; "
            f"worktree {worktree}{previous_note}"
        )
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


def _load_credential(name: str) -> str | None:
    directory = os.environ.get("CREDENTIALS_DIRECTORY")
    if not directory:
        return None
    path = Path(directory) / name
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return value or None


def _redis_kwargs() -> dict:
    password = _load_credential("redis-password") or os.environ.get("LUPIN_REDIS_PASSWORD")
    return {
        "redis_host": os.environ.get("LUPIN_REDIS_HOST", "localhost"),
        "redis_port": int(os.environ.get("LUPIN_REDIS_PORT", "6379")),
        "redis_username": os.environ.get("LUPIN_REDIS_USERNAME"),
        "redis_password": password,
    }


def _wait_for_server(session: str, label: str) -> None:
    deadline = time.monotonic() + SERVER_START_TIMEOUT
    while time.monotonic() < deadline:
        if _server_running(session):
            return
        time.sleep(0.25)
    raise LoopError(f"Herdr server did not start for {label} within {SERVER_START_TIMEOUT:g}s")


def _ensure_shared_server(session: str) -> None:
    """Start the machine's one Herdr server. The lock stops two starts racing."""
    lock = _machine_lock()
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if _server_running(session):
            return
        command = _lupin_command("herdr-server", "--session", session)
        rc, output = _systemd_run(None, "herdr", command)
        if rc and not _server_running(session):
            raise LoopError(output or "could not start the Herdr server")
        _wait_for_server(session, "this machine")
    finally:
        lock.close()


def _ensure_server(repo: str, session: str, platform: str) -> None:
    if _server_running(session):
        return
    if session == SESSION_NAME:
        _ensure_shared_server(session)
        return
    credentials = _runtime_paths(repo)
    command = _lupin_command("herdr-server", "--session", session)
    rc, output = _systemd_run(repo, "herdr", command, credentials=credentials, claude_limits=platform == "claude")
    if rc:
        raise LoopError(output or f"could not start Herdr server for {repo}")
    _wait_for_server(session, repo)


def _metadata_for_session(repo: str, session: str) -> dict:
    value = _read_metadata(repo) or {}
    if value.get("session") != session:
        value = {
            "version": 1,
            "repo": repo,
            "platform": "claude",
            "session": session,
            "started_at": _now(),
            "workspace_id": None,
            "pane_id": None,
            "state": "starting",
        }
    return value


def _workspace_env(session: str, repo: str) -> list[str]:
    """The shared server has no per-repo token, so give the repo's token to its workspace."""
    if session != SESSION_NAME:
        return []
    path = _runtime_paths(repo).get("gh-token")
    if not path:
        return []
    token = Path(path).read_text(encoding="utf-8").strip()
    return ["--env", f"GH_TOKEN={token}"] if token else []


def _create_workspace(session: str, directory: Path, repo: str) -> dict:
    result = _herdr_json(
        session,
        "workspace", "create", "--cwd", str(directory), "--label", repo, "--no-focus",
        *_workspace_env(session, repo),
    )
    workspace = result.get("workspace")
    root = result.get("root_pane")
    if not isinstance(workspace, dict) or not isinstance(root, dict):
        raise HerdrError("Herdr did not return a workspace and root pane")
    workspace_id = root.get("workspace_id") or workspace.get("id") or workspace.get("workspace_id")
    pane_id = root.get("pane_id")
    if not isinstance(workspace_id, str) or not isinstance(pane_id, str):
        raise HerdrError("Herdr workspace response did not include IDs")
    return {"id": workspace_id, "root_pane": root, "pane_id": pane_id}


def _find_workspace(session: str, metadata: dict) -> dict | None:
    return _workspace_by_id(session, metadata.get("workspace_id"))


def _record_workspace(repo: str, metadata: dict, workspace: dict) -> dict:
    value = dict(metadata)
    value["workspace_id"] = _workspace_id(workspace)
    value["pane_id"] = _pane_id(workspace)
    value["state"] = "running"
    value.setdefault("started_at", _now())
    _write_metadata(repo, value)
    return value


def _wait_agent(session: str, workspace_id: str) -> int:
    while True:
        if not _server_running(session):
            return 1
        workspace = _workspace_by_id(session, workspace_id)
        if workspace is None:
            return 0
        if _agent_state(session, workspace) == "done":
            return 0
        time.sleep(2)


def _launch_agent(
    session: str, platform: str, workspace: dict, prompt_file: str, resume: bool,
    name: str = AGENT_NAME, provider: str | None = None, model: str | None = None,
) -> int:
    pane = _pane_id(workspace)
    workspace_id = _workspace_id(workspace)
    if not pane or not workspace_id:
        raise HerdrError("Herdr workspace has no pane or workspace ID")
    # Herdr rejects newlines in agent arguments, so send the prompt as one line.
    prompt = " ".join(Path(prompt_file).read_text(encoding="utf-8").split())
    argv = [
        HERDR, "--session", session, "agent", "start", name, "--kind", platform,
        "--pane", pane, "--timeout", "300000", "--",
    ]
    # Herdr passes arguments after `--` to the agent. It rejects this flag before `--`.
    if platform == "claude":
        argv.append("--dangerously-skip-permissions")
    if resume and platform == "claude":
        argv.append("--continue")
    if platform == "omp":
        if provider:
            argv.extend(["--provider", provider])
        if model:
            argv.extend(["--model", model])
    argv.append(prompt)
    rc, output = _run(argv, timeout=310.0, env=_herdr_env())
    if rc:
        raise HerdrError(output or "Herdr could not start the agent")
    return _wait_agent(session, workspace_id)


def _wait_existing_agent(session: str, workspace_id: str) -> int:
    return _wait_agent(session, workspace_id)

def _agent_command(
    repo: str, session: str, workspace_id: str, platform: str, prompt_file: str,
    resume: bool, launch: bool, provider: str | None = None, model: str | None = None,
) -> list[str]:
    args = [
        "launch-agent" if launch else "wait-agent",
        "--repo", repo, "--session", session, "--workspace", workspace_id,
    ]
    if launch:
        args.extend(["--platform", platform, "--prompt-file", prompt_file])
        if provider:
            args.extend(["--provider", provider])
        if model:
            args.extend(["--model", model])
    if resume:
        args.append("--resume")
    return _lupin_command(*args)




def _run_worktree(repo: str, metadata: dict) -> Path:
    """Return the run's worktree. The agent never works in the main checkout."""
    value = metadata.get("worktree")
    if not isinstance(value, str) or not Path(value).is_dir():
        raise LoopError(f"the run for {repo} has no worktree; stop it and start again")
    return Path(value)


def _monitor_loop(
    repo: str, platform: str, session: str, prompt_file: str, resume: bool
) -> int:
    _ensure_server(repo, session, platform)
    metadata = _metadata_for_session(repo, session)
    workspace = _find_workspace(session, metadata)
    if workspace is None and metadata.get("state") in {"running", "needs_attention"}:
        metadata["state"] = "needs_attention"
        _write_metadata(repo, metadata)
        raise LoopError(f"saved Herdr workspace for {repo} is missing; review it before starting again")
    launch = workspace is None
    if launch:
        workspace = _create_workspace(session, _run_worktree(repo, metadata), repo)
        metadata = _record_workspace(repo, metadata, workspace)
    else:
        workspace_id = _workspace_id(workspace)
        if not workspace_id:
            raise HerdrError("saved Herdr workspace has no ID")
        if _agent_state(session, workspace) != "done" and _agent_for_workspace(session, workspace_id) is None:
            metadata["state"] = "needs_attention"
            _write_metadata(repo, metadata)
            raise LoopError(f"saved Herdr workspace for {repo} has no reported agent; stop it before starting again")
    workspace_id = _workspace_id(workspace)
    if not workspace_id:
        raise HerdrError("Herdr workspace has no ID")
    command = _agent_command(
        repo, session, workspace_id, platform, prompt_file, resume, launch,
        provider=metadata.get("provider"), model=metadata.get("model"),
    )
    max_holders = (
        int(os.environ.get("LUPIN_LOOP_CLAUDE_MAX_CONCURRENT", "1"))
        if platform == "claude" else 1
    )
    wait_s = 0.0 if launch and platform == "claude" else 10800.0
    try:
        rc = slots.hold(
            command, slot=platform, holder=unit_name(repo), wait=wait_s,
            ttl=LEASE_TTL, max_holders=max_holders,
        )
    except slots.SlotFull:
        if launch:
            _herdr_json(session, "workspace", "close", workspace_id)
            metadata["state"] = "failed"
            metadata["workspace_id"] = None
            metadata["pane_id"] = None
            removed_note = ""
            try:
                _, removed_note = _remove_worktree(repo, metadata)
            except LoopError as exc:
                removed_note = f"; {exc}"
            _write_metadata(repo, metadata)
            raise LoopError(f"{platform} slot is full{removed_note}")
        raise LoopError(f"{platform} slot is full")
    if rc:
        metadata["state"] = "needs_attention"
        _write_metadata(repo, metadata)
        raise LoopError(f"Herdr agent for {repo} stopped with status {rc}")
    while True:
        if not _server_running(session):
            metadata["state"] = "needs_attention"
            _write_metadata(repo, metadata)
            return 1
        if _workspace_by_id(session, workspace_id) is None:
            metadata["state"] = "stopped"
            metadata["stopped_at"] = _now()
            _write_metadata(repo, metadata)
            return 0
        time.sleep(2)


def worker(
    repo: str, platform: str, session: str, prompt_file: str, resume: bool = False,
    recovering: bool = False,
) -> int:
    repo = validate_repo(repo)
    platform = validate_platform(platform)
    if not _owns_session(repo, session):
        raise LoopError("Herdr session name does not match repo")
    if not (CODE_DIR / repo).is_dir():
        raise LoopError(f"repo directory does not exist: {CODE_DIR / repo}")
    credential = _load_credential("redis-password")
    if credential:
        os.environ["LUPIN_REDIS_PASSWORD"] = credential
    kwargs = _redis_kwargs()
    metadata = _metadata_for_session(repo, session)
    metadata.update({"repo": repo, "platform": platform, "session": session})
    metadata["prompt_file"] = prompt_file
    if not recovering:
        metadata["state"] = "starting"
    _write_metadata(repo, metadata)
    command = _lupin_command(
        "monitor", "--repo", repo, "--platform", platform, "--session", session,
        "--prompt-file", prompt_file,
    )
    if resume:
        command.append("--resume")
    try:
        return slots_redis.hold(
            command, slot=f"loop-{repo}", holder=unit_name(repo), wait=0.0,
            ttl=LEASE_TTL, max_holders=1, **kwargs,
        )
    except slots.SlotFull:
        metadata["state"] = "needs_attention"
        _write_metadata(repo, metadata)
        raise LoopError(f"fleet loop slot is full for {repo}")


def monitor(repo: str, platform: str, session: str, prompt_file: str, resume: bool) -> int:
    try:
        return _monitor_loop(repo, platform, session, prompt_file, resume)
    except LoopError:
        raise
    except Exception as exc:
        raise LoopError(str(exc)) from exc

def herdr_server(session: str) -> int:
    if not re.fullmatch(r"lupin-[A-Za-z0-9._-]{1,52}", session):
        raise LoopError("invalid Lupin Herdr session name")
    credential = _load_credential("gh-token")
    env = os.environ.copy()
    if credential:
        env["GH_TOKEN"] = credential
    env.pop("HERDR_ENV", None)
    argv = [HERDR, "--session", session, "server"]
    try:
        return subprocess.run(argv, env=env, check=False).returncode
    except OSError as exc:
        raise LoopError(f"could not start Herdr server: {exc}") from exc


def launch_agent(
    repo: str, session: str, workspace_id: str, platform: str, prompt_file: str, resume: bool,
    provider: str | None = None, model: str | None = None,
) -> int:
    workspace = _workspace_by_id(session, workspace_id)
    if workspace is None:
        return 0
    provider, model = validate_omp_options(platform, provider, model)
    return _launch_agent(session, platform, workspace, prompt_file, resume, _agent_name_for(repo, session), provider, model)

def wait_agent(session: str, workspace_id: str) -> int:
    return _wait_existing_agent(session, workspace_id)


def _save_report(repo: str, session: str, workspace: dict) -> Path:
    pane = _pane_id(workspace)
    if not pane:
        raise HerdrError("Herdr workspace has no pane ID")
    output = _herdr(session, "pane", "read", pane, "--source", "recent", "--lines", "100000", "--format", "text", timeout=PANE_READ_TIMEOUT)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True, mode=0o750)
    path = REPORTS_DIR / f"{validate_repo(repo)}-{_stamp()}-{time.time_ns() % 1000000:06d}.log"
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_text(output + "\n", encoding="utf-8")
    os.chmod(temporary, 0o640)
    os.replace(temporary, path)
    return path


def _start_server_for_read(repo: str, metadata: dict) -> str:
    session = metadata.get("session") or session_name(repo)
    if not isinstance(session, str):
        raise LoopError("loop state has an invalid Herdr session")
    _ensure_server(repo, session, metadata.get("platform", "claude"))
    return session


def _request_handoff(repo: str, session: str, workspace: dict, grace: float) -> str:
    """Ask the loop's agent to run /handoff and wait until it settles.

    Returns a note for the stop message, or "" when nothing needs saying.
    A wait that ends without a settled agent is not an error: the text sent
    to the agent says the stop happens anyway. Any other failure raises, so
    nothing is closed. `--force` skips this step.
    """
    workspace_id = _workspace_id(workspace)
    agent = _agent_for_workspace(session, workspace_id) if workspace_id else None
    if agent is None:
        return ""
    try:
        _herdr(
            session, "agent", "prompt", agent.get("name") or AGENT_NAME, HANDOFF_TEXT,
            "--wait", "--timeout", str(int(grace * 1000)),
            timeout=grace + HERDR_TIMEOUT,
        )
    except HerdrError as exc:
        if "timeout" in str(exc).lower():
            return f"; the agent did not finish its handoff in {int(grace)}s"
        raise LoopError(
            f"could not ask the agent for {repo} to hand off: {exc}. "
            "Nothing was stopped. Add --force to stop it anyway."
        ) from exc
    return ""


def stop_loop(repo: str, *, force: bool = False, grace: float = HANDOFF_GRACE_S) -> str:
    repo = validate_repo(repo)
    lock = _lock_file(repo)
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        metadata = _read_metadata(repo)
        if not metadata:
            raise LoopError(f"no Lupin loop state for {repo}")
        session = _start_server_for_read(repo, metadata)
        workspace = _find_workspace(session, metadata)
        report = None
        handoff_note = ""
        if workspace is not None:
            if not force:
                handoff_note = _request_handoff(repo, session, workspace, grace)
            report = _save_report(repo, session, workspace)
            workspace_id = _workspace_id(workspace)
            if not workspace_id:
                raise HerdrError("Herdr workspace has no ID")
            _herdr_json(session, "workspace", "close", workspace_id)
        _, state = _run(["systemctl", "is-active", f"{unit_name(repo)}.service"], timeout=SYSTEMCTL_CHECK_TIMEOUT)
        if state in {"active", "activating", "deactivating"}:
            rc, output = _run(
                _sudo_argv("systemctl", "stop", f"{unit_name(repo)}.service"),
                timeout=SYSTEMCTL_STOP_TIMEOUT,
            )
            if rc:
                raise LoopError(output or f"could not stop Lupin worker for {repo}")
        if session != SESSION_NAME and not _workspaces(session):
            _herdr_json(None, "session", "stop", session, "--json")
        _, worktree_note = _remove_worktree(repo, metadata)
        metadata["state"] = "stopped"
        metadata["stopped_at"] = _now()
        metadata["workspace_id"] = None
        metadata["pane_id"] = None
        _write_metadata(repo, metadata)
        message = (
            f"stopped {repo}"
            + (f"; report saved to {report}" if report else "")
            + handoff_note
            + worktree_note
        )
    finally:
        lock.close()
    _write_debrief(repo, metadata, forced=force)
    return message


def _write_debrief(repo: str, metadata: dict, *, forced: bool) -> None:
    """Write the debrief. The stop already happened, so a failure only prints a warning."""
    # Local import: debrief -> roadmap -> loop_runtime is a cycle.
    from . import debrief

    try:
        debrief.write_debrief(
            STATE_DIR, repo, CODE_DIR / repo, metadata.get("started_at"), forced=forced,
        )
    except Exception as exc:
        print(f"lupin loop: no debrief for {repo}: {exc}", file=sys.stderr)


def write_due_periodic_debriefs(now: datetime | None = None) -> None:
    """Write each periodic debrief that is due. A failure only prints a warning."""
    # Local import: debrief -> roadmap -> loop_runtime is a cycle.
    from . import debrief

    now = datetime.now(timezone.utc) if now is None else now
    try:
        repos = enabled_repos()
    except (LoopError, OSError) as exc:
        # enabled_repos writes the default file when it is missing. That write can raise OSError.
        print(f"lupin agent: no periodic debriefs: {exc} ({REPOS_FILE})", file=sys.stderr)
        return
    for repo in repos:
        checkout = CODE_DIR / repo
        if not checkout.is_dir():
            continue
        for period in debrief.PERIODS:
            try:
                if debrief.period_due(STATE_DIR, repo, period, now):
                    debrief.write_period(STATE_DIR, repo, checkout, period, now=now)
            except Exception as exc:
                print(f"lupin agent: no {period} debrief for {repo}: {exc}", file=sys.stderr)


def peek_loop(repo: str, lines: int = 60) -> str:
    repo = validate_repo(repo)
    if not isinstance(lines, int) or lines < 1 or lines > 5000:
        raise LoopError("lines must be between 1 and 5000")
    metadata = _read_metadata(repo)
    if not metadata:
        raise LoopError(f"no Lupin loop state for {repo}")
    session = metadata.get("session") or session_name(repo)
    if not _server_running(session):
        raise LoopError(f"Herdr session for {repo} is not running")
    workspace = _find_workspace(session, metadata)
    if not workspace:
        raise LoopError(f"Herdr workspace for {repo} is not available")
    pane = _pane_id(workspace)
    if not pane:
        raise HerdrError("Herdr workspace has no pane ID")
    return _herdr(session, "pane", "read", pane, "--source", "recent", "--lines", str(lines), "--format", "text")


def send_loop(repo: str, text: str) -> None:
    """Type one line into the loop's pane and press Enter."""
    repo = validate_repo(repo)
    metadata = _read_metadata(repo)
    if not metadata:
        raise LoopError(f"no Lupin loop state for {repo}")
    session = metadata.get("session") or session_name(repo)
    if not _server_running(session):
        raise LoopError(f"Herdr session for {repo} is not running")
    workspace = _find_workspace(session, metadata)
    pane = _pane_id(workspace) if workspace else None
    if not pane:
        raise LoopError(f"Herdr pane for {repo} is not available")
    _herdr(session, "pane", "send-text", pane, text)
    _herdr(session, "pane", "send-keys", pane, "Enter")


def loop_state(repo: str) -> dict:
    repo = validate_repo(repo)
    metadata = _read_metadata(repo)
    if not metadata:
        return {"repo": repo, "backend": "herdr", "state": "stopped", "session": session_name(repo)}
    session = metadata.get("session") or session_name(repo)
    if metadata.get("state") in {"stopped", "failed"}:
        return {
            "repo": repo,
            "platform": metadata.get("platform", "claude"),
            "backend": "herdr",
            "state": metadata["state"],
            "session": session,
            "since": metadata.get("started_at"),
        }
    if not _server_running(session):
        return {
            "repo": repo, "backend": "herdr", "state": "unknown", "session": session,
            "since": metadata.get("started_at"), "error": "Herdr server is not running",
        }
    workspace = _find_workspace(session, metadata)
    if workspace is None:
        saved_state = metadata.get("state")
        if saved_state in {"running", "needs_attention"}:
            state = "needs_attention"
            if saved_state != state:
                metadata["state"] = state
                _write_metadata(repo, metadata)
        else:
            state = saved_state if saved_state == "starting" else "unknown"
        return {
            "repo": repo,
            "platform": metadata.get("platform", "claude"),
            "backend": "herdr",
            "state": state,
            "session": session,
            "workspace_id": metadata.get("workspace_id"),
            "pane_id": metadata.get("pane_id"),
            "since": metadata.get("started_at"),
        }
    workspace_id = _workspace_id(workspace)
    agent = _agent_for_workspace(session, workspace_id) if workspace_id else None
    return {
        "repo": repo,
        "platform": metadata.get("platform", "claude"),
        "backend": "herdr",
        "state": _agent_state(session, workspace),
        "session": session,
        "workspace_id": workspace_id,
        "pane_id": _pane_id(workspace),
        "since": metadata.get("started_at"),
        "agent": agent.get("name", AGENT_NAME) if agent else AGENT_NAME,
    }


def local_loops() -> list[dict]:
    if not LOOPS_DIR.is_dir():
        return []
    result = []
    for path in sorted(LOOPS_DIR.glob("*.json")):
        try:
            metadata = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(metadata, dict):
            continue
        repo = metadata.get("repo")
        session = metadata.get("session")
        if (
            metadata.get("state") not in {"running", "starting", "needs_attention"}
            or not isinstance(repo, str)
            or not isinstance(session, str)
        ):
            continue
        try:
            state = loop_state(repo)
        except LoopError:
            state = {"repo": repo, "backend": "herdr", "state": "unknown", "session": session}
        if state.get("state") == "stopped":
            continue
        result.append({
            "repo": repo,
            "platform": metadata.get("platform", "claude"),
            "state": state.get("state", "unknown"),
            "since": metadata.get("started_at"),
            "backend": "herdr",
            "session": session,
            "workspace_id": state.get("workspace_id"),
            "pane_id": state.get("pane_id"),
        })
    return result


def attach_argv(repo: str, machine: str, local_host: str, ssh_target: str | None) -> list[str]:
    repo = validate_repo(repo)
    metadata = _read_metadata(repo)
    session = metadata.get("session") if metadata else session_name(repo)
    if machine == local_host:
        return [HERDR, "--session", session]
    if not ssh_target:
        raise LoopError(f"no ssh target is configured for {machine!r}")
    return [HERDR, "--remote", ssh_target, "--session", session]


def recover() -> list[str]:
    recovered = []
    if not LOOPS_DIR.is_dir():
        return recovered
    for path in sorted(LOOPS_DIR.glob("*.json")):
        try:
            metadata = json.loads(path.read_text(encoding="utf-8"))
            repo = validate_repo(metadata.get("repo"))
            session = metadata.get("session")
            platform = validate_platform(metadata.get("platform", "claude"))
            prompt_file = metadata.get("prompt_file")
        except (OSError, json.JSONDecodeError, LoopError):
            continue
        if metadata.get("state") not in {"running", "starting"}:
            continue
        if (
            not _owns_session(repo, session)
            or not isinstance(prompt_file, str)
            or not Path(prompt_file).is_file()
        ):
            continue
        rc, _ = _run(["systemctl", "is-active", "--quiet", f"{unit_name(repo)}.service"], timeout=3.0)
        if rc == 0:
            continue
        command = _lupin_command(
            "worker", "--repo", repo, "--platform", platform, "--session", session,
            "--prompt-file", prompt_file, "--recover",
        )
        if metadata.get("resume"):
            command.append("--resume")
        password_source = _password_source()
        creds = {"redis-password": password_source} if password_source else {}
        rc, output = _systemd_run(
            repo, "loop", command, credentials=creds, claude_limits=platform == "claude"
        )
        recovered.append(f"{repo}: {'restarted monitor' if rc == 0 else output}")
    return recovered


def _schedule_text(value: str, label: str, maximum: int = 64) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise LoopError(f"invalid {label}")
    return value


def _event_time(when: str) -> tuple[str, bool]:
    _schedule_text(when, "schedule time")
    if when.startswith("+"):
        span = when[1:]
        rc, output = _run(["systemd-analyze", "timespan", span], timeout=5.0)
        if rc:
            raise LoopError(f"invalid relative time span: {output}")
        return span, True
    rc, output = _run(["systemd-analyze", "calendar", when], timeout=5.0)
    if rc:
        raise LoopError(f"invalid calendar time: {output}")
    return when, False


def schedule_once(
    when: str,
    repos: list[str],
    platform: str | None = None,
    note: str | None = None,
    resume: bool = False,
    provider: str | None = None,
    model: str | None = None,
) -> str:
    if note is not None and (not isinstance(note, str) or len(note) > 8000 or "\x00" in note):
        raise LoopError("note is invalid or too long")
    selected = validate_platform(platform) if platform else None
    provider, model = validate_omp_options(selected, provider, model) if selected else (provider, model)
    if (provider is not None or model is not None) and selected != "omp":
        raise LoopError("--provider and --model require --platform omp")
    repos = [validate_repo(repo) for repo in repos]
    if not repos:
        raise LoopError("at least one repo is required for a one-off run")
    when_value, relative = _event_time(when)
    identifier = hashlib.sha256(f"{time.time_ns()}:{os.getpid()}:{repos}".encode()).hexdigest()[:16]
    entry = {
        "id": identifier,
        "repos": repos,
        "platform": selected,
        "provider": provider,
        "model": model,
        "note": note,
        "resume": bool(resume),
        "when": when,
        "created_at": _now(),
    }
    directory = STATE_DIR / "once"
    directory.mkdir(parents=True, exist_ok=True, mode=0o750)
    path = directory / f"{identifier}.json"
    path.write_text(json.dumps(entry, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(path, 0o640)
    unit = f"lupin-once-{identifier}"
    command = _lupin_command("once-fire", "--id", identifier)
    user = os.environ.get("LUPIN_LOOP_USER", os.environ.get("USER", "ghosta"))
    group = os.environ.get("LUPIN_LOOP_GROUP", "users")
    argv = _sudo_argv(
        "systemd-run", f"--unit={unit}", "--collect",
        f"--uid={user}", f"--gid={group}", "--timer-property=AccuracySec=1s",
    )
    if relative:
        argv.append(f"--on-active={when_value}")
    else:
        argv.append(f"--on-calendar={when_value}")
    argv.extend(_service_environment())
    argv.extend(["--", *command])
    rc, output = _run(argv, timeout=HERDR_TIMEOUT)
    if rc:
        path.unlink(missing_ok=True)
        raise LoopError(output or "could not schedule one-off loop")
    return f"scheduled {', '.join(repos)} for {when}"


def once_fire(identifier: str) -> list[str]:
    if not re.fullmatch(r"[a-f0-9]{16}", identifier):
        raise LoopError("invalid one-off schedule ID")
    path = STATE_DIR / "once" / f"{identifier}.json"
    try:
        entry = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LoopError(f"could not read one-off schedule: {exc}") from exc
    results = []
    for repo in entry.get("repos", []):
        try:
            ok, message = start_loop(
                repo,
                platform=entry.get("platform"),
                provider=entry.get("provider"),
                model=entry.get("model"),
                note=entry.get("note"),
                resume=entry.get("resume", False),
            )
        except LoopError as exc:
            ok = False
            message = f"could not start loop for {repo}: {exc}"
        results.append(message)
        if not ok:
            print(message, file=sys.stderr)
    path.unlink(missing_ok=True)
    return results


_TIMER_TRIGGER_RESET = (
    "OnCalendar=\n"
    "OnActiveSec=\n"
    "OnBootSec=\n"
    "OnStartupSec=\n"
    "OnUnitActiveSec=\n"
    "OnUnitInactiveSec=\n"
)


def schedule_local(args: list[str]) -> str:
    """Show or change the recurring systemd timer."""
    if not args or args == ["show"]:
        rc, output = _run(["systemctl", "list-timers", "delegation-loop.timer", "--no-pager"], timeout=10.0)
        if rc:
            raise LoopError(output or "could not read delegation-loop.timer")
        return output
    if args == ["pause"] or args == ["resume"]:
        action = "stop" if args[0] == "pause" else "start"
        rc, output = _run(_sudo_argv("systemctl", action, "delegation-loop.timer"), timeout=20.0)
        if rc:
            raise LoopError(output or f"could not {args[0]} delegation-loop.timer")
        return f"{args[0]}d delegation-loop.timer"
    state_dir = STATE_DIR
    dropin = state_dir / "timer-dropin.d" / "override.conf"
    if len(args) == 4 and args[0] == "first" and args[2] == "every":
        when, relative = _event_time(args[1])
        interval = _schedule_text(args[3], "timer interval")
        rc, output = _run(["systemd-analyze", "timespan", interval], timeout=5.0)
        if rc:
            raise LoopError(f"invalid timer interval: {output}")
        first = "OnActiveSec" if relative else "OnCalendar"
        content = (
            f"[Timer]\n{_TIMER_TRIGGER_RESET}"
            f"{first}={when}\nOnUnitActiveSec={interval}\nAccuracySec=1s\n"
        )
    elif len(args) == 2 and args[0] == "cal":
        expression = _schedule_text(args[1], "calendar expression", 256)
        rc, output = _run(["systemd-analyze", "calendar", expression], timeout=5.0)
        if rc:
            raise LoopError(f"invalid calendar expression: {output}")
        content = (
            f"[Timer]\n{_TIMER_TRIGGER_RESET}"
            f"OnCalendar={expression}\nAccuracySec=1s\n"
        )
    else:
        raise LoopError("usage: lupin schedule [show|first <when> every <interval>|cal <expression>]")
    dropin.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    dropin.write_text(content, encoding="utf-8")
    rc, output = _run(_sudo_argv("systemctl", "daemon-reload"), timeout=20.0)
    if rc:
        raise LoopError(output or "systemd daemon-reload failed")
    rc, output = _run(
        _sudo_argv("systemctl", "restart", "delegation-loop.timer"), timeout=20.0
    )
    if rc:
        raise LoopError(output or "could not restart delegation-loop.timer")
    return "updated delegation-loop.timer"


def status_rows(repos: list[str] | None = None) -> list[dict]:
    enabled = enabled_repos()
    values = {row["repo"]: row for row in local_loops()}
    selected = repos if repos is not None else list(dict.fromkeys([*enabled, *values]))
    return [
        values.get(repo, {
            "repo": repo,
            "platform": enabled.get(repo, "claude"),
            "state": "stopped",
            "backend": "herdr",
            "session": session_name(repo),
            "since": None,
        })
        for repo in selected
    ]


def _print_json(value: Any) -> None:
    print(json.dumps(value, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="lupin loop", add_help=True)
    sub = parser.add_subparsers(dest="action", required=True)
    worker_parser = sub.add_parser("worker", help=argparse.SUPPRESS)
    worker_parser.add_argument("--repo", required=True)
    worker_parser.add_argument("--platform", required=True)
    worker_parser.add_argument("--session", required=True)
    worker_parser.add_argument("--prompt-file", required=True)
    worker_parser.add_argument("--resume", action="store_true")
    worker_parser.add_argument("--recover", action="store_true")
    server_parser = sub.add_parser("herdr-server", help=argparse.SUPPRESS)
    server_parser.add_argument("--session", required=True)
    monitor_parser = sub.add_parser("monitor", help=argparse.SUPPRESS)
    monitor_parser.add_argument("--repo", required=True)
    monitor_parser.add_argument("--platform", required=True)
    monitor_parser.add_argument("--session", required=True)
    monitor_parser.add_argument("--prompt-file", required=True)
    monitor_parser.add_argument("--resume", action="store_true")
    launch_parser = sub.add_parser("launch-agent", help=argparse.SUPPRESS)
    launch_parser.add_argument("--repo", required=True)
    launch_parser.add_argument("--session", required=True)
    launch_parser.add_argument("--workspace", required=True)
    launch_parser.add_argument("--platform", required=True)
    launch_parser.add_argument("--prompt-file", required=True)
    launch_parser.add_argument("--provider", choices=("openai", "opencode-go"))
    launch_parser.add_argument("--model")
    launch_parser.add_argument("--resume", action="store_true")
    wait_parser = sub.add_parser("wait-agent", help=argparse.SUPPRESS)
    wait_parser.add_argument("--repo", required=True)
    wait_parser.add_argument("--session", required=True)
    wait_parser.add_argument("--workspace", required=True)
    once_parser = sub.add_parser("once-fire", help=argparse.SUPPRESS)
    once_parser.add_argument("--id", required=True)
    local_parser = sub.add_parser("local-action", help=argparse.SUPPRESS)
    local_parser.add_argument("local_action", choices=("stop", "peek", "send", "state", "schedule", "pause", "resume"))
    local_parser.add_argument("action_args", nargs=argparse.REMAINDER)
    sub.add_parser("recover", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        if args.action == "worker":
            return worker(
                args.repo, args.platform, args.session, args.prompt_file, args.resume, args.recover
            )
        if args.action == "herdr-server":
            return herdr_server(args.session)
        if args.action == "launch-agent":
            return launch_agent(
                args.repo,
                args.session,
                args.workspace,
                args.platform,
                args.prompt_file,
                args.resume,
                args.provider,
                args.model,
            )
        if args.action == "wait-agent":
            return wait_agent(args.session, args.workspace)
        if args.action == "once-fire":
            for item in once_fire(args.id):
                print(item)
            return 0
        if args.action == "recover":
            for item in recover():
                print(item)
            return 0
        if args.action == "local-action":
            if args.local_action == "stop":
                print(stop_loop(args.action_args[0], force="--force" in args.action_args[1:]))
            elif args.local_action == "peek":
                lines = int(args.action_args[1]) if len(args.action_args) > 1 else 60
                print(peek_loop(args.action_args[0], lines))
            elif args.local_action == "send":
                send_loop(args.action_args[0], args.action_args[1])
                print(f"sent to {args.action_args[0]}")
            elif args.local_action == "state":
                _print_json(loop_state(args.action_args[0]))
            elif args.local_action == "schedule":
                print(schedule_local(args.action_args))
            else:
                print(schedule_local([args.local_action]))
            return 0
    except (LoopError, slots.SlotFull, slots.CoordinatorUnreachable, slots_redis.CoordinatorUnreachable) as exc:
        print(f"lupin loop: {exc}", file=sys.stderr)
        return 2
    except (IndexError, ValueError) as exc:
        print(f"lupin loop: invalid arguments: {exc}", file=sys.stderr)
        return 2
    return 1
