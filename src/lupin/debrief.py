"""Written debrief for one loop stop or one period.

A debrief is one markdown file. A loop stop writes one. The agent writes
periodic debriefs (`PERIODS`) on a timer. Facts come from GitHub (`gh`).
The ledger lists shared repository events. A claim marks an issue that one loop
is working on. A normal stop and a periodic debrief also read the ledger and
issue claims in Redis. A forced stop does not. Files stay on the machine that
wrote them. A periodic debrief also gets screenshots of its dashboard pages.
A stop debrief does not.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from urllib.parse import urlencode

import redis

from . import claims, ledger, roadmap, slots_redis
from .slots import CoordinatorUnreachable

GH_TIMEOUT = 30.0
DEBRIEF_TIME_LIMIT_S = 10.0  # Time limit for all gh calls in one debrief. Redis reads do not count.
LIST_LIMIT = "200"
REPO_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
STAMP_FORMAT = "%Y%m%d-%H%M%S"
PERIODS = {"6h": timedelta(hours=6), "24h": timedelta(hours=24), "7d": timedelta(days=7)}
_PERIOD_NAMES = "|".join(map(re.escape, PERIODS))
# Stop files are `<file name time>.md`. Periodic files are `<file name time>-<period>.md`.
FILE_RE = re.compile(rf"^\d{{8}}-\d{{6}}(?:-(?:{_PERIOD_NAMES}))?\.md$")
# Screenshot links in a periodic debrief. `_screenshots` writes this shape only.
SHOT_PATH_RE = re.compile(
    rf"\d{{8}}-\d{{6}}(?:-(?:{_PERIOD_NAMES}))?-screenshots/[A-Za-z0-9_-]+\.png"
)
# Image link: a GitHub attachment URL or a screenshot path. Group 2 is the target.
IMAGE_LINK_RE = re.compile(rf"!\[([^\]]*)\]\((https://[^)\s]+|{SHOT_PATH_RE.pattern})\)")
EVIDENCE_DIRS = ("docs/", "evidence/")
EVIDENCE_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
}
MAX_EVIDENCE_BYTES = 10 * 1024 * 1024
FAILING_CONCLUSIONS = {"FAILURE", "TIMED_OUT", "STARTUP_FAILURE"}
FAILING_STATES = {"FAILURE", "ERROR"}
CSS = ".debrief img{max-width:100%;height:auto}"
SHOT_BROWSER = "chromium"  # Headless browser command. It must be on PATH.
SHOT_WINDOW = "1200,1000"
SHOT_TIMEOUT_S = 60.0
SHOT_SERVER_WAIT_S = 10.0
# (file name, alt text, caption, dashboard path). `{query}` is the debrief's query string.
SHOTS = (
    ("debrief-list.png", "Debrief list",
     "The list of debriefs on the dashboard. Newest first.", "/debrief"),
    ("debrief.png", "Debrief page",
     "One periodic debrief, as the dashboard shows it.", "/debrief?{query}"),
)


class DebriefError(Exception):
    """A debrief cannot be written or read."""


class GhTimeout(DebriefError):
    """A gh call hit its time limit."""


def _gh(args: list[str], cwd: str | None = None, timeout: float = GH_TIMEOUT):
    try:
        proc = subprocess.run(
            ["gh", *args], cwd=cwd, capture_output=True, text=True,
            check=False, timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise GhTimeout(f"gh {' '.join(args[:2])} failed: {exc}") from exc
    except OSError as exc:
        raise DebriefError(f"gh {' '.join(args[:2])} failed: {exc}") from exc
    if proc.returncode != 0:
        raise DebriefError(
            f"gh {' '.join(args[:2])} exited {proc.returncode}: {proc.stderr.strip()}"
        )
    try:
        return json.loads(proc.stdout or "null")
    except json.JSONDecodeError as exc:
        raise DebriefError(f"gh {' '.join(args[:2])} sent bad JSON") from exc


class _TimeLimit:
    """Time left for the gh calls of one debrief. Redis reads do not use it."""

    def __init__(self, seconds: float):
        self.left = seconds

    def gh(self, args: list[str], cwd: str | None = None):
        """Run gh with the time left. Raise `GhTimeout` when none is left."""
        if self.left <= 0:
            raise GhTimeout(f"gh {' '.join(args[:2])} not run: time limit reached")
        started = time.monotonic()
        try:
            return _gh(args, cwd=cwd, timeout=min(GH_TIMEOUT, self.left))
        finally:
            self.left -= time.monotonic() - started


def _list(time_limit: _TimeLimit, args: list[str]) -> tuple[list[dict] | None, str]:
    """Return (items, reason). Items is None when the call failed. Reason says why."""
    try:
        return time_limit.gh(args + ["--limit", LIST_LIMIT]) or [], ""
    except GhTimeout:
        return None, "time limit reached"
    except DebriefError:
        return None, "gh error"


def _cut(section: str, name: str, items: list | None, reason: str) -> list[str]:
    if items is None:
        return [f"- Not collected: {reason}. {section}: {name} list."]
    if len(items) < int(LIST_LIMIT):
        return []
    return [f"- {section}: {name} list cut at {LIST_LIMIT} items. Some items may be missing."]


def _parse(value) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    try:
        return parsed.astimezone(timezone.utc)
    except OverflowError:
        # Years 1 and 9999 can leave the datetime range when shifted to UTC.
        return None


def _iso(when: datetime) -> str:
    return when.strftime("%Y-%m-%dT%H:%M:%SZ")


def _in_window(value, start: datetime, end: datetime) -> bool:
    when = _parse(value)
    return when is not None and start <= when <= end


def _well_formed(event: dict) -> bool:
    """Return False when a field the debrief reads has the wrong type."""
    for key in ("next", "decisions"):
        texts = event.get(key, [])
        if not isinstance(texts, list) or not all(isinstance(text, str) for text in texts):
            return False
    return True


def _failing_checks(pr: dict) -> list[str]:
    names = []
    for check in pr.get("statusCheckRollup") or []:
        if check.get("conclusion") in FAILING_CONCLUSIONS or check.get("state") in FAILING_STATES:
            names.append(check.get("name") or check.get("context") or "unnamed check")
    return names


def _attachments(text: str | None) -> list[str]:
    found = []
    for match in roadmap.IMAGE.finditer(text or ""):
        attachment = roadmap.github_attachment_id(match.group(2))
        if attachment and attachment not in found:
            found.append(attachment)
    return found


def build_markdown(
    full_name: str,
    start: datetime,
    end: datetime,
    *,
    forced: bool = False,
    events: list[dict] | None = None,
    ledger_note: str | None = None,
    claimed: set[int] | None = None,
    time_limit: _TimeLimit | None = None,
    period: str | None = None,
) -> str:
    """Return the debrief for `full_name` over `[start, end]` as markdown.

    `events` is None when the ledger was not read. `claimed` is None when
    claims were not read. A forced stop uses GitHub facts only. A section
    whose gh call failed or timed out is marked as not collected. `period` is set for a
    periodic debrief. It replaces the Stop line.
    """
    day = start.strftime("%Y-%m-%d")
    repo = ["--repo", full_name]
    time_limit = _TimeLimit(DEBRIEF_TIME_LIMIT_S) if time_limit is None else time_limit
    window_events = (
        [] if forced or events is None
        else [
            e for e in events
            if _well_formed(e) and _in_window(e.get("timestamp"), start, end)
        ]
    )
    lines = [f"# Debrief: {full_name}", "", "## Window", f"- From: {_iso(start)}", f"- To: {_iso(end)}"]
    if period is None:
        lines.append("- Stop: forced, no handoff" if forced else "- Stop: normal")
    else:
        lines.append(f"- Period: {period}")
    lines.append("")

    merged_all, merged_reason = _list(time_limit, ["pr", "list", *repo, "--state", "merged",
                                "--search", f"merged:>={day}",
                                "--json", "number,title,mergedAt,mergeCommit"])
    merged = [pr for pr in merged_all or [] if _in_window(pr.get("mergedAt"), start, end)]
    closed_all, closed_reason = _list(time_limit, ["issue", "list", *repo, "--state", "closed",
                                "--search", f"closed:>={day}",
                                "--json", "number,title,closedAt"])
    closed = [issue for issue in closed_all or [] if _in_window(issue.get("closedAt"), start, end)]
    lines.append("## Shipped")
    for pr in merged:
        commit = (pr.get("mergeCommit") or {}).get("oid", "")[:7]
        lines.append(f"- PR #{pr['number']}: {pr['title']} (merged {pr['mergedAt']}, commit {commit})")
    for issue in closed:
        lines.append(f"- Issue #{issue['number']}: {issue['title']} (closed {issue['closedAt']})")
    if not merged and not closed and merged_all is not None and closed_all is not None:
        lines.append("- None.")
    lines += _cut("Shipped", "merged PR", merged_all, merged_reason)
    lines += _cut("Shipped", "closed issue", closed_all, closed_reason)
    lines.append("")

    lines.append("## Follow-up tasks")
    if forced:
        lines.append("- Forced stop. Ledger not read.")
    elif events is None:
        lines.append(f"- {ledger_note or 'No ledger read.'}")
    else:
        for event in window_events:
            for text in event.get("next", []):
                prefix = f"#{event['issue']}: " if event.get("issue") else ""
                lines.append(f"- {prefix}{text}")
        if not any(event.get("next") for event in window_events):
            lines.append("- None.")
    lines.append("")

    failing = []
    open_prs, open_reason = _list(time_limit, ["pr", "list", *repo, "--state", "open",
                              "--json", "number,title,statusCheckRollup"])
    for pr in open_prs or []:
        names = _failing_checks(pr)
        if names:
            failing.append((pr, names))
    blocked, blocked_reason = _list(time_limit, ["issue", "list", *repo, "--state", "open",
                                                 "--label", "blocked", "--json", "number,title"])
    decisions = [text for event in window_events for text in event.get("decisions", [])]
    risk = []
    for pr, names in failing:
        risk.append(f"- PR #{pr['number']}: {pr['title']} (failing: {', '.join(names)})")
    for issue in blocked or []:
        risk.append(f"- Issue #{issue['number']}: {issue['title']} (labelled blocked)")
    for text in decisions:
        risk.append(f"- Decision: {text}")
    decisions_read = not forced and events is not None
    lines.append("## Risk")
    lines.append("Derived from GitHub facts and ledger decisions. Not checked against real ledger rows.")
    lines += risk or (
        ["- None."] if decisions_read and open_prs is not None and blocked is not None else []
    )
    if not decisions_read:
        why = "forced stop" if forced else (ledger_note or "no ledger read")
        lines.append(f"- Decisions not collected: {why}.")
    lines += _cut("Risk", "open PR", open_prs, open_reason)
    lines += _cut("Risk", "blocked issue", blocked, blocked_reason)
    lines.append("")

    ready, ready_reason = _list(time_limit, ["issue", "list", *repo, "--state", "open",
                                             "--label", "ready", "--json", "number,title"])
    lines.append("## Opportunities")
    if forced:
        lines.append("Forced stop. Claims not read.")
    elif claimed is None:
        lines.append("Claims not read. Listed issues may already be claimed.")
    unclaimed = [issue for issue in ready or [] if not claimed or issue["number"] not in claimed]
    for issue in unclaimed:
        lines.append(f"- Issue #{issue['number']}: {issue['title']}")
    if not unclaimed and ready is not None:
        lines.append("- None.")
    lines += _cut("Opportunities", "ready issue", ready, ready_reason)
    lines.append("")

    lines.append("## Evidence")
    seen = []
    cut = []
    complete = True
    for kind, args in (
        ("Issue", ["issue", "list", *repo, "--state", "all",
                   "--search", f"updated:>={day}",
                   "--json", "number,title,body,comments,updatedAt"]),
        ("PR", ["pr", "list", *repo, "--state", "all",
                "--search", f"updated:>={day}",
                "--json", "number,title,body,comments,updatedAt"]),
    ):
        items, reason = _list(time_limit, args)
        cut += _cut("Evidence", kind, items, reason)
        complete = complete and items is not None
        for item in items or []:
            texts = []
            if _in_window(item.get("updatedAt"), start, end):
                texts.append(item.get("body"))
            texts += [c.get("body") for c in item.get("comments") or []
                      if _in_window(c.get("createdAt"), start, end)]
            for text in texts:
                for attachment in _attachments(text):
                    if attachment in seen:
                        continue
                    seen.append(attachment)
                    url = f"https://github.com/user-attachments/assets/{attachment}"
                    lines.append(f"- {kind} #{item['number']}: ![{kind} {item['number']} image]({url})")
    if not seen and complete:
        lines.append("- None.")
    lines += cut
    lines.append("")
    return "\n".join(lines)


def write_debrief(
    root: Path,
    repo: str,
    checkout: Path,
    started_at: str | None,
    *,
    forced: bool = False,
) -> Path:
    """Write one debrief under `root/debriefs/<repo>/` and return its path.

    Raises `DebriefError` or a Redis error. Nothing is written then. The gh
    calls share one time limit. If a gh call fails or times out, the file marks
    that section as not collected.
    """
    if not REPO_RE.fullmatch(repo or ""):
        raise DebriefError(f"invalid repo name {repo!r}")
    start = _parse(started_at)
    if start is None:
        raise DebriefError(f"loop state for {repo} has no start time")
    if not checkout.is_dir():
        raise DebriefError(f"no checkout at {checkout}")
    return _write(root, repo, checkout, start, datetime.now(timezone.utc), forced=forced)


def _write(
    root: Path,
    repo: str,
    checkout: Path,
    start: datetime,
    end: datetime,
    *,
    forced: bool,
    period: str | None = None,
) -> Path:
    """Build the debrief for `[start, end]` and write it. Return its path.

    Raises `DebriefError` or a Redis error. Nothing is written then.
    The gh calls share one time limit. If a section's gh call fails or
    times out, the file marks that section as not collected. `period` adds the
    period to the file name. A periodic file also gets screenshots.
    """
    time_limit = _TimeLimit(DEBRIEF_TIME_LIMIT_S)
    view = time_limit.gh(["repo", "view", "--json", "nameWithOwner"], cwd=str(checkout))
    full_name = view["nameWithOwner"]

    events = None
    claimed = None
    ledger_note = None
    if not forced:
        # Both Redis reads use one debrief client. Each Redis read uses the
        # debrief client timeouts (DEBRIEF_TIMEOUT_S).
        client = slots_redis.debrief_client(None, None)
        try:
            events = ledger.read_events(full_name, limit=None, client=client)
        except (CoordinatorUnreachable, redis.exceptions.RedisError, ValueError, KeyError) as exc:
            ledger_note = f"Ledger unavailable: {exc.__cause__ or exc}"
        try:
            held = claims.claims_for([full_name], client=client)
            claimed = {
                int(key.rsplit("#", 1)[1]) for key in held if key.startswith(f"{full_name}#")
            }
        except (CoordinatorUnreachable, redis.exceptions.RedisError, ValueError):
            claimed = None

    text = build_markdown(
        full_name, start, end, forced=forced, events=events,
        ledger_note=ledger_note, claimed=claimed, time_limit=time_limit, period=period,
    )
    folder = root / "debriefs" / repo
    folder.mkdir(parents=True, exist_ok=True, mode=0o750)
    stamp = end.strftime(STAMP_FORMAT)
    path = folder / (f"{stamp}.md" if period is None else f"{stamp}-{period}.md")
    _store(path, text)
    if period is not None:
        # The screenshots show the debrief page. That page must exist first.
        _store(path, text + _screenshots(root, repo, path))
    return path


def _store(path: Path, text: str) -> None:
    """Write `text` to `path` in one step."""
    fd, temporary = tempfile.mkstemp(prefix=".debrief.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.chmod(temporary, 0o640)
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _serve(root: Path, port: int) -> subprocess.Popen:
    """Start the dashboard on `port`, reading debriefs from `root`. Return once it answers."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "lupin.serve", "--port", str(port)],
        env={**os.environ, "LUPIN_LOOP_STATE_DIR": str(root)},
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + SHOT_SERVER_WAIT_S
    while proc.poll() is None and time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=1):
                return proc
        except OSError:
            time.sleep(0.2)
    proc.kill()
    proc.wait()
    raise DebriefError("dashboard server did not start")


def _shot(url: str, target: Path) -> str | None:
    """Save a screenshot of `url` at `target`. Return None, or the reason it failed."""
    argv = [
        SHOT_BROWSER, "--headless", "--disable-gpu", "--hide-scrollbars",
        f"--window-size={SHOT_WINDOW}", f"--screenshot={target}", url,
    ]
    try:
        # A new session gives the browser its own process group.
        proc = subprocess.Popen(
            argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
        )
    except OSError as exc:
        return f"browser did not run ({exc.strerror})"
    try:
        code = proc.wait(timeout=SHOT_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        # Kill the browser's child processes too.
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()
        return "browser timed out"
    if code or not target.is_file() or target.stat().st_size == 0:
        return f"browser exited {code} without a screenshot"
    return None


def _page_error(url: str) -> str | None:
    """Return None when `url` answers 200. Otherwise return the reason."""
    try:
        with urllib.request.urlopen(url, timeout=SHOT_SERVER_WAIT_S) as resp:
            status = resp.status
    except urllib.error.HTTPError as exc:
        status = exc.code
    except OSError as exc:
        return f"dashboard did not answer ({exc})"
    return None if status == 200 else f"dashboard returned HTTP {status}"


def _capture(root: Path, repo: str, name: str, folder: Path) -> dict[str, str]:
    """Save the dashboard screenshots in `folder`. Return {file name: reason} for each one missing."""
    if shutil.which(SHOT_BROWSER) is None:
        return {file: f"browser command {SHOT_BROWSER!r} not found on PATH" for file, *_ in SHOTS}
    folder.mkdir(parents=True, exist_ok=True, mode=0o750)
    port = _free_port()
    try:
        server = _serve(root, port)
    except (DebriefError, OSError) as exc:
        return {file: str(exc) for file, *_ in SHOTS}
    query = urlencode({"repo": repo, "file": name})
    missing = {}
    try:
        for file, _alt, _caption, path in SHOTS:
            url = f"http://127.0.0.1:{port}{path.format(query=query)}"
            # A page that is not 200 gives no screenshot of the debrief.
            reason = _page_error(url) or _shot(url, folder / file)
            if reason:
                missing[file] = reason
    finally:
        server.kill()
        server.wait()
    return missing


def _screenshots(root: Path, repo: str, path: Path) -> str:
    """Save the screenshots for the periodic debrief at `path`. Return its markdown section.

    A screenshot that is not saved is named in the section, with its reason.
    """
    stem = path.stem
    try:
        missing = _capture(root, repo, path.name, path.parent / f"{stem}-screenshots")
    except OSError as exc:
        missing = {file: f"could not save files ({exc})" for file, *_ in SHOTS}
    lines = ["", "## Screenshots"]
    for file, alt, caption, _path in SHOTS:
        if file in missing:
            lines.append(f"- Not captured: {alt} ({file}). Reason: {missing[file]}.")
        else:
            lines += ["", f"![{alt}]({stem}-screenshots/{file})", "", caption]
    return "\n".join(lines) + "\n"


def _period_ends(root: Path, repo: str, period: str) -> list[datetime]:
    """Return the end time of each `period` debrief for `repo`."""
    if period not in PERIODS:
        raise DebriefError(f"unknown period {period!r}")
    if not REPO_RE.fullmatch(repo or ""):
        raise DebriefError(f"invalid repo name {repo!r}")
    ends = []
    for path in (root / "debriefs" / repo).glob(f"*-{period}.md"):
        if FILE_RE.fullmatch(path.name) and path.is_file():
            stamp = path.name[:-len(f"-{period}.md")]
            try:
                ends.append(datetime.strptime(stamp, STAMP_FORMAT).replace(tzinfo=timezone.utc))
            except ValueError:
                pass  # Not a real date, such as month 13. Skip the file.
    return ends


def last_period_end(root: Path, repo: str, period: str, now: datetime | None = None) -> datetime | None:
    """Return the end time of the newest `period` debrief for `repo`.

    Skip files that end after `now`. A clock that stepped back can leave such files.
    Return None when there is no such debrief.
    """
    ends = _period_ends(root, repo, period)
    if now is not None:
        ends = [end for end in ends if end <= now]
    return max(ends, default=None)


def period_due(root: Path, repo: str, period: str, now: datetime) -> bool:
    """Return True when a `period` debrief is due.

    Return True also when one full period has passed since the last one.
    Return False when a period file ends within seven days after `now`.
    Such a file blocks the write until its end time has passed.
    A clock that stepped back can delay a write by up to seven days.
    """
    ends = _period_ends(root, repo, period)
    span = PERIODS[period]
    horizon = max(PERIODS.values())
    if any(now < end <= now + horizon for end in ends):
        return False
    last = last_period_end(root, repo, period, now)
    return last is None or now - last >= span


def write_period(
    root: Path, repo: str, checkout: Path, period: str, *, now: datetime | None = None,
) -> Path:
    """Write the `period` debrief for `repo` and return its path.

    The window starts at the last `period` debrief. When there is none, the
    window starts one period before `now`. The window ends at `now`.
    """
    end = datetime.now(timezone.utc) if now is None else now
    last = last_period_end(root, repo, period, end)
    start = end - PERIODS[period] if last is None else last
    if not checkout.is_dir():
        raise DebriefError(f"no checkout at {checkout}")
    return _write(root, repo, checkout, start, end, forced=False, period=period)


def _newest_first(item: tuple[str, str]) -> tuple[datetime, timedelta, str]:
    """Sort key: file name time, then period length. A stop file has no period, so it counts as zero."""
    repo, name = item
    try:
        when = datetime.strptime(name[:15], STAMP_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        when = datetime.min.replace(tzinfo=timezone.utc)  # Not a real date. Sort it last.
    return when, PERIODS.get(name[16:-3], timedelta(0)), repo


def list_debriefs(root: Path) -> list[tuple[str, str]]:
    """Return (repo, file name) for each debrief, newest first."""
    base = root / "debriefs"
    if not base.is_dir():
        return []
    found = []
    for folder in base.iterdir():
        if not REPO_RE.fullmatch(folder.name) or not folder.is_dir():
            continue
        for path in folder.iterdir():
            if FILE_RE.fullmatch(path.name) and path.is_file():
                found.append((folder.name, path.name))
    return sorted(found, key=_newest_first, reverse=True)


def read_debrief(root: Path, repo: str, name: str) -> str:
    """Return the markdown of one debrief. Raises `DebriefError` if absent."""
    if not REPO_RE.fullmatch(repo or "") or not FILE_RE.fullmatch(name or ""):
        raise DebriefError("no such debrief")
    path = root / "debriefs" / repo / name
    if not path.is_file():
        raise DebriefError("no such debrief")
    return path.read_text(encoding="utf-8")


def _image_src(target: str, repo: str) -> str | None:
    """Return the `src` for an image link. None when the link is not served."""
    if target.startswith("https://"):
        attachment = roadmap.github_attachment_id(target)
        return f"/image?id={attachment}" if attachment else None
    return "/debrief/shot?" + escape(urlencode({"repo": repo, "path": target}), quote=True)


def _inline(text: str, repo: str) -> str:
    parts = []
    position = 0
    for match in IMAGE_LINK_RE.finditer(text):
        parts.append(escape(text[position:match.start()]))
        src = _image_src(match.group(2), repo)
        if src:
            alt = escape(match.group(1), quote=True)
            parts.append(f"<img src='{src}' alt='{alt}' loading=lazy>")
        else:
            parts.append(escape(match.group(0)))
        position = match.end()
    parts.append(escape(text[position:]))
    return "".join(parts)


def render_html(markdown: str, repo: str) -> str:
    """Render the debrief markdown subset as HTML.

    Headings, bullets, paragraphs and images only. Images are GitHub
    attachments or this repo's screenshots. All other text is escaped.
    Links show as text.
    """
    out = []
    in_list = False
    for line in markdown.splitlines():
        if line.startswith("- "):
            if not in_list:
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{_inline(line[2:], repo)}</li>")
            continue
        if in_list:
            out.append("</ul>")
            in_list = False
        if line.startswith("# "):
            out.append(f"<h1>{_inline(line[2:], repo)}</h1>")
        elif line.startswith("## "):
            out.append(f"<h2>{_inline(line[3:], repo)}</h2>")
        elif line.strip():
            out.append(f"<p>{_inline(line, repo)}</p>")
    if in_list:
        out.append("</ul>")
    return "".join(out)


def read_screenshot(root: Path, repo: str, rel: str) -> tuple[bytes, str] | None:
    """Return (bytes, content type) for a screenshot of `repo`'s debriefs.

    `rel` must match `SHOT_PATH_RE`. Return None when it does not match,
    when the file is absent, or when it resolves outside the repo's folder.
    """
    if not REPO_RE.fullmatch(repo or "") or not SHOT_PATH_RE.fullmatch(rel or ""):
        return None
    folder = (root / "debriefs" / repo).resolve()
    try:
        target = (folder / rel).resolve(strict=True)
    except (OSError, RuntimeError):
        return None
    if not target.is_relative_to(folder) or not target.is_file():
        return None
    if target.stat().st_size > MAX_EVIDENCE_BYTES:
        return None
    return target.read_bytes(), "image/png"


def read_evidence(checkout: Path, rel: str) -> tuple[bytes, str] | None:
    """Return (bytes, content type) for an image under `docs/` or `evidence/`.

    Returns None for an absolute path, a `://` or `..` path, or another extension.
    Also None when the path is outside those folders or resolves outside the checkout.
    """
    if not rel or rel.startswith("/") or "://" in rel or ".." in rel or "\\" in rel or "\0" in rel:
        return None
    content_type = EVIDENCE_TYPES.get(Path(rel).suffix.lower())
    if content_type is None or not rel.startswith(EVIDENCE_DIRS):
        return None
    try:
        root = checkout.resolve(strict=True)
        target = (root / rel).resolve(strict=True)
    except (OSError, RuntimeError):
        return None
    if not target.is_relative_to(root):
        return None
    if not target.relative_to(root).as_posix().startswith(EVIDENCE_DIRS) or not target.is_file():
        return None
    if target.stat().st_size > MAX_EVIDENCE_BYTES:
        return None
    return target.read_bytes(), content_type
