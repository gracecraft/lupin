from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from lupin import loop_runtime


def test_systemd_run_uses_nixos_sudo_wrapper_when_not_on_path(monkeypatch, tmp_path):
    wrapper = tmp_path / "sudo"
    wrapper.touch()
    wrapper.chmod(0o755)
    monkeypatch.setattr(loop_runtime, "NIXOS_SUDO_WRAPPER", wrapper)
    monkeypatch.setenv("PATH", "")
    launched = []
    monkeypatch.setattr(
        loop_runtime,
        "_run",
        lambda argv, **kwargs: launched.append(argv) or (0, ""),
    )

    result = loop_runtime._systemd_run("widgets", "loop", ["lupin", "loop", "worker"])
    assert result == (0, "")

    assert launched[0][:3] == [str(wrapper), "-n", "systemd-run"]


def test_sudo_argv_uses_path_when_nixos_wrapper_is_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(loop_runtime, "NIXOS_SUDO_WRAPPER", tmp_path / "sudo")

    assert loop_runtime._sudo_argv("systemd-run") == ["sudo", "-n", "systemd-run"]


def test_loop_state_uses_herdr_agent_state_and_ids(monkeypatch):
    metadata = {
        "repo": "widgets",
        "platform": "claude",
        "session": "lupin-widgets-abc123",
        "started_at": "2025-01-02T03:04:05Z",
        "workspace_id": "workspace-1",
    }
    workspace = {
        "id": "workspace-1",
        "root_pane": {"pane_id": "pane-1", "workspace_id": "workspace-1"},
    }
    monkeypatch.setattr(loop_runtime, "_read_metadata", lambda repo: metadata)
    monkeypatch.setattr(loop_runtime, "_server_running", lambda session: True)
    monkeypatch.setattr(loop_runtime, "_find_workspace", lambda session, data: workspace)
    monkeypatch.setattr(
        loop_runtime,
        "_agent_for_workspace",
        lambda session, workspace_id: {"name": "lupin-loop", "agent_status": "blocked"},
    )

    state = loop_runtime.loop_state("widgets")

    assert state == {
        "repo": "widgets",
        "platform": "claude",
        "backend": "herdr",
        "state": "blocked",
        "session": "lupin-widgets-abc123",
        "workspace_id": "workspace-1",
        "pane_id": "pane-1",
        "since": "2025-01-02T03:04:05Z",
        "agent": "lupin-loop",
    }

def test_local_loops_marks_a_missing_running_workspace_for_attention(monkeypatch, tmp_path: Path):
    loops_dir = tmp_path / "herdr-loops"
    loops_dir.mkdir()
    path = loops_dir / "widgets.json"
    path.write_text(
        json.dumps({
            "repo": "widgets",
            "platform": "claude",
            "session": "lupin-widgets-abc123",
            "state": "running",
            "started_at": "2025-01-02T03:04:05Z",
            "workspace_id": "workspace-1",
            "pane_id": "pane-1",
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(loop_runtime, "LOOPS_DIR", loops_dir)
    monkeypatch.setattr(loop_runtime, "_server_running", lambda session: True)
    monkeypatch.setattr(loop_runtime, "_find_workspace", lambda session, metadata: None)

    loops = loop_runtime.local_loops()

    assert len(loops) == 1
    assert loops[0]["state"] == "needs_attention"
    assert loops[0]["workspace_id"] == "workspace-1"
    assert loops[0]["pane_id"] == "pane-1"
    assert json.loads(path.read_text(encoding="utf-8"))["state"] == "needs_attention"




@pytest.mark.parametrize("other_workspaces, stop_session", [([{"id": "other"}], False), ([], True)])
def test_stop_closes_only_loop_workspace_and_stops_empty_session(
    monkeypatch, tmp_path: Path, other_workspaces: list[dict], stop_session: bool
):
    metadata = {
        "repo": "widgets",
        "platform": "claude",
        "session": "lupin-widgets-abc123",
        "state": "running",
        "workspace_id": "workspace-1",
        "pane_id": "pane-1",
    }
    monkeypatch.setattr(loop_runtime, "STATE_DIR", tmp_path)
    actions = []
    writes = []
    monkeypatch.setattr(loop_runtime, "_read_metadata", lambda repo: dict(metadata))
    monkeypatch.setattr(loop_runtime, "_start_server_for_read", lambda repo, data: data["session"])
    monkeypatch.setattr(loop_runtime, "_find_workspace", lambda session, data: {"id": "workspace-1"})
    monkeypatch.setattr(loop_runtime, "_save_report", lambda repo, session, workspace: tmp_path / "report.log")
    monkeypatch.setattr(loop_runtime, "_workspace_id", lambda workspace: workspace["id"])
    monkeypatch.setattr(loop_runtime, "_herdr_json", lambda session, *args: actions.append((session, args)) or {})
    monkeypatch.setattr(loop_runtime, "_workspaces", lambda session: other_workspaces)
    monkeypatch.setattr(loop_runtime, "_write_metadata", lambda repo, value: writes.append(dict(value)))

    def run(argv, **kwargs):
        if argv[:3] == ["systemctl", "is-active", loop_runtime.unit_name("widgets") + ".service"]:
            return 0, "active"
        if argv[-4:] == [
            "-n",
            "systemctl",
            "stop",
            f"{loop_runtime.unit_name('widgets')}.service",
        ]:
            actions.append(("systemd", tuple(argv[-3:])))
            return 0, ""
        raise AssertionError(argv)

    monkeypatch.setattr(loop_runtime, "_run", run)

    result = loop_runtime.stop_loop("widgets")

    assert result == "stopped widgets; report saved to " + str(tmp_path / "report.log")
    assert ("lupin-widgets-abc123", ("workspace", "close", "workspace-1")) in actions
    assert any(item[0] == "systemd" for item in actions)
    assert any(item == (None, ("session", "stop", "lupin-widgets-abc123", "--json")) for item in actions) is stop_session
    assert writes[-1]["state"] == "stopped"
    assert writes[-1]["workspace_id"] is None


def test_local_loops_keeps_unknown_state_when_herdr_is_down(monkeypatch, tmp_path: Path):
    loops_dir = tmp_path / "herdr-loops"
    loops_dir.mkdir()
    (loops_dir / "widgets.json").write_text(
        '{"repo":"widgets","platform":"omp","session":"lupin-widgets-abc123",'
        '"state":"running","started_at":"2025-01-02T03:04:05Z"}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(loop_runtime, "LOOPS_DIR", loops_dir)
    monkeypatch.setattr(
        loop_runtime,
        "loop_state",
        lambda repo: {"repo": repo, "backend": "herdr", "state": "unknown", "session": "lupin-widgets-abc123"},
    )

    assert loop_runtime.local_loops() == [
        {
            "repo": "widgets",
            "platform": "omp",
            "state": "unknown",
            "since": "2025-01-02T03:04:05Z",
            "backend": "herdr",
            "session": "lupin-widgets-abc123",
            "workspace_id": None,
            "pane_id": None,
        }
    ]


def test_attach_uses_verified_herdr_remote_session():
    session = loop_runtime.session_name("widgets")
    assert loop_runtime.attach_argv("widgets", "mini", "jesus", "ghosta@mini.local") == [
        loop_runtime.HERDR, "--remote", "ghosta@mini.local", "--session", session
    ]


@pytest.mark.parametrize("expression", ["daily\nOnBootSec=0", "daily\x7f"])
def test_schedule_rejects_systemd_control_characters(
    monkeypatch, tmp_path: Path, expression: str
):
    monkeypatch.setattr(loop_runtime, "STATE_DIR", tmp_path)
    monkeypatch.setattr(loop_runtime, "_run", lambda *args, **kwargs: pytest.fail("systemd was called"))

    with pytest.raises(loop_runtime.LoopError, match="invalid calendar expression"):
        loop_runtime.schedule_local(["cal", expression])

    assert not (tmp_path / "timer-dropin.d" / "override.conf").exists()

@pytest.mark.parametrize(
    ("args", "settings"),
    [
        (
            ["first", "+2h", "every", "1h"],
            "OnActiveSec=2h\nOnUnitActiveSec=1h\nAccuracySec=1s\n",
        ),
        (
            ["first", "tomorrow 09:00", "every", "1h"],
            "OnCalendar=tomorrow 09:00\nOnUnitActiveSec=1h\nAccuracySec=1s\n",
        ),
        (["cal", "daily"], "OnCalendar=daily\nAccuracySec=1s\n"),
    ],
)
def test_schedule_replaces_previous_timer_triggers(
    monkeypatch, tmp_path: Path, args: list[str], settings: str
):
    monkeypatch.setattr(loop_runtime, "STATE_DIR", tmp_path)
    monkeypatch.setattr(loop_runtime, "_run", lambda *args, **kwargs: (0, ""))

    assert loop_runtime.schedule_local(args) == "updated delegation-loop.timer"

    override = (tmp_path / "timer-dropin.d" / "override.conf").read_text(encoding="utf-8")
    reset = (
        "[Timer]\n"
        "OnCalendar=\n"
        "OnActiveSec=\n"
        "OnBootSec=\n"
        "OnStartupSec=\n"
        "OnUnitActiveSec=\n"
        "OnUnitInactiveSec=\n"
    )
    assert override == reset + settings


def test_schedule_once_rejects_empty_repo_list_before_systemd(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(loop_runtime, "STATE_DIR", tmp_path)
    monkeypatch.setattr(loop_runtime, "_run", lambda *args, **kwargs: pytest.fail("systemd ran"))

    with pytest.raises(loop_runtime.LoopError, match="at least one repo"):
        loop_runtime.schedule_once("+2h", [])

    assert not (tmp_path / "once").exists()


def test_schedule_once_keeps_omp_provider_and_model(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(loop_runtime, "STATE_DIR", tmp_path)
    monkeypatch.setattr(loop_runtime, "_event_time", lambda when: ("2h", True))
    monkeypatch.setattr(loop_runtime, "_run", lambda *args, **kwargs: (0, ""))

    assert loop_runtime.schedule_once(
        "+2h", ["widgets"], platform="omp", provider="openai", model="openai/gpt-5.2"
    ) == "scheduled widgets for +2h"

    schedule, = (tmp_path / "once").glob("*.json")
    entry = json.loads(schedule.read_text(encoding="utf-8"))
    assert (entry["platform"], entry["provider"], entry["model"]) == (
        "omp", "openai", "openai/gpt-5.2"
    )



def test_once_fire_continues_after_one_repo_quota_error(monkeypatch, tmp_path, capsys):
    schedule_dir = tmp_path / "once"
    schedule_dir.mkdir()
    identifier = "a" * 16
    schedule = schedule_dir / f"{identifier}.json"
    schedule.write_text(json.dumps({"repos": ["widgets", "gizmos"]}), encoding="utf-8")
    monkeypatch.setattr(loop_runtime, "STATE_DIR", tmp_path)
    attempted = []

    def start(repo, **kwargs):
        attempted.append(repo)
        if repo == "widgets":
            raise loop_runtime.LoopError("quota snapshot is stale")
        return True, "started gizmos"

    monkeypatch.setattr(loop_runtime, "start_loop", start)

    results = loop_runtime.once_fire(identifier)

    assert attempted == ["widgets", "gizmos"]
    assert results == [
        "could not start loop for widgets: quota snapshot is stale",
        "started gizmos",
    ]
    assert "quota snapshot is stale" in capsys.readouterr().err
    assert not schedule.exists()


def test_repo_catalog_includes_repo_without_delegation_doc(monkeypatch, tmp_path: Path):
    code_dir = tmp_path / "code"
    (code_dir / "widgets").mkdir(parents=True)
    monkeypatch.setattr(loop_runtime, "CODE_DIR", code_dir)
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {})

    assert loop_runtime.repo_catalog() == [
        {
            "repo": "widgets",
            "loopable": True,
            "has_doc": False,
            "state": "disabled",
            "platform": "claude",
        }
    ]


def test_start_loop_skips_missing_checkout(monkeypatch, tmp_path: Path):
    code_dir = tmp_path / "code"
    monkeypatch.setattr(loop_runtime, "CODE_DIR", code_dir)
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {})
    monkeypatch.setattr(
        loop_runtime, "_run",
        lambda *args, **kwargs: pytest.fail("systemd must not run without a checkout"),
    )

    started, message = loop_runtime.start_loop("widgets")

    assert not started
    assert message == f"skip widgets: no checkout at {code_dir / 'widgets'}"


def test_start_loop_starts_without_delegation_doc_and_skips_duplicate(
    monkeypatch, tmp_path: Path, make_checkout
):
    state_dir = tmp_path / "state"
    code_dir = tmp_path / "code"
    make_checkout(code_dir / "widgets")
    monkeypatch.setattr(loop_runtime, "STATE_DIR", state_dir)
    monkeypatch.setattr(loop_runtime, "LOOPS_DIR", state_dir / "herdr-loops")
    monkeypatch.setattr(loop_runtime, "REPOS_FILE", state_dir / "repos")
    monkeypatch.setattr(loop_runtime, "CODE_DIR", code_dir)
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {})
    monkeypatch.setattr(loop_runtime, "_session_info", lambda session: None)

    def run(argv, **kwargs):
        if argv[0] == "systemctl":
            return 3, "inactive"
        if argv[0] == "tmux":
            return 1, "no session"
        raise AssertionError(argv)

    monkeypatch.setattr(loop_runtime, "_run", run)
    launches = []

    def systemd_run(repo, kind, command, **kwargs):
        launches.append((repo, kind, command, kwargs))
        return 0, ""

    monkeypatch.setattr(loop_runtime, "_systemd_run", systemd_run)

    started, message = loop_runtime.start_loop(
        "widgets", platform="omp", provider="openai", model="openai/gpt-5.2",
        note="review",
    )

    assert started
    assert "Herdr session" in message
    assert len(launches) == 1
    repo, kind, command, options = launches[0]
    assert repo == "widgets"
    assert kind == "loop"
    metadata = loop_runtime._read_metadata("widgets")
    assert metadata["state"] == "starting"
    assert metadata["platform"] == "omp"
    assert metadata["provider"] == "openai"
    assert metadata["model"] == "openai/gpt-5.2"
    prompt = Path(metadata["prompt_file"]).read_text(encoding="utf-8")
    assert prompt.endswith("review\n")
    assert command[1:] == [
        "loop",
        "worker",
        "--repo",
        "widgets",
        "--platform",
        "omp",
        "--session",
        loop_runtime.session_name("widgets"),
        "--prompt-file",
        metadata["prompt_file"],
    ]
    assert options["claude_limits"] is False

    started, message = loop_runtime.start_loop("widgets", platform="omp")

    assert not started
    assert message.startswith("skip widgets:")
    assert len(launches) == 1


def test_launch_agent_finds_the_pane_when_workspace_list_has_no_root_pane(monkeypatch, tmp_path: Path):
    # Herdr 0.9.3 `workspace list` rows carry `workspace_id` but no `root_pane`.
    prompt_file = tmp_path / "prompt"
    prompt_file.write_text("finish the handoff\n", encoding="utf-8")
    session = "lupin-widgets-abc123"
    calls = []
    answers = {
        ("workspace", "list"): {"workspaces": [
            {"workspace_id": "w1", "label": "widgets", "pane_count": 1},
        ]},
        ("pane", "list"): {"panes": [
            {"pane_id": "w9:p1", "workspace_id": "w9"},
            {"pane_id": "w1:p1", "workspace_id": "w1"},
        ]},
    }
    monkeypatch.setattr(loop_runtime, "_herdr_json", lambda got_session, *args: answers.get(args, {}))
    monkeypatch.setattr(loop_runtime, "_run", lambda argv, **kwargs: calls.append(argv) or (0, ""))
    monkeypatch.setattr(loop_runtime, "_wait_agent", lambda got_session, workspace_id: 0)

    assert loop_runtime.launch_agent(
        "widgets", session, "w1", "claude", str(prompt_file), resume=False
    ) == 0

    assert calls[0][calls[0].index("--pane") + 1] == "w1:p1"


def test_launch_agent_sends_the_prompt_without_newlines(monkeypatch, tmp_path: Path):
    # Herdr 0.9.3 rejects any newline in an agent argument.
    prompt_file = tmp_path / "prompt"
    prompt_file.write_text("first line\n\nsecond  line\n", encoding="utf-8")
    workspace = {"id": "w1", "root_pane": {"pane_id": "w1:p1", "workspace_id": "w1"}}
    calls = []
    monkeypatch.setattr(loop_runtime, "_run", lambda argv, **kwargs: calls.append(argv) or (0, ""))
    monkeypatch.setattr(loop_runtime, "_wait_agent", lambda got_session, workspace_id: 0)

    loop_runtime._launch_agent("lupin-widgets-abc123", "claude", workspace, str(prompt_file), resume=False)

    assert calls[0][-1] == "first line second line"


def test_launch_agent_uses_herdr_start_api(monkeypatch, tmp_path: Path):
    prompt_file = tmp_path / "prompt"
    prompt_file.write_text("finish the handoff\n", encoding="utf-8")
    session = "lupin-widgets-abc123"
    workspace = {
        "id": "workspace-1",
        "root_pane": {"pane_id": "pane-1", "workspace_id": "workspace-1"},
    }
    calls = []
    waited = []
    monkeypatch.setattr(
        loop_runtime,
        "_run",
        lambda argv, **kwargs: calls.append((argv, kwargs)) or (0, ""),
    )

    def wait_agent(got_session, workspace_id):
        waited.append((got_session, workspace_id))
        return 0

    monkeypatch.setattr(loop_runtime, "_wait_agent", wait_agent)

    assert loop_runtime._launch_agent(
        session, "claude", workspace, str(prompt_file), resume=True
    ) == 0

    argv, options = calls[0]
    assert argv == [
        loop_runtime.HERDR,
        "--session",
        session,
        "agent",
        "start",
        loop_runtime.AGENT_NAME,
        "--kind",
        "claude",
        "--pane",
        "pane-1",
        "--timeout",
        "300000",
        "--",
        "--dangerously-skip-permissions",
        "--continue",
        "finish the handoff",
    ]
    assert options["timeout"] == 310.0
    assert options["env"]["HERDR_ENV"] == "1"
    assert waited == [(session, "workspace-1")]
def test_launch_agent_passes_omp_provider_and_model(monkeypatch, tmp_path: Path):
    prompt_file = tmp_path / "prompt"
    prompt_file.write_text("finish the handoff\n", encoding="utf-8")
    session = "lupin-widgets-abc123"
    workspace = {
        "id": "workspace-1",
        "root_pane": {"pane_id": "pane-1", "workspace_id": "workspace-1"},
    }
    calls = []
    monkeypatch.setattr(loop_runtime, "_run", lambda argv, **kwargs: calls.append(argv) or (0, ""))
    monkeypatch.setattr(loop_runtime, "_wait_agent", lambda got_session, workspace_id: 0)

    assert loop_runtime._launch_agent(
        session, "omp", workspace, str(prompt_file), resume=False,
        provider="opencode-go", model="opencode-go/step-5-preview-free:xhigh",
    ) == 0

    assert calls[0][-5:] == [
        "--provider", "opencode-go",
        "--model", "opencode-go/step-5-preview-free:xhigh",
        "finish the handoff",
    ]


@pytest.mark.parametrize("resume", [False, True])
def test_launch_agent_starts_claude_with_skip_permissions(monkeypatch, tmp_path: Path, resume: bool):
    prompt_file = tmp_path / "prompt"
    prompt_file.write_text("finish the handoff\n", encoding="utf-8")
    workspace = {"id": "w1", "root_pane": {"pane_id": "w1:p1", "workspace_id": "w1"}}
    calls = []
    monkeypatch.setattr(loop_runtime, "_run", lambda argv, **kwargs: calls.append(argv) or (0, ""))
    monkeypatch.setattr(loop_runtime, "_wait_agent", lambda got_session, workspace_id: 0)

    loop_runtime._launch_agent(
        "lupin-widgets-abc123", "claude", workspace, str(prompt_file), resume=resume
    )

    argv = calls[0]
    assert argv[argv.index("--") + 1] == "--dangerously-skip-permissions"
    assert ("--continue" in argv) == resume
    assert argv[-1] == "finish the handoff"


def test_launch_agent_omp_does_not_skip_permissions(monkeypatch, tmp_path: Path):
    prompt_file = tmp_path / "prompt"
    prompt_file.write_text("finish the handoff\n", encoding="utf-8")
    workspace = {"id": "w1", "root_pane": {"pane_id": "w1:p1", "workspace_id": "w1"}}
    calls = []
    monkeypatch.setattr(loop_runtime, "_run", lambda argv, **kwargs: calls.append(argv) or (0, ""))
    monkeypatch.setattr(loop_runtime, "_wait_agent", lambda got_session, workspace_id: 0)

    loop_runtime._launch_agent(
        "lupin-widgets-abc123", "omp", workspace, str(prompt_file), resume=False
    )

    assert "--dangerously-skip-permissions" not in calls[0]


@pytest.mark.parametrize(
    "platform, provider, model",
    [
        ("claude", "openai", None),
        ("omp", "unsupported", None),
        ("omp", None, "bad\nmodel"),
    ],
)
def test_validate_omp_options_rejects_invalid_options(platform, provider, model):
    with pytest.raises(loop_runtime.LoopError):
        loop_runtime.validate_omp_options(platform, provider, model)



def test_start_loop_refuses_an_open_legacy_session(monkeypatch, tmp_path: Path):
    state_dir = tmp_path / "state"
    code_dir = tmp_path / "code"
    repo_dir = code_dir / "widgets"
    (repo_dir / "docs").mkdir(parents=True)
    (repo_dir / "docs" / "delegation-loop.md").touch()
    monkeypatch.setattr(loop_runtime, "STATE_DIR", state_dir)
    monkeypatch.setattr(loop_runtime, "LOOPS_DIR", state_dir / "herdr-loops")
    monkeypatch.setattr(loop_runtime, "REPOS_FILE", state_dir / "repos")
    monkeypatch.setattr(loop_runtime, "CODE_DIR", code_dir)
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {})
    monkeypatch.setattr(loop_runtime, "_session_info", lambda session: None)

    def run(argv, **kwargs):
        if argv[0] == "systemctl":
            return 3, "inactive"
        if argv[0] == "tmux":
            return 0, ""
        raise AssertionError(argv)

    monkeypatch.setattr(loop_runtime, "_run", run)
    monkeypatch.setattr(
        loop_runtime,
        "_systemd_run",
        lambda *args, **kwargs: pytest.fail("a legacy session must block Lupin"),
    )

    started, message = loop_runtime.start_loop("widgets")

    assert not started
    assert "legacy tmux loop-widgets is open" in message


def test_send_loop_types_the_text_then_presses_enter(monkeypatch):
    calls = []
    monkeypatch.setattr(loop_runtime, "_read_metadata", lambda repo: {"session": "lupin-widgets-abc123"})
    monkeypatch.setattr(loop_runtime, "_server_running", lambda session: True)
    monkeypatch.setattr(
        loop_runtime, "_find_workspace",
        lambda session, metadata: {"id": "w1", "root_pane": {"pane_id": "w1:p1", "workspace_id": "w1"}},
    )
    monkeypatch.setattr(loop_runtime, "_herdr", lambda session, *args, **kw: calls.append((session, args)) or "")

    loop_runtime.send_loop("widgets", "yes, keep going")

    assert calls == [
        ("lupin-widgets-abc123", ("pane", "send-text", "w1:p1", "yes, keep going")),
        ("lupin-widgets-abc123", ("pane", "send-keys", "w1:p1", "Enter")),
    ]


def test_send_loop_refuses_a_loop_that_is_not_running(monkeypatch):
    monkeypatch.setattr(loop_runtime, "_read_metadata", lambda repo: {"session": "lupin-widgets-abc123"})
    monkeypatch.setattr(loop_runtime, "_server_running", lambda session: False)

    with pytest.raises(loop_runtime.LoopError):
        loop_runtime.send_loop("widgets", "hello")


def _stop_harness(monkeypatch, tmp_path: Path, *, agent: dict | None, prompt_error: str | None = None):
    """Patch stop_loop's Herdr and systemd calls; return the ordered action list."""
    metadata = {
        "repo": "widgets",
        "platform": "claude",
        "session": "lupin-widgets-abc123",
        "state": "running",
        "workspace_id": "workspace-1",
        "pane_id": "pane-1",
    }
    actions = []
    monkeypatch.setattr(loop_runtime, "STATE_DIR", tmp_path)
    monkeypatch.setattr(loop_runtime, "_read_metadata", lambda repo: dict(metadata))
    monkeypatch.setattr(loop_runtime, "_start_server_for_read", lambda repo, data: data["session"])
    monkeypatch.setattr(loop_runtime, "_find_workspace", lambda session, data: {"id": "workspace-1"})
    monkeypatch.setattr(loop_runtime, "_save_report", lambda repo, session, workspace: tmp_path / "report.log")
    monkeypatch.setattr(loop_runtime, "_workspace_id", lambda workspace: workspace["id"])
    monkeypatch.setattr(loop_runtime, "_agent_for_workspace", lambda session, workspace_id: agent)
    monkeypatch.setattr(loop_runtime, "_herdr_json", lambda session, *args: actions.append(args) or {})
    monkeypatch.setattr(loop_runtime, "_workspaces", lambda session: [])
    monkeypatch.setattr(loop_runtime, "_write_metadata", lambda repo, value: None)
    monkeypatch.setattr(loop_runtime, "_run", lambda argv, **kwargs: (0, "inactive"))

    def herdr(session, *args, timeout=loop_runtime.HERDR_TIMEOUT):
        actions.append(args)
        if args[:2] == ("agent", "prompt") and prompt_error:
            raise loop_runtime.HerdrError(prompt_error)
        return ""

    monkeypatch.setattr(loop_runtime, "_herdr", herdr)
    return actions


def test_stop_asks_the_agent_for_a_handoff_before_closing(monkeypatch, tmp_path: Path):
    actions = _stop_harness(monkeypatch, tmp_path, agent={"name": "lupin-loop"})

    result = loop_runtime.stop_loop("widgets")

    prompt = next(a for a in actions if a[:2] == ("agent", "prompt"))
    assert prompt[2] == loop_runtime.AGENT_NAME
    assert "/handoff" in prompt[3]
    assert prompt[4:] == ("--wait", "--timeout", "600000")
    assert actions.index(prompt) < actions.index(("workspace", "close", "workspace-1"))
    assert result == "stopped widgets; report saved to " + str(tmp_path / "report.log")


def test_stop_force_skips_the_handoff(monkeypatch, tmp_path: Path):
    actions = _stop_harness(monkeypatch, tmp_path, agent={"name": "lupin-loop"})

    loop_runtime.stop_loop("widgets", force=True)

    assert not any(a[:2] == ("agent", "prompt") for a in actions)
    assert ("workspace", "close", "workspace-1") in actions


def test_stop_still_closes_when_the_handoff_wait_times_out(monkeypatch, tmp_path: Path):
    actions = _stop_harness(
        monkeypatch, tmp_path, agent={"name": "lupin-loop"}, prompt_error="timeout waiting for agent"
    )

    result = loop_runtime.stop_loop("widgets", grace=30)

    assert ("workspace", "close", "workspace-1") in actions
    assert result.endswith("; the agent did not finish its handoff in 30s")


def test_stop_closes_nothing_when_the_agent_cannot_be_asked(monkeypatch, tmp_path: Path):
    actions = _stop_harness(
        monkeypatch, tmp_path, agent={"name": "lupin-loop"}, prompt_error="agent_blocked"
    )

    with pytest.raises(loop_runtime.LoopError, match="Add --force"):
        loop_runtime.stop_loop("widgets")

    assert ("workspace", "close", "workspace-1") not in actions


# --- one Herdr session per machine -------------------------------------------

HERDR_AGENT_NAME = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")  # the rule `herdr agent start` reports


def test_every_repo_shares_one_session():
    assert loop_runtime.session_name("lupin") == loop_runtime.SESSION_NAME
    assert loop_runtime.session_name("ghostbook.nix") == loop_runtime.SESSION_NAME


@pytest.mark.parametrize(
    "repo, expected",
    [
        # Session names `herdr session list` showed on ralpha before the change.
        ("lupin", "lupin-lupin-9f955a0544"),
        ("roundsmith", "lupin-roundsmith-87403ef076"),
        ("ghostbook.nix", "lupin-ghostbook.nix-d80c71b78e"),
    ],
)
def test_legacy_session_name_matches_the_old_per_repo_names(repo, expected):
    assert loop_runtime.legacy_session_name(repo) == expected


@pytest.mark.parametrize("repo", ["lupin", "field-trip_2.0", "ghostbook.nix", "UPPER.Case", "a" * 100])
def test_agent_name_obeys_the_herdr_rule(repo):
    assert HERDR_AGENT_NAME.fullmatch(loop_runtime.agent_name(repo))


def test_agent_name_differs_for_repos_with_the_same_slug():
    assert loop_runtime.agent_name("Foo.bar") != loop_runtime.agent_name("foo-bar")


def test_a_repo_owns_the_shared_session_and_its_own_old_one_only():
    assert loop_runtime._owns_session("lupin", loop_runtime.SESSION_NAME)
    assert loop_runtime._owns_session("lupin", "lupin-lupin-9f955a0544")
    assert not loop_runtime._owns_session("lupin", "lupin-roundsmith-87403ef076")


def test_agent_lookup_matches_lupin_agents_in_the_workspace_only(monkeypatch):
    agents = [
        {"name": "scratch", "workspace_id": "w1"},
        {"name": "lupin-lupin-12345678", "workspace_id": "w2"},
        {"name": "lupin-loop", "workspace_id": "w3"},
    ]
    monkeypatch.setattr(loop_runtime, "_agents", lambda session: agents)

    assert loop_runtime._agent_for_workspace("s", "w1") is None
    assert loop_runtime._agent_for_workspace("s", "w2")["name"] == "lupin-lupin-12345678"
    assert loop_runtime._agent_for_workspace("s", "w3")["name"] == "lupin-loop"


def test_machine_server_unit_is_not_named_for_a_repo(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(loop_runtime, "CODE_DIR", tmp_path)
    launched = []
    monkeypatch.setattr(loop_runtime, "_run", lambda argv, **kwargs: launched.append(argv) or (0, ""))

    loop_runtime._systemd_run(None, "herdr", ["lupin", "loop", "herdr-server"])

    assert "--unit=lupin-herdr-machine" in launched[0]
    assert launched[0][launched[0].index("--working-directory") + 1] == str(tmp_path)


def test_shared_server_starts_once_with_no_repo_credentials(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(loop_runtime, "STATE_DIR", tmp_path)
    running = []
    monkeypatch.setattr(loop_runtime, "_server_running", lambda session: bool(running))
    started = []

    def systemd_run(repo, kind, command, **kwargs):
        started.append((repo, kind, command, kwargs))
        running.append(True)
        return 0, ""

    monkeypatch.setattr(loop_runtime, "_systemd_run", systemd_run)

    loop_runtime._ensure_server("widgets", loop_runtime.SESSION_NAME, "claude")
    loop_runtime._ensure_server("gadgets", loop_runtime.SESSION_NAME, "claude")

    assert len(started) == 1
    repo, kind, command, options = started[0]
    assert (repo, kind) == (None, "herdr")
    assert command[-2:] == ["--session", loop_runtime.SESSION_NAME]
    assert options == {}


def test_shared_server_start_error_is_ignored_when_another_start_won_the_race(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(loop_runtime, "STATE_DIR", tmp_path)
    states = iter([False, True, True])
    monkeypatch.setattr(loop_runtime, "_server_running", lambda session: next(states))
    monkeypatch.setattr(loop_runtime, "_systemd_run", lambda *args, **kwargs: (1, "unit already exists"))

    loop_runtime._ensure_server("widgets", loop_runtime.SESSION_NAME, "claude")


def test_workspace_in_shared_session_gets_the_repo_token(monkeypatch, tmp_path: Path):
    token = tmp_path / "token"
    token.write_text("ghp_abc\n", encoding="utf-8")
    monkeypatch.setattr(loop_runtime, "_runtime_paths", lambda repo: {"gh-token": str(token)})

    assert loop_runtime._workspace_env(loop_runtime.SESSION_NAME, "widgets") == ["--env", "GH_TOKEN=ghp_abc"]
    assert loop_runtime._workspace_env("lupin-widgets-abc123", "widgets") == []


def test_workspace_in_shared_session_without_a_token_gets_no_env(monkeypatch):
    monkeypatch.setattr(loop_runtime, "_runtime_paths", lambda repo: {})

    assert loop_runtime._workspace_env(loop_runtime.SESSION_NAME, "widgets") == []


def test_stop_leaves_the_shared_session_running(monkeypatch, tmp_path: Path):
    actions = _stop_harness(monkeypatch, tmp_path, agent=None)
    monkeypatch.setattr(loop_runtime, "_read_metadata", lambda repo: {
        "repo": "widgets", "platform": "claude", "session": loop_runtime.SESSION_NAME,
        "state": "running", "workspace_id": "workspace-1", "pane_id": "pane-1",
    })
    monkeypatch.setattr(loop_runtime, "_start_server_for_read", lambda repo, data: data["session"])

    loop_runtime.stop_loop("widgets")

    assert ("workspace", "close", "workspace-1") in actions
    assert not any(a[:2] == ("session", "stop") for a in actions)


def test_start_loop_works_while_the_shared_session_runs_other_loops(monkeypatch, tmp_path: Path, make_checkout):
    state_dir = tmp_path / "state"
    code_dir = tmp_path / "code"
    make_checkout(code_dir / "widgets")
    monkeypatch.setattr(loop_runtime, "STATE_DIR", state_dir)
    monkeypatch.setattr(loop_runtime, "LOOPS_DIR", state_dir / "herdr-loops")
    monkeypatch.setattr(loop_runtime, "REPOS_FILE", state_dir / "repos")
    monkeypatch.setattr(loop_runtime, "CODE_DIR", code_dir)
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {})
    # The shared session is running; no old per-repo session exists.
    monkeypatch.setattr(
        loop_runtime, "_session_info",
        lambda session: {"name": session, "running": True} if session == loop_runtime.SESSION_NAME else None,
    )
    monkeypatch.setattr(
        loop_runtime, "_run", lambda argv, **kwargs: (3, "inactive") if argv[0] == "systemctl" else (1, "no session")
    )
    monkeypatch.setattr(loop_runtime, "_systemd_run", lambda *args, **kwargs: (0, ""))

    started, message = loop_runtime.start_loop("widgets", platform="omp")

    assert started, message
    assert loop_runtime._read_metadata("widgets")["session"] == loop_runtime.SESSION_NAME


def test_start_loop_selects_profile_candidate_and_persists_it_for_recovery(
    monkeypatch, tmp_path: Path, make_checkout
):
    from lupin import quota_cache

    state_dir = tmp_path / "state"
    code_dir = tmp_path / "code"
    make_checkout(code_dir / "widgets")
    monkeypatch.setattr(loop_runtime, "STATE_DIR", state_dir)
    monkeypatch.setattr(loop_runtime, "LOOPS_DIR", state_dir / "herdr-loops")
    monkeypatch.setattr(loop_runtime, "REPOS_FILE", state_dir / "repos")
    monkeypatch.setattr(loop_runtime, "CODE_DIR", code_dir)
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {"widgets": "claude"})
    monkeypatch.setattr(
        loop_runtime,
        "orchestrator_profiles",
        lambda: {"widgets": ["opencode-go/step-5-preview-free:xhigh"]},
    )
    monkeypatch.setattr(loop_runtime.time, "time", lambda: 1735689600)
    now_ms = 1735689600 * 1000
    monkeypatch.setattr(
        quota_cache,
        "read_snapshot",
        lambda **kwargs: {
            "opencode-go": {
                "fetched_at": "2025-01-01T00:00:00+00:00",
                "rows": [{
                    "provider": "opencode-go",
                    "used_pct": 10,
                    "resets_at": now_ms + 60_000,
                }],
            }
        },
    )
    monkeypatch.setattr(loop_runtime, "_session_info", lambda session: None)
    monkeypatch.setattr(
        loop_runtime,
        "_run",
        lambda argv, **kwargs: (3, "inactive") if argv[0] == "systemctl" else (1, "no session"),
    )
    monkeypatch.setattr(loop_runtime, "_systemd_run", lambda *args, **kwargs: (0, ""))

    started, _ = loop_runtime.start_loop("widgets")

    assert started
    metadata = loop_runtime._read_metadata("widgets")
    assert metadata["platform"] == "omp"
    assert metadata["provider"] == "opencode-go"
    assert metadata["model"] == "opencode-go/step-5-preview-free:xhigh"


def test_disable_repo_removes_its_orchestrator_profile(monkeypatch, tmp_path):
    state_dir = tmp_path / "state"
    monkeypatch.setattr(loop_runtime, "REPOS_FILE", state_dir / "repos")
    monkeypatch.setattr(loop_runtime, "ORCHESTRATORS_FILE", state_dir / "orchestrators.json")
    loop_runtime.write_repos([("widgets", "omp"), ("gizmos", "claude")])
    loop_runtime._write_orchestrator_profiles({
        "widgets": ["openai/gpt-5"],
        "gizmos": ["claude"],
    })

    loop_runtime.disable_repo("widgets")

    assert loop_runtime.enabled_repos() == {"gizmos": "claude"}
    assert loop_runtime.orchestrator_profiles() == {"gizmos": ["claude"]}


# --- one worktree per run ----------------------------------------------------


def _git_out(path: Path, *args: str) -> str:
    """Run git in `path` for a test. Return its output."""
    return subprocess.run(
        ["git", "-C", str(path), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def _start_harness(monkeypatch, tmp_path: Path, make_checkout):
    """Patch start_loop's state, systemd, and Herdr calls. Return the checkout and the launches."""
    state_dir = tmp_path / "state"
    code_dir = tmp_path / "code"
    checkout = make_checkout(code_dir / "widgets")
    monkeypatch.setattr(loop_runtime, "STATE_DIR", state_dir)
    monkeypatch.setattr(loop_runtime, "LOOPS_DIR", state_dir / "herdr-loops")
    monkeypatch.setattr(loop_runtime, "REPOS_FILE", state_dir / "repos")
    monkeypatch.setattr(loop_runtime, "CODE_DIR", code_dir)
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {})
    monkeypatch.setattr(loop_runtime, "_session_info", lambda session: None)
    monkeypatch.setattr(
        loop_runtime, "_run",
        lambda argv, **kwargs: (3, "inactive") if argv[0] == "systemctl" else (1, "no session"),
    )
    launches = []
    monkeypatch.setattr(
        loop_runtime, "_systemd_run", lambda *args, **kwargs: launches.append(args) or (0, "")
    )
    return checkout, launches


@pytest.mark.parametrize(
    "name, is_dir, label",
    [
        ("MERGE_HEAD", False, "merge"),
        ("rebase-merge", True, "rebase"),
        ("rebase-apply", True, "rebase"),
        ("CHERRY_PICK_HEAD", False, "cherry-pick"),
        ("REVERT_HEAD", False, "revert"),
    ],
)
def test_start_loop_refuses_a_checkout_with_an_unfinished_git_operation(
    monkeypatch, tmp_path: Path, make_checkout, name, is_dir, label
):
    checkout, launches = _start_harness(monkeypatch, tmp_path, make_checkout)
    marker = checkout / ".git" / name
    if is_dir:
        marker.mkdir()
    else:
        marker.write_text(_git_out(checkout, "rev-parse", "HEAD") + "\n", encoding="utf-8")

    with pytest.raises(loop_runtime.LoopError) as error:
        loop_runtime.start_loop("widgets", platform="claude")

    assert f"{label} in progress" in str(error.value)
    assert str(marker.resolve()) in str(error.value)
    assert launches == []
    assert loop_runtime._read_metadata("widgets") is None
    assert not (tmp_path / "state" / "worktrees").exists()
    assert _git_out(checkout, "worktree", "list", "--porcelain").count("worktree ") == 1
    assert marker.exists()  # Lupin never clears it.


def test_start_loop_refuses_a_checkout_with_unmerged_files(monkeypatch, tmp_path: Path, make_checkout):
    checkout, launches = _start_harness(monkeypatch, tmp_path, make_checkout)
    blob = _git_out(checkout, "hash-object", "-w", "README.md")
    entries = "".join(f"100644 {blob} {stage}\tREADME.md\n" for stage in (1, 2, 3))
    subprocess.run(
        ["git", "-C", str(checkout), "update-index", "--index-info"],
        input=entries, text=True, check=True,
    )
    assert _git_out(checkout, "ls-files", "-u")  # The index has unmerged entries.

    with pytest.raises(loop_runtime.LoopError, match=r"unmerged files: README\.md"):
        loop_runtime.start_loop("widgets", platform="claude")

    assert launches == []
    assert loop_runtime._read_metadata("widgets") is None
    assert _git_out(checkout, "ls-files", "-u")  # Lupin leaves the index as it was.


def test_start_loop_refuses_a_directory_that_is_not_a_git_checkout(monkeypatch, tmp_path: Path):
    state_dir = tmp_path / "state"
    code_dir = tmp_path / "code"
    (code_dir / "widgets").mkdir(parents=True)
    monkeypatch.setattr(loop_runtime, "STATE_DIR", state_dir)
    monkeypatch.setattr(loop_runtime, "LOOPS_DIR", state_dir / "herdr-loops")
    monkeypatch.setattr(loop_runtime, "REPOS_FILE", state_dir / "repos")
    monkeypatch.setattr(loop_runtime, "CODE_DIR", code_dir)
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {})
    monkeypatch.setattr(loop_runtime, "_session_info", lambda session: None)
    monkeypatch.setattr(
        loop_runtime, "_run",
        lambda argv, **kwargs: (3, "inactive") if argv[0] == "systemctl" else (1, "no session"),
    )
    monkeypatch.setattr(
        loop_runtime, "_systemd_run", lambda *args, **kwargs: pytest.fail("no checkout, no loop")
    )

    with pytest.raises(loop_runtime.LoopError, match="git rev-parse failed"):
        loop_runtime.start_loop("widgets", platform="claude")


def test_start_loop_gives_each_run_a_new_branch_in_its_own_worktree(
    monkeypatch, tmp_path: Path, make_checkout
):
    checkout, launches = _start_harness(monkeypatch, tmp_path, make_checkout)

    started, message = loop_runtime.start_loop("widgets", platform="claude")

    assert started, message
    worktree = Path(loop_runtime._read_metadata("widgets")["worktree"])
    branch = _git_out(worktree, "branch", "--show-current")
    assert worktree.parent == tmp_path / "state" / "worktrees" / "widgets"
    assert checkout not in worktree.parents
    assert str(worktree) in message
    assert branch.startswith("lupin-loop/")
    assert _git_out(worktree, "rev-parse", "HEAD") == _git_out(checkout, "rev-parse", "origin/main")
    assert _git_out(checkout, "branch", "--show-current") == "main"
    assert str(worktree.resolve()) in _git_out(checkout, "worktree", "list", "--porcelain")
    no_upstream = subprocess.run(
        ["git", "-C", str(worktree), "config", "--get", f"branch.{branch}.remote"],
        capture_output=True, text=True,
    )
    assert no_upstream.returncode == 1  # The run branch does not track origin/main.


def test_agent_workspace_directory_is_the_run_worktree_not_the_checkout(
    monkeypatch, tmp_path: Path, make_checkout
):
    checkout, _ = _start_harness(monkeypatch, tmp_path, make_checkout)
    loop_runtime.start_loop("widgets", platform="claude")
    worktree = loop_runtime._read_metadata("widgets")["worktree"]
    herdr_calls = []

    def herdr_json(session, *args, **kwargs):
        herdr_calls.append(args)
        return {"workspace": {"id": "w1"}, "root_pane": {"workspace_id": "w1", "pane_id": "p1"}}

    monkeypatch.setattr(loop_runtime, "_ensure_server", lambda *args: None)
    monkeypatch.setattr(loop_runtime, "_server_running", lambda session: True)
    monkeypatch.setattr(loop_runtime, "_find_workspace", lambda session, metadata: None)
    monkeypatch.setattr(loop_runtime, "_workspace_by_id", lambda session, workspace_id: None)
    monkeypatch.setattr(loop_runtime, "_runtime_paths", lambda repo: {})
    monkeypatch.setattr(loop_runtime, "_herdr_json", herdr_json)
    monkeypatch.setattr(loop_runtime.slots, "hold", lambda *args, **kwargs: 0)

    rc = loop_runtime._monitor_loop(
        "widgets", "claude", loop_runtime.SESSION_NAME, "prompt.md", False
    )

    assert rc == 0
    create = next(call for call in herdr_calls if call[:2] == ("workspace", "create"))
    assert create[create.index("--cwd") + 1] == worktree
    assert str(checkout) not in create


@pytest.mark.parametrize("saved", [{}, {"worktree": "gone"}])
def test_monitor_does_not_fall_back_to_the_checkout_when_the_worktree_is_missing(
    monkeypatch, tmp_path: Path, saved: dict
):
    state_dir = tmp_path / "state"
    monkeypatch.setattr(loop_runtime, "STATE_DIR", state_dir)
    monkeypatch.setattr(loop_runtime, "LOOPS_DIR", state_dir / "herdr-loops")
    monkeypatch.setattr(loop_runtime, "CODE_DIR", tmp_path / "code")
    (tmp_path / "code" / "widgets").mkdir(parents=True)
    value = {"version": 1, "repo": "widgets", "session": loop_runtime.SESSION_NAME, "state": "starting"}
    if "worktree" in saved:
        value["worktree"] = str(tmp_path / saved["worktree"])
    loop_runtime._write_metadata("widgets", value)
    monkeypatch.setattr(loop_runtime, "_ensure_server", lambda *args: None)
    monkeypatch.setattr(loop_runtime, "_find_workspace", lambda session, metadata: None)
    monkeypatch.setattr(
        loop_runtime, "_create_workspace",
        lambda *args: pytest.fail("an agent must not start without its worktree"),
    )

    with pytest.raises(loop_runtime.LoopError, match="no worktree"):
        loop_runtime._monitor_loop(
            "widgets", "claude", loop_runtime.SESSION_NAME, "prompt.md", False
        )


def test_start_loop_removes_its_new_worktree_when_the_service_does_not_start(
    monkeypatch, tmp_path: Path, make_checkout
):
    checkout, _ = _start_harness(monkeypatch, tmp_path, make_checkout)
    monkeypatch.setattr(loop_runtime, "_systemd_run", lambda *args, **kwargs: (1, "boom"))

    started, message = loop_runtime.start_loop("widgets", platform="claude")

    assert (started, message) == (False, "boom")
    assert list((tmp_path / "state" / "worktrees" / "widgets").iterdir()) == []
    assert _git_out(checkout, "worktree", "list", "--porcelain").count("worktree ") == 1
    metadata = loop_runtime._read_metadata("widgets")
    assert metadata["state"] == "failed"
    assert "worktree" not in metadata


def test_start_loop_reports_a_worktree_git_keeps_when_the_service_does_not_start(
    monkeypatch, tmp_path: Path, make_checkout
):
    checkout, _ = _start_harness(monkeypatch, tmp_path, make_checkout)
    locked = []

    def fail_and_lock(*args, **kwargs):
        (path,) = (tmp_path / "state" / "worktrees" / "widgets").iterdir()
        _git_out(checkout, "worktree", "lock", str(path))
        locked.append(path)
        return 1, "boom"

    monkeypatch.setattr(loop_runtime, "_systemd_run", fail_and_lock)

    started, message = loop_runtime.start_loop("widgets", platform="claude")

    assert started is False
    assert message.startswith(f"boom; worktree kept at {locked[0]}")
    metadata = loop_runtime._read_metadata("widgets")
    assert metadata["state"] == "failed"
    assert metadata["worktree"] == str(locked[0])


def test_default_base_uses_origin_main_when_origin_head_is_not_set(tmp_path: Path, make_checkout):
    checkout = make_checkout(tmp_path / "code" / "widgets")
    assert loop_runtime._default_base(checkout) == "origin/main"
    _git_out(checkout, "remote", "set-head", "origin", "--delete")

    assert loop_runtime._default_base(checkout) == "origin/main"


def test_default_base_refuses_when_the_checkout_has_no_remote_branch(tmp_path: Path, make_checkout):
    checkout = make_checkout(tmp_path / "code" / "widgets")
    _git_out(checkout, "remote", "remove", "origin")

    with pytest.raises(loop_runtime.LoopError, match="cannot find the default branch"):
        loop_runtime._default_base(checkout)


def _run_with_worktree(monkeypatch, tmp_path: Path, make_checkout):
    """Start a run with a real worktree, then patch what stop_loop needs.

    Return the checkout, the worktree, and the list of saved loop states.
    """
    checkout, _ = _start_harness(monkeypatch, tmp_path, make_checkout)
    loop_runtime.start_loop("widgets", platform="claude")
    metadata = loop_runtime._read_metadata("widgets")
    _stop_harness(monkeypatch, tmp_path, agent=None)
    writes = []
    monkeypatch.setattr(loop_runtime, "_read_metadata", lambda repo: dict(metadata))
    monkeypatch.setattr(loop_runtime, "_write_metadata", lambda repo, value: writes.append(dict(value)))
    return checkout, Path(metadata["worktree"]), writes


@pytest.mark.parametrize("change", ["untracked", "modified"])
def test_stop_keeps_a_worktree_that_has_changes(
    monkeypatch, tmp_path: Path, make_checkout, change: str
):
    checkout, worktree, writes = _run_with_worktree(monkeypatch, tmp_path, make_checkout)
    changed = worktree / ("notes.txt" if change == "untracked" else "README.md")
    changed.write_text("unsaved work\n", encoding="utf-8")

    result = loop_runtime.stop_loop("widgets", force=True)

    assert f"worktree kept at {worktree}" in result
    assert changed.read_text(encoding="utf-8") == "unsaved work\n"
    assert str(worktree.resolve()) in _git_out(checkout, "worktree", "list", "--porcelain")
    assert writes[-1]["state"] == "stopped"
    assert writes[-1]["worktree"] == str(worktree)


def test_stop_removes_a_clean_worktree_and_never_forces(monkeypatch, tmp_path: Path, make_checkout):
    checkout, worktree, writes = _run_with_worktree(monkeypatch, tmp_path, make_checkout)
    branch = _git_out(worktree, "branch", "--show-current")
    calls = []
    real_git = loop_runtime._git
    monkeypatch.setattr(
        loop_runtime, "_git", lambda directory, *args: calls.append(args) or real_git(directory, *args)
    )

    result = loop_runtime.stop_loop("widgets", force=True)

    assert "worktree" not in result
    assert not worktree.exists()
    assert str(worktree.resolve()) not in _git_out(checkout, "worktree", "list", "--porcelain")
    assert ("worktree", "remove", str(worktree)) in calls
    assert not any(arg in {"--force", "-f"} for args in calls for arg in args)
    assert "worktree" not in writes[-1]
    assert branch not in _git_out(checkout, "branch", "--list")  # Merged, so git -d removes it.


def test_stop_fails_and_keeps_metadata_when_git_will_not_remove_a_clean_worktree(
    monkeypatch, tmp_path: Path, make_checkout
):
    checkout, worktree, writes = _run_with_worktree(monkeypatch, tmp_path, make_checkout)
    _git_out(checkout, "worktree", "lock", str(worktree))

    with pytest.raises(loop_runtime.LoopError, match=re.escape(f"worktree kept at {worktree}")):
        loop_runtime.stop_loop("widgets", force=True)

    assert worktree.is_dir()
    assert writes == []  # Metadata keeps the worktree path.


# --- review fixes: ledger copy, resume, cleanup, fetch ----------------------


def _git_as(path: Path, *args: str) -> str:
    """Run git in `path` with a test identity. Return its output."""
    return subprocess.run(
        ["git", "-C", str(path), "-c", "user.name=t", "-c", "user.email=t@example.com",
         "-c", "commit.gpgsign=false", *args],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


def _set_state(repo: str, state: str) -> None:
    value = loop_runtime._read_metadata(repo)
    value["state"] = state
    loop_runtime._write_metadata(repo, value)


def _worktree_count(checkout: Path) -> int:
    return _git_out(checkout, "worktree", "list", "--porcelain").count("worktree ")


def test_stop_keeps_an_ignored_ledger_file_outside_the_worktree(
    monkeypatch, tmp_path: Path, make_checkout
):
    checkout, worktree, _ = _run_with_worktree(monkeypatch, tmp_path, make_checkout)
    (checkout / ".git" / "info").mkdir(exist_ok=True)
    (checkout / ".git" / "info" / "exclude").write_text(".loop/\n", encoding="utf-8")
    (worktree / ".loop").mkdir()
    (worktree / ".loop" / "loop-state.json").write_text('{"entries": []}\n', encoding="utf-8")
    assert "!! .loop/" in _git_out(worktree, "status", "--porcelain", "--ignored")

    loop_runtime.stop_loop("widgets", force=True)

    kept = loop_runtime.STATE_DIR / "handoffs" / "widgets.json"
    assert kept.read_text(encoding="utf-8") == '{"entries": []}\n'
    assert not worktree.exists()


def test_next_prompt_names_the_kept_ledger_file_only_when_it_exists(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(loop_runtime, "STATE_DIR", tmp_path)
    monkeypatch.delenv("LUPIN_LOOP_PROMPT_FILE", raising=False)
    kept = tmp_path / "handoffs" / "widgets.json"

    before = Path(loop_runtime._write_prompt("widgets", None, False)).read_text(encoding="utf-8")
    assert str(kept) not in before

    kept.parent.mkdir()
    kept.write_text("{}\n", encoding="utf-8")
    after = Path(loop_runtime._write_prompt("widgets", None, False)).read_text(encoding="utf-8")
    assert str(kept) in after


def test_stop_keeps_an_ignored_handoff_file_outside_the_worktree(
    monkeypatch, tmp_path: Path, make_checkout
):
    checkout, worktree, _ = _run_with_worktree(monkeypatch, tmp_path, make_checkout)
    (checkout / ".git" / "info").mkdir(exist_ok=True)
    (checkout / ".git" / "info" / "exclude").write_text("HANDOFF.md\n", encoding="utf-8")
    (worktree / "HANDOFF.md").write_text("# Handoff\n", encoding="utf-8")
    assert "!! HANDOFF.md" in _git_out(worktree, "status", "--porcelain", "--ignored")

    loop_runtime.stop_loop("widgets", force=True)

    kept = loop_runtime.STATE_DIR / "handoffs" / "widgets.HANDOFF.md"
    assert kept.read_text(encoding="utf-8") == "# Handoff\n"
    assert not worktree.exists()


def test_next_prompt_names_the_kept_handoff_file_only_when_it_exists(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(loop_runtime, "STATE_DIR", tmp_path)
    monkeypatch.delenv("LUPIN_LOOP_PROMPT_FILE", raising=False)
    kept = tmp_path / "handoffs" / "widgets.HANDOFF.md"

    before = Path(loop_runtime._write_prompt("widgets", None, False)).read_text(encoding="utf-8")
    assert str(kept) not in before

    kept.parent.mkdir()
    kept.write_text("# Handoff\n", encoding="utf-8")
    after = Path(loop_runtime._write_prompt("widgets", None, False)).read_text(encoding="utf-8")
    assert str(kept) in after


def test_resume_reuses_the_recorded_worktree(monkeypatch, tmp_path: Path, make_checkout):
    checkout, launches = _start_harness(monkeypatch, tmp_path, make_checkout)
    loop_runtime.start_loop("widgets", platform="claude")
    worktree = Path(loop_runtime._read_metadata("widgets")["worktree"])
    _set_state("widgets", "stopped")

    started, message = loop_runtime.start_loop("widgets", platform="claude", resume=True)

    assert started, message
    assert Path(loop_runtime._read_metadata("widgets")["worktree"]) == worktree
    assert _worktree_count(checkout) == 2  # No new worktree.
    assert "--resume" in launches[-1][2]


def test_resume_refuses_a_missing_worktree_and_names_it(monkeypatch, tmp_path: Path, make_checkout):
    checkout, launches = _start_harness(monkeypatch, tmp_path, make_checkout)
    loop_runtime.start_loop("widgets", platform="claude")
    worktree = Path(loop_runtime._read_metadata("widgets")["worktree"])
    _set_state("widgets", "stopped")
    shutil.rmtree(worktree)
    _git_out(checkout, "worktree", "prune")
    launched = len(launches)

    with pytest.raises(loop_runtime.LoopError, match=re.escape(str(worktree))):
        loop_runtime.start_loop("widgets", platform="claude", resume=True)

    assert len(launches) == launched
    assert _worktree_count(checkout) == 1  # No new worktree.


def test_resume_refuses_a_worktree_on_another_branch(monkeypatch, tmp_path: Path, make_checkout):
    checkout, launches = _start_harness(monkeypatch, tmp_path, make_checkout)
    loop_runtime.start_loop("widgets", platform="claude")
    value = loop_runtime._read_metadata("widgets")
    worktree = Path(value["worktree"])
    value["branch"] = "lupin-loop/other"
    value["state"] = "stopped"
    loop_runtime._write_metadata("widgets", value)
    launched = len(launches)

    with pytest.raises(loop_runtime.LoopError, match=re.escape(str(worktree))):
        loop_runtime.start_loop("widgets", platform="claude", resume=True)

    assert len(launches) == launched


def test_start_loop_removes_the_previous_clean_worktree_first(
    monkeypatch, tmp_path: Path, make_checkout
):
    checkout, _ = _start_harness(monkeypatch, tmp_path, make_checkout)
    loop_runtime.start_loop("widgets", platform="claude")
    first = Path(loop_runtime._read_metadata("widgets")["worktree"])
    _set_state("widgets", "stopped")

    started, message = loop_runtime.start_loop("widgets", platform="claude")

    assert started, message
    second = Path(loop_runtime._read_metadata("widgets")["worktree"])
    assert not first.exists()
    assert second.is_dir()
    assert _worktree_count(checkout) == 2


def test_start_loop_refuses_when_the_previous_worktree_has_changes(
    monkeypatch, tmp_path: Path, make_checkout
):
    checkout, launches = _start_harness(monkeypatch, tmp_path, make_checkout)
    loop_runtime.start_loop("widgets", platform="claude")
    first = Path(loop_runtime._read_metadata("widgets")["worktree"])
    (first / "README.md").write_text("unsaved work\n", encoding="utf-8")
    _set_state("widgets", "stopped")
    launched = len(launches)

    with pytest.raises(loop_runtime.LoopError, match=re.escape(str(first))):
        loop_runtime.start_loop("widgets", platform="claude")

    assert len(launches) == launched
    assert first.is_dir()
    assert loop_runtime._read_metadata("widgets")["worktree"] == str(first)


@pytest.mark.parametrize("dirty", [False, True], ids=["clean", "dirty"])
@pytest.mark.parametrize(
    "step, error",
    [
        ("_write_prompt", RuntimeError),
        ("_write_metadata", RuntimeError),
        ("_systemd_run", loop_runtime.LoopError),
    ],
)
def test_start_failure_cleans_up_and_records_the_run_as_failed(
    monkeypatch, tmp_path: Path, make_checkout, step: str, error: type, dirty: bool
):
    checkout, _ = _start_harness(monkeypatch, tmp_path, make_checkout)
    real = getattr(loop_runtime, step)
    worktrees = tmp_path / "state" / "worktrees" / "widgets"
    fired = []

    def fail():
        fired.append(True)
        if dirty:
            (run_dir,) = worktrees.iterdir()
            (run_dir / "unsaved.txt").write_text("work\n", encoding="utf-8")
        raise error(f"forced {step}")

    if step == "_write_metadata":
        # Fail only the `starting` write, so the cleanup can still write metadata.
        def fail_the_starting_write(repo, value):
            if value["state"] == "starting" and not fired:
                fail()
            return real(repo, value)

        monkeypatch.setattr(loop_runtime, step, fail_the_starting_write)
    else:
        monkeypatch.setattr(loop_runtime, step, lambda *args, **kwargs: fail())

    started, message = loop_runtime.start_loop("widgets", platform="claude")

    assert started is False
    metadata = loop_runtime._read_metadata("widgets")
    assert metadata["state"] == "failed"
    if not dirty:
        assert message == f"forced {step}"
        assert list(worktrees.iterdir()) == []
        assert "worktree" not in metadata
        assert "branch" not in metadata
        assert _git_out(checkout, "branch", "--list", "lupin-loop/*") == ""
        assert _worktree_count(checkout) == 1
        monkeypatch.setattr(loop_runtime, step, real)
        assert loop_runtime.start_loop("widgets", platform="claude")[0] is True
        return
    (run_dir,) = worktrees.iterdir()
    assert message == f"forced {step}; worktree kept at {run_dir}: it has changes"
    assert (run_dir / "unsaved.txt").read_text(encoding="utf-8") == "work\n"
    assert metadata["worktree"] == str(run_dir)
    with pytest.raises(loop_runtime.LoopError, match=re.escape(str(run_dir))):
        loop_runtime.start_loop("widgets", platform="claude")


def test_resume_failure_keeps_the_recorded_worktree_and_records_it_as_failed(
    monkeypatch, tmp_path: Path, make_checkout
):
    checkout, _ = _start_harness(monkeypatch, tmp_path, make_checkout)
    loop_runtime.start_loop("widgets", platform="claude")
    worktree = Path(loop_runtime._read_metadata("widgets")["worktree"])
    _set_state("widgets", "stopped")

    def fail(*args, **kwargs):
        raise loop_runtime.LoopError("forced _systemd_run")

    monkeypatch.setattr(loop_runtime, "_systemd_run", fail)

    started, message = loop_runtime.start_loop("widgets", platform="claude", resume=True)

    assert (started, message) == (False, "forced _systemd_run")
    assert worktree.is_dir()
    metadata = loop_runtime._read_metadata("widgets")
    assert metadata["state"] == "failed"
    assert metadata["worktree"] == str(worktree)


def test_start_failure_records_failed_when_the_handoff_copy_raises(
    monkeypatch, tmp_path: Path, make_checkout
):
    _start_harness(monkeypatch, tmp_path, make_checkout)
    monkeypatch.setattr(loop_runtime, "_systemd_run", lambda *args, **kwargs: (1, "boom"))

    def copy_fails(repo, worktree):
        raise OSError("disk full")

    monkeypatch.setattr(loop_runtime, "_keep_handoff", copy_fails)

    # Follow-up: an OSError is not a LoopError, so it still escapes start_loop.
    with pytest.raises(OSError, match="disk full"):
        loop_runtime.start_loop("widgets", platform="claude")

    assert loop_runtime._read_metadata("widgets")["state"] == "failed"


def test_full_claude_slot_removes_the_new_worktree(monkeypatch, tmp_path: Path, make_checkout):
    checkout, _ = _start_harness(monkeypatch, tmp_path, make_checkout)
    loop_runtime.start_loop("widgets", platform="claude")
    worktree = Path(loop_runtime._read_metadata("widgets")["worktree"])
    monkeypatch.setattr(loop_runtime, "_ensure_server", lambda *args: None)
    monkeypatch.setattr(loop_runtime, "_server_running", lambda session: True)
    monkeypatch.setattr(loop_runtime, "_find_workspace", lambda session, metadata: None)
    monkeypatch.setattr(loop_runtime, "_runtime_paths", lambda repo: {})
    monkeypatch.setattr(
        loop_runtime, "_herdr_json",
        lambda session, *args, **kwargs: {
            "workspace": {"id": "w1"}, "root_pane": {"workspace_id": "w1", "pane_id": "p1"},
        },
    )

    def full(*args, **kwargs):
        raise loop_runtime.slots.SlotFull("full")

    monkeypatch.setattr(loop_runtime.slots, "hold", full)

    with pytest.raises(loop_runtime.LoopError, match="slot is full"):
        loop_runtime._monitor_loop("widgets", "claude", loop_runtime.SESSION_NAME, "prompt.md", False)

    metadata = loop_runtime._read_metadata("widgets")
    assert metadata["state"] == "failed"
    assert "worktree" not in metadata
    assert not worktree.exists()
    assert _worktree_count(checkout) == 1


def test_start_loop_branches_from_the_fetched_remote_default_branch(
    monkeypatch, tmp_path: Path, make_checkout
):
    _checkout, _ = _start_harness(monkeypatch, tmp_path, make_checkout)
    origin = tmp_path / "origins" / "widgets.git"
    clone = tmp_path / "scratch" / "clone"
    _git_as(tmp_path, "clone", "-q", str(origin), str(clone))
    (clone / "new.txt").write_text("new\n", encoding="utf-8")
    _git_as(clone, "add", "new.txt")
    _git_as(clone, "commit", "-q", "-m", "newer commit on origin")
    _git_as(clone, "push", "-q", "origin", "main")
    remote_main = _git_out(origin, "rev-parse", "main")

    started, message = loop_runtime.start_loop("widgets", platform="claude")

    assert started, message
    worktree = Path(loop_runtime._read_metadata("widgets")["worktree"])
    assert _git_out(worktree, "rev-parse", "HEAD") == remote_main


def test_start_loop_refuses_when_fetch_fails(monkeypatch, tmp_path: Path, make_checkout):
    checkout, launches = _start_harness(monkeypatch, tmp_path, make_checkout)
    _git_out(checkout, "remote", "set-url", "origin", str(tmp_path / "missing.git"))

    with pytest.raises(loop_runtime.LoopError, match="git fetch failed"):
        loop_runtime.start_loop("widgets", platform="claude")

    assert launches == []
    assert loop_runtime._read_metadata("widgets") is None
    assert _worktree_count(checkout) == 1


def test_stop_keeps_a_detached_commit_that_is_on_no_branch(
    monkeypatch, tmp_path: Path, make_checkout
):
    checkout, worktree, writes = _run_with_worktree(monkeypatch, tmp_path, make_checkout)
    _git_as(worktree, "checkout", "-q", "--detach")
    _git_as(worktree, "commit", "-q", "--allow-empty", "-m", "agent work")
    sha = _git_out(worktree, "rev-parse", "HEAD")

    result = loop_runtime.stop_loop("widgets", force=True)

    assert f"worktree kept at {worktree}" in result
    assert worktree.is_dir()
    assert _git_out(checkout, "cat-file", "-t", sha) == "commit"
    assert writes[-1]["worktree"] == str(worktree)


def test_stop_keeps_a_branch_with_unmerged_commits(monkeypatch, tmp_path: Path, make_checkout):
    checkout, worktree, _ = _run_with_worktree(monkeypatch, tmp_path, make_checkout)
    branch = _git_out(worktree, "branch", "--show-current")
    _git_as(worktree, "commit", "-q", "--allow-empty", "-m", "agent work")

    result = loop_runtime.stop_loop("widgets", force=True)

    assert not worktree.exists()
    assert branch in _git_out(checkout, "branch", "--list")
    assert f"branch {branch} kept" in result


@pytest.mark.parametrize(
    "module",
    ["lupin.loop_runtime", "lupin.agent", "lupin.roadmap", "lupin.debrief", "lupin.cli", "lupin.serve"],
)
def test_module_imports_in_a_fresh_process(module: str):
    # A new process imports this module first. Shared test imports hide cycles.
    src = Path(__file__).resolve().parents[1] / "src"
    env = {key: value for key, value in os.environ.items() if not key.startswith("LUPIN_")}
    env["PYTHONPATH"] = str(src)

    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        env=env, capture_output=True, text=True, timeout=60,
    )

    assert result.returncode == 0, result.stderr
