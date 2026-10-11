"""Tests for Lupin loop commands and their fleet routing."""

from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest
import redis as redis_lib

from lupin import agent, benchmark_fetch, cli, commands, loop_runtime, loops, machines, slots


@pytest.fixture(autouse=True)
def local_host(monkeypatch):
    monkeypatch.setattr(machines, "hostname", lambda: "h")
    yield "h"


def test_run_creates_herdr_worker_metadata_and_systemd_unit(
    monkeypatch, tmp_path, capsys, make_checkout
):
    state_dir = tmp_path / "state"
    code_dir = tmp_path / "code"
    repo_dir = code_dir / "widgets"
    make_checkout(repo_dir)
    (repo_dir / "docs").mkdir()
    (repo_dir / "docs" / "delegation-loop.md").write_text("run this repo\n", encoding="utf-8")
    monkeypatch.setattr(loop_runtime, "STATE_DIR", state_dir)
    monkeypatch.setattr(loop_runtime, "LOOPS_DIR", state_dir / "herdr-loops")
    monkeypatch.setattr(loop_runtime, "REPORTS_DIR", state_dir / "reports")
    monkeypatch.setattr(loop_runtime, "REPOS_FILE", state_dir / "repos")
    monkeypatch.setattr(loop_runtime, "CODE_DIR", code_dir)
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {})
    monkeypatch.setattr(
        loop_runtime,
        "_run",
        lambda argv, **kwargs: (
            (1, "inactive") if argv[0] in {"systemctl", "tmux"} else (0, '{"running": false}')
        ),
    )
    launched = []
    monkeypatch.setattr(
        loop_runtime,
        "_systemd_run",
        lambda repo, kind, command, **kwargs: launched.append((repo, kind, command, kwargs)) or (0, ""),
    )

    assert cli.main(["run", "widgets", "--json"]) == 0

    result = json.loads(capsys.readouterr().out)
    assert result[0]["started"] is True
    assert result[0]["repo"] == "widgets"
    assert result[0]["message"].startswith("started widgets (claude) in Herdr session ")
    repo, kind, command, options = launched[0]
    assert repo == "widgets"
    assert kind == "loop"
    assert command[1:3] == ["loop", "worker"]
    assert "--session" in command
    assert options["claude_limits"] is True
    metadata = json.loads((state_dir / "herdr-loops" / "widgets.json").read_text(encoding="utf-8"))
    assert metadata["state"] == "starting"
    assert metadata["platform"] == "claude"
    assert Path(metadata["prompt_file"]).is_file()


@pytest.mark.parametrize("argv", [["run", "--all", "--json"], ["once", "now", "--json"]])
def test_multi_repo_start_continues_after_quota_error(monkeypatch, capsys, argv):
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {
        "widgets": "omp",
        "gizmos": "claude",
    })
    attempted = []

    def start(repo, **kwargs):
        attempted.append(repo)
        if repo == "widgets":
            raise loop_runtime.LoopError("quota snapshot is stale")
        return True, "started gizmos"

    monkeypatch.setattr(loop_runtime, "start_loop", start)

    assert cli.main(argv) == 1

    assert attempted == ["widgets", "gizmos"]
    results = json.loads(capsys.readouterr().out)
    assert [(item["repo"], item["started"]) for item in results] == [
        ("widgets", False),
        ("gizmos", True),
    ]
    assert "quota snapshot is stale" in results[0]["message"]


def test_fleet_run_uses_only_per_machine_signing_keys(monkeypatch, tmp_path, capsys):
    key_dir = tmp_path / "keys"
    key_dir.mkdir()
    (key_dir / "jesus").write_text("jesus-secret\n", encoding="utf-8")
    monkeypatch.setenv("LUPIN_CMD_SIGNING_KEYS_DIR", str(key_dir))
    monkeypatch.setenv("LUPIN_CMD_SIGNING_KEY", "shared-secret")
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {"widgets": "omp"})
    monkeypatch.setattr(
        loop_runtime,
        "orchestrator_profiles",
        lambda: {"widgets": ["opencode-go/step-5-preview-free:xhigh"]},
    )
    monkeypatch.setattr(machines, "hostname", lambda: "pihome")
    monkeypatch.setattr(machines, "machines", lambda connection: [{"name": "jesus"}, {"name": "ralpha"}])
    monkeypatch.setattr(cli, "_fleet_connection", lambda args: {"redis_host": "redis"})
    dispatched = {}

    def dispatch(repos, records, **kwargs):
        dispatched.update(repos=repos, records=records, **kwargs)
        return [{"repo": "widgets", "machine": "jesus", "queued": True, "id": "cmd-1"}]

    monkeypatch.setattr(loops, "dispatch_fleet_runs", dispatch)

    assert cli.main(["fleet-run", "--json"]) == 0

    assert dispatched["signing_keys"] == {"jesus": "jesus-secret"}
    assert dispatched["local_host"] == "pihome"
    assert dispatched["repos"] == ["widgets"]
    assert json.loads(capsys.readouterr().out) == [
        {"repo": "widgets", "machine": "jesus", "queued": True, "id": "cmd-1"}
    ]



def test_fleet_run_refused_login_names_the_password_setting(auth_redis_port, monkeypatch, capsys):
    connection = {"redis_host": "127.0.0.1", "redis_port": auth_redis_port}
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {"widgets": "omp"})
    monkeypatch.setattr(cli, "_fleet_connection", lambda args: connection)

    assert cli.main(["fleet-run"]) == 3
    err = capsys.readouterr().err
    assert "refused the login" in err
    assert "Set --redis-password, LUPIN_REDIS_PASSWORD, or the systemd credential redis-password" in err
    assert "machine registry" not in err


def test_run_on_another_machine_refused_login_names_the_password_setting(
    auth_redis_port, monkeypatch, capsys
):
    connection = {"redis_host": "127.0.0.1", "redis_port": auth_redis_port}
    monkeypatch.setenv("LUPIN_CMD_SIGNING_KEY", "shared-secret")
    monkeypatch.setattr(machines, "hostname", lambda: "pihome")
    monkeypatch.setattr(cli, "_fleet_connection", lambda args: connection)

    assert cli.main(["run", "widgets", "--machine", "jesus"]) == 3
    err = capsys.readouterr().err
    assert "refused the login" in err
    assert "Set --redis-password, LUPIN_REDIS_PASSWORD, or the systemd credential redis-password" in err
    assert "For the loop start on 'jesus'." in err


def test_fleet_run_acl_denial_exits_three_and_names_the_acl(
    auth_redis_port, no_eval_kw, monkeypatch, tmp_path, capsys
):
    connection = {"redis_host": "127.0.0.1", "redis_port": auth_redis_port, **no_eval_kw}
    key_dir = tmp_path / "keys"
    key_dir.mkdir()
    (key_dir / "jesus").write_text("jesus-secret\n", encoding="utf-8")
    records = [
        {
            "name": "jesus",
            "state": "online",
            "actions": ["loop.run"],
            "repos": [{"repo": "widgets", "loopable": True}],
            "loops": [],
            "slots": {},
        }
    ]
    monkeypatch.setenv("LUPIN_CMD_SIGNING_KEYS_DIR", str(key_dir))
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {"widgets": "omp"})
    monkeypatch.setattr(machines, "hostname", lambda: "pihome")
    monkeypatch.setattr(machines, "machines", lambda connection: records)
    monkeypatch.setattr(cli, "_fleet_connection", lambda args: connection)

    assert cli.main(["fleet-run"]) == 3
    err = capsys.readouterr().err
    assert "redis denied the command" in err
    assert "User no-eval" in err
    assert "For fleet-run." in err
    assert "no-eval-pw" not in err


def test_run_on_another_machine_acl_denial_exits_three_and_names_the_acl(
    auth_redis_port, no_eval_kw, monkeypatch, capsys
):
    connection = {"redis_host": "127.0.0.1", "redis_port": auth_redis_port, **no_eval_kw}
    monkeypatch.setenv("LUPIN_CMD_SIGNING_KEY", "shared-secret")
    monkeypatch.setattr(machines, "hostname", lambda: "pihome")
    monkeypatch.setattr(cli, "_fleet_connection", lambda args: connection)

    assert cli.main(["run", "widgets", "--machine", "jesus"]) == 3
    err = capsys.readouterr().err
    assert "redis denied the command" in err
    assert "User no-eval" in err
    assert "For the loop start on 'jesus'." in err
    assert "no-eval-pw" not in err


def test_main_turns_an_uncaught_acl_denial_into_exit_three(monkeypatch, capsys):
    def denied(argv):
        raise redis_lib.exceptions.NoPermissionError("User x has no permissions to run the 'eval' command.")

    monkeypatch.setattr(cli, "_main", denied)
    assert cli.main(["status"]) == 3

    err = capsys.readouterr().err
    assert "redis denied the command" in err
    assert "a Redis step in this command" in err
    assert "Traceback" not in err


def test_fetch_benchmarks_acl_denial_exits_three_and_names_the_refresh(monkeypatch, capsys):
    def denied(**_kwargs):
        raise redis_lib.exceptions.NoPermissionError("User x has no permissions to run the 'eval' command.")

    monkeypatch.setattr(benchmark_fetch, "refresh_snapshot", denied)
    assert cli.main(["fetch-benchmarks"]) == 3

    err = capsys.readouterr().err
    assert "redis denied the command" in err
    assert "For the benchmark refresh." in err
    assert "Traceback" not in err


def test_fleet_run_queues_signed_run_to_worker(redis_port, flush_redis, monkeypatch, tmp_path, capsys):
    connection = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    key_dir = tmp_path / "keys"
    key_dir.mkdir()
    (key_dir / "jesus").write_text("jesus-secret\n", encoding="utf-8")
    records = [
        {
            "name": machine,
            "state": "online",
            "actions": ["loop.run"],
            "repos": [{"repo": "widgets", "loopable": True}],
            "loops": [],
            "slots": {},
        }
        for machine in ("jesus", "ralpha")
    ]
    monkeypatch.setenv("LUPIN_CMD_SIGNING_KEYS_DIR", str(key_dir))
    monkeypatch.setenv("LUPIN_CMD_SIGNING_KEY", "shared-secret")
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {"widgets": "omp"})
    monkeypatch.setattr(machines, "hostname", lambda: "pihome")
    monkeypatch.setattr(machines, "machines", lambda connection: records)
    monkeypatch.setattr(cli, "_fleet_connection", lambda args: connection)

    assert cli.main(["fleet-run", "--json"]) == 0

    queue = commands.get_queue("jesus", **connection)
    assert len(queue) == 1
    assert queue[0]["params"] == {"repo": "widgets"}
    assert commands.get_queue("ralpha", **connection) == []
    stored = commands._client(**connection).get(commands.cmd_key(queue[0]["id"]))
    assert commands.verify(json.loads(stored), "jesus-secret")
    assert json.loads(capsys.readouterr().out)[0]["machine"] == "jesus"


def test_future_once_defaults_to_enabled_repos(monkeypatch, tmp_path: Path, capsys):
    monkeypatch.setattr(loop_runtime, "STATE_DIR", tmp_path)
    monkeypatch.setattr(
        loop_runtime,
        "enabled_repos",
        lambda: {"widgets": "claude", "gizmos": "omp"},
    )
    monkeypatch.setattr(loop_runtime, "_service_environment", lambda: [])
    monkeypatch.setattr(loop_runtime, "_run", lambda *args, **kwargs: (0, ""))

    assert cli.main(["once", "+2h"]) == 0

    schedules = list((tmp_path / "once").glob("*.json"))
    assert len(schedules) == 1
    assert json.loads(schedules[0].read_text(encoding="utf-8"))["repos"] == [
        "widgets",
        "gizmos",
    ]
    assert capsys.readouterr().out == "scheduled widgets, gizmos for +2h\n"


def test_future_once_without_enabled_repos_does_not_schedule(monkeypatch, tmp_path: Path, capsys):
    monkeypatch.setattr(loop_runtime, "STATE_DIR", tmp_path)
    monkeypatch.setattr(loop_runtime, "enabled_repos", lambda: {})
    monkeypatch.setattr(
        loop_runtime, "_run", lambda *args, **kwargs: pytest.fail("systemd must not run")
    )

    assert cli.main(["once", "+2h"]) == 1

    assert "at least one repo is required" in capsys.readouterr().err
    assert not (tmp_path / "once").exists()


def test_loops_unreadable_repos_file_exits_one_without_traceback(monkeypatch, tmp_path: Path, capsys):
    repos = tmp_path / "repos"
    repos.write_bytes(b"\xff\n")
    monkeypatch.setattr(loop_runtime, "REPOS_FILE", repos)

    assert cli.main(["loops"]) == 1

    err = capsys.readouterr().err
    assert f"could not read {repos}" in err
    assert "Traceback" not in err



@pytest.mark.parametrize(
    "argv, action, params",
    [
        (
            ["run", "widgets", "--machine", "remote", "--platform", "omp", "--note", "review", "--resume"],
            "loop.run",
            {"repo": "widgets", "platform": "omp", "note": "review", "resume": True},
        ),
        (
            [
                "run", "widgets", "--machine", "remote", "--platform", "omp",
                "--provider", "opencode-go", "--model", "opencode-go/step-5-preview-free:xhigh",
            ],
            "loop.run",
            {
                "repo": "widgets",
                "platform": "omp",
                "provider": "opencode-go",
                "model": "opencode-go/step-5-preview-free:xhigh",
                "note": None,
                "resume": False,
            },
        ),
        (
            ["run", "--all", "--machine", "remote", "--note", "review"],
            "loop.run-all",
            {"note": "review"},
        ),
    ],
)
def test_run_routes_remote_start_through_signed_queue(argv, action, params, capsys):
    queued = {
        "mode": "queued",
        "id": "command-1",
        "result": {"id": "command-1", "state": "ok", "host": "remote", "output": "started widgets"},
    }
    argv.extend(["--signing-key", "target-key", "--json"])
    with (
        mock.patch.object(cli, "_fleet_connection", return_value={"redis_host": "redis"}),
        mock.patch.object(loops, "dispatch_loop_action", return_value=queued) as dispatch,
    ):
        code = cli.main(argv)

    assert code == 0
    assert dispatch.call_args.kwargs["machine"] == "remote"
    assert dispatch.call_args.kwargs["queue_action"] == action
    assert dispatch.call_args.kwargs["queue_params"] == params
    assert dispatch.call_args.kwargs["signing_key"] == "target-key"
    assert json.loads(capsys.readouterr().out)["output"] == "started widgets"

# --------------------------------------------------------------------------
# stop
# --------------------------------------------------------------------------


def test_stop_local_success_exits_zero(capsys):
    with mock.patch.object(loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 0, "output": ""}):
        code = cli.main(["stop", "widgets", "--machine", "h"])
    assert code == 0


def test_stop_local_failure_exits_one(capsys):
    with mock.patch.object(
        loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 1, "output": "boom"}
    ):
        code = cli.main(["stop", "widgets", "--machine", "h"])
    assert code == 1


def test_stop_json_shape(capsys):
    with mock.patch.object(loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 0, "output": "ok"}):
        code = cli.main(["stop", "widgets", "--machine", "h", "--json"])
    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(captured.out) == {"mode": "local", "returncode": 0, "output": "ok"}


def test_local_stop_runs_with_stop_time_limit(monkeypatch):
    seen = {}

    def fake_run(argv, timeout=20.0):
        seen["timeout"] = timeout
        return 0, ""

    monkeypatch.setattr(loops, "run_subprocess", fake_run)
    code = cli.main(["stop", "widgets", "--machine", "h"])
    assert code == 0
    assert seen["timeout"] == agent.ACTION_TIMEOUT_S["loop.stop"]


def test_remote_stop_queues_and_skips_local_runner(capsys):
    with (
        mock.patch.object(loops, "run_subprocess") as local_runner,
        mock.patch.object(loops.commands, "enqueue", return_value="cmd-1") as enqueue,
        mock.patch.object(loops.commands, "get_status", return_value={"state": "ok"}),
    ):
        code = cli.main(["stop", "widgets", "--machine", "other", "--signing-key", "k"])
    assert code == 0
    assert enqueue.call_args.args[:2] == ("other", "loop.stop")
    local_runner.assert_not_called()


@pytest.mark.parametrize(
    "extra, local_tail, force",
    [([], [], False), (["--force"], ["--force"], True)],
)
def test_stop_passes_force_to_local_and_queued_stops(extra, local_tail, force):
    with mock.patch.object(
        loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 0, "output": ""}
    ) as dispatch:
        cli.main(["stop", "widgets", "--machine", "h", *extra])
    kwargs = dispatch.call_args.kwargs
    assert kwargs["local_argv"] == ["lupin", "loop", "local-action", "stop", "widgets", *local_tail]
    assert kwargs["queue_params"] == {"repo": "widgets", "force": force}


def test_stop_ambiguous_machine_exits_five(capsys):
    with mock.patch.object(loops, "resolve_machine_for_repo", side_effect=loops.AmbiguousMachine("widgets", [])):
        code = cli.main(["stop", "widgets"])
    captured = capsys.readouterr()
    assert code == 5
    assert "widgets" in captured.err


def test_stop_resolves_machine_from_repo_when_not_given():
    with (
        mock.patch.object(loops, "resolve_machine_for_repo", return_value="jesus") as resolve,
        mock.patch.object(
            loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 0, "output": ""}
        ) as dispatch,
    ):
        code = cli.main(["stop", "widgets"])
    assert code == 0
    resolve.assert_called_once()
    assert resolve.call_args.args[0] == "widgets"
    assert dispatch.call_args.kwargs["machine"] == "jesus"


def test_stop_coordinator_unreachable_exits_three(capsys):
    with mock.patch.object(loops, "dispatch_loop_action", side_effect=slots.CoordinatorUnreachable("x")):
        code = cli.main(["stop", "widgets", "--machine", "jesus"])
    captured = capsys.readouterr()
    assert code == 3
    assert "cannot reach" in captured.err


def test_stop_missing_signing_key_exits_one(capsys):
    with mock.patch.object(loops, "dispatch_loop_action", side_effect=loops.MissingSigningKey("jesus")):
        code = cli.main(["stop", "widgets", "--machine", "jesus"])
    captured = capsys.readouterr()
    assert code == 1
    assert "signing-key" in captured.err


def test_stop_coordinator_unreachable_while_resolving_machine_exits_three(capsys):
    """A Redis outage while resolving which machine runs the repo must
    exit 3 ("cannot reach"), not 5 ("use --machine") -- picking a
    different machine would not fix an unreachable coordinator."""
    with mock.patch.object(loops, "resolve_machine_for_repo", side_effect=machines.CoordinatorUnreachable("x")):
        code = cli.main(["stop", "widgets"])
    captured = capsys.readouterr()
    assert code == 3
    assert "cannot reach" in captured.err


def test_stop_remote_ok_exits_zero():
    with mock.patch.object(
        loops, "dispatch_loop_action",
        return_value={"mode": "queued", "id": "abc123", "result": {"id": "abc123", "state": "ok"}},
    ):
        code = cli.main(["stop", "widgets", "--machine", "jesus", "--signing-key", "s"])
    assert code == 0


def test_stop_remote_still_running_exits_four():
    with mock.patch.object(
        loops, "dispatch_loop_action",
        return_value={"mode": "queued", "id": "abc123", "result": {"id": "abc123", "state": "running"}},
    ):
        code = cli.main(["stop", "widgets", "--machine", "jesus", "--signing-key", "s"])
    assert code == 4


def test_stop_remote_unknown_result_exits_four():
    with mock.patch.object(
        loops, "dispatch_loop_action", return_value={"mode": "queued", "id": "abc123", "result": None}
    ):
        code = cli.main(["stop", "widgets", "--machine", "jesus", "--signing-key", "s"])
    assert code == 4


def test_stop_remote_failed_exits_one():
    with mock.patch.object(
        loops, "dispatch_loop_action",
        return_value={"mode": "queued", "id": "abc123", "result": {"id": "abc123", "state": "failed"}},
    ):
        code = cli.main(["stop", "widgets", "--machine", "jesus", "--signing-key", "s"])
    assert code == 1


# --------------------------------------------------------------------------
# peek
# --------------------------------------------------------------------------


def test_peek_default_lines_is_sixty():
    with mock.patch.object(
        loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 0, "output": "pane"}
    ) as dispatch:
        code = cli.main(["peek", "widgets", "--machine", "h"])
    assert code == 0
    assert dispatch.call_args.kwargs["local_argv"] == ["lupin", "loop", "local-action", "peek", "widgets", "60"]
    assert dispatch.call_args.kwargs["queue_params"] == {"repo": "widgets", "lines": 60}


def test_peek_custom_lines():
    with mock.patch.object(
        loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 0, "output": ""}
    ) as dispatch:
        cli.main(["peek", "widgets", "20", "--machine", "h"])
    assert dispatch.call_args.kwargs["local_argv"] == ["lupin", "loop", "local-action", "peek", "widgets", "20"]


def test_peek_prints_pane_output(capsys):
    with mock.patch.object(
        loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 0, "output": "pane text\n"}
    ):
        cli.main(["peek", "widgets", "--machine", "h"])
    assert "pane text" in capsys.readouterr().out


# --------------------------------------------------------------------------
# attach
# --------------------------------------------------------------------------


def test_attach_local_print_shows_herdr_argv(capsys):
    code = cli.main(["attach", "widgets", "--machine", "h", "--print"])
    assert code == 0
    assert capsys.readouterr().out.strip() == f"{loop_runtime.HERDR} --session {loop_runtime.session_name('widgets')}"


def test_attach_remote_print_shows_herdr_remote_argv(capsys):
    with mock.patch.object(loops, "ssh_target_for", return_value="ghosta@jesus.local"):
        code = cli.main(["attach", "widgets", "--machine", "jesus", "--print"])
    assert code == 0
    assert capsys.readouterr().out.strip() == (
        f"{loop_runtime.HERDR} --remote ghosta@jesus.local --session {loop_runtime.session_name('widgets')}"
    )


def test_attach_remote_without_ssh_target_exits_one(capsys):
    with mock.patch.object(loops, "ssh_target_for", return_value=None):
        code = cli.main(["attach", "widgets", "--machine", "jesus", "--print"])
    captured = capsys.readouterr()
    assert code == 1
    assert "ssh-targets" in captured.err or "ssh target" in captured.err


def test_attach_ambiguous_machine_exits_five(capsys):
    with mock.patch.object(loops, "resolve_machine_for_repo", side_effect=loops.AmbiguousMachine("widgets", [])):
        code = cli.main(["attach", "widgets", "--print"])
    assert code == 5


def test_attach_coordinator_unreachable_while_resolving_machine_exits_three(capsys):
    with mock.patch.object(loops, "resolve_machine_for_repo", side_effect=machines.CoordinatorUnreachable("x")):
        code = cli.main(["attach", "widgets", "--print"])
    captured = capsys.readouterr()
    assert code == 3
    assert "cannot reach" in captured.err


def test_attach_execs_when_not_print(monkeypatch):
    # Real `os.execvp` replaces this process and never returns -- `_cmd_attach`
    # has no `return` after that call (see its `# pragma: no cover` line), so
    # `sys.exit(None)` is what actually runs the process down, with exit code
    # 0. The mock below lets the call return instead of exec'ing, which is
    # why `cli.main` gives back `None` here rather than `0`.
    calls = []
    monkeypatch.setattr(cli.os, "execvp", lambda prog, argv: calls.append((prog, argv)))
    code = cli.main(["attach", "widgets", "--machine", "h"])
    assert code is None
    command = [loop_runtime.HERDR, "--session", loop_runtime.session_name("widgets")]
    assert calls == [(loop_runtime.HERDR, command)]


# --------------------------------------------------------------------------
# schedule
# --------------------------------------------------------------------------


def test_schedule_bare_means_show():
    with mock.patch.object(
        loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 0, "output": ""}
    ) as dispatch:
        code = cli.main(["schedule", "--machine", "h"])
    assert code == 0
    assert dispatch.call_args.kwargs["local_argv"] == ["lupin", "loop", "local-action", "schedule"]
    assert dispatch.call_args.kwargs["queue_action"] == "schedule.show"


def test_schedule_cal_builds_expected_argv():
    with mock.patch.object(
        loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 0, "output": ""}
    ) as dispatch:
        code = cli.main(["schedule", "--machine", "h", "cal", "*-*-* 00/5:00:00"])
    assert code == 0
    assert dispatch.call_args.kwargs["local_argv"] == [
        "lupin", "loop", "local-action", "schedule", "cal", "*-*-* 00/5:00:00"
    ]
    assert dispatch.call_args.kwargs["queue_params"] == {"mode": "cal", "expr": "*-*-* 00/5:00:00"}


def test_schedule_cal_with_newline_is_rejected_before_dispatch(capsys):
    with mock.patch.object(loops, "dispatch_loop_action") as dispatch:
        code = cli.main(["schedule", "--machine", "h", "cal", "bad\nexpr"])
    assert code == 1
    dispatch.assert_not_called()
    assert capsys.readouterr().err  # some explanation was printed


def test_schedule_first_builds_expected_argv():
    with mock.patch.object(
        loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 0, "output": ""}
    ) as dispatch:
        code = cli.main(["schedule", "--machine", "h", "first", "+2h5m", "every", "5h15m"])
    assert code == 0
    assert dispatch.call_args.kwargs["local_argv"] == [
        "lupin", "loop", "local-action", "schedule", "first", "+2h5m", "every", "5h15m"
    ]
    assert dispatch.call_args.kwargs["queue_params"] == {"mode": "first", "when": "+2h5m", "interval": "5h15m"}


def test_schedule_first_with_newline_is_rejected_before_dispatch(capsys):
    """`validate_schedule_token` rejects a newline in `when`/`interval` --
    the "first" branch must catch that `RejectedCommand` itself, the same
    way the "cal" branch already does, instead of letting it crash out
    of `cli.main` uncaught."""
    with mock.patch.object(loops, "dispatch_loop_action") as dispatch:
        code = cli.main(["schedule", "--machine", "h", "first", "bad\nwhen", "every", "5h"])
    assert code == 1
    dispatch.assert_not_called()
    assert capsys.readouterr().err


def test_schedule_first_rejects_wrong_every_literal(capsys):
    """The middle token must be the literal word "every" -- anything else
    is a malformed invocation, not a stand-in for the keyword."""
    with mock.patch.object(loops, "dispatch_loop_action") as dispatch:
        code = cli.main(["schedule", "--machine", "h", "first", "+2h5m", "xyz", "5h15m"])
    assert code == 1
    dispatch.assert_not_called()
    assert capsys.readouterr().err


def test_schedule_defaults_machine_to_local_host():
    with mock.patch.object(
        loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 0, "output": ""}
    ) as dispatch:
        cli.main(["schedule"])
    assert dispatch.call_args.kwargs["machine"] == "h"


# --------------------------------------------------------------------------
# pause / resume
# --------------------------------------------------------------------------


def test_pause_single_machine_default_local():
    with mock.patch.object(
        loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 0, "output": ""}
    ) as dispatch:
        code = cli.main(["pause"])
    assert code == 0
    assert dispatch.call_args.kwargs["machine"] == "h"
    assert dispatch.call_args.kwargs["local_argv"] == ["lupin", "loop", "local-action", "pause"]
    assert dispatch.call_args.kwargs["queue_action"] == "schedule.pause"


def test_resume_single_machine():
    with mock.patch.object(
        loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 0, "output": ""}
    ) as dispatch:
        code = cli.main(["resume", "--machine", "jesus", "--signing-key", "s"])
    assert code == 0
    assert dispatch.call_args.kwargs["machine"] == "jesus"
    assert dispatch.call_args.kwargs["queue_action"] == "schedule.resume"


def test_pause_all_dispatches_to_every_registered_machine(capsys):
    records = [{"name": "jesus"}, {"name": "mini"}]
    with (
        mock.patch.object(machines, "machines", return_value=records),
        mock.patch.object(
            loops, "dispatch_loop_action", return_value={"mode": "local", "returncode": 0, "output": ""}
        ) as dispatch,
    ):
        code = cli.main(["pause", "--all", "--json"])
    assert code == 0
    assert sorted(call.kwargs["machine"] for call in dispatch.call_args_list) == ["jesus", "mini"]
    payload = json.loads(capsys.readouterr().out)
    assert set(payload) == {"jesus", "mini"}


def test_pause_all_worst_exit_code_wins(capsys):
    records = [{"name": "jesus"}, {"name": "mini"}]

    def fake_dispatch(*, machine, **kwargs):
        if machine == "jesus":
            return {"mode": "local", "returncode": 0, "output": ""}
        return {"mode": "local", "returncode": 1, "output": "boom"}

    with (
        mock.patch.object(machines, "machines", return_value=records),
        mock.patch.object(loops, "dispatch_loop_action", side_effect=fake_dispatch),
    ):
        code = cli.main(["pause", "--all"])
    assert code == 1


def test_pause_all_continues_past_one_machines_unreachable_coordinator(capsys):
    """One machine's `CoordinatorUnreachable` must not abort the whole
    `--all` fan-out -- the other machines still get tried, and their
    results (plus the failing machine's own) are still reported."""
    records = [{"name": "jesus"}, {"name": "mini"}]

    def fake_dispatch(*, machine, **kwargs):
        if machine == "jesus":
            raise slots.CoordinatorUnreachable("x")
        return {"mode": "local", "returncode": 0, "output": ""}

    with (
        mock.patch.object(machines, "machines", return_value=records),
        mock.patch.object(loops, "dispatch_loop_action", side_effect=fake_dispatch) as dispatch,
    ):
        code = cli.main(["pause", "--all", "--json"])
    assert sorted(call.kwargs["machine"] for call in dispatch.call_args_list) == ["jesus", "mini"]
    assert code == 3
    payload = json.loads(capsys.readouterr().out)
    assert set(payload) == {"jesus", "mini"}
    assert "error" in payload["jesus"]


def test_pause_all_worst_exit_code_is_order_independent():
    """The same set of per-machine outcomes must give the same overall
    exit code no matter which machine's result came back first -- not
    "whichever non-zero code showed up first"."""
    records = [{"name": "jesus"}, {"name": "mini"}]

    def dispatch_unreachable_then_failed(*, machine, **kwargs):
        if machine == "jesus":
            raise slots.CoordinatorUnreachable("x")
        return {"mode": "local", "returncode": 1, "output": "boom"}

    def dispatch_failed_then_unreachable(*, machine, **kwargs):
        if machine == "jesus":
            return {"mode": "local", "returncode": 1, "output": "boom"}
        raise slots.CoordinatorUnreachable("x")

    with (
        mock.patch.object(machines, "machines", return_value=records),
        mock.patch.object(loops, "dispatch_loop_action", side_effect=dispatch_unreachable_then_failed),
    ):
        code_a = cli.main(["pause", "--all", "--json"])

    with (
        mock.patch.object(machines, "machines", return_value=records),
        mock.patch.object(loops, "dispatch_loop_action", side_effect=dispatch_failed_then_unreachable),
    ):
        code_b = cli.main(["pause", "--all", "--json"])

    assert code_a == code_b == 3


def test_pause_all_severity_beats_first_non_zero_wins():
    """Exit 1 (a real failure) must outrank exit 4 (result unknown) no
    matter which machine reports which -- the old "first non-zero wins"
    rule would have given 4 in one of these two orderings and 1 in the
    other, for the exact same two outcomes."""
    records = [{"name": "jesus"}, {"name": "mini"}]
    unknown = {"mode": "queued", "id": "x", "result": {"id": "x", "state": "running"}}
    failed = {"mode": "local", "returncode": 1, "output": "boom"}

    def jesus_unknown_mini_failed(*, machine, **kwargs):
        return unknown if machine == "jesus" else failed

    def jesus_failed_mini_unknown(*, machine, **kwargs):
        return failed if machine == "jesus" else unknown

    with (
        mock.patch.object(machines, "machines", return_value=records),
        mock.patch.object(loops, "dispatch_loop_action", side_effect=jesus_unknown_mini_failed),
    ):
        code_a = cli.main(["pause", "--all"])

    with (
        mock.patch.object(machines, "machines", return_value=records),
        mock.patch.object(loops, "dispatch_loop_action", side_effect=jesus_failed_mini_unknown),
    ):
        code_b = cli.main(["pause", "--all"])

    assert code_a == code_b == 1
