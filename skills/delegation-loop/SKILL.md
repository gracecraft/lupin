---
name: delegation-loop
description: >-
  Coordinate an issue backlog with Lupin. Use this skill when starting or
  continuing an issue loop, selecting work for agents, watching a fleet, or
  handing off a loop run.
compatibility: >-
  Requires Lupin, GitHub CLI, an agent runner, and the `delegation-loop`,
  `triage`, `ship`, `code-review`, and `handoff` skills on each worker host. Redis is
  required for fleet claims.
---

# Run a Lupin delegation loop

Read the root `AGENTS.md` first. If `docs/delegation-loop.md` exists, read
it; Lupin can run without it. Follow the repo's branch, worktree, test, and
release rules. Read the latest issue comments.

Upstream repos (`gracecraft-software/<repo>`) are read-only for loops. A loop
does not push to upstream. It does not merge into upstream.

Each repo has one integration branch on the fork (`gracecraft-ro/<repo>`),
named `release/next`. It starts from the upstream default branch. The fork
default branch is not used for work.

Use an isolated worktree for each worker. Do not let parallel workers edit the
same checkout. Each worker creates its worktree from the integration branch:

```sh
git fetch fork
git worktree add <path> -b <branch> fork/release/next
```

Keep each worktree under `.claude/worktrees/`. Record the machine, worktree
path, branch, and issue in the dispatch and the repository's handoff record.
When a worker runs in a Herdr pane, record its agent name, workspace ID, pane
ID, tab ID, and cwd.
`herdr agent list` reports all of them. See "Talk to an agent in a Herdr
pane" below.

## Install skills on loop hosts

The `skills/` tree is the source. Installing the Lupin command does not
install these files. A Nix host flake maps the five required directories from
its pinned Lupin input into each runner's skill set. A rebuild creates or
updates the links; do not copy the files by hand. The `ui-pressure-test`
skill is optional. Add it to a selected runner when a task needs visual QA.

- Claude Code: `~/.claude/skills/`
- Pi: `~/.pi/agent/skills/`
- OMP: `~/.omp/agent/skills/`

Add all five required names to each host's skill set, including any curated
subset. Add `ui-pressure-test` to hosts that run UI pressure tests. After a
rebuild, check that every required `SKILL.md` is readable:

```sh
for root in "$HOME/.claude/skills" "$HOME/.pi/agent/skills" "$HOME/.omp/agent/skills"; do
  for name in delegation-loop triage ship code-review handoff; do
    test -r "$root/$name/SKILL.md" || {
      echo "Missing $root/$name/SKILL.md" >&2
      exit 1
    }
  done
done
```

Start a new agent session and confirm it loads `/delegation-loop`, `/triage`,
`/ship`, `/code-review`, and `/handoff`. If the task needs UI pressure testing, also
confirm it loads `/ui-pressure-test`. Do not start a worker if a skill it
needs is missing.

## Read the backlog

From a configured checkout, run:

```sh
lupin roadmap --stage all --json
lupin roadmap --dag --json
lupin review-route --prefetch 123 --repo OWNER/REPO
lupin place 123 --json
```

The roadmap ranks issues. The DAG view shows dependencies. Prefetch gets the
latest issue or pull request text and comments. Placement recommends a model,
effort, and machine. It does not claim work or start an agent.

Use `/triage` to decide what is ready. Read the latest comments and repo
instructions. Check likely files and live work before you run issues in
parallel. Run issues together only when their work does not overlap.

`lupin quota` displays the shared Redis snapshot and refreshes it with local
provider data when this machine has credentials. `route()` still reads quota
from the local machine, so the two results may differ.

## Claim and label work

Use the same issue and holder to claim, renew, and release work:

```sh
lupin claim OWNER/REPO#123 --holder SESSION
lupin renew-claim OWNER/REPO#123 --holder SESSION
lupin release-claim OWNER/REPO#123 --holder SESSION
```

By default, Lupin expires a claim after 10 minutes. Use `--ttl SECONDS` to
change this limit. Renew every 2 minutes while work continues. Each renewal
starts the timer again. If the claim is not renewed, Redis removes it when the
timer ends. `lupin reconcile` runs once per call. It releases claims with no
renewal for 10 minutes, even when `--ttl` is longer. Its schedule sets the
cleanup delay.

Lupin does not yet sync claims to GitHub. Issue #43 tracks automatic
`claimed` label updates. Until it ships, the claim owner must update the issue
manually. Use the repo's `claimed` label. Add it during repo setup if it does
not exist. If you cannot add it, post the comment and report that the label is
missing. Include the machine, worktree, branch, and holder:

```sh
gh issue edit 123 --repo OWNER/REPO --add-label claimed
gh issue comment 123 --repo OWNER/REPO \
  --body "Claimed: machine=HOST; worktree=PATH; branch=BRANCH; holder=SESSION"
```

When the claim ends, remove the label and post the result, including the PR
number or reason the work stopped:

```sh
gh issue edit 123 --repo OWNER/REPO --remove-label claimed
gh issue comment 123 --repo OWNER/REPO --body "Released: PR #123"
```

The claim owner makes these updates; workers must not post duplicate claim
comments. If the Redis claim fails, do not mark it as claimed. Follow the repo's
local coordination rule instead. If a lease expires or `lupin reconcile`
releases it, check that the GitHub label and comment also match.

A claim needs Redis. If Redis is not available, Lupin has no local claim
fallback. Do not report a Lupin claim as active.

For related issues, `lupin quest start --issue 123 --issue 124` claims and
orders the issues, then registers a quest on a machine. It does not start an
agent. Do not claim those issues again.

## Dispatch work

Give each worker a short brief with:

- Issue goal and acceptance checks.
- Latest comments and open dependencies.
- Likely files and known parallel work.
- Model, effort, and machine recommendation.
- Claim or quest ID.
- Machine, absolute worktree path, and branch.
- Required tests and smoke checks.

Tell implementation workers to follow `AGENTS.md`, rule 1 under "Pull requests,
review, and handoff". Do not repeat the `/ship` implementation checklist.

When an issue makes a major change to a web app's user interface or key user
journey, and a preview is ready, dispatch a separate QA task with
`ui-pressure-test`. Keep the test agent read-only with respect to source code,
and keep its browser data separate from implementation work. Ask for evidence,
prioritized tickets, and a list of untested items. Do not ask the test agent to
fix findings.
Use the runner's worktree isolation feature when it has one. Otherwise, create
a separate worktree and verify the worker's working directory. On a Herdr
worker, use the Herdr worktree commands:

```sh
herdr worktree list --cwd "$PWD"
git fetch fork
herdr worktree create --branch BRANCH --base fork/release/next --cwd "$PWD"
herdr worktree open --path PATH --cwd "$PWD"
```

`--trust-repository` gives Git trust for one command. Use it only after the
owner verified the repository. Do not use it as a retry for a failed worktree
command.

## Communicate with workers

Use the agent runner's prompt and message tools for its own workers. Lupin's
remote commands control loops. They do not send free-form messages.

### Talk to an agent in a Herdr pane

A Lupin loop runs in one Herdr session on its machine. That session is
`lupin-loops`. Every loop is one workspace in it, and every workspace has one
named agent in it. A worker that the runner starts itself runs in the runner's
own session instead.

Herdr is the only way to send a message to a running agent. Read the agent
name from `herdr agent list`; do not guess it. An agent name is valid only
while that agent is running.

```sh
herdr agent list                                   # read the name from the output
herdr agent prompt REPO_NAME "report progress" --wait --timeout 120000
herdr agent read REPO_NAME --source recent-unwrapped --lines 120
```

The timeout value is in milliseconds. Omit `--timeout` only for a wait that
can last a long time; a prompt without it can wait forever.

Use `--wait` for normal work. It returns when the agent is ready for input.
Add `--until blocked` only when you want to wait for a question or an
approval. Use `--source detection` only when you check why Herdr does not
recognize the agent.

Agent states:

|State|Meaning|What to do|
|---|---|---|
|`idle`|Ready for input|Send the next prompt|
|`done`|Ready for input|Send the next prompt. The CLI state decides this, not the badge in the app|
|`working`|The agent is busy|Wait or read output|
|`blocked`|The agent stopped on a question or an approval|Read the output, then ask the owner. Do not answer for the owner|
|`unknown`|Herdr cannot tell what the agent does|Not proof that it finished. Check the pane yourself|

A timeout does not prove the text arrived, and a timeout does not prove it
did not arrive. A prompt that reports a stall can also be delivered. Read the
agent before you send the text again.

To read output from a pane, or to run a test or a smoke check beside the
worker, use the pane commands. `pane run` sends one command, and
`pane wait-output` returns when the output matches:

```sh
herdr pane run <pane_id> "pytest -q"
herdr pane wait-output <pane_id> --match "passed" --timeout 120000
herdr pane read <pane_id> --source recent-unwrapped --lines 120
```

Address panes by pane ID, or with `--current` for your own pane. Never use
another client's focused pane.

To start a worker in a Herdr pane, make the pane first, then start the agent
in it. `agent start` needs a shell that waits at its prompt. It never makes,
splits, or moves a pane:

```sh
herdr pane split --current --direction right --cwd "$PWD" --no-focus
herdr agent start worker-1 --kind omp --pane <pane-id-from-above>
```

The kind must be an installed one. `herdr integration status` lists them. An
agent with a kind that Herdr does not know shows as `unknown`.

### Read or control a loop on another machine

```sh
herdr --machine MACHINE agent list
lupin peek REPO --machine MACHINE
lupin attach REPO --machine MACHINE
lupin stop REPO --machine MACHINE
lupin schedule --machine MACHINE
lupin pause --machine MACHINE
lupin resume --machine MACHINE
```

`peek` reads output. `attach` opens the loop terminal. The other commands
control the loop or its schedule.

`herdr --machine MACHINE` must come before the subcommand on every command,
including the discovery command. It cannot be used with `--session` or
`--remote`. It uses the saved machine profile and its default session, so it
does not reach the `lupin-loops` session. To reach the loops on another
machine, use `lupin attach REPO --machine MACHINE`.

Locally, address a loop with the session name:

```sh
herdr --session lupin-loops agent list
```

## Review and merge each pull request

Dispatch `/code-review` for every pull request, including docs-only changes.
Review the current PR diff, not only the issue or a worker's report. Re-fetch
the latest PR comments and reviews before merge.

A worker pushes only to the fork, with `git push fork <branch>`. It does not
push to upstream. The PR base is `release/next` on the fork:

<!-- markdownlint-disable MD013 -->
```sh
gh pr create --repo gracecraft-ro/<repo> --base release/next --title "<title>" --body "Closes #N"
```
<!-- markdownlint-enable MD013 -->

The PR body has `Closes #N`. The merge into `release/next` does not close the
issue. The orchestrator (the agent that dispatches and merges work) closes it
manually, in the same work session.

Do not open a feature PR against upstream `main`. If the push to the fork
fails, the worker stops. The worker reports the local branch name and commit
range. Do not review or merge the branch.

Do not use the commit and PR section of `/ship`. It can push to `origin` and
open an upstream PR. After the PR opens, report its number to the orchestrator.

Use `lupin review-route --category CATEGORY --size SIZE --mode separate` for
a reviewer recommendation. Compare it with the issue's implementation route
and the worker's model tier.

To save tokens, one reviewer may review several small, independent PRs in one
dispatch. Keep a separate verdict for each PR. Review complex or high-risk
changes alone. Keep visual or 3D reviews to one or two PRs per dispatch.

The reviewer must be capable of doing the issue and must not be below the
worker's model tier. Prefer a reviewer one tier higher when quota allows.
Never dispatch Fable without the user's approval. Use Opus sparingly because
it costs more.

If the review finds a problem, dispatch a `fix` worker with the exact finding.
Tell it to push with `git push fork <branch>` and update the same PR. Review
the latest PR commit. Repeat until the reviewer approves the current head SHA.
The head SHA is the newest commit ID on the branch. The orchestrator (the agent
that dispatches and merges work) posts the verdict and findings on the PR.

Before a branch is merged, it must contain the current `release/next`. Fetch
the fork first. Then run this check. `BRANCH` is the PR branch name:

```sh
git fetch fork
git merge-base --is-ancestor fork/release/next fork/BRANCH
```

Exit code 0 means the fork branch contains `release/next`. Then continue to the
merge rules below. Exit code 1 means it does not. Use this step only for an open
fork PR that is approved at its current head SHA. The worker syncs the PR branch
with `git merge fork/release/next`. A sync is not a merge of the pull request.
If the sync has conflicts, report them and stop. If it has no conflicts, push
the branch to the fork. Run the check again. If it exits 0, get a new approval
at the new head (see the next paragraph). Then continue to the merge rules.
Otherwise stop. Do not rebase. Any other exit code from the first check means
the check failed. Report it and stop. Do not merge.

An approval binds to the head SHA. If the sync changed the head, get a new
approval at the new head before you merge. The reviewer may limit that review
to the files the sync changed. The gate (the set of checks that `AGENTS.md`
defines) runs in every case.

The orchestrator merges into `release/next` only when all four are true:

- The PR is open on the fork, `gracecraft-ro/<repo>`, with base `release/next`.
- A reviewer approves the current head SHA.
- The fork branch contains the current `release/next`. The ancestor check
  asks git whether one branch contains another. The check is in the
  `delegation-loop` skill.
- The repo's full gate, as its `AGENTS.md` defines it, passes.

The reviewer is a different agent from the worker. The gate command is in the
repo's `AGENTS.md`. If the repo has no gate command, report that. Do not merge.

A branch with no fork PR is not merged.

When all four conditions above are true, merge the PR with this command. No
owner sign-off is needed. An approval at the current head SHA is still required.
Record the approved SHA before you merge. The command creates a merge commit:

```sh
gh pr merge <PR-number> --repo gracecraft-ro/<repo> --merge \
  --match-head-commit <approved-SHA>
```

Do not rebase. Do not force-push. A worker or reviewer never merges a pull
request.

### Preview server

Each repo runs one preview server. It serves the repo's `release/next` only.
Do not run a server for a feature branch.

1. Create the preview worktree once. Detached means the worktree follows a
   commit, not a branch. From the repo root, run:

   ```sh
   git fetch fork
   git worktree add --detach .claude/worktrees/preview fork/release/next
   ```

2. After each merge into `release/next`, the orchestrator updates the preview.
   From the repo root, run:

   ```sh
   git -C .claude/worktrees/preview fetch fork
   git -C .claude/worktrees/preview checkout --detach fork/release/next
   ```

   Then it restarts the server.
3. To bind means to choose the address a server listens on. Bind the server to
   `127.0.0.1` or to the Tailscale IP address. Tailscale is a private network
   that links your own machines. Never bind to `0.0.0.0`.
4. Set `LUPIN_LOOP_STATE_DIR` to a scratch path. Do not point the preview at
   the real fleet Redis unless the test needs it.
5. Keep the server running in a Herdr pane or with `systemd-run --user`. The
   `systemd-run` command starts a command as a background service. The server
   must keep running after an agent session ends.
6. The repo's `AGENTS.md` has a "Preview server" section. It names the start
   command, the port, and the machine. It gives the tunnel command,
   `ssh -N -L PORT:127.0.0.1:PORT MACHINE`, and the local URL,
   `http://localhost:PORT`. The port must be free on that machine. Check with
   `ss -ltn`. The `ss` command lists listening ports. If the repo has no
   runnable app, that section says `Preview server: none` and gives the reason.
7. A PR does not need a tunnel command. The PR body says whether the change is
   visible only after the merge.

## Monitor and finish

Use `lupin machines`, `lupin peek`, and `lupin attach` to check live work. On
a Herdr worker, `herdr --session lupin-loops agent list` shows each loop's
state, workspace, and pane in one call. Read the final diff and run the
repo's required gate and a smoke check. A worker's success report is not
proof. Re-read issue and PR comments before closure.

After the merge and the preview restart, check that the preview port answers.
Run `curl -fsS -o /dev/null http://127.0.0.1:PORT/` on the preview machine. Use
the Tailscale IP address instead if the server binds to one. Exit code 0
means the port answers. Report the result in the handoff.

Append dispatch and handoff events to the shared Redis ledger:

```sh
lupin ledger append OWNER/REPO --event dispatch --issue N \
  --status running --branch BRANCH
lupin ledger append OWNER/REPO --event handoff --issue N --status STATUS \
  --summary TEXT --highlights TEXT --evidence TEXT --decisions TEXT --next TEXT
lupin ledger read OWNER/REPO --json
```

Add `--child N` for each split issue. If Redis is unavailable, ledger
commands exit 3. The roadmap shows no ledger annotations and adds a warning.
Use `.loop/loop-state.json` only when a ledger command exits 3. The
`handoff` skill gives the format. At the start of a session, read the ledger
and that file.

A worker's task ends when its issue is merged into `release/next`. A task also
ends when it is blocked on an owner decision. A pull request that waits for
review is not blocked. The orchestrator dispatches the review and any fix. The
fix goes on the same branch.

"Blocked" means an owner decision is needed. An example is a model-routing
policy change.

Upstream `main` is owner-only. The owner opens one pull request from
`release/next` on `gracecraft-ro/<repo>` to `main` on
`gracecraft-software/<repo>` when they choose. Loops do not wait for it.

Continue working until the backlog is empty or every remaining item is blocked
on an owner decision. Then run `/handoff`. State the exact next action and any
missing owner input.
