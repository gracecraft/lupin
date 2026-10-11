# lupin — the delegation loop

This file gives loop-specific guidance for this repo. It is optional; Lupin
can run without it. Read the root `AGENTS.md` first.

## Where this repo lives

This repo is `gracecraft-software/lupin`. The owner clones it to
`~/Code/Projects/lupin` and OrbStack mounts it into the `jesus` sandbox at
`/code/lupin`, the same way `ghostbook.nix` is mounted at
`/code/ghostbook.nix`.

A loop agent does not edit the main checkout in `/code/lupin`. Each run gets
its own Git worktree in the loop state directory. See "State" in `AGENTS.md`.

## Rule 1: push to the fork, not to the upstream repo

The sandbox `gh` account is `gracecraft-ro`. It cannot push to
`gracecraft-software/lupin` (`gh api repos/gracecraft-software/lupin --jq
.permissions` -> `push: false, triage: false`). It owns a fork,
`gracecraft-ro/lupin`, and can push there. So a worker pushes to the fork. It opens
the PR on the fork, against `release/next`. Closing or labeling an issue can
fail without triage access. Check before you write it into a report.

1. Fetch the fork. From the repo checkout, run:

   ```bash
   git fetch fork
   ```

2. Create a worktree for your branch. Keep its path under `.claude/worktrees/`.
   Run:

   ```bash
   git worktree add <path> -b <branch> fork/release/next
   ```

3. Push with `git push fork <branch>`. Never push to `origin`, `main`, or
   `release/next`. Never run `git remote add`. Do not use the commit and PR
   section of `/ship`. It pushes to `origin` when direct push is allowed, and it
   can add a remote. Open the PR on the fork:

   <!-- markdownlint-disable MD013 -->
   ```sh
   gh pr create --repo gracecraft-ro/<repo> --base release/next --title "<title>" --body "Closes #N"
   ```
   <!-- markdownlint-enable MD013 -->

   If the PR opens, report its number to the orchestrator (the agent that
   dispatches and merges work). The orchestrator dispatches `/code-review`.

On a Herdr worker, use the Herdr worktree commands:

```bash
herdr worktree list --cwd /code/lupin
git -C /code/lupin fetch fork
herdr worktree create --branch <branch> --base fork/release/next --cwd /code/lupin
```

A linked worktree shares its remotes with the checkout it came from. If that
checkout has no `fork` remote, stop. Then report the missing remote to the
orchestrator. Do not add a remote.

`lupin run` still starts each loop from `origin/HEAD`, which is upstream `main`.
`origin/HEAD` is the default branch on `origin`. `lupin run` does not start from
`fork/release/next`. Until the owner changes `lupin run`, do not use it for
feature work. Create feature worktrees with the Herdr worktree commands above,
or the manual steps in rule 1.

## Pull request and review

Every feature change goes through a pull request. The shared steps are in
"Review and merge each pull request" in the `delegation-loop` skill. In this
repo:

1. The base branch is the integration branch, `release/next`, on the fork
   (`gracecraft-ro/lupin`). A feature PR never targets upstream `main`.
2. A worker uses these `/ship` sections. They are "Check the issue and
   checkout", "Implement and verify", "Save and attach evidence", and "Report
   and release". It does not use its commit and PR section. It follows rule 1
   for the push and the PR. If the push fails, report the branch name and commit
   range. Do not merge that branch.
3. The orchestrator (the agent that dispatches and merges work) dispatches
   `/code-review` for every PR, including docs-only changes. The reviewer is
   not the worker. The reviewer's model tier is not lower than the worker's.
4. If the review finds a problem, dispatch a fix worker with the exact
   finding. It pushes the fix to the same branch with `git push fork <branch>`.
   Repeat until the reviewer approves the current head SHA. The head SHA is the
   newest commit ID on the branch.
5. The orchestrator posts the verdict on the PR.
6. The orchestrator merges into `release/next` only when all four are true:
   1. The PR is open on the fork, `gracecraft-ro/lupin`, with base
      `release/next`.
   2. A reviewer approves the current head SHA.
   3. The fork branch contains the current `release/next`. The ancestor check
      asks git whether one branch contains another. The check is in the
      `delegation-loop` skill.
   4. The repo's full gate, as its `AGENTS.md` defines it, passes.
   Record the approved SHA before you merge. Merge with
   `gh pr merge <PR-number> --repo gracecraft-ro/lupin --merge
   --match-head-commit <approved-SHA>`. Do not rebase. Do not force-push.
   See the paragraph that starts "Before a branch is merged" in the
   `delegation-loop` skill. A worker or reviewer never merges a pull request.
   The PR body has `Closes #N`. The merge into `release/next` does not close
   the issue. The orchestrator (the agent that dispatches and merges work)
   closes it manually, in the same work session.

Changes to this policy go to upstream `main`, which the owner merges. Feature
work goes to fork `release/next`.

At the end of a session, run `/handoff`.

### Preview server

This repo follows "Preview server" in the `delegation-loop` skill. For this
repo:

- The start command and port are in "Preview server" in `AGENTS.md`. The
  machine is not named yet.
- Set `LUPIN_LOOP_STATE_DIR` to a preview-only path. The default is
  `/var/lib/delegation-loop`, which is the real fleet state.
- Do not set `LUPIN_REDIS_HOST` to the fleet Redis unless the test needs it.
- The state directory does not change the Herdr session. New runs use the
  `lupin-loops` session (`SESSION_NAME` in `src/lupin/loop_runtime.py`). Older
  loops may use an old session until they stop.
- Do not use the start, stop, or run controls on the preview dashboard.

## Other rules

This repo is a plain Python package and a Nix flake, not a live system
config. It carries none of `ghostbook.nix`'s extra danger rules (no
activation scripts, no machine state to break). The one rule that still
applies is rule 1 above — push to the fork, not to the upstream repo.

Before you merge or dispatch anything, check for work already done:

```bash
git -C /code/lupin fetch fork
git -C /code/lupin branch --no-merged fork/release/next
```
