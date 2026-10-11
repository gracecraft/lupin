# lupin

`lupin` is a tool for multi-machine task loops. It does four jobs:

1. It picks a model and effort level for a task. Commands: `route`,
   `classify`, `review-route`. `fetch-models` fetches a daily snapshot of
   which models each subscription can call today, and their prices.
2. It controls slots. A slot is a resource with a limit on how many
   callers can use it at one time. Commands: `acquire`, `hold`, `release`,
   `status`.
3. It serves a dashboard for the loops, for both viewing and controlling
   them (start/stop/close a loop, add or remove a repo, trigger a run,
   adjust a machine's slots). Command: `serve`.
4. It coordinates a fleet of machines: issue claims, machine records, quests
   (sets of issues worked together on one machine), scheduled runs, and shared
   repository events. Commands: `claim`, `renew-claim`, `release-claim`, `join`,
   `heartbeat`, `drain`, `undrain`, `machines`, `place`, `quest`, `reconcile`,
   `roadmap`, `ledger`, `fleet-run`.
   Loop lifecycle: `run`, `once`, `enable`, `disable`, `loops`, `stop`, `peek`,
   `attach`, `schedule`, `pause`, `resume`.

One program, `lupin`, with subcommands. Run `lupin --help` for the full list.

## Commands

```
lupin route <category> <size> [--no-bmo] [--primary-effort E] [--json]
lupin classify --issue-json FILE [--diff-stat FILE] [--json]
lupin fetch-models [--snapshot-file FILE] [--no-write] [--json]
lupin fetch-benchmarks [--force] [--json]
lupin quota [--json]
lupin review-route (--category C --size S | --issue-json FILE) [--mode M]
lupin review-route --prefetch N[,N...] [--repo OWNER/REPO]
lupin acquire <slot> --holder H [--wait SECONDS] [--max N] [--ttl SECONDS]
lupin hold (--lease ID | <slot> --holder H --wait S) [--ttl SECONDS] -- <command>
lupin release --lease ID
lupin status [--json]
lupin ledger append OWNER/REPO --event EVENT [--issue N] [--status S] \
  [--branch B] [--summary TEXT] [--highlights TEXT] [--evidence TEXT] \
  [--decisions TEXT] [--next TEXT] [--child N] [--json]
lupin ledger read OWNER/REPO [--limit N] [--json]
lupin serve [--bind 127.0.0.1] [--port 8788] [--roadmap REPO]
lupin agent [--machine M] [--batch N] [--poll-interval S]
lupin run <repo> [--machine M] [--platform claude|omp] [--provider P] [--model M] [--note TEXT] [--resume]
lupin fleet-run [--json]
lupin once <when> [repo ...] [--platform claude|omp] [--provider P] [--model M] [--note TEXT] [--resume]
lupin enable <repo> [--platform claude|omp] [--orchestrator SELECTOR ...] [--clear-orchestrators]
lupin disable <repo>
lupin loops [repo] [--machine M] [--json]
lupin stop <repo> [--machine M] [--force] [--wait S] [--json]
lupin peek <repo> [LINES] [--machine M] [--json]
lupin attach <repo> [--machine M] [--print]
lupin schedule [--machine M] [--json]
lupin schedule cal <expr> [--machine M] [--json]
lupin schedule first <when> every <interval> [--machine M] [--json]
lupin pause [--machine M | --all] [--json]
lupin resume [--machine M | --all] [--json]
```

`route` and `classify` print a plain result by default (`model effort`, or
`category size`). Add `--json` for a JSON object instead.

`ledger append` stores events in Redis. It records the time and host. Repeat
digest options to add more than one item. Repeat `--child` for each split
issue. `ledger read --json` returns the latest 10 events, oldest first.
Use `--limit N` to choose another positive count. Both commands exit 3 if
Redis is unavailable. The roadmap shows no ledger annotations and a warning.
It ignores `.loop/loop-state.json`.

`review-route --prefetch` fetches issue/PR text up front -- body, comments,
and (for a PR) reviews and a diff stat (changed files, additions/deletions,
no diff text) -- one JSON blob keyed by number, always printed as JSON (no
plain form; the output is nested data, not a one-line result). A number
that is neither an issue nor a PR gets `{"error": ...}` in its own slot
instead of failing the whole batch.

`acquire` prints a lease ID on success. Exit code 2 means the slot is full.
The caller must skip and try again later. Exit code 3 means the `redis`
backend cannot reach the coordinator. The slot has no local fallback.

Only the `bmo` slot falls back to `local`. If Redis is unreachable, `acquire`,
`renew`, `release`, and `status` use `local` for `bmo` and print a warning.
A refused login never falls back. It raises `CoordinatorAuthFailed` for every
slot. An ACL denial raises `NoPermissionError` for every slot. It never falls back.

For a refused login or an ACL denial, the `lupin` command exits with code 3 and
prints one line that names the setting to check. For a refused login, the setting
is `--redis-password` or `LUPIN_REDIS_PASSWORD`. For a fleet command, it also names
the systemd credential `redis-password`. The line never shows a password.
Some read paths are not covered yet. See `docs/redis-schema.md`.

The `local` backend's coordinator is the filesystem, so it never returns 3.

`hold` acquires a lease (or reuses one from `--lease`), runs `<command>`,
renews the lease while the command runs, and releases it when the command
ends — on a normal exit, a non-zero exit, or the command being killed by a
signal.

`fetch-models` checks which model IDs each subscription can call today and
what they cost. It saves the result to
`~/.local/state/lupin/model-snapshot.json` unless `--no-write` is given. It
also publishes the snapshot to Redis, so the PiHome dashboard shows pulls
from other fleet machines. A Redis error leaves the local file in place and
prints a warning. Model lists for opencode-go and claude come from live
provider calls. Codex uses `omp models` when available; otherwise it uses
models.dev and is marked `live: false`. Prices
come from models.dev, matched by model ID. A model with no match has
`price: null`. Promo pricing has no live source, so `promo` stays `null`.

A subscription whose fetch fails — an expired OAuth token, a missing key —
does not empty its row in the fleet snapshot. The fetch keeps that
subscription's last verified list, marks it `live: false` with the reason,
and the Models page shows the rows with that reason under each one.

`run <repo>` starts a Lupin worker and a Herdr workspace. The agent works
in a new Git worktree, not in the main checkout (see "State"). `run` refuses
to start if the main checkout has an unfinished Git operation. `run --all`
starts every enabled repo. Add `--machine M` to use the signed fleet queue
on another machine; `run --all --machine M` starts its enabled repos.
Without a profile, Lupin uses the repo's configured platform. To let Lupin
choose among orchestrators, add a profile when you enable the repo. Repeat
`--orchestrator` for each allowed choice:

```
lupin enable my-repo --platform omp \
  --orchestrator openai/gpt-5 \
  --orchestrator opencode-go/step-5-preview-free:xhigh \
  --orchestrator claude
```

Profiles are local to each worker. Set them on each worker that can start the
repo. At loop start, Lupin uses quota data from the shared cache. The data
must be less than five minutes old and include usable quota for a candidate.
Lupin skips blocked providers, then picks the candidate with the most quota
headroom across its known windows. Profile order breaks ties. If no candidate
has fresh, usable quota, Lupin refuses to start the loop.

Explicit `--platform`, `--provider`, or `--model` options select a fixed
orchestrator for that run and skip profile routing. For OMP, use
`--platform omp` and optionally `--provider openai|opencode-go` or
`--model MODEL`. For example, use
`--model opencode-go/step-5-preview-free:xhigh`. Set auto-compaction for the
selected model in OMP, near 200k tokens. Lupin passes the model to OMP; it
does not set or check OMP's compaction settings. These options work with
`run` and `once`; future one-off runs keep them. `run --all` applies them to
each repo.
`once now` starts the listed repos. If you omit repos, it starts every
enabled repo. A future `once` run uses the same default. It fails if no repo
is enabled. `schedule first` sets the first timer run. Use `+2h` for a
relative time or a calendar expression such as `tomorrow 09:00`. The
interval controls later runs.
`disable` also removes the repo's orchestrator profile. Use
`--clear-orchestrators` with `enable` to clear a profile without disabling the repo.

`fleet-run` sends each enabled repo to one online worker. It skips the local
machine, workers with an active loop for that repo, and workers without that
repo's checkout.
Fleet and dashboard run-now actions use the target worker's local platform
and orchestrator profile. Configure the profile on each worker that can start
the repo.

The coordinator needs one signing key file per worker. Lupin reads these
files from `LUPIN_CMD_SIGNING_KEYS_DIR`, or from systemd's
`CREDENTIALS_DIRECTORY` when the first variable is unset. Each file name is
the worker name. Each worker needs `lupin agent` and its matching key. The
command queues runs but does not wait for them to start.

On PiHome, the systemd timer runs `fleet-run` every 5h15m. The dashboard can
start or stop that timer.

PiHome sends work to other machines. It does not run loops, and it has no
Herdr. Only worker machines (for example `jesus` and `ralpha`) run loops.
Use the dashboard to see all machines. Use Herdr on a worker to see its
loops.

`loops` reads state from the Herdr API. It reports the agent state, backend,
session, workspace and pane IDs. It does not infer state from pane text.
Each machine has one Herdr session, `lupin-loops`, and one Herdr server for
it. Each run has one workspace in that session. Each workspace gets its
repo's GitHub token as `GH_TOKEN`. `stop` closes the workspace and leaves
the session running. The loops on a machine share the server's memory.
Loops that started before this change keep their own session until you
stop them. A completed workspace stays open for review and blocks another
run until you stop it. A Herdr state of `needs_attention` means the
workspace needs review. If Herdr cannot provide state, Lupin reports
`unknown`. Both states block a
new run until you resolve the state.

`run`, `stop`, `peek`, `schedule`, `pause`, and `resume` can target any
fleet machine. Local actions run through Lupin. Remote actions use the signed
Redis queue and wait up to `--wait` seconds (default 20). The target must run
`lupin agent` with Redis access and its own signing key. The sender also needs
that key. Inject it only into the Lupin process with protected secret
management. Do not pass it in command arguments or save it in global
environment settings. Exit code 4 means the result is unknown after the
wait; check it with `lupin cmd status <id>`. `stop`, `peek`, and `attach`
need `--machine` if Lupin cannot find one machine for the repo.

`stop` asks the agent to run `/handoff` before it closes anything. It waits up
to 10 minutes for the agent to finish. Then it saves a report, closes the
workspace, and stops the worker. Then it removes the run's worktree and branch,
if git allows it (see "State"). If the wait ends first, `stop` continues and
says so in its output. If Herdr cannot send the request (for example, the
agent waits for your answer), `stop` closes nothing and exits non-zero. Use
`--force` to stop at once without a handoff. A remote `stop` can take longer
than `--wait`. Then it exits with code 4. Check it with `lupin cmd status <id>`.
The dashboard Stop button always uses `--force`. Its request cannot wait 10
minutes.

`attach` never uses the queue. It opens Herdr here or connects to the remote
Herdr server over SSH. `--print` shows the command instead of running it.
Remote attach needs a local SSH target in `~/.config/lupin/ssh-targets`, one
`<machine> <target>` pair per line. Blank lines and `#` comments are ignored.
Add one line for each worker machine. Do not add PiHome. It has no Herdr:

```
jesus   ghosta@jesus.orb.local
ralpha  user@192.168.44.217
```

This file is local config, not fleet state. Herdr must be installed on both
machines. SSH must allow key-based access. A missing target makes `attach`
fail instead of guessing a hostname.

To also use these machines in Herdr itself, run `herdr machine add <target>`
for each one. `herdr machine list` shows the saved machines.

`fetch-benchmarks` gets public benchmark scores. It does not run models.
The lookup worker is a free opencode-go model (`step-5-preview-free`) run
through `omp -p` with maximum thinking. It reads each model's own
Artificial Analysis page directly -- the page states the Intelligence
Index score in plain text, and one page read costs no search quota --
and falls back to web search for a model the site does not list. The
model list is split into calls of 6 models, up to 2 at a time, and each
score is cached with its own timestamp, so a refresh asks only about the
models whose score is missing, unscored, or older than 20 hours, and a
failed call loses only its own models. A lookup that finds nothing does
not erase a verified score. Use `--force` to fetch everything now.
Plain output lists each unscored model and the reason from the agent.
Model IDs come from the latest `fetch-models` snapshot. If that snapshot
is missing or empty, Lupin uses the active picks in `model-tiers.json`.

A fresh result may include a short, source-linked note about a model's
publicly reported strengths or limits. The agent gets zero-price model IDs
only when a live model snapshot reports zero input and output cost.

The Models page shows scores from public sources by task category. It uses
exact model IDs only. It does not copy a provider-family score to each model.
Missing cells mean no verified score is available. The Notes column shows
source-backed public observations. Older snapshots may not include notes.
The page does not show private test results.
"Perf" is each model's average score as a percent of the best score in each
benchmark, averaged equally across scored categories. "Value" is Perf divided
by the mean input and output price. Future Lupin tests may run one model at a
time. The page does not run private model tests.

`quota` reads provider quota on machines with provider access and publishes
it to shared Redis. The `/usage` page reads these shared rows. It also reads
each machine's local 7-day token and cost totals, stored in separate
per-machine Redis snapshots. The page adds totals by provider and marks a
snapshot stale after five minutes. Each snapshot expires after 24 hours.
Systemd runs `lupin quota` every five minutes on Jesus and Ralpha.

## How a slot's limit (`max`) works

A slot does not know its own limit until something tells it. The first
`acquire` call for a new slot sets the limit, from `--max` (default: 1).
Every later `acquire` call for that slot keeps the stored limit — `--max` on
a later call does nothing. `status` reads the same stored limit.

## State

The `local` backend stores slot state as files, under
`$LUPIN_STATE_ROOT` or `~/.lupin/slots/` by default. Each slot is one
directory; each active holder is one file inside it, holding a PID and an
expiry time. The `redis` backend (`slots_redis.py`) covers leases that
several machines must see. Its schema is in `docs/redis-schema.md`.

Loop state is stored under `$LUPIN_LOOP_STATE_DIR` or
`/var/lib/delegation-loop`. Lupin stores enabled repos in `repos`, per-repo
orchestrator profiles in `orchestrators.json`, loop metadata in
`herdr-loops/`, run prompts in `notes/`, stop reports in `reports/`, run
worktrees in `worktrees/`, and one-off schedules in `once/`. `locks/`
serializes local start and stop actions. Herdr keeps its own session and
workspace state.
The dashboard caches GitHub data in `~/.local/state/lupin/cache.json`.
The same file also holds each checkout's repo identity, so a restart does
not pay for `gh` again.

Each run has its own Git worktree in `worktrees/<repo>/` in the loop state
directory. A worktree is a second working copy of the repo, with its own files
and branch. This one is on a new branch, `lupin-loop/<time>`. `run` runs
`git fetch origin` first. The branch starts at the fetched remote default
branch: the target of `origin/HEAD`, or else `origin/main` or `origin/master`.
If the fetch fails, `run` refuses to start. The agent works in this worktree.
The main checkout in `LUPIN_LOOP_CODE_DIR` is never the agent's working
directory.

`run` removes the previous run's worktree and branch before it adds a new one.
If git keeps that worktree, `run` refuses to start and names its path. With
`--resume`, `run` reuses the worktree that the last run recorded. It never
creates a worktree on resume. If that worktree is missing, or is on another
branch, `run` refuses and names the path. If `run` fails after it adds a
worktree, it removes that worktree and branch. If git keeps the worktree, the
error names its path. If the platform slot is full when the agent launches, the
loop removes its worktree at once.

`run` refuses to start when the main checkout has a merge, rebase,
cherry-pick, or revert in progress, or has unmerged files. The error names the
state and the path. Lupin never aborts, resets, stashes, or cleans the
checkout. Fix the checkout by hand, then run again.

`stop` removes the run's worktree, then its branch. Git refuses to remove a
worktree that has modified or untracked files. In that case `stop` keeps the
worktree and prints its path. `stop` also keeps the worktree when its commit is
on no branch. `stop` never passes `--force` to git and never deletes a branch
with `-D`.

Before removal, `stop` copies two files from the worktree into `handoffs/` in
the loop state directory. `.loop/loop-state.json` becomes `<repo>.json`.
`HANDOFF.md` becomes `<repo>.HANDOFF.md`. A file the worktree does not have
keeps its earlier copy. The next `run` prompt names each copy that exists. Git
deletes the other ignored files in the worktree. Lupin does not keep them.

After removal, `stop` runs `git branch -d` on the run branch. Git refuses that
when the checkout HEAD does not contain the branch tip. The branch then stays,
and `stop` prints its name. Delete it by hand after its work is merged.
`git -C <checkout> worktree list` shows kept worktrees.

`/roadmap` returns the page shell at once and fills in the board or list
from `/roadmap/board`, which renders one HTML fragment per query and keeps
the last results for `LUPIN_ROADMAP_FRAGMENT_TTL` seconds (default 30).
`?full=1` renders the whole page in one response, with no JavaScript.

The Repos and Roadmap pages read the checkouts in `LUPIN_LOOP_CODE_DIR`
(default `/code`) on the machine that runs `lupin serve`. A repo with no
checkout there does not appear. The serving user must be able to read each
checkout. If a different user owns it, Git reports "dubious ownership" and
the page shows no issues. Add the checkout to Git's `safe.directory` list.


## Dashboard mockups

`ui.dc.html` contains dashboard mockups. Read the matching section before
changing a dashboard page. For example, the Machines page is section `1k`.
`docs/ui-mockups.md` shows a screenshot for each mockup section. Use the
matching screenshot as a visual reference. Use `ui.dc.html` for layout and
content. Treat its values as sample data.
Keep real data and controls accurate. Do not show shared fleet capacity as a
per-machine limit or add controls that the current code cannot support.

## Code layout

- `src/lupin/route.py`, `src/lupin/classify.py`, `src/lupin/model-tiers.json`
  — moved from `ghostbook.nix`. See the comments at the top of each file for
  the source commit.
- `src/lupin/slots.py` — the `local` slot backend. New code.
- `src/lupin/slots_redis.py` — the `redis` slot backend, with a fallback to
  `local` for the `bmo` slot.
- `src/lupin/_lease_runtime.py` — the `hold` subprocess/lease-renewal code
  shared by both slot backends above.
- `src/lupin/review_dispatch.py` — picks which lock a routed model needs.
- `src/lupin/free_gate.py` — routes, then reserves the fleet `bmo` Redis
  slot for a `bmo:` pick before calling it final (issue #37).
- `src/lupin/serve.py`, `src/lupin/roadmap.py` — the dashboard. Moved from
  `ghostbook.nix`'s `hosts/jesus/loopgui/` (issue #204).
- `src/lupin/quota.py` — the `/usage` page's quota reads, split out of
  `serve.py`.
- `src/lupin/quota_cache.py` — `lupin quota`'s shared Redis cache: one
  machine's real quota reading, published for the whole fleet to read
  (issue #38).
- `src/lupin/usage_cache.py` — publishes each machine's 7-day usage totals
  for the `/usage` page to add across the fleet.
- `src/lupin/model_fetch.py` — `fetch-models`: a daily snapshot of model
  IDs and prices per subscription.
- `src/lupin/claims.py` — `claim`/`renew-claim`/`release-claim`: one GitHub
  issue claimed by one host at a time.
- `src/lupin/ledger.py` — appends and reads shared repository events in Redis.
- `src/lupin/machines.py` — `join`/`heartbeat`/`drain`/`undrain`/`machines`:
  the fleet's machine registry.
- `src/lupin/loops.py` — signed local-or-remote dispatch for loop controls.
  Attach uses Herdr directly over SSH.
- `src/lupin/loop_runtime.py` — Herdr sessions, workspaces, state, reports,
  schedules, and systemd worker lifecycle.
- `src/lupin/quest.py` — `quest start`/`stop`/`focus`/`release`: a set of
  issues worked together on one machine.
- `src/lupin/place.py` — `place`: picks which fleet machine should run a
  task.
- `src/lupin/reconcile.py` — `reconcile`: applies the automatic claim/
  focus/quest release rules.
- `src/lupin/roadmap_cli.py` — the `roadmap` subcommand's own CLI surface
  (distinct from `roadmap.py`, which the dashboard also uses).
- `src/lupin/cli.py` — the `lupin` command. It owns the argument parsing for
  every subcommand; the modules above take an argument list instead of
  parsing their own.
- `tests/` — one test file per module above.

## Build and test

Full gate (the repo's gate, used by the merge rules):

```
nix flake check                                    # build + test, all systems
nix shell nixpkgs#python3Packages.pytest -c pytest -v
```

## Pull requests, review, and handoff

Every feature change goes through a pull request to the fork's `release/next`.
Policy changes go to upstream `main` for the owner to merge. The loop policy is
in `docs/delegation-loop.md`.

1. Push with `git push fork <branch>`. Never push to `origin`, `main`, or
   `release/next`. Never run `git remote add`. Do not use the commit and PR
   section of `/ship`. It pushes to `origin` when direct push is allowed, and it
   can add a remote. Open the PR on the fork:

   <!-- markdownlint-disable MD013 -->
   ```sh
   gh pr create --repo gracecraft-ro/<repo> --base release/next --title "<title>" --body "Closes #N"
   ```
   <!-- markdownlint-enable MD013 -->

   The PR body has `Closes #N`. The merge into `release/next` does not close
   the issue. The orchestrator (the agent that dispatches and merges work)
   closes it manually, in the same work session. If the push fails, report the
   branch name and commit range. Do not merge that branch. If the PR opens,
   report its number to the orchestrator. The orchestrator dispatches
   `/code-review`.
2. Do not merge your own work. A reviewer who is not the author runs
   `/code-review`. The orchestrator (the agent that dispatches and merges
   work) merges into `release/next` only when all four are true:
   1. The PR is open on the fork, `gracecraft-ro/<repo>`, with base
      `release/next`.
   2. A reviewer approves the current head SHA. The head SHA is the newest
      commit ID on the branch.
   3. The fork branch contains the current `release/next`. The ancestor check
      asks git whether one branch contains another. The check is in the
      `delegation-loop` skill.
   4. The repo's full gate, as its `AGENTS.md` defines it, passes.

   A worker or reviewer never merges a pull request.
3. At the end of a session, run `/handoff`. It runs `lupin ledger append`.
4. Loop details for this repo: `docs/delegation-loop.md`.

### Preview server

This repo has a runnable dashboard. Its preview serves `release/next`.

- Worktree: `.claude/worktrees/preview`. The skill gives the setup commands.
- Start command. Run it from the preview worktree:

```sh
nix develop --command env \
  LUPIN_LOOP_STATE_DIR="$HOME/.local/state/lupin-preview" PYTHONPATH=src \
  python3 -c '
import sys
from lupin.cli import main
sys.exit(main(["serve", "--bind", "127.0.0.1", "--port", "8789"]))
'
```

- Port: `8789`. Check it is free with `ss -ltn` first. The `ss` command lists
  listening ports.
- Machine: not set yet. The owner names it.
- Owner tunnel command: `ssh -N -L 8789:127.0.0.1:8789 MACHINE`. A tunnel
  forwards a port on your computer to the machine.
- Local URL: `http://localhost:8789`.
- Keep it running: a Herdr pane, or `systemd-run --user`. The `systemd-run`
  command starts a command as a background service.
- To bind means to choose the address a server listens on. Never bind to
  `0.0.0.0`. This is a security rule.
- Do not set `LUPIN_REDIS_HOST` to the fleet Redis unless the test needs it.
- Do not use the start, stop, or run controls on this dashboard. New runs use
  the `lupin-loops` session. Older loops may use an old session until they
  stop.
