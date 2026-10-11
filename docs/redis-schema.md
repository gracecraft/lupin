# Redis schema (v1)

This is the data model `lupin` uses once the `redis` backend exists
(issue #210). Every key has the prefix `lupin:v1:`.

## Keys

| Key | Type | Holds | Replaces |
| --- | --- | --- | --- |
| `slot:<name>` | sorted set (member = holder, score = expiry in ms) | `bmo`, the declared `repo:<repo>` UI cap, and enforced `loop-<repo>` fleet locks (one active worker per repo). | `omp.lock` |
| `claim:<owner>/<repo>#<n>` | string (JSON: host, session, since), with a TTL | one claim per GitHub issue | nothing today |
| `ledger:<owner>/<repo>` | stream (`XADD`/`XRANGE`) | `ts host event issue status branch summary highlights evidence decisions next children` | `.loop/loop-state.json` is ignored; no local fallback |
| `machine:<name>` | string (JSON), with a TTL | one fleet machine's status — see Fleet keys below | nothing today |
| `focus:<quest>` | string (JSON), no TTL | which machine a quest is pinned to — see Fleet keys below | nothing today |
| `quest:<id>` | string (JSON), no TTL | one quest's issues, order, machine, state — see Fleet keys below | nothing today |
| `seq:quest` | string (integer, via a Lua script) | the counter that mints the next quest ID | nothing today |
| `cmdq:<machine>` | sorted set (member = command id, score = issued_at in ms) | one machine's pending commands, oldest first — see Command queue keys below | nothing today |
| `cmd:<id>` | string (JSON), with a TTL | one signed command — see Command queue keys below | nothing today |
| `cmdres:<id>` | string (JSON), with a TTL | one command's result — see Command queue keys below | nothing today |
| `cmdlog` | capped stream (`XADD ... MAXLEN ~`) | one entry per enqueue and one per terminal outcome — the audit trail | nothing today |
| `benchmark-snapshot` | string (JSON), with a TTL | one fleet-wide benchmark/quality score per model ID — see Fleet keys below | nothing today |
| `model-snapshot` | string (JSON), with a TTL | latest model list and prices from `lupin fetch-models`; a subscription whose fetch failed keeps its last verified list, marked stale (`model_fetch._merge_previous_subscriptions`) | nothing today |
| `gh-cache:<owner>/<repo>:<cache-key>` | string (JSON: `{"data": ...}`), with a TTL | one read-only `gh` lookup's cached result — see Fleet keys below | each machine's own direct `gh` call for the same lookup |
| `quota-snapshot` | string (JSON), with a TTL | one fleet-wide quota reading per provider — see Fleet keys below | nothing today |
| `usage-snapshot:<machine>` | string (JSON), with a TTL | one machine's 7-day usage rows — see Fleet keys below | nothing today |
These are new keys for the fleet CLI (issues #6–#14, split from #2) and the
cross-machine command queue (issue #28, split from #27). They stay under
`v1`: `v1` is the shape of each key, not the whole file, and adding a key
doesn't change the shape of any key that already exists.

## Acquire and release

**Acquire** is one Lua script, one atomic call:
1. Drop any holder past its expiry.
2. If the caller already holds the slot, renew its lease.
3. Else, if the slot is under its max, add the caller as a holder.
4. Else, return 0 (busy).

**Release** is a compare-and-delete script. Only the current holder can
release its own entry.

Each Redis slot is fleet-wide. Its holders and limit are shared across all
machines. The Machines page shows each slot once, apart from machine status.

## Repository ledger

`lupin ledger append OWNER/REPO` adds one event to the shared stream.
`lupin ledger read OWNER/REPO` returns up to the latest 10 events, oldest
first. Use `--limit N` to choose another positive count. Add `--json` for a
JSON array. An empty stream returns `[]`.

Each event has a UTC `ts`, a `host`, and an event name. Optional fields are
an issue number, status, branch, summary, and digest lists: highlights,
evidence, decisions, and next steps. `children` is a list of issue numbers
split from the event's issue. List fields are JSON arrays in Redis.

Use these commands to write and read events:

```sh
lupin ledger append OWNER/REPO --event handoff --issue 42 \
  --status done --branch BRANCH --summary TEXT \
  --highlights TEXT --evidence TEXT --decisions TEXT --next TEXT \
  --child 43
lupin ledger read OWNER/REPO --limit 50 --json
```

The Python API accepts `limit=None` to read all events. The roadmap does this
because older events can hold the current status for an issue.

Ledger commands exit 3 when Redis is unavailable. The roadmap then shows no
ledger annotations and adds a warning. Lupin does not read or copy the old
`.loop/loop-state.json` file.

The roadmap reads the stream on each page load. It reloads about every five
minutes while open, so new events appear without a manual refresh.

## Fleet keys

These back the fleet CLI (`lupin join`, `heartbeat`, `drain`, `machines`,
`quest ...` — issues #7, #11–#13).

### `machine:<name>`

Written by `lupin join` and `lupin heartbeat`; read by `lupin machines` and
`lupin place` when picking a machine. Renewed every 30s; a machine that
misses two renewals (120s since its `heartbeat` field) counts as offline —
same convention as the `bmo` slot lease (see TTLs below), checked by
comparing a stored timestamp to now, not by Redis expiring the key. The
Redis key itself gets a longer TTL (40 min), only to clean up records for
machines retired long ago — that longer TTL is not what decides
online/offline.

```json
{
  "version": "0.4.0",
  "heartbeat": "2026-10-05T12:00:00Z",
  "state": "online",
  "slots": {"bmo": {"used": 1, "max": 1}},
  "providers": ["claude", "openai"],
  "quota": {
    "claude": {"pct_left": 42, "resets_at": "2026-10-05T18:00:00Z"}
  },
  "loops": [{
    "repo": "lupin",
    "platform": "claude",
    "state": "working",
    "since": "2026-10-05T11:00:00Z",
    "backend": "herdr",
    "session": "lupin-lupin-1234567890",
    "workspace_id": "workspace-1",
    "pane_id": "pane-1"
  }],
  "session_backend": "herdr",
  "repos": [{"repo": "field-trip", "enabled": true, "loopable": true}],
  "actions": ["loop.peek", "loop.run", "loop.run-all", "loop.state", "loop.stop", "schedule.pause", "schedule.resume", "schedule.set", "schedule.show"]
}
```

Each `loops` entry reports Herdr's agent state and workspace IDs. `since`
is an ISO 8601 UTC time. The heartbeat does not infer state from pane text.

`state` is `"online"` or `"draining"` (`lupin drain`/`undrain` set it).
A draining machine accepts only its `DRAIN_ALLOWED` actions: `loop.stop`,
`loop.peek`, `loop.state`, `schedule.show`, and `schedule.pause`. It can
stop work or read state, but it cannot start work. `loop.run` starts a loop;
`schedule.set` and `schedule.resume` can start one later.

`actions` is this machine's `agent.py` `ACTIONS` table (issue #27/#28),
written by `_write_record` so it can never list an action the agent here
doesn't actually run.

`repos` lists directories under `/code` on this machine. Each item has a
repo name, whether it is enabled, and whether it has a loop doc. The
dashboard combines these lists. Each machine must run the Lupin version
that sends this field before its repos appear.

### `focus:<quest>`

Written by `lupin quest focus`/`lupin quest release`; read by `lupin quest`
and `lupin place`. No TTL — `quest release` deletes the key outright, so a
stale focus doesn't need to time out.

```json
{
  "machine": "jesus",
  "pinned": false,
  "since": "2026-10-05T12:00:00Z",
  "release_when": "quest done"
}
```

`release_when` is a cached, human-readable guess (idle, stalled, done —
C10's job to compute); `lupin quest` shows it without recomputing it.

### `quest:<id>`

Written by `lupin quest start`/`lupin quest stop`; read by `lupin quest`
and `lupin quest status`. No TTL — `quest stop` deletes the key.

```json
{
  "issues": [11, 12, 13],
  "targets": ["gracecraft/lupin#11", "gracecraft/lupin#12", "gracecraft/lupin#13"],
  "order": [11, 12, 13],
  "waits_on": [{"number": 12, "blocker": 11}],
  "machine": "jesus",
  "state": "running"
}
```

`targets` is `issues` in the same order, each written as the `claim:<...>`
key it maps to (`owner/repo#n`) -- a quest's issues can come from different
repos, so `stop` needs this to find each one's claim. `waits_on` lists each
issue-blocker pair where both are in the quest -- `quest start` prints one
"waits on" line per pair. Omitted when no issue in the quest blocks
another. `platform`/`note` are optional, carried over as-is from `quest
start`'s own flags.

### `seq:quest`

A plain string holding the last-issued quest ID. `INCR` isn't on this
repo's ACL command list (see below), and Redis checks ACL permissions on
commands called from inside a script too — so the script can't just call
`INCR` either. `lupin quest start` mints the next ID with a Lua script via
`EVAL` that reads and writes the counter with `GET`/`SET` only, both
already allowed:

```lua
local n = tonumber(redis.call('GET', KEYS[1]) or '0') + 1
redis.call('SET', KEYS[1], n)
return n
```

No ACL change needed — `GET`, `SET`, and `EVAL` are already on the list.

### `benchmark-snapshot`

Written by `lupin fetch-benchmarks` (issue #17's reopen; see
`benchmark_fetch.py`); read by `lupin fetch-benchmarks` and the Models
page. One key, fleet-wide — a benchmark score doesn't depend on which
machine looked it up, unlike `model:<name>`'s per-machine model-fetch
snapshot, so there is no `benchmark-snapshot:<machine>` variant.

```json
{
  "fetched_at": "2026-10-08T04:20:00+00:00",
  "live": true,
  "source": "omp -p opencode-go/step-5-preview-free thinking=max, web search",
  "scores": [
    {"id": "qwen3.7-max", "score": 29, "scale": "Artificial Analysis Intelligence Index, 0-100",
     "source": "https://artificialanalysis.ai/models/qwen3-7-max", "as_of": "2026-10-08",
     "fetched_at": "2026-10-08T04:20:00+00:00"},
    {"id": "omen-alpha", "score": null, "reason": "not found",
     "fetched_at": "2026-10-08T04:20:00+00:00"}
  ]
}
```

Each score carries its own `fetched_at`. One refresh asks the agent only
about the models whose entry is missing, unscored, or older than 20 hours,
and merges the answer over the cached entries, so a model's score keeps
yesterday's stamp while today's run refreshes the rest. A fetch that fails
entirely leaves the cached scores in place — `live: false` describes this
run, it does not replace the cache. `stale_reason` also appears on an
otherwise-live snapshot when some shards failed; the merged `scores` list
is then the answer for the models that did refresh.

Each score can also include an optional `note` object with `text` and a
public `source` URL. The Models page shows these source-backed remarks in
its Notes column. A note is omitted when the agent finds no useful
evidence. Older snapshots may not have notes. The agent receives a
zero-price model list only when both prices are zero in a live
`fetch-models` snapshot.

TTL is retention only (7 days, `benchmark_fetch.REDIS_KEY_TTL`) —
freshness is decided per entry by comparing its `fetched_at` to a 20-hour
window (`benchmark_fetch.CACHE_FRESH_SECONDS`), not by the key expiring;
the same "stored timestamp, not Redis TTL" convention as `machine:<name>`'s
offline detection above.

The lookup worker is a free opencode-go model (`step-5-preview-free`) run
through `omp -p` with `--thinking max` and two tools, `read` and
`web_search`: `read` fetches each model's own `artificialanalysis.ai`
page, which states the Intelligence Index in plain text and costs no
search quota, and `web_search` is the fallback for a model the site does
not list. The list is sharded 6 models per call, up to 2 calls at a time
(`benchmark_fetch.SHARD_SIZE`, `MAX_PARALLEL_SHARDS`); a wider fan-out
measured 2026-10-08 exhausted the free search providers and every shard
returned `score: null`. A null never overwrites a verified score. See
`benchmark_fetch.py`'s docstring for the measurements.

Only one machine fetches at a time: `lupin fetch-benchmarks` wraps the
actual dispatch in `slot:benchmark-fetch` (max 1 holder, no renewal — the
lease TTL is computed from the work this run will do,
`benchmark_fetch._lock_ttl`, so it already covers every round of the
fan-out; a crashed holder's lease expires on its own).
A machine that finds the slot already held just reads whatever is in
`benchmark-snapshot` right now instead of waiting.

### `gh-cache:<owner>/<repo>:<cache-key>`

Backs `place`/`quest`/`roadmap`'s read-only `gh` lookups (issue #35):
issue state, issue body/labels, quest-labeled issues, dependency links.
Written by `pihome` (the fixed value of `gh_cache.CANONICAL_GH_FETCHER`
— a hard pin to one named machine, not a race any machine could win).
Every other machine reads it while that fetcher is alive. When the pinned
machine is not maintaining the cache — draining, offline, or never joined
— a machine that needs the data fetches it and publishes through the same
`gh-fetch/<owner>/<repo>` slot, so the first machine to win still answers
for the fleet and no two machines fetch the same repo at once.
Read by every machine, `pihome` included.
The roadmap uses the dashboard's Redis connection for this cache.

```json
{"data": {"number": 42, "state": "OPEN"}}
```

`data` is whatever the wrapped `gh` call returns, wrapped so a legitimate
`null` result (e.g. "this issue does not exist") is still a cache hit, not
indistinguishable from "nothing cached yet".

TTL 5 minutes — long enough that a burst of `place`/`quest`/`roadmap` calls
across several machines shares one fetch, short enough that a placement or
quest decision isn't made against issue data that's badly stale.

`<cache-key>` names which lookup, since the same repo backs several
different queries that must not share one cache entry: `issues:<state>`
(issue list), `comments:<state>` (per-issue comment pagination),
`dependencies` (blockedBy/blocking links), `quests` (quest-labeled issues),
`issue:<number>` (`place`'s single-issue lookup), `issue-state:<number>`
(`roadmap_cli`'s blocker-exists check), `quest-locate-issue:<number>`
(`quest`'s issue-to-repo lookup).

Guarded by the existing `slot:<name>` shape above, as
`slot:gh-fetch/<owner>/<repo>` (max 1 holder, `/` not `:` -- a colon in the
slot name breaks `release`/`renew`'s lease-id parsing, which splits on the
first colon) — this only stops `pihome` from running the same fetch twice
if two `lupin` invocations there race each other. A non-`pihome` machine
never takes this lock and never calls `gh` for these lookups at all; on a
miss it reports "no data yet" instead.

### `quota-snapshot`

Backs `lupin quota` and `serve.py`'s `/usage` page (issue #38). Quota is
one shared account per provider (Claude, opencode-go, OpenAI/Codex — issue
#36), so one real reading per provider is the fleet's answer, not
something to merge across machines.

```json
{
  "claude": {
    "rows": [{"provider": "claude", "duration": "PT5H", "used_pct": 42, "resets_at": 1_790_547_474_348}],
    "fetched_at": "2026-10-07T12:00:00+00:00",
    "fetched_by": "jesus"
  },
  "openai": {
    "rows": [{"provider": "openai", "duration": "PT5H", "used_pct": 10, "resets_at": 1_790_550_000_000}],
    "fetched_at": "2026-10-07T11:58:00+00:00",
    "fetched_by": "mini"
  }
}
```

One key, one entry per provider — each provider goes stale/fresh on its
own, so each carries its own `fetched_at`/`fetched_by`. `rows` is
`quota.quota_usage()`'s own row shape for that provider (one row per
window: 5 hours, 7 days, 30 days), kept whole rather than collapsed to one
number, so a reader can show duration, percent left, and time to reset —
not just one of them. A provider's `duration` is written as the plain
string `quota.QuotaDuration`'s own value serializes to (`"PT5H"` etc.);
`quota_cache.py` converts it back to the real enum on read.

Unlike `gh-cache`'s fixed `pihome` pin, there is no fixed canonical
machine here: credentials for different providers can live on different
machines, unknown in advance. Instead, whichever machine's own local
`quota.quota_usage()` call actually returns a real `used_pct` for a
provider is treated as that provider's fetcher for this round — a machine
with no credentials for a provider never has real data for it, so it
never writes for it, and can never clobber a good reading from elsewhere.

TTL on the key is retention only (24 hours) — freshness is judged per
provider, by comparing that provider's own `fetched_at` to 5 minutes
(`quota_cache.CACHE_TTL`), the same "stored timestamp, not Redis TTL"
convention `benchmark-snapshot`/`machine:<name>` already use. A provider
entry older than that is still shown (better than nothing) but is no
longer trusted to trigger a skip — the next `lupin quota` run on a
credentialed machine republishes it.

Guarded by `slot:quota-fetch/<provider>` (max 1 holder, non-blocking, no
wait) — only stops two `lupin` processes on the *same* machine from
publishing the same provider at once, same narrow job the `gh-fetch`/
`benchmark-fetch` locks do.

### `usage-snapshot:<machine>`

`lupin quota` writes one 7-day usage snapshot per machine. It contains that
machine's local Claude and OMP rows, the publish time, and the machine name.
Each machine updates only its own key.

The `/usage` page reads all snapshots and adds token and cost totals by
provider. It shows the publish age and marks a snapshot stale after 5 minutes.
Each key expires after 24 hours. A missing snapshot is not shown as zero.

## Command queue keys

These back the cross-machine command queue (`lupin cmd send|status|queue`,
`lupin agent` — issue #28, implementing #27's design). One machine enqueues
a signed command; the target machine's `lupin agent` runs it through a
fixed action table: `loop.stop`, `loop.run`, `loop.run-all`, `loop.peek`,
`loop.state`, `schedule.show`, `schedule.set`, `schedule.pause`, and
`schedule.resume`.

`loop.stop` takes `repo` and an optional boolean `force`. Without `force`, the
stop asks the agent for a handoff first.

The agent gives `loop.stop` a budget of 1740 seconds (29 minutes). This is
`ACTION_TIMEOUT_S["loop.stop"]` in `agent.py`. The budget is a sum of worst
cases. No single timer enforces it. It has three parts:

- The command read. The agent reads the command with `_client`. The action is
  not known until the read ends. So every action uses `_client` for this read.
- Up to five Redis calls come after the read. They use `debrief_client`.
  They run in this order:
  - Claim the command.
  - Read the claim entry again. This runs only when the claim returns nil.
  - Write the result.
  - Remove the command from the queue.
  - Write the audit line.

  A nil reply means the key already exists. The key can hold this attempt's
  claim. This happens after a lost reply. It can also hold another poller's
  claim. The entry is this attempt's claim only when `state` is `running` and
  `claim` matches this attempt's token. The budget counts five calls. It covers
  the claim read even when that read does not run.
- The stop subprocess. Its timer is `SUBPROCESS_TIMEOUT_S["loop.stop"]`, passed
  to `subprocess.run`.

The subprocess timer is the budget minus the worst case for the other two
parts:

    COMMAND_READ_WORST_S = 328 seconds
    SUBPROCESS_TIMEOUT_S = 1740 - 328 - 5 x 14 = 1342 seconds

The budget does not cover the prune read, the pending-list read, or earlier
commands in the same poll. See known limits.

Other actions get `EXEC_TIMEOUT_S`, which is 120 seconds.

## Redis clients

`slots_redis.debrief_client` makes a Redis client with short timeouts. The stop
path uses it for the calls after the read. The debrief uses it for its three
Redis calls.

The client has these settings:

- Each connect waits up to 1 second. This is `DEBRIEF_TIMEOUT_S`.
- Each read waits up to 1 second.
- redis-py does not retry (`retries=0`).
- `_call_with_retry` makes at most two attempts.

`_client` is the default client. Its timeout is 2 seconds. redis-py retries a
failed command 10 times. The command read uses `_client`. The other callers also
use `_client`, such as slots, claims, and the ledger.

## One debrief_client call

The worst case is 14 seconds. This is `DEBRIEF_CALL_WORST_S` in `agent.py`:

    2 x (2 + 5) x 1 = 14

- Two attempts (`_call_with_retry`).
- Each attempt opens a new connection. It waits for one connect per address.
  `localhost` has two addresses, IPv6 and IPv4.
- Each attempt then makes five round trips. Each round trip is one request and
  one reply. The five are: `HELLO 3`, `CLIENT MAINT_NOTIFICATIONS ON`,
  `CLIENT SETINFO LIB-NAME`, `CLIENT SETINFO LIB-VER`, and the command.
  The test `test_command_takes_five_round_trips` counts them on the wire.
- Each wait is up to 1 second.

## The command read

The worst case is 328 seconds. This is `COMMAND_READ_WORST_S` in `agent.py`:

    2 x (11 x (2 + 5) x 2 + 10 x DEFAULT_CLIENT_BACKOFF_CAP_S) = 328

- Two attempts (`_call_with_retry`).
- Each attempt makes 11 tries. That is one try, then 10 redis-py retries.
- Each try waits for one connect per address. Then it waits for five round
  trips. Each wait is up to `CONNECT_TIMEOUT`, which is 2 seconds.
- Before each retry, the client waits up to `DEFAULT_CLIENT_BACKOFF_CAP_S`.
  That is the redis-py backoff cap.

This is the worst case, not a measured time.

## Debrief in the subprocess

The debrief runs inside the stop subprocess, after the repo lock is released.
The debrief has these waits:

- All `gh` calls share one time limit of 10 seconds (`DEBRIEF_TIME_LIMIT_S`).
  A call that hits the limit leaves a `Not collected` note in the debrief.
- Three Redis calls, each up to 14 seconds (`DEBRIEF_CALL_WORST_S`):
  - A ledger read (one `XRANGE`).
  - A claims scan (`SCAN`). See known limits.
  - A claims read. This is one batch of `GET` commands. The client sends them
    together and gets one reply. It is not `MGET`. `MGET` returns nothing for a
    key of the wrong type. So that claim would vanish. A `GET` batch raises the
    error.

The debrief client gets no host, port, or password from its caller. It connects
to `localhost:6379` without auth. Issue #118 tracks this.

Periodic debriefs run on a background thread of `lupin agent`, not in the stop
subprocess. They do not block commands. They read the same Redis keys as stop
debriefs. They add no new keys. Their file names end in `-6h`, `-24h`, or
`-7d`. They have no time budget entry. The "Budget result" section and the
"Lock waits" section cover stop debriefs only. They connect to `localhost:6379`
without auth, as stop debriefs do (issue #118).
Each one also saves two screenshots in a folder named `<file name>-screenshots/`.
A screenshot uses a headless browser and starts a dashboard on a free port. It
has no time budget entry. A failed screenshot does not stop the debrief.

## Lock waits

Two lock waits have no time limit of their own. They are not in the budget table.

- The repo lock in `stop_loop` (`loop_runtime.py:1112`, blocking `flock`).
- The machine lock in `_ensure_shared_server` (`loop_runtime.py:771`, blocking
  `flock`). The stop path reaches it only when the shared Herdr server is down.

Both waits run inside the stop subprocess. Only the subprocess limit ends them.
If a wait reaches that limit, the stop ends. The debrief is then not written.

## Known limits

- The budget does not cover the prune read, the pending-list read, or earlier
  commands in the same poll. These run before the command read.
- The stop claim uses `debrief_client`. A read that waits more than 1 second
  makes an attempt fail. Then `_call_with_retry` makes one more attempt. This
  leads to three cases:
  - The first try writes the claim. Its reply is lost. The retry returns nil.
    The claim read finds this attempt's claim token. The stop runs once.
  - The claim is written. Its reply is lost. The retry or the claim read then
    fails. The stop does not run. The command stays queued. The record stays
    `running`. `lupin agent` exits with code 3. The next start marks the record
    `failed`. It also removes the command from the queue (`startup_scan`).
  - Another poller claims the id first. This poller returns `lost-race`. It does
    not touch the queue or run the command.
- Non-stop actions use `_client`. It allows 2 seconds per read. This limit does
  not affect them. The test `test_non_stop_command_survives_a_reply_slower_than_the_stop_bound`
  checks this.
- If the stop runs, but the result write fails, the record stays `running`
  until the next start. The next start marks it `failed`. Its reason starts with
  `orphaned`. So a stop that ran can show `failed`. The test
  `test_stop_result_write_failure_leaves_the_claim_until_restart` checks this.
- A `ZREM` after a run can fail with a connection or timeout error. This error
  ends the poll. `lupin agent` exits with code 3. The entry stays queued.
  `cmdres` holds the final state. `cmdlog` has no line for the run. When a poll
  reaches the entry after a restart, it does this:
  - Before `expires_at` + 30 seconds, it returns `lost-race`. It does not run
    the command.
  - From `expires_at` + 30 seconds on, it returns `expired`. It logs an
    `expired` line with reason `ttl` to `cmdlog`. `cmdres` still says `ok`.
- The claims scan counts as one request and reply. `SCAN` returns keys in pages.
  Each extra page is one more request and reply. The budget does not count the
  extra pages.
- A server that sends data slowly can keep one reply going. Each read on
  `debrief_client` waits up to 1 second. For `loop.stop`, these are the calls
  after the command read. Each read on `_client` waits up to 2 seconds. The
  command read uses `_client`. The reply as a whole has no time limit. The
  budget does not bound this case.
- Name lookup (`getaddrinfo`) is not covered by the timeouts.

## Budget result

Worst case, in seconds. The terms are in `tests/test_agent.py`.

| Part | Terms | Seconds |
| --- | --- | --- |
| Subprocess, listed | loop_runtime waits 983.25 plus gh 10 plus three debrief Redis calls 3 x 14 | 1035.25 |
| Subprocess timer | 1740 - 328 - 5 x 14 | 1342.00 |
| Agent Redis calls | command read 328 plus five calls at 14 | 398.00 |
| Listed total | 1035.25 + 398 | 1433.25 |
| Budget | `ACTION_TIMEOUT_S["loop.stop"]` | 1740.00 |
| Margin | 1740 - 1433.25 | 306.75 |

The test `test_stop_time_limit_covers_the_listed_timeouts` checks the listed
total against the budget. The test
`test_stop_subprocess_cap_leaves_room_for_agent_redis_calls` checks the
subprocess timer.

### `cmd:<id>`

Written once by `lupin cmd send`, via one `EVAL` that does `SET ... NX`
here and `ZADD cmdq:<target>` together — `MULTI` isn't on the ACL list (see
below), so this is the atomic primitive instead. The `PX` TTL (1h, fixed)
is just retention — how long the record stays around to look up, not
whether the command is still valid to run. That's `expires_at`, a field
inside the JSON, checked on the target host with a 30s clock-skew
allowance.

```json
{
  "v": 1,
  "id": "a1b2c3d4e5f6...",
  "target": "jesus",
  "action": "loop.stop",
  "params": {"repo": "lupin"},
  "actor": "grace",
  "issuer": "pihome",
  "issued_at": 1759708800.123,
  "expires_at": 1759708920.123,
  "sig": "..."
}
```

`actor` is who asked for this (audit only, never trusted for
authorization — that's the HMAC's job). `issuer` is what sent it. `sig` is
HMAC-SHA256 over a canonical JSON encoding (`json.dumps(..., sort_keys=True,
separators=(",", ":"))`) of every other field, keyed by a secret shared
with the target machine only — per-target HMAC, not a Redis ACL selector,
so a compromised host can't forge a command for a different one.

### `cmdres:<id>`

Claimed with `SET ... NX`. The first writer wins a race between two pollers
on the same id. The poller that made the claim then overwrites it with the
final result.

`state` is one of `queued`, `running`, `ok`, `failed`, `rejected`, `expired`.
A `queued` entry has no `cmdres` yet. Only its `cmd:<id>` key exists.

A `running` entry has a `claim` field. It holds a new token for each claim
attempt. If `SET ... NX` returns nil, the poller reads the entry again. The
claim belongs to this attempt when `claim` matches its token and `state` is
`running`. Otherwise, another poller holds the claim, or a result exists.

```json
{"id": "a1b2c3d4e5f6...", "state": "ok", "host": "jesus", "action": "loop.stop", "exit_code": 0, "output": "...", "truncated": false}
```

A `running` entry that is still queued when `lupin agent` restarts is marked
`failed`. Its reason is `orphaned: still running when the agent restarted`. The
startup scan does this. It never runs the command again.
Three cases can leave such an entry for any action:

- The agent process stopped after it wrote the claim, and before it wrote the
  result. A crash or a kill can cause this. The command can have run in
  full, in part, or not at all. The startup scan does not check whether the
  command ran.
- The claim was written, but its reply was lost. The retry or the claim read
  then failed. The command did not run. The agent exited with code 3.
- The command ran, but the result write failed. The agent exited with code 3.
  The command did run.

`output` is the last 8 KiB of combined stdout+stderr. `rejected` entries and
`failed` entries without a run carry a `reason` string instead.

### `cmdlog`

The stream has one entry for each enqueue. It has one entry for each terminal
outcome (`ok`/`failed`/`rejected`/`expired`). The stream is capped with
`MAXLEN ~ 2000`. This is the audit trail. Some outcomes have no line:

- A Redis connection or timeout error on the write drops the line.
- A run whose `ZREM` fails has no line. If a later poll expires the entry,
  and its ZREM succeeds, that poll writes an `expired` line. `cmdres` still
  says `ok`. If the prune removes the entry first, no line is written.

## TTLs

Starting values, not measurements — change them once real use shows better
numbers.

| Resource | Renew every | TTL |
| --- | --- | --- |
| Slot lease | 30s | 120s |
| Claim | 2 min | 10 min |
| Machine heartbeat | 30s | 120s |
| Command record (`cmd:<id>`) | n/a — not renewed | 1 hour (retention only, see below) |
| Command result (`cmdres:<id>`) | n/a — not renewed | 1 hour |
| `slot:benchmark-fetch` lease | n/a — not renewed | `benchmark_fetch._lock_ttl()`: shard rounds × `SHARD_TIMEOUT` + 60s headroom (~8 min for a 54-model day) |
| `benchmark-snapshot` | n/a — not renewed | 7 days (retention only, see below) |
| `model-snapshot` | n/a — not renewed | 7 days (retention only) |
| GitHub data cache (`gh-cache:...`) | n/a — not renewed | 5 min |
| GitHub fetch lock (`slot:gh-fetch/<owner>/<repo>`) | n/a — held only for one fetch | 2 min |
| `quota-snapshot` | n/a — not renewed | 24 hours (retention only; freshness is per-provider, 5 min, see above) |
| Quota fetch lock (`slot:quota-fetch/<provider>`) | n/a — held only for one fetch, no wait | 1 min |

A command's Redis retention (1h) is not the same thing as how long it's
valid to run — that's `expires_at` inside the record (120s after
`issued_at` by default, the "pickup deadline"), checked by `lupin agent`
with a 30s clock-skew allowance. A command can sit in Redis, inspectable,
long after it's stopped being runnable.

## ACL command list

Each host gets its own Redis user (`lupin-jesus`, `lupin-ralpha`,
`lupin-mac`), limited to keys under `~lupin:*` and these commands:

```
PING GET SET DEL PEXPIRE ZADD ZREM ZCARD ZRANGE ZREMRANGEBYSCORE
XADD XRANGE XREVRANGE SCAN EVAL EVALSHA SCRIPT|LOAD
```

## Fallback when Redis is unreachable

Connect timeout 2s, 1 retry, then:

| Resource | Fallback |
| --- | --- |
| `bmo` slot | Local lock (`omp.lock`), plus a warning in the journal. Same cross-host risk as today. |
| Claim | `lupin claim` exits 3. The orchestrator starts no new issue, but keeps working anything already in progress. |
| Ledger | `lupin` always writes the local file too. The Redis copy misses the entry — v1 has no replay. |
| Host-scope slot | No change — these never use Redis. |
| Command queue | `lupin cmd send`/`lupin agent` exit 3. No local fallback, same as claims — a command only means anything if the target machine can see it. |
| Benchmark snapshot | `lupin fetch-benchmarks` reports `live: false` with a `stale_reason` (exit 0, same convention as `model_fetch.py`'s own failure cases — see `cli.py`'s exit-code table, "any other error" doesn't fit this, it's a data-availability fact, not a usage error). No local fallback — a fleet-shared cache has nothing meaningful to fall back to on one machine, and a failed run leaves the last cached scores on the key untouched rather than erasing them. |
| GitHub data cache | `pihome` calls `gh` directly anyway (it just can't publish for other machines). Every other machine reports "no data yet" instead of calling `gh` itself — no direct-call fallback here, unlike the resources above. |
| Quota snapshot | A machine with real provider credentials still returns its own live reading (it just can't publish for other machines). A machine with no credentials for a provider has nothing to fall back to and reports "no data cached yet" for it. |

After an outage ends, a holder tries to renew its lease. If the lease
already expired, `lupin` logs "lease lost" and tries to acquire again.

## Refused login and ACL denial

Redis refuses a login when the password is bad or missing.
Redis refuses a command when the ACL denies it.
Both cases are answers from Redis, not outages.

| Call | Login refused | ACL denied |
| --- | --- | --- |
| `acquire` | `CoordinatorAuthFailed` | `NoPermissionError` |
| `renew` | `CoordinatorAuthFailed` | `NoPermissionError` |
| `release` | `CoordinatorAuthFailed` | `NoPermissionError` |
| `set_max` | `CoordinatorAuthFailed` | `NoPermissionError` |
| `status` | `CoordinatorAuthFailed` | `NoPermissionError` |

These results apply to every slot, `bmo` included. No refusal falls back to `local`.

The `lupin` command exits with code 3 for a refused login and for an ACL denial.
It prints one line that names the setting to check. It never shows the password.
For a fleet command, the setting list includes the systemd credential `redis-password`.
A failed renew or release during `hold` prints one line. `hold` keeps the exit code of its command.

Known gap: some read paths still print `cannot reach` for a refused login.
Some cache reads return no data for a refused login. Exit codes for these paths are not covered by the rule above.

## Persistence

AOF, `appendfsync everysec`, so the claim ledger survives a restart. Leases
still end by TTL, not by the AOF.
