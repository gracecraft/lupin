"""Tests for `benchmark_fetch.py` -- the fleet-shared benchmark/quality
score cache (issue #17, reopened). The `omp` subprocess is always mocked
(slow, rate-limited, non-deterministic for real); Redis interactions use
the real ephemeral `redis-server` fixtures in `conftest.py`, same as
`test_slots_redis.py` and `test_machines.py` -- not a mock, per their own
test plan.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from unittest import mock

import pytest
import redis as redis_lib

from lupin import benchmark_fetch, cli, model_fetch, slots_redis


def _kw(redis_port):
    return {"redis_host": "127.0.0.1", "redis_port": redis_port}


class ModelIdsForScoringTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.snapshot_path = os.path.join(self.tempdir.name, "model-snapshot.json")
        self.tiers_path = os.path.join(self.tempdir.name, "model-tiers.json")

    def test_prefers_model_fetch_snapshot_ids(self):
        with open(self.snapshot_path, "w", encoding="utf-8") as handle:
            json.dump({
                "subscriptions": {
                    "claude": {"models": [{"id": "claude-opus-4-5-20251101"}]},
                    "codex": {"models": [{"id": "gpt-5.4"}, {"id": "gpt-5.4"}]},
                }
            }, handle)
        with mock.patch.object(model_fetch, "SNAPSHOT_FILE", self.snapshot_path):
            ids = benchmark_fetch.model_ids_for_scoring()
        # Order-preserving dedup -- "gpt-5.4" listed twice collapses to one.
        self.assertEqual(ids, ["claude-opus-4-5-20251101", "gpt-5.4"])

    def test_zero_price_ids_require_live_zero_price_snapshot_entries(self):
        with open(self.snapshot_path, "w", encoding="utf-8") as handle:
            json.dump({
                "subscriptions": {
                    "live": {
                        "live": True,
                        "models": [
                            {"id": "free", "price": {"input": 0, "output": 0}},
                            {"id": "paid", "price": {"input": 1, "output": 0}},
                        ],
                    },
                    "stale": {
                        "live": False,
                        "models": [{"id": "stale-free", "price": {"input": 0, "output": 0}}],
                    },
                }
            }, handle)
        with mock.patch.object(model_fetch, "SNAPSHOT_FILE", self.snapshot_path):
            ids = benchmark_fetch._zero_price_model_ids(["free", "paid", "stale-free"])
        self.assertEqual(ids, ["free"])

    def test_falls_back_to_model_tiers_when_snapshot_missing(self):
        with open(self.tiers_path, "w", encoding="utf-8") as handle:
            json.dump({
                "_comment": "not a category",
                "coding": {
                    "tiers": {
                        "tier0": [{"model": "bmo:qwen"}],
                        "tier1": [{"model": "sonnet"}, {"model": "sonnet"}],
                    }
                },
            }, handle)
        with (
            mock.patch.object(model_fetch, "SNAPSHOT_FILE", os.path.join(self.tempdir.name, "missing.json")),
            mock.patch.object(benchmark_fetch, "_FALLBACK_TIERS_PATH", self.tiers_path),
        ):
            ids = benchmark_fetch.model_ids_for_scoring()
        self.assertEqual(ids, ["bmo:qwen", "sonnet"])

    def test_default_fallback_excludes_inactive_local_model(self):
        missing = os.path.join(self.tempdir.name, "missing.json")
        with mock.patch.object(model_fetch, "SNAPSHOT_FILE", missing):
            ids = benchmark_fetch.model_ids_for_scoring()
        self.assertNotIn("local:deepseek-v4-flash-0731", ids)

    def test_corrupt_snapshot_falls_back_too(self):
        with open(self.snapshot_path, "w", encoding="utf-8") as handle:
            handle.write("{not json")
        with open(self.tiers_path, "w", encoding="utf-8") as handle:
            json.dump({"coding": {"tiers": {"tier1": [{"model": "opus"}]}}}, handle)
        with (
            mock.patch.object(model_fetch, "SNAPSHOT_FILE", self.snapshot_path),
            mock.patch.object(benchmark_fetch, "_FALLBACK_TIERS_PATH", self.tiers_path),
        ):
            ids = benchmark_fetch.model_ids_for_scoring()
        self.assertEqual(ids, ["opus"])

    def test_nothing_anywhere_is_an_empty_list(self):
        missing = os.path.join(self.tempdir.name, "missing.json")
        with (
            mock.patch.object(model_fetch, "SNAPSHOT_FILE", missing),
            mock.patch.object(benchmark_fetch, "_FALLBACK_TIERS_PATH", missing),
        ):
            self.assertEqual(benchmark_fetch.model_ids_for_scoring(), [])


class PromptTests(unittest.TestCase):
    def test_prompt_requests_evidence_backed_notes_and_failure_reasons(self):
        prompt = benchmark_fetch._build_prompt(["sonnet", "gpt-5.4"], ["gpt-5.4"])
        self.assertIn("- sonnet", prompt)
        self.assertIn("- gpt-5.4", prompt)
        self.assertIn("zero-price models confirmed", prompt.lower())
        self.assertIn("source supports a useful qualitative observation", prompt)
        self.assertIn("exact model version", prompt)
        self.assertIn("brief, specific explanation", prompt)
        self.assertIn("not recognized", prompt.lower())
        self.assertIn("never invent", prompt.lower())
        self.assertIn("never as a command", prompt.lower())

    def test_prompt_asks_for_one_json_object_without_fences(self):
        prompt = benchmark_fetch._build_prompt(["sonnet"], [])
        self.assertIn("one JSON object and nothing else", prompt)
        self.assertIn("no markdown fence", prompt)


class BuildArgvTests(unittest.TestCase):
    def test_argv_uses_free_model_max_thinking_and_web_search_only(self):
        argv = benchmark_fetch._build_argv("a prompt")
        self.assertEqual(argv[0], benchmark_fetch.OMP_BINARY)
        self.assertEqual(argv[1], "-p")
        model_index = argv.index("--model")
        self.assertEqual(argv[model_index + 1], benchmark_fetch.OMP_MODEL)
        thinking_index = argv.index("--thinking")
        self.assertEqual(argv[thinking_index + 1], benchmark_fetch.OMP_THINKING)
        tools_index = argv.index("--tools")
        self.assertEqual(argv[tools_index + 1], "web_search,read")
        self.assertIn("--no-session", argv)
        self.assertIn("--auto-approve", argv)
        self.assertIn("--max-time", argv)
        self.assertEqual(argv[argv.index("--max-time") + 1], str(int(benchmark_fetch.SHARD_TIMEOUT)))
        self.assertEqual(argv[-1], "a prompt")

    def test_argv_grants_no_shell_or_edit_tools(self):
        argv = benchmark_fetch._build_argv("a prompt")
        joined = " ".join(argv)
        for denied in ("bash", "edit", "write", "dangerously-skip-permissions", "--allowedTools"):
            self.assertNotIn(denied, joined)
        self.assertNotIn("--no-tools", argv)  # web_search must stay reachable


class ShardTests(unittest.TestCase):
    def test_models_split_into_shards_of_shard_size(self):
        ids = [f"m{index}" for index in range(13)]
        shards = benchmark_fetch._shards(ids)
        self.assertEqual([len(shard) for shard in shards], [6, 6, 1])
        self.assertEqual([model for shard in shards for model in shard], ids)

    def test_one_shard_per_six_models(self):
        scores = [{"id": f"m{index}", "score": 50.0} for index in range(13)]
        fake = _fake(_omp_result(scores))
        with mock.patch.object(subprocess, "run", return_value=fake) as run:
            result = benchmark_fetch.fetch_benchmark_scores([f"m{index}" for index in range(13)])
        # 13 models, shards of 6 -> 3 calls, no more than 2 at a time.
        self.assertEqual(run.call_count, 3)
        self.assertTrue(result["live"])
        self.assertEqual(len(result["scores"]), 13)

    def test_lock_ttl_covers_every_round_of_the_fan_out(self):
        # 6 models: one shard, one round. 12 models: 2 shards, 2 workers,
        # still one round. 13 models: 3 shards, 2 workers, two rounds.
        self.assertEqual(benchmark_fetch._lock_ttl(6), benchmark_fetch.SHARD_TIMEOUT + 60.0)
        self.assertEqual(benchmark_fetch._lock_ttl(12), benchmark_fetch.SHARD_TIMEOUT + 60.0)
        self.assertEqual(benchmark_fetch._lock_ttl(13), 2 * benchmark_fetch.SHARD_TIMEOUT + 60.0)


class ParseScoresTests(unittest.TestCase):
    def test_parses_past_the_working_status_line(self):
        stdout = 'Working...\n{"scores": [{"id": "x", "score": 1}]}'
        self.assertEqual(benchmark_fetch._parse_scores(stdout), [{"id": "x", "score": 1}])

    def test_parses_a_markdown_fenced_reply(self):
        stdout = 'some prose\n```json\n{"scores": []}\n```\ntrailing prose'
        self.assertEqual(benchmark_fetch._parse_scores(stdout), [])

    def test_garbage_stdout_is_none(self):
        self.assertIsNone(benchmark_fetch._parse_scores("the agent said nothing useful"))

    def test_json_without_scores_list_is_none(self):
        self.assertIsNone(benchmark_fetch._parse_scores('{"result": "ok"}'))


def _omp_result(scores):
    return json.dumps({"scores": scores})


def _fake(stdout, returncode=0, stderr=""):
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr=stderr)


class FetchBenchmarkScoresTests(unittest.TestCase):
    def test_empty_model_list_is_unavailable_without_a_subprocess_call(self):
        with mock.patch.object(subprocess, "run") as run:
            result = benchmark_fetch.fetch_benchmark_scores([])
        run.assert_not_called()
        self.assertFalse(result["live"])
        self.assertIn("stale_reason", result)

    def test_every_call_is_bounded_by_the_shard_timeout(self):
        fake = _fake(_omp_result([{"id": "a", "score": 1}]))
        with mock.patch.object(subprocess, "run", return_value=fake) as run:
            benchmark_fetch.fetch_benchmark_scores(["a"])
        self.assertEqual(run.call_args.kwargs["timeout"], benchmark_fetch.SHARD_TIMEOUT)

    def test_one_failed_shard_does_not_sink_the_rest(self):
        def side_effect(argv, **kwargs):
            # The shard asked about b1/b2/b3/c1 fails; the first shard answers.
            if "b1" in argv[-1]:
                return _fake("", returncode=1, stderr="rate limited")
            answered = [model_id for model_id in ("a1", "a2", "a3", "a4", "a5", "a6") if f"- {model_id}" in argv[-1]]
            return _fake(_omp_result([{"id": model_id, "score": 50.0} for model_id in answered]))

        with mock.patch.object(subprocess, "run", side_effect=side_effect):
            # 10 models, shards of 6 -> shard one is a1..a6, shard two is
            # b1/b2/b3/c1 and fails.
            result = benchmark_fetch.fetch_benchmark_scores(["a1", "a2", "a3", "a4", "a5", "a6", "b1", "b2", "b3", "c1"])

        self.assertTrue(result["live"])
        # The failed shard's models are the only ones missing, and the run
        # says so instead of silently dropping them.
        self.assertEqual({entry["id"] for entry in result["scores"]}, {"a1", "a2", "a3", "a4", "a5", "a6"})
        self.assertIn("stale_reason", result)
        self.assertIn("b1", result["stale_reason"])
        self.assertIn("c1", result["stale_reason"])

    def test_all_shards_failing_keeps_previous_scores_and_reports_not_live(self):
        previous = [{"id": "cached", "score": 10, "fetched_at": "2026-10-07T00:00:00+00:00"}]
        with mock.patch.object(subprocess, "run", side_effect=FileNotFoundError):
            result = benchmark_fetch.fetch_benchmark_scores(["a", "b"], previous=previous)
        self.assertFalse(result["live"])
        # The cached entry survives: a bad day dims the data, it does not
        # wipe it. (fetch_benchmark_scores returns an _unavailable result,
        # which refresh_snapshot leaves alone -- see its own tests.)
        self.assertIn("omp CLI not found", result["stale_reason"])

    def test_timeout_is_unavailable_not_a_crash(self):
        with mock.patch.object(
            subprocess, "run", side_effect=subprocess.TimeoutExpired(cmd="omp", timeout=1)
        ):
            result = benchmark_fetch.fetch_benchmark_scores(["sonnet"])
        self.assertFalse(result["live"])
        self.assertIn("timed out", result["stale_reason"])

    def test_nonzero_exit_is_unavailable(self):
        with mock.patch.object(subprocess, "run", return_value=_fake("", returncode=1, stderr="boom")):
            result = benchmark_fetch.fetch_benchmark_scores(["sonnet"])
        self.assertFalse(result["live"])
        self.assertIn("boom", result["stale_reason"])

    def test_unparseable_output_is_unavailable(self):
        with mock.patch.object(subprocess, "run", return_value=_fake("I could not find anything")):
            result = benchmark_fetch.fetch_benchmark_scores(["sonnet"])
        self.assertFalse(result["live"])
        self.assertIn("JSON scores object", result["stale_reason"])

    def test_all_shards_failing_keeps_previous_scores_and_reports_not_live(self):
        previous = [{"id": "cached", "score": 10, "fetched_at": "2026-10-07T00:00:00+00:00"}]
        with mock.patch.object(subprocess, "run", side_effect=FileNotFoundError):
            result = benchmark_fetch.fetch_benchmark_scores(["a", "b"], previous=previous)
        self.assertFalse(result["live"])
        self.assertIn("omp CLI not found", result["stale_reason"])
        # Nothing new was fetched, so nothing about `previous` is asserted
        # here -- refresh_snapshot owns merging and its own tests cover the
        # "a bad day cannot wipe yesterday's scores" contract.

    def test_malformed_scores_drop_bad_entries_and_notes(self):
        scores = [
            {"id": "sonnet", "score": 80, "note": {"text": "Unsafe link", "source": "javascript:alert(1)"}},
            {"id": "", "score": 10},
            "not a dict",
            {"id": "opus", "score": "high"},
            {"id": "long", "score": 81, "note": {"text": "x" * 281, "source": "https://example.org"}},
            {"id": "no-source", "score": 82, "note": {"text": "No citation", "source": None}},
        ]
        with mock.patch.object(subprocess, "run", return_value=_fake(_omp_result(scores))):
            result = benchmark_fetch.fetch_benchmark_scores(["sonnet"])
        self.assertTrue(result["live"])
        self.assertEqual(
            result["scores"],
            [
                {"id": "sonnet", "score": 80, "fetched_at": result["scores"][0]["fetched_at"]},
                {"id": "long", "score": 81, "fetched_at": result["scores"][0]["fetched_at"]},
                {"id": "no-source", "score": 82, "fetched_at": result["scores"][0]["fetched_at"]},
            ],
        )


class MergeScoresTests(unittest.TestCase):
    def test_new_entry_replaces_previous_by_id(self):
        merged = benchmark_fetch._merge_scores(
            [{"id": "a", "score": 1, "fetched_at": "2026-10-01T00:00:00+00:00"}],
            [{"id": "a", "score": 2}],
            "2026-10-08T00:00:00+00:00",
        )
        self.assertEqual([entry["score"] for entry in merged], [2])
        self.assertEqual(merged[0]["fetched_at"], "2026-10-08T00:00:00+00:00")

    def test_new_entry_replaces_a_date_suffixed_previous(self):
        merged = benchmark_fetch._merge_scores(
            [{"id": "claude-opus-4-5-20251101", "score": 1, "fetched_at": "2026-10-01T00:00:00+00:00"}],
            [{"id": "claude-opus-4-5", "score": 2}],
            "2026-10-08T00:00:00+00:00",
        )
        self.assertEqual([entry["id"] for entry in merged], ["claude-opus-4-5"])

    def test_null_result_never_erases_a_verified_score(self):
        # Measured 2026-10-08: search providers rate-limit a wide fan-out,
        # and the agent then answers score: null for models it scored the
        # day before. That null must not overwrite the verified score.
        merged = benchmark_fetch._merge_scores(
            [{"id": "a", "score": 58, "fetched_at": "2026-10-07T00:00:00+00:00"}],
            [{"id": "a", "score": None, "reason": "search providers all rate-limited"}],
            "2026-10-08T00:00:00+00:00",
        )
        self.assertEqual(merged[0]["score"], 58)
        self.assertEqual(merged[0]["fetched_at"], "2026-10-07T00:00:00+00:00")

    def test_null_result_still_lands_when_nothing_was_cached(self):
        merged = benchmark_fetch._merge_scores(
            [],
            [{"id": "new", "score": None, "reason": "no public leaderboard entry"}],
            "2026-10-08T00:00:00+00:00",
        )
        self.assertIsNone(merged[0]["score"])
        self.assertIn("reason", merged[0])

    def test_untouched_entries_keep_their_own_fetched_at(self):
        merged = benchmark_fetch._merge_scores(
            [
                {"id": "old", "score": 1, "fetched_at": "2026-10-01T00:00:00+00:00"},
                {"id": "no-stamp", "score": 3},
            ],
            [{"id": "new", "score": 2}],
            "2026-10-08T00:00:00+00:00",
        )
        by_id = {entry["id"]: entry for entry in merged}
        self.assertEqual(by_id["old"]["fetched_at"], "2026-10-01T00:00:00+00:00")
        self.assertEqual(by_id["no-stamp"]["fetched_at"], "2026-10-08T00:00:00+00:00")
        self.assertEqual(by_id["new"]["fetched_at"], "2026-10-08T00:00:00+00:00")

    def test_newest_entry_time_drives_the_snapshot_field(self):
        entries = [
            {"fetched_at": "2026-10-01T00:00:00+00:00"},
            {"fetched_at": "2026-10-08T00:00:00+00:00"},
            {"fetched_at": "2026-10-05T00:00:00+00:00"},
        ]
        self.assertEqual(benchmark_fetch._newest_entry_time(entries), "2026-10-08T00:00:00+00:00")


class IdsNeedingRefreshTests(unittest.TestCase):
    def setUp(self):
        self.fresh = benchmark_fetch._now_iso()
        self.stale = "2000-01-01T00:00:00+00:00"

    def test_fresh_scored_entry_is_not_asked_again(self):
        cached = [{"id": "a", "score": 1, "fetched_at": self.fresh}]
        self.assertEqual(benchmark_fetch._ids_needing_refresh(cached, ["a"], force=False), [])

    def test_unscored_entry_is_retried_every_day(self):
        # A model with no public score yet must be retried: that is how a
        # score that appears on Thursday gets found.
        cached = [{"id": "a", "score": None, "fetched_at": self.fresh}]
        self.assertEqual(benchmark_fetch._ids_needing_refresh(cached, ["a"], force=False), ["a"])

    def test_old_entry_is_asked_again(self):
        cached = [{"id": "a", "score": 1, "fetched_at": self.stale}]
        self.assertEqual(benchmark_fetch._ids_needing_refresh(cached, ["a"], force=False), ["a"])

    def test_date_suffix_match_skips_the_ask(self):
        cached = [{"id": "claude-opus-4-5", "score": 1, "fetched_at": self.fresh}]
        self.assertEqual(benchmark_fetch._ids_needing_refresh(cached, ["claude-opus-4-5-20251101"], force=False), [])

    def test_force_asks_everything(self):
        cached = [{"id": "a", "score": 1, "fetched_at": self.fresh}]
        self.assertEqual(benchmark_fetch._ids_needing_refresh(cached, ["a", "b"], force=True), ["a", "b"])


class ScoreMatchingTests(unittest.TestCase):
    def test_exact_id_match(self):
        scores = [{"id": "sonnet", "score": 80}]
        self.assertEqual(benchmark_fetch.match_score("sonnet", scores)["score"], 80)

    def test_date_suffix_is_stripped_for_matching(self):
        scores = [{"id": "claude-opus-4-5", "score": 73.1}]
        self.assertEqual(
            benchmark_fetch.match_score("claude-opus-4-5-20251101", scores)["score"], 73.1
        )

    def test_no_match_is_none(self):
        self.assertIsNone(benchmark_fetch.match_score("made-up", [{"id": "sonnet", "score": 1}]))


# The cache/lock tests need the real-redis-server fixtures (`redis_port`,
# `flush_redis`, `closed_port`) that `conftest.py` defines for pytest, not
# unittest -- written as plain functions, same as `test_slots_redis.py`.

def test_read_snapshot_missing_key_is_none(redis_port, flush_redis):
    assert benchmark_fetch.read_snapshot(**_kw(redis_port)) is None


def test_read_snapshot_corrupt_value_is_none(redis_port, flush_redis):
    client = redis_lib.Redis(host="127.0.0.1", port=redis_port)
    client.set(benchmark_fetch.REDIS_KEY, "{not json")
    assert benchmark_fetch.read_snapshot(**_kw(redis_port)) is None


def test_read_snapshot_unreachable_redis_is_none_not_a_crash(closed_port):
    assert benchmark_fetch.read_snapshot(redis_host="127.0.0.1", redis_port=closed_port) is None


def test_refresh_skips_the_agent_when_every_entry_is_fresh(redis_port, flush_redis):
    fresh = {
        "fetched_at": benchmark_fetch._now_iso(),
        "live": True,
        "scores": [{"id": "sonnet", "score": 1, "fetched_at": benchmark_fetch._now_iso()}],
    }
    client = redis_lib.Redis(host="127.0.0.1", port=redis_port)
    client.set(benchmark_fetch.REDIS_KEY, json.dumps(fresh))

    with (
        mock.patch.object(benchmark_fetch, "fetch_benchmark_scores") as fetch,
        mock.patch.object(benchmark_fetch, "model_ids_for_scoring", return_value=["sonnet"]),
    ):
        result = benchmark_fetch.refresh_snapshot(**_kw(redis_port))

    fetch.assert_not_called()
    assert result == fresh


def test_refresh_asks_only_for_the_models_that_need_it(redis_port, flush_redis):
    now = benchmark_fetch._now_iso()
    stale = "2000-01-01T00:00:00+00:00"
    cached = {
        "fetched_at": stale,
        "live": True,
        "scores": [
            {"id": "fresh", "score": 1, "fetched_at": now},
            {"id": "stale", "score": 2, "fetched_at": stale},
            {"id": "gone", "score": 3, "fetched_at": stale},
        ],
    }
    client = redis_lib.Redis(host="127.0.0.1", port=redis_port)
    client.set(benchmark_fetch.REDIS_KEY, json.dumps(cached))
    fresh = {
        "fetched_at": now,
        "live": True,
        "source": "test",
        "scores": [
            {"id": "fresh", "score": 1, "fetched_at": now},
            {"id": "stale", "score": 22, "fetched_at": now},
            {"id": "new", "score": 4, "fetched_at": now},
        ],
    }

    with (
        mock.patch.object(benchmark_fetch, "fetch_benchmark_scores", return_value=fresh) as fetch,
        mock.patch.object(benchmark_fetch, "model_ids_for_scoring", return_value=["fresh", "stale", "new"]),
    ):
        result = benchmark_fetch.refresh_snapshot(**_kw(redis_port))

    # "fresh" was fresh, "gone" left the live model list: only the models
    # with no usable recent score are asked about, and the cached scores
    # pass through as `previous` so they cannot be lost.
    fetch.assert_called_once_with(["stale", "new"], previous=cached["scores"])
    assert result == fresh
    assert json.loads(client.get(benchmark_fetch.REDIS_KEY)) == fresh
    assert slots_redis.status(**_kw(redis_port))[benchmark_fetch.LOCK_SLOT]["holders"] == 0


def test_refresh_force_bypasses_freshness_but_still_takes_the_lock(redis_port, flush_redis):
    fresh_cached = {
        "fetched_at": benchmark_fetch._now_iso(),
        "live": True,
        "scores": [{"id": "old", "score": 1, "fetched_at": benchmark_fetch._now_iso()}],
    }
    client = redis_lib.Redis(host="127.0.0.1", port=redis_port)
    client.set(benchmark_fetch.REDIS_KEY, json.dumps(fresh_cached))
    new = {"fetched_at": benchmark_fetch._now_iso(), "live": True, "source": "test", "scores": [{"id": "new", "score": 2}]}

    with (
        mock.patch.object(benchmark_fetch, "fetch_benchmark_scores", return_value=new) as fetch,
        mock.patch.object(benchmark_fetch, "model_ids_for_scoring", return_value=["old"]),
    ):
        result = benchmark_fetch.refresh_snapshot(force=True, **_kw(redis_port))

    fetch.assert_called_once()
    assert result == new


def test_refresh_reads_cache_instead_of_fetching_when_lock_is_busy(redis_port, flush_redis):
    cached = {
        "fetched_at": "2000-01-01T00:00:00+00:00",
        "live": True,
        "scores": [{"id": "old", "score": 1, "fetched_at": "2000-01-01T00:00:00+00:00"}],
    }
    client = redis_lib.Redis(host="127.0.0.1", port=redis_port)
    client.set(benchmark_fetch.REDIS_KEY, json.dumps(cached))
    # Another machine already holds the lock.
    slots_redis.acquire(benchmark_fetch.LOCK_SLOT, "other-machine", max_holders=1, **_kw(redis_port))

    with (
        mock.patch.object(benchmark_fetch, "fetch_benchmark_scores") as fetch,
        mock.patch.object(benchmark_fetch, "model_ids_for_scoring", return_value=["old"]),
    ):
        result = benchmark_fetch.refresh_snapshot(**_kw(redis_port))

    fetch.assert_not_called()
    assert result == cached


def test_refresh_unreachable_redis_is_honest_not_a_crash(closed_port):
    with (
        mock.patch.object(benchmark_fetch, "fetch_benchmark_scores") as fetch,
        mock.patch.object(benchmark_fetch, "model_ids_for_scoring", return_value=["x"]),
    ):
        result = benchmark_fetch.refresh_snapshot(redis_host="127.0.0.1", redis_port=closed_port)

    fetch.assert_not_called()
    assert result["live"] is False
    assert "unreachable" in result["stale_reason"]


def test_refresh_refused_login_raises_and_does_not_say_unreachable(auth_redis_port, no_client_retry):
    with (
        mock.patch.object(benchmark_fetch, "fetch_benchmark_scores") as fetch,
        mock.patch.object(benchmark_fetch, "model_ids_for_scoring", return_value=["x"]),
    ):
        with pytest.raises(slots_redis.CoordinatorAuthFailed) as caught:
            benchmark_fetch.refresh_snapshot(redis_host="127.0.0.1", redis_port=auth_redis_port)

    fetch.assert_not_called()
    assert "unreachable" not in str(caught.value)
    assert "Check the Redis password" in str(caught.value)


def test_refresh_releases_lock_even_if_fetch_raises(redis_port, flush_redis):
    with (
        mock.patch.object(benchmark_fetch, "fetch_benchmark_scores", side_effect=RuntimeError("boom")),
        mock.patch.object(benchmark_fetch, "model_ids_for_scoring", return_value=["x"]),
    ):
        with pytest.raises(RuntimeError):
            benchmark_fetch.refresh_snapshot(**_kw(redis_port))

    assert slots_redis.status(**_kw(redis_port))[benchmark_fetch.LOCK_SLOT]["holders"] == 0


def test_refresh_cache_write_failure_is_reported_and_result_kept(redis_port, flush_redis, capsys):
    fresh = {"fetched_at": benchmark_fetch._now_iso(), "live": True, "source": "test", "scores": [{"id": "x", "score": 1}]}
    client = redis_lib.Redis(host="127.0.0.1", port=redis_port)
    client.set = mock.Mock(side_effect=redis_lib.exceptions.ConnectionError("boom"))

    with (
        mock.patch.object(benchmark_fetch, "_client", return_value=client),
        mock.patch.object(benchmark_fetch, "fetch_benchmark_scores", return_value=fresh),
        mock.patch.object(benchmark_fetch, "model_ids_for_scoring", return_value=["x"]),
    ):
        result = benchmark_fetch.refresh_snapshot(**_kw(redis_port))

    err = capsys.readouterr().err
    assert result == fresh
    assert len(err.splitlines()) == 1
    assert "not cached" in err and "boom" in err
    assert slots_redis.status(**_kw(redis_port))[benchmark_fetch.LOCK_SLOT]["holders"] == 0


def test_refresh_with_no_models_anywhere_is_unavailable_without_the_lock(redis_port, flush_redis):
    with (
        mock.patch.object(benchmark_fetch, "fetch_benchmark_scores") as fetch,
        mock.patch.object(benchmark_fetch, "model_ids_for_scoring", return_value=[]),
    ):
        result = benchmark_fetch.refresh_snapshot(**_kw(redis_port))

    fetch.assert_not_called()
    assert result["live"] is False
    assert "no model ids" in result["stale_reason"]


class CliFetchBenchmarksTests(unittest.TestCase):
    def test_cli_prints_summary_and_respects_force(self):
        fixed = {"fetched_at": "2026-10-07T00:00:00+00:00", "live": True, "scores": [{"id": "x", "score": 1}]}
        with mock.patch.object(benchmark_fetch, "refresh_snapshot", return_value=fixed) as refresh:
            code = cli.main(["fetch-benchmarks", "--force", "--json"])
        self.assertEqual(code, 0)
        refresh.assert_called_once()
        self.assertTrue(refresh.call_args.kwargs["force"])

    def test_cli_default_is_not_forced(self):
        fixed = {"fetched_at": "2026-10-07T00:00:00+00:00", "live": False, "stale_reason": "x", "scores": []}
        with mock.patch.object(benchmark_fetch, "refresh_snapshot", return_value=fixed) as refresh:
            code = cli.main(["fetch-benchmarks"])
        self.assertEqual(code, 0)
        self.assertFalse(refresh.call_args.kwargs["force"])

    def test_cli_plain_output_lists_each_unscored_model_and_reason(self):
        fixed = {
            "fetched_at": "2000-01-01T00:00:00+00:00",
            "live": True,
            "scores": [
                {"id": "missing", "score": None, "reason": "no public leaderboard entry"},
                {"id": "x", "score": 1},
            ],
        }
        with (
            mock.patch.object(benchmark_fetch, "refresh_snapshot", return_value=fixed),
            mock.patch("builtins.print") as output,
        ):
            self.assertEqual(cli.main(["fetch-benchmarks"]), 0)
        self.assertEqual(output.call_count, 2)
        self.assertRegex(
            output.call_args_list[0].args[0],
            r"benchmarks: 1/2 scored \(live; fetched \d+h ago\)",
        )
        self.assertEqual(
            output.call_args_list[1].args[0],
            "  unscored: missing — no public leaderboard entry",
        )


if __name__ == "__main__":
    unittest.main()
