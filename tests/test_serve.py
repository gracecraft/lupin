import contextlib
import html
import io
import socket
import sys
import threading
import urllib.error
import urllib.request

import ipaddress
import json
import os
import tempfile
import time
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from importlib import resources
from unittest import mock
from urllib.parse import quote

import pytest
import redis as redis_lib

from lupin import (
    cli, claims, commands, ledger, machines, quest, quota_cache, roadmap, serve, slots,
    slots_redis, usage_cache,
)


def _kw(redis_port):
    return {"redis_host": "127.0.0.1", "redis_port": redis_port}


def _issue_json(number, state="OPEN"):
    return {"number": number, "state": state}


def _fake_locate(table):
    """Stand-in for `quest._locate_issue` -- see test_quest.py's copy of
    this same helper for the full contract."""

    def _locate(number, repos, code_dir, **kw):
        return table.get(number)

    return _locate


def _quest_handler(redis_port):
    """A `Handler` wired to the test's throwaway redis-server, with the
    same `Handler.__new__` + mocked I/O pattern `DashboardRouteTests` uses
    for GET routes -- do_POST needs `.headers`/`.rfile` too."""
    handler = serve.Handler.__new__(serve.Handler)
    handler.fleet_connection = _kw(redis_port)
    handler.host_ok = mock.Mock(return_value=True)
    handler.reply = mock.Mock()
    handler.redirect = mock.Mock()
    return handler


def _post_body(handler, path, fields: dict) -> None:
    from urllib.parse import urlencode

    body = urlencode(fields, doseq=True).encode("utf-8")
    handler.path = path
    handler.headers = {"Content-Length": str(len(body))}
    handler.rfile = io.BytesIO(body)


class TimerTests(unittest.TestCase):
    def test_lists_lupin_timers_not_watchdogs(self):
        identifier = "0123456789abcdef"
        rows = [
            {"unit": "delegation-loop-claude-watchdog.timer", "next": 10_000_000},
            {"unit": "delegation-loop-watchdog.timer", "next": 15_000_000},
            {"unit": "delegation-loop.timer", "next": 20_000_000},
            {"unit": f"lupin-once-{identifier}.timer", "next": 30_000_000},
            {"unit": "delegation-loop-once-123.timer", "next": 40_000_000},
        ]
        with mock.patch.object(serve, "run", return_value=(0, json.dumps(rows))):
            timers = serve.timers()

        self.assertEqual(
            [timer["unit"] for timer in timers],
            ["delegation-loop.timer", f"lupin-once-{identifier}.timer"],
        )

    def test_render_reads_one_off_repos_from_lupin_state(self):
        identifier = "0123456789abcdef"
        state = {
            "loops": [],
            "enabled": ["repo-enabled"],
            "timers": [
                {"unit": "delegation-loop.timer", "next": 1_800_000_000, "last": None},
                {
                    "unit": f"lupin-once-{identifier}.timer",
                    "next": 1_800_000_060,
                    "last": None,
                },
            ],
            "timer_active": True,
            "repos": [],
        }
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = Path(temporary)
            once_dir = state_dir / "once"
            once_dir.mkdir()
            (once_dir / f"{identifier}.json").write_text(
                json.dumps({"repos": ["repo-one", "repo-two"]}), encoding="utf-8"
            )
            with (
                mock.patch.object(serve.loop_runtime, "STATE_DIR", state_dir),
                mock.patch.object(serve, "run") as run,
            ):
                page = serve.render_dashboard(state).decode()

        self.assertIn("all enabled repos", page)
        self.assertIn("<td>repo-one, repo-two", page)
        run.assert_not_called()

    def test_render_falls_back_when_one_off_state_is_missing_or_bad(self):
        identifier = "0123456789abcdef"
        unit = f"lupin-once-{identifier}.timer"
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = Path(temporary)
            once_dir = state_dir / "once"
            once_dir.mkdir()
            state_file = once_dir / f"{identifier}.json"
            for contents in (None, "{"):
                if contents is None:
                    state_file.unlink(missing_ok=True)
                else:
                    state_file.write_text(contents, encoding="utf-8")
                with (
                    self.subTest(contents=contents),
                    mock.patch.object(serve.loop_runtime, "STATE_DIR", state_dir),
                ):
                    self.assertEqual(serve.timer_repository(unit), unit)

    def test_rendered_next_run_timestamps_include_timezone(self):
        state = {
            "loops": [],
            "enabled": [],
            "timers": [
                {
                    "unit": "delegation-loop.timer",
                    "next": 1_800_000_000,
                    "last": None,
                }
            ],
            "timer_active": True,
            "repos": [],
        }
        with mock.patch.object(serve.time, "strftime", wraps=serve.time.strftime) as fmt:
            page = serve.render_dashboard(state).decode()

        self.assertEqual(fmt.call_count, 2)
        self.assertTrue(all(call.args[0].endswith("%Z") for call in fmt.call_args_list))
        self.assertRegex(page, r"\d{2}:\d{2}:\d{2} [A-Z]{2,5}")

    def test_dashboard_offers_commands_for_enabled_repos_without_doc_warnings(self):
        state = {
            "loops": [],
            "enabled": ["repo-enabled", "repo-disabled"],
            "timers": [],
            "timer_active": False,
            "repos": [
                {"repo": "repo-enabled", "state": "enabled", "loopable": True},
                {"repo": "repo-disabled", "state": "disabled", "loopable": True},
                {"repo": "repo-no-doc", "state": "disabled", "loopable": True, "has_doc": False},
            ],
        }

        page = serve.render_dashboard(state).decode()

        self.assertIn("lupin once now repo-enabled", page)
        self.assertIn("data-once-repo='repo-enabled'", page)
        self.assertIn("<th>one-off command</th>", page)
        self.assertNotIn("data-once-repo='repo-disabled'", page)
        self.assertNotIn("data-once-repo='repo-no-doc'", page)
        self.assertNotIn("lupin once repo-disabled", page)
        self.assertNotIn("repo(s) missing docs/delegation-loop.md", page)


class TimeFormattingTests(unittest.TestCase):
    def test_time_until_reset_formats_remaining_time(self):
        self.assertEqual(serve.time_until_reset(90_060_000, now_ms=0), "1d 1h 1m")
        self.assertEqual(serve.time_until_reset(30_000, now_ms=0), "<1m")
        self.assertEqual(serve.time_until_reset(0, now_ms=0), "now")
        self.assertEqual(serve.time_until_reset(None, now_ms=0), "-")


class QuotaRenderingTests(unittest.TestCase):
    """`/usage` rendering only. The shared quota and 7-day usage snapshots
    are mocked, so these tests cover how rows become HTML.
    """

    def test_quota_bar_marks_window_time_and_quota_progress(self):
        row = {
            "provider": "openai",
            "duration": serve.QuotaDuration.WEEKLY,
            "used_pct": 42,
            "resets_at": 302_400_000,
        }
        rendered = serve.render_quota_row(row, now_ms=0)

        self.assertEqual(serve.time_remaining_pct(row, now_ms=0), 50)
        self.assertEqual(serve.quota_elapsed_pct(row, now_ms=0), 50)
        self.assertIn("width:42.0%", rendered)
        self.assertIn("left:50.0%", rendered)
        self.assertIn("3d 12h", rendered)
        self.assertIn("42% used", rendered)

    def test_quota_section_groups_by_provider_and_precedes_totals(self):
        # Real-shaped row, as `quota.quota_usage()` would produce it --
        # confirmed against that module's own tests.
        reset_at_ms = 1_790_547_474_348
        rows = [{
            "provider": "opencode-go",
            "duration": serve.QuotaDuration.MONTHLY,
            "label": "Monthly limit",
            "used_pct": 64.0,
            "resets_at": reset_at_ms,
            "generated_at": "2026-01-01 00:00:00",
        }]
        snapshot = {
            "opencode-go": {
                "rows": rows,
                "fetched_at": "2026-01-01T00:00:00+00:00",
                "fetched_by": "jesus",
            }
        }
        with (
            mock.patch.object(quota_cache, "read_snapshot", return_value=snapshot),
            mock.patch.object(usage_cache, "read_snapshot", return_value={}),
        ):
            page = serve.render_usage().decode()

        self.assertIn(
            f"data-window-duration='P30D' data-resets-at-ms='{reset_at_ms}'",
            page,
        )
        self.assertIn("<h3>opencode-go</h3>", page)
        self.assertIn("<span><strong>36%</strong> available</span>", page)
        self.assertIn("64% used", page)
        self.assertIn("quota-meter-elapsed", page)
        self.assertIn("Needs attention", page)
        self.assertIn("Most used", page)
        self.assertIn("Next reset", page)
        self.assertLess(page.index("<h2>Quota</h2>"), page.index("<h2>7-day totals</h2>"))
        self.assertIn("Data timestamp: 2026-01-01 00:00:00", page)
        # Staleness is visible, per provider -- fetched via this fake "jesus".
        self.assertIn("fetched", page)
        self.assertIn("via jesus", page)
        self.assertIn("stale", page)

    def test_quota_note_and_error_rows_render_as_dim_text(self):
        omp_snapshot = {
            "omp": {
                "rows": [{"provider": "omp", "error": "unavailable (RuntimeError)"}],
                "fetched_at": "2026-01-01T00:00:00+00:00",
                "fetched_by": "jesus",
            }
        }
        with (
            mock.patch.object(quota_cache, "read_snapshot", return_value=omp_snapshot),
            mock.patch.object(usage_cache, "read_snapshot", return_value={}),
        ):
            page = serve.render_usage().decode()
        self.assertIn("<h3>omp</h3>", page)
        self.assertIn("<p class=dim>unavailable (RuntimeError)</p>", page)

        claude_snapshot = {
            "claude": {
                "rows": [{"provider": "claude", "note": "quota unavailable"}],
                "fetched_at": "2026-01-01T00:00:00+00:00",
                "fetched_by": "jesus",
            }
        }
        with (
            mock.patch.object(quota_cache, "read_snapshot", return_value=claude_snapshot),
            mock.patch.object(usage_cache, "read_snapshot", return_value={}),
        ):
            page = serve.render_usage().decode()
        self.assertIn("<h3>claude</h3>", page)
        self.assertIn("quota unavailable", page)

    def test_seven_day_totals_aggregate_machine_snapshots_and_show_staleness(self):
        now = serve.datetime.now(serve.timezone.utc).isoformat()
        snapshots = {
            "jesus": {
                "rows": [
                    {
                        "provider": "claude",
                        "input_tokens": 130,
                        "output_tokens": None,
                        "cost": None,
                        "period": "last 7 days",
                        "source": "/claude/stats.json",
                        "last_update": "2026-10-08",
                    },
                    {
                        "provider": "openai-codex",
                        "input_tokens": 15,
                        "output_tokens": 10,
                        "cost": 0.5,
                        "period": "last 7 days",
                        "source": "/omp/stats.db",
                        "last_update": "2026-10-08 00:00:00",
                    },
                    {
                        "provider": "opencode-go",
                        "error": "unavailable (FileNotFoundError)",
                        "source": "/opencode/stats.db",
                    },
                ],
                "fetched_at": now,
                "fetched_by": "jesus",
            },
            "ralpha": {
                "rows": [{
                    "provider": "openai-codex",
                    "input_tokens": 6,
                    "output_tokens": 5,
                    "cost": 0.25,
                    "period": "last 7 days",
                    "source": "/omp/stats.db",
                    "last_update": "2026-10-08 00:00:00",
                }],
                "fetched_at": "2000-01-01T00:00:00+00:00",
                "fetched_by": "ralpha",
            },
        }
        with (
            mock.patch.object(quota_cache, "read_snapshot", return_value={}),
            mock.patch.object(usage_cache, "read_snapshot", return_value=snapshots),
        ):
            page = serve.render_usage().decode()

        self.assertIn("<td>claude</td><td>130</td><td>-</td><td>not tracked</td>", page)
        self.assertIn("<td>openai-codex</td><td>21</td><td>15</td><td>$0.75</td>", page)
        self.assertIn(
            "<td>opencode-go</td><td colspan=3>unavailable (FileNotFoundError)</td>",
            page,
        )
        self.assertIn("Usage snapshots:", page)
        self.assertIn("jesus fetched", page)
        self.assertIn("ralpha fetched", page)
        self.assertIn("stale", page)

    def test_missing_usage_snapshot_is_not_rendered_as_zero_totals(self):
        with (
            mock.patch.object(quota_cache, "read_snapshot", return_value={}),
            mock.patch.object(usage_cache, "read_snapshot", return_value={}),
        ):
            page = serve.render_usage().decode()

        self.assertIn("No shared 7-day usage data cached yet", page)
        self.assertNotIn("<td>claude</td><td>0</td>", page)


class BindAddressTests(unittest.TestCase):
    def test_allows_loopback(self):
        self.assertTrue(serve.bind_allowed(ipaddress.ip_address("127.0.0.1")))

    def test_allows_tailnet_range(self):
        self.assertTrue(serve.bind_allowed(ipaddress.ip_address("100.64.0.11")))

    def test_rejects_lan_address(self):
        self.assertFalse(serve.bind_allowed(ipaddress.ip_address("192.168.1.5")))

    def test_rejects_any_address(self):
        self.assertFalse(serve.bind_allowed(ipaddress.ip_address("0.0.0.0")))

    def test_rejects_tailnet_range_over_ipv6(self):
        # TAILNET_RANGE is an IPv4 network; an IPv6 address never matches it
        # even if its numeric value would overlap, so this must still be
        # loopback-or-nothing for v6.
        self.assertFalse(serve.bind_allowed(ipaddress.ip_address("::1:0:0:0")))


class DashboardRouteTests(unittest.TestCase):
    def test_usage_route_and_dashboard_link(self):
        state = {"loops": [], "enabled": [], "timers": [], "timer_active": False, "repos": []}
        self.assertIn("href='/usage'", serve.render_dashboard(state).decode())
        handler = serve.Handler.__new__(serve.Handler)
        handler.path = "/usage"
        handler.host_ok = mock.Mock(return_value=True)
        handler.reply = mock.Mock()
        with mock.patch.object(serve, "render_usage", return_value=b"usage page"):
            handler.do_GET()
        handler.reply.assert_called_once_with(b"usage page")

    def test_favicon_route_returns_svg(self):
        handler = serve.Handler.__new__(serve.Handler)
        handler.path = "/favicon.ico"
        handler.host_ok = mock.Mock(return_value=True)
        handler.send_response = mock.Mock()
        handler.send_header = mock.Mock()
        handler.end_headers = mock.Mock()
        handler.wfile = mock.Mock()

        handler.do_GET()

        handler.send_response.assert_called_once_with(200)
        self.assertIn(
            mock.call("Content-Type", "image/svg+xml"), handler.send_header.call_args_list
        )
        self.assertIn(
            mock.call("Cache-Control", "public, max-age=86400"),
            handler.send_header.call_args_list,
        )
        handler.wfile.write.assert_called_once_with(serve.FAVICON)
        head = serve.page("test", "").decode()
        self.assertIn("href='/favicon.ico' type='image/svg+xml'", head)
        self.assertIn("name=viewport", head)


class MachinesRouteUnitTests(unittest.TestCase):
    """Routing/dispatch logic only -- mocked machines/slots_redis calls, no
    real Redis. See MachinesPageIntegrationTests below for real data.
    """

    def test_machines_route_renders_records(self):
        handler = serve.Handler.__new__(serve.Handler)
        handler.path = "/machines"
        handler.fleet_connection = {"redis_host": "127.0.0.1"}
        handler.host_ok = mock.Mock(return_value=True)
        handler.reply = mock.Mock()
        with (
            mock.patch.object(serve.machines, "machines", return_value=[]) as fake,
            mock.patch.object(serve.slots_redis, "status", return_value={}),
        ):
            handler.do_GET()
        fake.assert_called_once_with({"redis_host": "127.0.0.1"})
        handler.reply.assert_called_once()

    def test_machines_route_unreachable_coordinator_is_502(self):
        handler = serve.Handler.__new__(serve.Handler)
        handler.path = "/machines"
        handler.fleet_connection = {}
        handler.host_ok = mock.Mock(return_value=True)
        handler.reply = mock.Mock()
        with mock.patch.object(
            serve.machines, "machines", side_effect=serve.machines.CoordinatorUnreachable("machine registry")
        ):
            handler.do_GET()
        status = handler.reply.call_args.args[1]
        self.assertEqual(status, 502)

    def test_slot_max_route_rejects_non_numeric_max(self):
        handler = serve.Handler.__new__(serve.Handler)
        handler.path = "/machines/slot-max"
        handler.headers = {"Content-Length": "15"}
        handler.rfile = io.BytesIO(b"slot=bmo&max=x")
        handler.host_ok = mock.Mock(return_value=True)
        handler.reply = mock.Mock()
        handler.do_POST()
        status = handler.reply.call_args.args[1]
        self.assertEqual(status, 400)

    def test_slot_max_route_rejects_unicode_digit_isdigit_cannot_parse(self):
        # '²' (superscript two) is str.isdigit() == True but int()
        # raises ValueError on it. The route must not crash on this -- it
        # should reject the request with the same clean 400 as "max=x".
        body = "slot=bmo&max=²".encode()
        handler = serve.Handler.__new__(serve.Handler)
        handler.path = "/machines/slot-max"
        handler.headers = {"Content-Length": str(len(body))}
        handler.rfile = io.BytesIO(body)
        handler.host_ok = mock.Mock(return_value=True)
        handler.reply = mock.Mock()
        handler.do_POST()
        status = handler.reply.call_args.args[1]
        self.assertEqual(status, 400)

    def test_slot_max_route_calls_set_max_and_redirects(self):
        handler = serve.Handler.__new__(serve.Handler)
        body = b"slot=bmo&max=3"
        handler.path = "/machines/slot-max"
        handler.headers = {"Content-Length": str(len(body))}
        handler.rfile = io.BytesIO(body)
        handler.fleet_connection = {"redis_host": "127.0.0.1"}
        handler.host_ok = mock.Mock(return_value=True)
        handler.send_response = mock.Mock()
        handler.send_header = mock.Mock()
        handler.end_headers = mock.Mock()
        with mock.patch.object(serve.slots_redis, "set_max") as fake_set_max:
            handler.do_POST()
        fake_set_max.assert_called_once_with("bmo", 3, redis_host="127.0.0.1")
        handler.send_response.assert_called_once_with(303)
        self.assertIn(mock.call("Location", "/machines"), handler.send_header.call_args_list)


class ModelTierTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.tiers_path = os.path.join(self.tempdir.name, "model-tiers.json")

    def write_tiers(self, data, raw=None):
        with open(self.tiers_path, "w", encoding="utf-8") as handle:
            handle.write(raw if raw is not None else json.dumps(data))

    def render(self):
        with mock.patch.object(serve, "MODEL_TIERS_PATH", self.tiers_path):
            return serve.render_model_tiers().decode()

    def test_full_three_tier_category_shows_every_model_and_effort_in_order(self):
        self.write_tiers({
            "_comment": "not a category",
            "coding": {
                "source": "Artificial Analysis Coding Agent Index",
                "last_verified": "2026-10-03",
                "tiers": {
                    "tier0": [{"model": "bmo:qwen", "effort": "low"}],
                    "tier1": [
                        {"model": "sonnet", "effort": "medium"},
                        {"model": "sonnet", "effort": "high"},
                    ],
                    "tier2": [{"model": "opus", "effort": "xhigh"}],
                },
                "note": "pick a tier and go",
            },
        })
        page = self.render()

        self.assertIn("<h3>coding</h3>", page)
        self.assertIn("verified 2026-10-03", page)
        self.assertIn("Artificial Analysis Coding Agent Index", page)
        self.assertIn("pick a tier and go", page)
        # The "_comment" key is prose, not a category card.
        self.assertNotIn("not a category", page)
        self.assertIn("xhigh", page)

    def test_tier_picks_keep_the_file_order_as_a_fallback_chain(self):
        rendered = serve.render_tier_picks({
            "tier1": [
                {"model": "sonnet", "effort": "medium"},
                {"model": "sonnet", "effort": "high"},
            ],
        })
        self.assertEqual(rendered.count("<span class=tier-pick>"), 2)
        self.assertLess(
            rendered.index("sonnet</span><span class=dim>medium"),
            rendered.index("sonnet</span><span class=dim>high"),
        )
        self.assertEqual(rendered.count("&rarr;"), 1)
        # A tier the row omits still gets a row, marked none.
        self.assertIn("<span class='tier-pick dim'>none</span>", rendered)

    def test_category_without_a_score_or_with_a_short_tier_list(self):
        # The real file has no numeric per-model score at all, and
        # frontend-ui/prose have no tier0 -- neither may be assumed.
        self.write_tiers({
            "frontend-ui": {
                "source": "manual (mirrors ship/SKILL.md step 8)",
                "last_verified": "2026-10-03",
                "tiers": {
                    "tier1": [{"model": "sonnet", "effort": "high"}],
                    "tier2": [{"model": "opus", "effort": "high"}],
                },
                "note": "no tier0: UI review always escalates to tier2",
            },
        })
        page = self.render()

        self.assertIn("<h3>frontend-ui</h3>", page)
        self.assertIn("manual (mirrors ship/SKILL.md step 8)", page)
        # Every tier key still gets a row; the absent one reads "none".
        for tier in serve.MODEL_TIER_ORDER:
            self.assertIn(f"<span class=tier-name>{tier}</span>", page)
        self.assertIn("<span class='tier-pick dim'>none</span>", page)
        # "score" is fine elsewhere on the page (the "All models" table's
        # Perf/Value columns, issue #17's reopen) -- just not inside this
        # category's own tier-card, which has no per-model score data.
        card_start = page.index("<h3>frontend-ui</h3>")
        card_end = page.index("</section>", card_start)
        self.assertNotIn("score", page[card_start:card_end])

    def test_file_sourced_text_is_escaped(self):
        self.write_tiers({
            "prose": {
                "source": "LMArena <b>Creative</b> Writing",
                "last_verified": "2026-10-03",
                "tiers": {"tier2": [{"model": "a<b", "effort": "x'high"}]},
                "note": 'Opus leads both, at "5.5"',
            },
        })
        page = self.render()

        self.assertIn("LMArena &lt;b&gt;Creative&lt;/b&gt; Writing", page)
        self.assertIn("a&lt;b", page)
        self.assertIn("x&#x27;high", page)
        self.assertIn("Opus leads both, at &quot;5.5&quot;", page)
        self.assertNotIn("<b>Creative</b>", page)

    def test_missing_or_malformed_file_reports_instead_of_crashing(self):
        with mock.patch.object(serve, "MODEL_TIERS_PATH", self.tiers_path + ".missing"):
            page = serve.render_model_tiers().decode()
        self.assertIn("unavailable (FileNotFoundError)", page)
        self.assertIn(self.tiers_path, page)

        self.write_tiers(None, raw="{")
        self.assertIn("unavailable (JSONDecodeError)", self.render())

        # Valid JSON that is not an object of categories.
        self.write_tiers(None, raw="[1, 2, 3]")
        self.assertIn("no categories in the file", self.render())

        self.write_tiers({"coding": {"tiers": {"tier0": [{"model": "opus"}]}}})
        page = self.render()
        self.assertIn("verified -", page)
        self.assertIn("not recorded", page)

    def test_shipped_model_tiers_file_renders_every_category(self):
        real_path = str(resources.files("lupin").joinpath("model-tiers.json"))
        with mock.patch.object(serve, "MODEL_TIERS_PATH", real_path):
            rows = serve.model_tiers()
            page = serve.render_model_tiers().decode()

        categories = {row["category"] for row in rows}
        self.assertEqual(
            categories,
            {"coding", "general", "frontend-ui", "translation", "prose", "cad-spatial"},
        )
        for category in categories:
            self.assertIn(f"<h3>{category}</h3>", page)

    def test_model_tiers_route_and_dashboard_link(self):
        state = {"loops": [], "enabled": [], "timers": [], "timer_active": False, "repos": []}
        self.assertIn("href='/model-tiers'", serve.render_dashboard(state).decode())
        handler = serve.Handler.__new__(serve.Handler)
        handler.path = "/model-tiers"
        handler.host_ok = mock.Mock(return_value=True)
        handler.reply = mock.Mock()
        with mock.patch.object(serve, "render_model_tiers", return_value=b"tiers page"):
            handler.do_GET()
        handler.reply.assert_called_once_with(b"tiers page")

    def test_model_tiers_route_passes_sent_query_through(self):
        handler = serve.Handler.__new__(serve.Handler)
        handler.path = "/model-tiers?sent=pulled+today%27s+models"
        handler.host_ok = mock.Mock(return_value=True)
        handler.reply = mock.Mock()
        with mock.patch.object(serve, "render_model_tiers") as fake_render:
            handler.do_GET()
        fake_render.assert_called_once_with(sent="pulled today's models", connection={})


class ModelSnapshotTests(unittest.TestCase):
    """`load_model_snapshot`/`snapshot_models`/`match_live_model` -- the
    issue #16 snapshot read and its alias-matching, independent of
    `model_tiers()`'s category/tier rows."""

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.snapshot_path = os.path.join(self.tempdir.name, "model-snapshot.json")

    def write_snapshot(self, data):
        with open(self.snapshot_path, "w", encoding="utf-8") as handle:
            handle.write(data if isinstance(data, str) else json.dumps(data))

    def test_missing_file_is_none(self):
        with (
            mock.patch.object(serve.model_fetch, "SNAPSHOT_FILE", self.snapshot_path),
            mock.patch.object(serve.model_fetch, "read_shared_snapshot", return_value=None),
        ):
            self.assertIsNone(serve.load_model_snapshot())

    def test_corrupt_file_is_none_not_a_crash(self):
        self.write_snapshot("{not json")
        with (
            mock.patch.object(serve.model_fetch, "SNAPSHOT_FILE", self.snapshot_path),
            mock.patch.object(serve.model_fetch, "read_shared_snapshot", return_value=None),
        ):
            self.assertIsNone(serve.load_model_snapshot())

    def test_non_object_json_is_none(self):
        self.write_snapshot([1, 2, 3])
        with (
            mock.patch.object(serve.model_fetch, "SNAPSHOT_FILE", self.snapshot_path),
            mock.patch.object(serve.model_fetch, "read_shared_snapshot", return_value=None),
        ):
            self.assertIsNone(serve.load_model_snapshot())

    def test_valid_file_round_trips(self):
        data = {"fetched_at": "2026-10-07T00:00:00+00:00", "subscriptions": {}}
        self.write_snapshot(data)
        with (
            mock.patch.object(serve.model_fetch, "SNAPSHOT_FILE", self.snapshot_path),
            mock.patch.object(serve.model_fetch, "read_shared_snapshot", return_value=None),
        ):
            self.assertEqual(serve.load_model_snapshot(), data)

    def test_shared_snapshot_takes_precedence_over_local_copy(self):
        local = {"fetched_at": "2026-10-06T00:00:00+00:00", "subscriptions": {}}
        shared = {"fetched_at": "2026-10-07T00:00:00+00:00", "subscriptions": {}}
        self.write_snapshot(local)
        with (
            mock.patch.object(serve.model_fetch, "SNAPSHOT_FILE", self.snapshot_path),
            mock.patch.object(serve.model_fetch, "read_shared_snapshot", return_value=shared),
        ):
            self.assertEqual(serve.load_model_snapshot({"redis_host": "fleet"}), shared)

    def test_snapshot_models_flattens_every_subscription(self):
        snapshot = {
            "subscriptions": {
                "claude": {
                    "live": True,
                    "models": [
                        {"id": "claude-sonnet-4-5-20250929", "display_name": "Claude Sonnet 4.5",
                         "price": {"input": 3, "output": 15}, "promo": None},
                    ],
                },
                "codex": {
                    "live": False,
                    "stale_reason": "no credentials",
                    "models": [{"id": "gpt-5.4", "price": None, "promo": None}],
                },
            }
        }
        rows = serve.snapshot_models(snapshot)
        self.assertEqual(len(rows), 2)
        claude_row = next(r for r in rows if r["id"] == "claude-sonnet-4-5-20250929")
        self.assertTrue(claude_row["live"])
        self.assertEqual(claude_row["display_name"], "Claude Sonnet 4.5")
        codex_row = next(r for r in rows if r["id"] == "gpt-5.4")
        self.assertFalse(codex_row["live"])
        self.assertEqual(codex_row["stale_reason"], "no credentials")

    def test_snapshot_models_skips_malformed_entries(self):
        snapshot = {
            "subscriptions": {
                "claude": {"live": True, "models": [{"id": ""}, "not a dict", {"no": "id field"}]},
                "codex": "not a dict either",
            }
        }
        self.assertEqual(serve.snapshot_models(snapshot), [])

    def test_snapshot_models_handles_no_snapshot(self):
        self.assertEqual(serve.snapshot_models(None), [])

    def test_match_live_model_by_substring(self):
        models = [
            {"id": "claude-sonnet-4-5-20250929", "display_name": "Claude Sonnet 4.5"},
            {"id": "claude-opus-4-5-20251101", "display_name": "Claude Opus 4.5"},
        ]
        match = serve.match_live_model("sonnet", models)
        self.assertEqual(match["id"], "claude-sonnet-4-5-20250929")

    def test_match_live_model_strips_bmo_and_local_prefixes(self):
        # bmo/local aliases never match -- model_fetch only covers claude,
        # opencode-go, and codex, not bmo's or a local model server's catalog.
        models = [{"id": "claude-sonnet-4-5", "display_name": "Claude Sonnet"}]
        self.assertIsNone(serve.match_live_model("bmo:qwen3.8-flash-next", models))
        self.assertIsNone(serve.match_live_model("local:deepseek-v4-flash-0731", models))

    def test_match_live_model_strips_the_subscription_prefix(self):
        # A tier entry like "opencode-go/glm-5.3" names the service the
        # snapshot already records in its own `subscription` field, so the
        # prefix is dropped before matching -- otherwise every prefixed
        # pick on the Models page would read "no live data".
        models = [{"id": "glm-5.3", "display_name": None}]
        self.assertEqual(serve.match_live_model("opencode-go/glm-5.3", models)["id"], "glm-5.3")
        self.assertEqual(serve.match_live_model("openai/gpt-6-luna", models), None)
        self.assertIsNone(serve.match_live_model("opencode-go/", models))

    def test_match_live_model_no_match_is_none(self):
        self.assertIsNone(serve.match_live_model("fable", [{"id": "claude-opus-4-5"}]))

    def test_format_price_variants(self):
        self.assertEqual(serve._format_price(None), "no price data")
        self.assertEqual(serve._format_price({}), "no price data")
        self.assertEqual(
            serve._format_price({"input": 3, "output": 15}), "$3.00 / $15.00 per Mtok"
        )
        self.assertEqual(serve._format_price({"input": None, "output": None}), "no price data")

    def test_format_benchmark_cell(self):
        self.assertEqual(
            serve._format_score([]),
            "<span title='No source-verified score for this model'>—</span>",
        )
        self.assertEqual(
            serve._format_score([{
                "score": 72,
                "benchmark": "Example benchmark",
                "metric": "Index",
                "source": "https://example.test",
            }]),
            "<span title='Example benchmark · Index · https://example.test'>72</span>",
        )

    def test_format_value_uses_normalized_performance_and_blended_price(self):
        self.assertEqual(serve._format_value(None, {"input": 3, "output": 15}), "—")
        self.assertEqual(serve._format_value(100, None), "—")
        self.assertEqual(serve._format_value(100, {"input": None, "output": None}), "—")
        self.assertEqual(serve._format_value(100, {"input": 3, "output": 15}), "11.1 pts/$")


class AllModelsTableTests(unittest.TestCase):
    """`render_model_tiers`'s "All models" table and tier-pick live badges,
    sourced from `model_fetch`'s snapshot (issue #16)."""

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.tiers_path = os.path.join(self.tempdir.name, "model-tiers.json")
        self.snapshot_path = os.path.join(self.tempdir.name, "model-snapshot.json")

    def write_tiers(self, data):
        with open(self.tiers_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(data))

    def write_snapshot(self, data):
        with open(self.snapshot_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(data))

    def render(self, sent=None):
        with (
            mock.patch.object(serve, "MODEL_TIERS_PATH", self.tiers_path),
            mock.patch.object(serve.model_fetch, "SNAPSHOT_FILE", self.snapshot_path),
            mock.patch.object(serve.model_fetch, "read_shared_snapshot", return_value=None),
        ):
            return serve.render_model_tiers(sent=sent).decode()

    def test_render_uses_fleet_snapshot_timestamp_and_models(self):
        self.write_tiers({"coding": {"tiers": {}}})
        shared = {
            "fetched_at": "2026-10-07T00:00:00+00:00",
            "subscriptions": {
                "claude": {
                    "live": True,
                    "models": [{"id": "claude-opus-4-5", "price": None, "promo": None}],
                },
            },
        }
        with (
            mock.patch.object(serve, "MODEL_TIERS_PATH", self.tiers_path),
            mock.patch.object(serve.model_fetch, "read_shared_snapshot", return_value=shared),
            mock.patch.object(serve.benchmark_fetch, "read_snapshot", return_value=None),
        ):
            page = serve.render_model_tiers(connection={"redis_host": "fleet"}).decode()

        self.assertIn("claude-opus-4-5", page)
        self.assertNotIn("Last pulled: never pulled", page)

    def test_no_snapshot_file_shows_placeholder_not_a_crash(self):
        self.write_tiers({"coding": {"tiers": {"tier1": [{"model": "sonnet", "effort": "high"}]}}})
        page = self.render()
        self.assertIn("No model snapshot yet", page)
        self.assertIn("no live data", page)
        self.assertIn("never pulled", page)

    def test_matched_tier_pick_shows_live_price_not_no_live_data(self):
        self.write_tiers({"coding": {"tiers": {"tier1": [{"model": "sonnet", "effort": "high"}]}}})
        self.write_snapshot({
            "fetched_at": "2026-10-07T00:00:00+00:00",
            "subscriptions": {
                "claude": {
                    "live": True,
                    "models": [{"id": "claude-sonnet-4-5", "price": {"input": 3, "output": 15}, "promo": None}],
                },
            },
        })
        page = self.render()
        self.assertIn("$3.00 / $15.00 per Mtok", page)
        self.assertNotIn("no live data", page)

    def test_all_models_table_lists_every_subscription_model(self):
        self.write_tiers({"coding": {"tiers": {}}})
        self.write_snapshot({
            "fetched_at": "2026-10-07T00:00:00+00:00",
            "subscriptions": {
                "claude": {
                    "live": True,
                    "models": [{"id": "claude-opus-4-5", "price": {"input": 5, "output": 25}, "promo": None}],
                },
                "codex": {
                    "live": False,
                    "stale_reason": "no ~/.codex credentials",
                    "models": [{"id": "gpt-5.4", "price": None, "promo": None}],
                },
            },
        })
        page = self.render()
        self.assertIn("claude-opus-4-5", page)
        self.assertIn("$5.00 / $25.00 per Mtok", page)
        self.assertIn("gpt-5.4", page)
        self.assertIn("no price data", page)
        self.assertIn("no ~/.codex credentials", page)
        self.assertIn("Empty cells mean no verified public score is available", page)

    def test_model_matrix_uses_category_sources_and_normalizes_perf(self):
        self.write_tiers({"coding": {"tiers": {}}})
        self.write_snapshot({
            "fetched_at": "2026-10-07T00:00:00+00:00",
            "subscriptions": {
                "claude": {
                    "live": True,
                    "models": [{"id": "claude-sonnet-5-5", "display_name": "Sonnet 5.5", "price": {"input": 3, "output": 15}, "promo": None}],
                },
                "opencode-go": {
                    "live": True,
                    "models": [{"id": "glm-5.3", "display_name": "GLM-5.3", "price": {"input": 1, "output": 4}, "promo": None}],
                },
            },
        })
        benchmark_snapshot = {
            "fetched_at": "2026-10-07T06:00:00+00:00",
            "scores": [
                {"id": "claude-sonnet-5-5", "score": 80, "scale": "Artificial Analysis Intelligence Index (0-100)", "source": "https://artificialanalysis.ai/leaderboards/models"},
                {"id": "glm-5.3", "score": 40, "scale": "Artificial Analysis Intelligence Index (0-100)", "source": "https://artificialanalysis.ai/leaderboards/models", "note": {"text": "Useful & limited.", "source": "https://example.org/glm-note"}},
            ],
        }
        with mock.patch.object(serve.benchmark_fetch, "read_snapshot", return_value=benchmark_snapshot):
            page = self.render()
        self.assertIn("Artificial Analysis Coding Agent Index v1.5", page)
        self.assertIn("EQ-Bench Creative Writing v3", page)
        self.assertIn("Alconost reports scores by provider family", page)
        self.assertIn("68", page)
        self.assertIn("54", page)
        self.assertIn("80", page)
        self.assertIn("100.0%", page)
        self.assertIn("11.1 pts/$", page)
        self.assertIn("Useful &amp; limited.", page)
        self.assertIn("href='https://example.org/glm-note'", page)
        self.assertIn("No private model tests were run", page)

    def test_perf_uses_unavailable_benchmark_leader_as_denominator(self):
        model = {"id": "glm-5.3"}
        _categories, _scores, performance = serve.benchmark_catalog.matrix([model], None)
        self.assertAlmostEqual(performance["glm-5.3"], 54 / 68 * 100)


    def test_missing_benchmark_snapshot_keeps_matrix_blank(self):
        self.write_tiers({"coding": {"tiers": {}}})
        self.write_snapshot({
            "fetched_at": "2026-10-07T00:00:00+00:00",
            "subscriptions": {
                "claude": {
                    "live": True,
                    "models": [{"id": "claude-opus-4-5", "price": {"input": 5, "output": 25}, "promo": None}],
                },
            },
        })
        with mock.patch.object(serve.benchmark_fetch, "read_snapshot", return_value=None):
            page = self.render()
        self.assertIn("No source-verified Intelligence Index scores are cached", page)
        self.assertIn("No source-verified score for this model", page)
        self.assertIn("<th>perf</th>", page)
        self.assertIn("<td>—</td>", page)

    def test_benchmark_status_counts_only_numeric_scores(self):
        self.write_tiers({})
        benchmark_snapshot = {
            "fetched_at": "2026-10-07T06:00:00+00:00",
            "live": True,
            "scores": [
                {"id": "one", "score": None, "reason": "unable to verify"},
                {"id": "two", "score": None, "reason": "unable to verify"},
            ],
        }
        with mock.patch.object(
            serve.benchmark_fetch, "read_snapshot", return_value=benchmark_snapshot
        ):
            page = self.render()
        self.assertIn("(0/2 scored)", page)

    def test_sent_message_is_shown_and_escaped(self):
        self.write_tiers({})
        page = self.render(sent="pull failed: <boom>")
        self.assertIn("pull failed: &lt;boom&gt;", page)

    def test_refresh_form_posts_to_model_tiers_refresh(self):
        self.write_tiers({})
        page = self.render()
        self.assertIn("action='/model-tiers/refresh'", page)
        self.assertIn("Pull models", page)

    def test_refresh_benchmarks_form_posts_to_model_tiers_refresh_benchmarks(self):
        self.write_tiers({})
        page = self.render()
        self.assertIn("action='/model-tiers/refresh-benchmarks'", page)
        self.assertIn("Pull benchmarks", page)


class ModelTiersRefreshRouteTests(unittest.TestCase):
    def test_refresh_route_fetches_saves_and_redirects(self):
        handler = serve.Handler.__new__(serve.Handler)
        handler.path = "/model-tiers/refresh"
        handler.headers = {"Content-Length": "0"}
        handler.rfile = io.BytesIO(b"")
        handler.host_ok = mock.Mock(return_value=True)
        handler.send_response = mock.Mock()
        handler.send_header = mock.Mock()
        handler.end_headers = mock.Mock()
        handler.fleet_connection = {"redis_host": "fleet"}
        fake_snapshot = {"fetched_at": "2026-10-07T00:00:00+00:00", "subscriptions": {}}
        with (
            mock.patch.object(serve.model_fetch, "snapshot", return_value=fake_snapshot) as fake_fetch,
            mock.patch.object(serve.model_fetch, "save_snapshot") as fake_save,
            mock.patch.object(serve.model_fetch, "publish_snapshot", return_value=True) as fake_publish,
        ):
            handler.do_POST()
        fake_fetch.assert_called_once_with()
        fake_save.assert_called_once_with(fake_snapshot)
        fake_publish.assert_called_once_with(fake_snapshot, redis_host="fleet")
        handler.send_response.assert_called_once_with(303)
        location = next(
            call.args[1] for call in handler.send_header.call_args_list if call.args[0] == "Location"
        )
        self.assertIn("pulled%20today%27s%20model%20list", location)
        self.assertTrue(location.startswith("/model-tiers?sent="))

    def test_refresh_failure_redirects_with_message_not_a_500(self):
        handler = serve.Handler.__new__(serve.Handler)
        handler.path = "/model-tiers/refresh"
        handler.headers = {"Content-Length": "0"}
        handler.rfile = io.BytesIO(b"")
        handler.host_ok = mock.Mock(return_value=True)
        handler.send_response = mock.Mock()
        handler.send_header = mock.Mock()
        handler.end_headers = mock.Mock()
        with mock.patch.object(serve.model_fetch, "snapshot", side_effect=RuntimeError("network down")):
            handler.do_POST()
        handler.send_response.assert_called_once_with(303)
        location = next(
            call.args[1] for call in handler.send_header.call_args_list if call.args[0] == "Location"
        )
        self.assertIn("pull%20failed%3A%20network%20down", location)


class ModelTiersRefreshBenchmarksRouteTests(unittest.TestCase):
    """The "Pull benchmarks" button's route -- mirrors
    `ModelTiersRefreshRouteTests` above, but for `benchmark_fetch` instead
    of `model_fetch`."""

    def test_refresh_benchmarks_route_forces_a_fetch_and_redirects(self):
        handler = serve.Handler.__new__(serve.Handler)
        handler.path = "/model-tiers/refresh-benchmarks"
        handler.headers = {"Content-Length": "0"}
        handler.rfile = io.BytesIO(b"")
        handler.host_ok = mock.Mock(return_value=True)
        handler.send_response = mock.Mock()
        handler.send_header = mock.Mock()
        handler.end_headers = mock.Mock()
        fake_snapshot = {"live": True, "scores": [{"id": "claude-opus-4-5", "score": 75.0}]}
        with mock.patch.object(
            serve.benchmark_fetch, "refresh_snapshot", return_value=fake_snapshot
        ) as fake_refresh:
            handler.do_POST()
        # A human clicking the button means "I want a fresh pull right now" --
        # `force=True` -- but it still goes through the single-fetcher lock,
        # so this does not bypass another machine's in-progress fetch.
        fake_refresh.assert_called_once_with(force=True)
        handler.send_response.assert_called_once_with(303)
        location = next(
            call.args[1] for call in handler.send_header.call_args_list if call.args[0] == "Location"
        )
        self.assertTrue(location.startswith("/model-tiers?sent="))
        self.assertIn("1%20models", location)

    def test_refresh_benchmarks_not_live_redirects_with_stale_reason(self):
        handler = serve.Handler.__new__(serve.Handler)
        handler.path = "/model-tiers/refresh-benchmarks"
        handler.headers = {"Content-Length": "0"}
        handler.rfile = io.BytesIO(b"")
        handler.host_ok = mock.Mock(return_value=True)
        handler.send_response = mock.Mock()
        handler.send_header = mock.Mock()
        handler.end_headers = mock.Mock()
        fake_snapshot = {"live": False, "stale_reason": "benchmark-fetch slot is held elsewhere"}
        with mock.patch.object(serve.benchmark_fetch, "refresh_snapshot", return_value=fake_snapshot):
            handler.do_POST()
        location = next(
            call.args[1] for call in handler.send_header.call_args_list if call.args[0] == "Location"
        )
        self.assertIn("benchmark%20pull%20did%20not%20complete", location)
        self.assertIn("held%20elsewhere", location)

    def test_refresh_benchmarks_failure_redirects_with_message_not_a_500(self):
        handler = serve.Handler.__new__(serve.Handler)
        handler.path = "/model-tiers/refresh-benchmarks"
        handler.headers = {"Content-Length": "0"}
        handler.rfile = io.BytesIO(b"")
        handler.host_ok = mock.Mock(return_value=True)
        handler.send_response = mock.Mock()
        handler.send_header = mock.Mock()
        handler.end_headers = mock.Mock()
        with mock.patch.object(
            serve.benchmark_fetch, "refresh_snapshot", side_effect=RuntimeError("redis down")
        ):
            handler.do_POST()
        handler.send_response.assert_called_once_with(303)
        location = next(
            call.args[1] for call in handler.send_header.call_args_list if call.args[0] == "Location"
        )
        self.assertIn("pull%20failed%3A%20redis%20down", location)


class RoadmapCliTests(unittest.TestCase):
    def setUp(self):
        self.model = {
            "nodes": [
                {
                    "number": 42,
                    "priority": "P1",
                    "size": "size-m",
                    "title": "Example issue",
                    "body": "Full issue body",
                    "comments": [{"body": "Full comment text"}],
                },
                {
                    "number": 41,
                    "priority": "P2",
                    "size": "size-s",
                    "title": "Prerequisite",
                    "body": "",
                    "comments": [],
                },
            ],
            "edges": [{"from": 41, "to": 42, "kind": "depends"}],
            "stages": [{"name": "Next batch", "numbers": [42, 41]}],
        }

    def run_cli(self, *args):
        output = io.StringIO()
        errors = io.StringIO()
        with (
            mock.patch.object(sys, "argv", ["serve", *args]),
            mock.patch.object(serve, "code_repos", return_value=[{"repo": "sample", "loopable": True}]),
            mock.patch.object(serve.roadmap, "cached_model", return_value=self.model),
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(errors),
        ):
            status = serve.main()
        return status, output.getvalue(), errors.getvalue()

    def test_default_shows_stage_and_issue_summary(self):
        status, output, _ = self.run_cli("--roadmap", "sample")
        self.assertEqual(status, 0)
        self.assertIn("#42 Next batch P1 size-m 1 comments deps:#41", output)
        self.assertNotIn("Full issue body", output)
        self.assertNotIn("Full comment text", output)

    def test_verbose_shows_body_and_comment(self):
        status, output, _ = self.run_cli("--roadmap", "sample", "--verbose")
        self.assertEqual(status, 0)
        self.assertIn("Full issue body", output)
        self.assertIn("Full comment text", output)

    def test_json_is_valid(self):
        status, output, _ = self.run_cli("--roadmap", "sample", "--json")
        self.assertEqual(status, 0)
        data = json.loads(output)
        self.assertEqual(data["issues"][0]["bucket"], "Next batch")
        self.assertEqual(data["issues"][0]["deps"], [41])

    def test_unknown_repo_exits_two_without_traceback(self):
        with mock.patch.object(sys, "argv", ["serve", "--roadmap", "missing"]), mock.patch.object(
            serve, "code_repos", return_value=[]
        ):
            errors = io.StringIO()
            with contextlib.redirect_stderr(errors):
                status = serve.main()
        self.assertEqual(status, 2)
        self.assertIn("unknown repository", errors.getvalue())
        self.assertNotIn("Traceback", errors.getvalue())


class FleetStateTests(unittest.TestCase):
    """`fleet_state` (issue #15): the dashboard's machines + claims read,
    mocked here so these run without a real Redis. The real-Redis,
    real-HTTP round trip is in `test_dashboard_shows_real_fleet_data_from_redis`
    / `test_dashboard_degrades_when_redis_unreachable` below.
    """

    def test_coordinator_unreachable_returns_empty_with_error(self):
        with mock.patch.object(
            serve.machines, "machines",
            side_effect=machines.CoordinatorUnreachable("machine registry"),
        ):
            result = serve.fleet_state({"redis_host": "127.0.0.1", "redis_port": 1})
        self.assertEqual(result["machines"], [])
        self.assertEqual(result["claims"], {})
        self.assertIn("machine registry", result["fleet_error"])

    def test_machines_and_claims_merge_when_repo_identity_resolves(self):
        machine_rows = [{"name": "jesus", "state": "online"}]
        with (
            mock.patch.object(serve.machines, "machines", return_value=machine_rows),
            mock.patch.object(serve, "enabled_repos", return_value=["widgets"]),
            mock.patch.object(serve.roadmap, "_repo_identity", return_value=("acme", "widgets", None)),
            mock.patch.object(
                serve.claims, "claims_for",
                return_value={"acme/widgets#7": {"session": "loop-widgets#1"}},
            ) as claims_for,
        ):
            result = serve.fleet_state({"redis_host": "127.0.0.1", "redis_port": 1})
        self.assertEqual(result["machines"], machine_rows)
        self.assertEqual(result["claims"], {"acme/widgets#7": {"session": "loop-widgets#1"}})
        self.assertIsNone(result["fleet_error"])
        claims_for.assert_called_once_with(
            ["acme/widgets"],
            redis_host="127.0.0.1", redis_port=1, redis_username=None, redis_password=None,
        )

    def test_repos_with_no_resolvable_owner_skip_the_claims_lookup(self):
        with (
            mock.patch.object(serve.machines, "machines", return_value=[]),
            mock.patch.object(serve, "enabled_repos", return_value=["widgets"]),
            mock.patch.object(
                serve.roadmap, "_repo_identity", return_value=(None, None, "no gh access")
            ),
            mock.patch.object(serve.claims, "claims_for") as claims_for,
        ):
            result = serve.fleet_state({})
        claims_for.assert_not_called()
        self.assertEqual(result["claims"], {})

    def test_claims_unreachable_does_not_blank_the_machines_list(self):
        machine_rows = [{"name": "jesus", "state": "online"}]
        with (
            mock.patch.object(serve.machines, "machines", return_value=machine_rows),
            mock.patch.object(serve, "enabled_repos", return_value=["widgets"]),
            mock.patch.object(serve.roadmap, "_repo_identity", return_value=("acme", "widgets", None)),
            mock.patch.object(
                serve.claims, "claims_for",
                side_effect=claims.CoordinatorUnreachable("claims_for"),
            ),
        ):
            result = serve.fleet_state({})
        self.assertEqual(result["machines"], machine_rows)
        self.assertEqual(result["claims"], {})
        self.assertIsNone(result["fleet_error"])


class FleetDashboardRenderTests(unittest.TestCase):
    """render_dashboard's new "Fleet" section. `base_state` omits
    machines/claims/fleet_error on purpose for the first test -- older
    callers (and the other `DashboardRouteTests`-style tests above) build
    state dicts without those keys, so render_dashboard must still work.
    """

    base_state = {"loops": [], "enabled": [], "timers": [], "timer_active": True, "repos": []}

    def test_missing_fleet_keys_degrade_to_empty_sections(self):
        page = serve.render_dashboard(dict(self.base_state)).decode()
        self.assertIn("No machines registered.", page)
        self.assertIn("No claimed issues.", page)

    def test_fleet_error_shown_instead_of_machine_table(self):
        state = dict(self.base_state, fleet_error="machine registry: connection refused")
        page = serve.render_dashboard(state).decode()
        self.assertIn("Fleet registry unreachable", page)
        self.assertIn("connection refused", page)
        self.assertNotIn("No machines registered.", page)

    def test_machines_and_claims_render_as_table_rows(self):
        state = dict(
            self.base_state,
            fleet_error=None,
            machines=[
                {
                    "name": "jesus", "state": "online", "version": "1.2.3",
                    "version_mismatch": False, "heartbeat": "2026-10-06T00:00:00Z",
                },
                {
                    "name": "ralpha", "state": "offline", "version": "1.0.0",
                    "version_mismatch": True, "heartbeat": "2026-10-05T00:00:00Z",
                },
            ],
            claims={"acme/widgets#7": {"session": "loop-widgets#1", "host": "jesus"}},
        )
        page = serve.render_dashboard(state).decode()
        self.assertIn("jesus", page)
        self.assertIn("ralpha", page)
        self.assertIn("<span class='pill on'>online</span>", page)
        self.assertIn("<span class='pill off'>offline</span>", page)
        self.assertIn("mismatch", page)
        self.assertIn("acme/widgets#7", page)
        self.assertIn("loop-widgets#1", page)


class ServeArgsParsingTests(unittest.TestCase):
    """Closes the gap noted in issue #15: `cli.main()` strictly parses the
    full argv (including `serve`'s) before dispatching to `serve.main()`,
    so `lupin serve --redis-host ...` needs `_serve_args` to accept these
    flags or it fails before serve.py ever sees them.
    """

    def test_top_level_parser_accepts_redis_flags_for_serve(self):
        args = cli._build_parser().parse_args(
            [
                "serve",
                "--redis-host", "10.0.0.1",
                "--redis-port", "6380",
                "--redis-username", "u",
                "--redis-password", "p",
                "--config-path", "/tmp/fleet.json",
            ]
        )
        self.assertEqual(args.redis_host, "10.0.0.1")
        self.assertEqual(args.redis_port, 6380)


def _write_machine_record(redis_port, name, *, state="online"):
    """Write a `machine:<name>` record directly -- same minimal shape as
    `test_machines.py`'s own `_write_raw_record` helper, not shared across
    test files since it's a few lines and each file's fixtures differ.
    """
    record = {
        "version": "0.0.0+dev",
        "heartbeat": machines._now_iso(),
        "state": state,
        "slots": {},
        "providers": [],
        "quota": {},
    }
    client = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    client.set(f"{machines.PREFIX}machine:{name}", json.dumps(record))


def _free_test_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextlib.contextmanager
def _live_dashboard(connection: dict):
    """Run the real `serve.Handler` on a real socket, like `serve.main()`
    does, so a test can hit it with a real HTTP request -- not the
    `Handler.__new__` + mocked-`reply` style the rest of this file uses,
    which never exercises a real socket or a real `do_GET` response body.
    Class attributes are restored afterward since `Handler` is shared
    module state.
    """
    port = _free_test_port()
    orig_connection = serve.Handler.fleet_connection
    orig_hosts = serve.Handler.allowed_hosts
    orig_peek = serve.Handler.peek_lines
    serve.Handler.fleet_connection = connection
    serve.Handler.allowed_hosts = {f"127.0.0.1:{port}"}
    serve.Handler.peek_lines = 5
    serve._FRAGMENT_CACHE.clear()

    class Server(ThreadingHTTPServer):
        daemon_threads = True

    httpd = Server(("127.0.0.1", port), serve.Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield port
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)
        serve.Handler.fleet_connection = orig_connection
        serve.Handler.allowed_hosts = orig_hosts
        serve.Handler.peek_lines = orig_peek


def test_dashboard_shows_real_fleet_data_from_redis(redis_port, flush_redis):
    """The actual regression this issue fixes: a real machine record and a
    real claim in Redis both show up on the rendered page, fetched over a
    real HTTP request against a real running server -- not a mock.
    """
    _write_machine_record(redis_port, "jesus", state="online")
    claims.claim(
        "acme/widgets#7", "loop-widgets#1", redis_host="127.0.0.1", redis_port=redis_port
    )
    connection = {
        "redis_host": "127.0.0.1", "redis_port": redis_port,
        "redis_username": None, "redis_password": None,
    }

    with (
        mock.patch.object(serve, "enabled_repos", return_value=["widgets"]),
        mock.patch.object(serve.roadmap, "_repo_identity", return_value=("acme", "widgets", None)),
        _live_dashboard(connection) as port,
    ):
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=15) as resp:
            status = resp.status
            body = resp.read().decode()

    assert status == 200
    assert "jesus" in body
    assert "<span class='pill on'>online</span>" in body
    assert "acme/widgets#7" in body
    assert "loop-widgets#1" in body


def test_roadmap_route_reads_ledger_from_dashboard_connection(
    redis_port, flush_redis
):
    connection = {
        "redis_host": "127.0.0.1", "redis_port": redis_port,
        "redis_username": None, "redis_password": None,
    }
    ledger.append_event(
        "acme/widgets",
        {
            "event": "handoff", "issue": 7, "status": "ready",
            "summary": "Shared handoff.", "highlights": ["Visible from dashboard."],
        },
        **connection,
    )
    issue = {"number": 7, "title": "Issue", "body": "", "labels": []}

    with (
        mock.patch.object(
            serve, "code_repos", return_value=[{"repo": "widgets", "loopable": True}]
        ),
        mock.patch.object(
            serve.roadmap, "cached_github", return_value=([issue], {}, [])
        ),
        mock.patch.object(
            serve.roadmap, "_repo_identity", return_value=("acme", "widgets", None)
        ),
        _live_dashboard(connection) as port,
    ):
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/roadmap/board?repo=widgets&issue=7&issue_repo=widgets",
            timeout=15,
        ) as response:
            status = response.status
            body = response.read().decode()

    assert status == 200
    assert "Shared handoff." in body
    assert "Visible from dashboard." in body


def test_roadmap_defaults_to_list_for_large_backlogs(
    redis_port, flush_redis
):
    connection = {
        "redis_host": "127.0.0.1", "redis_port": redis_port,
        "redis_username": None, "redis_password": None,
    }
    issues = [
        {"number": number, "title": f"Issue {number}", "body": "", "labels": []}
        for number in range(1, roadmap.BOARD_MAX_ISSUES + 2)
    ]
    model = roadmap.build_model(issues, {}, [], repo="widgets")
    model["closedNodes"] = []

    with (
        mock.patch.object(
            serve, "code_repos", return_value=[{"repo": "widgets", "loopable": True}]
        ),
        mock.patch.object(roadmap, "cached_combined_model", return_value=model),
        _live_dashboard(connection) as port,
    ):
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/roadmap", timeout=15) as response:
            shell = response.read().decode()
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/roadmap/board", timeout=15) as response:
            default_body = response.read().decode()
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/roadmap/board?view=board", timeout=15
        ) as response:
            board_body = response.read().decode()

    assert "roadmap-fragment" in shell
    assert "data-src='/roadmap/board'" in shell
    assert "Issue 1" not in shell
    assert "Any priority" in default_body
    assert "Issue 1" in default_body
    assert "All tags" not in default_body
    assert "All tags" in board_body


def test_roadmap_route_reads_github_cache_from_dashboard_connection(
    redis_port, flush_redis, auth_redis_port, monkeypatch, tmp_path
):
    connection = {
        "redis_host": "127.0.0.1", "redis_port": redis_port,
        "redis_username": None, "redis_password": None,
    }
    resolve_connection = machines.resolve_connection

    def resolve_default_connection(**kwargs):
        if not any(kwargs.values()):
            return {
                "redis_host": "127.0.0.1", "redis_port": auth_redis_port,
                "redis_username": None, "redis_password": "test-pass",
            }
        return resolve_connection(**kwargs)

    monkeypatch.setattr(machines, "DEFAULT_CONFIG_PATH", tmp_path / "fleet.json")
    monkeypatch.setattr(machines, "resolve_connection", resolve_default_connection)
    monkeypatch.setattr(roadmap, "_GITHUB_CACHE", {})
    monkeypatch.setattr(roadmap, "_persist_cache", lambda: None)

    issue = {"number": 7, "title": "Cached dashboard issue", "body": "", "labels": []}
    redis_lib.Redis(
        host="127.0.0.1", port=redis_port, decode_responses=True
    ).set(
        f"{roadmap.gh_cache.PREFIX}gh-cache:acme/widgets:issues:open",
        json.dumps({"data": [issue]}),
        ex=roadmap.gh_cache.CACHE_TTL,
    )

    with (
        mock.patch.object(
            serve, "code_repos", return_value=[{"repo": "widgets", "loopable": True}]
        ),
        mock.patch.object(
            roadmap, "_repo_identity", return_value=("acme", "widgets", None)
        ),
        _live_dashboard(connection) as port,
    ):
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/roadmap/board", timeout=15
        ) as response:
            status = response.status
            body = response.read().decode()

    assert "Cached dashboard issue" in body


def test_roadmap_fragment_is_served_from_cache_without_a_rebuild(
    redis_port, flush_redis
):
    """The rendered fragment is cached: a reload inside the window must not
    build the models again, because a build can wait on Redis and `gh`."""
    connection = {
        "redis_host": "127.0.0.1", "redis_port": redis_port,
        "redis_username": None, "redis_password": None,
    }
    model = roadmap.build_model(
        [{"number": 3, "title": "Cached card", "body": "", "labels": []}],
        {}, [], repo="widgets",
    )
    model["closedNodes"] = []
    builds = []

    def build(repo, path, *, connection=None):
        builds.append(repo)
        return model

    with (
        mock.patch.object(
            serve, "code_repos", return_value=[{"repo": "widgets", "loopable": True}]
        ),
        mock.patch.object(roadmap, "cached_combined_model", side_effect=build),
        _live_dashboard(connection) as port,
    ):
        for _ in range(2):
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/roadmap/board", timeout=15
            ) as response:
                body = response.read().decode()

    assert builds == ["widgets"]
    assert "Cached card" in body


def test_roadmap_full_query_renders_the_whole_page_without_javascript(
    redis_port, flush_redis
):
    """`full=1` is the no-JavaScript path: one response, board included."""
    connection = {
        "redis_host": "127.0.0.1", "redis_port": redis_port,
        "redis_username": None, "redis_password": None,
    }
    model = roadmap.build_model(
        [{"number": 4, "title": "Whole page card", "body": "", "labels": []}],
        {}, [], repo="widgets",
    )
    model["closedNodes"] = []

    with (
        mock.patch.object(
            serve, "code_repos", return_value=[{"repo": "widgets", "loopable": True}]
        ),
        mock.patch.object(roadmap, "cached_combined_model", return_value=model),
        _live_dashboard(connection) as port,
    ):
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/roadmap?full=1", timeout=15
        ) as response:
            body = response.read().decode()

    assert body.startswith("<!doctype html>")
    assert "Whole page card" in body
    assert "roadmap-fragment" not in body


def test_roadmap_shows_why_a_repo_has_no_issues(redis_port, flush_redis):
    """A repo whose GitHub read failed renders no cards, which used to be
    indistinguishable from a repo with no issues. The board now names the
    repo and the reason, so a blank lane is not read as "nothing to do".
    """
    connection = {
        "redis_host": "127.0.0.1", "redis_port": redis_port,
        "redis_username": None, "redis_password": None,
    }
    models = {
        "widgets": roadmap.build_model(
            [{"number": 4, "title": "Whole page card", "body": "", "labels": []}],
            {}, [], repo="widgets",
        ),
        "broken": roadmap.build_model(
            [], {}, [],
            warnings=["GitHub issue data is unavailable: no git remotes found"],
            repo="broken",
        ),
    }
    for model in models.values():
        model["closedNodes"] = []

    with (
        mock.patch.object(
            serve, "code_repos",
            return_value=[
                {"repo": "widgets", "loopable": True},
                {"repo": "broken", "loopable": True},
            ],
        ),
        mock.patch.object(
            roadmap, "cached_combined_model", side_effect=lambda repo, path, **kw: models[repo]
        ),
        _live_dashboard(connection) as port,
    ):
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/roadmap/board", timeout=15
        ) as response:
            body = response.read().decode()

    assert "Whole page card" in body
    assert "class='repo-warnings'" in body
    assert "broken" in body
    assert "no git remotes found" in body


def test_roadmap_list_view_repo_filter_hides_other_repos_warnings(
    redis_port, flush_redis
):
    """`?repo=` narrows the list to one repo; its warnings must narrow too,
    or a filtered view still reports failures for repos it is not showing.
    """
    connection = {
        "redis_host": "127.0.0.1", "redis_port": redis_port,
        "redis_username": None, "redis_password": None,
    }
    models = {
        "widgets": roadmap.build_model(
            [{"number": 4, "title": "Widget card", "body": "", "labels": []}],
            {}, [], repo="widgets",
        ),
        "broken": roadmap.build_model(
            [], {}, [],
            warnings=["GitHub issue data is unavailable: no git remotes found"],
            repo="broken",
        ),
    }

    with (
        mock.patch.object(
            serve, "code_repos",
            return_value=[
                {"repo": "widgets", "loopable": True},
                {"repo": "broken", "loopable": True},
            ],
        ),
        mock.patch.object(
            roadmap, "cached_model", side_effect=lambda repo, path, **kw: models[repo]
        ),
        _live_dashboard(connection) as port,
    ):
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/roadmap/board?view=list&repo=widgets", timeout=15
        ) as response:
            body = response.read().decode()

    assert "Widget card" in body
    assert "class='repo-warnings'" not in body
    assert "no git remotes found" not in body




def test_dashboard_degrades_when_redis_unreachable(closed_port):
    """No Redis listening on the other end -- the page must still render
    200 with a plain "unreachable" message, not 500.
    """
    connection = {
        "redis_host": "127.0.0.1", "redis_port": closed_port,
        "redis_username": None, "redis_password": None,
    }

    with (
        mock.patch.object(serve, "timers", return_value=[]),
        mock.patch.object(serve, "timer_active", return_value=False),
        mock.patch.object(serve, "enabled_repos", return_value=[]),
        mock.patch.object(serve, "code_repos", return_value=[]),
        mock.patch.object(serve.loop_runtime, "local_loops", return_value=[]),
        _live_dashboard(connection) as port,
    ):
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=15) as resp:
            status = resp.status
            body = resp.read().decode()

    assert status == 200
    assert "Fleet registry unreachable" in body


def test_do_post_quest_start_creates_quest_writes_redis_and_redirects(
    redis_port, flush_redis, monkeypatch
):
    locate_table = {
        11: ("repo-a", "acme/repo-a", _issue_json(11)),
        12: ("repo-a", "acme/repo-a", _issue_json(12)),
    }
    monkeypatch.setattr(quest, "_locate_issue", _fake_locate(locate_table))
    monkeypatch.setattr(
        quest.roadmap, "cached_dependency_dag", lambda repos, code_dir: {"repos": {}}
    )
    monkeypatch.setattr(quest.place_mod, "place", lambda task, connection: {"pick": "jesus"})
    monkeypatch.setattr(serve, "enabled_repos", lambda: ["repo-a"])

    handler = _quest_handler(redis_port)
    _post_body(handler, "/quest/start", {"repo": "repo-a", "issue": ["11", "12"]})

    handler.do_POST()

    handler.redirect.assert_called_once()
    (location,), _kwargs = handler.redirect.call_args
    assert location.startswith("/roadmap?repo=repo-a&quest=q")
    quest_id = location.rpartition("quest=")[2]

    record = quest.read_quest(quest_id, _kw(redis_port))
    assert record["issues"] == [11, 12]
    assert record["machine"] == "jesus"
    held = claims.claims_for(["acme/repo-a"], **_kw(redis_port))
    assert held["acme/repo-a#11"]["session"] == f"quest:{quest_id}"
    assert held["acme/repo-a#12"]["session"] == f"quest:{quest_id}"


def test_do_post_quest_start_rejects_no_issues_selected(redis_port, flush_redis):
    handler = _quest_handler(redis_port)
    _post_body(handler, "/quest/start", {"repo": "repo-a"})

    handler.do_POST()

    handler.redirect.assert_not_called()
    handler.reply.assert_called_once()
    body, status = handler.reply.call_args[0]
    assert status == 400
    assert b"select at least one issue" in body


def test_do_post_quest_stop_releases_claims_deletes_record_and_redirects(
    redis_port, flush_redis, monkeypatch
):
    locate_table = {21: ("repo-a", "acme/repo-a", _issue_json(21))}
    monkeypatch.setattr(quest, "_locate_issue", _fake_locate(locate_table))
    monkeypatch.setattr(
        quest.roadmap, "cached_dependency_dag", lambda repos, code_dir: {"repos": {}}
    )
    monkeypatch.setattr(quest.place_mod, "place", lambda task, connection: {"pick": "jesus"})
    started = quest.start([21], ["repo-a"], connection=_kw(redis_port))

    handler = _quest_handler(redis_port)
    _post_body(handler, "/quest/stop", {"id": started["id"], "repo": "repo-a"})

    handler.do_POST()

    handler.redirect.assert_called_once_with("/roadmap?repo=repo-a")
    assert quest.read_quest(started["id"], _kw(redis_port)) is None
    held = claims.claims_for(["acme/repo-a"], **_kw(redis_port))
    assert "acme/repo-a#21" not in held


def test_do_post_quest_stop_unknown_id_errors_without_redirect(redis_port, flush_redis):
    handler = _quest_handler(redis_port)
    _post_body(handler, "/quest/stop", {"id": "q999", "repo": "repo-a"})

    handler.do_POST()

    handler.redirect.assert_not_called()
    handler.reply.assert_called_once()


def _raw_post(port: int, path: str, headers: dict, body: bytes) -> bytes:
    """Send a hand-built POST request over a real socket -- `urllib` would
    always compute a correct, numeric `Content-Length` and a valid utf-8
    body itself, so it can't reproduce the malformed requests below.
    Returns everything read back before the server closes the connection.
    """
    header_lines = "".join(f"{key}: {value}\r\n" for key, value in headers.items())
    request = (
        f"POST {path} HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{port}\r\n"
        f"{header_lines}"
        "Connection: close\r\n\r\n"
    ).encode("ascii") + body
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(10)
        sock.connect(("127.0.0.1", port))
        sock.sendall(request)
        chunks = []
        try:
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                chunks.append(chunk)
        except socket.timeout:
            pass
    return b"".join(chunks)


def test_do_post_non_numeric_content_length_returns_clean_400(redis_port, flush_redis):
    """A malformed `Content-Length` must not crash do_POST -- it used to
    raise an unhandled ValueError inside `int(...)`, dropping the
    connection with no HTTP response at all instead of a 400.
    """
    connection = _kw(redis_port)
    with _live_dashboard(connection) as port:
        response = _raw_post(
            port, "/quest/stop", {"Content-Length": "bogus"}, b""
        )

    assert response.startswith(b"HTTP/1.0 400") or response.startswith(b"HTTP/1.1 400")


def test_do_post_non_utf8_body_returns_clean_400(redis_port, flush_redis):
    """A body that isn't valid utf-8 must not crash do_POST -- it used to
    raise an unhandled UnicodeDecodeError inside `raw.decode("utf-8")`,
    dropping the connection with no HTTP response at all instead of a 400.
    """
    body = b"\xff\xfe\xfd"
    connection = _kw(redis_port)
    with _live_dashboard(connection) as port:
        response = _raw_post(
            port, "/quest/stop", {"Content-Length": str(len(body))}, body
        )

    assert response.startswith(b"HTTP/1.0 400") or response.startswith(b"HTTP/1.1 400")


def test_quest_state_partitions_pending_and_done(redis_port, flush_redis, monkeypatch):
    locate_table = {
        31: ("repo-a", "acme/repo-a", _issue_json(31)),
        32: ("repo-a", "acme/repo-a", _issue_json(32)),
    }
    monkeypatch.setattr(quest, "_locate_issue", _fake_locate(locate_table))
    monkeypatch.setattr(
        quest.roadmap, "cached_dependency_dag", lambda repos, code_dir: {"repos": {}}
    )
    monkeypatch.setattr(quest.place_mod, "place", lambda task, connection: {"pick": "jesus"})
    started = quest.start([31, 32], ["repo-a"], connection=_kw(redis_port))
    # Simulate #31 merging: something else (e.g. `reconcile`) releases its
    # claim the way a closed/merged issue's own claim gets released.
    claims.release_claim("acme/repo-a#31", f"quest:{started['id']}", **_kw(redis_port))

    handler = serve.Handler.__new__(serve.Handler)
    handler.fleet_connection = _kw(redis_port)

    state = handler.quest_state(started["id"])

    assert state["id"] == started["id"]
    assert state["machine"] == "jesus"
    assert state["done"] == [31]
    assert state["pending"] == [32]
    assert state["total"] == 2


def test_quest_state_returns_none_for_blank_or_missing_id(redis_port, flush_redis):
    handler = serve.Handler.__new__(serve.Handler)
    handler.fleet_connection = _kw(redis_port)
    assert handler.quest_state("") is None
    assert handler.quest_state("q999") is None


def test_render_page_has_quest_checkbox_and_start_button():
    issues = [{"number": 5, "title": "Do thing", "body": "", "labels": []}]
    model = roadmap.build_model(issues, {}, [], repo="nix")
    render = lambda title, body, css, js: body

    page = roadmap.render_page("nix", ["nix"], model, render)

    assert "<form id=quest-start" in page
    assert "action='/quest/start'" in page
    assert "Start quest" in page
    assert "form=quest-start name=issue value='5'" in page


def test_render_page_shows_progress_and_stop_button_when_quest_running():
    issues = [{"number": 5, "title": "Do thing", "body": "", "labels": []}]
    model = roadmap.build_model(issues, {}, [], repo="nix")
    render = lambda title, body, css, js: body
    quest_state = {
        "id": "q7", "machine": "jesus", "state": "running",
        "pending": [5], "done": [], "total": 1,
    }

    page = roadmap.render_page("nix", ["nix"], model, render, quest_state)

    assert "quest q7" in page
    assert "0/1 done" in page
    assert "Stop quest" in page
    assert "name=id value='q7'" in page


def _post_handler(path, form_body, connection):
    handler = serve.Handler.__new__(serve.Handler)
    handler.path = path
    handler.headers = {"Content-Length": str(len(form_body))}
    handler.rfile = io.BytesIO(form_body)
    handler.fleet_connection = connection
    handler.cmd_signing_key = None
    handler.host_ok = mock.Mock(return_value=True)
    handler.reply = mock.Mock()
    handler.send_response = mock.Mock()
    handler.send_header = mock.Mock()
    handler.end_headers = mock.Mock()
    return handler


def _get_handler(path, connection):
    handler = serve.Handler.__new__(serve.Handler)
    handler.path = path
    handler.fleet_connection = connection
    handler.cmd_signing_key = None
    handler.host_ok = mock.Mock(return_value=True)
    handler.reply = mock.Mock()
    return handler


def test_post_route_that_raises_an_acl_denial_is_502_not_a_traceback(monkeypatch):
    handler = _post_handler("/quest/stop", b"id=quest-1", {"redis_host": "127.0.0.1", "redis_port": 1})

    def denied(self, form):
        raise redis_lib.exceptions.NoPermissionError("User x has no permissions to run the 'eval' command.")

    monkeypatch.setattr(serve.Handler, "do_quest_stop", denied)
    handler.do_POST()

    assert handler.reply.call_args.args[1] == 502
    body = html.unescape(handler.reply.call_args.args[0].decode())
    assert "redis denied the command" in body
    assert "For POST /quest/stop." in body


class TestMachinesPageIntegration:
    """Real `redis-server` fixtures (issue #20), same rule as
    test_machines.py/test_slots_redis.py -- not mocked, so a rendering bug
    or a `set_max` that doesn't actually change what `status()` reports
    would be caught here, not just a call-was-made assertion.
    """

    def test_page_renders_mockup_machine_cards_and_shared_slots_once(
        self, redis_port, flush_redis, tmp_path, monkeypatch
    ):
        # join() before acquire() on purpose -- join's own written `slots`
        # snapshot would be empty at this point. The page still has to show
        # the lease below, so it must read slots_redis.status() live.
        kw = _kw(redis_port)
        names = ["jesus", "pihome", "ralpha"]
        for name in names:
            monkeypatch.setattr(machines, "hostname", lambda name=name: name)
            loops = (
                [{"repo": "lupin", "platform": "omp", "state": "running"}]
                if name == "pihome"
                else []
            )
            machines.join(
                f"127.0.0.1:{redis_port}",
                config_path=tmp_path / "fleet.json",
                loops=loops,
            )
        monkeypatch.setattr(machines, "hostname", lambda: "pihome")
        machines.drain(kw)
        monkeypatch.setattr(
            serve,
            "timers",
            lambda: [{"unit": "delegation-loop.timer", "next": time.time() + 60}],
        )
        monkeypatch.setattr(serve, "timer_active", lambda: True)
        slots_redis.acquire("bmo", "worker-a", max_holders=1, **kw)

        handler = _get_handler("/machines", kw)
        handler.do_GET()

        body = handler.reply.call_args.args[0].decode()
        for name in names:
            assert name in body
        assert "2 of 3 machines online" in body
        assert "This machine" in body
        assert "Timer running" in body
        assert "Fleet-wide capacity" in body
        assert body.count("<b>bmo</b><span class=mono>1 / 1</span>") == 1
        assert body.count("class=machine-card") == 3
        assert "lupin · omp" in body
        assert "Draining. Running loops can finish" in body

    def test_unreachable_coordinator_is_502(self, closed_port):
        handler = _get_handler("/machines", {"redis_host": "127.0.0.1", "redis_port": closed_port})
        handler.do_GET()
        assert handler.reply.call_args.args[1] == 502

    def test_refused_login_on_slot_max_is_502_and_names_the_password(self, auth_redis_port, no_client_retry):
        handler = _post_handler("/machines/slot-max", b"slot=bmo&max=5", _kw(auth_redis_port))
        handler.do_POST()

        assert handler.reply.call_args.args[1] == 502
        body = html.unescape(handler.reply.call_args.args[0].decode())
        assert "Check the Redis password" in body
        assert "or the systemd credential redis-password" in body
        assert "For slot 'bmo'." in body
        assert "unreachable" not in body

    def test_slot_max_control_changes_what_status_reports(self, redis_port, flush_redis):
        kw = _kw(redis_port)
        slots_redis.acquire("bmo", "a", max_holders=2, **kw)
        assert slots_redis.status(**kw)["bmo"]["max"] == 2

        handler = _post_handler("/machines/slot-max", b"slot=bmo&max=5", kw)
        handler.do_POST()

        handler.send_response.assert_called_once_with(303)
        assert slots_redis.status(**kw)["bmo"]["max"] == 5

    def test_lowering_max_below_holders_keeps_them_but_blocks_new_acquires(self, redis_port, flush_redis):
        kw = _kw(redis_port)
        slots_redis.acquire("bmo", "a", max_holders=2, **kw)
        slots_redis.acquire("bmo", "b", **kw)
        assert slots_redis.status(**kw)["bmo"] == {"holders": 2, "max": 2}

        handler = _post_handler("/machines/slot-max", b"slot=bmo&max=1", kw)
        handler.do_POST()

        # Existing holders are not evicted by a lowered max.
        assert slots_redis.status(**kw)["bmo"] == {"holders": 2, "max": 1}
        # A new acquire is blocked until holders drop back under the max.
        with pytest.raises(slots.SlotFull):
            slots_redis.acquire("bmo", "c", **kw)


def _loops_handler(connection=None):
    """A `Handler` for the `/loops` routes, mocked reply/redirect like
    `_quest_handler` above -- these tests check routing and validation, not
    real I/O. `cmd_signing_key` defaults to `None` (local-machine close/
    restart needs no key; a test that needs one sets it directly)."""
    handler = serve.Handler.__new__(serve.Handler)
    handler.fleet_connection = connection if connection is not None else {}
    handler.cmd_signing_key = None
    handler.host_ok = mock.Mock(return_value=True)
    handler.reply = mock.Mock()
    handler.redirect = mock.Mock()
    return handler


def _loop_entry(repo, status, machine, session=None, **fields):
    return {
        "repo": repo,
        "enabled": True,
        "status": status,
        "agent_status": status,
        "machine": machine,
        "backend": "herdr",
        "session": session,
        **fields,
    }


class GatherLoopsTests(unittest.TestCase):
    def test_local_loop_state_includes_herdr_identifiers(self):
        state = {
            "repo": "a", "backend": "herdr", "state": "working",
            "session": "lupin-a", "workspace_id": "w-1", "pane_id": "p-2",
            "platform": "omp",
        }
        with (
            mock.patch.object(serve, "code_repos", return_value=[{"repo": "a", "loopable": True}]),
            mock.patch.object(serve.loop_runtime, "local_loops", return_value=[state]),
            mock.patch.object(serve, "enabled_repos", return_value=["a"]),
            mock.patch.object(serve, "fleet_state", return_value={"claims": {}, "machines": [], "fleet_error": None}),
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
        ):
            entry = serve.gather_loops({})["entries"][0]
        self.assertEqual(entry["agent_status"], "working")
        self.assertEqual(entry["backend"], "herdr")
        self.assertEqual(entry["session"], "lupin-a")
        self.assertEqual(entry["workspace_id"], "w-1")
        self.assertEqual(entry["pane_id"], "p-2")
        self.assertEqual(entry["platform"], "omp")

    def test_remote_heartbeat_state_wins_over_issue_claims(self):
        with (
            mock.patch.object(serve, "code_repos", return_value=[{"repo": "a", "loopable": True}]),
            mock.patch.object(serve.loop_runtime, "local_loops", return_value=[]),
            mock.patch.object(serve, "enabled_repos", return_value=["a"]),
            mock.patch.object(
                serve, "fleet_state",
                return_value={
                    "claims": {"acme/a#1": {"host": "stale"}},
                    "machines": [{"name": "jesus", "loops": [{
                        "repo": "a", "state": "needs_attention", "backend": "herdr",
                        "session": "lupin-a", "workspace_id": "w", "pane_id": "p",
                    }]}],
                    "fleet_error": None,
                },
            ),
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
        ):
            entry = serve.gather_loops({})["entries"][0]
        self.assertEqual(entry["machine"], "jesus")
        self.assertEqual(entry["agent_status"], "needs_attention")
        self.assertEqual(entry["session"], "lupin-a")


    def test_offline_machine_loop_state_is_unknown(self):
        with (
            mock.patch.object(serve, "code_repos", return_value=[{"repo": "a", "loopable": True}]),
            mock.patch.object(serve.loop_runtime, "local_loops", return_value=[]),
            mock.patch.object(serve, "enabled_repos", return_value=["a"]),
            mock.patch.object(
                serve,
                "fleet_state",
                return_value={
                    "claims": {},
                    "machines": [{
                        "name": "jesus",
                        "state": "offline",
                        "loops": [{
                            "repo": "a",
                            "state": "working",
                            "backend": "herdr",
                            "session": "lupin-a",
                        }],
                    }],
                    "fleet_error": None,
                },
            ),
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
        ):
            entry = serve.gather_loops({})["entries"][0]

        self.assertEqual(entry["machine"], "jesus")
        self.assertEqual(entry["status"], "unknown")
        self.assertEqual(entry["agent_status"], "unknown")

    def test_claims_do_not_infer_a_loop_without_herdr_state(self):
        with (
            mock.patch.object(serve, "code_repos", return_value=[{"repo": "a", "loopable": True}]),
            mock.patch.object(serve.loop_runtime, "local_loops", return_value=[]),
            mock.patch.object(serve, "enabled_repos", return_value=["a"]),
            mock.patch.object(serve, "fleet_state", return_value={
                "claims": {"acme/a#1": {"host": "jesus"}}, "machines": [], "fleet_error": None,
            }),
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
        ):
            entry = serve.gather_loops({})["entries"][0]
        self.assertEqual(entry["agent_status"], "stopped")
        self.assertEqual(entry["machine"], "pihome")

    def test_gather_loops_includes_repos_without_delegation_docs(self):
        with (
            mock.patch.object(
                serve, "code_repos",
                return_value=[{"repo": "x", "state": "disabled", "loopable": True, "has_doc": False}],
            ),
            mock.patch.object(serve.loop_runtime, "local_loops", return_value=[]),
            mock.patch.object(serve, "enabled_repos", return_value=[]),
            mock.patch.object(serve, "fleet_state", return_value={"claims": {}, "machines": [], "fleet_error": None}),
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
        ):
            data = serve.gather_loops({})
        self.assertEqual([row["repo"] for row in data["entries"]], ["x"])
        self.assertEqual(data["entries"][0]["status"], "stopped")

    def test_gather_loops_lists_remote_loops_when_server_has_no_code_dir(self):
        machine_records = [
            {"name": "jesus", "state": "online",
             "loops": [{"repo": "roundsmith", "state": "working", "backend": "herdr"}]},
            {"name": "ralpha", "state": "online",
             "loops": [{"repo": "ghostbook.nix", "state": "idle", "backend": "herdr"}]},
        ]
        with (
            mock.patch.object(serve, "code_repos", return_value=[]),
            mock.patch.object(serve.loop_runtime, "local_loops", return_value=[]),
            mock.patch.object(serve, "enabled_repos", return_value=[]),
            mock.patch.object(serve, "fleet_state", return_value={
                "claims": {}, "machines": machine_records, "fleet_error": None,
            }),
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
        ):
            entries = serve.gather_loops({})["entries"]
        self.assertEqual(
            [(e["repo"], e["machine"], e["status"]) for e in entries],
            [("ghostbook.nix", "ralpha", "idle"), ("roundsmith", "jesus", "working")],
        )

class LoopsRouteUnitTests(unittest.TestCase):
    """Routing logic for controls that use Lupin and Herdr."""

    def test_loops_route_renders(self):
        handler = _loops_handler()
        handler.path = "/loops"
        with mock.patch.object(
            serve, "gather_loops",
            return_value={"entries": [], "machines": [], "fleet_error": None, "local_host": "h"},
        ):
            handler.do_GET()
        handler.reply.assert_called_once()

    def test_loops_route_rejects_bad_lines(self):
        handler = _loops_handler()
        handler.path = "/loops?lines=abc"
        handler.do_GET()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_loops_fullscreen_unknown_repo_is_404(self):
        handler = _loops_handler()
        handler.path = "/loops?repo=missing&fullscreen=1"
        with mock.patch.object(
            serve, "gather_loops",
            return_value={"entries": [], "machines": [], "fleet_error": None, "local_host": "h"},
        ):
            handler.do_GET()
        self.assertEqual(handler.reply.call_args.args[1], 404)

    def test_loops_route_uses_herdr_peek_and_shows_state_ids(self):
        entries = [_loop_entry("a", "running", "h", session="herdr-a", workspace_id="w", pane_id="p")]
        handler = _loops_handler()
        handler.path = "/loops?repo=a"
        with (
            mock.patch.object(
                serve, "gather_loops",
                return_value={"entries": entries, "machines": [], "fleet_error": None, "local_host": "h"},
            ),
            mock.patch.object(serve, "loop_tail", return_value="hello there") as tail,
            mock.patch.object(serve.loops, "ssh_target_for", return_value=None),
        ):
            handler.do_GET()
        tail.assert_called_once_with("a", "h", 60, {}, None)
        body = handler.reply.call_args.args[0].decode()
        self.assertIn("hello there", body)
        self.assertIn("workspace: w", body)
        self.assertIn("pane: p", body)
        self.assertIn("Attach: <code>herdr --session herdr-a</code>", body)
        self.assertIn("Herdr state: running", body)

    def test_remote_attach_command_uses_the_configured_ssh_target(self):
        data = {
            "entries": [_loop_entry("a", "working", "jesus", session="session-a")],
            "local_host": "pihome",
            "machines": [],
            "fleet_error": None,
        }
        with mock.patch.object(serve.loops, "ssh_target_for", return_value="ghosta@jesus.local"):
            page = serve.render_loops(data, group="repo", selected_repo="a", selected_tail=None, lines=60).decode()
        self.assertIn("herdr --remote ghosta@jesus.local --session session-a", page)
        # Also offered: a command that needs only ssh, so Herdr versions cannot differ.
        self.assertIn("ssh -t ghosta@jesus.local herdr --session session-a", page)

    def test_stopped_loop_form_offers_online_machine_targets(self):
        data = {
            "entries": [_loop_entry("a", "stopped", "pihome")],
            "local_host": "pihome",
            "machines": [
                {"name": "jesus", "state": "online", "slots": {}},
                {"name": "ralpha", "state": "draining", "slots": {}},
            ],
            "fleet_error": None,
        }

        body = serve.render_loops(
            data, group="repo", selected_repo="a", selected_tail=None, lines=60
        ).decode()

        self.assertIn("<select name=machine>", body)
        self.assertIn("<option value='jesus'>jesus</option>", body)
        self.assertNotIn("value='ralpha'", body)

    def test_unknown_remote_loop_has_no_lifecycle_controls(self):
        data = {
            "entries": [_loop_entry("a", "unknown", "jesus", session="lupin-a")],
            "local_host": "pihome",
            "machines": [{"name": "jesus", "state": "offline", "slots": {}}],
            "fleet_error": None,
        }

        body = serve.render_loops(
            data, group="repo", selected_repo="a", selected_tail=None, lines=60
        ).decode()

        self.assertIn("State cannot be verified; actions are disabled.", body)
        self.assertNotIn("action=/loops/start", body)
        self.assertNotIn("action=/loops/close", body)
        self.assertNotIn("action=/loops/state", body)


    def test_close_route_rejects_bad_repo_name(self):
        handler = _loops_handler()
        _post_body(handler, "/loops/close", {"repo": "../etc", "machine": "h"})
        handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_loop_tail_returns_the_output_of_a_remote_peek(self):
        queued = {"mode": "queued", "id": "c1", "result": {"state": "ok", "output": "pane text\n"}}
        with (
            mock.patch.object(serve.machines, "hostname", return_value="h"),
            mock.patch.object(serve.loops, "dispatch_loop_action", return_value=queued) as dispatch,
        ):
            out = serve.loop_tail("a", "jesus", 60, {}, "key")
        self.assertEqual(out, "pane text\n")
        self.assertGreater(dispatch.call_args.kwargs["wait_s"], 0)

    def test_loop_tail_says_so_when_a_remote_peek_has_not_finished(self):
        queued = {"mode": "queued", "id": "c1", "result": None}
        with (
            mock.patch.object(serve.machines, "hostname", return_value="h"),
            mock.patch.object(serve.loops, "dispatch_loop_action", return_value=queued),
        ):
            out = serve.loop_tail("a", "jesus", 60, {}, "key")
        self.assertIn("not finished", out)

    def test_send_route_rejects_bad_repo_name(self):
        handler = _loops_handler()
        _post_body(handler, "/loops/send", {"repo": "../etc", "machine": "h", "text": "hi"})
        handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_send_route_rejects_empty_text(self):
        handler = _loops_handler()
        _post_body(handler, "/loops/send", {"repo": "a", "machine": "h", "text": "   "})
        handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_send_route_runs_the_local_lupin_action(self):
        handler = _loops_handler()
        _post_body(handler, "/loops/send", {"repo": "a", "machine": "h", "text": "yes, keep going"})
        with (
            mock.patch.object(serve.machines, "hostname", return_value="h"),
            mock.patch.object(serve, "run", return_value=(0, "")) as fake_run,
        ):
            handler.do_POST()
        fake_run.assert_called_once_with(
            ["lupin", "loop", "local-action", "send", "a", "yes, keep going"], timeout=20.0
        )
        handler.redirect.assert_called_once_with("/loops?repo=a")

    def test_send_route_remote_machine_without_signing_key_is_rejected(self):
        handler = _loops_handler()
        _post_body(handler, "/loops/send", {"repo": "a", "machine": "jesus", "text": "hi"})
        with mock.patch.object(serve.machines, "hostname", return_value="h"):
            handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_start_route_rejects_bad_repo_name(self):
        handler = _loops_handler()
        _post_body(handler, "/loops/start", {"repo": "bad name", "machine": "h"})
        handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_stop_route_runs_the_local_lupin_action(self):
        handler = _loops_handler()
        _post_body(handler, "/loops/close", {"repo": "a", "machine": "h", "scope": "repo"})
        with (
            mock.patch.object(serve.machines, "hostname", return_value="h"),
            mock.patch.object(serve, "run", return_value=(0, "")) as fake_run,
        ):
            handler.do_POST()
        fake_run.assert_called_once_with(
            ["lupin", "loop", "local-action", "stop", "a", "--force"], timeout=20.0
        )
        handler.redirect.assert_called_once_with("/loops?repo=a")

    def test_close_route_scope_all_stops_every_active_loop_on_that_machine(self):
        entries = [
            _loop_entry("a", "running", "h", session={"name": "loop-a"}),
            _loop_entry("b", "running", "h", session={"name": "loop-b"}),
            _loop_entry("c", "stopped", "h"),
        ]
        handler = _loops_handler()
        _post_body(handler, "/loops/close", {"repo": "a", "machine": "h", "scope": "all"})
        with (
            mock.patch.object(serve.machines, "hostname", return_value="h"),
            mock.patch.object(serve, "gather_loops", return_value={"entries": entries}),
            mock.patch.object(serve, "run", return_value=(0, "")) as fake_run,
        ):
            handler.do_POST()
        self.assertEqual(
            [call.args[0] for call in fake_run.call_args_list],
            [["lupin", "loop", "local-action", "stop", "a", "--force"],
             ["lupin", "loop", "local-action", "stop", "b", "--force"]],
        )

    def test_stop_route_reports_a_local_lupin_failure(self):
        handler = _loops_handler()
        _post_body(handler, "/loops/close", {"repo": "a", "machine": "h", "scope": "repo"})
        with (
            mock.patch.object(serve.machines, "hostname", return_value="h"),
            mock.patch.object(serve, "run", return_value=(1, "boom")),
        ):
            handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 502)
        handler.redirect.assert_not_called()

    def test_close_route_remote_machine_without_signing_key_is_rejected(self):
        handler = _loops_handler()
        _post_body(handler, "/loops/close", {"repo": "a", "machine": "jesus", "scope": "repo"})
        with mock.patch.object(serve.machines, "hostname", return_value="h"):
            handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_start_route_runs_lupin_on_the_local_machine(self):
        handler = _loops_handler()
        _post_body(handler, "/loops/start", {"repo": "a", "machine": "h"})
        with (
            mock.patch.object(serve.machines, "hostname", return_value="h"),
            mock.patch.object(serve, "_repo_platforms", return_value={}),
            mock.patch.object(serve, "run", return_value=(0, "")) as fake_run,
        ):
            handler.do_POST()
        fake_run.assert_called_once_with(["lupin", "run", "a"], timeout=20.0)
        handler.redirect.assert_called_once_with("/loops?repo=a")

    def test_coordinator_cannot_start_a_local_loop(self):
        handler = _loops_handler()
        _post_body(handler, "/loops/start", {"repo": "a", "machine": "pihome"})
        with (
            mock.patch.dict("os.environ", {"LUPIN_LOOP_COORDINATOR_ONLY": "1"}),
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
        ):
            handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_peek_route_uses_the_local_lupin_action(self):
        handler = _loops_handler()
        handler.path = "/peek?repo=a&machine=h&lines=12"
        with (
            mock.patch.object(serve.machines, "hostname", return_value="h"),
            mock.patch.object(serve, "run", return_value=(0, "herdr output")) as fake_run,
        ):
            handler.do_GET()
        fake_run.assert_called_once_with(
            ["lupin", "loop", "local-action", "peek", "a", "12"], timeout=20.0
        )
        self.assertIn("herdr output", handler.reply.call_args.args[0].decode())

    def test_state_route_uses_the_local_lupin_action(self):
        handler = _loops_handler()
        _post_body(handler, "/loops/state", {"repo": "a", "machine": "h"})
        with (
            mock.patch.object(serve.machines, "hostname", return_value="h"),
            mock.patch.object(serve, "run", return_value=(0, '{"state":"working"}')) as fake_run,
        ):
            handler.do_POST()
        fake_run.assert_called_once_with(
            ["lupin", "loop", "local-action", "state", "a"], timeout=20.0
        )
        self.assertIn("working", handler.reply.call_args.args[0].decode())


    def test_start_route_remote_machine_without_signing_key_is_rejected(self):
        handler = _loops_handler()
        _post_body(handler, "/loops/start", {"repo": "a", "machine": "jesus"})
        with mock.patch.object(serve.machines, "hostname", return_value="h"):
            handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_nav_has_a_loops_link(self):
        self.assertIn("href='/loops'", serve.render_nav("loops"))


class TestLoopsPageIntegration:
    """Real `redis-server` fixtures, same rule as TestMachinesPageIntegration
    -- a wrong enqueue shape, or a claim lookup that doesn't actually
    attribute the right host, would be caught here, not just a
    call-was-made assertion.
    """

    def test_herdr_heartbeat_state_is_grouped_by_remote_machine(self, redis_port, flush_redis):
        kw = _kw(redis_port)
        with (
            mock.patch.object(serve, "code_repos", return_value=[{"repo": "widgets", "loopable": True}]),
            mock.patch.object(serve, "enabled_repos", return_value=["widgets"]),
            mock.patch.object(serve.loop_runtime, "local_loops", return_value=[]),
            mock.patch.object(serve, "fleet_state", return_value={
                "machines": [{"name": "jesus", "loops": [{
                    "repo": "widgets", "state": "working", "backend": "herdr",
                    "session": "herdr-widgets", "workspace_id": "w", "pane_id": "p",
                }]}],
                "claims": {},
                "fleet_error": None,
            }),
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
        ):
            handler = _get_handler("/loops?group=machine", kw)
            handler.do_GET()
        body = handler.reply.call_args.args[0].decode()
        assert "jesus" in body
        assert "working" in body
        assert "Herdr state: working" in body

    def test_close_enqueues_loop_stop_for_a_remote_machine(self, redis_port, flush_redis):
        kw = _kw(redis_port)
        handler = _post_handler("/loops/close", b"repo=widgets&machine=jesus&scope=repo", kw)
        handler.cmd_signing_key = "secret"
        with mock.patch.object(serve.machines, "hostname", return_value="pihome"):
            handler.do_POST()

        raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
        queued = raw.zrange("lupin:v1:cmdq:jesus", 0, -1)
        assert len(queued) == 1
        stored = json.loads(raw.get(f"lupin:v1:cmd:{queued[0]}"))
        assert stored["action"] == "loop.stop"
        assert stored["params"] == {"repo": "widgets", "force": True}
        assert stored["target"] == "jesus"
        assert stored["issuer"] == "pihome"
        assert commands.verify(stored, "secret") is True
        handler.send_response.assert_called_once_with(303)

    def test_start_enqueues_loop_run_for_a_remote_machine(self, redis_port, flush_redis):
        kw = _kw(redis_port)
        handler = _post_handler("/loops/start", b"repo=widgets&machine=jesus", kw)
        handler.cmd_signing_key = "secret"
        with mock.patch.object(serve.machines, "hostname", return_value="pihome"):
            handler.do_POST()

        raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
        queued = raw.zrange("lupin:v1:cmdq:jesus", 0, -1)
        assert len(queued) == 1
        stored = json.loads(raw.get(f"lupin:v1:cmd:{queued[0]}"))
        assert stored["action"] == "loop.run"
        assert stored["params"] == {"repo": "widgets"}

    def test_peek_enqueues_a_signed_remote_action(self, redis_port, flush_redis):
        kw = _kw(redis_port)
        handler = _get_handler("/peek", kw)
        handler.cmd_signing_key = "secret"
        with mock.patch.object(serve.machines, "hostname", return_value="pihome"):
            handler.do_peek({"repo": "widgets", "machine": "jesus", "lines": "17"})
        raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
        queued = raw.zrange("lupin:v1:cmdq:jesus", 0, -1)
        assert len(queued) == 1
        stored = json.loads(raw.get(f"lupin:v1:cmd:{queued[0]}"))
        assert stored["action"] == "loop.peek"
        assert stored["params"] == {"repo": "widgets", "lines": 17}
        assert commands.verify(stored, "secret") is True

    def test_state_enqueues_a_signed_remote_action(self, redis_port, flush_redis):
        kw = _kw(redis_port)
        handler = _get_handler("/loops/state", kw)
        handler.cmd_signing_key = "secret"
        with mock.patch.object(serve.machines, "hostname", return_value="pihome"):
            handler.do_loops_state({"repo": ["widgets"], "machine": ["jesus"]})
        raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
        queued = raw.zrange("lupin:v1:cmdq:jesus", 0, -1)
        assert len(queued) == 1
        stored = json.loads(raw.get(f"lupin:v1:cmd:{queued[0]}"))
        assert stored["action"] == "loop.state"
        assert stored["params"] == {"repo": "widgets"}
        assert commands.verify(stored, "secret") is True
    def test_close_scope_all_enqueues_one_command_per_loop_known_remotely(self, redis_port, flush_redis):
        kw = _kw(redis_port)
        entries = [
            {"repo": "widgets", "enabled": True, "status": "working", "machine": "jesus", "session": None},
            {"repo": "gizmos", "enabled": True, "status": "needs_attention", "machine": "jesus", "session": None},
        ]
        handler = _post_handler("/loops/close", b"repo=widgets&machine=jesus&scope=all", kw)
        handler.cmd_signing_key = "secret"
        with (
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
            mock.patch.object(serve, "gather_loops", return_value={"entries": entries}),
        ):
            handler.do_POST()

        raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
        queued = raw.zrange("lupin:v1:cmdq:jesus", 0, -1)
        assert len(queued) == 2


def _schedule_handler(connection=None):
    """A `Handler` for the `/schedule` routes -- same mocked reply/redirect
    style `_loops_handler` uses: these tests check routing, validation, and
    dispatch targets, not real I/O or a real fleet registry."""
    handler = serve.Handler.__new__(serve.Handler)
    handler.fleet_connection = connection if connection is not None else {}
    handler.cmd_signing_key = None
    handler.host_ok = mock.Mock(return_value=True)
    handler.reply = mock.Mock()
    handler.redirect = mock.Mock()
    return handler


class ScheduleHelperTests(unittest.TestCase):
    """Pure functions `render_schedule`/the POST handlers build on --
    exercised directly so the ranking/count rules are covered without
    needing a full page render or request."""

    def test_rank_candidates_orders_by_free_slots_then_name(self):
        records = [
            {"name": "jesus", "state": "online", "slots": {"bmo": {"used": 0, "max": 1}}},
            {"name": "mini", "state": "online", "slots": {"bmo": {"used": 0, "max": 2}}},
            {"name": "build-box", "state": "draining", "slots": {}},
            {"name": "ghost", "state": "offline", "slots": {}},
        ]
        ranked = serve._rank_candidates(records, "pihome")
        self.assertEqual(
            [r["name"] for r in ranked],
            ["mini", "jesus", "pihome"],
        )

    def test_rank_candidates_excludes_a_draining_local_host_not_just_remotes(self):
        records = [{"name": "pihome", "state": "draining", "slots": {}}]
        ranked = serve._rank_candidates(records, "pihome")
        self.assertEqual(ranked, [])

    def test_machine_available_accepts_unregistered_local_host_only(self):
        records = [{"name": "jesus", "state": "online"}]
        self.assertTrue(serve._machine_available("pihome", records, "pihome"))
        self.assertFalse(serve._machine_available("someone-else", records, "pihome"))
        self.assertTrue(serve._machine_available("jesus", records, "pihome"))

    def test_machine_available_rejects_a_draining_or_offline_registered_machine(self):
        records = [
            {"name": "jesus", "state": "draining"},
            {"name": "ghost", "state": "offline"},
        ]
        self.assertFalse(serve._machine_available("jesus", records, "pihome"))
        self.assertFalse(serve._machine_available("ghost", records, "pihome"))

    def test_timer_loop_count_counts_enabled_repos_for_the_recurring_timer(self):
        self.assertEqual(
            serve._timer_loop_count("delegation-loop.timer", "all enabled repos", ["a", "b", "c"]),
            3,
        )

    def test_timer_loop_count_counts_parsed_repos_for_a_one_off_timer(self):
        unit = "delegation-loop-once-abcd.timer"
        self.assertEqual(serve._timer_loop_count(unit, "widgets, gizmos", []), 2)

    def test_timer_loop_count_falls_back_to_one_when_unparsed(self):
        unit = "delegation-loop-once-abcd.timer"
        self.assertEqual(serve._timer_loop_count(unit, unit, []), 1)

    def test_coordinator_only_mode_excludes_local_machine(self):
        records = [
            {"name": "pihome", "state": "online", "slots": {}},
            {"name": "jesus", "state": "online", "slots": {}},
        ]
        with mock.patch.dict("os.environ", {"LUPIN_LOOP_COORDINATOR_ONLY": "1"}):
            ranked = serve._rank_candidates(records, "pihome")
            available = serve._machine_available("pihome", records, "pihome")
        self.assertEqual([record["name"] for record in ranked], ["jesus"])
        self.assertFalse(available)


class ScheduleRenderTests(unittest.TestCase):
    def _data(self, **overrides):
        data = {
            "now": 1_800_000_000.0,
            "enabled": ["widgets"],
            "timers": [
                {
                    "unit": "delegation-loop.timer",
                    "activates": "x",
                    "next": 1_800_000_600.0,
                    "last": 1_799_000_000.0,
                },
            ],
            "timer_active": True,
            "machines": [],
            "fleet_error": None,
            "local_host": "pihome",
        }
        data.update(overrides)
        return data

    def test_renders_timer_card_table_and_run_now_form(self):
        page = serve.render_schedule(self._data()).decode()
        self.assertIn("delegation-loop.timer", page)
        self.assertIn("<button type=submit>Stop</button>", page)
        self.assertIn("Run now", page)
        self.assertIn("<select name=repo>", page)
        self.assertIn("<select name=place>", page)
        self.assertIn("lupin once", page)

    def test_stopped_timer_shows_start_button(self):
        page = serve.render_schedule(self._data(timer_active=False)).decode()
        self.assertIn("<button type=submit>Start</button>", page)

    def test_draining_machine_option_is_disabled(self):
        data = self._data(machines=[{"name": "build-box", "state": "draining", "slots": {}}])
        page = serve.render_schedule(data).decode()
        self.assertIn("value='build-box' disabled", page)


    def test_coordinator_only_schedule_hides_local_machine(self):
        machines = [
            {"name": "pihome", "state": "online", "slots": {}},
            {"name": "jesus", "state": "online", "slots": {}},
        ]
        with mock.patch.dict("os.environ", {"LUPIN_LOOP_COORDINATOR_ONLY": "1"}):
            page = serve.render_schedule(self._data(machines=machines)).decode()
        self.assertNotIn("<option value='pihome'", page)
        self.assertIn("<option value='jesus'", page)

    def test_success_banner_shown_when_sent(self):
        page = serve.render_schedule(self._data(), sent="Started 1 loop(s): widgets@pihome").decode()
        self.assertIn("Started 1 loop(s)", page)

    def test_nav_has_a_schedule_link(self):
        self.assertIn("href='/schedule'", serve.render_nav("schedule"))


class ScheduleTimerRouteTests(unittest.TestCase):
    def test_rejects_bad_action(self):
        handler = _schedule_handler()
        _post_body(handler, "/schedule/timer", {"action": "pause"})
        handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_start_calls_systemctl_and_redirects(self):
        handler = _schedule_handler()
        _post_body(handler, "/schedule/timer", {"action": "start"})
        with mock.patch.object(serve, "run", return_value=(0, "")) as fake_run:
            handler.do_POST()
        fake_run.assert_called_once_with(["systemctl", "start", "delegation-loop.timer"], timeout=10.0)
        handler.redirect.assert_called_once_with("/schedule")

    def test_stop_calls_systemctl(self):
        handler = _schedule_handler()
        _post_body(handler, "/schedule/timer", {"action": "stop"})
        with mock.patch.object(serve, "run", return_value=(0, "")) as fake_run:
            handler.do_POST()
        fake_run.assert_called_once_with(["systemctl", "stop", "delegation-loop.timer"], timeout=10.0)

    def test_systemctl_failure_is_502(self):
        handler = _schedule_handler()
        _post_body(handler, "/schedule/timer", {"action": "stop"})
        with mock.patch.object(serve, "run", return_value=(1, "boom")):
            handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 502)
        handler.redirect.assert_not_called()


class ScheduleRunRouteUnitTests(unittest.TestCase):
    """Routing/validation/dispatch-target logic only -- mocked
    machines.machines()/run()/commands.enqueue, no real Redis or
    subprocess. See TestSchedulePageIntegration below for the real-Redis
    enqueue shape.
    """

    def test_missing_repo_is_400(self):
        handler = _schedule_handler()
        _post_body(handler, "/schedule/run", {"place": "any"})
        handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_missing_placement_is_400(self):
        handler = _schedule_handler()
        _post_body(handler, "/schedule/run", {"repo": "all"})
        with mock.patch.object(serve, "enabled_repos", return_value=["widgets"]):
            handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_unknown_repo_is_400(self):
        handler = _schedule_handler()
        _post_body(handler, "/schedule/run", {"repo": "nope", "place": "any"})
        with mock.patch.object(serve, "enabled_repos", return_value=["widgets"]):
            handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_bad_loop_count_is_400(self):
        handler = _schedule_handler()
        _post_body(handler, "/schedule/run", {"repo": "all", "cnt": "9", "place": "any"})
        with mock.patch.object(serve, "enabled_repos", return_value=["widgets"]):
            handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_bad_placement_is_400(self):
        handler = _schedule_handler()
        _post_body(handler, "/schedule/run", {"repo": "all", "place": "not-a-machine"})
        with (
            mock.patch.object(serve, "enabled_repos", return_value=["widgets"]),
            mock.patch.object(serve.machines, "hostname", return_value="h"),
            mock.patch.object(serve.machines, "machines", return_value=[]),
        ):
            handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_remote_machine_without_signing_key_is_rejected(self):
        handler = _schedule_handler()
        _post_body(handler, "/schedule/run", {"repo": "a-repo", "place": "jesus"})
        with (
            mock.patch.object(serve, "enabled_repos", return_value=["a-repo"]),
            mock.patch.object(serve.machines, "hostname", return_value="h"),
            mock.patch.object(
                serve.machines, "machines",
                return_value=[{"name": "jesus", "state": "online", "slots": {}}],
            ),
        ):
            handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)

    def test_draining_specific_machine_is_rejected(self):
        handler = _schedule_handler()
        _post_body(handler, "/schedule/run", {"repo": "a-repo", "place": "jesus"})
        with (
            mock.patch.object(serve, "enabled_repos", return_value=["a-repo"]),
            mock.patch.object(serve.machines, "hostname", return_value="h"),
            mock.patch.object(
                serve.machines, "machines",
                return_value=[{"name": "jesus", "state": "draining", "slots": {}}],
            ),
        ):
            handler.do_POST()
        self.assertEqual(handler.reply.call_args.args[1], 400)



class TestSchedulePageIntegration:
    """Real `redis-server` fixtures, same rule as TestLoopsPageIntegration
    -- a wrong enqueue shape, or a ranking that doesn't actually read live
    slot data, would be caught here, not just a call-was-made assertion.
    """

    def test_page_renders_real_machine_data(self, redis_port, flush_redis):
        kw = _kw(redis_port)
        _write_machine_record(redis_port, "jesus", state="online")
        with (
            mock.patch.object(serve, "timers", return_value=[]),
            mock.patch.object(serve, "timer_active", return_value=False),
            mock.patch.object(serve, "enabled_repos", return_value=["widgets"]),
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
        ):
            handler = _get_handler("/schedule", kw)
            handler.do_GET()
        body = handler.reply.call_args.args[0].decode()
        assert "jesus" in body
        assert "Run now" in body

    def test_run_now_enqueues_loop_run_for_a_remote_machine(self, redis_port, flush_redis):
        kw = _kw(redis_port)
        _write_machine_record(redis_port, "jesus", state="online")
        handler = _post_handler("/schedule/run", b"repo=widgets&cnt=4&place=jesus", kw)
        handler.cmd_signing_key = "secret"
        with (
            mock.patch.object(serve, "enabled_repos", return_value=["widgets"]),
            mock.patch.object(serve, "_repo_platforms", return_value={"widgets": "omp"}),
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
        ):
            handler.do_POST()

        raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
        queued = raw.zrange("lupin:v1:cmdq:jesus", 0, -1)
        assert len(queued) == 1
        stored = json.loads(raw.get(f"lupin:v1:cmd:{queued[0]}"))
        assert stored["action"] == "loop.run"
        assert stored["params"] == {"repo": "widgets"}
        assert stored["target"] == "jesus"
        assert stored["issuer"] == "pihome"
        assert commands.verify(stored, "secret") is True
        handler.send_response.assert_called_once_with(303)

    def test_run_now_rejects_a_draining_named_machine(self, redis_port, flush_redis):
        kw = _kw(redis_port)
        _write_machine_record(redis_port, "jesus", state="draining")
        handler = _post_handler("/schedule/run", b"repo=widgets&place=jesus", kw)
        handler.cmd_signing_key = "secret"
        handler.reply = mock.Mock()  # a rejection replies with a body; _post_handler has no wfile
        with (
            mock.patch.object(serve, "enabled_repos", return_value=["widgets"]),
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
        ):
            handler.do_POST()
        assert handler.reply.call_args.args[1] == 400

        raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
        assert raw.zrange("lupin:v1:cmdq:jesus", 0, -1) == []

    def test_spread_enqueues_across_every_online_machine(self, redis_port, flush_redis):
        kw = _kw(redis_port)
        _write_machine_record(redis_port, "jesus", state="online")
        _write_machine_record(redis_port, "mini", state="online")
        handler = _post_handler(
            "/schedule/run", b"repo=all&cnt=2&place=spread", kw
        )
        handler.cmd_signing_key = "secret"
        with (
            mock.patch.object(
                serve, "enabled_repos",
                return_value=["a-repo", "b-repo", "c-repo"],
            ),
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
            mock.patch.dict("os.environ", {"LUPIN_LOOP_COORDINATOR_ONLY": "1"}),
        ):
            handler.do_POST()

        raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
        payloads = {}
        for machine in ("jesus", "mini"):
            queued = raw.zrange(f"lupin:v1:cmdq:{machine}", 0, -1)
            assert len(queued) == 1
            payloads[machine] = json.loads(raw.get(f"lupin:v1:cmd:{queued[0]}"))
        assert [
            (payload["target"], payload["params"])
            for payload in (payloads["jesus"], payloads["mini"])
        ] == [
            ("jesus", {"repo": "a-repo"}),
            ("mini", {"repo": "b-repo"}),
        ]
        assert all(commands.verify(payload, "secret") for payload in payloads.values())
        assert raw.zrange("lupin:v1:cmdq:pihome", 0, -1) == []
        handler.send_response.assert_called_once_with(303)


# --------------------------------------------------------------------------
# Repos page (issue #23)
# --------------------------------------------------------------------------


def _repos_handler(connection=None):
    """A `Handler` for the `/repos` routes, same mocked reply/redirect
    style `_loops_handler`/`_schedule_handler` use."""
    handler = serve.Handler.__new__(serve.Handler)
    handler.fleet_connection = connection if connection is not None else {}
    handler.cmd_signing_key = None
    handler.host_ok = mock.Mock(return_value=True)
    handler.reply = mock.Mock()
    handler.redirect = mock.Mock()
    return handler


def _make_repo(tmp_path, name, *, doc=True):
    """A throwaway `<tmp_path>/<name>` directory, with a delegation doc
    unless `doc=False` -- the fixture every filesystem-touching `/repos/*`
    route test below builds on, with `CODE_DIR` pointed at `tmp_path` so
    none of this ever touches the real /code.
    """
    repo_dir = tmp_path / name
    repo_dir.mkdir()
    if doc:
        (repo_dir / "docs").mkdir()
        (repo_dir / "docs" / "delegation-loop.md").write_text("# doc\n")
    return repo_dir


class RepoHelperTests(unittest.TestCase):
    def test_repo_slot_name_is_prefixed_so_it_cannot_collide_with_bmo(self):
        self.assertEqual(serve._repo_slot_name("widgets"), "repo:widgets")

    def test_delegation_doc_template_names_the_repo(self):
        text = serve._delegation_doc_template("widgets")
        self.assertIn("widgets", text)

    def test_write_enabled_repos_round_trips_through_enabled_repos(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = os.path.join(tmp, "state")
            with (
                mock.patch.object(serve, "STATE_DIR", state_dir),
                mock.patch.object(serve, "REPOS_FILE", os.path.join(state_dir, "repos")),
            ):
                serve.write_enabled_repos(["b-repo", "a-repo"])
                self.assertEqual(serve.enabled_repos(), ["b-repo", "a-repo"])

    def test_enabled_repos_recognizes_a_bare_name_line(self):
        """The dashboard's own `/repos/add` writes one name per line."""
        with tempfile.TemporaryDirectory() as tmp:
            repos_file = os.path.join(tmp, "repos")
            with open(repos_file, "w", encoding="utf-8") as fh:
                fh.write("widgets\n")
            with mock.patch.object(serve, "REPOS_FILE", repos_file):
                self.assertEqual(serve.enabled_repos(), ["widgets"])

    def test_enabled_repos_reads_a_platform_field(self):
        """A platform name follows the repo name in `REPOS_FILE`."""
        with tempfile.TemporaryDirectory() as tmp:
            repos_file = os.path.join(tmp, "repos")
            with open(repos_file, "w", encoding="utf-8") as fh:
                fh.write("widgets claude\n")
            with mock.patch.object(serve, "REPOS_FILE", repos_file):
                self.assertEqual(serve.enabled_repos(), ["widgets"])

    def test_slot_controls_disable_the_lower_button_at_the_minimum(self):
        html = serve._repo_slot_controls("widgets", 1)
        self.assertIn("disabled", html)

    def test_slot_controls_post_the_computed_neighbor_values(self):
        html = serve._repo_slot_controls("widgets", 3)
        self.assertNotIn("disabled", html)
        self.assertIn("name=max value='2'", html)
        self.assertIn("name=max value='4'", html)

    def test_code_repos_allows_a_directory_without_delegation_doc(self):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "widgets").mkdir()
            with (
                mock.patch.object(serve, "CODE_DIR", tmp),
                mock.patch.object(serve, "enabled_repos", return_value=[]),
            ):
                self.assertEqual(
                    serve.code_repos(),
                    [{
                        "repo": "widgets",
                        "state": "disabled",
                        "loopable": True,
                        "has_doc": False,
                    }],
                )


class GatherReposTests(unittest.TestCase):
    """`gather_repos` (issue #23) -- mocked `code_repos`/`gather_loops`/
    `slots_redis.status`, no real Redis or filesystem."""

    def test_folds_running_status_and_slot_max_into_code_repos(self):
        with (
            mock.patch.object(
                serve, "code_repos",
                return_value=[{"repo": "a", "state": "enabled", "loopable": True}],
            ),
            mock.patch.object(
                serve, "gather_loops",
                return_value={
                    "entries": [{"repo": "a", "status": "running", "machine": "h", "session": None}],
                    "machines": [], "fleet_error": None, "local_host": "h",
                },
            ),
            mock.patch.object(serve.slots_redis, "status", return_value={"repo:a": {"holders": 0, "max": 3}}),
        ):
            data = serve.gather_repos({})
        row = data["repos"][0]
        self.assertTrue(row["running"])
        self.assertEqual(row["machine"], "h")
        self.assertEqual(row["max"], 3)

    def test_repo_without_doc_defaults_to_local_host_not_running_no_max(self):
        with (
            mock.patch.object(
                serve, "code_repos",
                return_value=[{"repo": "x", "state": "disabled", "loopable": True, "has_doc": False}],
            ),
            mock.patch.object(
                serve, "gather_loops",
                return_value={"entries": [], "machines": [], "fleet_error": None, "local_host": "h"},
            ),
            mock.patch.object(serve.slots_redis, "status", return_value={}),
        ):
            data = serve.gather_repos({})
        row = data["repos"][0]
        self.assertFalse(row["running"])
        self.assertEqual(row["machine"], "h")
        self.assertIsNone(row["max"])

    def test_a_herdr_heartbeat_counts_as_running_on_its_machine(self):
        with (
            mock.patch.object(
                serve, "code_repos",
                return_value=[{"repo": "a", "state": "enabled", "loopable": True}],
            ),
            mock.patch.object(
                serve, "gather_loops",
                return_value={
                    "entries": [{
                        "repo": "a", "status": "needs_attention", "agent_status": "needs_attention",
                        "machine": "jesus", "session": "herdr-a", "backend": "herdr",
                    }],
                    "machines": [], "fleet_error": None, "local_host": "h",
                },
            ),
            mock.patch.object(serve.slots_redis, "status", return_value={}),
        ):
            data = serve.gather_repos({})
        row = data["repos"][0]
        self.assertTrue(row["running"])
        self.assertEqual(row["machine"], "jesus")

    def test_remote_repo_inventory_renders_enable_and_start_controls(self):
        machine = {
            "name": "jesus",
            "state": "online",
            "actions": ["loop.run", "repo.enable"],
            "repos": [{"repo": "roundsmith", "enabled": False, "loopable": True}],
            "loops": [],
        }
        with (
            mock.patch.object(serve, "code_repos", return_value=[]),
            mock.patch.object(
                serve, "gather_loops",
                return_value={
                    "entries": [],
                    "machines": [machine],
                    "fleet_error": None,
                    "local_host": "pihome",
                },
            ),
            mock.patch.object(serve.slots_redis, "status", return_value={}),
        ):
            data = serve.gather_repos({})

        page = serve.render_repos(data, add="existing").decode()

        assert [repo["repo"] for repo in data["repos"]] == ["roundsmith"]
        assert "Enable on jesus" in page
        assert "action=/repos/enable" in page
        assert "action=/loops/start" not in page
        assert "No local repos under /code" in page

    def test_enabled_remote_repo_has_start_control(self):
        machine = {
            "name": "jesus",
            "state": "online",
            "actions": ["loop.run", "repo.enable"],
            "repos": [{"repo": "roundsmith", "enabled": True, "loopable": True}],
            "loops": [],
        }
        with (
            mock.patch.object(serve, "code_repos", return_value=[]),
            mock.patch.object(
                serve, "gather_loops",
                return_value={
                    "entries": [],
                    "machines": [machine],
                    "fleet_error": None,
                    "local_host": "pihome",
                },
            ),
            mock.patch.object(serve.slots_redis, "status", return_value={}),
        ):
            data = serve.gather_repos({})

        page = serve.render_repos(data).decode()
        assert "Start on jesus" in page
        assert "action=/loops/start" in page
        assert "action=/repos/enable" not in page

    def test_remote_controls_remain_available_for_a_local_repo_row(self):
        machine = {
            "name": "jesus",
            "state": "online",
            "actions": ["loop.run"],
            "repos": [{"repo": "roundsmith", "enabled": True, "loopable": True}],
            "loops": [],
        }
        with (
            mock.patch.object(
                serve, "code_repos",
                return_value=[{
                    "repo": "roundsmith", "state": "disabled",
                    "loopable": True, "has_doc": True,
                }],
            ),
            mock.patch.object(
                serve, "gather_loops",
                return_value={
                    "entries": [],
                    "machines": [machine],
                    "fleet_error": None,
                    "local_host": "pihome",
                },
            ),
            mock.patch.object(serve.slots_redis, "status", return_value={}),
        ):
            data = serve.gather_repos({})

        page = serve.render_repos(data).decode()
        assert "Add to schedule" in page
        assert "Start on jesus" in page

    def test_overview_shows_remote_repo_without_a_local_checkout(self):
        machine = {
            "name": "jesus",
            "state": "online",
            "repos": [{"repo": "roundsmith", "enabled": True, "loopable": True}],
            "loops": [],
        }
        with (
            mock.patch.object(serve, "enabled_repos", return_value=[]),
            mock.patch.object(serve, "code_repos", return_value=[]),
            mock.patch.object(serve, "timers", return_value=[]),
            mock.patch.object(serve, "timer_active", return_value=False),
            mock.patch.object(
                serve, "fleet_state",
                return_value={
                    "machines": [machine],
                    "claims": {},
                    "fleet_error": None,
                },
            ),
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
        ):
            state = serve.gather(25, {})

        page = serve.render_dashboard(state).decode()

        assert state["enabled"] == ["roundsmith"]
        assert "<td>roundsmith</td>" in page
        assert "available on jesus" in page
        assert "lupin once now roundsmith" not in page



class RepoRenderTests(unittest.TestCase):
    def _data(self, **overrides):
        data = {
            "repos": [
                {"repo": "widgets", "state": "enabled", "loopable": True, "has_doc": True, "running": True, "machine": "pihome", "max": 2},
                {"repo": "gizmos", "state": "disabled", "loopable": True, "has_doc": True, "running": False, "machine": "pihome", "max": None},
                {"repo": "scratch", "state": "disabled", "loopable": True, "has_doc": False, "running": False, "machine": "pihome", "max": None},
            ],
            "machines": [],
            "fleet_error": None,
            "local_host": "pihome",
        }
        data.update(overrides)
        return data

    def test_renders_repo_states_and_marks_missing_docs_optional(self):
        page = serve.render_repos(self._data()).decode()
        self.assertIn("widgets", page)
        self.assertIn("<span class='pill on'>enabled</span>", page)
        self.assertIn("<span class=pill>disabled</span>", page)
        self.assertIn("no doc (optional)", page)

    def test_enabled_row_has_run_now_and_remove(self):
        page = serve.render_repos(self._data()).decode()
        self.assertIn("action=/schedule/run", page)
        self.assertIn("/repos?remove=widgets", page)

    def test_disabled_row_has_add_to_schedule_not_a_remove_link(self):
        page = serve.render_repos(self._data()).decode()
        self.assertIn("action=/repos/add", page)
        self.assertNotIn("/repos?remove=gizmos", page)

    def test_no_doc_repo_keeps_schedule_controls(self):
        page = serve.render_repos(self._data()).decode()
        self.assertIn("no doc (optional)", page)
        self.assertIn("repo=scratch", page)

    def test_slot_max_stepper_shows_the_current_value(self):
        page = serve.render_repos(self._data()).decode()
        self.assertIn("<span class=mono>2</span>", page)

    def test_empty_state_shown_when_no_repos(self):
        page = serve.render_repos(self._data(repos=[])).decode()
        self.assertIn("No repos yet", page)

    def test_sent_banner_shown(self):
        page = serve.render_repos(self._data(), sent="widgets added to the schedule").decode()
        self.assertIn("widgets added to the schedule", page)

    def test_add_panel_offers_schedule_and_optional_doc_generation(self):
        page = serve.render_repos(self._data(), add="existing").decode()
        self.assertIn("action=/repos/add", page)
        self.assertIn("action=/repos/generate-docs", page)
        self.assertIn("no delegation doc (optional)", page)

    def test_add_panel_new_tab_is_disabled_with_a_note(self):
        page = serve.render_repos(self._data(), add="new").decode()
        self.assertIn("Not implemented yet", page)
        self.assertIn("disabled", page)

    def test_add_panel_says_no_repos_found_when_code_repos_is_empty(self):
        """No local repos and no fleet report means there is nothing to add."""
        page = serve.render_repos(self._data(repos=[]), add="existing").decode()
        self.assertIn("No repos found under /code", page)
        self.assertNotIn("already on the schedule", page)

    def test_add_panel_says_every_repo_already_enabled_when_none_are_left_to_add(self):
        repos = [{"repo": "widgets", "state": "enabled", "loopable": True,
                   "running": False, "machine": "pihome", "max": 2}]
        page = serve.render_repos(self._data(repos=repos), add="existing").decode()
        self.assertIn("Every repo under /code is already on the schedule", page)
        self.assertNotIn("No repos found", page)

    def test_doc_panel_view_mode_shows_the_text(self):
        page = serve.render_repos(self._data(), doc_repo="widgets", doc_text="hello doc").decode()
        self.assertIn("hello doc", page)
        self.assertIn("widgets/docs/delegation-loop.md", page)

    def test_doc_panel_edit_mode_shows_a_textarea(self):
        page = serve.render_repos(
            self._data(), doc_repo="widgets", doc_text="hello doc", doc_edit=True
        ).decode()
        self.assertIn("<textarea name=text", page)

    def test_schedule_panel_shows_the_lupin_command(self):
        page = serve.render_repos(self._data(), schedule_repo="widgets").decode()
        self.assertIn("lupin once", page)
        self.assertIn("widgets", page)

    def test_remove_panel_asks_to_type_the_repo_name(self):
        page = serve.render_repos(self._data(), remove_repo="widgets").decode()
        self.assertIn("Type the repo name to confirm", page)

    def test_fleet_error_note_shown(self):
        page = serve.render_repos(self._data(fleet_error="boom")).decode()
        self.assertIn("boom", page)

    def test_nav_has_a_repos_link(self):
        self.assertIn("href='/repos'", serve.render_nav("repos"))


class TestReposPageRoutes:
    """Routing/validation logic for the `/repos` write routes (issue #23).
    `CODE_DIR` is pointed at a throwaway `tmp_path`, never the real /code --
    same reasoning the rest of this module mocks `run()` for, just for the
    filesystem instead of a subprocess.
    """

    def test_repos_route_renders(self, monkeypatch):
        handler = _repos_handler()
        handler.path = "/repos"
        monkeypatch.setattr(
            serve, "gather_repos",
            lambda connection: {"repos": [], "machines": [], "fleet_error": None, "local_host": "h"},
        )
        handler.do_GET()
        handler.reply.assert_called_once()

    def test_doc_route_unknown_repo_is_404(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        handler = _repos_handler()
        handler.path = "/repos?doc=missing"
        handler.do_GET()
        assert handler.reply.call_args.args[1] == 404

    def test_doc_route_reads_the_real_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets")
        monkeypatch.setattr(
            serve, "gather_repos",
            lambda connection: {"repos": [], "machines": [], "fleet_error": None, "local_host": "h"},
        )
        handler = _repos_handler()
        handler.path = "/repos?doc=widgets"
        handler.do_GET()
        body = handler.reply.call_args.args[0].decode()
        assert "# doc" in body

    def test_doc_route_returns_404_when_optional_doc_is_absent(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets", doc=False)
        handler = _repos_handler()
        handler.path = "/repos?doc=widgets"
        handler.do_GET()
        assert handler.reply.call_args.args[1] == 404

    def test_remote_enable_route_dispatches_to_reported_worker(self, monkeypatch):
        machine = {
            "name": "jesus",
            "state": "online",
            "actions": ["repo.enable"],
            "repos": [{"repo": "widgets", "enabled": False, "loopable": True}],
        }
        monkeypatch.setattr(serve.machines, "hostname", lambda: "pihome")
        monkeypatch.setattr(serve.machines, "machines", lambda connection: [machine])
        dispatch = mock.Mock(return_value={"mode": "queued", "id": "cmd", "result": None})
        monkeypatch.setattr(serve.loops, "dispatch_loop_action", dispatch)
        handler = _repos_handler()
        handler._signing_key_for = lambda target: "jesus-key"

        _post_body(handler, "/repos/enable", {"repo": "widgets", "machine": "jesus"})
        handler.do_POST()

        dispatch.assert_called_once_with(
            machine="jesus",
            local_host="pihome",
            local_argv=["lupin", "enable", "widgets"],
            queue_action="repo.enable",
            queue_params={"repo": "widgets"},
            connection={},
            signing_key="jesus-key",
            actor="lupin-dashboard",
            issuer="pihome",
        )
        handler.redirect.assert_called_once()

    def test_add_route_rejects_a_missing_repository(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        handler = _repos_handler()
        _post_body(handler, "/repos/add", {"repo": "ghost"})
        handler.do_POST()
        assert handler.reply.call_args.args[1] == 400

    def test_add_route_accepts_a_repo_without_a_delegation_doc(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets", doc=False)
        monkeypatch.setattr(serve, "enabled_repos", lambda: ["other"])
        written = {}
        monkeypatch.setattr(serve, "write_enabled_repos", lambda names: written.setdefault("names", names))
        handler = _repos_handler()
        _post_body(handler, "/repos/add", {"repo": "widgets"})
        handler.do_POST()
        assert written["names"] == ["other", "widgets"]
        handler.redirect.assert_called_once()
        assert "added" in handler.redirect.call_args.args[0]

    def test_add_route_is_a_no_op_when_already_enabled(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets")
        monkeypatch.setattr(serve, "enabled_repos", lambda: ["widgets"])

        def _fail(names):
            raise AssertionError("should not write -- already enabled")

        monkeypatch.setattr(serve, "write_enabled_repos", _fail)
        handler = _repos_handler()
        _post_body(handler, "/repos/add", {"repo": "widgets"})
        handler.do_POST()
        handler.redirect.assert_called_once()

    def test_generate_docs_route_rejects_a_repo_that_already_has_one(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets")
        handler = _repos_handler()
        _post_body(handler, "/repos/generate-docs", {"repo": "widgets"})
        handler.do_POST()
        assert handler.reply.call_args.args[1] == 400

    def test_generate_docs_route_rejects_an_unknown_directory(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        handler = _repos_handler()
        _post_body(handler, "/repos/generate-docs", {"repo": "ghost"})
        handler.do_POST()
        assert handler.reply.call_args.args[1] == 400

    def test_generate_docs_route_writes_a_doc_and_adds_to_the_schedule(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets", doc=False)
        monkeypatch.setattr(serve, "enabled_repos", lambda: [])
        written = {}
        monkeypatch.setattr(serve, "write_enabled_repos", lambda names: written.setdefault("names", names))
        handler = _repos_handler()
        _post_body(handler, "/repos/generate-docs", {"repo": "widgets"})
        handler.do_POST()
        doc_path = tmp_path / "widgets" / "docs" / "delegation-loop.md"
        assert doc_path.is_file()
        assert "widgets" in doc_path.read_text()
        assert written["names"] == ["widgets"]

    def test_remove_route_rejects_a_repo_not_on_the_schedule(self, monkeypatch):
        monkeypatch.setattr(serve, "enabled_repos", lambda: ["other"])
        handler = _repos_handler()
        _post_body(handler, "/repos/remove", {"repo": "widgets", "confirm": "widgets"})
        handler.do_POST()
        assert handler.reply.call_args.args[1] == 400

    def test_remove_route_rejects_a_mismatched_confirmation(self, monkeypatch):
        monkeypatch.setattr(serve, "enabled_repos", lambda: ["widgets"])
        handler = _repos_handler()
        _post_body(handler, "/repos/remove", {"repo": "widgets", "confirm": "not-it"})
        handler.do_POST()
        assert handler.reply.call_args.args[1] == 400

    def test_remove_route_removes_a_confirmed_repo(self, monkeypatch):
        monkeypatch.setattr(serve, "enabled_repos", lambda: ["widgets", "other"])
        disabled = []
        monkeypatch.setattr(serve.loop_runtime, "disable_repo", disabled.append)
        handler = _repos_handler()
        _post_body(handler, "/repos/remove", {"repo": "widgets", "confirm": "widgets"})
        handler.do_POST()
        assert disabled == ["widgets"]
        handler.redirect.assert_called_once()

    def test_doc_save_route_rejects_a_missing_repository(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        handler = _repos_handler()
        _post_body(handler, "/repos/doc/save", {"repo": "ghost", "text": "hi"})
        handler.do_POST()
        assert handler.reply.call_args.args[1] == 400

    def test_doc_save_route_does_not_create_a_missing_optional_doc(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets", doc=False)
        handler = _repos_handler()
        _post_body(handler, "/repos/doc/save", {"repo": "widgets", "text": "hi"})
        handler.do_POST()
        assert handler.reply.call_args.args[1] == 400
        assert not (tmp_path / "widgets" / "docs" / "delegation-loop.md").exists()

    def test_doc_save_route_overwrites_the_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets")
        handler = _repos_handler()
        _post_body(handler, "/repos/doc/save", {"repo": "widgets", "text": "new content"})
        handler.do_POST()
        doc_path = tmp_path / "widgets" / "docs" / "delegation-loop.md"
        assert doc_path.read_text() == "new content"
        handler.redirect.assert_called_once_with("/repos?doc=widgets&sent=saved")

    def test_slot_max_route_rejects_a_missing_repository(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        handler = _repos_handler()
        _post_body(handler, "/repos/slot-max", {"repo": "ghost", "max": "2"})
        handler.do_POST()
        assert handler.reply.call_args.args[1] == 400

    def test_slot_max_route_rejects_a_non_positive_max(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets")
        handler = _repos_handler()
        _post_body(handler, "/repos/slot-max", {"repo": "widgets", "max": "0"})
        handler.do_POST()
        assert handler.reply.call_args.args[1] == 400

    def test_slot_max_route_calls_set_max_with_the_repo_prefixed_slot_name(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets")
        calls = {}

        def fake_set_max(slot, max_value, **kw):
            calls["slot"] = slot
            calls["max"] = max_value
            return max_value

        monkeypatch.setattr(serve.slots_redis, "set_max", fake_set_max)
        handler = _repos_handler()
        _post_body(handler, "/repos/slot-max", {"repo": "widgets", "max": "3"})
        handler.do_POST()
        assert calls == {"slot": "repo:widgets", "max": 3}
        handler.redirect.assert_called_once_with("/repos")

    def test_slot_max_route_502_on_coordinator_unreachable(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets")

        def raise_unreachable(slot, max_value, **kw):
            raise slots.CoordinatorUnreachable(slot)

        monkeypatch.setattr(serve.slots_redis, "set_max", raise_unreachable)
        handler = _repos_handler()
        _post_body(handler, "/repos/slot-max", {"repo": "widgets", "max": "3"})
        handler.do_POST()
        assert handler.reply.call_args.args[1] == 502

    def test_slot_max_route_refused_login_names_the_repo_slot(
        self, tmp_path, monkeypatch, auth_redis_port, no_client_retry
    ):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets")
        handler = _repos_handler(_kw(auth_redis_port))
        _post_body(handler, "/repos/slot-max", {"repo": "widgets", "max": "3"})
        handler.do_POST()

        assert handler.reply.call_args.args[1] == 502
        body = html.unescape(handler.reply.call_args.args[0].decode())
        assert "Check the Redis password" in body
        assert "For slot 'repo:widgets'." in body
        assert "unreachable" not in body

    def test_schedule_route_rejects_a_missing_repository(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        handler = _repos_handler()
        _post_body(handler, "/repos/schedule", {"repo": "ghost", "when": "now"})
        handler.do_POST()
        assert handler.reply.call_args.args[1] == 400

    def test_schedule_route_rejects_a_missing_when(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets")
        handler = _repos_handler()
        _post_body(handler, "/repos/schedule", {"repo": "widgets", "when": ""})
        handler.do_POST()
        assert handler.reply.call_args.args[1] == 400

    def test_schedule_route_runs_a_local_lupin_once_command(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets", doc=False)
        calls = []

        def fake_run(argv, timeout=10.0):
            calls.append(argv)
            return (0, "")

        monkeypatch.setattr(serve, "run", fake_run)
        handler = _repos_handler()
        _post_body(handler, "/repos/schedule", {"repo": "widgets", "when": "tomorrow 09:00"})
        handler.do_POST()
        assert calls == [["lupin", "once", "tomorrow 09:00", "widgets"]]
        handler.redirect.assert_called_once()
        assert "scheduled" in handler.redirect.call_args.args[0]

    def test_schedule_route_reports_lupin_failure(self, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets")
        monkeypatch.setattr(serve, "run", lambda argv, timeout=10.0: (1, "boom"))
        handler = _repos_handler()
        _post_body(handler, "/repos/schedule", {"repo": "widgets", "when": "now"})
        handler.do_POST()
        assert handler.reply.call_args.args[1] == 502
        handler.redirect.assert_not_called()


class TestReposPageIntegration:
    """Real `redis-server` fixtures, same rule as TestMachinesPageIntegration
    -- checks the real slot shape (`repo:<repo>`), not just that `set_max`
    was called with the right arguments.
    """

    def test_slot_max_route_changes_what_status_reports(self, redis_port, flush_redis, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets")
        kw = _kw(redis_port)
        handler = _post_handler("/repos/slot-max", b"repo=widgets&max=4", kw)
        handler.do_POST()
        handler.send_response.assert_called_once_with(303)
        assert slots_redis.status(**kw)["repo:widgets"]["max"] == 4

    def test_repos_page_renders_the_live_slot_max(self, redis_port, flush_redis, tmp_path, monkeypatch):
        monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
        _make_repo(tmp_path, "widgets")
        kw = _kw(redis_port)
        slots_redis.set_max("repo:widgets", 5, **kw)
        with (
            mock.patch.object(serve, "enabled_repos", return_value=["widgets"]),
            mock.patch.object(serve.loop_runtime, "local_loops", return_value=[]),
            mock.patch.object(
                serve, "fleet_state",
                return_value={"machines": [], "claims": {}, "fleet_error": None},
            ),
            mock.patch.object(serve.machines, "hostname", return_value="pihome"),
        ):
            handler = _get_handler("/repos", kw)
            handler.do_GET()
        body = handler.reply.call_args.args[0].decode()
        assert "<span class=mono>5</span>" in body


DEBRIEF_ATTACHMENT = "11111111-2222-3333-4444-555555555555"
DEBRIEF_TEXT = (
    "# Debrief: acme/widgets\n"
    "## Shipped\n"
    "- PR #11: Shipped feature <b>x</b>\n"
    f"- Issue #5: ![shot](https://github.com/user-attachments/assets/{DEBRIEF_ATTACHMENT})\n"
)


def _write_debrief_file(root, repo, name, text):
    folder = root / "debriefs" / repo
    folder.mkdir(parents=True, exist_ok=True)
    (folder / name).write_text(text, encoding="utf-8")


def test_debrief_index_lists_newest_first_with_utc_file_name_times(tmp_path, monkeypatch):
    monkeypatch.setattr(serve, "STATE_DIR", str(tmp_path))
    _write_debrief_file(tmp_path, "widgets", "20261001-090000.md", "# old\n")
    _write_debrief_file(tmp_path, "widgets", "20261002-090000.md", "# new\n")

    handler = _get_handler("/debrief", {})
    handler.do_GET()

    body = handler.reply.call_args.args[0].decode()
    assert "href='/debrief?repo=widgets&amp;file=20261002-090000.md'" in body
    assert "2026-10-02 09:00:00 UTC" in body
    assert body.index("20261002-090000") < body.index("20261001-090000")


def test_debrief_index_shows_the_period_of_a_periodic_debrief(tmp_path, monkeypatch):
    monkeypatch.setattr(serve, "STATE_DIR", str(tmp_path))
    _write_debrief_file(tmp_path, "widgets", "20261002-090000-6h.md", "# six\n")

    handler = _get_handler("/debrief", {})
    handler.do_GET()

    body = handler.reply.call_args.args[0].decode()
    assert "2026-10-02 09:00:00 UTC -6h" in body


def test_debrief_index_says_when_there_are_none(tmp_path, monkeypatch):
    monkeypatch.setattr(serve, "STATE_DIR", str(tmp_path))

    handler = _get_handler("/debrief", {})
    handler.do_GET()

    assert b"No debriefs yet." in handler.reply.call_args.args[0]


def test_debrief_page_renders_markdown_escapes_text_and_maps_images(tmp_path, monkeypatch):
    monkeypatch.setattr(serve, "STATE_DIR", str(tmp_path))
    _write_debrief_file(tmp_path, "widgets", "20261002-090000.md", DEBRIEF_TEXT)

    handler = _get_handler("/debrief?repo=widgets&file=20261002-090000.md", {})
    handler.do_GET()

    body = handler.reply.call_args.args[0].decode()
    assert "<h1>Debrief: acme/widgets</h1>" in body
    assert "<li>PR #11: Shipped feature &lt;b&gt;x&lt;/b&gt;</li>" in body
    assert f"<img src='/image?id={DEBRIEF_ATTACHMENT}'" in body


@pytest.mark.parametrize("query", [
    "repo=nope&file=20261002-090000.md",
    "repo=widgets&file=../../etc/passwd",
    "repo=widgets&file=notes.md",
])
def test_debrief_page_refuses_unknown_or_bad_names_with_404(tmp_path, monkeypatch, query):
    monkeypatch.setattr(serve, "STATE_DIR", str(tmp_path))
    _write_debrief_file(tmp_path, "widgets", "20261002-090000.md", DEBRIEF_TEXT)

    handler = _get_handler(f"/debrief?{query}", {})
    handler.do_GET()

    body, status = handler.reply.call_args.args
    assert status == 404
    assert b"no such debrief" in body


def test_evidence_route_serves_a_repo_image_over_http(tmp_path, monkeypatch):
    monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
    (tmp_path / "widgets" / "docs").mkdir(parents=True)
    (tmp_path / "widgets" / "docs" / "shot.png").write_bytes(b"\x89PNG-test")

    with (
        mock.patch.object(serve, "enabled_repos", return_value=["widgets"]),
        _live_dashboard({}) as port,
    ):
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/evidence?repo=widgets&path=docs/shot.png", timeout=15
        ) as resp:
            assert resp.status == 200
            assert resp.headers["Content-Type"] == "image/png"
            assert resp.headers["Content-Security-Policy"] == "default-src 'none'; sandbox"
            assert resp.headers["X-Content-Type-Options"] == "nosniff"
            assert resp.read() == b"\x89PNG-test"


@pytest.mark.parametrize("query", [
    "repo=widgets&path=docs/../shot.png",
    "repo=widgets&path=/etc/passwd.png",
    "repo=widgets&path=src/shot.png",
    "repo=widgets&path=docs/shot.svg",
    "repo=other&path=docs/shot.png",
    "repo=../widgets&path=docs/shot.png",
    "repo=widgets",
])
def test_evidence_route_refuses_with_404_over_http(tmp_path, monkeypatch, query):
    monkeypatch.setattr(serve, "CODE_DIR", str(tmp_path))
    (tmp_path / "widgets" / "docs").mkdir(parents=True)
    (tmp_path / "widgets" / "docs" / "shot.png").write_bytes(b"\x89PNG-test")
    (tmp_path / "widgets" / "docs" / "shot.svg").write_bytes(b"<svg/>")
    (tmp_path / "widgets" / "shot.png").write_bytes(b"\x89PNG-test")
    (tmp_path / "widgets" / "src").mkdir()
    (tmp_path / "widgets" / "src" / "shot.png").write_bytes(b"\x89PNG-test")

    with (
        mock.patch.object(serve, "enabled_repos", return_value=["widgets"]),
        _live_dashboard({}) as port,
    ):
        with pytest.raises(urllib.error.HTTPError) as refused:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/evidence?{query}", timeout=15)

    assert refused.value.code == 404


DEBRIEF_SHOT_REL = "20261010-120000-6h-screenshots/debrief.png"


def test_debrief_shot_route_serves_a_screenshot_over_http(tmp_path, monkeypatch):
    monkeypatch.setattr(serve, "STATE_DIR", str(tmp_path))
    shots = tmp_path / "debriefs" / "widgets" / "20261010-120000-6h-screenshots"
    shots.mkdir(parents=True)
    (shots / "debrief.png").write_bytes(b"\x89PNG-test")

    with _live_dashboard({}) as port:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/debrief/shot?repo=widgets&path={quote(DEBRIEF_SHOT_REL, safe='')}",
            timeout=15,
        ) as resp:
            assert resp.status == 200
            assert resp.headers["Content-Type"] == "image/png"
            assert resp.headers["Content-Security-Policy"] == "default-src 'none'; sandbox"
            assert resp.read() == b"\x89PNG-test"


@pytest.mark.parametrize("rel", [
    "20261010-120000-6h-screenshots/../../../secret.png",
    "../../secret.png",
    "/etc/passwd.png",
    "20261010-120000-6h-screenshots/missing.png",
])
def test_debrief_shot_route_refuses_with_404_over_http(tmp_path, monkeypatch, rel):
    monkeypatch.setattr(serve, "STATE_DIR", str(tmp_path))
    shots = tmp_path / "debriefs" / "widgets" / "20261010-120000-6h-screenshots"
    shots.mkdir(parents=True)
    (shots / "debrief.png").write_bytes(b"\x89PNG-test")
    (tmp_path / "secret.png").write_bytes(b"\x89PNG-secret")

    with _live_dashboard({}) as port:
        with pytest.raises(urllib.error.HTTPError) as refused:
            urllib.request.urlopen(
                f"http://127.0.0.1:{port}/debrief/shot?repo=widgets&path={quote(rel, safe='')}",
                timeout=15,
            )

    assert refused.value.code == 404


if __name__ == "__main__":
    unittest.main()
