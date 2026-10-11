"""Tests for GitHub-issue claims (issue #6).

Uses the real `redis-server` fixtures in `conftest.py` -- `redis_port`,
`flush_redis`, `closed_port` -- shared with `test_slots_redis.py`.
"""

from __future__ import annotations

import time

import pytest
import redis as redis_lib

from lupin import cli, claims, slots


def test_claim_succeeds_and_is_visible_in_redis(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    claims.claim("gracecraft/lupin#6", "host-a:session-1", **kw)

    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    value = raw.get("lupin:v1:claim:gracecraft/lupin#6")
    assert value is not None
    assert '"session": "host-a:session-1"' in value or "session" in value


def test_claim_is_idempotent_for_the_same_holder(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    claims.claim("gracecraft/lupin#6", "same-holder", **kw)
    # A retry by the same holder renews rather than raising ClaimHeld.
    claims.claim("gracecraft/lupin#6", "same-holder", **kw)


def test_claim_fails_when_already_held_by_someone_else(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    claims.claim("gracecraft/lupin#6", "first-holder", **kw)
    with pytest.raises(claims.ClaimHeld):
        claims.claim("gracecraft/lupin#6", "second-holder", **kw)


def test_renew_claim_fails_for_non_holder(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    claims.claim("gracecraft/lupin#6", "real-holder", **kw)
    assert claims.renew_claim("gracecraft/lupin#6", "impostor", **kw) is False


def test_renew_claim_succeeds_for_holder(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    claims.claim("gracecraft/lupin#6", "real-holder", ttl=0.2, **kw)
    assert claims.renew_claim("gracecraft/lupin#6", "real-holder", ttl=10, **kw) is True
    # Still there after the original (short) TTL would have expired.
    time.sleep(0.3)
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    assert raw.get("lupin:v1:claim:gracecraft/lupin#6") is not None


def test_release_claim_fails_for_non_holder(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    claims.claim("gracecraft/lupin#6", "real-holder", **kw)
    assert claims.release_claim("gracecraft/lupin#6", "impostor", **kw) is False
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    assert raw.get("lupin:v1:claim:gracecraft/lupin#6") is not None


def test_release_claim_succeeds_for_holder(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    claims.claim("gracecraft/lupin#6", "real-holder", **kw)
    assert claims.release_claim("gracecraft/lupin#6", "real-holder", **kw) is True
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    assert raw.get("lupin:v1:claim:gracecraft/lupin#6") is None
    # Releasing twice is not an error -- already gone is the end state anyway.
    assert claims.release_claim("gracecraft/lupin#6", "real-holder", **kw) is False


def test_claim_expires_after_its_ttl(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    claims.claim("gracecraft/lupin#6", "first-holder", ttl=0.05, **kw)
    time.sleep(0.2)
    # Once expired, a different holder can claim it clean.
    claims.claim("gracecraft/lupin#6", "second-holder", **kw)
    assert claims.renew_claim("gracecraft/lupin#6", "first-holder", **kw) is False


def test_claims_for_across_repos_mixed_claimed_and_unclaimed(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    claims.claim("gracecraft/lupin#6", "holder-a", **kw)
    claims.claim("gracecraft/nix#212", "holder-b", **kw)

    result = claims.claims_for(["gracecraft/lupin", "gracecraft/nix", "gracecraft/other"], **kw)

    assert set(result) == {"gracecraft/lupin#6", "gracecraft/nix#212"}
    assert result["gracecraft/lupin#6"]["session"] == "holder-a"
    assert result["gracecraft/nix#212"]["session"] == "holder-b"


def test_claims_for_only_returns_repos_asked_for(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    claims.claim("gracecraft/lupin#6", "holder-a", **kw)
    claims.claim("gracecraft/nix#212", "holder-b", **kw)

    result = claims.claims_for(["gracecraft/lupin"], **kw)
    assert set(result) == {"gracecraft/lupin#6"}


def test_claims_for_raises_when_a_claim_key_has_the_wrong_type(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    raw = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    raw.rpush("lupin:v1:claim:gracecraft/lupin#6", "not a claim")

    # A wrong-type key is a bug, not an empty claim. It must raise, not vanish.
    with pytest.raises(redis_lib.exceptions.ResponseError):
        claims.claims_for(["gracecraft/lupin"], **kw)


def test_claims_for_skips_a_claim_that_expired_after_the_scan(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    claims.claim("gracecraft/lupin#6", "holder-a", **kw)
    claims.claim("gracecraft/lupin#7", "holder-b", **kw)
    real = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)

    class ExpireAfterScan:
        """Scans as usual, then deletes #6 before the read, as if its TTL ran out."""

        def __getattr__(self, name):
            return getattr(real, name)

        def scan_iter(self, *args, **kwargs):
            keys = list(real.scan_iter(*args, **kwargs))
            real.delete("lupin:v1:claim:gracecraft/lupin#6")
            return iter(keys)

    monkeypatch.setattr(claims, "_client", lambda *_args: ExpireAfterScan())

    result = claims.claims_for(["gracecraft/lupin"], **kw)

    assert set(result) == {"gracecraft/lupin#7"}


def test_claims_for_retry_reads_every_key(redis_port, flush_redis, monkeypatch):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    claims.claim("gracecraft/lupin#6", "holder-a", **kw)
    claims.claim("gracecraft/lupin#7", "holder-b", **kw)
    real = redis_lib.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    dropped = []

    class DropFirstExecute:
        """Runs the first pipeline, then drops the reply as if the link broke."""

        def __getattr__(self, name):
            return getattr(real, name)

        def pipeline(self, **kwargs):
            pipe = real.pipeline(**kwargs)

            class Pipe:
                def __getattr__(self, name):
                    return getattr(pipe, name)

                def execute(self, *args, **kwargs):
                    replies = pipe.execute(*args, **kwargs)
                    if not dropped:
                        dropped.append(True)
                        raise redis_lib.exceptions.ConnectionError("reply lost")
                    return replies

            return Pipe()

    monkeypatch.setattr(claims, "_client", lambda *_args: DropFirstExecute())

    result = claims.claims_for(["gracecraft/lupin"], **kw)

    assert dropped == [True]
    assert set(result) == {"gracecraft/lupin#6", "gracecraft/lupin#7"}


def test_unreachable_redis_raises_for_every_claim_call(closed_port):
    kw = {"redis_host": "127.0.0.1", "redis_port": closed_port}
    with pytest.raises(slots.CoordinatorUnreachable):
        claims.claim("gracecraft/lupin#6", "a", **kw)
    with pytest.raises(slots.CoordinatorUnreachable):
        claims.renew_claim("gracecraft/lupin#6", "a", **kw)
    with pytest.raises(slots.CoordinatorUnreachable):
        claims.release_claim("gracecraft/lupin#6", "a", **kw)
    with pytest.raises(slots.CoordinatorUnreachable):
        claims.claims_for(["gracecraft/lupin"], **kw)


def test_bad_target_format_raises_value_error(redis_port, flush_redis):
    kw = {"redis_host": "127.0.0.1", "redis_port": redis_port}
    with pytest.raises(ValueError):
        claims.claim("not-a-valid-target", "a", **kw)


def test_cli_claim_renew_release_exit_codes(redis_port, flush_redis, capsys, monkeypatch):
    # This sandbox's shell sets LUPIN_REDIS_USERNAME for the real deployment
    # Redis; clear it so the CLI's --redis-username default doesn't try to
    # auth against the test's plain (no-ACL) redis-server fixture.
    monkeypatch.delenv("LUPIN_REDIS_USERNAME", raising=False)
    monkeypatch.delenv("LUPIN_REDIS_PASSWORD", raising=False)
    common = ["--redis-host", "127.0.0.1", "--redis-port", str(redis_port)]

    code = cli.main(["claim", "gracecraft/lupin#6", "--holder", "real-holder", *common])
    capsys.readouterr()
    assert code == 0

    code = cli.main(["claim", "gracecraft/lupin#6", "--holder", "other-holder", *common])
    capsys.readouterr()
    assert code == 2

    code = cli.main(["renew-claim", "gracecraft/lupin#6", "--holder", "impostor", *common])
    capsys.readouterr()
    assert code == 1

    code = cli.main(["renew-claim", "gracecraft/lupin#6", "--holder", "real-holder", *common])
    capsys.readouterr()
    assert code == 0

    code = cli.main(["release-claim", "gracecraft/lupin#6", "--holder", "impostor", *common])
    capsys.readouterr()
    assert code == 1

    code = cli.main(["release-claim", "gracecraft/lupin#6", "--holder", "real-holder", *common])
    captured = capsys.readouterr()
    assert code == 0, captured.err


def test_cli_claim_unreachable_redis_exits_3(closed_port, capsys):
    common = ["--redis-host", "127.0.0.1", "--redis-port", str(closed_port)]
    code = cli.main(["claim", "gracecraft/lupin#6", "--holder", "a", *common])
    captured = capsys.readouterr()
    assert code == 3
    assert "cannot reach the redis coordinator" in captured.err
