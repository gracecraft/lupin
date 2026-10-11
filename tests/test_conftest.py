"""Checks for the autouse clean_lupin_env fixture in conftest.py."""

import os


def test_clean_lupin_env_leaves_only_local_backend():
    lupin_names = sorted(name for name in os.environ if name.startswith("LUPIN_"))
    assert lupin_names == ["LUPIN_BACKEND"]
    assert os.environ["LUPIN_BACKEND"] == "local"
