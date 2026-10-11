"""Code shared by every slot-lease backend.

Both `slots.py` (`local`) and `slots_redis.py` (`redis`) need the same two
things: a way to split a lease id back into its slot and token, and a way to
run a subprocess while renewing a lease, releasing it no matter how the
subprocess ends. Keeping these here means a new backend does not have to
copy the subprocess/signal-handling code to get `hold` right.
"""

from __future__ import annotations

import contextlib
import signal
import subprocess
import sys
import threading
from typing import Callable


def split_lease(lease: str) -> tuple[str, str]:
    """Split a lease id like `"bmo:abc123"` into `("bmo", "abc123")`."""
    slot, sep, token = lease.partition(":")
    if not sep or not slot or not token:
        raise ValueError(f"malformed lease id: {lease!r}")
    return slot, token


def run_with_lease(
    command: list[str],
    lease: str,
    *,
    ttl: float,
    renew: Callable[[str], bool],
    release: Callable[[str], bool],
) -> int:
    """Run `command`, renewing `lease` on a timer, releasing it on exit.

    `renew`/`release` take the lease id and do the backend-specific work.
    Returns the child's exit code, or 128 + signal number if a signal killed
    it. See `slots.hold`'s docstring for why release always runs and why
    SIGTERM forwarding only applies on the main thread.
    """
    renew_interval = max(ttl / 3, 0.1)
    stop = threading.Event()

    def _renew_loop() -> None:
        failing = False
        while not stop.wait(renew_interval):
            try:
                renew(lease)
                failing = False
            except Exception as exc:
                # Keep renewing. Print only the first failure after a success.
                if not failing:
                    print(f"lupin: lease {lease} not renewed. {exc}", file=sys.stderr)
                failing = True

    renewer = threading.Thread(target=_renew_loop, daemon=True)
    renewer.start()

    proc = subprocess.Popen(command)

    def _forward_sigterm(signum: int, _frame: object) -> None:
        with contextlib.suppress(ProcessLookupError):
            proc.send_signal(signum)

    previous_handler = None
    if threading.current_thread() is threading.main_thread():
        previous_handler = signal.signal(signal.SIGTERM, _forward_sigterm)
    try:
        returncode = proc.wait()
    finally:
        stop.set()
        renewer.join(timeout=renew_interval + 1)
        if previous_handler is not None:
            signal.signal(signal.SIGTERM, previous_handler)
        release(lease)

    if returncode < 0:
        return 128 - returncode
    return returncode
