"""Convenience CLI for the 60-second smoke verification.

Default mode: boots `python -m cryptobot`, opens the dashboard URL in a
browser, and prints reminders for what the operator should watch.

Headless mode (``--headless``): boots the scanner for ``--timeout``
seconds, polls ``GET /events`` to verify at least one SSE tick lands,
then terminates with exit code 0 (PASS) or 1 (FAIL). Suitable for CI
and agent invocation — no browser, no human-only Ctrl-C loop (review
finding #53).

Run from repo root:
    python tools/smoke_check.py
    python tools/smoke_check.py --headless --timeout 30
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import webbrowser
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen


URL = "http://127.0.0.1:8800/index.html"
EVENTS_URL = "http://127.0.0.1:8800/events"

#: THREE local src roots for FOUR packages: ``polymarket/src`` holds the shared
#: engine AND the Polymarket adapter (one src per venue since 2026-08-04), and
#: the other two venues keep theirs. The scanner reaches its venue lazily through
#: ``cryptobot.venues.registry``, so a single-root PYTHONPATH imports core fine
#: and then dies with a ``ModuleNotFoundError`` at the first adapter use, well
#: after this script has reported "booting". Mirrors the ``PY_SRC_PATH`` list in
#: ``polymarket/infra/deploy.sh`` — keep the two in step.
SRC_ROOTS: tuple[tuple[str, ...], ...] = (
    ("polymarket", "src"),
    ("predictfun", "live", "src"),
    ("kalshi", "live", "src"),
)


def _py_src_path(repo: Path, inherited: str = "") -> str:
    """PYTHONPATH for a subprocess that imports core AND any venue package.

    ``os.pathsep`` IS the separator deploy.sh derives from ``uname`` by hand
    (';' on Windows, ':' elsewhere): getting it wrong fails SILENTLY, because a
    ':'-joined list arrives at a Windows interpreter as one nonexistent
    directory. Python knows its own platform, so no case statement is needed.
    """
    parts = [str(repo.joinpath(*root)) for root in SRC_ROOTS]
    if inherited:
        parts.append(inherited)
    return os.pathsep.join(parts)


def _poll_for_first_tick(timeout_s: float) -> bool:
    """Return True once at least one SSE event arrives, False on timeout."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            resp = urlopen(EVENTS_URL, timeout=2.0)
            try:
                # Read enough bytes to be sure we got past `: connected\n\n`
                # and have a real `event: tick` frame.
                buf = b""
                end = time.monotonic() + 5.0
                while time.monotonic() < end:
                    chunk = resp.read(1024)
                    if not chunk:
                        break
                    buf += chunk
                    if b"event: tick" in buf:
                        return True
            finally:
                resp.close()
        except (URLError, OSError):
            time.sleep(0.5)
            continue
    return False


def main():
    parser = argparse.ArgumentParser(description="cryptobot smoke check")
    parser.add_argument(
        "--headless", action="store_true",
        help="non-interactive: poll /events, exit 0/1 instead of waiting on Ctrl-C",
    )
    parser.add_argument(
        "--timeout", type=int, default=60,
        help="seconds to wait for a tick before failing (headless mode)",
    )
    args = parser.parse_args()

    repo = Path(__file__).resolve().parent.parent
    print(f"booting cryptobot scanner from {repo}")
    # The packages live under several src roots after the 2026-07-28 restructure;
    # pass them all on PYTHONPATH so this works without an editable install.
    env = dict(os.environ)
    env["PYTHONPATH"] = _py_src_path(repo, env.get("PYTHONPATH", ""))
    proc = subprocess.Popen(
        [sys.executable, "-m", "cryptobot"],
        cwd=str(repo),
        env=env,
    )
    try:
        if args.headless:
            print(f"polling {EVENTS_URL} for first tick (timeout={args.timeout}s)")
            ok = _poll_for_first_tick(timeout_s=float(args.timeout))
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            if ok:
                print("PASS — SSE tick observed")
                return 0
            print("FAIL — no SSE tick within timeout")
            return 1

        time.sleep(2.0)
        print(f"opening dashboard: {URL}")
        try:
            webbrowser.open(URL)
        except Exception:
            pass
        print()
        print("watch the dashboard for ~60s and verify:")
        print("  [ ] StatStrip updates every second")
        print("  [ ] MarketGrid shows per-sleeve sub-rows under each coin")
        print("  [ ] LedgerTable shows PENDING rows landing on entry-minute ticks")
        print("  [ ] GatePanel skip counts move in real time")
        print("  [ ] Exposure tile shows current usage vs cap")
        print()
        print("press Ctrl-C to stop the scanner")
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())
