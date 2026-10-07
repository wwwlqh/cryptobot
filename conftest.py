"""Root conftest: keep pytest's temp tree inside the repo.

The default temp root, ``%LOCALAPPDATA%\\Temp\\pytest-of-<user>``, is ACL-locked
on this box (it cannot even be listed), so every ``tmp_path`` fixture died with
``PermissionError: [WinError 5]`` and ~17 tests never ran.

``PYTEST_DEBUG_TEMPROOT`` moves only the ROOT; pytest still creates its normal
``pytest-of-<user>/pytest-N`` numbered dirs underneath, with locking and
rotation — so unlike a fixed ``--basetemp`` (which pytest CLEARS at session
start), concurrent pytest sessions do not wipe each other's temp dirs. This
repo does get concurrent sessions, so that distinction has bitten before:
a fixed basetemp produced 170 spurious errors across two parallel runs.

The tree is disposable and gitignored (``.pytest_tmp/``); pytest's own rotation
keeps only the 3 most recent runs.
"""
import os
from pathlib import Path

_TEMPROOT = Path(__file__).parent / ".pytest_tmp"
_TEMPROOT.mkdir(exist_ok=True)
os.environ.setdefault("PYTEST_DEBUG_TEMPROOT", str(_TEMPROOT))
