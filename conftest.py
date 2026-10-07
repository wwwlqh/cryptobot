"""Root conftest: keep pytest's temp tree inside the repo.

On some Windows machines the default temp root
(``%LOCALAPPDATA%\\Temp\\pytest-of-<user>``) is not accessible, which makes
every ``tmp_path`` fixture fail with ``PermissionError``.

``PYTEST_DEBUG_TEMPROOT`` moves only the root; pytest still creates numbered
``pytest-of-<user>/pytest-N`` directories under it with locking and rotation,
so unlike a fixed ``--basetemp`` (cleared at session start) two concurrent
runs do not wipe each other's files. The tree is gitignored (``.pytest_tmp/``).
"""
import os
from pathlib import Path

_TEMPROOT = Path(__file__).parent / ".pytest_tmp"
_TEMPROOT.mkdir(exist_ok=True)
os.environ.setdefault("PYTEST_DEBUG_TEMPROOT", str(_TEMPROOT))
