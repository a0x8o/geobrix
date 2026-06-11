"""Ensure the third-party `pmtiles` package is importable in this directory.

pytest's default `prepend` importmode re-inserts the test root (`test/`) at
sys.path[0] when it imports each test module.  The project has a `test/pmtiles/`
package that shadows the real `pmtiles` PyPI package, so bare
`from pmtiles.tile import ...` picks up the wrong package.

Fix: pre-import `pmtiles` (and key sub-modules) here at conftest load time,
before pytest re-inserts the test root for `test_header.py`.  Once the real
package is cached in `sys.modules`, the subsequent re-insertion of the test
root cannot shadow it.
"""

import sys
from pathlib import Path

_TEST_ROOT = str(Path(__file__).resolve().parents[2])  # .../python/geobrix/test

# Temporarily remove the test root so we import the real pmtiles from site-packages.
_saved = [p for p in sys.path if p == _TEST_ROOT]
sys.path = [p for p in sys.path if p != _TEST_ROOT]

# Pre-import pmtiles and its sub-modules into sys.modules.
import pmtiles  # noqa: E402
import pmtiles.tile  # noqa: E402
import pmtiles.reader  # noqa: E402
import pmtiles.writer  # noqa: E402

# Restore the test root (pytest relies on it for other tests).
sys.path = _saved + sys.path
