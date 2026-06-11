"""Test-root conftest: dependency-aware collection guard for the light-tier suites.

WHY: The benchmark tests under ``test/bench`` and the lightweight DataSource tests
under ``test/ds`` import packages (``databricks.labs.gbx.bench.*`` /
``databricks.labs.gbx.ds.*``) whose import chain pulls in light-tier deps --
``rasterio``, ``shapely``, ``h3``, ``quadbin``, ``pmtiles``, ``numpy``, ``scipy``
-- that are NOT installed in the remote heavyweight CI Python environment
(``requirements-ci.txt`` ships none of them; the lightweight CI uses
``requirements-pyrx-ci.txt``, and the heavy job runs with ``--ignore=test/pyrx``).
pytest imports a test module at COLLECTION time to read it, so a bare
``-m "not bench"`` marker filter does not help: the import (and its
``ModuleNotFoundError``) fires before the marker is ever seen, turning into a
collection ERROR that fails the build.

A directory-level ``collect_ignore`` prevents pytest from even importing those
dirs when the deps are absent. It is gated on ``rasterio`` being importable (the
canonical signal for the light-tier dependency set; ``pmtiles`` et al. ship in the
same locks):

  * Remote heavyweight CI (no rasterio) -> ``collect_ignore = ["bench", "ds"]`` -> skipped.
  * Local / Docker / pyrx CI (rasterio present) -> not ignored -> collected and run.
  * Explicit ``gbx:test:python --path test/ds/...`` in Docker -> deps present,
    so nothing is ignored and the targeted tests still run.

This is robust to the real cause (missing deps) rather than relying on each
caller remembering to pass ``--ignore``.
"""

import importlib.util

# Skip the light-tier suites when their dependencies are not installed.
if importlib.util.find_spec("rasterio") is None:
    collect_ignore = ["bench", "ds"]
