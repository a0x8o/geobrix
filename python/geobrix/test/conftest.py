"""Test-root conftest: dependency-aware collection guard for the bench suite.

WHY: The benchmark tests under ``test/bench`` are on-demand only. They import
the bench package (``databricks.labs.gbx.bench.*``), whose import chain pulls in
heavy/lightweight deps -- ``rasterio``, ``shapely``, ``h3``, ``quadbin``,
``numpy``, ``scipy`` -- that are NOT installed in the remote heavyweight CI
Python environment (``requirements-ci.txt`` ships none of them; pyrx-only CI uses
``requirements-pyrx-ci.txt``). pytest imports a test module at COLLECTION time to
read it, so a bare ``-m "not bench"`` marker filter does not help: the import
(and its ``ModuleNotFoundError``) fires before the marker is ever seen, turning
into a collection ERROR that fails the build.

A directory-level ``collect_ignore`` prevents pytest from even importing
``test/bench`` when the deps are absent. It is gated on ``rasterio`` being
importable (the canonical signal for the pyrx/bench dependency set):

  * Remote heavyweight CI (no rasterio)  -> ``collect_ignore = ["bench"]`` -> skipped.
  * Local / Docker / pyrx CI (rasterio present) -> not ignored -> collected and run.
  * Explicit ``gbx:test:python --path test/bench/...`` in Docker -> deps present,
    so nothing is ignored and the targeted bench tests still run.

This is robust to the real cause (missing deps) rather than relying on each
caller remembering to pass ``--ignore=test/bench``.
"""

import importlib.util

# Skip the on-demand bench suite when its dependencies are not installed.
if importlib.util.find_spec("rasterio") is None:
    collect_ignore = ["bench"]
