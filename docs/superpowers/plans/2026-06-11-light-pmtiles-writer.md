# Light Tiled-Output Framework + PMTiles Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a pure-Python/PySpark DataSource V2 PMTiles writer (`pmtiles_gbx`) on a new, shared, tier-neutral tiled-output framework at `databricks.labs.gbx.ds.tiles`, with distributed spatial sharding (fixed + adaptive), a separate overview archive, and a GeoJSON/STAC catalog.

**Architecture:** A new tier-neutral package `databricks.labs.gbx.ds` (migrated from `pyrx/ds`) hosts the framework: `grid.py` (SlippyGrid tile math), `_header.py` (PMTiles header/sniff), `backend.py` (PMTilesBackend assembly), `catalog.py` (STAC/TileJSON), `shard.py` (entries-driven scratch + shard assignment). The `pmtiles_gbx` DataSource streams per-partition indexed scratch in `write()` and does all shard assignment + assembly from entries metadata in `commit()` — tile bytes never land on the driver, which is what enables both fixed `shardZoom` and adaptive `targetTilesPerShard` sharding and centralizes assembly (one writer per file, no overview write race).

**Tech Stack:** Python 3.12, PySpark 4.x DataSource V2 (`pyspark.sql.datasource`), the Protomaps `pmtiles` PyPI package (`Writer`, `zxy_to_tileid`, `TileType`, `Compression`, `Reader`), pure-Python `os`/`open` I/O (Serverless-safe). Tests with pytest; local Spark for round-trips; Docker for doc-tests + heavy parity.

**Reference spec:** `docs/superpowers/specs/2026-06-11-light-pmtiles-writer-design.md`

---

## Repo conventions the engineer must know

- **Light tier is Serverless-safe:** product code under `pyrx/` and the new `ds/` must NEVER use `._jvm`, `._jsc`, `.sparkContext`, `.rdd`, `.conf.set(`, `SparkConf`, `.setConf(`. A guard test enforces this (Task 3 extends it). The `bench/` package is exempt (harness-only) and is NOT scanned.
- **Namespace packages:** `databricks/`, `databricks/labs/`, `databricks/labs/gbx/` have **no** `__init__.py` (PEP 420). `pyrx/` and `pyrx/ds/` DO have `__init__.py`. The new `gbx/ds/` and `gbx/ds/tiles/` get `__init__.py`.
- **The canonical tile struct** is `databricks.labs.gbx.pyrx._serde.TILE_SCHEMA` = `(cellid: long, raster: binary, metadata: map<string,string>)`. `_serde`, `_env`, and `core.tiling` **stay in `pyrx`** — the migrated `ds/` code keeps importing them from `databricks.labs.gbx.pyrx`.
- **Beta, no aliases:** the migration is a clean move — `pyrx/ds/` is deleted, no compat shim.
- **Commits:** before each commit run `chmod -R u+rwX .git/objects` (this env drops the execute bit on git object dirs). Commit message trailer is `Co-authored-by: Isaac` (never a human name). Keep subjects ≤72 chars with a WHY body for non-trivial commits.
- **Where tests live:** Python unit/integration tests under `python/geobrix/test/...`; doc-test example code under `docs/tests/python/...` (only runs in Docker); doc pages under `docs/docs/...`.
- **Run Python tests** from repo root with the project venv: `python/geobrix/.venv-pyrx/bin/python -m pytest <path> -v` (or `gbx:test:python --path <rel-to-python/geobrix/test>`). Framework unit tests need no Spark and no Docker; round-trip tests use a local SparkSession; heavy-parity + doc-tests run in Docker via `gbx:test:python-docs`.

---

## File Structure

**New framework package** `python/geobrix/src/databricks/labs/gbx/ds/`:
- `__init__.py` — package doc + auto-register on import (mirrors `pyrx/ds/__init__.py`).
- `register.py` — `register(spark)` registers ALL light DataSources (raster_gbx, gtiff_gbx, pmtiles_gbx).
- `raster.py`, `gtiff.py`, `writer.py`, `_write.py`, `_encode.py`, `_listing.py` — **migrated** from `pyrx/ds/` unchanged except import prefix.
- `pmtiles.py` — `PMTilesGbxDataSource` (write-only) + `PMTilesGbxWriter` + `PMTilesCommitMessage`.
- `tiles/__init__.py` — empty package marker.
- `tiles/grid.py` — `Grid` protocol + `SlippyGrid` (web-mercator XYZ tile math).
- `tiles/_header.py` — `sniff_tile_type`, `HeaderInfo`, `build_header_info`.
- `tiles/backend.py` — `TileArchiveBackend` protocol + `PMTilesBackend`.
- `tiles/catalog.py` — `CatalogWriter` protocol, `ShardInfo`, `STACManifestCatalog`, `TileJSONCatalog`.
- `tiles/shard.py` — `Entry`, `ScratchWriter`, `read_entries`, `assign_shards`, `stream_sorted`.

**New/moved tests** `python/geobrix/test/ds/`:
- `conftest.py` — session-scoped local `spark` fixture.
- migrated `test_register.py`, `test_raster_datasource.py`, `test_gtiff_datasource.py`, `test_writer.py`, `test_encode.py`, `test_listing.py`, `test_write_helper.py`, `test_reader_parity.py`, `test_writer_parity.py`, `test_serverless_no_spark_config.py`.
- new `test_pmtiles.py` (round-trip single + sharded + overview + adaptive).
- new `test_pmtiles_parity.py` (Docker, skip-if-heavy-unavailable).
- `tiles/test_grid.py`, `tiles/test_header.py`, `tiles/test_backend.py`, `tiles/test_catalog.py`, `tiles/test_shard.py`.

**Docs:** `docs/docs/writers/pmtiles_gbx.mdx`, `docs/tests/python/writers/pmtiles_gbx_examples.py` + `test_pmtiles_gbx_examples.py`, `docs/sidebars.js`, `docs/docs/writers/overview.mdx`.

**Config:** `python/geobrix/pyproject.toml` (`[light]` extra), `python/geobrix/src/databricks/labs/gbx/bench/readers.py` (register import + pmtiles path).

---

### Task 1: Add `pmtiles` dependency to the `[light]` extra

**Files:**
- Modify: `python/geobrix/pyproject.toml` (the `light = [...]` block)

- [ ] **Step 1: Add the dependency**

In `python/geobrix/pyproject.toml`, inside the `light = [` list, add the `pmtiles` line next to `quadbin`:

```toml
    "h3>=4.0,<5",
    "quadbin>=0.2,<0.3",
    # Protomaps PMTiles archive writer/reader (pure-Python) for the pmtiles_gbx
    # tiled-output backend. Writer needs ascending tileid; Reader used in tests.
    "pmtiles>=3.4,<4",
    "scikit-image>=0.22,<1",
```

- [ ] **Step 2: Install into the project venv and verify import**

Run:
```bash
python/geobrix/.venv-pyrx/bin/pip install -e "python/geobrix[light]" >/dev/null 2>&1
python/geobrix/.venv-pyrx/bin/python -c "from pmtiles.writer import Writer; from pmtiles.tile import zxy_to_tileid, TileType, Compression; from pmtiles.reader import Reader, MemorySource; print('pmtiles OK')"
```
Expected: `pmtiles OK`

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/pyproject.toml
git commit -m "build(light): add pmtiles dependency for pmtiles_gbx backend

Co-authored-by: Isaac"
```

---

### Task 2: Migrate `pyrx/ds/` → `gbx/ds/` (move files, fix internal imports)

This is the precursor step from the spec. Move the seven DataSource modules + the package init + tests into the tier-neutral package. `_serde`/`_env`/`core` stay in `pyrx`.

**Files:**
- Move: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/{__init__,register,raster,gtiff,writer,_write,_encode,_listing}.py` → `python/geobrix/src/databricks/labs/gbx/ds/`
- Move: `python/geobrix/test/pyrx/ds/*` → `python/geobrix/test/ds/`

- [ ] **Step 1: Create the new package dir and move source files with git**

Run:
```bash
mkdir -p python/geobrix/src/databricks/labs/gbx/ds
git mv python/geobrix/src/databricks/labs/gbx/pyrx/ds/__init__.py   python/geobrix/src/databricks/labs/gbx/ds/__init__.py
git mv python/geobrix/src/databricks/labs/gbx/pyrx/ds/register.py   python/geobrix/src/databricks/labs/gbx/ds/register.py
git mv python/geobrix/src/databricks/labs/gbx/pyrx/ds/raster.py     python/geobrix/src/databricks/labs/gbx/ds/raster.py
git mv python/geobrix/src/databricks/labs/gbx/pyrx/ds/gtiff.py      python/geobrix/src/databricks/labs/gbx/ds/gtiff.py
git mv python/geobrix/src/databricks/labs/gbx/pyrx/ds/writer.py     python/geobrix/src/databricks/labs/gbx/ds/writer.py
git mv python/geobrix/src/databricks/labs/gbx/pyrx/ds/_write.py     python/geobrix/src/databricks/labs/gbx/ds/_write.py
git mv python/geobrix/src/databricks/labs/gbx/pyrx/ds/_encode.py    python/geobrix/src/databricks/labs/gbx/ds/_encode.py
git mv python/geobrix/src/databricks/labs/gbx/pyrx/ds/_listing.py   python/geobrix/src/databricks/labs/gbx/ds/_listing.py
rmdir python/geobrix/src/databricks/labs/gbx/pyrx/ds 2>/dev/null || true
```

- [ ] **Step 2: Fix the import prefix in the moved source files**

Only the `pyrx.ds` self-reference changes; `pyrx._serde` / `pyrx._env` / `pyrx.core` stay. In every moved file, replace the package-internal prefix `databricks.labs.gbx.pyrx.ds` with `databricks.labs.gbx.ds`:

```bash
grep -rl "databricks.labs.gbx.pyrx.ds" python/geobrix/src/databricks/labs/gbx/ds/ \
  | xargs sed -i '' 's/databricks\.labs\.gbx\.pyrx\.ds/databricks.labs.gbx.ds/g'
```
(On Linux/Docker use `sed -i` without the `''`.)

Then verify the only remaining `pyrx` references in the moved files are the legitimate `_serde`/`_env`/`core` ones:
```bash
grep -rn "gbx.pyrx" python/geobrix/src/databricks/labs/gbx/ds/
```
Expected: only lines importing `databricks.labs.gbx.pyrx._serde`, `...pyrx._env`, or `...pyrx.core...` — no `pyrx.ds`.

- [ ] **Step 3: Move the tests and fix their import prefix**

Run:
```bash
mkdir -p python/geobrix/test/ds
git mv python/geobrix/test/pyrx/ds/* python/geobrix/test/ds/
rmdir python/geobrix/test/pyrx/ds 2>/dev/null || true
grep -rl "databricks.labs.gbx.pyrx.ds" python/geobrix/test/ds/ \
  | xargs sed -i '' 's/databricks\.labs\.gbx\.pyrx\.ds/databricks.labs.gbx.ds/g'
```

- [ ] **Step 4: Add a local Spark fixture for the ds test dir**

Create `python/geobrix/test/ds/conftest.py` (only if one was not moved in Step 3; if `conftest.py` already exists there, skip this step):

```python
"""Shared fixtures for the gbx.ds DataSource tests."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-ds-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()
```

- [ ] **Step 5: Run the migrated ds suite**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds -v -p no:cacheprovider`
Expected: PASS for all migrated tests **except** `test_serverless_no_spark_config.py`, which still roots its scan at `pyrx` and asserts the ds files live under pyrx — that test is fixed in Task 3. (If it fails on the file-list assertion, that is expected here.)

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects
git add -A python/geobrix/src/databricks/labs/gbx/ds python/geobrix/test/ds python/geobrix/src/databricks/labs/gbx/pyrx
git commit -m "refactor(ds): migrate pyrx/ds raster reader+writer to tier-neutral gbx/ds

Precursor for the tiled-output framework: the DataSource package is no
longer raster-specific. _serde/_env/core stay in pyrx and are imported
from there. Serverless guard + external refs updated in the next commit.

Co-authored-by: Isaac"
```

---

### Task 3: Re-point external references (bench, doc-tests, Serverless guard, sidebars)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/readers.py` (3 import sites)
- Modify: `python/geobrix/test/ds/test_serverless_no_spark_config.py`
- Modify: `docs/tests/python/{readers,writers}/*` (register imports in example code)
- Modify: `docs/docs/{readers,writers}/*.mdx` (any register-note text referencing `pyrx.ds.register`)

- [ ] **Step 1: Update bench readers.py imports**

In `python/geobrix/src/databricks/labs/gbx/bench/readers.py`, replace all three occurrences of:
```python
from databricks.labs.gbx.pyrx.ds.register import register
```
with:
```python
from databricks.labs.gbx.ds.register import register
```

- [ ] **Step 2: Update doc-test example register imports**

Replace the register import in the reader/writer example code so doc-tests still import a valid module:
```bash
grep -rl "databricks.labs.gbx.pyrx.ds.register" docs/tests docs/docs \
  | xargs sed -i '' 's/databricks\.labs\.gbx\.pyrx\.ds\.register/databricks.labs.gbx.ds.register/g'
grep -rl "databricks.labs.gbx.pyrx.ds" docs/tests docs/docs \
  | xargs sed -i '' 's/databricks\.labs\.gbx\.pyrx\.ds/databricks.labs.gbx.ds/g'
```
Then verify nothing in docs still references the old path:
```bash
grep -rn "pyrx.ds" docs/ ; echo "exit:$?"
```
Expected: no matches (`exit:1` from grep).

- [ ] **Step 3: Rewrite the Serverless guard to scan pyrx AND gbx/ds**

Replace the body of `python/geobrix/test/ds/test_serverless_no_spark_config.py` with a two-root scan (pyrx product code stays scanned; the migrated ds code is now a second root; `bench/` stays excluded):

```python
"""Serverless safety guard: light product code must not mutate Spark config
or reach the JVM bridge. Scans pyrx + gbx.ds (NOT bench, which is harness-only)."""

import re
from pathlib import Path

import databricks.labs.gbx.ds as gbx_ds
import databricks.labs.gbx.pyrx as pyrx

_FORBIDDEN = {
    "spark config mutation": re.compile(r"\.conf\.set\s*\("),
    "SparkConf": re.compile(r"\bSparkConf\b"),
    "setConf": re.compile(r"\.setConf\s*\("),
    "setSystemProperty": re.compile(r"\bsetSystemProperty\b"),
    "JVM bridge (_jvm)": re.compile(r"\._jvm\b"),
    "JVM bridge (_jsc)": re.compile(r"\._jsc\b"),
    "sparkContext access": re.compile(r"\.sparkContext\b"),
    "RDD API": re.compile(r"\.rdd\b"),
}

_ROOTS = (
    Path(pyrx.__file__).resolve().parent,
    Path(gbx_ds.__file__).resolve().parent,
)


def _source_files():
    for root in _ROOTS:
        for p in root.rglob("*.py"):
            if "__pycache__" in p.parts:
                continue
            yield p


def test_light_product_never_mutates_spark_config_or_uses_jvm_bridge():
    violations = []
    for path in _source_files():
        for i, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("#", 1)[0]
            for label, pat in _FORBIDDEN.items():
                if pat.search(code):
                    violations.append(f"{path.name}:{i} [{label}] -> {line.strip()}")
    assert not violations, (
        "light product code must be Serverless-safe. Found:\n  "
        + "\n  ".join(violations)
    )


def test_serverless_scan_includes_ds_modules():
    """The migrated DataSource modules must be in scope of the scan."""
    files = {p.name for p in _source_files()}
    for required in (
        "raster.py",
        "gtiff.py",
        "writer.py",
        "_write.py",
        "register.py",
        "_encode.py",
        "_listing.py",
    ):
        assert required in files, f"{required} not covered by Serverless scan"
```

- [ ] **Step 4: Run the guard + a bench import smoke check**

Run:
```bash
python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/test_serverless_no_spark_config.py -v -p no:cacheprovider
python/geobrix/.venv-pyrx/bin/python -c "import databricks.labs.gbx.bench.readers; print('bench import OK')"
```
Expected: both guard tests PASS; `bench import OK`.

- [ ] **Step 5: Fix the sidebars register-note + commit**

Search `docs/sidebars.js` and any `docs/docs/**/*.mdx` "register" notes for the string `pyrx.ds` and update to `ds` (the Step 2 grep already covered `.mdx`; confirm sidebars):
```bash
grep -rn "pyrx.ds" docs/sidebars.js ; echo "exit:$?"
```
Expected: no matches.

```bash
chmod -R u+rwX .git/objects
git add -A python/geobrix/src/databricks/labs/gbx/bench python/geobrix/test/ds docs
git commit -m "refactor(ds): re-point bench, doc-tests, and Serverless guard to gbx.ds

Bench register imports, doc-test register imports + mdx notes now use
databricks.labs.gbx.ds.register. Serverless guard scans pyrx + gbx.ds
(bench stays excluded as harness-only).

Co-authored-by: Isaac"
```

---

### Task 4: `tiles/grid.py` — Grid protocol + SlippyGrid

Pure-Python web-mercator tile math. No Spark, no Docker.

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/tiles/__init__.py`
- Create: `python/geobrix/src/databricks/labs/gbx/ds/tiles/grid.py`
- Test: `python/geobrix/test/ds/tiles/test_grid.py`

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/ds/tiles/__init__.py` (empty) and `python/geobrix/test/ds/tiles/test_grid.py`:

```python
import math

from databricks.labs.gbx.ds.tiles.grid import SlippyGrid


def test_tile_bbox_world_at_zoom0():
    g = SlippyGrid()
    minlon, minlat, maxlon, maxlat = g.tile_bbox(0, 0, 0)
    assert minlon == -180.0
    assert maxlon == 180.0
    # web-mercator clamps latitude near +/-85.0511
    assert math.isclose(maxlat, 85.0511, abs_tol=1e-3)
    assert math.isclose(minlat, -85.0511, abs_tol=1e-3)


def test_tile_bbox_ordering_and_quadrant():
    g = SlippyGrid()
    # z1 tile (1,0,0) is the NW quadrant: lon [-180,0], lat [0, ~85]
    minlon, minlat, maxlon, maxlat = g.tile_bbox(1, 0, 0)
    assert (minlon, maxlon) == (-180.0, 0.0)
    assert minlat >= -0.001 and maxlat > minlat


def test_parent_clamps_and_shifts():
    g = SlippyGrid()
    # a z8 tile's parent at shard zoom 6 drops 2 bits
    assert g.parent(8, 130, 85, 6) == (6, 130 >> 2, 85 >> 2)
    # parent at a zoom deeper than the tile clamps to the tile itself
    assert g.parent(4, 3, 5, 6) == (4, 3, 5)
    # parent at the same zoom is identity
    assert g.parent(6, 12, 7, 6) == (6, 12, 7)


def test_tiles_for_bbox_covers_point():
    g = SlippyGrid()
    # London ~ (-0.12, 51.5) at zoom 6 -> a single covering tile
    tiles = list(g.tiles_for_bbox((-0.13, 51.49, -0.11, 51.51), 6))
    assert len(tiles) >= 1
    for z, x, y in tiles:
        bb = g.tile_bbox(z, x, y)
        assert bb[0] <= -0.12 <= bb[2]


def test_buffered_bbox_expands():
    g = SlippyGrid()
    base = g.tile_bbox(6, 32, 21)
    buf = g.buffered_bbox(6, 32, 21, 0.25)
    assert buf[0] < base[0] and buf[2] > base[2]
    assert buf[1] < base[1] and buf[3] > base[3]
```

- [ ] **Step 2: Run it to verify failure**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/tiles/test_grid.py -v -p no:cacheprovider`
Expected: FAIL — `ModuleNotFoundError: ... ds.tiles.grid`.

- [ ] **Step 3: Implement grid.py**

Create `python/geobrix/src/databricks/labs/gbx/ds/tiles/__init__.py`:
```python
"""gbx.ds.tiles — shared lightweight tiled-output framework (grid, sharding,
catalog, archive backends)."""
```

Create `python/geobrix/src/databricks/labs/gbx/ds/tiles/grid.py`:
```python
"""Grid-pluggable tile math. ``SlippyGrid`` is the web-mercator XYZ grid used by
PMTiles; future backends (COG-by-quadbin) add their own ``Grid`` implementation."""

from __future__ import annotations

import math
from typing import Iterable, Protocol, Tuple, runtime_checkable

BBox = Tuple[float, float, float, float]  # (minlon, minlat, maxlon, maxlat)
TileKey = Tuple[int, int, int]  # (z, x, y)


@runtime_checkable
class Grid(Protocol):
    """The minimal tile math every tiled-output backend needs."""

    def tile_bbox(self, z: int, x: int, y: int) -> BBox: ...

    def parent(self, z: int, x: int, y: int, shard_zoom: int) -> TileKey: ...

    def tiles_for_bbox(self, bbox: BBox, zoom: int) -> Iterable[TileKey]: ...

    def buffered_bbox(self, z: int, x: int, y: int, buffer: float) -> BBox: ...


class SlippyGrid:
    """Web-mercator slippy-map (XYZ) grid."""

    def tile_bbox(self, z: int, x: int, y: int) -> BBox:
        n = 2 ** z
        minlon = x / n * 360.0 - 180.0
        maxlon = (x + 1) / n * 360.0 - 180.0
        lat_top = self._lat(y, n)
        lat_bot = self._lat(y + 1, n)
        return (minlon, min(lat_top, lat_bot), maxlon, max(lat_top, lat_bot))

    @staticmethod
    def _lat(y: int, n: int) -> float:
        return math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))

    def parent(self, z: int, x: int, y: int, shard_zoom: int) -> TileKey:
        sz = min(shard_zoom, z)
        shift = z - sz
        return (sz, x >> shift, y >> shift)

    def tiles_for_bbox(self, bbox: BBox, zoom: int) -> Iterable[TileKey]:
        minlon, minlat, maxlon, maxlat = bbox
        n = 2 ** zoom
        x0 = int((minlon + 180.0) / 360.0 * n)
        x1 = int((maxlon + 180.0) / 360.0 * n)
        y0 = self._lat_to_y(maxlat, n)
        y1 = self._lat_to_y(minlat, n)
        for x in range(max(0, x0), min(n - 1, x1) + 1):
            for y in range(max(0, y0), min(n - 1, y1) + 1):
                yield (zoom, x, y)

    @staticmethod
    def _lat_to_y(lat: float, n: int) -> int:
        lat = max(min(lat, 85.05112878), -85.05112878)
        rad = math.radians(lat)
        return int((1.0 - math.asinh(math.tan(rad)) / math.pi) / 2.0 * n)

    def buffered_bbox(self, z: int, x: int, y: int, buffer: float) -> BBox:
        minlon, minlat, maxlon, maxlat = self.tile_bbox(z, x, y)
        dx = (maxlon - minlon) * buffer
        dy = (maxlat - minlat) * buffer
        return (minlon - dx, minlat - dy, maxlon + dx, maxlat + dy)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/tiles/test_grid.py -v -p no:cacheprovider`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/ds/tiles/__init__.py \
        python/geobrix/src/databricks/labs/gbx/ds/tiles/grid.py \
        python/geobrix/test/ds/tiles/__init__.py \
        python/geobrix/test/ds/tiles/test_grid.py
git commit -m "feat(ds/tiles): add Grid protocol + SlippyGrid web-mercator tile math

Co-authored-by: Isaac"
```

---

### Task 5: `tiles/_header.py` — tile-type sniff + PMTiles header assembly

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/tiles/_header.py`
- Test: `python/geobrix/test/ds/tiles/test_header.py`

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/ds/tiles/test_header.py`:
```python
from pmtiles.tile import TileType, Compression

from databricks.labs.gbx.ds.tiles.grid import SlippyGrid
from databricks.labs.gbx.ds.tiles._header import (
    sniff_tile_type,
    build_header_info,
)

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 8
JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 8
WEBP = b"RIFF\x00\x00\x00\x00WEBP" + b"\x00" * 4
GZIP_MVT = b"\x1f\x8b\x08\x00" + b"\x00" * 8


def test_sniff_known_types():
    assert sniff_tile_type(PNG) == TileType.PNG
    assert sniff_tile_type(JPEG) == TileType.JPEG
    assert sniff_tile_type(WEBP) == TileType.WEBP
    assert sniff_tile_type(GZIP_MVT) == TileType.MVT


def test_build_header_info_zoom_and_bbox():
    g = SlippyGrid()
    tiles = [(6, 32, 21), (6, 33, 21), (7, 64, 42)]
    info = build_header_info(
        tiles, g, TileType.PNG, Compression.NONE, {"name": "demo"}
    )
    assert info.min_zoom == 6
    assert info.max_zoom == 7
    minlon, minlat, maxlon, maxlat = info.bbox
    assert minlon < maxlon and minlat < maxlat
    hd = info.header_dict()
    assert hd["min_zoom"] == 6 and hd["max_zoom"] == 7
    assert hd["tile_type"] == TileType.PNG
    assert hd["center_zoom"] == 6
    assert isinstance(hd["min_lon_e7"], int)
```

- [ ] **Step 2: Run it to verify failure**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/tiles/test_header.py -v -p no:cacheprovider`
Expected: FAIL — `ModuleNotFoundError: ... _header`.

- [ ] **Step 3: Implement _header.py**

Create `python/geobrix/src/databricks/labs/gbx/ds/tiles/_header.py`:
```python
"""PMTiles header assembly + tile-type sniffing from magic bytes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Tuple

from pmtiles.tile import Compression, TileType

from databricks.labs.gbx.ds.tiles.grid import BBox, Grid, TileKey


def sniff_tile_type(data: bytes) -> TileType:
    """Detect tile encoding from magic bytes; default MVT for vector payloads."""
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return TileType.PNG
    if data[:3] == b"\xff\xd8\xff":
        return TileType.JPEG
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return TileType.WEBP
    if data[4:8] == b"ftyp" and b"avif" in data[8:20]:
        return TileType.AVIF
    # MVT is protobuf (often gzipped) with no reliable magic.
    return TileType.MVT


def _e7(v: float) -> int:
    return int(round(v * 1e7))


@dataclass
class HeaderInfo:
    tile_type: TileType
    tile_compression: Compression
    min_zoom: int
    max_zoom: int
    bbox: BBox
    metadata: Dict[str, object]

    def header_dict(self) -> Dict[str, object]:
        minlon, minlat, maxlon, maxlat = self.bbox
        clon = (minlon + maxlon) / 2.0
        clat = (minlat + maxlat) / 2.0
        return {
            "tile_type": self.tile_type,
            "tile_compression": self.tile_compression,
            "min_zoom": self.min_zoom,
            "max_zoom": self.max_zoom,
            "min_lon_e7": _e7(minlon),
            "min_lat_e7": _e7(minlat),
            "max_lon_e7": _e7(maxlon),
            "max_lat_e7": _e7(maxlat),
            "center_zoom": self.min_zoom,
            "center_lon_e7": _e7(clon),
            "center_lat_e7": _e7(clat),
        }


def build_header_info(
    tiles: Iterable[TileKey],
    grid: Grid,
    tile_type: TileType,
    tile_compression: Compression,
    metadata: Dict[str, object],
) -> HeaderInfo:
    """Compute min/max zoom + union bbox over a set of (z,x,y) tiles."""
    tiles = list(tiles)
    if not tiles:
        raise ValueError("build_header_info requires at least one tile")
    zs = [z for z, _, _ in tiles]
    minlon = minlat = float("inf")
    maxlon = maxlat = float("-inf")
    for z, x, y in tiles:
        bb = grid.tile_bbox(z, x, y)
        minlon, minlat = min(minlon, bb[0]), min(minlat, bb[1])
        maxlon, maxlat = max(maxlon, bb[2]), max(maxlat, bb[3])
    return HeaderInfo(
        tile_type=tile_type,
        tile_compression=tile_compression,
        min_zoom=min(zs),
        max_zoom=max(zs),
        bbox=(minlon, minlat, maxlon, maxlat),
        metadata=metadata,
    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/tiles/test_header.py -v -p no:cacheprovider`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/ds/tiles/_header.py \
        python/geobrix/test/ds/tiles/test_header.py
git commit -m "feat(ds/tiles): add tile-type sniff + PMTiles header assembly

Co-authored-by: Isaac"
```

---

### Task 6: `tiles/backend.py` — TileArchiveBackend protocol + PMTilesBackend

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/tiles/backend.py`
- Test: `python/geobrix/test/ds/tiles/test_backend.py`

- [ ] **Step 1: Write the failing test (round-trips via the pmtiles Reader)**

Create `python/geobrix/test/ds/tiles/test_backend.py`:
```python
import os
import tempfile

from pmtiles.reader import MmapSource, Reader
from pmtiles.tile import Compression, TileType, zxy_to_tileid

from databricks.labs.gbx.ds.tiles.grid import SlippyGrid
from databricks.labs.gbx.ds.tiles._header import build_header_info
from databricks.labs.gbx.ds.tiles.backend import PMTilesBackend

PNG = b"\x89PNG\r\n\x1a\n"


def test_pmtiles_backend_round_trip():
    g = SlippyGrid()
    tiles = [(6, 32, 21), (6, 33, 21)]
    # sorted-by-tileid stream of (tileid, bytes)
    payload = {t: PNG + bytes([i]) for i, t in enumerate(tiles)}
    stream = sorted(
        ((zxy_to_tileid(z, x, y), payload[(z, x, y)]) for (z, x, y) in tiles)
    )
    info = build_header_info(tiles, g, TileType.PNG, Compression.NONE, {"name": "t"})

    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "shard.pmtiles")
        PMTilesBackend().assemble(iter(stream), info, out)
        assert os.path.getsize(out) > 0
        with open(out, "rb") as f:
            r = Reader(MmapSource(f))
            assert r.get(6, 32, 21) == payload[(6, 32, 21)]
            assert r.get(6, 33, 21) == payload[(6, 33, 21)]
            assert r.header()["min_zoom"] == 6
            assert r.metadata()["name"] == "t"
```

- [ ] **Step 2: Run it to verify failure**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/tiles/test_backend.py -v -p no:cacheprovider`
Expected: FAIL — `ModuleNotFoundError: ... backend`.

- [ ] **Step 3: Implement backend.py**

Create `python/geobrix/src/databricks/labs/gbx/ds/tiles/backend.py`:
```python
"""Tile-archive backends: turn one shard's sorted (tileid, bytes) stream into a
container file. ``PMTilesBackend`` is the first; MBTiles/MVT-dir slot in later."""

from __future__ import annotations

from typing import Iterator, Protocol, Tuple

from pmtiles.writer import Writer

from databricks.labs.gbx.ds.tiles._header import HeaderInfo

SortedTiles = Iterator[Tuple[int, bytes]]  # ascending tileid


class TileArchiveBackend(Protocol):
    def assemble(
        self, sorted_tiles: SortedTiles, header_info: HeaderInfo, out_path: str
    ) -> None: ...


class PMTilesBackend:
    """Assemble a single ``.pmtiles`` archive from ascending-tileid tiles."""

    def assemble(
        self, sorted_tiles: SortedTiles, header_info: HeaderInfo, out_path: str
    ) -> None:
        with open(out_path, "wb") as f:
            writer = Writer(f)
            for tileid, data in sorted_tiles:
                writer.write_tile(tileid, data)
            writer.finalize(header_info.header_dict(), header_info.metadata)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/tiles/test_backend.py -v -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/ds/tiles/backend.py \
        python/geobrix/test/ds/tiles/test_backend.py
git commit -m "feat(ds/tiles): add TileArchiveBackend protocol + PMTilesBackend

Co-authored-by: Isaac"
```

---

### Task 7: `tiles/catalog.py` — CatalogWriter + STACManifestCatalog + TileJSONCatalog

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/tiles/catalog.py`
- Test: `python/geobrix/test/ds/tiles/test_catalog.py`

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/ds/tiles/test_catalog.py`:
```python
import json
import os
import tempfile

from databricks.labs.gbx.ds.tiles.catalog import (
    ShardInfo,
    STACManifestCatalog,
    TileJSONCatalog,
)

SHARDS = [
    ShardInfo("6/32/21.pmtiles", 6, 14, (-0.5, 51.0, 0.0, 51.5)),
    ShardInfo("6/33/21.pmtiles", 6, 14, (0.0, 51.0, 0.5, 51.5)),
]


def test_stac_manifest_shape():
    with tempfile.TemporaryDirectory() as d:
        path = STACManifestCatalog().write(SHARDS, d)
        assert os.path.basename(path) == "catalog.json"
        doc = json.load(open(path))
        assert doc["type"] == "FeatureCollection"
        assert len(doc["features"]) == 2
        feat = doc["features"][0]
        assert feat["geometry"]["type"] == "Polygon"
        assert feat["properties"]["pmtiles"] == "6/32/21.pmtiles"
        assert feat["properties"]["minzoom"] == 6
        assert feat["properties"]["maxzoom"] == 14
        assert feat["bbox"] == [-0.5, 51.0, 0.0, 51.5]


def test_tilejson_shape():
    with tempfile.TemporaryDirectory() as d:
        path = TileJSONCatalog().write(SHARDS, d)
        doc = json.load(open(path))
        assert doc["tilejson"] == "3.0.0"
        assert doc["minzoom"] == 6 and doc["maxzoom"] == 14
        # union bounds across shards
        assert doc["bounds"] == [-0.5, 51.0, 0.5, 51.5]
        assert len(doc["shards"]) == 2
```

- [ ] **Step 2: Run it to verify failure**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/tiles/test_catalog.py -v -p no:cacheprovider`
Expected: FAIL — `ModuleNotFoundError: ... catalog`.

- [ ] **Step 3: Implement catalog.py**

Create `python/geobrix/src/databricks/labs/gbx/ds/tiles/catalog.py`:
```python
"""Catalog writers over a set of shards. Default = a GeoJSON/STAC-style manifest
(one feature per shard with bbox + relative URL); TileJSON is an option. VRT and
full STAC-spec catalogs slot in later (see spec)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import List, Protocol, Tuple

BBox = Tuple[float, float, float, float]


@dataclass
class ShardInfo:
    rel_path: str  # path relative to the catalog (e.g. "6/32/21.pmtiles")
    min_zoom: int
    max_zoom: int
    bbox: BBox


class CatalogWriter(Protocol):
    def write(self, shards: List[ShardInfo], out_dir: str) -> str: ...


def _bbox_polygon(bbox: BBox) -> dict:
    minlon, minlat, maxlon, maxlat = bbox
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [minlon, minlat],
                [maxlon, minlat],
                [maxlon, maxlat],
                [minlon, maxlat],
                [minlon, minlat],
            ]
        ],
    }


def _union(shards: List[ShardInfo]) -> BBox:
    minlon = min(s.bbox[0] for s in shards)
    minlat = min(s.bbox[1] for s in shards)
    maxlon = max(s.bbox[2] for s in shards)
    maxlat = max(s.bbox[3] for s in shards)
    return (minlon, minlat, maxlon, maxlat)


class STACManifestCatalog:
    """GeoJSON FeatureCollection (STAC-style): one feature per shard."""

    def write(self, shards: List[ShardInfo], out_dir: str) -> str:
        features = [
            {
                "type": "Feature",
                "bbox": list(s.bbox),
                "geometry": _bbox_polygon(s.bbox),
                "properties": {
                    "pmtiles": s.rel_path,
                    "minzoom": s.min_zoom,
                    "maxzoom": s.max_zoom,
                },
            }
            for s in shards
        ]
        doc = {"type": "FeatureCollection", "features": features}
        path = os.path.join(out_dir, "catalog.json")
        with open(path, "w") as f:
            json.dump(doc, f)
        return path


class TileJSONCatalog:
    """Minimal TileJSON 3.0.0 over the shards (union bounds + a shards array)."""

    def write(self, shards: List[ShardInfo], out_dir: str) -> str:
        bounds = _union(shards)
        doc = {
            "tilejson": "3.0.0",
            "minzoom": min(s.min_zoom for s in shards),
            "maxzoom": max(s.max_zoom for s in shards),
            "bounds": list(bounds),
            "shards": [
                {
                    "pmtiles": s.rel_path,
                    "bounds": list(s.bbox),
                    "minzoom": s.min_zoom,
                    "maxzoom": s.max_zoom,
                }
                for s in shards
            ],
        }
        path = os.path.join(out_dir, "catalog.json")
        with open(path, "w") as f:
            json.dump(doc, f)
        return path
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/tiles/test_catalog.py -v -p no:cacheprovider`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/ds/tiles/catalog.py \
        python/geobrix/test/ds/tiles/test_catalog.py
git commit -m "feat(ds/tiles): add CatalogWriter + STAC manifest + TileJSON catalogs

Co-authored-by: Isaac"
```

---

### Task 8: `tiles/shard.py` — indexed scratch + entries-driven shard assignment

The keystone: per-partition indexed scratch (`ScratchWriter`), driver-side `assign_shards` (fixed + adaptive) over entries metadata, and `stream_sorted` to read each shard's bytes back in tileid order.

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/tiles/shard.py`
- Test: `python/geobrix/test/ds/tiles/test_shard.py`

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/ds/tiles/test_shard.py`:
```python
import os
import tempfile

from pmtiles.tile import zxy_to_tileid

from databricks.labs.gbx.ds.tiles.grid import SlippyGrid
from databricks.labs.gbx.ds.tiles.shard import (
    OVERVIEW,
    ScratchWriter,
    assign_shards,
    read_entries,
    stream_sorted,
)


def _write_scratch(scratch_dir, rows):
    w = ScratchWriter(scratch_dir)
    for z, x, y, data in rows:
        w.add(z, x, y, zxy_to_tileid(z, x, y), data)
    return w.close()  # (bin_path, idx_path)


def test_scratch_round_trip_and_stream_sorted():
    g = SlippyGrid()
    with tempfile.TemporaryDirectory() as d:
        rows = [
            (6, 33, 21, b"B"),
            (6, 32, 21, b"A"),
            (7, 64, 42, b"C"),
        ]
        _bin, idx = _write_scratch(d, rows)
        entries = read_entries(idx, d)
        assert len(entries) == 3
        streamed = list(stream_sorted(entries))
        # ascending tileid
        ids = [tid for tid, _ in streamed]
        assert ids == sorted(ids)
        assert {data for _, data in streamed} == {b"A", b"B", b"C"}


def test_fixed_assignment_and_overview_split():
    g = SlippyGrid()
    with tempfile.TemporaryDirectory() as d:
        rows = [
            (6, 32, 21, b"a"),  # body shard (6,32,21)
            (8, 130, 85, b"b"),  # body, parent (6, 32, 21)
            (3, 4, 2, b"o"),  # overview (z<6)
        ]
        _bin, idx = _write_scratch(d, rows)
        entries = read_entries(idx, d)
        groups = assign_shards(entries, shard_zoom=6, grid=g)
        assert OVERVIEW in groups
        assert len(groups[OVERVIEW]) == 1
        body_keys = [k for k in groups if k != OVERVIEW]
        # both body tiles share parent (6,32,21)
        assert body_keys == [(6, 32, 21)]
        assert len(groups[(6, 32, 21)]) == 2


def test_adaptive_subdivides_dense_cells():
    g = SlippyGrid()
    with tempfile.TemporaryDirectory() as d:
        # 4 z8 tiles under (6,32,21) but in two distinct z7 children
        rows = [
            (8, 128, 84, b"1"),
            (8, 129, 84, b"2"),
            (8, 130, 86, b"3"),
            (8, 131, 86, b"4"),
        ]
        _bin, idx = _write_scratch(d, rows)
        entries = read_entries(idx, d)
        # target 2 per shard -> base z6 cell (4 tiles) must subdivide
        groups = assign_shards(
            entries, shard_zoom=6, grid=g, target_tiles_per_shard=2
        )
        assert all(len(v) <= 2 for v in groups.values())
        # variable zoom: at least one shard deeper than 6
        assert any(k[0] > 6 for k in groups)
```

- [ ] **Step 2: Run it to verify failure**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/tiles/test_shard.py -v -p no:cacheprovider`
Expected: FAIL — `ModuleNotFoundError: ... shard`.

- [ ] **Step 3: Implement shard.py**

Create `python/geobrix/src/databricks/labs/gbx/ds/tiles/shard.py`:
```python
"""Entries-driven sharding. ``write()`` (executor) appends tile bytes to a
per-partition indexed scratch (bytes file + entries index) with NO shard
assignment. ``commit()`` (driver) reads only the entries metadata, assigns each
tile to a shard (fixed or adaptive), then streams each shard's bytes back in
tileid order — tile bytes never load on the driver in bulk."""

from __future__ import annotations

import json
import os
import uuid
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Tuple

from databricks.labs.gbx.ds.tiles.grid import Grid, TileKey

OVERVIEW = "overview"
_MAX_SHARD_ZOOM = 14  # cap adaptive subdivision depth


@dataclass
class Entry:
    z: int
    x: int
    y: int
    tileid: int
    offset: int
    length: int
    bin_path: str


class ScratchWriter:
    """Append tile bytes to one partition's scratch bin + collect an index."""

    def __init__(self, scratch_dir: str):
        os.makedirs(scratch_dir, exist_ok=True)
        uid = uuid.uuid4().hex
        self.bin_path = os.path.join(scratch_dir, f"part-{uid}.bin")
        self.idx_path = os.path.join(scratch_dir, f"part-{uid}.idx")
        self._f = open(self.bin_path, "wb")
        self._entries: List[Tuple[int, int, int, int, int, int]] = []
        self._offset = 0

    def add(self, z: int, x: int, y: int, tileid: int, data: bytes) -> None:
        self._f.write(data)
        self._entries.append((z, x, y, tileid, self._offset, len(data)))
        self._offset += len(data)

    def close(self) -> Tuple[str, str]:
        self._f.close()
        with open(self.idx_path, "w") as fh:
            json.dump(
                {"bin": os.path.basename(self.bin_path), "entries": self._entries},
                fh,
            )
        return self.bin_path, self.idx_path


def read_entries(idx_path: str, scratch_dir: str) -> List[Entry]:
    with open(idx_path) as fh:
        doc = json.load(fh)
    bin_path = os.path.join(scratch_dir, doc["bin"])
    return [
        Entry(z, x, y, tid, off, length, bin_path)
        for (z, x, y, tid, off, length) in doc["entries"]
    ]


def assign_shards(
    entries: List[Entry],
    shard_zoom: int,
    grid: Grid,
    target_tiles_per_shard: Optional[int] = None,
) -> Dict[object, List[Entry]]:
    """Group entries into shards. ``z < shard_zoom`` go to OVERVIEW; the rest are
    keyed by fixed parent or adaptively subdivided. Keys are (sz,sx,sy) tuples."""
    overview = [e for e in entries if e.z < shard_zoom]
    body = [e for e in entries if e.z >= shard_zoom]

    if target_tiles_per_shard is None:
        groups: Dict[object, List[Entry]] = defaultdict(list)
        for e in body:
            groups[grid.parent(e.z, e.x, e.y, shard_zoom)].append(e)
        result: Dict[object, List[Entry]] = dict(groups)
    else:
        result = _adaptive(body, shard_zoom, grid, target_tiles_per_shard)

    if overview:
        result[OVERVIEW] = overview
    return result


def _adaptive(
    entries: List[Entry], base_zoom: int, grid: Grid, target: int
) -> Dict[object, List[Entry]]:
    result: Dict[object, List[Entry]] = {}

    def recurse(zoom: int, cell_entries: List[Entry]) -> None:
        if len(cell_entries) <= target or zoom >= _MAX_SHARD_ZOOM:
            e0 = cell_entries[0]
            result[grid.parent(e0.z, e0.x, e0.y, zoom)] = cell_entries
            return
        buckets: Dict[TileKey, List[Entry]] = defaultdict(list)
        for e in cell_entries:
            buckets[grid.parent(e.z, e.x, e.y, zoom + 1)].append(e)
        # If subdivision did not actually split (all entries clamp to <= zoom),
        # keep them here to avoid infinite recursion.
        if len(buckets) == 1 and zoom + 1 > max(e.z for e in cell_entries):
            e0 = cell_entries[0]
            result[grid.parent(e0.z, e0.x, e0.y, zoom)] = cell_entries
            return
        for sub in buckets.values():
            recurse(zoom + 1, sub)

    base: Dict[TileKey, List[Entry]] = defaultdict(list)
    for e in entries:
        base[grid.parent(e.z, e.x, e.y, base_zoom)].append(e)
    for cell in base.values():
        recurse(base_zoom, cell)
    return result


def stream_sorted(entries: List[Entry]) -> Iterator[Tuple[int, bytes]]:
    """Yield (tileid, bytes) in ascending tileid order, reading from scratch bins.
    Duplicate tileids (should not occur in non-overlapping shards) are dropped."""
    ordered = sorted(entries, key=lambda e: e.tileid)
    handles: Dict[str, object] = {}
    seen = set()
    try:
        for e in ordered:
            if e.tileid in seen:
                continue
            seen.add(e.tileid)
            fh = handles.get(e.bin_path)
            if fh is None:
                fh = handles[e.bin_path] = open(e.bin_path, "rb")
            fh.seek(e.offset)
            yield e.tileid, fh.read(e.length)
    finally:
        for fh in handles.values():
            fh.close()
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/tiles/test_shard.py -v -p no:cacheprovider`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/ds/tiles/shard.py \
        python/geobrix/test/ds/tiles/test_shard.py
git commit -m "feat(ds/tiles): entries-driven indexed scratch + shard assignment

ScratchWriter appends per-partition bytes + an index; assign_shards groups
from entries metadata (fixed parent or adaptive targetTilesPerShard, plus an
overview split); stream_sorted reads each shard's bytes in tileid order.

Co-authored-by: Isaac"
```

---

### Task 9: `gbx/ds/pmtiles.py` — PMTilesGbxDataSource + PMTilesGbxWriter

Wire the framework into a write-only DataSource V2. `write()` → scratch; `commit()` → assign + assemble (single / sharded / overview) + catalog; `abort()` → cleanup.

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/pmtiles.py`
- Test: `python/geobrix/test/ds/test_pmtiles.py`

- [ ] **Step 1: Write the failing test (local Spark round-trip: single + sharded + overview)**

Create `python/geobrix/test/ds/test_pmtiles.py`:
```python
import json
import os

from pmtiles.reader import MmapSource, Reader

from databricks.labs.gbx.ds.register import register

PNG = b"\x89PNG\r\n\x1a\n"


def _png(tag: int) -> bytes:
    return PNG + bytes([tag])


def _rows(spark, tiles):
    data = [(z, x, y, bytearray(_png(i))) for i, (z, x, y) in enumerate(tiles)]
    return spark.createDataFrame(data, schema="z int, x int, y int, bytes binary")


def _read_tile(path, z, x, y):
    with open(path, "rb") as f:
        return Reader(MmapSource(f)).get(z, x, y)


def test_single_archive(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "world.pmtiles")
    tiles = [(6, 32, 21), (6, 33, 21), (7, 64, 42)]
    _rows(spark, tiles).write.format("pmtiles_gbx").mode("overwrite").option(
        "shardZoom", "0"
    ).save(out)
    assert os.path.isfile(out)
    assert _read_tile(out, 6, 32, 21) is not None
    assert _read_tile(out, 7, 64, 42) is not None


def test_sharded_with_overview_and_catalog(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "tileset_out")
    tiles = [(6, 32, 21), (8, 130, 85), (3, 4, 2)]  # body, body(same parent), overview
    _rows(spark, tiles).write.format("pmtiles_gbx").mode("overwrite").save(out)

    tileset = os.path.join(out, "tileset")
    assert os.path.isfile(os.path.join(tileset, "6", "32", "21.pmtiles"))
    assert os.path.isfile(os.path.join(tileset, "overview.pmtiles"))
    catalog = json.load(open(os.path.join(tileset, "catalog.json")))
    assert catalog["type"] == "FeatureCollection"
    # body tile reads back from its shard
    assert _read_tile(
        os.path.join(tileset, "6", "32", "21.pmtiles"), 6, 32, 21
    ) is not None
    # overview tile reads back from overview archive
    assert _read_tile(os.path.join(tileset, "overview.pmtiles"), 3, 4, 2) is not None
    # scratch cleaned up
    assert not os.path.isdir(os.path.join(out, "_scratch"))


def test_append_mode_rejected(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "appendme")
    os.makedirs(out, exist_ok=True)
    open(os.path.join(out, "marker"), "w").close()
    import pytest

    with pytest.raises(Exception):
        _rows(spark, [(6, 32, 21)]).write.format("pmtiles_gbx").mode("append").save(
            out
        )
```

- [ ] **Step 2: Run it to verify failure**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/test_pmtiles.py -v -p no:cacheprovider`
Expected: FAIL — `pmtiles_gbx` is not a registered format / `ModuleNotFoundError`.

- [ ] **Step 3: Implement pmtiles.py**

Create `python/geobrix/src/databricks/labs/gbx/ds/pmtiles.py`:
```python
"""``pmtiles_gbx`` — pure-Python DataSource V2 PMTiles writer on the shared
tiled-output framework. Write-only. Default = sharded (shardZoom=6) with a
separate overview.pmtiles + a STAC manifest; shardZoom=0 = single archive."""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional

from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    StructField,
    StructType,
)

from databricks.labs.gbx.ds.tiles import shard as _shard
from databricks.labs.gbx.ds.tiles._header import build_header_info, sniff_tile_type
from databricks.labs.gbx.ds.tiles.backend import PMTilesBackend
from databricks.labs.gbx.ds.tiles.catalog import (
    STACManifestCatalog,
    ShardInfo,
    TileJSONCatalog,
)
from databricks.labs.gbx.ds.tiles.grid import SlippyGrid

INPUT_SCHEMA = StructType(
    [
        StructField("z", IntegerType(), nullable=False),
        StructField("x", IntegerType(), nullable=False),
        StructField("y", IntegerType(), nullable=False),
        StructField("bytes", BinaryType(), nullable=False),
    ]
)

_COMPRESSION = {
    "none": Compression.NONE,
    "gzip": Compression.GZIP,
    "brotli": Compression.BROTLI,
    "zstd": Compression.ZSTD,
}
_TILETYPE = {
    "png": TileType.PNG,
    "jpeg": TileType.JPEG,
    "jpg": TileType.JPEG,
    "webp": TileType.WEBP,
    "avif": TileType.AVIF,
    "mvt": TileType.MVT,
}
_CATALOGS = {"stac": STACManifestCatalog, "tilejson": TileJSONCatalog}


def assert_input_schema(schema: StructType) -> None:
    names = [f.name for f in schema.fields]
    if names != ["z", "x", "y", "bytes"]:
        raise ValueError(
            "pmtiles_gbx requires exactly columns (z:int, x:int, y:int, "
            f"bytes:binary); got {names}"
        )


@dataclass
class PMTilesCommitMessage(WriterCommitMessage):
    bin_path: str
    idx_path: str


class PMTilesGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "pmtiles_gbx"

    def schema(self) -> StructType:
        return INPUT_SCHEMA

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        assert_input_schema(schema)
        path = self.options.get("path")
        if not path:
            raise ValueError("pmtiles_gbx writer requires an output path (.save(path)).")
        return PMTilesGbxWriter(path, dict(self.options), overwrite)


class PMTilesGbxWriter(DataSourceWriter):
    def __init__(self, path: str, options: Dict[str, str], overwrite: bool):
        self.path = path
        self.overwrite = overwrite
        self.shard_zoom = int(options.get("shardZoom", "6"))
        tps = options.get("targetTilesPerShard")
        self.target_tiles_per_shard = int(tps) if tps else None
        self.catalog_kind = options.get("catalog", "stac").lower()
        if self.catalog_kind not in _CATALOGS and self.catalog_kind != "none":
            raise ValueError(f"unknown catalog {self.catalog_kind!r}")
        tt = options.get("tileType")
        self.tile_type_override = _TILETYPE[tt.lower()] if tt else None
        self.tile_compression = _COMPRESSION[options.get("tileCompression", "none").lower()]
        self.metadata = json.loads(options["metadata"]) if options.get("metadata") else {}
        self.scratch_dir = os.path.join(self.path, "_scratch")
        self.grid = SlippyGrid()

        if not self.overwrite and self._target_exists():
            raise ValueError(
                "pmtiles_gbx does not support append; a finalized archive cannot be "
                "appended to. Use .mode('overwrite')."
            )
        if self.overwrite:
            self._clear_target()

    # ---- driver-side path helpers (no Spark internals; pure os) ----
    def _is_single(self) -> bool:
        return self.shard_zoom == 0

    def _target_exists(self) -> bool:
        return os.path.exists(self.path) and (
            os.path.isfile(self.path) or bool(os.listdir(self.path))
        )

    def _clear_target(self) -> None:
        if os.path.isfile(self.path):
            os.remove(self.path)
        elif os.path.isdir(self.path):
            shutil.rmtree(self.path)

    # ---- executor: stream bytes to indexed scratch ----
    def write(self, iterator: Iterator) -> WriterCommitMessage:
        writer = _shard.ScratchWriter(self.scratch_dir)
        for row in iterator:
            z, x, y, data = int(row[0]), int(row[1]), int(row[2]), bytes(row[3])
            writer.add(z, x, y, zxy_to_tileid(z, x, y), data)
        bin_path, idx_path = writer.close()
        return PMTilesCommitMessage(bin_path=bin_path, idx_path=idx_path)

    # ---- driver: assemble shards + catalog from entries ----
    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        entries: List[_shard.Entry] = []
        for msg in messages:
            if isinstance(msg, PMTilesCommitMessage):
                entries.extend(_shard.read_entries(msg.idx_path, self.scratch_dir))
        try:
            if not entries:
                return
            if self._is_single():
                self._assemble_single(entries)
            else:
                self._assemble_sharded(entries)
        finally:
            shutil.rmtree(self.scratch_dir, ignore_errors=True)

    def _tile_type(self, sample: bytes) -> TileType:
        return self.tile_type_override or sniff_tile_type(sample)

    def _assemble_single(self, entries: List[_shard.Entry]) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tiles = [(e.z, e.x, e.y) for e in entries]
        sample = next(iter(_shard.stream_sorted(entries[:1])))[1]
        info = build_header_info(
            tiles, self.grid, self._tile_type(sample), self.tile_compression, self.metadata
        )
        PMTilesBackend().assemble(_shard.stream_sorted(entries), info, self.path)

    def _assemble_sharded(self, entries: List[_shard.Entry]) -> None:
        tileset = os.path.join(self.path, "tileset")
        os.makedirs(tileset, exist_ok=True)
        groups = _shard.assign_shards(
            entries, self.shard_zoom, self.grid, self.target_tiles_per_shard
        )
        shard_infos: List[ShardInfo] = []
        for key, group in groups.items():
            sample = next(iter(_shard.stream_sorted(group[:1])))[1]
            tiles = [(e.z, e.x, e.y) for e in group]
            info = build_header_info(
                tiles, self.grid, self._tile_type(sample), self.tile_compression,
                self.metadata,
            )
            if key == _shard.OVERVIEW:
                rel = "overview.pmtiles"
            else:
                sz, sx, sy = key
                rel = os.path.join(str(sz), str(sx), f"{sy}.pmtiles")
            out_path = os.path.join(tileset, rel)
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            PMTilesBackend().assemble(_shard.stream_sorted(group), info, out_path)
            shard_infos.append(
                ShardInfo(rel, info.min_zoom, info.max_zoom, info.bbox)
            )
        if self.catalog_kind != "none":
            _CATALOGS[self.catalog_kind]().write(shard_infos, tileset)

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        shutil.rmtree(self.scratch_dir, ignore_errors=True)
        self._clear_target()
```

- [ ] **Step 4: Temporarily register pmtiles_gbx so the test can load it**

The test calls `register(spark)` from `gbx.ds.register`, which does not yet include the new source. Add it now (this anticipates Task 10; keep it minimal): edit `python/geobrix/src/databricks/labs/gbx/ds/register.py` to import + include `PMTilesGbxDataSource`:

```python
from typing import Optional

from pyspark.sql import SparkSession

from databricks.labs.gbx.ds.gtiff import GTiffGbxDataSource
from databricks.labs.gbx.ds.pmtiles import PMTilesGbxDataSource
from databricks.labs.gbx.ds.raster import RasterGbxDataSource

_SOURCES = (RasterGbxDataSource, GTiffGbxDataSource, PMTilesGbxDataSource)


def register(spark: Optional[SparkSession] = None) -> None:
    """Register all light DataSources. Uses the active session if not given."""
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    for source in _SOURCES:
        spark.dataSource.register(source)
```

(If `register.py` had a `_try_register_on_import()` helper that lists sources, update it the same way.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/test_pmtiles.py -v -p no:cacheprovider`
Expected: PASS (3 tests: single, sharded+overview+catalog, append rejected).

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/ds/pmtiles.py \
        python/geobrix/src/databricks/labs/gbx/ds/register.py \
        python/geobrix/test/ds/test_pmtiles.py
git commit -m "feat(ds): add pmtiles_gbx writer (single/sharded/overview + catalog)

Write-only DataSource V2 on the tiles framework: write() streams indexed
scratch, commit() assigns shards + assembles archives + overview + catalog
from entries metadata, abort() cleans up. Registered in gbx.ds.register.

Co-authored-by: Isaac"
```

---

### Task 10: Extend the Serverless guard to cover the framework + finalize register

**Files:**
- Modify: `python/geobrix/test/ds/test_serverless_no_spark_config.py`

- [ ] **Step 1: Add the new modules to the scan-coverage assertion**

The two-root scan from Task 3 already walks `gbx/ds/` recursively (so `tiles/*.py` and `pmtiles.py` are scanned for forbidden patterns automatically). Extend the explicit coverage assertion so a future accidental deletion is caught. In `test_serverless_scan_includes_ds_modules`, extend the required list:

```python
    for required in (
        "raster.py",
        "gtiff.py",
        "writer.py",
        "_write.py",
        "register.py",
        "_encode.py",
        "_listing.py",
        "pmtiles.py",
        "grid.py",
        "_header.py",
        "backend.py",
        "catalog.py",
        "shard.py",
    ):
        assert required in files, f"{required} not covered by Serverless scan"
```

- [ ] **Step 2: Run the full ds suite green**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds -v -p no:cacheprovider`
Expected: PASS for everything (migrated raster/gtiff/writer tests, framework `tiles/` tests, `test_pmtiles.py`, both Serverless tests).

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/ds/test_serverless_no_spark_config.py
git commit -m "test(ds): cover tiles framework + pmtiles in Serverless guard scan

Co-authored-by: Isaac"
```

---

### Task 11: Light-vs-heavy parity (Docker integration, skip-if-heavy-unavailable)

Confirm `pmtiles_gbx` (single mode) and the heavy `pmtiles` writer produce archives that decode to the same `z/x/y → bytes` set. Heavy needs the JAR + GDAL, so the test is skipped when heavy is unavailable (as the reader/writer parity tests already do).

**Files:**
- Create: `python/geobrix/test/ds/test_pmtiles_parity.py`

- [ ] **Step 1: Write the parity test**

Create `python/geobrix/test/ds/test_pmtiles_parity.py`:
```python
"""Light vs heavy PMTiles parity. Skipped unless the heavy `pmtiles` writer is
registered (JAR + GDAL present, i.e. on-cluster / Docker with heavy env)."""

import os

import pytest
from pmtiles.reader import MmapSource, Reader

from databricks.labs.gbx.ds.register import register

PNG = b"\x89PNG\r\n\x1a\n"


def _heavy_available(spark) -> bool:
    try:
        spark.read.format("pmtiles")
        return True
    except Exception:
        return False


def _decode_all(path):
    out = {}
    with open(path, "rb") as f:
        r = Reader(MmapSource(f))
        for z in range(0, 10):
            for x in range(0, 2 ** z):
                for y in range(0, 2 ** z):
                    t = r.get(z, x, y)
                    if t is not None:
                        out[(z, x, y)] = t
    return out


def test_light_vs_heavy_single_archive(spark, tmp_path):
    if not _heavy_available(spark):
        pytest.skip("heavy pmtiles writer unavailable (no JAR/GDAL)")
    register(spark)
    tiles = [(2, 1, 1), (2, 2, 1), (3, 4, 3)]
    rows = [(z, x, y, bytearray(PNG + bytes([i]))) for i, (z, x, y) in enumerate(tiles)]
    df = spark.createDataFrame(rows, schema="z int, x int, y int, bytes binary")

    light_out = str(tmp_path / "light.pmtiles")
    df.write.format("pmtiles_gbx").mode("overwrite").option("shardZoom", "0").save(
        light_out
    )

    heavy_out = str(tmp_path / "heavy.pmtiles")
    df.write.format("pmtiles").mode("overwrite").save(heavy_out)

    assert _decode_all(light_out) == _decode_all(heavy_out)
```

- [ ] **Step 2: Run in Docker (heavy env)**

Run via the doc/integration test path inside the dev container (dispatch as a Task subagent — it touches Docker and may take minutes):
```bash
gbx:test:python --path test/ds/test_pmtiles_parity.py
```
Expected: PASS on-cluster/Docker, or SKIP locally if heavy is unavailable. (Local host run: SKIP is acceptable and expected.)

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/ds/test_pmtiles_parity.py
git commit -m "test(ds): light-vs-heavy pmtiles parity (skip-if-heavy-unavailable)

Co-authored-by: Isaac"
```

---

### Task 12: Bench — add a PMTiles write-timing path

Extend the writer bench so `run_format_write` can time `pmtiles_gbx` (and the heavy `pmtiles`). Cluster execution is operator-driven (not part of the green-tests gate); this task adds the code path + a unit-level smoke check.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/readers.py`
- Test: `python/geobrix/test/bench/test_readers_pmtiles.py` (create dir/file if absent)

- [ ] **Step 1: Write a smoke test for the pmtiles write path**

Create `python/geobrix/test/bench/test_readers_pmtiles.py` (create `python/geobrix/test/bench/` if it does not exist):
```python
"""Smoke test: the writer bench can run a pmtiles_gbx write and return a row."""

import os

import pytest
from pyspark.sql import SparkSession

from databricks.labs.gbx.bench.readers import run_pmtiles_write


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[2]").appName("bench-pmtiles").getOrCreate()
    yield s
    s.stop()


def test_run_pmtiles_write_returns_row(spark, tmp_path):
    out = str(tmp_path / "bench_tiles")
    row = run_pmtiles_write(
        spark,
        out_path=out,
        run_id="t",
        warmup=0,
        measured=1,
        n_tiles=8,
        shard_zoom=0,
        write_fmt="pmtiles_gbx",
    )
    assert row.category == "writer"
    assert row.fn == "pmtiles_gbx"
    assert row.elapsed_s is not None
    assert os.path.exists(out)
```

- [ ] **Step 2: Run it to verify failure**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/bench/test_readers_pmtiles.py -v -p no:cacheprovider`
Expected: FAIL — `ImportError: cannot import name 'run_pmtiles_write'`.

- [ ] **Step 3: Implement run_pmtiles_write in bench/readers.py**

Add to `python/geobrix/src/databricks/labs/gbx/bench/readers.py` (reuse the existing `time_iters` / `ResultRow` helpers already imported in that module — match their names exactly as used by `run_format_write`):
```python
def run_pmtiles_write(
    spark,
    out_path: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    n_tiles: int = 1000,
    shard_zoom: int = 0,
    write_fmt: str = "pmtiles_gbx",
):
    """Time a PMTiles write of `n_tiles` synthetic PNG tiles. write_fmt is
    'pmtiles_gbx' (light) or 'pmtiles' (heavy). Returns a single writer ResultRow."""
    from databricks.labs.gbx.ds.register import register

    if write_fmt == "pmtiles_gbx":
        register(spark)

    png = b"\x89PNG\r\n\x1a\n"
    # generate a z-grid of n_tiles tiles at a zoom that fits them
    z = max(1, (max(1, n_tiles) - 1).bit_length() // 2 + 1)
    side = 2 ** z
    rows = []
    for i in range(n_tiles):
        x, y = i % side, (i // side) % side
        rows.append((z, x, y, bytearray(png + i.to_bytes(4, "big"))))
    df = spark.createDataFrame(
        rows, schema="z int, x int, y int, bytes binary"
    ).cache()
    df.count()

    def _write():
        writer = df.write.format(write_fmt).mode("overwrite")
        if write_fmt == "pmtiles_gbx":
            writer = writer.option("shardZoom", str(shard_zoom))
        writer.save(out_path)

    elapsed = time_iters(_write, warmup=warmup, measured=measured)
    return ResultRow(
        run_id=run_id,
        fn=write_fmt,
        category="writer",
        mode="spark-path",
        elapsed_s=elapsed,
    )
```

NOTE: `time_iters` returns the median measured time in seconds and `ResultRow` is the dataclass already used by `run_format_write` in this file. If `ResultRow`'s required fields differ from the kwargs above, match them to the existing `run_format_write` construction (read the top of `readers.py`) — do not invent new fields.

- [ ] **Step 4: Run the test to verify it passes**

Run: `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/bench/test_readers_pmtiles.py -v -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/bench/readers.py \
        python/geobrix/test/bench/test_readers_pmtiles.py
git commit -m "feat(bench): add run_pmtiles_write timing path (light + heavy)

Co-authored-by: Isaac"
```

---

### Task 13: Docs — `pmtiles_gbx.mdx` + doc-test example + sidebar + overview

Doc tests ARE the documentation source and run only in Docker.

**Files:**
- Create: `docs/tests/python/writers/pmtiles_gbx_examples.py`
- Create: `docs/tests/python/writers/test_pmtiles_gbx_examples.py`
- Create: `docs/docs/writers/pmtiles_gbx.mdx`
- Modify: `docs/sidebars.js` (Lightweight → Writers → Named)
- Modify: `docs/docs/writers/overview.mdx` (cross-link)

- [ ] **Step 1: Write the doc-test example code (string constants + verification functions)**

Create `docs/tests/python/writers/pmtiles_gbx_examples.py`:
```python
"""Executable doc examples for the lightweight pmtiles_gbx writer (run in Docker)."""

import json
import os
import tempfile

from pmtiles.reader import MmapSource, Reader

PNG = b"\x89PNG\r\n\x1a\n"

WRITE_PMTILES_SHARDED = """# Lightweight PMTiles writer — distributed spatial sharding (default).
# Input is a tile pyramid: (z, x, y, bytes). shardZoom=6 emits one
# tileset/{z}/{x}/{y}.pmtiles per populated parent + overview.pmtiles + a
# STAC catalog.json.
from databricks.labs.gbx.ds.register import register
register(spark)
df.write.format("pmtiles_gbx").mode("overwrite").option("shardZoom", "6").save(OUT_DIR)"""

WRITE_PMTILES_SINGLE = """# Single-archive PMTiles: shardZoom=0 packs every tile into one .pmtiles file.
from databricks.labs.gbx.ds.register import register
register(spark)
df.write.format("pmtiles_gbx").mode("overwrite").option("shardZoom", "0").save(OUT_FILE)"""

OPTIONS_NOTE = """# Knobs (sensible defaults):
#   shardZoom            6  -> sharded; 0 -> single archive
#   targetTilesPerShard  adaptive sharding (subdivide dense cells)
#   catalog              stac (default) | tilejson | none
#   tileType             auto-sniff (png/jpeg/webp/mvt); override if needed
#   tileCompression      none (default) | gzip | brotli | zstd
#   metadata             JSON string -> archive metadata"""


def _pyramid_df(spark, tiles):
    rows = [(z, x, y, bytearray(PNG + bytes([i]))) for i, (z, x, y) in enumerate(tiles)]
    return spark.createDataFrame(rows, schema="z int, x int, y int, bytes binary")


def write_pmtiles_single(spark):
    """Verify WRITE_PMTILES_SINGLE: write one archive, read tiles back."""
    from databricks.labs.gbx.ds.register import register

    register(spark)
    df = _pyramid_df(spark, [(2, 1, 1), (2, 2, 1), (3, 4, 3)])
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "world.pmtiles")
        df.write.format("pmtiles_gbx").mode("overwrite").option(
            "shardZoom", "0"
        ).save(out)
        with open(out, "rb") as f:
            r = Reader(MmapSource(f))
            assert r.get(2, 1, 1) is not None
            assert r.get(3, 4, 3) is not None


def write_pmtiles_sharded(spark):
    """Verify WRITE_PMTILES_SHARDED: sharded output + overview + STAC catalog."""
    from databricks.labs.gbx.ds.register import register

    register(spark)
    df = _pyramid_df(spark, [(6, 32, 21), (8, 130, 85), (3, 4, 2)])
    with tempfile.TemporaryDirectory() as d:
        df.write.format("pmtiles_gbx").mode("overwrite").option(
            "shardZoom", "6"
        ).save(d)
        tileset = os.path.join(d, "tileset")
        assert os.path.isfile(os.path.join(tileset, "6", "32", "21.pmtiles"))
        assert os.path.isfile(os.path.join(tileset, "overview.pmtiles"))
        cat = json.load(open(os.path.join(tileset, "catalog.json")))
        assert cat["type"] == "FeatureCollection" and cat["features"]
```

Create `docs/tests/python/writers/test_pmtiles_gbx_examples.py`:
```python
"""Executes the pmtiles_gbx writer doc examples (Docker)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pmtiles_gbx_examples as ex  # noqa: E402


def test_write_pmtiles_single(spark):
    ex.write_pmtiles_single(spark)


def test_write_pmtiles_sharded(spark):
    ex.write_pmtiles_sharded(spark)
```

- [ ] **Step 2: Write the doc page**

Create `docs/docs/writers/pmtiles_gbx.mdx` (match the frontmatter/`sidebar_position` convention of the sibling `gtiff_gbx.mdx`; pick the next free position in the Lightweight → Writers → Named group):
```jsx
---
sidebar_position: 6
---

import CodeFromTest from '@site/src/components/CodeFromTest';
import pmtilesEx from '!!raw-loader!../../tests/python/writers/pmtiles_gbx_examples.py';

# Lightweight PMTiles Writer (`pmtiles_gbx`)

Pure-Python, JAR-free, Serverless-safe writer that packages a tile pyramid
(`(z, x, y, bytes)`) into [PMTiles](https://docs.protomaps.com/pmtiles/) archives
using distributed **spatial sharding**: each populated parent tile becomes one
bounded, non-overlapping `.pmtiles` shard, plus a global `overview.pmtiles` and a
catalog over the shards. See the [spatial-sharding model](#spatial-sharding) below.

## Sharded output (default)

<CodeFromTest code={pmtilesEx} language="python" functionName="WRITE_PMTILES_SHARDED"
  source="docs/tests/python/writers/pmtiles_gbx_examples.py"
  testFile="docs/tests/python/writers/test_pmtiles_gbx_examples.py" />

Output layout:

```
OUT_DIR/tileset/{z}/{x}/{y}.pmtiles   # one per populated parent (Z >= shardZoom)
OUT_DIR/tileset/overview.pmtiles      # Z < shardZoom global overview
OUT_DIR/tileset/catalog.json          # STAC/GeoJSON manifest
```

## Single archive

<CodeFromTest code={pmtilesEx} language="python" functionName="WRITE_PMTILES_SINGLE"
  source="docs/tests/python/writers/pmtiles_gbx_examples.py"
  testFile="docs/tests/python/writers/test_pmtiles_gbx_examples.py" />

## Options

<CodeFromTest code={pmtilesEx} language="python" functionName="OPTIONS_NOTE"
  source="docs/tests/python/writers/pmtiles_gbx_examples.py"
  testFile="docs/tests/python/writers/test_pmtiles_gbx_examples.py" />

## Spatial sharding {#spatial-sharding}

The writer treats tiled output as immutable, spatially-indexed shards: partition
the world by a grid, emit one bounded `.pmtiles` per parent tile, and deliver a
catalog over the shards rather than one merged file. This keeps shards
independently regenerable and lets a browser fetch only the shard for the area in
view. Set `shardZoom=0` for a single merged archive.
```

- [ ] **Step 3: Add to the sidebar**

In `docs/sidebars.js`, find the **Lightweight → Writers → Named** items array (the one containing `'writers/gtiff_gbx'`) and add `'writers/pmtiles_gbx'` beside it:
```javascript
{ type: 'category', label: 'Named', collapsed: false, items: ['writers/gtiff_gbx', 'writers/pmtiles_gbx'] },
```
(Read the current `docs/sidebars.js` to place it in the correct tier group — the writers nav is tiered `Overview | Lightweight | Heavyweight`; `pmtiles_gbx` is Lightweight.)

- [ ] **Step 4: Cross-link in the writers overview**

In `docs/docs/writers/overview.mdx`, add a one-line entry for the lightweight PMTiles writer in the lightweight writers list, e.g.:
```markdown
- **`pmtiles_gbx`** — package a tile pyramid into spatially-sharded PMTiles archives + a catalog.
```

- [ ] **Step 5: Run the doc tests + internals-leak check (Docker; dispatch as a Task subagent)**

Run:
```bash
gbx:test:python-docs --path writers/ --log pmtiles-docs.log
grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/ ; echo "exit:$?"
```
Expected: writer doc tests PASS (including the two new `pmtiles_gbx` tests); the wave-leak grep prints nothing (`exit:1`).

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/tests/python/writers/pmtiles_gbx_examples.py \
        docs/tests/python/writers/test_pmtiles_gbx_examples.py \
        docs/docs/writers/pmtiles_gbx.mdx docs/sidebars.js docs/docs/writers/overview.mdx
git commit -m "docs(writers): add lightweight pmtiles_gbx writer page + doc-tests

Co-authored-by: Isaac"
```

---

## Final verification (after all tasks)

- [ ] **Full light ds suite:** `python/geobrix/.venv-pyrx/bin/python -m pytest python/geobrix/test/ds python/geobrix/test/bench/test_readers_pmtiles.py -v` — all green (heavy-parity SKIPs locally).
- [ ] **Serverless guard:** both tests in `test_serverless_no_spark_config.py` green; scan covers all `tiles/*.py` + `pmtiles.py`.
- [ ] **Python lint (CI gate):** `gbx:lint:python --check` (isort/black/flake8) — confirm in-container per the host-vs-Docker black caveat before push.
- [ ] **Doc tests + internals-leak (Docker):** `gbx:test:python-docs --path writers/`; `grep -rn -iE "wave [0-9]+" docs/docs/` prints nothing.
- [ ] **Binding parity unaffected:** `pmtiles_gbx` is a writer (a DataSource format), not a registered SQL/Scala function — it is NOT added to `registered_functions.txt`/`function-info.json`. Confirm `gbx:test:bindings` still passes unchanged.
- [ ] **Heavy parity on-cluster (optional, operator):** run `test_pmtiles_parity.py` in the heavy Docker/cluster env to confirm decoded-tile parity.
- [ ] **Bench on-cluster (optional, operator):** time `run_pmtiles_write` light vs heavy; record the ratio.

---

## Self-Review notes (plan vs spec)

- **Spec coverage:** framework `grid/_header/backend/catalog/shard` → Tasks 4–8; `pmtiles_gbx` DataSource (write/commit/abort, options, single+sharded+overview) → Task 9; entries-driven commit + fixed + adaptive sharding → Task 8 + 9; STAC default + tilejson → Task 7; overview.pmtiles → Task 9; nested layout → Task 9/13; migration precursor → Tasks 2–3; register unification → Task 9/10; pmtiles dep → Task 1; Serverless guard extension → Tasks 3,10; bench → Task 12; docs → Task 13; light-vs-heavy parity → Task 11.
- **Deferred per spec (not in this plan):** COG-by-quadbin + VRT catalog + `QuadbinGrid`; MVT-dir/MBTiles backends; bottom-up raster pyramid / global-scaling / sparse-skip / resampling buffer; light PMTiles reader; full STAC-spec compliance + meta-PMTiles; `pyvx`/`pygx`. `SlippyGrid` is the only `Grid` implementation built now (YAGNI — `QuadbinGrid` lands with the COG backend).
- **Type consistency:** `Entry`, `ScratchWriter`, `assign_shards`, `stream_sorted`, `OVERVIEW` (shard.py); `HeaderInfo.header_dict()`, `build_header_info`, `sniff_tile_type` (_header.py); `PMTilesBackend.assemble(sorted_tiles, header_info, out_path)` (backend.py); `ShardInfo(rel_path, min_zoom, max_zoom, bbox)`, `STACManifestCatalog`, `TileJSONCatalog` (catalog.py); `SlippyGrid.parent(z,x,y,shard_zoom)` clamps `sz=min(shard_zoom,z)` — used consistently in shard assignment and pmtiles.py.
- **Verify-during-impl (from spec) folded into steps:** pmtiles `finalize` header keys confirmed against the installed package (Tasks 5–6 round-trip); ascending-tileid requirement handled by `stream_sorted` (Task 8); scratch naming uuid-unique + cleaned on commit/abort (Tasks 8–9); empty-input returns without writing (Task 9 `commit` guard).
