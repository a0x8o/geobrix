# Light Readers — Raster (Python DataSource V2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add pure-Python/PySpark raster readers (`raster_gbx` catch-all, `gtiff_gbx` named) and a writer built on PySpark 4.x DataSource V2, as a 1:1 swap-out for the GDAL-backed Scala `gdal`/`gtiff_gdal` readers, with a bench reader mode to prove perf + distribution.

**Architecture:** A new `pyrx/ds/` subpackage. Pure-Python primitives (tile-grid sizing, GTiff re-encode + metadata, recursive listing) are composed by a `DataSourceReader` whose `partitions()` emits one partition per file and whose `read()` tiles+re-encodes each file into `(source, tile)` rows matching `pyrx._serde.TILE_SCHEMA`. The named reader subclasses the catch-all and injects a driver preset (mirrors Scala `dsExtraMap`). A `DataSourceWriter` writes `tile.raster` GTiff bytes back out. Everything is pure Python (no `_jvm`/`sparkContext`/`.rdd`) for Serverless. A new `bench/readers.py` mode times light vs heavy reads over a corpus.

**Tech Stack:** Python 3.12, PySpark 4.1.2 (`pyspark.sql.datasource`), rasterio (already in the `[pyrx]` extra), numpy, pytest. Heavy comparison runs in the `geobrix-dev` Docker container (needs the JAR + GDAL).

**Reference spec:** `docs/superpowers/specs/2026-06-11-light-readers-raster-design.md` (read the "Revision 2026-06-11" + "Parity contract" sections — the parity model is decoded-pixel-array within tolerance, NOT byte-for-byte).

---

## Execution status (2026-06-11)

- **T1–T8 — DONE.** Primitives + `raster_gbx`/`gtiff_gbx`/writer/`register` + Serverless guard. TDD, code-reviewed (critical fix: reuse `core.tiling` for split parity instead of a divergent raw-byte model), 21 tests green in `.venv-pyrx`, lint clean. Commits `90192d7`→`3026ec4`, `340a69d`.
- **T11 — DONE.** `bench/readers.py` (pure-local + spark-path) + `gbx:bench:readers` command + unit test. Commit `0168d96`. (Note: reader bench is its own module/command, not a FnSpec — `category="reader"`, `fn="raster_gbx_read"`.)
- **T9 — WRITTEN, execution BLOCKED on Docker.** `test_reader_parity.py` committed (`f6d40e6`), marked `integration`, JAR-backed fixture + real SRTM sample. Run once Docker is available.
- **T10 — BLOCKED on Docker.** Full `ds/` suite + CI-black check in the `geobrix-dev` container.
- **T12 — BLOCKED on Docker + cluster.** Wheel/JAR staging builds in Docker; needs a running bench cluster (none up). `notebooks/tests/databricks_cluster_config.env` is present.

**Blocker:** Docker Desktop refuses commands ("Sign in enforced by your administrators"). All Docker-routed steps (T9 run, T10, T12 staging) wait on that sign-in.

---

## Ground-truth facts (verified against Scala source — do not re-derive)

- **Schema:** `source: string` + `tile: struct{cellid: long, raster: binary, metadata: map<string,string>}`. Single source of truth: `python/geobrix/src/databricks/labs/gbx/pyrx/_serde.py::TILE_SCHEMA` (`cellid` LongType non-null, `raster` BinaryType non-null, `metadata` MapType(String,String) nullable). **Always import it — never redeclare.**
- **cellid:** `-1` on every emitted tile (`GDAL_Reader.scala:30` writes `-1L`).
- **tile.raster:** re-encoded **GTiff, DEFLATE** (`RasterDriver.writeToBytes` coerces to GTiff regardless of source). Not raw bytes.
- **One row per tile.** Tiling = `BalancedSubdivision.getTileSize` (power-of-4 split by `sizeInMB`, default 16) → grid of windows. Sub-16MB raster → 1 tile → 1 row.
- **metadata (11 keys):** `path, sourcePath, driver, format, last_command, last_error, all_parents, size, compression, isZipped, isSubset`. Fixed values from heavy: `driver="GTiff"`, `format="GTiff"`, `last_error=""`, `size="-1"`, `compression="DEFLATE"`, `isZipped="false"`, `isSubset="false"`, `last_command="windowed_extract -srcwin <x> <y> <w> <h>"`, `all_parents="<sourcePath>;"`, `sourcePath=<file path>`, `path=<in-memory uri>`.
- **Options:** `path` (required), `sizeInMB` (default `"16"`), `filterRegex` (default `".*"`). Recursive **regex** match on full path, not glob.
- **Corrupt file:** fail-fast, no `ignoreCorruptFiles` (`GDAL_Reader.scala:17`).
- **Named reader:** Scala `GTiff_DataSource.shortName()=="gtiff_gdal"`, injects `driver->"GTiff"`. Our light names: catch-all `raster_gbx`, named `gtiff_gbx`.

## PySpark DataSource V2 API (verified, PySpark 4.1.2)

`from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceWriter, InputPartition, WriterCommitMessage`

- `DataSource.__init__(self, options: Dict[str,str])` → stores `self.options`.
- `@classmethod DataSource.name(cls) -> str`
- `DataSource.schema(self) -> Union[StructType, str]`
- `DataSource.reader(self, schema: StructType) -> DataSourceReader`
- `DataSource.writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter`
- `DataSourceReader.partitions(self) -> Sequence[InputPartition]`
- `@abstractmethod DataSourceReader.read(self, partition) -> Iterator[Tuple]` (yields **tuples**, not Row; tuple field order = schema field order)
- `InputPartition(value)` — subclass with extra attrs; **must be picklable**.
- `@abstractmethod DataSourceWriter.write(self, iterator: Iterator[Row]) -> WriterCommitMessage` (receives **Row** objects)
- `DataSourceWriter.commit(self, messages) -> None` / `abort(self, messages) -> None`
- Register: `spark.dataSource.register(MyDataSourceClass)` (pass the **class**).

## File structure

| File | Responsibility |
|---|---|
| `python/geobrix/src/databricks/labs/gbx/pyrx/ds/__init__.py` | Subpackage marker + re-exports |
| `.../pyrx/ds/_tiling.py` | Port of `BalancedSubdivision` tile-grid math → list of windows |
| `.../pyrx/ds/_encode.py` | Windowed-read + GTiff(DEFLATE) re-encode + 11-key metadata → `(cellid, bytes, meta)` |
| `.../pyrx/ds/_listing.py` | Recursive path listing with `filterRegex` |
| `.../pyrx/ds/raster.py` | `RasterGbxDataSource` (`raster_gbx`) + reader + partition |
| `.../pyrx/ds/gtiff.py` | `GTiffGbxDataSource` (`gtiff_gbx`) — subclass + driver preset |
| `.../pyrx/ds/writer.py` | `RasterGbxWriter` + `RasterCommitMessage` |
| `.../pyrx/ds/register.py` | `register(spark)` + opportunistic-on-import guard |
| `python/geobrix/src/databricks/labs/gbx/bench/readers.py` | Reader bench mode (light vs heavy) |
| Tests under `python/geobrix/test/pyrx/ds/` | one test file per module |

All test commands run in Docker: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/ds/ --log <name>.log`. For a single node, append `::test_name` to the path. Local-venv runs (`source .venv-pyrx/bin/activate && pytest ...`) are fine for the pure-Python phases (1-3) that need no JAR.

---

## Task 1: Tile-grid math (`_tiling.py`)

Port `BalancedSubdivision.getTileSize` + grid enumeration as a pure function. No rasterio, no Spark — just integer math, so it's trivially testable and matches heavy row-count.

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/__init__.py`
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/_tiling.py`
- Test: `python/geobrix/test/pyrx/ds/__init__.py`, `python/geobrix/test/pyrx/ds/test_tiling.py`

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/pyrx/ds/__init__.py` (empty) and `python/geobrix/test/pyrx/ds/test_tiling.py`:

```python
"""Unit tests for the BalancedSubdivision port (pure integer math)."""
from databricks.labs.gbx.pyrx.ds import _tiling


def _bytes_per_pixel(dtype: str) -> int:
    import numpy as np
    return np.dtype(dtype).itemsize


def test_small_raster_is_single_tile():
    # 4x3 float32 single band = 48 bytes << 16 MiB -> one tile covering whole raster
    windows = _tiling.plan_windows(width=4, height=3, bands=1, dtype="float32", size_mib=16)
    assert windows == [(0, 0, 4, 3)]


def test_tile_count_is_power_of_four_when_split():
    # Force a split: 4096x4096 float64 x4 bands ~= 512 MiB, size limit 16 MiB.
    windows = _tiling.plan_windows(width=4096, height=4096, bands=4, dtype="float64", size_mib=16)
    n = len(windows)
    # nx == ny == 2^k -> count is a perfect square AND a power of four
    side = int(round(n ** 0.5))
    assert side * side == n, f"{n} tiles is not a square grid"
    assert (side & (side - 1)) == 0, f"side {side} is not a power of two"


def test_windows_tile_the_full_raster_without_gaps_or_overlap():
    width, height = 1000, 700
    windows = _tiling.plan_windows(width=width, height=height, bands=2, dtype="uint8", size_mib=1)
    # Reconstruct coverage: every pixel covered exactly once.
    covered = 0
    for col_off, row_off, win_w, win_h in windows:
        assert col_off + win_w <= width
        assert row_off + win_h <= height
        covered += win_w * win_h
    assert covered == width * height


def test_get_tile_size_matches_scala_ceil_div():
    # getTileSize(ds, destMiB) returns (tileX, tileY) = ceil(x/nx), ceil(y/ny)
    nx, ny, tile_x, tile_y = _tiling.tile_grid(width=1000, height=700, bands=2, dtype="uint8", size_mib=1)
    assert tile_x == -(-1000 // nx)  # ceil div
    assert tile_y == -(-700 // ny)
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_tiling.py -v`
Expected: FAIL — `ModuleNotFoundError: ... pyrx.ds._tiling` (or `__init__` missing).

- [ ] **Step 3: Implement `_tiling.py`**

Create `python/geobrix/src/databricks/labs/gbx/pyrx/ds/__init__.py`:

```python
"""pyrx.ds — pure-Python/PySpark DataSource V2 raster readers + writer.

Light-tier swap-out for the GDAL-backed Scala readers. See
docs/superpowers/specs/2026-06-11-light-readers-raster-design.md.
"""
```

Create `python/geobrix/src/databricks/labs/gbx/pyrx/ds/_tiling.py`:

```python
"""Port of Scala BalancedSubdivision tile-grid math (power-of-4 split).

Pure integer math so the light reader emits the SAME number of tiles per
raster as the heavy reader (row-count parity). Mirrors
``BalancedSubdivision.getTileSize`` in
src/main/scala/.../rasterx/operations/BalancedSubdivision.scala.
"""
from __future__ import annotations

from typing import List, Tuple

import numpy as np


def _mem_size_bytes(width: int, height: int, bands: int, dtype: str) -> int:
    """In-memory size of the raster, matching RasterAccessors.memSize."""
    return width * height * bands * int(np.dtype(dtype).itemsize)


def _num_splits_k(width: int, height: int, bands: int, dtype: str, size_mib: int) -> int:
    """Number of quad-split rounds k (nx=ny=2^k, tiles=4^k). Mirrors the Scala while-loop."""
    size_bytes = _mem_size_bytes(width, height, bands, dtype)
    limit = size_mib * 1024 * 1024
    k = 0
    # while k<9 and (sizeBytes >> 2k) > limit and 4^(k+1) <= 512
    while k < 9 and (size_bytes >> (2 * k)) > limit and (1 << (2 * (k + 1))) <= 512:
        k += 1
    return k


def tile_grid(width: int, height: int, bands: int, dtype: str, size_mib: int) -> Tuple[int, int, int, int]:
    """Return (nx, ny, tile_x, tile_y): grid divisions and per-tile pixel dims (ceil-div)."""
    k = _num_splits_k(width, height, bands, dtype, size_mib)
    nx = 1 << k
    ny = 1 << k
    tile_x = -(-width // nx)   # ceil div
    tile_y = -(-height // ny)
    return nx, ny, tile_x, tile_y


def plan_windows(width: int, height: int, bands: int, dtype: str, size_mib: int) -> List[Tuple[int, int, int, int]]:
    """List of (col_off, row_off, win_w, win_h) windows tiling the raster, no gaps/overlap."""
    _nx, _ny, tile_x, tile_y = tile_grid(width, height, bands, dtype, size_mib)
    windows: List[Tuple[int, int, int, int]] = []
    row_off = 0
    while row_off < height:
        win_h = min(tile_y, height - row_off)
        col_off = 0
        while col_off < width:
            win_w = min(tile_x, width - col_off)
            windows.append((col_off, row_off, win_w, win_h))
            col_off += tile_x
        row_off += tile_y
    return windows
```

- [ ] **Step 4: Run it and confirm it passes**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_tiling.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyrx/ds/__init__.py \
        python/geobrix/src/databricks/labs/gbx/pyrx/ds/_tiling.py \
        python/geobrix/test/pyrx/ds/__init__.py \
        python/geobrix/test/pyrx/ds/test_tiling.py
git commit -m "feat(pyrx-ds): port BalancedSubdivision tile-grid math"
```

---

## Task 2: Tile re-encode + metadata (`_encode.py`)

Given an open rasterio dataset and one window, windowed-read the pixels, write an in-memory GTiff (DEFLATE), and build the 11-key metadata map. Returns `(cellid=-1, gtiff_bytes, metadata)`.

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/_encode.py`
- Test: `python/geobrix/test/pyrx/ds/test_encode.py`

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/pyrx/ds/test_encode.py`:

```python
"""Unit tests for windowed GTiff re-encode + metadata."""
import numpy as np
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.ds import _encode

# 11 keys the heavy reader emits (WindowedExtract.scala:108-119)
EXPECTED_METADATA_KEYS = {
    "path", "sourcePath", "driver", "format", "last_command", "last_error",
    "all_parents", "size", "compression", "isZipped", "isSubset",
}


def test_encode_tile_roundtrips_pixels(gtiff_bytes):
    # gtiff_bytes fixture (conftest): 4x3 float32 single band, values arange(12)
    with MemoryFile(gtiff_bytes) as mf, mf.open() as ds:
        cellid, raster_bytes, meta = _encode.encode_tile(
            ds, window=(0, 0, 4, 3), source_path="/data/sample.tif", all_parents=""
        )
    assert cellid == -1
    with MemoryFile(raster_bytes) as mf2, mf2.open() as ds2:
        assert ds2.count == 1
        assert (ds2.width, ds2.height) == (4, 3)
        out = ds2.read(1)
    expected = np.arange(12, dtype="float32").reshape(3, 4)
    np.testing.assert_allclose(out, expected, rtol=1e-6)


def test_encode_metadata_key_set(gtiff_bytes):
    with MemoryFile(gtiff_bytes) as mf, mf.open() as ds:
        _cellid, _b, meta = _encode.encode_tile(
            ds, window=(0, 0, 4, 3), source_path="/data/sample.tif", all_parents=""
        )
    assert set(meta.keys()) == EXPECTED_METADATA_KEYS
    # fixed-value parity with heavy
    assert meta["driver"] == "GTiff"
    assert meta["format"] == "GTiff"
    assert meta["compression"] == "DEFLATE"
    assert meta["isZipped"] == "false"
    assert meta["isSubset"] == "false"
    assert meta["last_error"] == ""
    assert meta["sourcePath"] == "/data/sample.tif"
    assert meta["last_command"] == "windowed_extract -srcwin 0 0 4 3"


def test_encode_subwindow_reads_only_that_window(gtiff_bytes):
    with MemoryFile(gtiff_bytes) as mf, mf.open() as ds:
        _c, raster_bytes, _m = _encode.encode_tile(
            ds, window=(2, 0, 2, 3), source_path="/data/sample.tif", all_parents=""
        )
    with MemoryFile(raster_bytes) as mf2, mf2.open() as ds2:
        assert (ds2.width, ds2.height) == (2, 3)
        out = ds2.read(1)
    full = np.arange(12, dtype="float32").reshape(3, 4)
    np.testing.assert_allclose(out, full[:, 2:4], rtol=1e-6)
```

NOTE: the `gtiff_bytes` fixture already exists in `python/geobrix/test/pyrx/conftest.py` (session-scoped, 4x3 float32). To make it visible under `test/pyrx/ds/`, conftest fixtures in a parent dir are auto-inherited by pytest — no action needed.

- [ ] **Step 2: Run it and confirm it fails**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_encode.py -v`
Expected: FAIL — `_encode` has no `encode_tile`.

- [ ] **Step 3: Implement `_encode.py`**

Create `python/geobrix/src/databricks/labs/gbx/pyrx/ds/_encode.py`:

```python
"""Windowed GTiff(DEFLATE) re-encode + 11-key metadata, matching the heavy reader.

Mirrors RasterDriver.writeToBytes (always GTiff/DEFLATE on the wire) and
WindowedExtract metadata. tile.raster is NOT raw source bytes.
"""
from __future__ import annotations

from typing import Dict, Tuple

import rasterio
from rasterio.io import MemoryFile
from rasterio.windows import Window

CELLID_FRESH = -1  # GDAL_Reader.scala:30 writes -1L for un-tessellated tiles


def encode_tile(
    ds: "rasterio.DatasetReader",
    window: Tuple[int, int, int, int],
    source_path: str,
    all_parents: str,
    compression: str = "DEFLATE",
) -> Tuple[int, bytes, Dict[str, str]]:
    """Read one window, re-encode it as an in-memory GTiff, return (cellid, bytes, metadata)."""
    col_off, row_off, win_w, win_h = window
    rio_window = Window(col_off, row_off, win_w, win_h)
    data = ds.read(window=rio_window)  # (bands, h, w)

    profile = ds.profile.copy()
    profile.update(
        driver="GTiff",
        width=win_w,
        height=win_h,
        compress=compression.lower(),
        transform=ds.window_transform(rio_window),
    )
    # nodata/dtype/count/crs carried from source profile.

    with MemoryFile() as mf:
        with mf.open(**profile) as out:
            out.write(data)
        raster_bytes = mf.read()

    metadata = {
        "path": f"/vsimem/light_{abs(hash((source_path, col_off, row_off))) & 0xffffffff}.tif",
        "sourcePath": source_path,
        "driver": "GTiff",
        "format": "GTiff",
        "last_command": f"windowed_extract -srcwin {col_off} {row_off} {win_w} {win_h}",
        "last_error": "",
        "all_parents": f"{source_path};{all_parents}",
        "size": "-1",
        "compression": compression,
        "isZipped": "false",
        "isSubset": "false",
    }
    return CELLID_FRESH, raster_bytes, metadata
```

- [ ] **Step 4: Run it and confirm it passes**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_encode.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyrx/ds/_encode.py \
        python/geobrix/test/pyrx/ds/test_encode.py
git commit -m "feat(pyrx-ds): windowed GTiff re-encode + 11-key metadata"
```

---

## Task 3: Recursive path listing (`_listing.py`)

Mirror `HadoopUtils.listAllHadoopFiles(path, conf, filterRegex)`: recursively walk a dir (or accept a single file), keep paths whose full string matches `filterRegex`.

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/_listing.py`
- Test: `python/geobrix/test/pyrx/ds/test_listing.py`

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/pyrx/ds/test_listing.py`:

```python
"""Unit tests for recursive path listing with regex filter."""
import os

import pytest

from databricks.labs.gbx.pyrx.ds import _listing


@pytest.fixture
def tree(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "one.tif").write_bytes(b"x")
    (tmp_path / "a" / "two.tif").write_bytes(b"x")
    (tmp_path / "a" / "skip.txt").write_bytes(b"x")
    (tmp_path / "b").mkdir()
    (tmp_path / "b" / "three.tif").write_bytes(b"x")
    return tmp_path


def test_lists_all_files_recursively_default_regex(tree):
    files = _listing.list_files(str(tree), filter_regex=".*")
    assert len(files) == 4
    assert all(os.path.isabs(f) for f in files)


def test_regex_filters_by_full_path(tree):
    files = _listing.list_files(str(tree), filter_regex=r".*\.tif$")
    assert len(files) == 3
    assert all(f.endswith(".tif") for f in files)


def test_single_file_path_returns_that_file(tree):
    target = str(tree / "a" / "one.tif")
    files = _listing.list_files(target, filter_regex=".*")
    assert files == [target]


def test_no_match_raises(tree):
    with pytest.raises(FileNotFoundError):
        _listing.list_files(str(tree), filter_regex=r".*\.nope$")
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_listing.py -v`
Expected: FAIL — no `list_files`.

- [ ] **Step 3: Implement `_listing.py`**

Create `python/geobrix/src/databricks/labs/gbx/pyrx/ds/_listing.py`:

```python
"""Recursive file listing with a regex filter (mirrors HadoopUtils.listAllHadoopFiles).

Local-filesystem only — fits FUSE-mounted UC Volumes (/Volumes/...). Returns
sorted absolute paths so partition ordering is deterministic.
"""
from __future__ import annotations

import os
import re
from typing import List


def list_files(path: str, filter_regex: str = ".*") -> List[str]:
    """Return sorted absolute file paths under ``path`` whose full path matches ``filter_regex``."""
    pattern = re.compile(filter_regex)
    abspath = os.path.abspath(path)

    if os.path.isfile(abspath):
        candidates = [abspath] if pattern.match(abspath) else []
    else:
        candidates = []
        for root, _dirs, names in os.walk(abspath):
            for name in names:
                full = os.path.join(root, name)
                if pattern.match(full):
                    candidates.append(full)

    if not candidates:
        raise FileNotFoundError(
            f"No files under {path!r} matched filterRegex {filter_regex!r}"
        )
    return sorted(candidates)
```

- [ ] **Step 4: Run it and confirm it passes**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_listing.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyrx/ds/_listing.py \
        python/geobrix/test/pyrx/ds/test_listing.py
git commit -m "feat(pyrx-ds): recursive path listing with regex filter"
```

---

## Task 4: Catch-all `raster_gbx` DataSource

Compose the primitives into a `DataSource`/`DataSourceReader`. `partitions()` lists files (driver), `read()` tiles+encodes one file (executor), yielding tuples `(source, (cellid, raster, metadata))` in `TILE_SCHEMA` order.

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/raster.py`
- Test: `python/geobrix/test/pyrx/ds/test_raster_datasource.py`

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/pyrx/ds/test_raster_datasource.py`:

```python
"""Integration tests for the raster_gbx DataSource (uses local Spark)."""
import numpy as np
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.ds.raster import RasterGbxDataSource

EXPECTED_METADATA_KEYS = {
    "path", "sourcePath", "driver", "format", "last_command", "last_error",
    "all_parents", "size", "compression", "isZipped", "isSubset",
}


def _write_sample(path, width=4, height=3):
    from rasterio.transform import from_origin
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    profile = dict(driver="GTiff", width=width, height=height, count=1,
                   dtype="float32", crs="EPSG:4326",
                   transform=from_origin(10.0, 50.0, 0.5, 0.5), nodata=-9999.0)
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)


def test_schema_matches_tile_schema():
    from databricks.labs.gbx.pyrx import _serde
    ds = RasterGbxDataSource(options={"path": "/tmp/none"})
    schema = ds.schema()
    assert [f.name for f in schema.fields] == ["source", "tile"]
    assert schema["tile"].dataType == _serde.TILE_SCHEMA


def test_read_single_file_yields_one_row(spark, tmp_path):
    f = tmp_path / "sample.tif"
    _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(f))
    rows = df.collect()
    assert len(rows) == 1  # < 16 MiB -> 1 tile
    row = rows[0]
    assert row["source"] == str(f)
    assert row["tile"]["cellid"] == -1
    assert set(row["tile"]["metadata"].keys()) == EXPECTED_METADATA_KEYS
    # decode the re-encoded GTiff and check pixels
    with MemoryFile(bytes(row["tile"]["raster"])) as mf, mf.open() as out:
        arr = out.read(1)
    np.testing.assert_allclose(arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6)


def test_read_directory_one_partition_per_file(spark, tmp_path):
    for i in range(3):
        _write_sample(str(tmp_path / f"s{i}.tif"))
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").option("filterRegex", r".*\.tif$").load(str(tmp_path))
    assert df.rdd.getNumPartitions() == 3  # one partition per file
    assert df.count() == 3


def test_corrupt_file_fails_fast(spark, tmp_path):
    bad = tmp_path / "bad.tif"
    bad.write_bytes(b"not a raster")
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(bad))
    import pytest
    with pytest.raises(Exception):
        df.collect()
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_raster_datasource.py -v`
Expected: FAIL — no `raster` module / `RasterGbxDataSource`.

- [ ] **Step 3: Implement `raster.py`**

Create `python/geobrix/src/databricks/labs/gbx/pyrx/ds/raster.py`:

```python
"""raster_gbx — catch-all pure-Python DataSource V2 raster reader.

1:1 swap-out for the Scala ``gdal`` reader: recursively lists files, splits each
into BalancedSubdivision tiles, re-encodes each tile as GTiff, emits
(source, tile) rows matching pyrx._serde.TILE_SCHEMA. Pure Python (Serverless).
"""
from __future__ import annotations

from typing import Dict, Iterator, Sequence, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructField, StructType, StringType

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.ds import _encode, _listing, _tiling


def reader_schema() -> StructType:
    """(source, tile) — tile from the single-source TILE_SCHEMA."""
    return StructType([
        StructField("source", StringType(), nullable=False),
        StructField("tile", _serde.TILE_SCHEMA, nullable=False),
    ])


class _FilePartition(InputPartition):
    """One source file = one partition (picklable)."""

    def __init__(self, file_path: str, size_mib: int):
        self.file_path = file_path
        self.size_mib = size_mib


class RasterGbxReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("raster_gbx requires a 'path' (e.g. .load(path)).")
        self.size_mib = int(options.get("sizeInMB", "16"))
        self.filter_regex = options.get("filterRegex", ".*")

    def partitions(self) -> Sequence[InputPartition]:
        files = _listing.list_files(self.path, self.filter_regex)
        return [_FilePartition(f, self.size_mib) for f in files]

    def read(self, partition: "_FilePartition") -> Iterator[Tuple]:
        # rasterio imported inside read() so the import lands on executors.
        import rasterio

        from databricks.labs.gbx.pyrx import _env
        _env.configure_gdal_env()  # worker-side GDAL/PROJ env (matches pyrx UDF pattern)

        with rasterio.open(partition.file_path) as ds:
            windows = _tiling.plan_windows(
                width=ds.width, height=ds.height, bands=ds.count,
                dtype=ds.dtypes[0], size_mib=partition.size_mib,
            )
            for win in windows:
                cellid, raster_bytes, meta = _encode.encode_tile(
                    ds, window=win, source_path=partition.file_path, all_parents="",
                )
                yield (partition.file_path, (cellid, raster_bytes, meta))


class RasterGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "raster_gbx"

    def schema(self) -> StructType:
        return reader_schema()

    def reader(self, schema: StructType) -> DataSourceReader:
        return RasterGbxReader(self.options)
```

- [ ] **Step 4: Run it and confirm it passes**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_raster_datasource.py -v`
Expected: PASS (4 tests). If the local `spark` fixture is missing under `test/pyrx/ds/`, it is inherited from `test/pyrx/conftest.py` (module-scoped `local[2]`); confirm by running. If Arrow/typing errors appear, ensure the tuple field order is `(source, (cellid, raster, metadata))`.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyrx/ds/raster.py \
        python/geobrix/test/pyrx/ds/test_raster_datasource.py
git commit -m "feat(pyrx-ds): raster_gbx catch-all DataSource V2 reader"
```

---

## Task 5: Named `gtiff_gbx` reader

Subclass the catch-all, override `name()`, and inject a `driver="GTiff"` preset into options (the light analogue of Scala `dsExtraMap`).

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/gtiff.py`
- Test: `python/geobrix/test/pyrx/ds/test_gtiff_datasource.py`

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/pyrx/ds/test_gtiff_datasource.py`:

```python
"""gtiff_gbx named reader: same output as raster_gbx with driver preset."""
import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.ds.gtiff import GTiffGbxDataSource


def _write_sample(path):
    data = np.arange(12, dtype="float32").reshape(3, 4)
    profile = dict(driver="GTiff", width=4, height=3, count=1, dtype="float32",
                   crs="EPSG:4326", transform=from_origin(10.0, 50.0, 0.5, 0.5))
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)


def test_name_is_gtiff_gbx():
    assert GTiffGbxDataSource.name() == "gtiff_gbx"


def test_driver_preset_injected():
    ds = GTiffGbxDataSource(options={"path": "/tmp/x"})
    reader = ds.reader(ds.schema())
    assert reader.driver == "GTiff"


def test_reads_geotiff_like_catch_all(spark, tmp_path):
    f = tmp_path / "s.tif"
    _write_sample(str(f))
    spark.dataSource.register(GTiffGbxDataSource)
    df = spark.read.format("gtiff_gbx").load(str(f))
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["tile"]["metadata"]["driver"] == "GTiff"
    assert rows[0]["tile"]["cellid"] == -1
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_gtiff_datasource.py -v`
Expected: FAIL — no `gtiff` module.

- [ ] **Step 3: Implement `gtiff.py`**

Create `python/geobrix/src/databricks/labs/gbx/pyrx/ds/gtiff.py`:

```python
"""gtiff_gbx — named GeoTIFF reader. Light analogue of Scala GTiff_DataSource:
extends the catch-all and presets driver="GTiff" (the dsExtraMap mirror).
"""
from __future__ import annotations

from typing import Dict

from pyspark.sql.datasource import DataSourceReader
from pyspark.sql.types import StructType

from databricks.labs.gbx.pyrx.ds.raster import RasterGbxDataSource, RasterGbxReader


class GTiffGbxReader(RasterGbxReader):
    def __init__(self, options: Dict[str, str]):
        super().__init__(options)
        self.driver = "GTiff"  # preset; rasterio detects the driver, kept for parity/metadata


class GTiffGbxDataSource(RasterGbxDataSource):
    @classmethod
    def name(cls) -> str:
        return "gtiff_gbx"

    def reader(self, schema: StructType) -> DataSourceReader:
        return GTiffGbxReader(self.options)
```

- [ ] **Step 4: Run it and confirm it passes**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_gtiff_datasource.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyrx/ds/gtiff.py \
        python/geobrix/test/pyrx/ds/test_gtiff_datasource.py
git commit -m "feat(pyrx-ds): gtiff_gbx named reader (driver preset)"
```

---

## Task 6: Writer (`writer.py`)

DataSource write path: enforce the exact `(source, tile)` schema, write each row's `tile.raster` GTiff bytes to a file under the output path, `commit`/`abort`.

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/writer.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/gtiff.py` (add `writer()` + `schema` validation)
- Test: `python/geobrix/test/pyrx/ds/test_writer.py`

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/pyrx/ds/test_writer.py`:

```python
"""Round-trip: raster_gbx read -> gtiff_gbx write -> re-read; + strict schema."""
import os

import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.ds.gtiff import GTiffGbxDataSource
from databricks.labs.gbx.pyrx.ds.raster import RasterGbxDataSource


def _write_sample(path):
    data = np.arange(12, dtype="float32").reshape(3, 4)
    profile = dict(driver="GTiff", width=4, height=3, count=1, dtype="float32",
                   crs="EPSG:4326", transform=from_origin(10.0, 50.0, 0.5, 0.5))
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)


def test_round_trip(spark, tmp_path):
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)

    df = spark.read.format("raster_gbx").load(str(src))
    df.write.format("gtiff_gbx").mode("overwrite").save(str(out_dir))

    written = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert len(written) == 1
    with rasterio.open(os.path.join(out_dir, written[0])) as ds:
        arr = ds.read(1)
    np.testing.assert_allclose(arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6)


def test_strict_schema_rejects_extra_columns(spark, tmp_path):
    import pytest
    from pyspark.sql import functions as F
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src)).withColumn("extra", F.lit(1))
    with pytest.raises(Exception):
        df.write.format("gtiff_gbx").mode("overwrite").save(str(tmp_path / "o2"))
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_writer.py -v`
Expected: FAIL — `gtiff_gbx` has no writer.

- [ ] **Step 3: Implement `writer.py` and wire it into `gtiff.py`**

Create `python/geobrix/src/databricks/labs/gbx/pyrx/ds/writer.py`:

```python
"""gtiff_gbx writer (DataSource V2 write path).

Enforces the exact (source, tile) schema like the heavy GDAL writer, writes each
row's tile.raster GTiff bytes to a file under the output path. Pure Python.
"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Iterator, List, Optional

from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType

from databricks.labs.gbx.pyrx.ds.raster import reader_schema


@dataclass
class RasterCommitMessage(WriterCommitMessage):
    paths: List[str]


def assert_write_schema(schema: StructType) -> None:
    """Exact (source, tile) — extras OR missing both fail (matches GDAL writer)."""
    expected = reader_schema()
    if [f.name for f in schema.fields] != [f.name for f in expected.fields]:
        raise ValueError(
            f"gtiff_gbx writer requires exactly columns "
            f"{[f.name for f in expected.fields]}, got {[f.name for f in schema.fields]}"
        )


class RasterGbxWriter(DataSourceWriter):
    def __init__(self, path: str, schema: StructType, overwrite: bool):
        assert_write_schema(schema)
        self.path = path
        self.overwrite = overwrite

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        os.makedirs(self.path, exist_ok=True)
        written: List[str] = []
        for row in iterator:
            raster_bytes = bytes(row["tile"]["raster"])
            out = os.path.join(self.path, f"raster_{uuid.uuid4().hex}.tif")
            with open(out, "wb") as fh:
                fh.write(raster_bytes)
            written.append(out)
        return RasterCommitMessage(paths=written)

    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        return None  # files already durable; nothing to finalize

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        for msg in messages:
            if isinstance(msg, RasterCommitMessage):
                for p in msg.paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
```

Modify `python/geobrix/src/databricks/labs/gbx/pyrx/ds/gtiff.py` — add the writer method to `GTiffGbxDataSource` (append the import and method):

```python
# add at top imports:
from pyspark.sql.datasource import DataSourceWriter
from databricks.labs.gbx.pyrx.ds.writer import RasterGbxWriter

# add inside GTiffGbxDataSource:
    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        path = self.options.get("path")
        if not path:
            raise ValueError("gtiff_gbx writer requires an output path (.save(path)).")
        return RasterGbxWriter(path, schema, overwrite)
```

- [ ] **Step 4: Run it and confirm it passes**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_writer.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyrx/ds/writer.py \
        python/geobrix/src/databricks/labs/gbx/pyrx/ds/gtiff.py \
        python/geobrix/test/pyrx/ds/test_writer.py
git commit -m "feat(pyrx-ds): gtiff_gbx DataSource V2 writer + strict schema"
```

---

## Task 7: `register(spark)` + opportunistic import

Mirror `pyrx.functions.register`: a `register(spark)` that registers all light DataSources, plus an opportunistic attempt on `pyrx.ds` import guarded for no-active-session.

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/register.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/__init__.py` (re-export + opportunistic register)
- Test: `python/geobrix/test/pyrx/ds/test_register.py`

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/pyrx/ds/test_register.py`:

```python
"""register(spark) makes all light raster formats resolvable."""
import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.ds import register as ds_register


def _write_sample(path):
    data = np.arange(12, dtype="float32").reshape(3, 4)
    profile = dict(driver="GTiff", width=4, height=3, count=1, dtype="float32",
                   crs="EPSG:4326", transform=from_origin(10.0, 50.0, 0.5, 0.5))
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)


def test_register_makes_both_formats_loadable(spark, tmp_path):
    f = tmp_path / "s.tif"
    _write_sample(str(f))
    ds_register.register(spark)
    assert spark.read.format("raster_gbx").load(str(f)).count() == 1
    assert spark.read.format("gtiff_gbx").load(str(f)).count() == 1
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_register.py -v`
Expected: FAIL — no `register` module.

- [ ] **Step 3: Implement `register.py` and update `__init__.py`**

Create `python/geobrix/src/databricks/labs/gbx/pyrx/ds/register.py`:

```python
"""Register the light raster DataSources with a Spark session.

Mirrors pyrx.functions.register: call once, consciously. The format strings
raster_gbx / gtiff_gbx do not collide with the Scala-registered gdal /
gtiff_gdal, so both tiers coexist.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from databricks.labs.gbx.pyrx.ds.gtiff import GTiffGbxDataSource
from databricks.labs.gbx.pyrx.ds.raster import RasterGbxDataSource

_SOURCES = (RasterGbxDataSource, GTiffGbxDataSource)


def register(spark: SparkSession = None) -> None:
    """Register raster_gbx + gtiff_gbx. Uses the active session if not given."""
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    for source in _SOURCES:
        spark.dataSource.register(source)


def _try_register_on_import() -> None:
    """Best-effort register if a session is already live (no-op otherwise)."""
    try:
        spark = SparkSession.getActiveSession()
        if spark is not None:
            register(spark)
    except Exception:
        pass  # never fail an import; explicit register() remains available
```

Append to `python/geobrix/src/databricks/labs/gbx/pyrx/ds/__init__.py`:

```python
from databricks.labs.gbx.pyrx.ds.register import register  # noqa: E402,F401
from databricks.labs.gbx.pyrx.ds.register import _try_register_on_import

_try_register_on_import()
```

- [ ] **Step 4: Run it and confirm it passes**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_register.py -v`
Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyrx/ds/register.py \
        python/geobrix/src/databricks/labs/gbx/pyrx/ds/__init__.py \
        python/geobrix/test/pyrx/ds/test_register.py
git commit -m "feat(pyrx-ds): register(spark) + opportunistic import"
```

---

## Task 8: Serverless guard covers `ds/`

Confirm the existing Serverless scan (`test_serverless_no_spark_config.py`) walks `pyrx/ds/*.py`. The scan globs all `*.py` under `pyrx/`; add an explicit assertion that the new dir is covered so a future refactor can't silently drop it.

**Files:**
- Modify: `python/geobrix/test/pyrx/test_serverless_no_spark_config.py`

- [ ] **Step 1: Read the current scan + add a guard test**

First inspect how `_pyrx_source_files()` collects files (it uses `rglob("*.py")` over the pyrx package root — confirm). Then append:

```python
def test_serverless_scan_includes_ds_subpackage():
    files = {p.name for p in _pyrx_source_files()}
    # the new DataSource modules must be in scope of the Serverless scan
    for required in ("raster.py", "gtiff.py", "writer.py", "register.py",
                     "_tiling.py", "_encode.py", "_listing.py"):
        assert required in files, f"{required} not covered by Serverless scan"
```

- [ ] **Step 2: Run it**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/test_serverless_no_spark_config.py -v`
Expected: PASS — both the existing forbidden-pattern test (now also scanning `ds/`) and the new coverage guard. If the forbidden-pattern test FAILS, a `ds/` module used a banned call (`.conf.set`, `._jvm`, `.sparkContext`, `.rdd`) — fix the module (none of the Task 1-7 code uses these).

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/pyrx/test_serverless_no_spark_config.py
git commit -m "test(pyrx-ds): assert Serverless scan covers ds/ subpackage"
```

---

## Task 9: Light-vs-heavy parity test (Docker / integration)

The real swap-out proof: same sample file through heavy `gdal` and light `raster_gbx`, decode both and compare pixel arrays within tolerance. Needs the JAR + GDAL → Docker only; mark `integration`.

**Files:**
- Create: `python/geobrix/test/pyrx/ds/test_reader_parity.py`

- [ ] **Step 1: Write the parity test**

Create `python/geobrix/test/pyrx/ds/test_reader_parity.py`:

```python
"""Light vs heavy reader parity (Docker; needs JAR + sample data)."""
import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile

pytestmark = pytest.mark.integration

SAMPLE = "/Volumes/main/geobrix_samples/geobrix-examples/london/sentinel2.tif"  # adjust to an existing sample
REL_TOL = 1e-3
ABS_TOL = 1e-3


def _decode(raster_bytes):
    with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as ds:
        return ds.read()  # (bands, h, w)


def test_raster_gbx_matches_gdal(spark_with_jar):
    # spark_with_jar: a session with the geobrix JAR on the classpath (Docker conftest).
    from databricks.labs.gbx.pyrx.ds.register import register
    register(spark_with_jar)

    heavy = spark_with_jar.read.format("gdal").load(SAMPLE).orderBy("source").collect()
    light = spark_with_jar.read.format("raster_gbx").load(SAMPLE).orderBy("source").collect()

    assert len(light) == len(heavy), "tile/row count differs"
    for h, l in zip(heavy, light):
        assert l["tile"]["cellid"] == -1 == h["tile"]["cellid"]
        assert set(l["tile"]["metadata"].keys()) == set(h["tile"]["metadata"].keys())
        ha, la = _decode(h["tile"]["raster"]), _decode(l["tile"]["raster"])
        assert ha.shape == la.shape, "tile pixel dims differ"
        np.testing.assert_allclose(la, ha, rtol=REL_TOL, atol=ABS_TOL)
```

NOTE for the implementer: (a) pick an actual sample file that exists in the Volume — list `/Volumes/main/geobrix_samples/...` first and fix `SAMPLE`. (b) If no `spark_with_jar` fixture exists, the Docker conftest builds a session with the JAR; reuse however other heavy/integration pyrx tests obtain a JAR-backed session (grep `test/pyrx` for `.format("gdal")` usage). (c) If row counts differ, the tiling port (Task 1) needs reconciliation with `BalancedSubdivision` for that file's size/dtype — debug `_num_splits_k` against `memSize`.

- [ ] **Step 2: Run it in Docker**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/ds/test_reader_parity.py --with-integration --log reader-parity.log`
Expected: PASS. Watch the log; report progress every ~30s per the repo convention.

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/pyrx/ds/test_reader_parity.py
git commit -m "test(pyrx-ds): light-vs-heavy raster reader pixel-parity (integration)"
```

---

## Task 10: Full ds/ suite green in Docker

Run the entire new suite in the canonical Docker path (the parity definition of done for unit tests).

- [ ] **Step 1: Run the suite**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/ds/ --log ds-suite.log`
Expected: all non-integration tests PASS. Fix any Docker-vs-venv discrepancy (e.g. GDAL env) in the relevant module, not in the test.

- [ ] **Step 2: Lint**

Run: `bash scripts/commands/gbx-lint-python.sh --fix` then verify with the in-container check per the host-vs-Docker black caveat. Re-run the suite if reformatted.

- [ ] **Step 3: Commit any fixes**

```bash
chmod -R u+rwX .git/objects
git add -A python/geobrix
git commit -m "chore(pyrx-ds): lint + Docker suite green"
```

---

## Task 11: Bench reader mode (`bench/readers.py`)

Add a reader bench reusing `results.ResultRow` / `store` / `compare`. Two surfaces: pure-local (single-file open+tile+encode, light rasterio vs heavy via the JAR path) and spark-path (N files distributed). Light meaningfully slower than heavy is a deprecation blocker (record the ratio like the function bench).

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/bench/readers.py`
- Create: `scripts/commands/gbx-bench-readers.md`, `scripts/commands/gbx-bench-readers.sh`
- Test: `python/geobrix/test/bench/test_readers_bench.py`

- [ ] **Step 1: Write a unit test for the pure-local timing path**

First grep `python/geobrix/test/bench/` for the existing bench test style and the `time_iters` import path. Create `python/geobrix/test/bench/test_readers_bench.py`:

```python
"""Unit test: reader bench pure-local path produces a ResultRow with timing."""
import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.bench import readers


def _write_sample(path):
    data = np.arange(12, dtype="float32").reshape(3, 4)
    profile = dict(driver="GTiff", width=4, height=3, count=1, dtype="float32",
                   crs="EPSG:4326", transform=from_origin(10.0, 50.0, 0.5, 0.5))
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)


def test_pure_local_reader_bench_emits_result(tmp_path):
    f = tmp_path / "s.tif"
    _write_sample(str(f))
    rows = readers.run_pure_local_reader(
        files=[str(f)], run_id="t", warmup=1, measured=3, size_mib=16,
    )
    assert len(rows) == 1
    r = rows[0]
    assert r.api == "lightweight"
    assert r.fn == "raster_gbx_read"
    assert r.mode == "pure-core"
    assert r.iter_median_s >= 0.0
    assert r.status == "ok"
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/bench/test_readers_bench.py -v`
Expected: FAIL — no `bench.readers`.

- [ ] **Step 3: Implement `bench/readers.py`**

Create `python/geobrix/src/databricks/labs/gbx/bench/readers.py`. Reuse `time_iters` from `runner` and `ResultRow` from `results` (confirm exact import paths by reading those modules). Skeleton with the real timing call:

```python
"""Reader bench: time the light raster reader (and compare to heavy gdal).

Pure-local surface times the per-file open+tile+encode in-process (no Spark).
Spark-path surface times a distributed read over a corpus. Emits results.ResultRow
so it shares store/compare plumbing with the function bench.
"""
from __future__ import annotations

from typing import List

from databricks.labs.gbx.bench import results
from databricks.labs.gbx.bench.runner import time_iters
from databricks.labs.gbx.bench.env import describe_env  # confirm this helper name in env/results
from databricks.labs.gbx.pyrx.ds import _encode, _tiling


def _read_one_file_light(file_path: str, size_mib: int) -> int:
    """Open + tile + re-encode one file; return tile count (forces the work)."""
    import rasterio
    n = 0
    with rasterio.open(file_path) as ds:
        windows = _tiling.plan_windows(ds.width, ds.height, ds.count, ds.dtypes[0], size_mib)
        for win in windows:
            _encode.encode_tile(ds, window=win, source_path=file_path, all_parents="")
            n += 1
    return n


def run_pure_local_reader(files: List[str], run_id: str, warmup: int, measured: int,
                          size_mib: int = 16) -> List[results.ResultRow]:
    rows: List[results.ResultRow] = []
    for f in files:
        stats = time_iters(lambda: _read_one_file_light(f, size_mib), warmup, measured)
        rows.append(_to_result_row(run_id, f, stats))  # build ResultRow from stats + env
    return rows
```

The implementer must fill `_to_result_row` using the actual `ResultRow` field names from `results.py` (run_id, api="lightweight", fn="raster_gbx_read", category="reader", mode="pure-core", iter_median_s from `stats["iter_median_ms"]/1000`, status="ok", plus the env_* fields from the env helper). Read `runner.py::run_pure_core` for the exact ResultRow construction pattern and copy it. Add a `run_spark_path_reader(spark, path, run_id, ...)` that times `spark.read.format("raster_gbx").load(path).count()` (or `.foreach`), recording `rows` and `iter_median_s`, mirroring how `runner.py` times spark-path functions.

- [ ] **Step 4: Run the unit test and confirm it passes**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/bench/test_readers_bench.py -v`
Expected: PASS.

- [ ] **Step 5: Add the `gbx:bench:readers` command**

Create `scripts/commands/gbx-bench-readers.md` (title, description, usage `bash scripts/commands/gbx-bench-readers.sh [OPTIONS]`, options `--corpus`, `--mode {pure-local|spark-path|both}`, `--out`, `--run-id`, `--size-mib`, `--warmup`, `--measured`, `--with-heavy`, `--log`, `--help`, two examples).

Create `scripts/commands/gbx-bench-readers.sh` modeled on `scripts/commands/gbx-bench-lightweight.sh`: source `common.sh`, resolve `SCRIPT_DIR`/`PROJECT_ROOT`, support `--help`, `--log` via `resolve_log_path`, `check_docker` if `--mode spark-path`/`--with-heavy` (needs JAR), and invoke a Python entry (`python -m databricks.labs.gbx.bench.readers ...` — add a small `__main__`/argparse to `readers.py`) via `run_in_pyrx_venv`. No placeholders — implement real arg parsing and the python invocation.

`chmod +x scripts/commands/gbx-bench-readers.sh`.

- [ ] **Step 6: Smoke-run the command**

Run: `bash scripts/commands/gbx-bench-readers.sh --mode pure-local --corpus <a small local dir of tif> --warmup 1 --measured 3 --log bench-readers.log`
Expected: writes ResultRows; prints a summary path. Verify the result store has `fn="raster_gbx_read"` rows.

- [ ] **Step 7: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/bench/readers.py \
        python/geobrix/test/bench/test_readers_bench.py \
        scripts/commands/gbx-bench-readers.md scripts/commands/gbx-bench-readers.sh
git commit -m "feat(bench): reader bench mode (light raster reader timing)"
```

---

## Task 12: Cluster spark-path reader bench + perf gate (validation)

Run the distributed reader bench on the bench cluster (light `raster_gbx` vs heavy `gdal`) at the standard scale, confirm distribution (partition count tracks file count) and the light-vs-heavy ratio. This is the perf-viability evidence for the swap-out.

- [ ] **Step 1: Stage the wheel + run**

Per the cluster-bench memories: build/stage the `[pyrx]` wheel (`gbx:data:push-wheel`), confirm the bench cluster is up (poll libraries INSTALLED), run `gbx:bench:readers --mode spark-path --with-heavy --corpus <Volume corpus of ~1000 tiles> --run-id <id>`. Give the run's `bench-out/<run_id>/summary.md` link at the end (per repo convention). Do NOT launch duplicate runs.

- [ ] **Step 2: Record findings**

Append a short findings note to the spec's Performance section (or a `prompts/refactoring/2026-06-..-reader-bench-findings.md`, gitignored): light-vs-heavy ratio for pure-local and spark-path, parallel-efficiency (per-partition spread), and whether the light reader meets the no-regression bar. If light is meaningfully slower, file a perf follow-up (deprecation blocker per the perf-parity memory).

- [ ] **Step 3: Commit any spec/doc update**

```bash
chmod -R u+rwX .git/objects
git add docs/superpowers/specs/2026-06-11-light-readers-raster-design.md
git commit -m "docs(spec): record reader bench light-vs-heavy findings"
```

---

## Out of scope (own follow-up plans)

- Vector readers (`vector_gbx`, `shapefile_gbx`, `geojson_gbx`, `gpkg_gbx`, `file_gdb_gbx`) via pyogrio.
- `pygx` grid I/O.
- User-facing docs pages for the light readers (doc-tests under `docs/tests/`); add once the API stabilizes. The unit + parity tests here are the contract in the interim.
- Refactoring the 2 test-only DDL strings in `test_functions_spark.py` to import `TILE_SCHEMA` (latent drift; noted, not blocking).

## Self-review notes (for the executor)

- **Tuple order matters:** reader `read()` yields `(source, (cellid, raster, metadata))` — the inner tuple order must match `TILE_SCHEMA` field order (cellid, raster, metadata).
- **Import rasterio inside `read()`/`write()`** so it resolves on executors, and call `_env.configure_gdal_env()` worker-side (matches the pyrx UDF pattern).
- **Never redeclare the tile schema** — always `from databricks.labs.gbx.pyrx import _serde; _serde.TILE_SCHEMA`.
- **No `_jvm`/`sparkContext`/`.rdd`/`.conf.set`** anywhere in `ds/` (Serverless; Task 8 enforces).
- **Parity is pixel-level within tolerance, never byte-equal** (Task 9).
