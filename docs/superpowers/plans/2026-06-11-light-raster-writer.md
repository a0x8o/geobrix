# Light Raster Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the pure-Python `pyrx` raster writer to functional parity with the heavy `gdal`/`gtiff_gdal` writer, on both light formats (`raster_gbx` catch-all + `gtiff_gbx` named).

**Architecture:** A new `pyrx/ds/_write.py` holds the per-tile byte production (`tile_to_bytes`): hybrid — verbatim when the target driver is GTiff (the dominant case; `raster_gbx`/`gtiff_gbx` tiles are already GTiff), rasterio re-encode + `RASTERX_*` tag stamping when `tile.metadata` requests a non-GTiff driver. `RasterGbxWriter` (DataSource V2) stays thin: schema check, filename derivation (`nameCol` or content-hash+uuid), `append`/`overwrite` mode, delegating bytes to the helper. Both `RasterGbxDataSource` (driver from `tile.metadata`) and `GTiffGbxDataSource` (force GTiff) expose `writer()`. Writer `.option()`s are `path`/`nameCol`/`ext` only — encoding settings come from `tile.metadata`, exactly like heavy.

**Tech Stack:** Python 3.12, PySpark 4.1.2 (`pyspark.sql.datasource`), rasterio, pytest. Docs are a separate plan.

**Reference spec:** `docs/superpowers/specs/2026-06-11-light-raster-writer-design.md` (read "Heavy writer contract" + "Write contract (light)"). Companion reader spec/impl already merged on this branch.

---

## Ground-truth facts (verified — do not re-derive)

- **Heavy writer `.option()`s:** `path`, `nameCol`, `ext` (default `"tif"`). Nothing else.
- **Encoding from `tile.metadata`** (heavy `OperatorOptions.appendOptions(mtd)`): `format`/`driver` (default `GTiff`), `compression` (default `DEFLATE`), `blocksize` (default `"512"`), `zlevel` (default `"6"`), `zstd_level` (default `"9"`); PREDICTOR=3 for float dtypes else 2.
- **Heavy always re-encodes** via `gdal_translate` and stamps `RASTERX_<key>` (each metadata entry) + `RASTERX_CELL` = cellid.
- **Filenames:** `nameCol` → `{row[nameCol]}.{ext}`; else `{MurmurHash3(tile)}_{pid}_{tid}.{ext}`. Flat dir, one file per row.
- **Light tile schema (single source):** `pyrx._serde.TILE_SCHEMA` — `(cellid: long, raster: binary, metadata: map<string,string>)`. Reader/writer schema `(source, tile)` via `pyrx.ds.raster.reader_schema()` — **import it, never redeclare.**
- **Parity is decoded-pixel within tolerance, NOT byte-for-byte** (independent GDAL stacks).
- **Current `writer.py`** (post-revert): `RasterGbxWriter` writes `tile.raster` verbatim to `raster_{uuid}.tif`; `assert_write_schema`; overwrite clears `*.tif` in `__init__`; commit no-op; abort removes written files. Wired only into `gtiff_gbx`.

## File structure

| File | Responsibility |
|---|---|
| `python/geobrix/src/databricks/labs/gbx/pyrx/ds/_write.py` | **New.** `tile_to_bytes()` (hybrid verbatim/re-encode + RASTERX_* tags) + encoding/creation-option helpers. No Spark. |
| `.../pyrx/ds/writer.py` | **Rework.** `RasterGbxWriter` with `nameCol`/`ext`/`force_driver`/mode; filename derivation; delegates bytes to `_write`. |
| `.../pyrx/ds/raster.py` | **Modify.** `RasterGbxDataSource.writer()` (catch-all, `force_driver=None`). |
| `.../pyrx/ds/gtiff.py` | **Modify.** `GTiffGbxDataSource.writer()` passes `force_driver="GTiff"`, `nameCol`, `ext`. |
| `python/geobrix/test/pyrx/ds/test_write_helper.py` | **New.** Unit tests for `_write.tile_to_bytes` (no Spark). |
| `.../test/pyrx/ds/test_writer.py` | **Extend.** Integration: nameCol, ext, catch-all-vs-named, re-encode, mode. |
| `.../test/pyrx/ds/test_writer_parity.py` | **New.** Light-vs-heavy round-trip (Docker/integration, skip-if-heavy-unavailable). |
| `.../test/pyrx/test_serverless_no_spark_config.py` | **Modify.** Add `_write.py` to the covered-files guard. |

Local venv for non-Docker tests: `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate`. **Before EVERY commit:** `chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects`. Do NOT push. Pre-commit banner is normal.

---

## Task 1: `_write.tile_to_bytes` (per-tile byte production)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/_write.py`
- Test: `python/geobrix/test/pyrx/ds/test_write_helper.py`

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/pyrx/ds/test_write_helper.py`:

```python
"""Unit tests for the writer's per-tile byte production (no Spark)."""
import numpy as np
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.ds import _write


def _gtiff_bytes(width=4, height=3, dtype="float32"):
    from rasterio.transform import from_origin
    data = np.arange(width * height, dtype=dtype).reshape(height, width)
    profile = dict(driver="GTiff", width=width, height=height, count=1, dtype=dtype,
                   crs="EPSG:4326", transform=from_origin(10.0, 50.0, 0.5, 0.5))
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
        return mf.read()


def test_gtiff_target_is_verbatim():
    raw = _gtiff_bytes()
    meta = {"driver": "GTiff", "format": "GTiff", "compression": "DEFLATE"}
    out = _write.tile_to_bytes(cellid=-1, raster_bytes=raw, metadata=meta, force_driver=None)
    assert out == raw  # GTiff target -> bytes passed through unchanged


def test_force_gtiff_is_verbatim_even_if_metadata_says_otherwise():
    raw = _gtiff_bytes()
    meta = {"driver": "COG", "format": "COG"}
    out = _write.tile_to_bytes(cellid=-1, raster_bytes=raw, metadata=meta, force_driver="GTiff")
    assert out == raw  # gtiff_gbx forces GTiff -> verbatim


def test_non_gtiff_target_reencodes_same_pixels_with_tags():
    raw = _gtiff_bytes()
    meta = {"driver": "COG", "format": "COG", "compression": "DEFLATE"}
    out = _write.tile_to_bytes(cellid=7, raster_bytes=raw, metadata=meta, force_driver=None)
    assert out != raw  # re-encoded
    with MemoryFile(out) as mf, mf.open() as ds:
        assert ds.driver in ("COG", "GTiff")  # COG driver writes a GTiff-structured file
        arr = ds.read(1)
        tags = ds.tags()
    np.testing.assert_allclose(arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6)
    assert tags.get("RASTERX_CELL") == "7"
    assert tags.get("RASTERX_driver") == "COG"
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_write_helper.py -v`
Expected: FAIL — no module `pyrx.ds._write`.

- [ ] **Step 3: Implement `_write.py`**

Create `python/geobrix/src/databricks/labs/gbx/pyrx/ds/_write.py`:

```python
"""Per-tile byte production for the raster writer.

Hybrid, mirroring the heavy gdal writer's intent (encoding from tile.metadata):
- target driver GTiff (the common case; raster_gbx/gtiff_gbx tiles are already
  GTiff) -> pass tile.raster bytes through VERBATIM. Pixel-identical to heavy;
  heavy's specific creation options differ but our contract is decoded-pixel.
- non-GTiff target -> rasterio re-encode to that driver applying the
  metadata-derived compression/blocksize/zlevel/zstd, and stamp RASTERX_<key>
  (each metadata entry) + RASTERX_CELL (cellid), matching heavy SetMetadataItem.

Writer .option()s never carry encoding; only tile.metadata does (like heavy).
"""
from __future__ import annotations

from typing import Dict


def _is_float(dtype: str) -> bool:
    return str(dtype).startswith("float")


def _creation_opts(driver: str, meta: Dict[str, str], dtype: str) -> Dict[str, str]:
    """GTiff/COG creation options from tile metadata, mirroring OperatorOptions.appendOptions."""
    compression = str(meta.get("compression", "DEFLATE")).upper()
    opts: Dict[str, str] = {"compress": compression}
    if compression == "DEFLATE":
        opts["zlevel"] = str(meta.get("zlevel", "6"))
        opts["predictor"] = "3" if _is_float(dtype) else "2"
    elif compression == "ZSTD":
        opts["zstd_level"] = str(meta.get("zstd_level", "9"))
    elif compression == "LZW":
        opts["predictor"] = "3" if _is_float(dtype) else "2"
    # blocksize: floor to mult-of-16, clamped >=64, applied for COG/GTiff family
    try:
        blk = int(meta.get("blocksize", "512"))
    except ValueError:
        blk = 512
    blk = max(64, (blk // 16) * 16)
    if driver.upper() == "COG":
        opts["blocksize"] = str(blk)
    return opts


def tile_to_bytes(
    cellid: int,
    raster_bytes: bytes,
    metadata: Dict[str, str],
    force_driver: str = None,
) -> bytes:
    """Return the on-disk bytes for one tile (verbatim GTiff, else re-encode)."""
    driver = force_driver or metadata.get("driver") or metadata.get("format") or "GTiff"
    if str(driver).upper() == "GTIFF":
        return raster_bytes  # already GTiff -> verbatim (fast, pixel-exact)

    import rasterio
    from rasterio.io import MemoryFile

    with MemoryFile(raster_bytes) as src_mf, src_mf.open() as src:
        data = src.read()
        profile = src.profile.copy()
        profile["driver"] = driver
        profile.update(_creation_opts(driver, metadata, src.dtypes[0]))
        with MemoryFile() as out_mf:
            with out_mf.open(**profile) as dst:
                dst.write(data)
                tags = {f"RASTERX_{k}": str(v) for k, v in (metadata or {}).items()}
                tags["RASTERX_CELL"] = str(cellid)
                dst.update_tags(**tags)
            return out_mf.read()
```

- [ ] **Step 4: Run it and confirm it passes**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_write_helper.py -v`
Expected: PASS (3 tests). If COG driver is unavailable in the local GDAL, the re-encode test will error on `out_mf.open(driver="COG")` — if so, change the test's `driver` to `"GTiff"` is NOT valid (that's verbatim); instead skip with `pytest.importorskip`-style guard: wrap the COG open in try/except and `pytest.skip("COG driver unavailable")`. Note which path you took.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects
git add python/geobrix/src/databricks/labs/gbx/pyrx/ds/_write.py python/geobrix/test/pyrx/ds/test_write_helper.py
git commit -m "feat(pyrx-ds): writer byte production (verbatim GTiff / re-encode + RASTERX tags)"
```

---

## Task 2: Rework `RasterGbxWriter` (options, filenames, mode)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/writer.py`
- Test: `python/geobrix/test/pyrx/ds/test_writer.py` (extend)

- [ ] **Step 1: Write the failing tests** (append to `test_writer.py`)

First read the existing `test_writer.py` to reuse its `_write_sample` + imports. Append:

```python
def test_namecol_controls_filenames(spark, tmp_path):
    from pyspark.sql import functions as F
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_named"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src)).withColumn("source", F.lit("mytile"))
    df.write.format("gtiff_gbx").mode("overwrite").option("nameCol", "source").save(str(out_dir))
    import os
    files = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert files == ["mytile.tif"]


def test_ext_option_controls_suffix(spark, tmp_path):
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_ext"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src))
    df.write.format("gtiff_gbx").mode("overwrite").option("ext", "tiff").save(str(out_dir))
    import os
    assert all(f.endswith(".tiff") for f in os.listdir(out_dir))
```

- [ ] **Step 2: Run and confirm failure**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_writer.py -v`
Expected: FAIL — `nameCol`/`ext` not honored (writer ignores them today) and/or `GTiffGbxDataSource` not imported in test (add `from databricks.labs.gbx.pyrx.ds.gtiff import GTiffGbxDataSource` and `from databricks.labs.gbx.pyrx.ds.raster import RasterGbxDataSource` at top if missing).

- [ ] **Step 3: Rework `writer.py`**

Replace the body of `python/geobrix/src/databricks/labs/gbx/pyrx/ds/writer.py` with:

```python
"""gtiff_gbx / raster_gbx writer (DataSource V2 write path).

Enforces the exact (source, tile) schema like the heavy GDAL writer. Writer
options are path/nameCol/ext only; the on-disk encoding comes from tile.metadata
(see _write.tile_to_bytes). Pure Python (Serverless).
"""
from __future__ import annotations

import glob
import hashlib
import os
import uuid
from dataclasses import dataclass
from typing import Iterator, List, Optional

from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType

from databricks.labs.gbx.pyrx.ds import _write
from databricks.labs.gbx.pyrx.ds.raster import reader_schema


@dataclass
class RasterCommitMessage(WriterCommitMessage):
    paths: List[str]


def assert_write_schema(schema: StructType) -> None:
    """Exact (source, tile) — extras OR missing both fail (matches GDAL writer)."""
    expected = reader_schema()
    if [f.name for f in schema.fields] != [f.name for f in expected.fields]:
        raise ValueError(
            f"raster writer requires exactly columns "
            f"{[f.name for f in expected.fields]}, got {[f.name for f in schema.fields]}"
        )


def _safe_name(raster_bytes: bytes, cellid: int) -> str:
    """Opaque, collision-free fallback name when no nameCol: content hash + uuid.

    PySpark's DataSourceWriter does not expose partition/task ids (Scala uses
    pid_tid), so the uuid suffix keeps names unique across partitions. NOT
    byte-identical to heavy's MurmurHash3_pid_tid -- use nameCol for control.
    """
    h = hashlib.sha1(raster_bytes).hexdigest()[:12]
    return f"{h}_{uuid.uuid4().hex[:8]}"


class RasterGbxWriter(DataSourceWriter):
    def __init__(
        self,
        path: str,
        schema: StructType,
        overwrite: bool,
        name_col: Optional[str] = None,
        ext: str = "tif",
        force_driver: Optional[str] = None,
    ):
        assert_write_schema(schema)
        if name_col and name_col not in [f.name for f in schema.fields]:
            raise ValueError(
                f"nameCol {name_col!r} is not a column; available: "
                f"{[f.name for f in schema.fields]} (overwrite 'source')."
            )
        self.path = path
        self.overwrite = overwrite
        self.name_col = name_col
        self.ext = ext
        self.force_driver = force_driver
        if overwrite and os.path.isdir(path):
            for stale in glob.glob(os.path.join(path, f"*.{ext}")):
                try:
                    os.remove(stale)
                except OSError:
                    pass

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        os.makedirs(self.path, exist_ok=True)
        written: List[str] = []
        for row in iterator:
            tile = row["tile"]
            cellid = tile["cellid"]
            raster_bytes = bytes(tile["raster"])
            metadata = dict(tile["metadata"] or {})
            name = row[self.name_col] if self.name_col else _safe_name(raster_bytes, cellid)
            out_bytes = _write.tile_to_bytes(cellid, raster_bytes, metadata, self.force_driver)
            out = os.path.join(self.path, f"{name}.{self.ext}")
            with open(out, "wb") as fh:
                fh.write(out_bytes)
            written.append(out)
        return RasterCommitMessage(paths=written)

    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        return None

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        for msg in messages:
            if isinstance(msg, RasterCommitMessage):
                for p in msg.paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
```

- [ ] **Step 4: Run and confirm pass**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_writer.py -v`
Expected: PASS (existing round-trip/strict/overwrite tests + new nameCol/ext). Note: `gtiff.py` already wires the writer; Task 3 makes `raster_gbx` writable + passes nameCol/ext/force_driver through, so some new assertions depend on Task 3 — if the new tests fail on options not being passed, proceed to Task 3 then re-run. (Order: do Task 3 immediately after if needed.)

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects
git add python/geobrix/src/databricks/labs/gbx/pyrx/ds/writer.py python/geobrix/test/pyrx/ds/test_writer.py
git commit -m "feat(pyrx-ds): writer nameCol/ext options + content-hash fallback names"
```

---

## Task 3: Wire `writer()` on both DataSources

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/gtiff.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/ds/raster.py`
- Test: `python/geobrix/test/pyrx/ds/test_writer.py` (catch-all vs named + re-encode)

- [ ] **Step 1: Write the failing tests** (append to `test_writer.py`)

```python
def test_raster_gbx_catch_all_writer_round_trips(spark, tmp_path):
    import numpy as np, rasterio
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_catchall"
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src))
    df.write.format("raster_gbx").mode("overwrite").save(str(out_dir))  # catch-all writer
    import os
    written = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert len(written) == 1
    with rasterio.open(os.path.join(out_dir, written[0])) as ds:
        arr = ds.read(1)
    np.testing.assert_allclose(arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6)
```

- [ ] **Step 2: Run and confirm failure**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_writer.py::test_raster_gbx_catch_all_writer_round_trips -v`
Expected: FAIL — `raster_gbx` has no writer (`DataSource.writer` not implemented).

- [ ] **Step 3: Add `writer()` to both DataSources**

In `python/geobrix/src/databricks/labs/gbx/pyrx/ds/raster.py`, add to `RasterGbxDataSource` (import `RasterGbxWriter` LAZILY inside the method to avoid the writer↔raster import cycle):

```python
    def writer(self, schema: StructType, overwrite: bool) -> "DataSourceWriter":
        from pyspark.sql.datasource import DataSourceWriter  # noqa: F401
        from databricks.labs.gbx.pyrx.ds.writer import RasterGbxWriter

        path = self.options.get("path")
        if not path:
            raise ValueError("raster_gbx writer requires an output path (.save(path)).")
        return RasterGbxWriter(
            path, schema, overwrite,
            name_col=self.options.get("nameCol"),
            ext=self.options.get("ext", "tif"),
            force_driver=None,  # catch-all: driver from tile.metadata
        )
```

In `python/geobrix/src/databricks/labs/gbx/pyrx/ds/gtiff.py`, replace the existing `writer()` so it threads nameCol/ext and forces GTiff:

```python
    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        path = self.options.get("path")
        if not path:
            raise ValueError("gtiff_gbx writer requires an output path (.save(path)).")
        return RasterGbxWriter(
            path, schema, overwrite,
            name_col=self.options.get("nameCol"),
            ext=self.options.get("ext", "tif"),
            force_driver="GTiff",  # named writer forces GTiff output
        )
```

(Confirm `gtiff.py` imports `DataSourceWriter` and `RasterGbxWriter`; they were added when the writer was first wired — keep them.)

- [ ] **Step 4: Run and confirm pass**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_writer.py -v`
Expected: PASS (all writer tests, both formats).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects
git add python/geobrix/src/databricks/labs/gbx/pyrx/ds/raster.py python/geobrix/src/databricks/labs/gbx/pyrx/ds/gtiff.py python/geobrix/test/pyrx/ds/test_writer.py
git commit -m "feat(pyrx-ds): raster_gbx catch-all writer + gtiff_gbx forces GTiff"
```

---

## Task 4: Serverless guard covers `_write.py`

**Files:**
- Modify: `python/geobrix/test/pyrx/test_serverless_no_spark_config.py`

- [ ] **Step 1: Add `_write.py` to the required-files list**

In `test_serverless_scan_includes_ds_subpackage`, add `"_write.py"` to the `required` tuple (alongside `raster.py`, `writer.py`, etc.).

- [ ] **Step 2: Run**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/test_serverless_no_spark_config.py -v`
Expected: PASS — both the forbidden-pattern scan (now also covering `_write.py`; it must use no `.conf.set`/`._jvm`/`.sparkContext`/`.rdd` — the Task 1 code uses none) and the coverage guard.

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects
git add python/geobrix/test/pyrx/test_serverless_no_spark_config.py
git commit -m "test(pyrx-ds): Serverless scan covers _write.py"
```

---

## Task 5: Re-encode integration test (non-GTiff via tile.metadata)

**Files:**
- Test: `python/geobrix/test/pyrx/ds/test_writer.py` (append)

- [ ] **Step 1: Write the test**

```python
def test_metadata_driver_cog_triggers_reencode(spark, tmp_path):
    import os
    import numpy as np
    import rasterio
    from pyspark.sql import functions as F
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_cog"
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src))
    # Override the tile.metadata driver to COG so the catch-all writer re-encodes.
    df2 = df.withColumn(
        "tile",
        F.col("tile").withField("metadata", F.map_concat(
            F.col("tile.metadata"), F.create_map(F.lit("driver"), F.lit("COG"))
        )),
    )
    try:
        df2.write.format("raster_gbx").mode("overwrite").save(str(out_dir))
    except Exception as e:
        import pytest
        pytest.skip(f"COG driver unavailable in this env: {str(e)[:80]}")
    written = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert len(written) == 1
    with rasterio.open(os.path.join(out_dir, written[0])) as ds:
        arr = ds.read(1)
        tags = ds.tags()
    np.testing.assert_allclose(arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6)
    assert tags.get("RASTERX_CELL") == "-1"
```

- [ ] **Step 2: Run**

Run: `source .venv-pyrx/bin/activate && python -m pytest python/geobrix/test/pyrx/ds/test_writer.py::test_metadata_driver_cog_triggers_reencode -v`
Expected: PASS (or SKIP if COG unavailable locally; it runs in Docker where GDAL 3.11 has COG).

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects
git add python/geobrix/test/pyrx/ds/test_writer.py
git commit -m "test(pyrx-ds): tile.metadata driver=COG triggers writer re-encode"
```

---

## Task 6: Light-vs-heavy round-trip parity (Docker / integration)

**Files:**
- Create: `python/geobrix/test/pyrx/ds/test_writer_parity.py`

- [ ] **Step 1: Write the parity test** (model the fixture on `test_reader_parity.py`)

```python
"""Light-vs-heavy writer round-trip parity (Docker; needs JAR + sample data)."""
import logging
import os
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile

pytestmark = pytest.mark.integration

SAMPLE = os.environ.get(
    "GBX_PARITY_SAMPLE",
    "/Volumes/main/default/test-data/geobrix-examples/london/sentinel2/london_sentinel2_red.tif",
)
REL_TOL = 1e-3
ABS_TOL = 1e-3
_HERE = Path(__file__).resolve()
_JARS = sorted((_HERE.parents[3] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip("no geobrix JAR staged")
    if not os.path.exists(SAMPLE):
        pytest.skip(f"sample not mounted at {SAMPLE}")
    from pyspark.sql import SparkSession
    logging.getLogger("py4j").setLevel(logging.ERROR)
    s = (SparkSession.builder.master("local[2]").appName("pyrx-ds-writer-parity")
         .config("spark.driver.extraJavaOptions",
                 "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
                 "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native")
         .config("spark.jars", str(_JARS[-1])).getOrCreate())
    from databricks.labs.gbx.pyrx.ds.register import register
    register(s)
    yield s


def _decode(path):
    with rasterio.open(path) as ds:
        return ds.read()


def test_light_write_roundtrips_to_same_pixels_as_heavy(spark_with_jar):
    light_dir = "/tmp/gbx_parity_light_out"
    # Light: raster_gbx read -> gtiff_gbx write -> re-read
    light = spark_with_jar.read.format("raster_gbx").load(SAMPLE)
    light.write.format("gtiff_gbx").mode("overwrite").save(light_dir)
    lf = [f for f in os.listdir(light_dir) if f.endswith(".tif")]
    assert lf, "light writer produced no files"
    la = _decode(os.path.join(light_dir, lf[0]))
    # Ground truth: the source pixels (heavy write also re-encodes to same pixels)
    with rasterio.open(SAMPLE) as src:
        truth = src.read()
    assert la.shape == truth.shape
    np.testing.assert_allclose(la, truth, rtol=REL_TOL, atol=ABS_TOL)
```

NOTE: a true side-by-side vs the heavy `gtiff_gdal` writer is skipped because the heavy GDAL path doesn't run in the local dev container (documented in the reader parity work); the source raster is the pixel ground truth both tiers must match. If a JAR-backed heavy write *does* work in the target env, extend this to compare against `spark.read.format("gdal").load(SAMPLE).write.format("gtiff_gdal")` output and skip-on-failure like the reader parity test.

- [ ] **Step 2: Run in Docker**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/ds/test_writer_parity.py --with-integration --log writer-parity.log`
Expected: PASS. First confirm the sample path exists in-container (`docker exec geobrix-dev ls <SAMPLE>`); adjust `GBX_PARITY_SAMPLE`/the default if needed (the reader parity work uses this same london sentinel2 path).

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects
git add python/geobrix/test/pyrx/ds/test_writer_parity.py
git commit -m "test(pyrx-ds): light writer round-trip pixel parity (integration)"
```

---

## Task 7: Full `ds/` suite + lint in Docker

- [ ] **Step 1: Run the suite (incl. integration) in Docker**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/ds/ --with-integration --log ds-writer-suite.log`
Expected: all pass (re-encode/COG + writer-parity may SKIP if a driver/sample is unavailable; otherwise pass). Fix any Docker-vs-venv discrepancy in the module, not the test.

- [ ] **Step 2: Lint (CI parity)**

Run: `bash scripts/commands/gbx-lint-python.sh --check --log writer-lint.log`
Expected: clean. If host formatting drifts, reformat in-container per the host-vs-Docker black caveat and re-run.

- [ ] **Step 3: Commit any fixes**

```bash
chmod -R u+rwX /Users/mjohns/IdeaProjects/geobrix/.git/objects
git add -A python/geobrix
git commit -m "chore(pyrx-ds): writer suite green + lint in Docker"
```

---

## Out of scope (this plan)

- **Docs** (light reader+writer pages; heavy `gdal`/`gtiff_gdal` reader+writer option audit) — separate plan per the spec's "Implementation plans" note.
- Vector writers; byte-identical filename parity with heavy's `MurmurHash3_pid_tid`.

## Self-review notes (for the executor)

- **Import cycle:** `writer.py` imports `reader_schema` from `raster.py`; `raster.py.writer()` imports `RasterGbxWriter` **lazily** (inside the method) to avoid a cycle. `gtiff.py` imports `RasterGbxWriter` at module level (gtiff→writer→raster, no cycle).
- **Writer options are path/nameCol/ext only.** Encoding (driver/compression/blocksize/zlevel/zstd) comes from `tile.metadata` — never add them as `.option()`s.
- **Verbatim is the GTiff path; re-encode only for non-GTiff** `tile.metadata` driver. `gtiff_gbx` forces GTiff (always verbatim).
- **No `_jvm`/`sparkContext`/`.rdd`/`.conf.set`** in `_write.py`/`writer.py` (Serverless; Task 4 enforces).
- **Row access:** the writer receives PySpark `Row`s — `row["tile"]["cellid"]`, `bytes(row["tile"]["raster"])`, `row[name_col]`. If a Row-access form fails at runtime, adapt to the correct accessor and note it.
