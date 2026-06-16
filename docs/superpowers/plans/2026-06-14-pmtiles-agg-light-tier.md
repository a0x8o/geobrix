# Lightweight `gbx_pmtiles_agg` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a lightweight, Serverless-safe implementation of `gbx_pmtiles_agg` — a grouped aggregate that folds a group of `(bytes, z, x, y)` map tiles into one PMTiles v3 archive (BINARY) — behind the same SQL name and Column-wrapper import as the heavy tier.

**Architecture:** A tier-neutral `_agg_light.py` in the existing `databricks.labs.gbx.pmtiles` package holds a `pandas_udf` grouped aggregate that reuses the `ds.tiles` archive assembler (`SlippyGrid` + `build_header_info` + `pmtiles.writer.Writer`), writing to an in-memory `BytesIO`. A shared `register_pmtiles_agg(spark)` is wired into both `pyrx.register` and `pyvx.register` (PMTiles is format-agnostic — raster *or* vector tiles — so it belongs to neither raster nor vector). No new SQL name is introduced (`gbx_pmtiles_agg` already exists for heavy). Parity is decoded-tile parity (not byte-identical: heavy uses NONE internal-compression, light GZIP).

**Tech Stack:** Python 3.12, PySpark `pandas_udf` (GROUPED_AGG), the `pmtiles` PyPI lib (`>=3.4,<4`, in the `[light]` extra), `ds.tiles` assembler, pytest (Docker for Spark fixtures, JAR-gated for cross-tier parity).

**Spec:** `docs/superpowers/specs/2026-06-14-pmtiles-agg-light-tier-design.md`
**Branch:** `pygx-light` (add-on; does not "finish" the branch — pygx BNG Phase 2 follows separately).

---

## File Structure

| File | Responsibility |
|---|---|
| `python/geobrix/src/databricks/labs/gbx/pmtiles/_agg_light.py` (create) | `_assemble_archive(...)` BytesIO assembler, `_pmtiles_agg_udf` grouped-agg `pandas_udf`, `register_pmtiles_agg(spark)`, `_MAX_ARCHIVE_BYTES`, `_LIGHT_REGISTERED` flag |
| `python/geobrix/src/databricks/labs/gbx/pmtiles/__init__.py` (modify) | re-export `register_pmtiles_agg` |
| `python/geobrix/src/databricks/labs/gbx/pmtiles/functions.py` (modify, fallback only) | tier-aware `pmtiles_agg` wrapper *iff* Task 2's `call_function` test fails |
| `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (modify) | call `register_pmtiles_agg(spark)` at end of `register` |
| `python/geobrix/src/databricks/labs/gbx/pyvx/functions.py` (modify) | call `register_pmtiles_agg(spark)` at end of `register` |
| `python/geobrix/test/pmtiles/__init__.py` + `conftest.py` (create) | light no-JAR `spark` fixture |
| `python/geobrix/test/pmtiles/test_agg_light_core.py` (create) | Spark-free assembler tests |
| `python/geobrix/test/pmtiles/test_agg_light_udf.py` (create) | registered-UDF + wrapper + dual-register tests |
| `python/geobrix/test/pmtiles/test_serverless_safety.py` (create) | no `_jvm`/`conf`/`rdd` guard |
| `python/geobrix/test/ds/test_pmtiles_agg_parity.py` (create) | JAR-gated cross-tier decoded parity |
| `python/geobrix/src/databricks/labs/gbx/bench/readers.py` (modify) | `run_pmtiles_agg(...)` bench leg |
| `python/geobrix/src/databricks/labs/gbx/bench/cluster.py` (modify) | `_CELL_PMTILES_AGG` dispatch cell |
| `notebooks/tests/push_and_run_bench_on_cluster.py` (modify) | `--benchmark-pmtiles-agg` / `--pmtiles-agg-only` flags |
| `docs/docs/api/pmtiles-functions.mdx` (modify) | per-function tier: `gbx_pmtiles_agg` → `<Tier both/> <Impl groupedAgg/>` + lib note |
| `docs/docs/api/execution-tiers.mdx` (modify) | move `gbx_pmtiles_agg` out of heavy-only |
| `docs/docs/api/performance.mdx` + `benchmarking.mdx` (modify) | pmtiles_agg light-vs-heavy result |

---

## Task 1: Spark-free archive assembler core

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pmtiles/_agg_light.py`
- Test: `python/geobrix/test/pmtiles/test_agg_light_core.py`
- Create: `python/geobrix/test/pmtiles/__init__.py` (empty)

- [ ] **Step 1: Write the failing tests**

`python/geobrix/test/pmtiles/test_agg_light_core.py`:
```python
"""Spark-free tests for the light PMTiles archive assembler."""
import io

import pytest
from pmtiles.reader import MmapSource, Reader

from databricks.labs.gbx.pmtiles._agg_light import _assemble_archive, _MAX_ARCHIVE_BYTES

_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16          # sniffs as PNG
def _mvt(i):  # arbitrary non-magic bytes => sniffs as MVT
    return b"mvt-payload-" + bytes([i % 256]) + b"\x00\x01\x02"


def _decode(blob, tmp_path):
    p = tmp_path / "a.pmtiles"
    p.write_bytes(blob)
    out = {}
    with open(p, "rb") as f:
        r = Reader(MmapSource(f))
        for z in range(0, 6):
            n = 2 ** z
            for x in range(n):
                for y in range(n):
                    t = r.get(z, x, y)
                    if t is not None:
                        out[(z, x, y)] = t
    return out


def test_single_tile_roundtrip(tmp_path):
    blob = _assemble_archive([_mvt(1)], [3], [2], [4], {})
    assert blob is not None
    assert _decode(blob, tmp_path) == {(3, 2, 4): _mvt(1)}


def test_multi_zoom_roundtrip(tmp_path):
    data = [_mvt(1), _mvt(2), _mvt(3)]
    zs, xs, ys = [2, 3, 3], [1, 2, 5], [1, 4, 6]
    got = _decode(_assemble_archive(data, zs, xs, ys, {}), tmp_path)
    assert got == {(2, 1, 1): _mvt(1), (3, 2, 4): _mvt(2), (3, 5, 6): _mvt(3)}


def test_png_payload_roundtrip(tmp_path):
    got = _decode(_assemble_archive([_PNG], [1], [0], [0], {}), tmp_path)
    assert got == {(1, 0, 0): _PNG}


def test_metadata_roundtrip(tmp_path):
    blob = _assemble_archive([_mvt(1)], [0], [0], [0], {"name": "demo", "n": 1})
    p = tmp_path / "m.pmtiles"
    p.write_bytes(blob)
    with open(p, "rb") as f:
        r = Reader(MmapSource(f))
        assert r.metadata().get("name") == "demo"


def test_null_payloads_skipped(tmp_path):
    got = _decode(_assemble_archive([None, _mvt(2), None], [0, 1, 0], [0, 1, 0], [0, 1, 0], {}), tmp_path)
    assert got == {(1, 1, 1): _mvt(2)}


def test_empty_group_returns_none():
    assert _assemble_archive([], [], [], [], {}) is None
    assert _assemble_archive([None], [0], [0], [0], {}) is None


def test_duplicate_tileid_dropped(tmp_path):
    # two rows for the same (z,x,y): keep first, no Writer error
    got = _decode(_assemble_archive([_mvt(1), _mvt(9)], [2, 2], [1, 1], [1, 1], {}), tmp_path)
    assert got == {(2, 1, 1): _mvt(1)}


def test_cap_exceeded_raises():
    big = b"\x00" * (_MAX_ARCHIVE_BYTES + 1)
    with pytest.raises(ValueError, match="exceeds"):
        _assemble_archive([big], [0], [0], [0], {})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd python/geobrix && python -m pytest test/pmtiles/test_agg_light_core.py -q`
Expected: FAIL — `ModuleNotFoundError: ... pmtiles._agg_light`.

- [ ] **Step 3: Implement the assembler**

`python/geobrix/src/databricks/labs/gbx/pmtiles/_agg_light.py`:
```python
"""Lightweight gbx_pmtiles_agg — tier-neutral grouped aggregate.

PMTiles archives raster OR vector tiles, so this lives in the pmtiles package
(not pyrx/pyvx) and is registered from BOTH light tiers. Reuses the ds.tiles
assembler; writes to an in-memory BytesIO. Serverless-safe: spark.udf.register +
Column expressions only (no _jvm / spark.conf / rdd).
"""

from __future__ import annotations

import io
import json
from typing import Optional, Sequence

import pandas as pd
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import BinaryType
from pmtiles.tile import Compression, zxy_to_tileid
from pmtiles.writer import Writer

from databricks.labs.gbx.ds.tiles._header import build_header_info, sniff_tile_type
from databricks.labs.gbx.ds.tiles.grid import SlippyGrid

# Mirror heavy PMTilesAcc's 100 MiB accumulation cap so the failure mode matches.
_MAX_ARCHIVE_BYTES = 100 * 1024 * 1024

# Set True by register_pmtiles_agg; only consulted by the fallback wrapper path.
_LIGHT_REGISTERED = False


def _assemble_archive(
    data: Sequence,
    zs: Sequence,
    xs: Sequence,
    ys: Sequence,
    metadata: Optional[dict] = None,
) -> Optional[bytes]:
    """Fold a group's (bytes, z, x, y) tiles into one PMTiles v3 archive (bytes).

    Null payloads are skipped; an all-null/empty group returns None. Tiles are
    written in ascending Hilbert TileID order; duplicate (z,x,y) keep the first.
    """
    tiles = []
    seen = set()
    total = 0
    first_payload = None
    for d, z, x, y in zip(data, zs, xs, ys):
        if d is None:
            continue
        b = bytes(d)
        total += len(b)
        if total > _MAX_ARCHIVE_BYTES:
            raise ValueError(
                f"pmtiles_agg group payload exceeds {_MAX_ARCHIVE_BYTES} bytes; "
                "split into more groups or fewer tiles per archive"
            )
        tileid = zxy_to_tileid(int(z), int(x), int(y))
        if tileid in seen:
            continue
        seen.add(tileid)
        if first_payload is None:
            first_payload = b
        tiles.append((int(z), int(x), int(y), tileid, b))
    if not tiles:
        return None

    tile_type = sniff_tile_type(first_payload)
    info = build_header_info(
        [(z, x, y) for (z, x, y, _, _) in tiles],
        SlippyGrid(),
        tile_type,
        Compression.NONE,
        metadata or {},
    )
    buf = io.BytesIO()
    writer = Writer(buf)
    for (_, _, _, tileid, b) in sorted(tiles, key=lambda t: t[3]):
        writer.write_tile(tileid, b)
    writer.finalize(info.header_dict(), info.metadata)
    return buf.getvalue()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd python/geobrix && python -m pytest test/pmtiles/test_agg_light_core.py -q`
Expected: PASS (8 passed). If `MmapSource`/`Reader` import fails, confirm the `pmtiles` lib is installed in the venv (`pip show pmtiles`).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pmtiles/_agg_light.py python/geobrix/test/pmtiles/__init__.py python/geobrix/test/pmtiles/test_agg_light_core.py
git commit -m "feat(pmtiles): light pmtiles_agg archive assembler core

Co-authored-by: Isaac"
```

---

## Task 2: Grouped-agg UDF, register helper, and wrapper resolution

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pmtiles/_agg_light.py` (append the UDF + register helper)
- Modify: `python/geobrix/src/databricks/labs/gbx/pmtiles/__init__.py`
- Modify (fallback only): `python/geobrix/src/databricks/labs/gbx/pmtiles/functions.py`
- Create: `python/geobrix/test/pmtiles/conftest.py`
- Test: `python/geobrix/test/pmtiles/test_agg_light_udf.py`

This task RESOLVES the open question from the spec: does the existing `pmtiles.functions.pmtiles_agg` wrapper (which uses `f.call_function("gbx_pmtiles_agg", ...)`) compose with a registered pandas GROUPED_AGG UDF inside `.agg()`? The test decides; the fallback is implemented only if it fails.

- [ ] **Step 1: Create the light spark fixture**

`python/geobrix/test/pmtiles/conftest.py` (mirror `test/pyvx/conftest.py`):
```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("pmtiles-light-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    yield s
    s.stop()
```

- [ ] **Step 2: Write the failing tests**

`python/geobrix/test/pmtiles/test_agg_light_udf.py`:
```python
"""Registered-UDF tests for the light gbx_pmtiles_agg (no JAR)."""
import io

from pmtiles.reader import MmapSource, Reader
from pyspark.sql import functions as F

from databricks.labs.gbx.pmtiles import functions as pt
from databricks.labs.gbx.pmtiles._agg_light import register_pmtiles_agg

_MVT_A = b"mvt-a\x00\x01"
_MVT_B = b"mvt-b\x00\x02"


def _decode(blob, tmp_path):
    p = tmp_path / "r.pmtiles"
    p.write_bytes(blob)
    out = {}
    with open(p, "rb") as fh:
        r = Reader(MmapSource(fh))
        for z in range(0, 6):
            n = 2 ** z
            for x in range(n):
                for y in range(n):
                    t = r.get(z, x, y)
                    if t is not None:
                        out[(z, x, y)] = t
    return out


def _rows(spark):
    return spark.createDataFrame(
        [("grp", _MVT_A, 3, 2, 4), ("grp", _MVT_B, 3, 5, 6)],
        ["g", "tile", "z", "x", "y"],
    )


def test_wrapper_in_agg(spark, tmp_path):
    register_pmtiles_agg(spark)
    df = _rows(spark)
    out = df.groupBy("g").agg(pt.pmtiles_agg("tile", "z", "x", "y").alias("arc"))
    blob = out.collect()[0]["arc"]
    assert _decode(blob, tmp_path) == {(3, 2, 4): _MVT_A, (3, 5, 6): _MVT_B}


def test_sql_name_in_agg(spark, tmp_path):
    register_pmtiles_agg(spark)
    _rows(spark).createOrReplaceTempView("tiles_v")
    blob = spark.sql(
        "SELECT gbx_pmtiles_agg(tile, z, x, y) AS arc FROM tiles_v GROUP BY g"
    ).collect()[0]["arc"]
    assert _decode(blob, tmp_path) == {(3, 2, 4): _MVT_A, (3, 5, 6): _MVT_B}


def test_metadata_passthrough(spark, tmp_path):
    register_pmtiles_agg(spark)
    df = _rows(spark).withColumn("meta", F.lit('{"name": "demo"}'))
    out = df.groupBy("g").agg(
        pt.pmtiles_agg("tile", "z", "x", "y", "meta").alias("arc")
    )
    blob = out.collect()[0]["arc"]
    p = tmp_path / "md.pmtiles"
    p.write_bytes(blob)
    with open(p, "rb") as fh:
        assert Reader(MmapSource(fh)).metadata().get("name") == "demo"
```

- [ ] **Step 3: Run tests to verify they fail**

Run (in Docker — needs Spark + the light env; see CLAUDE.md doc-test note):
`bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pmtiles/test_agg_light_udf.py --log pmtiles-agg-udf.log`
Expected: FAIL — `register_pmtiles_agg` import error (not yet defined).

- [ ] **Step 4: Implement the UDF + register helper**

Append to `python/geobrix/src/databricks/labs/gbx/pmtiles/_agg_light.py`:
```python
@pandas_udf(BinaryType())
def _pmtiles_agg_udf(
    data: pd.Series,
    z: pd.Series,
    x: pd.Series,
    y: pd.Series,
    metadata_json: pd.Series,
) -> Optional[bytes]:
    """GROUPED_AGG: fold one group's tiles into a PMTiles archive (BINARY)."""
    meta = {}
    if metadata_json is not None and len(metadata_json) > 0:
        for m in metadata_json:
            if m is not None and str(m).strip():
                meta = json.loads(m)
                break
    return _assemble_archive(data, z, x, y, meta)


def register_pmtiles_agg(spark: SparkSession = None) -> None:
    """Register the light gbx_pmtiles_agg grouped aggregate (Serverless-safe).

    Called by both pyrx.register and pyvx.register, and usable standalone.
    """
    global _LIGHT_REGISTERED
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    spark.udf.register("gbx_pmtiles_agg", _pmtiles_agg_udf)
    _LIGHT_REGISTERED = True
```

`python/geobrix/src/databricks/labs/gbx/pmtiles/__init__.py` (replace empty file):
```python
from databricks.labs.gbx.pmtiles._agg_light import register_pmtiles_agg

__all__ = ["register_pmtiles_agg"]
```

- [ ] **Step 5: Run tests**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pmtiles/test_agg_light_udf.py --log pmtiles-agg-udf.log`
Expected outcomes:
- **All 3 PASS** → the `call_function` wrapper composes; **do nothing to `functions.py`**. Skip Step 6.
- **`test_sql_name_in_agg` passes but `test_wrapper_in_agg` FAILS** (the wrapper's `call_function` doesn't resolve the registered pandas UDAF in the DataFrame `.agg()`) → apply Step 6 (fallback), then re-run.

- [ ] **Step 6 (FALLBACK — apply only if `test_wrapper_in_agg` failed): tier-aware wrapper**

Edit `python/geobrix/src/databricks/labs/gbx/pmtiles/functions.py` — make `pmtiles_agg` prefer the directly-callable light UDF when the light tier is registered (mirrors the pygx `quadbin_cellunion_agg` direct-object pattern), else fall back to `call_function` for heavy:
```python
def pmtiles_agg(bytes_col, z, x, y, metadata_json=None):
    meta = f.lit("{}") if metadata_json is None else _col(metadata_json)
    from databricks.labs.gbx.pmtiles import _agg_light
    if _agg_light._LIGHT_REGISTERED:
        return _agg_light._pmtiles_agg_udf(
            _col(bytes_col), _col(z), _col(x), _col(y), meta
        )
    return f.call_function(
        "gbx_pmtiles_agg", _col(bytes_col), _col(z), _col(x), _col(y), meta
    )
```
(Preserve the existing `meta` defaulting logic already in `functions.py`; only add the `_LIGHT_REGISTERED` branch.) Re-run Step 5 → all 3 PASS.

- [ ] **Step 7: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pmtiles/ python/geobrix/test/pmtiles/conftest.py python/geobrix/test/pmtiles/test_agg_light_udf.py
git commit -m "feat(pmtiles): light pmtiles_agg grouped-agg UDF + register helper

Resolve the wrapper path (call_function vs direct udf object) by test.

Co-authored-by: Isaac"
```

---

## Task 3: Wire into pyrx + pyvx register (and standalone)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (end of `register`, ~line 101)
- Modify: `python/geobrix/src/databricks/labs/gbx/pyvx/functions.py` (end of `register`, ~line 279)
- Test: `python/geobrix/test/pmtiles/test_agg_light_udf.py` (append)

- [ ] **Step 1: Write the failing tests**

Append to `python/geobrix/test/pmtiles/test_agg_light_udf.py`:
```python
def _agg_registered(spark):
    names = {r.function for r in spark.sql("SHOW USER FUNCTIONS").collect()}
    return any(n.endswith("gbx_pmtiles_agg") for n in names)


def test_pyrx_register_installs_pmtiles_agg(spark):
    from databricks.labs.gbx.pyrx import functions as rx
    rx.register(spark)
    assert _agg_registered(spark)


def test_pyvx_register_installs_pmtiles_agg(spark):
    from databricks.labs.gbx.pyvx import functions as vx
    vx.register(spark)
    assert _agg_registered(spark)
```

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path "python/geobrix/test/pmtiles/test_agg_light_udf.py::test_pyrx_register_installs_pmtiles_agg python/geobrix/test/pmtiles/test_agg_light_udf.py::test_pyvx_register_installs_pmtiles_agg" --log pmtiles-reg.log`
Expected: FAIL (the registers don't yet install `gbx_pmtiles_agg`).

- [ ] **Step 3: Add the hook to pyrx**

In `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`, at the END of `register` (after the last `spark.udtf.register("gbx_rst_xyzpyramid", _RstXyzPyramidUDTF)` line ~101), append:
```python
    # PMTiles archive aggregate is format-agnostic (raster or vector tiles);
    # register it from the light raster tier too.
    from databricks.labs.gbx.pmtiles import register_pmtiles_agg
    register_pmtiles_agg(spark)
```

- [ ] **Step 4: Add the hook to pyvx**

In `python/geobrix/src/databricks/labs/gbx/pyvx/functions.py`, at the END of `register` (after the last `spark.udtf.register("gbx_st_interpolateelevationgeom", _InterpElevGeomUDTF)` line ~279), append:
```python
    from databricks.labs.gbx.pmtiles import register_pmtiles_agg
    register_pmtiles_agg(spark)
```

- [ ] **Step 5: Run tests**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pmtiles/ --log pmtiles-all.log`
Expected: PASS (all pmtiles light tests).

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py python/geobrix/src/databricks/labs/gbx/pyvx/functions.py python/geobrix/test/pmtiles/test_agg_light_udf.py
git commit -m "feat(pmtiles): register pmtiles_agg from pyrx and pyvx

Co-authored-by: Isaac"
```

---

## Task 4: Serverless-safety guard

**Files:**
- Test: `python/geobrix/test/pmtiles/test_serverless_safety.py` (create)

- [ ] **Step 1: Write the test**

Mirror the established Serverless guard (no `_jvm` / `sparkContext._jsc` / `.rdd` / `spark.conf.set` in the light source):
```python
"""The light pmtiles_agg module must be Serverless/Connect-safe."""
import inspect

from databricks.labs.gbx.pmtiles import _agg_light

_FORBIDDEN = ("_jvm", "sparkContext", ".rdd", "spark.conf.set", "_jsc")


def test_no_spark_internal_access():
    src = inspect.getsource(_agg_light)
    for bad in _FORBIDDEN:
        assert bad not in src, f"Serverless-unsafe access: {bad}"
```

- [ ] **Step 2: Run**

Run: `cd python/geobrix && python -m pytest test/pmtiles/test_serverless_safety.py -q`
Expected: PASS (the module uses only `spark.udf.register` + Column exprs).

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/pmtiles/test_serverless_safety.py
git commit -m "test(pmtiles): Serverless-safety guard for light pmtiles_agg

Co-authored-by: Isaac"
```

---

## Task 5: JAR-gated cross-tier decoded parity

**Files:**
- Test: `python/geobrix/test/ds/test_pmtiles_agg_parity.py` (create)

Mirror the JAR gating + decode helper from `test/ds/test_pmtiles_parity.py`. Register light, capture archive; register heavy, capture archive; assert decoded tile-dicts + metadata equal (NOT byte-identical).

- [ ] **Step 1: Write the test**

```python
"""Cross-tier decoded parity for gbx_pmtiles_agg (light vs heavy). JAR-gated."""
from pathlib import Path

import pytest
from pmtiles.reader import MmapSource, Reader

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
_JARS = sorted((_HERE.parents[3] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

# Two MVT-ish payloads + one POLYGON-derived payload across two zooms.
_TILES = [
    ("g", b"tile-point-0\x00", 3, 2, 4),
    ("g", b"tile-point-1\x00", 3, 5, 6),
    ("g", b"tile-polygon-\x07\x08\x09", 2, 1, 1),
]


def _decode(path):
    out = {}
    with open(path, "rb") as f:
        r = Reader(MmapSource(f))
        for z in range(0, 8):
            n = 2 ** z
            for x in range(n):
                for y in range(n):
                    t = r.get(z, x, y)
                    if t is not None:
                        out[(z, x, y)] = t
    return out, r.metadata()


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip("no geobrix JAR staged")
    from pyspark.sql import SparkSession
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pmtiles-agg-parity")
        .config("spark.jars", str(_JARS[0]))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


def _archive(spark, register_fn, tmp_path, name):
    register_fn(spark)
    from databricks.labs.gbx.pmtiles import functions as pt
    df = spark.createDataFrame(_TILES, ["g", "tile", "z", "x", "y"])
    blob = (
        df.groupBy("g")
        .agg(pt.pmtiles_agg("tile", "z", "x", "y").alias("arc"))
        .collect()[0]["arc"]
    )
    p = tmp_path / f"{name}.pmtiles"
    p.write_bytes(blob)
    return _decode(p)


def test_decoded_tile_parity(spark_with_jar, tmp_path):
    from databricks.labs.gbx.pmtiles._agg_light import register_pmtiles_agg
    from databricks.labs.gbx.pmtiles import functions as heavy_pt

    light_tiles, _ = _archive(spark_with_jar, register_pmtiles_agg, tmp_path, "light")
    heavy_tiles, _ = _archive(spark_with_jar, heavy_pt.register, tmp_path, "heavy")
    assert light_tiles == heavy_tiles
```

- [ ] **Step 2: Run (requires a staged JAR; otherwise skips)**

Build + stage the JAR into `python/geobrix/lib/` (see CLAUDE.md / `gbx:data:push-jar` builds the fat jar; for local parity, copy the `*-jar-with-dependencies.jar` into `python/geobrix/lib/`). Then:
Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_pmtiles_agg_parity.py --log pmtiles-agg-parity.log`
Expected: PASS, or SKIP ("no geobrix JAR staged") when no JAR is present.

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/ds/test_pmtiles_agg_parity.py
git commit -m "test(pmtiles): JAR-gated cross-tier decoded parity for pmtiles_agg

Co-authored-by: Isaac"
```

---

## Task 6: Documentation (tier flip + lib note)

**Files:**
- Modify: `docs/docs/api/pmtiles-functions.mdx`
- Modify: `docs/docs/api/execution-tiers.mdx`

- [ ] **Step 1: Per-function tier on the PMTiles functions page**

`docs/docs/api/pmtiles-functions.mdx` is page-level `<Tier heavy/>` (line ~11). It can no longer be page-level heavy. Add `import { Impl } from '@site/src/components/Tier';` if not present, change the page-level statement to scope it to the still-heavy entries, and under the `gbx_pmtiles_agg` heading place:
```mdx
<Tier both/> <Impl groupedAgg/>

:::note Lightweight tier
Powered by the **pmtiles** package. Grouped aggregate — `groupBy(...).agg(pt.pmtiles_agg("tile", "z", "x", "y"))` folds a group's `(bytes, z, x, y)` tiles into one PMTiles v3 archive (BINARY). Registered by both `pyrx.register` and `pyvx.register` (PMTiles archives raster or vector tiles).
:::
```
Leave any genuinely heavy-only PMTiles entries as `<Tier heavy/>`.

- [ ] **Step 2: execution-tiers.mdx**

Move `gbx_pmtiles_agg` out of the heavy-only column/list into the both-tiers grouping (grep `pmtiles_agg` in `docs/docs/api/execution-tiers.mdx`; if it's listed under a heavy-only section, relocate it).

- [ ] **Step 3: Verify the docs build**

Run: `cd docs && npm run build`
Expected: SUCCESS, no broken links. Then `grep -rn -iE "wave [0-9]+" docs/docs/` → empty.

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/docs/api/pmtiles-functions.mdx docs/docs/api/execution-tiers.mdx
git commit -m "docs(pmtiles): gbx_pmtiles_agg now both tiers (grouped-agg + lib note)

Co-authored-by: Isaac"
```

---

## Task 7: Bench leg — `run_pmtiles_agg`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/readers.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/cluster.py`
- Modify: `notebooks/tests/push_and_run_bench_on_cluster.py`

Mirror `run_mvt_agg` (`bench/readers.py:857-1120`) — a grouped-agg leg — NOT `run_pmtiles_write`. Read `run_mvt_agg` in full and copy its structure (tier register → synthetic corpus → validation pass → `time_iters` → `ResultRow`), changing the aggregate and the corpus to PMTiles tiles.

- [ ] **Step 1: Add `run_pmtiles_agg` to `bench/readers.py`**

```python
def run_pmtiles_agg(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,            # "lightweight" or "heavyweight"
    n_tiles: int = 1000,
    n_groups: int = 1,
    where: str = "cluster",
) -> "ResultRow":
    """Grouped-agg bench: fold n_tiles synthetic PNG tiles into PMTiles archive(s)."""
    from pyspark.sql import functions as F

    if api == "lightweight":
        from databricks.labs.gbx.pmtiles import register_pmtiles_agg
        register_pmtiles_agg(spark)
        from databricks.labs.gbx.pmtiles import functions as pt
    else:
        from databricks.labs.gbx.pmtiles import functions as pt
        pt.register(spark)

    # Synthetic PNG tiles at zoom z over a 2^z grid, split into n_groups.
    z = 6
    n = 2 ** z
    png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 256
    rows = []
    for i in range(n_tiles):
        x, y = i % n, (i // n) % n
        rows.append((i % n_groups, bytearray(png), z, x, y))
    df = (
        spark.createDataFrame(rows, ["g", "tile", "z", "x", "y"])
        .withColumn("tile", F.col("tile").cast("binary"))
        .cache()
    )
    df.count()

    def _job():
        return (
            df.groupBy("g")
            .agg(pt.pmtiles_agg("tile", "z", "x", "y").alias("arc"))
            .count()
        )

    _job()  # validation pass (untimed)
    median = time_iters(_job, warmup, measured)
    df.unpersist()
    return ResultRow(
        run_id=run_id,
        category="pmtiles_agg",
        mode="spark-path",
        api=api,
        fn="pmtiles_agg",
        row_count=n_tiles,
        median_seconds=median,
        parity_status="n/a",
        where=where,
    )
```
**Note:** match `ResultRow(...)`'s exact keyword set to the one `run_mvt_agg` uses (read its return statement and copy the field names verbatim — adjust only `category`/`fn`/`row_count`). If `time_iters`/`ResultRow` need imports, they're already imported at the top of `readers.py` (used by `run_mvt_agg`).

- [ ] **Step 2: Add the cluster dispatch cell**

In `bench/cluster.py`, add `_CELL_PMTILES_AGG` modeled on `_CELL_PMTILES` (lines 640-754) but calling `run_pmtiles_agg` for both `api="lightweight"` and `api="heavyweight"` and appending both ResultRows. Wire it near line 1779:
```python
    if benchmark_pmtiles_agg or pmtiles_agg_only:
        cells.append(_cell(_CELL_PMTILES_AGG))
```
Add `benchmark_pmtiles_agg=False, pmtiles_agg_only=False` to `build_notebook(...)`'s signature alongside the existing `benchmark_pmtiles`/`pmtiles_only` params.

- [ ] **Step 3: Add launcher flags**

In `notebooks/tests/push_and_run_bench_on_cluster.py`, alongside `--benchmark-pmtiles`/`--pmtiles-only` (parsed ~lines 254-267), add:
```python
    parser.add_argument("--benchmark-pmtiles-agg", action="store_true")
    parser.add_argument("--pmtiles-agg-only", action="store_true")
```
and thread `benchmark_pmtiles_agg=args.benchmark_pmtiles_agg, pmtiles_agg_only=args.pmtiles_agg_only` into the `cluster.build_notebook(...)` call.

- [ ] **Step 4: Local smoke test**

Run: `cd python/geobrix && python -c "from databricks.labs.gbx.bench.readers import run_pmtiles_agg; print('import ok')"`
Expected: `import ok` (no syntax/name errors). The full leg runs on cluster in Task 8.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/bench/readers.py python/geobrix/src/databricks/labs/gbx/bench/cluster.py notebooks/tests/push_and_run_bench_on_cluster.py
git commit -m "feat(bench): light-vs-heavy pmtiles_agg grouped-agg leg

Co-authored-by: Isaac"
```

---

## Task 8: Cluster bench run + benchmarking docs

**Files:**
- Modify: `docs/docs/api/benchmarking.mdx`
- Modify: `docs/docs/api/performance.mdx`

- [ ] **Step 1: Build + stage artifacts, then run the leg on the warm bench cluster**

Per the bench memories: build+stage BOTH the fat jar + tests.jar before cluster start (`gbx:data:push-jar` / `push-wheel`), keep the standing 0519 bench cluster warm. Run ONLY the pmtiles_agg leg at 1000 tiles, both tiers:
```bash
python notebooks/tests/push_and_run_bench_on_cluster.py --pmtiles-agg-only --row-counts 1000 --spark-measured 5
```
Verify exactly one geobrix-bench run on the cluster (don't double-launch). Capture the light-vs-heavy median + decoded parity status.

- [ ] **Step 2: Update benchmarking.mdx + performance.mdx**

Add the pmtiles_agg result to `docs/docs/api/benchmarking.mdx` (#results) and the execution-shape/narrative to `performance.mdx`, framed noise-aware (grouped-agg, decoded parity, the GZIP-vs-NONE internal-compression note). Per the standing rule, any bench change must be reflected in `benchmarking.mdx` in the same stroke. Give the run's `bench-out/<run_id>/summary.md` link.

- [ ] **Step 3: Verify docs build + commit**

```bash
cd docs && npm run build   # SUCCESS, no broken links
grep -rn -iE "wave [0-9]+" docs/docs/   # empty
cd .. && chmod -R u+rwX .git/objects
git add docs/docs/api/benchmarking.mdx docs/docs/api/performance.mdx
git commit -m "docs(bench): pmtiles_agg light-vs-heavy result

Co-authored-by: Isaac"
```

- [ ] **Step 4: Push the branch**

```bash
gh auth switch --user mjohns-databricks
export QC_OVERRIDE=1
git push origin pygx-light
```

---

## Final review

- [ ] Dispatch a final code-reviewer over the whole add-on (the `_agg_light` module, the two register hooks, all tests, the bench leg, the docs). Confirm: decoded parity (not byte) is the asserted contract; no new SQL name leaked into `registered_functions.txt`/`function-info.json`; Serverless-safe; `gbx:test:bindings` still green; no wave/internal vocab in docs.
- [ ] Confirm `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pmtiles/` is green and `test/ds/test_pmtiles_agg_parity.py` passes (or skips cleanly without a JAR).

---

## Self-review notes (author)

- **Spec coverage:** neutral home (Task 1-2), dual register + standalone (Task 3), decoded parity (Task 5), no-new-SQL-name (called out, Task 6 confirms bindings untouched), Serverless safety (Task 4), docs tier flip + lib note + execution-tiers + benchmarking (Task 6, 8), bench leg (Task 7-8), the `call_function` wrapper question resolved by test with documented fallback (Task 2). 100 MiB cap + magic-byte sniff + Hilbert order + leaf-dir capability all in the Task-1 assembler. All spec sections map to a task.
- **Type/name consistency:** `_assemble_archive(data, zs, xs, ys, metadata)`, `_pmtiles_agg_udf(data, z, x, y, metadata_json)`, `register_pmtiles_agg(spark)`, `_MAX_ARCHIVE_BYTES`, `_LIGHT_REGISTERED` — used identically across Tasks 1-7. `SlippyGrid`, `build_header_info`, `sniff_tile_type`, `zxy_to_tileid`, `Compression.NONE`, `pmtiles.writer.Writer`, `pmtiles.reader.{MmapSource,Reader}` match the recon'd real symbols.
- **No placeholders:** every code step has real code; the one "mirror `run_mvt_agg`" step in Task 7 includes a complete runnable function plus the explicit delta (copy `ResultRow` kwargs verbatim) because the bench leg intentionally mirrors an existing 260-line function.
