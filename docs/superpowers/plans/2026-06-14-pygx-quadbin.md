# pygx Phase 1 — quadbin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the 10 lightweight quadbin GridX functions in a new pure-Python `databricks.labs.gbx.pygx` package, at exact cell-set parity with the heavy Scala tier.

**Architecture:** Pure-Python/PySpark, Serverless/Connect-safe (only `spark.udf.register`/`spark.udtf.register` + Column exprs — never `_jvm`/`spark.conf.set`/`.rdd`). Cell math via the `quadbin` PyPI lib (already a dep) + ported `Quadbin.scala` logic where the lib lacks a primitive; geometry via shapely → **EWKB** (SRID 4326). Mirrors the just-completed `pyvx` package patterns.

**Tech Stack:** Python 3.12, `quadbin` (0.2.x), `shapely` 2.x, PySpark `@udf`/`pandas_udf`. **No new dependencies** (`quadbin` + `shapely` already in the `[light]` extra).

**Spec:** `docs/superpowers/specs/2026-06-14-pygx-light-tier-design.md` (Phase 1). **Branch:** `pygx-light`. Out of scope: BNG (separate plan), `gbx_custom_*` (stays heavy-only).

---

## The 10 functions (parity targets, all `gbx_quadbin_*`)

| Function | Shape | Output | Light source |
|---|---|---|---|
| `pointascell(lon,lat,res)` | scalar | BIGINT | `quadbin.point_to_cell(lon,lat,res)`; res ∈ [0,26] |
| `resolution(cell)` | scalar | INT | `quadbin.get_resolution(cell)` |
| `kring(cell,k)` | scalar | ARRAY\<BIGINT\> | `quadbin.k_ring(cell,k)`; k ≥ 0 |
| `distance(a,b)` | scalar | INT | **custom**: same-res-or-error, Chebyshev on `cell_to_tile` coords |
| `polyfill(geom,res)` | scalar | ARRAY\<BIGINT\> | **custom**: bbox-cell enumeration matching `Quadbin.scala`; res ∈ [0,20] |
| `aswkb(cell)` | scalar | BINARY (EWKB polygon, SRID 4326) | `cell_to_bounding_box` → shapely box → `to_wkb(include_srid=True)` |
| `centroid(cell)` | scalar | BINARY (EWKB point) | `cell_to_point` → shapely Point → EWKB |
| `cellunion(cells)` | scalar | BINARY (EWKB MultiPolygon) | per-cell polygon → shapely `unary_union` → EWKB |
| `tessellate(geom,res)` | scalar | ARRAY\<STRUCT\<cell:BIGINT, geom:BINARY\>\> | polyfill bbox → per-cell shapely intersection → EWKB |
| `cellunion_agg(cell)` | grouped-agg | BINARY (EWKB MultiPolygon) | grouped-agg `pandas_udf` returning BINARY directly |

Heavy reference: `src/main/scala/com/databricks/labs/gbx/gridx/quadbin/` + `gridx/grid/Quadbin.scala`. Key heavy facts: `resolution = ((cell >>> 52) & 0x1f).toInt`; geometry outputs use `JTS.toEWKB` after `setSRID(4326)`; `distance` requires equal resolution; `polyfill`/`tessellate` use the geometry **envelope** (bbox), res ≤ 20.

## Conventions (every task)
- Spark-free core tests (`_quadbin.py`) run on host: `.venv-pyrx/bin/python -m pytest <path> -v`. Registered-fn + parity tests run in the `geobrix-dev` container: `bash scripts/commands/gbx-test-python.sh --path <path> --log <name>.log`.
- Commit (no push unless a task says so): `chmod -R u+rwX .git/objects`; subject ≤72 + WHY body; trailer exactly `Co-authored-by: Isaac`.
- Serverless guard: never add `_jvm`/`sparkContext`/`.rdd`/`spark.conf.set` to `pygx`.

## File Structure
| File | Responsibility |
|---|---|
| `python/geobrix/src/databricks/labs/gbx/pygx/__init__.py` | package marker (docstring) |
| `pygx/_env.py` | `assert_quadbin_available()` (quadbin + shapely guard) |
| `pygx/_geom.py` | `parse_geom` (WKB/EWKB/WKT/EWKT) — copy of pyvx's |
| `pygx/_serde.py` | `QUADBIN_CELL_SCHEMA` (tessellate struct) |
| `pygx/_quadbin.py` | cell math (lib + ported logic) + shapely EWKB geometry build |
| `pygx/functions.py` | `register(spark)` + UDFs/agg + Column wrappers |
| `python/geobrix/test/pygx/conftest.py` | spark fixture (copy of pyvx's) |
| `python/geobrix/test/pygx/test_quadbin_core.py` | Spark-free `_quadbin` unit tests |
| `python/geobrix/test/pygx/test_quadbin_udf.py` | registered-fn tests |
| `python/geobrix/test/pygx/test_parity_quadbin.py` | JAR-gated cross-tier parity |
| `python/geobrix/test/pygx/test_geom.py` | `parse_geom` unit tests |

---

## Task 1: Package skeleton + env guard + geom + serde

**Files:** create `pygx/__init__.py`, `pygx/_env.py`, `pygx/_geom.py`, `pygx/_serde.py`, `test/pygx/__init__.py`, `test/pygx/conftest.py`, `test/pygx/test_geom.py`.

- [ ] **Step 1: failing test** `test/pygx/test_geom.py`
```python
import pytest
shapely = pytest.importorskip("shapely")
from shapely import to_wkb, set_srid, get_srid  # noqa: E402
from shapely.geometry import Point  # noqa: E402
from databricks.labs.gbx.pygx import _geom


def test_parse_wkb_wkt_ewkt_ewkb_none():
    assert _geom.parse_geom(None) is None
    assert _geom.parse_geom("") is None
    assert _geom.parse_geom(to_wkb(Point(1, 2))).equals(Point(1, 2))
    assert _geom.parse_geom("POINT (1 2)").equals(Point(1, 2))
    g = _geom.parse_geom("SRID=4326;POINT (1 2)")
    assert g.equals(Point(1, 2)) and get_srid(g) == 4326
    e = _geom.parse_geom(to_wkb(set_srid(Point(1, 2), 4326), include_srid=True))
    assert get_srid(e) == 4326
```

- [ ] **Step 2: run → FAIL** `.venv-pyrx/bin/python -m pytest python/geobrix/test/pygx/test_geom.py -v` (no `pygx`).

- [ ] **Step 3: implement the package files**

`pygx/__init__.py`:
```python
"""pygx — pure-Python/PySpark light GridX tier (Serverless-safe).

Mirrors the heavyweight ``gridx`` functions (``gbx_quadbin_*``, ``gbx_bng_*``)
with no JVM, no JAR, no native GDAL. See databricks.labs.gbx.pygx.functions.
"""
```

`pygx/_geom.py` — copy `python/geobrix/src/databricks/labs/gbx/pyvx/_geom.py` verbatim (the `parse_geom(x)` handling WKB/EWKB bytes + WKT/EWKT text, empty→None, `SRID=` prefix → `set_srid`).

`pygx/_env.py`:
```python
"""Environment checks for the pygx light tier."""


def assert_quadbin_available() -> None:
    """Raise a clear ImportError if the quadbin light deps are missing."""
    missing = []
    try:
        import quadbin  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("quadbin")
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("shapely")
    if missing:
        raise ImportError(
            "pygx quadbin requires the [light] extra; missing: "
            + ", ".join(missing)
            + ". Install with: pip install 'geobrix[light]'"
        )
```

`pygx/_serde.py`:
```python
from pyspark.sql.types import BinaryType, LongType, StructField, StructType

QUADBIN_CELL_SCHEMA = StructType(
    [
        StructField("cell", LongType(), False),
        StructField("geom", BinaryType(), True),
    ]
)
```

`test/pygx/__init__.py` — empty. `test/pygx/conftest.py` — copy `test/pyvx/conftest.py` verbatim (change `appName` to `"pygx-tests"`).

- [ ] **Step 4: run → PASS** (6 assertions).

- [ ] **Step 5: commit**
```bash
git add python/geobrix/src/databricks/labs/gbx/pygx/ python/geobrix/test/pygx/
git commit -m "feat(pygx): package skeleton + geom/env/serde for quadbin

New pure-Python GridX package; parse_geom (WKB/EWKB/WKT/EWKT) mirroring pyvx,
quadbin/shapely env guard, tessellate cell struct schema. Spark-free, TDD."
```

---

## Task 2: `_quadbin.py` cell-ID math — pointascell, resolution, kring, distance

**Files:** create `pygx/_quadbin.py`, `test/pygx/test_quadbin_core.py`.

The `quadbin` 0.2.x public API (verified): `point_to_cell`, `get_resolution`, `k_ring`, `cell_to_point`, `cell_to_boundary`, `cell_to_bounding_box`, `cell_to_tile`, `tile_to_cell`, `geometry_to_cells`, `cell_area`. There is **no** `cell_distance`/`polyfill_bbox`.

- [ ] **Step 1: failing tests** `test/pygx/test_quadbin_core.py`
```python
import pytest
pytest.importorskip("quadbin")
import quadbin  # noqa: E402
from databricks.labs.gbx.pygx import _quadbin


def test_pointascell_matches_lib():
    cell = _quadbin.point_as_cell(-122.4194, 37.7749, 10)
    assert cell == quadbin.point_to_cell(-122.4194, 37.7749, 10)
    assert _quadbin.resolution(cell) == 10


def test_resolution_bitformula():
    cell = quadbin.point_to_cell(0.0, 0.0, 14)
    assert _quadbin.resolution(cell) == ((cell >> 52) & 0x1F)  # heavy formula


def test_kring_matches_lib_and_includes_center():
    cell = quadbin.point_to_cell(0.0, 0.0, 10)
    ring = _quadbin.k_ring(cell, 1)
    assert cell in ring and len(ring) == 9 and sorted(ring) == sorted(quadbin.k_ring(cell, 1))


def test_distance_same_resolution_chebyshev():
    a = quadbin.point_to_cell(0.0, 0.0, 10)
    b = quadbin.point_to_cell(0.5, 0.5, 10)
    ta, tb = quadbin.cell_to_tile(a), quadbin.cell_to_tile(b)
    expected = max(abs(ta[0] - tb[0]), abs(ta[1] - tb[1]))
    assert _quadbin.distance(a, b) == expected


def test_distance_mismatched_resolution_raises():
    a = quadbin.point_to_cell(0.0, 0.0, 10)
    b = quadbin.point_to_cell(0.0, 0.0, 11)
    with pytest.raises(ValueError, match="same resolution"):
        _quadbin.distance(a, b)


def test_pointascell_resolution_validation():
    with pytest.raises(ValueError):
        _quadbin.point_as_cell(0.0, 0.0, 27)  # > 26
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** the four functions in `pygx/_quadbin.py`. `cell_to_tile` returns `(x, y, z)`; use indices 0/1 for the Chebyshev distance. Mirror `Quadbin.scala` validation (`point_as_cell` res ∈ [0,26]; `distance` equal-resolution).
```python
"""Pure-Python quadbin GridX core for the pygx light tier.

Cell math via the `quadbin` package; logic the package lacks (distance, bbox
polyfill) is ported to match the heavy `gridx/grid/Quadbin.scala` exactly.
Geometry outputs are EWKB (SRID 4326), matching heavy's JTS.toEWKB.
"""
import quadbin

_MAX_RES = 26
_MAX_POLYFILL_RES = 20


def point_as_cell(lon: float, lat: float, resolution: int) -> int:
    z = int(resolution)
    if z < 0 or z > _MAX_RES:
        raise ValueError(f"quadbin resolution must be in [0,{_MAX_RES}]; got {z}")
    return quadbin.point_to_cell(float(lon), float(lat), z)


def resolution(cell: int) -> int:
    return quadbin.get_resolution(int(cell))


def k_ring(cell: int, k: int) -> list:
    if int(k) < 0:
        raise ValueError(f"k must be >= 0; got {k}")
    return list(quadbin.k_ring(int(cell), int(k)))


def distance(cell_a: int, cell_b: int) -> int:
    if resolution(cell_a) != resolution(cell_b):
        raise ValueError("quadbin_distance: cells must be at same resolution")
    ax, ay = quadbin.cell_to_tile(int(cell_a))[:2]
    bx, by = quadbin.cell_to_tile(int(cell_b))[:2]
    return int(max(abs(ax - bx), abs(ay - by)))
```

- [ ] **Step 4: run → PASS** (6 tests).

- [ ] **Step 5: commit** (`feat(pygx): quadbin cell-id math (pointascell/resolution/kring/distance)`).

---

## Task 3: `_quadbin.py` geometry — aswkb, centroid, cellunion (EWKB)

**Files:** modify `pygx/_quadbin.py`, `test/pygx/test_quadbin_core.py`.

Heavy emits **EWKB** (`JTS.toEWKB`, SRID 4326). Light: shapely `to_wkb(geom, include_srid=True)` after `set_srid(geom, 4326)`. Use `quadbin.cell_to_bounding_box(cell)` → `(west, south, east, north)` for the polygon, `quadbin.cell_to_point(cell)` for the centroid (returns a GeoJSON-ish point or `(lon,lat)` — verify the return shape in Step 3 and adapt).

- [ ] **Step 1: failing tests** (append)
```python
from shapely import from_wkb, get_srid  # noqa: E402

def test_aswkb_is_ewkb_polygon_srid4326():
    cell = quadbin.point_to_cell(0.0, 0.0, 10)
    g = from_wkb(_quadbin.as_wkb(cell))
    assert g.geom_type == "Polygon" and get_srid(g) == 4326
    w, s, e, n = quadbin.cell_to_bounding_box(cell)
    assert abs(g.bounds[0] - w) < 1e-9 and abs(g.bounds[2] - e) < 1e-9


def test_centroid_is_ewkb_point_srid4326():
    cell = quadbin.point_to_cell(0.0, 0.0, 10)
    g = from_wkb(_quadbin.centroid(cell))
    assert g.geom_type == "Point" and get_srid(g) == 4326


def test_cellunion_is_ewkb_and_covers_cells():
    cells = quadbin.k_ring(quadbin.point_to_cell(0.0, 0.0, 8), 1)
    g = from_wkb(_quadbin.cell_union(list(cells)))
    assert g.geom_type in ("Polygon", "MultiPolygon") and get_srid(g) == 4326


def test_cellunion_empty_or_none_is_none():
    assert _quadbin.cell_union([]) is None
    assert _quadbin.cell_union(None) is None
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** (append to `_quadbin.py`). First confirm the `cell_to_point` / `cell_to_bounding_box` return shapes via `.venv-pyrx/bin/python -c "import quadbin; print(quadbin.cell_to_bounding_box(quadbin.point_to_cell(0,0,10))); print(quadbin.cell_to_point(quadbin.point_to_cell(0,0,10)))"` and adapt the unpacking.
```python
from shapely import set_srid, to_wkb, union_all
from shapely.geometry import Point, box


def _ewkb(geom) -> bytes:
    return to_wkb(set_srid(geom, 4326), include_srid=True)


def as_wkb(cell: int) -> bytes:
    w, s, e, n = quadbin.cell_to_bounding_box(int(cell))
    return _ewkb(box(w, s, e, n))


def centroid(cell: int) -> bytes:
    pt = quadbin.cell_to_point(int(cell))  # confirm shape in Step 3; expect [lon, lat] or geojson
    lon, lat = (pt["coordinates"] if isinstance(pt, dict) else pt)
    return _ewkb(Point(lon, lat))


def cell_union(cells) -> "bytes | None":
    if not cells:
        return None
    polys = [box(*quadbin.cell_to_bounding_box(int(c))) for c in cells if c is not None]
    if not polys:
        return None
    return _ewkb(union_all(polys))
```

- [ ] **Step 4: run → PASS.**

- [ ] **Step 5: commit** (`feat(pygx): quadbin geometry — aswkb/centroid/cellunion (EWKB SRID 4326)`).

---

## Task 4: `_quadbin.py` polyfill + tessellate

**Files:** modify `pygx/_quadbin.py`, `test/pygx/test_quadbin_core.py`.

Heavy `polyfill`/`tessellate` use the geometry **envelope** (bbox) at the resolution. Port `Quadbin.scala`'s bbox-cell enumeration: compute the bbox `(w,s,e,n)`, then enumerate every cell whose tile lies within the bbox tile range at `res`. Reference `gridx/grid/Quadbin.scala` for the exact enumeration (tile range from the corner cells via `cell_to_tile`/`tile_to_cell`); the parity test (Task 7) is the exact-cell-set definition of done. Tessellate = polyfill the bbox, then per cell intersect its polygon with the input geom and emit `(cell, EWKB(intersection))`, dropping empty intersections, matching heavy.

- [ ] **Step 1: failing tests** (append; use `_geom.parse_geom` for input)
```python
from shapely.geometry import box as _box  # noqa: E402
from shapely import to_wkb as _to_wkb  # noqa: E402

def test_polyfill_bbox_cells_resolution():
    geom = _to_wkb(_box(-0.1, -0.1, 0.1, 0.1))
    cells = _quadbin.polyfill(geom, 12)
    assert len(cells) > 0
    assert all(_quadbin.resolution(c) == 12 for c in cells)


def test_polyfill_resolution_validation():
    import pytest
    with pytest.raises(ValueError):
        _quadbin.polyfill(_to_wkb(_box(0, 0, 1, 1)), 21)  # > 20


def test_tessellate_returns_cell_geom_pairs():
    geom = _to_wkb(_box(-0.05, -0.05, 0.05, 0.05))
    chips = _quadbin.tessellate(geom, 12)
    assert len(chips) > 0
    cell0, gwkb0 = chips[0]
    assert isinstance(cell0, int)
    from shapely import from_wkb, get_srid
    g0 = from_wkb(gwkb0)
    assert get_srid(g0) == 4326 and not g0.is_empty
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** `polyfill(geom, res)` and `tessellate(geom, res)` in `_quadbin.py`. Use `_geom.parse_geom` on the input; compute `.bounds`; enumerate bbox cells matching `Quadbin.scala` (validate res ∈ [0,20]); tessellate intersects each cell's `box(*cell_to_bounding_box)` with the parsed geom. Return `polyfill` → `list[int]`; `tessellate` → `list[(int, bytes)]`. (Import `parse_geom` from `._geom`.) **The exact enumeration must match `Quadbin.scala`; if `geometry_to_cells` on the bbox polygon yields the same set, prefer it; otherwise port the tile-range loop.** Verify against heavy in Task 7.

- [ ] **Step 4: run → PASS.**

- [ ] **Step 5: commit** (`feat(pygx): quadbin polyfill + tessellate (bbox cells, EWKB chips)`).

---

## Task 5: `functions.py` — register + UDFs + agg + wrappers

**Files:** create `pygx/functions.py`, `test/pygx/test_quadbin_udf.py`.

Mirror `pyvx/functions.py`: `ColLike`, `_col`, scalar `spark.udf.register` with explicit return types, a grouped-agg `pandas_udf` returning BINARY for `cellunion_agg`, and Column wrappers. `tessellate` returns `ARRAY<STRUCT<cell,geom>>` via a plain `@f.udf(ArrayType(QUADBIN_CELL_SCHEMA))` (heavy returns an array, not exploded — no UDTF needed). `polyfill`/`kring`/`cellunion` return `ArrayType(LongType())` or `BinaryType()`.

- [ ] **Step 1: failing test** `test/pygx/test_quadbin_udf.py`
```python
import pytest
pytest.importorskip("quadbin")
shapely = pytest.importorskip("shapely")
from shapely import from_wkb, get_srid, to_wkb  # noqa: E402
from shapely.geometry import box  # noqa: E402
from databricks.labs.gbx.pygx import functions as gx


def test_pointascell_and_resolution(spark):
    gx.register(spark)
    row = spark.sql("SELECT gbx_quadbin_pointascell(-122.4194, 37.7749, 10) AS c").collect()[0]
    import quadbin
    assert row["c"] == quadbin.point_to_cell(-122.4194, 37.7749, 10)
    r = spark.sql(f"SELECT gbx_quadbin_resolution({row['c']}) AS r").collect()[0]
    assert r["r"] == 10


def test_aswkb_ewkb(spark):
    gx.register(spark)
    import quadbin
    c = quadbin.point_to_cell(0.0, 0.0, 10)
    out = spark.sql(f"SELECT gbx_quadbin_aswkb({c}) AS w").collect()[0]
    g = from_wkb(bytes(out["w"]))
    assert g.geom_type == "Polygon" and get_srid(g) == 4326


def test_tessellate_struct_array(spark):
    gx.register(spark)
    df = spark.createDataFrame([(bytearray(to_wkb(box(-0.05, -0.05, 0.05, 0.05))),)], "g binary")
    df.createOrReplaceTempView("v")
    rows = spark.sql("SELECT t.cell, t.geom FROM v LATERAL VIEW explode(gbx_quadbin_tessellate(g, 12)) AS t").collect()
    assert len(rows) > 0 and isinstance(rows[0]["cell"], int)


def test_cellunion_agg(spark):
    gx.register(spark)
    import quadbin
    cells = list(quadbin.k_ring(quadbin.point_to_cell(0.0, 0.0, 8), 1))
    df = spark.createDataFrame([(c,) for c in cells], "cell long")
    out = df.agg(gx.quadbin_cellunion_agg("cell").alias("u")).collect()[0]
    g = from_wkb(bytes(out["u"]))
    assert g.geom_type in ("Polygon", "MultiPolygon") and get_srid(g) == 4326
```

- [ ] **Step 2: run → FAIL** (`bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pygx/test_quadbin_udf.py --log quadbin-udf.log`).

- [ ] **Step 3: implement `functions.py`.** Imports + `ColLike`/`_col` copied from `pyvx/functions.py`. Scalar UDFs wrap `_quadbin` functions; register with explicit return types. `cellunion_agg` is a grouped-agg `pandas_udf(BinaryType())` taking one `pd.Series` of cells → `_quadbin.cell_union(list-of-cells)`. Example skeleton:
```python
from typing import List, Union
import pandas as pd
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, BinaryType, IntegerType, LongType

from . import _env, _geom, _quadbin
from ._serde import QUADBIN_CELL_SCHEMA

ColLike = Union[Column, str, bool, int, float, bytes]

def _col(x: ColLike):
    return x if isinstance(x, (Column, str)) else f.lit(x)

def _pointascell(lon, lat, res): return _quadbin.point_as_cell(lon, lat, res)
def _resolution(cell): return _quadbin.resolution(cell)
def _kring(cell, k): return _quadbin.k_ring(cell, k)
def _distance(a, b): return _quadbin.distance(a, b)
def _polyfill(geom, res): return _quadbin.polyfill(geom, res)
def _aswkb(cell): return _quadbin.as_wkb(cell)
def _centroid(cell): return _quadbin.centroid(cell)
def _cellunion(cells): return _quadbin.cell_union(list(cells) if cells else cells)
def _tessellate(geom, res):
    return [(int(c), g) for (c, g) in _quadbin.tessellate(geom, res)]

@pandas_udf(BinaryType())
def _cellunion_agg_udf(cell: pd.Series) -> bytes:
    return _quadbin.cell_union([int(c) for c in cell if c is not None])

def register(spark: SparkSession = None) -> None:
    """Register the pygx quadbin SQL functions (Serverless-safe: udf only)."""
    _env.assert_quadbin_available()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    spark.udf.register("gbx_quadbin_pointascell", _pointascell, LongType())
    spark.udf.register("gbx_quadbin_resolution", _resolution, IntegerType())
    spark.udf.register("gbx_quadbin_kring", _kring, ArrayType(LongType()))
    spark.udf.register("gbx_quadbin_distance", _distance, IntegerType())
    spark.udf.register("gbx_quadbin_polyfill", _polyfill, ArrayType(LongType()))
    spark.udf.register("gbx_quadbin_aswkb", _aswkb, BinaryType())
    spark.udf.register("gbx_quadbin_centroid", _centroid, BinaryType())
    spark.udf.register("gbx_quadbin_cellunion", _cellunion, BinaryType())
    spark.udf.register("gbx_quadbin_tessellate", _tessellate, ArrayType(QUADBIN_CELL_SCHEMA))
    spark.udf.register("gbx_quadbin_cellunion_agg", _cellunion_agg_udf)
```
Then add Column wrappers mirroring `pyvx` (`quadbin_pointascell(lon,lat,res)` → `f.call_function("gbx_quadbin_pointascell", _col(lon),_col(lat),_col(res))`, etc., and `quadbin_cellunion_agg(cell)` → `_cellunion_agg_udf(_col(cell))`).

- [ ] **Step 4: run → PASS** (4 tests). Also run the Serverless guard (`test/pyrx/test_serverless_no_spark_config.py` if it scans pygx, else confirm no `_jvm`/`conf` in pygx).

- [ ] **Step 5: commit** (`feat(pygx): register 10 quadbin functions (udf + grouped-agg)`).

---

## Task 6: function-info + bindings

**Files:** modify `docs/tests/python/api/gridx_functions_sql.py` (only if quadbin examples need the `mode`/arg refresh — they exist already; verify), `pygx/functions.py` (ensure all 10 Column wrappers exist for import-parity).

- [ ] **Step 1:** confirm the 10 `gbx_quadbin_*` are in `docs/tests-function-info/registered_functions.txt` (they are) and have `*_sql_example()` in `docs/tests/python/api/gridx_functions_sql.py` (they do). No new entries needed; this task verifies parity, not adds.
- [ ] **Step 2:** run `bash scripts/commands/gbx-test-bindings.sh --log bindings-quadbin.log` → PASS (every registered quadbin fn present in Scala + Python + function-info). Fix upstream if it fails.
- [ ] **Step 3: commit** if any wrapper/example changed (`docs(pygx): quadbin binding/function-info parity`); otherwise note "no change — parity already holds" and skip.

---

## Task 7: cross-tier parity (JAR-gated)

**Files:** create `test/pygx/test_parity_quadbin.py`.

- [ ] **Step 1: rebuild + stage the JAR** (heavy unchanged so far, but the parity test needs a JAR present): `bash scripts/commands/gbx-data-push-jar.sh` then copy `target/geobrix-0.4.0-jar-with-dependencies.jar` → `python/geobrix/lib/`. (If unchanged from a prior stage, a present JAR suffices.)

- [ ] **Step 2: write the JAR-gated parity test** — copy the gating block from `test/pyvx/test_parity_mvt.py` (the `_JARS` glob on `parents[2]/"lib"` + `spark_with_jar` fixture + active-session skip). Register light then heavy (`from databricks.labs.gbx.pygx import functions as gx; gx.register(spark)`; heavy via `from databricks.labs.gbx.gridx.quadbin import functions as hx; hx.register(spark)` — collect light result first, then heavy overwrites the SQL name, then collect heavy). Assert per function:
  - **Exact**: `pointascell`, `resolution`, `kring` (sorted set equality), `distance`, `polyfill` (sorted cell-set equality), `tessellate` (cell-set equality), `cellunion`/`cellunion_agg` (decoded-geometry equality), over a deterministic point/geometry/cell-list fixture.
  - **Geometry within 1e-6**: decode both tiers' EWKB (`shapely.from_wkb`), assert `get_srid==4326` both, and coordinates equal within 1e-6 (use `g_light.equals_exact(g_heavy, 1e-6)` or `.normalize()` compare).
  - **Contingency:** if `pointascell`/`polyfill`/`tessellate` cell sets diverge (lib vs `Quadbin.scala`), port the exact `Quadbin.scala` encode/enumeration into `_quadbin.py` until exact — exact parity is the bar (no tolerance on cell IDs).

- [ ] **Step 3: run in Docker** `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pygx/test_parity_quadbin.py --with-integration --log parity-quadbin.log` → green (skips only without JAR).

- [ ] **Step 4: commit** (`test(pygx): light-vs-heavy quadbin exact parity (cells + EWKB geom)`).

---

## Task 8: bench harness — quadbin legs

**Files:** modify `python/geobrix/src/databricks/labs/gbx/bench/` (`corpus_grid.py` new or extend `corpus_vector.py`; `readers.py` `run_quadbin_*`; `cluster.py` `_CELL_GRID_QUADBIN`); `notebooks/tests/push_and_run_bench_on_cluster.py` (`--grid-quadbin-only`).

- [ ] **Step 1:** add a corpus generator (points for pointascell; geometries for polyfill/tessellate; cell-id arrays for cellunion) mirroring `bench/corpus_vector.py` style. 
- [ ] **Step 2:** add `run_quadbin_pointascell` / `run_quadbin_polyfill` / `run_quadbin_tessellate` / `run_quadbin_cellunion_agg` (representative coverage: a scalar, a geom→array, a struct-array, an agg) to `readers.py`, mirroring `run_legacy_aswkb`/`run_triangulate` signatures, with light-vs-heavy timing + **exact cell-set / decoded-geom parity** assertions.
- [ ] **Step 3:** add `_CELL_GRID_QUADBIN` to `cluster.py` (mirror `_CELL_VECTOR_TIN`: light leg collected before heavy registration) + the launcher `--grid-quadbin-only` flag (mirror `--vector-tin-only`).
- [ ] **Step 4: local smoke** at tiny scale in the `geobrix-dev` container (resolve SQL, both tiers run, parity verdicts PASS). Do NOT run the cluster here.
- [ ] **Step 5: commit** (`feat(bench): quadbin light-vs-heavy bench legs (--grid-quadbin-only)`).

---

## Task 9: cluster bench run

- [ ] **Step 1:** controller-orchestrated (not a subagent): build+stage JAR+wheel to the sample-data Volume; restart the bench cluster; poll libs INSTALLED; run `gbx:bench:cluster --grid-quadbin-only` once; verify one run + rows; fetch `summary.md`; terminate the cluster after capture.
- [ ] **Step 2:** record the light-vs-heavy medians + exact-parity verdicts (for the docs in Task 10). No commit (bench writes to the Volume/table).

---

## Task 10: docs — all surfaces

**Files:** `docs/docs/api/gridx-functions.mdx`, `execution-tiers.mdx`, `performance.mdx`, `benchmarking.mdx`, `README.md`, `docs/src/pages/index.js`, `docs/docs/intro.mdx`; `function-info`.

- [ ] **Step 1: `gridx-functions.mdx`** — the page currently has a single page-level `<Tier heavy/>`. Restructure so the **quadbin section** carries `<Tier both/>` with a lightweight note ("Powered by the **quadbin** package + shapely; the `quadbin_distance`/`quadbin_polyfill` cell math mirrors the heavy implementation"), while the **custom-grid** and **BNG** sections keep `<Tier heavy/>`. Document the EWKB SRID-4326 geometry outputs.
- [ ] **Step 2: `execution-tiers.mdx`** — in the "heavyweight-only" reasons, move quadbin out (it's now both-tier) while KEEPING `gbx_custom_*` (custom grids) **and** BNG (`gbx_bng_*`) heavyweight-only. Update the GridX framing accordingly.
- [ ] **Step 3: `performance.mdx`** — add a "GridX (pygx)" subsection: quadbin execution shapes (scalar UDFs + the `cellunion_agg` grouped-agg + the `tessellate` array-UDF), the `pygx/_quadbin.py` module (quadbin lib + shapely), and the perf narrative from the Task 9 numbers.
- [ ] **Step 4: `benchmarking.mdx`** — fill the **Grid tab** (currently "Grid (soon)") with the quadbin light-vs-heavy timing + exact-parity verdicts from Task 9.
- [ ] **Step 5: README / `index.js` / `intro.mdx`** — reflect quadbin lightweight availability. README GridX bullet: note quadbin is now lightweight (`pygx`), BNG still heavyweight (planned). `index.js` GridX card + the heavyweight-only line. `intro.mdx` note pygx alongside pyrx/pyvx.
- [ ] **Step 6:** `function-info` regen if examples changed; `cd docs && npm run build` → SUCCESS; `grep -rn -iE "wave [0-9]+" docs/docs/` → empty.
- [ ] **Step 7: commit** (`docs(pygx): quadbin lightweight tier across all surfaces`).

---

## Self-Review

**Spec coverage:** ✅ all 10 functions (Tasks 2–5); ✅ EWKB SRID-4326 (Tasks 3–4); ✅ distance/polyfill ported-not-lib (Tasks 2,4, corrected); ✅ exact cell-set parity (Task 7); ✅ Serverless-safe (Task 5 guard); ✅ no new deps (uses existing quadbin/shapely); ✅ bench (Tasks 8–9); ✅ all doc surfaces incl. performance.mdx + keeping custom-grid/BNG heavy-only (Task 10); ✅ function-info/binding parity (Task 6).

**Placeholder scan:** the two soft spots are intentional: `_quadbin.centroid` unpacking is verified-in-step-3 (lib return shape), and `polyfill`/`tessellate` enumeration references `Quadbin.scala` with the parity test (Task 7) as the exact definition of done — a faithful-port pattern (same as pyvx's Sloan), not a placeholder.

**Type consistency:** `_quadbin` function names (`point_as_cell`, `resolution`, `k_ring`, `distance`, `as_wkb`, `centroid`, `cell_union`, `polyfill`, `tessellate`), SQL names (`gbx_quadbin_*`), schema (`QUADBIN_CELL_SCHEMA`), and the `register`/wrapper shapes are consistent across tasks.
