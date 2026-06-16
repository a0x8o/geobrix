# pyvx MVT Light Tier — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the MVT slice of the pure-Python/PySpark light VectorX tier (`pyvx`) — `st_asmvt` (aggregator) + `st_asmvt_pyramid` (generator) — as a drop-in swap for the heavyweight `vectorx` MVT functions, and upgrade **both** tiers to encode MVT attributes with native protobuf value types.

**Architecture:** A new `pyvx` package mirrors `pyrx`: pure-Python encode/tiling helpers (Spark-free, unit-tested) wrapped by Serverless-safe Spark wiring (`spark.udf.register` for the aggregator, `spark.udtf.register` for the pyramid — no `_jvm`/`conf.set`/`.rdd`). `mapbox-vector-tile` + `shapely` + `pyproj` do the encoding. The heavyweight Scala `MvtWriter` is upgraded from all-`OFTString` to native OGR field types so the tiers emit byte-equivalent typed tiles; parity is checked at the decoded-feature level.

**Tech Stack:** Python 3.12, PySpark (Spark Connect / Serverless), `mapbox-vector-tile`, `shapely 2`, `pyproj`; Scala 2.13 / Spark 4 / OGR (heavy MVT). Tests: pytest (light), ScalaTest in the `geobrix-dev` Docker container (heavy), `gbx:*` commands.

**Spec:** `docs/superpowers/specs/2026-06-13-pyvx-mvt-light-tier-design.md`.

---

## File Structure

**New — light package** (`python/geobrix/src/databricks/labs/gbx/pyvx/`):
- `__init__.py` — package marker + docstring.
- `_env.py` — assert `mapbox_vector_tile` / `shapely` importable (mirrors `pyrx/_env.py`).
- `_serde.py` — WKB↔shapely; `attrs` struct/Row → native-typed property dict; the `(z,x,y,mvt_bytes)` tile struct schema.
- `_mvt.py` — Spark-free encode helpers: `encode_layer(features, layer_name, extent)` and `pyramid_tiles(geom, attrs, min_z, max_z, layer_name, extent)` (generator of `(z,x,y,bytes)`).
- `functions.py` — `register(spark)`, `SQL_REGISTRY`, the `st_asmvt` grouped-agg UDF + Column wrapper, and the `st_asmvt_pyramid` UDTF + wrapper. Signatures mirror `vectorx/functions.py`.

**New — light tests** (`python/geobrix/test/pyvx/`):
- `conftest.py`, `test_mvt_encode.py` (Spark-free), `test_asmvt.py`, `test_asmvt_pyramid.py`, `test_parity_mvt.py` (Docker/JAR integration).

**Modified — heavy (Scala):**
- `src/main/scala/com/databricks/labs/gbx/vectorx/mvt/MvtWriter.scala` — native OGR field types.
- `src/main/scala/com/databricks/labs/gbx/vectorx/expressions/ST_AsMvt.scala` — ensure `encodeAttrs`/`decodeAttrs` preserve typed values (not stringified).
- `src/test/scala/com/databricks/labs/gbx/vectorx/expressions/ST_AsMvtTest.scala` — assert native-typed decoded values.

**Modified — packaging / bench / docs:**
- `python/geobrix/pyproject.toml` — add `mapbox-vector-tile` to the `light` extra.
- `python/geobrix/test/pyrx/test_serverless_no_spark_config.py` — extend `_source_files()` to cover `pyvx`.
- `python/geobrix/src/databricks/labs/gbx/bench/readers.py`, `.../bench/cluster.py`, `notebooks/tests/push_and_run_bench_on_cluster.py` — MVT light-vs-heavy bench + `--mvt-only`.
- `docs/docs/` — pyvx MVT page + Benchmarking **Vector** tab.

---

## Task 1: De-risk — verify Python UDTF on Serverless / Spark Connect

**Files:**
- Create (scratch, not committed): `/tmp/udtf_probe.py`

**Goal:** Confirm the generator can be a Python UDTF (approach 2B). If UDTFs don't register/run on the target (Serverless / Spark Connect), fall back to approach 2A (`pandas_udf(ArrayType(tile_struct))` + caller `explode`) for Task 5. This is a spike, not TDD.

- [ ] **Step 1: Write a trivial UDTF probe**

```python
# /tmp/udtf_probe.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf

@udtf(returnType="z int, x int, y int")
class Fan:
    def eval(self, n: int):
        for i in range(n):
            yield (0, i, i)

spark = SparkSession.builder.getOrCreate()
spark.udtf.register("fan", Fan)
spark.sql("SELECT f.* FROM (SELECT 3 AS n) t, LATERAL fan(t.n) f").show()
print("UDTF_OK")
```

- [ ] **Step 2: Run it on the local venv AND record the Serverless/Connect answer**

Run locally: `.venv-pyrx/bin/python /tmp/udtf_probe.py` — expect `UDTF_OK` and 3 rows.
Then confirm on the real target: run the same probe in a Serverless / Spark Connect notebook (or the bench cluster via a one-off). Record whether `spark.udtf.register` + `LATERAL` works over Connect.

- [ ] **Step 3: Record the decision in the plan + spec**

Append a one-line note to `docs/superpowers/specs/2026-06-13-pyvx-mvt-light-tier-design.md` under "Risks": either "UDTF verified on Serverless/Connect — Task 5 uses 2B" or "UDTF unsupported — Task 5 uses 2A fallback (pandas_udf(ArrayType)+explode)". All later steps that say "UDTF (2B)" switch to the array+explode form if 2A was chosen.

- [ ] **Step 4: Commit the decision note**

```bash
git add docs/superpowers/specs/2026-06-13-pyvx-mvt-light-tier-design.md
git commit -m "docs(spec): record pyvx pyramid generator approach (UDTF vs explode)"
```

---

## Task 2: Package skeleton + dependency + env guard

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyvx/__init__.py`
- Create: `python/geobrix/src/databricks/labs/gbx/pyvx/_env.py`
- Modify: `python/geobrix/pyproject.toml` (the `light = [...]` block)
- Test: `python/geobrix/test/pyvx/test_env.py`

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/pyvx/test_env.py
def test_pyvx_imports_and_env_ok():
    import databricks.labs.gbx.pyvx as pyvx  # noqa: F401
    from databricks.labs.gbx.pyvx import _env
    # Raises a clear ImportError if mapbox-vector-tile / shapely are missing.
    _env.assert_mvt_available()
```

- [ ] **Step 2: Run it to verify it fails**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_env.py -v`
Expected: FAIL (`ModuleNotFoundError: databricks.labs.gbx.pyvx`).

- [ ] **Step 3: Add the dependency**

In `python/geobrix/pyproject.toml`, add to the `light = [...]` list (after `pyproj>=3.6`):

```toml
    "mapbox-vector-tile>=2.0,<3",
```

Then install into the venv: `.venv-pyrx/bin/pip install 'mapbox-vector-tile>=2.0,<3'`.

- [ ] **Step 4: Create the package + env guard**

```python
# python/geobrix/src/databricks/labs/gbx/pyvx/__init__.py
"""pyvx — pure-Python/PySpark light VectorX tier (Serverless-safe).

Mirrors the heavyweight ``vectorx`` MVT functions (``gbx_st_*``) with no JVM,
no JAR, and no native GDAL. See databricks.labs.gbx.pyvx.functions.
"""
```

```python
# python/geobrix/src/databricks/labs/gbx/pyvx/_env.py
"""Environment checks for the pyvx light tier."""


def assert_mvt_available() -> None:
    """Raise a clear ImportError if the MVT light deps are missing."""
    missing = []
    try:
        import mapbox_vector_tile  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("mapbox-vector-tile")
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("shapely")
    if missing:
        raise ImportError(
            "pyvx requires the [light] extra; missing: "
            + ", ".join(missing)
            + ". Install with: pip install 'geobrix[light]'"
        )
```

Also create empty `python/geobrix/test/pyvx/__init__.py` if the test package needs it (match the `test/pyrx/` layout).

- [ ] **Step 5: Run the test to verify it passes**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_env.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/pyproject.toml python/geobrix/src/databricks/labs/gbx/pyvx/ python/geobrix/test/pyvx/
git commit -m "feat(pyvx): package skeleton + mapbox-vector-tile dep + env guard"
```

---

## Task 3: Pure-Python MVT encode core (`_serde.py` + `_mvt.py`) — native typing

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyvx/_serde.py`
- Create: `python/geobrix/src/databricks/labs/gbx/pyvx/_mvt.py`
- Test: `python/geobrix/test/pyvx/test_mvt_encode.py`

This is the heart: encoding features (geometry + **native-typed** attributes) to MVT bytes, Spark-free.

- [ ] **Step 1: Write the failing tests (decode-and-assert native types)**

```python
# python/geobrix/test/pyvx/test_mvt_encode.py
import mapbox_vector_tile as mvt
from shapely.geometry import Point
from shapely import to_wkb

from databricks.labs.gbx.pyvx import _mvt


def _decode(blob, layer="layer"):
    tile = mvt.decode(blob)
    return tile[layer]["features"]


def test_encode_layer_preserves_native_attr_types():
    feats = [
        {"geometry": to_wkb(Point(10, 20)), "properties": {"name": "a", "pop": 42, "h": 3.5, "ok": True}},
    ]
    blob = _mvt.encode_layer(feats, layer_name="layer", extent=4096)
    props = _decode(blob)[0]["properties"]
    assert props["name"] == "a"
    assert props["pop"] == 42 and isinstance(props["pop"], int)
    assert props["h"] == 3.5 and isinstance(props["h"], float)
    assert props["ok"] is True


def test_encode_layer_unsupported_type_falls_back_to_string():
    feats = [{"geometry": to_wkb(Point(1, 1)), "properties": {"b": b"\x00\x01"}}]
    blob = _mvt.encode_layer(feats, layer_name="layer", extent=4096)
    props = _decode(blob)[0]["properties"]
    assert isinstance(props["b"], str)  # bytes -> str fallback


def test_pyramid_tiles_caps_and_schema():
    # A point at lon/lat 0,0 over zooms 0..2 -> one tile per zoom (3 rows).
    rows = list(_mvt.pyramid_tiles(to_wkb(Point(0.0, 0.0)), {"id": 7}, 0, 2, "layer", 4096))
    zs = sorted(r[0] for r in rows)
    assert zs == [0, 1, 2]
    for (z, x, y, blob) in rows:
        assert isinstance(z, int) and isinstance(x, int) and isinstance(y, int)
        assert isinstance(blob, (bytes, bytearray)) and len(blob) > 0


def test_pyramid_rejects_too_many_tiles():
    import pytest
    with pytest.raises(ValueError):
        # whole-world polygon at high zoom blows the 10^6 cap
        from shapely.geometry import box
        list(_mvt.pyramid_tiles(to_wkb(box(-179, -85, 179, 85)), {}, 0, 20, "layer", 4096))
```

- [ ] **Step 2: Run to verify they fail**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_mvt_encode.py -v`
Expected: FAIL (`_mvt` has no `encode_layer`/`pyramid_tiles`).

- [ ] **Step 3: Implement `_serde.py`**

```python
# python/geobrix/src/databricks/labs/gbx/pyvx/_serde.py
"""Geometry + attribute marshalling for pyvx MVT encoding (Spark-free)."""
from typing import Any, Dict

from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)

# Output tile struct, identical to the heavy generator's row shape.
TILE_SCHEMA = StructType(
    [
        StructField("z", IntegerType(), False),
        StructField("x", IntegerType(), False),
        StructField("y", IntegerType(), False),
        StructField("mvt_bytes", BinaryType(), True),
    ]
)

# Python native types that map to a native MVT Value; everything else -> str().
_NATIVE = (bool, int, float, str)


def to_native_props(attrs: Any) -> Dict[str, Any]:
    """Coerce an attrs mapping/Row into a dict of MVT-native property values.

    bool/int/float/str pass through (mapbox-vector-tile picks the matching MVT
    Value field); any other type (bytes, datetime, list, dict) is str()-ified;
    None values are dropped (no field emitted), matching the heavy writer.
    """
    if attrs is None:
        return {}
    items = attrs.asDict().items() if hasattr(attrs, "asDict") else dict(attrs).items()
    out: Dict[str, Any] = {}
    for k, v in items:
        if v is None:
            continue
        out[str(k)] = v if isinstance(v, _NATIVE) else str(v)
    return out
```

- [ ] **Step 4: Implement `_mvt.py`**

```python
# python/geobrix/src/databricks/labs/gbx/pyvx/_mvt.py
"""Pure-Python MVT encoding + XYZ pyramid tiling (Spark-free, Serverless-safe)."""
import math
from typing import Any, Dict, Iterator, List, Tuple

import mapbox_vector_tile as mvt
from shapely import from_wkb
from shapely.geometry import box
from shapely.ops import transform

from ._serde import to_native_props

MAX_ZOOM = 20
MAX_TILES = 1_000_000
DEFAULT_EXTENT = 4096


def encode_layer(features: List[Dict[str, Any]], layer_name: str, extent: int = DEFAULT_EXTENT) -> bytes:
    """Encode features (each {'geometry': WKB bytes, 'properties': dict}) into one MVT layer.

    Geometry is expected in tile-local coordinates (caller transformed). Property
    values keep their native Python type; non-native types are str()-ified.
    """
    layer_feats = []
    for f in features:
        geom = f["geometry"]
        shp = from_wkb(bytes(geom)) if isinstance(geom, (bytes, bytearray)) else geom
        if shp is None or shp.is_empty:
            continue
        layer_feats.append({"geometry": shp, "properties": to_native_props(f.get("properties"))})
    return mvt.encode(
        {"name": layer_name, "features": layer_feats},
        default_options={"extents": extent},
    )


def _lonlat_to_tile(lon: float, lat: float, z: int) -> Tuple[int, int]:
    n = 2 ** z
    x = int((lon + 180.0) / 360.0 * n)
    lat_r = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(lat_r)) / math.pi) / 2.0 * n)
    return max(0, min(n - 1, x)), max(0, min(n - 1, y))


def _tile_bounds(z: int, x: int, y: int) -> Tuple[float, float, float, float]:
    n = 2 ** z
    lon1 = x / n * 360.0 - 180.0
    lon2 = (x + 1) / n * 360.0 - 180.0
    lat1 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat2 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return lon1, min(lat1, lat2), lon2, max(lat1, lat2)


def _to_tile_local(geom, z: int, x: int, y: int, extent: int):
    """Project a 4326 geometry into [0, extent] tile-pixel space for tile (z,x,y)."""
    minx, miny, maxx, maxy = _tile_bounds(z, x, y)
    sx = extent / (maxx - minx)
    sy = extent / (maxy - miny)
    return transform(lambda xs, ys, zs=None: ((xs - minx) * sx, (maxy - ys) * sy), geom)


def pyramid_tiles(
    geom_wkb,
    attrs: Any,
    min_z: int,
    max_z: int,
    layer_name: str,
    extent: int = DEFAULT_EXTENT,
) -> Iterator[Tuple[int, int, int, bytes]]:
    """Yield (z, x, y, mvt_bytes) for every tile a 4326 feature intersects across [min_z, max_z].

    Yields incrementally (no buffering) to keep the worker memory flat. Caps:
    max_z <= MAX_ZOOM; total intersecting tiles <= MAX_TILES (raises ValueError).
    """
    if max_z > MAX_ZOOM:
        raise ValueError(f"max_z {max_z} exceeds MAX_ZOOM {MAX_ZOOM}")
    shp = from_wkb(bytes(geom_wkb)) if isinstance(geom_wkb, (bytes, bytearray)) else geom_wkb
    if shp is None or shp.is_empty:
        return
    props = to_native_props(attrs)
    minx, miny, maxx, maxy = shp.bounds
    # Pre-count tiles to enforce the cap before emitting anything.
    total = 0
    spans = {}
    for z in range(min_z, max_z + 1):
        x0, y1 = _lonlat_to_tile(minx, miny, z)
        x1, y0 = _lonlat_to_tile(maxx, maxy, z)
        xr, yr = range(min(x0, x1), max(x0, x1) + 1), range(min(y0, y1), max(y0, y1) + 1)
        spans[z] = (xr, yr)
        total += len(xr) * len(yr)
        if total > MAX_TILES:
            raise ValueError(f"pyramid would emit > {MAX_TILES} tiles; narrow the zoom range")
    for z in range(min_z, max_z + 1):
        xr, yr = spans[z]
        for x in xr:
            for y in yr:
                tb = box(*_tile_bounds(z, x, y))
                clipped = shp.intersection(tb)
                if clipped.is_empty:
                    continue
                local = _to_tile_local(clipped, z, x, y, extent)
                blob = encode_layer(
                    [{"geometry": local, "properties": props}], layer_name, extent
                )
                yield (z, x, y, blob)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_mvt_encode.py -v`
Expected: PASS (all 4). If `mapbox_vector_tile.encode` signature differs in the installed version, adjust the `default_options`/`extents` kwarg to match that version's API (verify with `python -c "import mapbox_vector_tile, inspect; print(inspect.signature(mapbox_vector_tile.encode))"`).

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyvx/_serde.py python/geobrix/src/databricks/labs/gbx/pyvx/_mvt.py python/geobrix/test/pyvx/test_mvt_encode.py
git commit -m "feat(pyvx): pure-Python MVT encode + XYZ pyramid (native attr types)"
```

---

## Task 4: `st_asmvt` aggregator + `register(spark)`

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyvx/functions.py`
- Test: `python/geobrix/test/pyvx/test_asmvt.py`, `python/geobrix/test/pyvx/conftest.py`

- [ ] **Step 1: Write `conftest.py` (spark fixture)**

```python
# python/geobrix/test/pyvx/conftest.py
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("pyvx-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()
```

- [ ] **Step 2: Write the failing aggregator test**

```python
# python/geobrix/test/pyvx/test_asmvt.py
import mapbox_vector_tile as mvt
from shapely import to_wkb
from shapely.geometry import Point

from databricks.labs.gbx.pyvx import functions as vx


def test_st_asmvt_aggregates_group_to_one_tile(spark):
    vx.register(spark)
    rows = [
        (0, 0, 0, bytearray(to_wkb(Point(100.0, 200.0))), "a", 1),
        (0, 0, 0, bytearray(to_wkb(Point(300.0, 400.0))), "b", 2),
    ]
    df = spark.createDataFrame(rows, "z int, x int, y int, geom binary, name string, pop int")
    from pyspark.sql import functions as f

    out = (
        df.groupBy("z", "x", "y")
        .agg(vx.st_asmvt(f.col("geom"), f.struct("name", "pop"), "layer").alias("mvt"))
        .collect()
    )
    assert len(out) == 1
    blob = bytes(out[0]["mvt"])
    feats = mvt.decode(blob)["layer"]["features"]
    assert len(feats) == 2
    pops = sorted(ff["properties"]["pop"] for ff in feats)
    assert pops == [1, 2]
    assert all(isinstance(ff["properties"]["pop"], int) for ff in feats)
```

- [ ] **Step 3: Run to verify it fails**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_asmvt.py -v`
Expected: FAIL (`functions` module missing / `st_asmvt` undefined).

- [ ] **Step 4: Implement `functions.py` (aggregator + register)**

```python
# python/geobrix/src/databricks/labs/gbx/pyvx/functions.py
"""pyvx light VectorX API — MVT functions (Serverless-safe).

Signatures mirror databricks.labs.gbx.vectorx.functions so light <-> heavy is a
one-line import swap. Register once with vx.register(spark), then use on columns.
"""
from typing import Union

import pandas as pd
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import BinaryType

from . import _env, _mvt

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Union[Column, str]:
    if isinstance(x, Column) or isinstance(x, str):
        return x
    return f.lit(x)


# --- st_asmvt: grouped-aggregate pandas UDF -------------------------------------------------
@pandas_udf(BinaryType())
def _asmvt_udf(geom: pd.Series, attrs: pd.Series, layer: pd.Series) -> bytes:
    """Grouped-agg: encode one group's features into a single MVT layer blob."""
    layer_name = "layer"
    if layer is not None and len(layer) > 0 and layer.iloc[0] is not None:
        layer_name = str(layer.iloc[0])
    feats = [
        {"geometry": bytes(g), "properties": a}
        for g, a in zip(geom, attrs)
        if g is not None and len(bytes(g)) > 0
    ]
    return _mvt.encode_layer(feats, layer_name=layer_name)


def register(spark: SparkSession = None) -> None:
    """Register the pyvx MVT SQL functions (Serverless-safe: udf/udtf only)."""
    _env.assert_mvt_available()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    spark.udf.register("gbx_st_asmvt", _asmvt_udf)
    # st_asmvt_pyramid registration is added in Task 5.


def st_asmvt(geom_wkb: ColLike, attrs: ColLike, layer_name: ColLike) -> Column:
    """Aggregator: encode a group of features into an MVT protobuf blob (BINARY).

    geom_wkb: per-row WKB geometry in tile-local coordinates.
    attrs:    per-row attribute struct (native-typed in the output tile).
    layer_name: constant MVT layer name (plain str -> literal).
    """
    if isinstance(layer_name, str):
        layer_name = f.lit(layer_name)
    return _asmvt_udf(_col(geom_wkb), _col(attrs), _col(layer_name))
```

Note: `_asmvt_udf` is a grouped-aggregate pandas UDF — receives each group's columns as `pd.Series`. Registering it via `spark.udf.register("gbx_st_asmvt", _asmvt_udf)` exposes the SQL name; the Python `st_asmvt(...)` wrapper calls the UDF object directly so `df.groupBy(...).agg(vx.st_asmvt(...))` works.

- [ ] **Step 5: Run the test to verify it passes**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_asmvt.py -v`
Expected: PASS. (If the installed pandas-UDF API requires an explicit `functionType`/`PandasUDFType.GROUPED_AGG`, add it; on Spark 3.5+/4.0 the type-hinted `pandas_udf` returning a scalar from `Series` inputs is treated as a grouped-agg in `.agg(...)`.)

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyvx/functions.py python/geobrix/test/pyvx/test_asmvt.py python/geobrix/test/pyvx/conftest.py
git commit -m "feat(pyvx): st_asmvt grouped-agg UDF + register"
```

---

## Task 5: `st_asmvt_pyramid` generator (UDTF — 2B; or 2A fallback)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyvx/functions.py`
- Test: `python/geobrix/test/pyvx/test_asmvt_pyramid.py`

> Use the approach chosen in Task 1. The **2B (UDTF)** form is below; if Task 1 selected **2A**, implement `st_asmvt_pyramid` as a `pandas_udf(ArrayType(_serde.TILE_SCHEMA))` returning the list from `_mvt.pyramid_tiles`, and the test calls it as `df.select(f.explode(vx.st_asmvt_pyramid(...)).alias("t"))`.

- [ ] **Step 1: Write the failing test (2B / UDTF)**

```python
# python/geobrix/test/pyvx/test_asmvt_pyramid.py
import mapbox_vector_tile as mvt
from shapely import to_wkb
from shapely.geometry import Point

from databricks.labs.gbx.pyvx import functions as vx


def test_st_asmvt_pyramid_fans_out_per_tile(spark):
    vx.register(spark)
    df = spark.createDataFrame(
        [(bytearray(to_wkb(Point(0.0, 0.0))), "a", 7)],
        "geom binary, name string, id int",
    )
    df.createOrReplaceTempView("feats")
    out = spark.sql(
        "SELECT t.z, t.x, t.y, t.mvt_bytes "
        "FROM feats, LATERAL gbx_st_asmvt_pyramid(geom, struct(name, id), 0, 2, 'layer', 4096) t"
    ).collect()
    zs = sorted({r["z"] for r in out})
    assert zs == [0, 1, 2]
    blob = bytes([r for r in out if r["z"] == 2][0]["mvt_bytes"])
    feats = mvt.decode(blob)["layer"]["features"]
    assert feats[0]["properties"]["id"] == 7
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_asmvt_pyramid.py -v`
Expected: FAIL (`gbx_st_asmvt_pyramid` not registered).

- [ ] **Step 3: Implement the UDTF + register + wrapper (2B)**

Add to `functions.py`:

```python
from pyspark.sql.functions import udtf


@udtf(returnType=_mvt_tile_return())  # defined below
class _AsMvtPyramidUDTF:
    def eval(self, geom_wkb, attrs, min_z: int, max_z: int, layer_name=None, extent=None):
        ln = "layer" if layer_name is None else str(layer_name)
        ex = _mvt.DEFAULT_EXTENT if extent is None else int(extent)
        # yield incrementally — never build the full list (fan-out OOM guard)
        for z, x, y, blob in _mvt.pyramid_tiles(geom_wkb, attrs, int(min_z), int(max_z), ln, ex):
            yield (z, x, y, blob)
```

Add the return-type helper and extend `register`:

```python
def _mvt_tile_return():
    from ._serde import TILE_SCHEMA
    return TILE_SCHEMA


# inside register(spark), after the st_asmvt line:
    spark.udtf.register("gbx_st_asmvt_pyramid", _AsMvtPyramidUDTF)
```

And the Python convenience wrapper (DataFrame ergonomics + parity with the heavy signature):

```python
def st_asmvt_pyramid(
    geom_wkb: ColLike,
    attrs: ColLike,
    min_z: ColLike,
    max_z: ColLike,
    layer_name: Union[ColLike, None] = None,
    extent: Union[ColLike, None] = None,
):
    """Generator: one (z,x,y,mvt_bytes) row per intersecting tile across [min_z,max_z].

    Light tier is a Python UDTF — invoke as a table function:
        SELECT t.* FROM features, LATERAL gbx_st_asmvt_pyramid(geom, attrs, 0, 12, 'layer', 4096) t
    The output schema (z,x,y,mvt_bytes) matches the heavyweight generator and feeds gbx_pmtiles_agg.
    """
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_st_asmvt_pyramid(geom, attrs, min_z, max_z, layer, extent) t"
    )
```

(The `st_asmvt_pyramid` Python function documents the UDTF call form; the usable surface is the registered `gbx_st_asmvt_pyramid` SQL table function. If Task 1 chose 2A, instead make `st_asmvt_pyramid` return `_pyramid_array_udf(...)` and drop the `raise`.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_asmvt_pyramid.py -v`
Expected: PASS.

- [ ] **Step 5: Add a cap-enforcement test + run full pyvx suite**

```python
def test_pyramid_cap_raises(spark):
    vx.register(spark)
    from shapely.geometry import box
    df = spark.createDataFrame([(bytearray(to_wkb(box(-179, -85, 179, 85))),)], "geom binary")
    df.createOrReplaceTempView("big")
    import pytest
    with pytest.raises(Exception):
        spark.sql(
            "SELECT t.z FROM big, LATERAL gbx_st_asmvt_pyramid(geom, struct(), 0, 20, 'l', 4096) t"
        ).collect()
```

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/ -v`
Expected: all pyvx tests PASS.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyvx/functions.py python/geobrix/test/pyvx/test_asmvt_pyramid.py
git commit -m "feat(pyvx): st_asmvt_pyramid generator (UDTF, incremental yield)"
```

---

## Task 6: Serverless-safety guard covers pyvx

**Files:**
- Modify: `python/geobrix/test/pyrx/test_serverless_no_spark_config.py`

- [ ] **Step 1: Extend the guard's source-file set to include pyvx**

In `_source_files()`, add the pyvx package dir alongside the existing pyrx/ds dirs. The function currently globs the light source roots; add:

```python
    roots.append(Path(__file__).parents[2] / "src" / "databricks" / "labs" / "gbx" / "pyvx")
```

(Match the existing `roots`/`Path(...)` construction in that file — read it first and append the `pyvx` root the same way the `pyrx` root is added.)

- [ ] **Step 2: Run the guard**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_serverless_no_spark_config.py -v`
Expected: PASS (pyvx uses only `spark.udf.register`/`spark.udtf.register` + Column/UDF exprs — no `_jvm`/`conf.set`/`.rdd`).

- [ ] **Step 3: Commit**

```bash
git add python/geobrix/test/pyrx/test_serverless_no_spark_config.py
git commit -m "test(pyvx): cover pyvx in the Serverless-safety guard"
```

---

## Task 7: Heavy `MvtWriter` — native OGR field types

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/vectorx/mvt/MvtWriter.scala` (attr field-type logic, ~lines 94–127)
- Modify (if needed): `src/main/scala/com/databricks/labs/gbx/vectorx/expressions/ST_AsMvt.scala` (`encodeAttrs`/`decodeAttrs`, ~lines 109–153) to preserve typed values
- Test: `src/test/scala/com/databricks/labs/gbx/vectorx/expressions/ST_AsMvtTest.scala`

Runs in the `geobrix-dev` Docker container.

- [ ] **Step 1: Write the failing Scala test (native-typed decode)**

Add to `ST_AsMvtTest.scala` a test that encodes an int + double attr, re-reads the MVT bytes with the OGR `MVT` driver, and asserts the field comes back numeric (not string):

```scala
test("st_asmvt encodes numeric attributes with native MVT value types") {
    vectorx.functions.register(spark)
    import vectorx.functions._
    import org.apache.spark.sql.functions.{col, struct, lit}

    val gf = new org.locationtech.jts.geom.GeometryFactory()
    val pt = gf.createPoint(new org.locationtech.jts.geom.Coordinate(0.5, 0.5))
    val df = spark.createDataFrame(Seq((JTS.toWKB(pt), 42, 3.5)))
        .toDF("geom_wkb", "pop", "h")

    val mvtBytes = df.agg(
        st_asmvt(col("geom_wkb"), struct(col("pop"), col("h")), lit("layer1")).as("mvt")
    ).collect().head.getAs[Array[Byte]]("mvt")

    // Decode with OGR MVT driver and assert field types are numeric, not string.
    val (popType, hType) = MvtTestUtil.readFieldTypes(mvtBytes, "layer1", Seq("pop", "h"))
    assert(popType == org.gdal.ogr.ogrConstants.OFTInteger || popType == org.gdal.ogr.ogrConstants.OFTInteger64)
    assert(hType == org.gdal.ogr.ogrConstants.OFTReal)
}
```

Add a small `MvtTestUtil.readFieldTypes` test helper (in the test tree) that writes the bytes to a `/vsimem/` path, opens with the OGR `MVT` driver, and returns the layer's field types. If introducing a helper is undesirable, inline the `/vsimem/` open in the test. (Use `GDALManager.initOgr()` for driver registration, per the repo's thread-safety rule.)

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.vectorx.expressions.ST_AsMvtTest' --log mvt-native.log`
Expected: FAIL (current writer makes every field `OFTString`).

- [ ] **Step 3: Make `MvtWriter` infer native field types**

Replace the all-`OFTString` field creation (line ~105) and the `v.toString` SetField (line ~124) with type-aware logic. For each field, infer the OGR type from the first non-null value's runtime type, then set with the typed setter:

```scala
// helper: OGR field type for a Scala attribute value
private def ogrFieldType(v: Any): Int = v match {
    case _: Int | _: java.lang.Integer            => ogrConstants.OFTInteger
    case _: Long | _: java.lang.Long              => ogrConstants.OFTInteger64
    case _: Double | _: Float
       | _: java.lang.Double | _: java.lang.Float => ogrConstants.OFTReal
    case _: Boolean | _: java.lang.Boolean        => ogrConstants.OFTInteger // subtype Boolean below
    case _                                        => ogrConstants.OFTString
}

// field creation: type per field from first non-null value across features
schema.foreach { fieldName =>
    val firstVal = features.iterator.map(_._2).filter(_ != null)
        .flatMap(m => m.get(fieldName)).find(_ != null)
    val ft = firstVal.map(ogrFieldType).getOrElse(ogrConstants.OFTString)
    val fd = new FieldDefn(fieldName, ft)
    if (firstVal.exists(_.isInstanceOf[Boolean])) fd.SetSubType(ogrConstants.OFSTBoolean)
    layer.CreateField(fd)
    fd.delete()
}

// per-feature SetField: typed setters, str fallback
attrs.get(fieldName).foreach {
    case v: Int     => feat.SetField(fieldName, v)
    case v: Long    => feat.SetFieldInteger64(feat.GetFieldIndex(fieldName), v)
    case v: Double  => feat.SetField(fieldName, v)
    case v: Float   => feat.SetField(fieldName, v.toDouble)
    case v: Boolean => feat.SetField(fieldName, if (v) 1 else 0)
    case null       => ()
    case v          => feat.SetField(fieldName, v.toString)
}
```

Adjust to the exact OGR Java binding method names/imports present in the file (read the current imports; `ogrConstants` may be imported as `ogr` constants). Update the v0.4.0 "all OFTString" comment (lines 25–26) to describe native typing.

- [ ] **Step 4: Ensure `ST_AsMvt.encodeAttrs`/`decodeAttrs` preserve types**

Read `ST_AsMvt.scala` lines ~109–153. If `encodeAttrs` serializes attribute values to strings (so `decodeAttrs` yields a `Map[String, String]`), change the (de)serialization to preserve the Catalyst field types — carry the struct `dataType` so `decodeAttrs` returns `Map[String, Any]` with `Int`/`Long`/`Double`/`Boolean`/`String` runtime values that `MvtWriter.ogrFieldType` can switch on. The Task-7 test is the definition of done: if it passes, types are preserved end to end.

- [ ] **Step 5: Run the suite to verify it passes**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.vectorx.expressions.ST_AsMvtTest' --log mvt-native.log`
Expected: all ST_AsMvt tests PASS (including the new native-type test; existing "non-empty blob"/"layer name" tests still hold).

- [ ] **Step 6: Update any heavy MVT docs that say "stringified"**

`grep -rn -i "OFTString\|stringif\|as string" src/main/scala/com/databricks/labs/gbx/vectorx/mvt/ docs/docs/` and update the doc/comment wording to "native value types". Re-run the wave-leak check is not needed; just keep wording accurate.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/vectorx/mvt/MvtWriter.scala src/main/scala/com/databricks/labs/gbx/vectorx/expressions/ST_AsMvt.scala src/test/scala/com/databricks/labs/gbx/vectorx/expressions/
git commit -m "feat(vectorx): native MVT attribute value types (was OFTString)"
```

---

## Task 8: Light-vs-heavy decoded-feature parity test

**Files:**
- Test: `python/geobrix/test/pyvx/test_parity_mvt.py`

Integration test — needs the heavy JAR in `python/geobrix/lib/` and the Docker container with `/Volumes` (per the corpus-parity convention). Guard with a skip when the JAR/Spark-with-JVM isn't present (like the existing corpus-parity tests).

- [ ] **Step 1: Write the parity test**

```python
# python/geobrix/test/pyvx/test_parity_mvt.py
import os

import mapbox_vector_tile as mvt
import pytest
from shapely import to_wkb
from shapely.geometry import Point

pytestmark = pytest.mark.skipif(
    not os.environ.get("GBX_HEAVY_JAR"),
    reason="needs heavyweight JAR (set GBX_HEAVY_JAR); run in geobrix-dev Docker",
)


def _feats(blob, layer="layer"):
    return {(round(p["properties"]["id"])): p["properties"] for p in mvt.decode(blob)[layer]["features"]}


def test_light_vs_heavy_asmvt_decoded_parity(spark):
    from databricks.labs.gbx.pyvx import functions as vx
    from databricks.labs.gbx.vectorx import functions as hx
    from pyspark.sql import functions as f

    vx.register(spark)
    hx.register(spark)
    rows = [(bytearray(to_wkb(Point(100.0, 200.0))), 1, 3.5), (bytearray(to_wkb(Point(300.0, 400.0))), 2, 9.0)]
    df = spark.createDataFrame(rows, "geom binary, id int, h double")

    light = bytes(df.agg(vx.st_asmvt(f.col("geom"), f.struct("id", "h"), "layer")).collect()[0][0])
    heavy = bytes(df.agg(hx.st_asmvt(f.col("geom"), f.struct("id", "h"), "layer")).collect()[0][0])

    lf, hf = _feats(light), _feats(heavy)
    assert lf.keys() == hf.keys()
    for k in lf:
        assert lf[k]["id"] == hf[k]["id"] and isinstance(lf[k]["id"], int)
        assert abs(float(lf[k]["h"]) - float(hf[k]["h"])) < 1e-9
```

- [ ] **Step 2: Run it in Docker (JAR present)**

Run in the container (which stages the JAR + a JVM Spark): the existing `gbx:test:python` path for integration, e.g.
`bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_parity_mvt.py --log pyvx-parity.log`
(start the container with the volumes script if the test reads corpus; for this synthetic test only the JAR is needed). Expected: PASS — decoded features (ids + native-typed values) match across tiers.

- [ ] **Step 3: Commit**

```bash
git add python/geobrix/test/pyvx/test_parity_mvt.py
git commit -m "test(pyvx): light-vs-heavy decoded-feature MVT parity"
```

---

## Task 9: Bench — `st_asmvt` light-vs-heavy timing + parity

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/readers.py` (add `run_mvt_agg`)
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/cluster.py` (add `_CELL_MVT`, wire `mvt_only`)
- Modify: `notebooks/tests/push_and_run_bench_on_cluster.py` (add `--mvt-only` flag + run_id suffix)

Mirror the existing PMTiles bench (`run_pmtiles_write` + `_CELL_PMTILES` + `--pmtiles-only`) — it is the closest precedent (a non-reader op with a parity check).

- [ ] **Step 1: Add `run_mvt_agg` to `readers.py`**

Implement `run_mvt_agg(spark, run_id, warmup, measured, *, api, where="cluster")` that: builds (or reads) a synthetic features DataFrame keyed by `(z,x,y)`, times `groupBy("z","x","y").agg(<tier>.st_asmvt(...))` (light = `pyvx`, heavy = `vectorx`), and returns a `ResultRow(category="mvt", fn="st_asmvt", mode="spark-path", ...)`. Follow `run_pmtiles_write`'s structure (timing via `time_iters`, env via `capture_env`, registration per tier).

- [ ] **Step 2: Add `_CELL_MVT` to `cluster.py` and a `mvt_only` branch**

Mirror `_CELL_PMTILES`: when `MVT_ONLY`/`BENCHMARK_MVT`, run `run_mvt_agg` for light and heavy, `_sink` the rows, decode both tiers' tiles and assert decoded-feature parity (like the PMTiles cell), then display the `category='mvt'` rows.

- [ ] **Step 3: Add the `--mvt-only` flag to the launcher**

In `push_and_run_bench_on_cluster.py`: parse `--mvt-only`/`--benchmark-mvt`, give `mvt_only` its own run_id suffix (`-mvt`), thread `mvt_only` into `cfg` and the cell selection (mirror `pmtiles_only`). Ensure the row-pool guard skip already covers `*-only` runs (it does, after the earlier fix).

- [ ] **Step 4: Run the bench on the cluster**

Run: `bash scripts/commands/gbx-bench-cluster.sh --mvt-only --spark-warmup 0 --spark-measured 1`
Expected: 2 rows (`api=lightweight` + `heavyweight`, `category=mvt`), parity PASS, light/heavy timings.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/ notebooks/tests/push_and_run_bench_on_cluster.py
git commit -m "feat(bench): st_asmvt light-vs-heavy MVT bench + parity (--mvt-only)"
```

---

## Task 10: Docs — pyvx MVT page + Benchmarking Vector tab + binding consistency

**Files:**
- Create: `docs/docs/api/` (or the VectorX docs location) pyvx MVT page
- Modify: `docs/docs/api/benchmarking.mdx` (the **Vector (soon)** tab)
- Verify: `docs/tests-function-info/registered_functions.txt` + `function-info.json` + `gbx:test:bindings`

- [ ] **Step 1: Write the pyvx MVT doc page**

Mirror the readers/writers doc template (Options near the top if any, Next Steps last, light+heavy tabs via `groupId="gbx-tier"`). Show: `register(spark)`, `groupBy(...).agg(pyvx.st_asmvt(geom, attrs, "layer"))`, and the `LATERAL gbx_st_asmvt_pyramid(...)` table-function usage; note native attribute typing and Serverless support. Use real example code consistent with the doc-test convention if adding a doc test; otherwise show prose examples (per the light-vector-pages precedent).

- [ ] **Step 2: Fill the Benchmarking Vector tab**

In `benchmarking.mdx`, replace the **Vector (soon)** placeholder TabItem content with the `st_asmvt` light-vs-heavy numbers from Task 9 (timing table + parity note), matching the Readers & Writers tab style.

- [ ] **Step 3: Verify binding parity + function-info**

`gbx_st_asmvt` / `gbx_st_asmvt_pyramid` already exist in `registered_functions.txt` (heavy). Run `bash scripts/commands/gbx-test-bindings.sh` (or the equivalent) to confirm Scala name + Python binding(s) + `function-info.json` stay consistent now that `pyvx` adds a second Python binding. Fix any gap upstream (no placeholders).

- [ ] **Step 4: Build the docs + wave-leak check**

Run: `cd docs && npm run build` (expect SUCCESS) and `grep -rn -iE "wave [0-9]+" docs/docs/` (expect empty).

- [ ] **Step 5: Commit**

```bash
git add docs/
git commit -m "docs(pyvx): MVT light tier page + benchmarking Vector tab"
```

---

## Final review

After all tasks: dispatch a final code review across the branch (light pyvx package, heavy MVT native-typing change, bench, docs), confirm the full pyvx pytest suite + the ST_AsMvt Scala suite are green, then finish the branch (PR into `beta/0.4.0`) per the project's PR flow.
