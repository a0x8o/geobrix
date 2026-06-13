# pyvx VectorX TIN + Legacy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port the 4 remaining heavy VectorX `gbx_st_*` functions (`st_legacyaswkb`, `st_triangulate`, `st_interpolateelevationbbox`, `st_interpolateelevationgeom`) to the pyvx light tier, with cross-tier alignment, so the light tier is a genuine exit from heavy.

**Architecture:** Pure-Python/PySpark light tier (Serverless/Connect-safe: `udf`/`udtf` + Column only — never `_jvm`/`spark.conf.set`/`.rdd`). TIN triangulation = scipy `Delaunay` + a hand-rolled Sloan constraint-recovery core in `pyvx/_tin.py`; legacy decode = shapely in `pyvx/_legacy.py`. Heavy gains a `mode` param (`"constrained"` default / `"conforming"` opt-in) and two legacy bug-fixes (preserve Z, preserve holes).

**Tech Stack:** Python 3.12, scipy `Delaunay`, shapely 2.x (WKB/WKT I/O), numpy, PySpark `@udf`/`@udtf`; Scala 2.13 / JTS (heavy).

**Spec:** `docs/superpowers/specs/2026-06-13-pyvx-vectorx-tin-legacy-light-tier-design.md`

**Branch:** `pyvx-light`. **PR:** #38 (draft — won't merge until this lands).

**No new dependencies** — `scipy>=1.11.0`, `shapely>=2.0.0`, `mapbox-vector-tile` are already in the `[light]` / `[test]` extras of `python/geobrix/pyproject.toml`.

---

## Conventions every task follows

- Run Python tests via the dev container: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/<file> --log <name>.log`. Spark-free unit tests (`_tin.py`, `_legacy.py`) also run on the host venv `.venv-pyrx/bin/python -m pytest`.
- Run Scala tests: `bash scripts/commands/gbx-test-scala.sh --suites '<fqcn>' --log <name>.log`.
- Commit messages: subject ≤72 chars + a WHY body for non-trivial commits; trailer exactly `Co-authored-by: Isaac`. Before commit: `chmod -R u+rwX .git/objects`.
- Serverless-safety: the pyvx package must never reference `_jvm`, `sparkContext`, `.rdd`, or `spark.conf.set`. `test/pyvx/test_serverless_no_spark_config.py` already guards this — keep it green.

## Geometry input contract (cross-cutting — applies to EVERY geom-accepting function)

Every pyvx function that accepts a geometry argument MUST accept the **same set of encodings**, consistent with the heavy ST surface: **WKB, EWKB, WKT, and EWKT** (`SRID=<n>;<wkt>`). This is centralized in one helper so the contract can't drift between functions.

- Create `python/geobrix/src/databricks/labs/gbx/pyvx/_geom.py` with:

```python
"""Shared geometry-input parsing for the pyvx light tier.

Every geom-accepting pyvx function uses parse_geom so the accepted encodings
(WKB / EWKB / WKT / EWKT) stay consistent across the ST surface and match the
heavyweight tier (which accepts BINARY|STRING for geometry inputs).
"""
from typing import Any, Optional
from shapely import from_wkb, from_wkt, set_srid
from shapely.geometry.base import BaseGeometry


def parse_geom(x: Any) -> Optional[BaseGeometry]:
    """Parse a geometry from WKB/EWKB bytes or WKT/EWKT text. None -> None.

    shapely.from_wkb already reads EWKB (SRID embedded). shapely.from_wkt does
    NOT understand the EWKT 'SRID=<n>;' prefix, so strip+apply it here.
    """
    if x is None:
        return None
    if isinstance(x, (bytes, bytearray)):
        return from_wkb(bytes(x))  # handles WKB and EWKB
    s = str(x).strip()
    if s[:5].upper() == "SRID=":
        srid_part, _, wkt_part = s.partition(";")
        geom = from_wkt(wkt_part)
        try:
            return set_srid(geom, int(srid_part[5:]))
        except ValueError:
            return geom
    return from_wkt(s)
```

- All geom-accepting functions call `parse_geom` (TIN `points`/`breaklines` via `_geoms_from_array`, the geom-grid `grid_origin`). Where a task below shows inline `from_wkb`/`from_wkt`, replace it with `parse_geom`.
- **Consistency audit (Task 12):** confirm the existing `st_asmvt` geom input (`_asmvt_udf`) accepts the same set (today it takes WKB bytes only) and align it to `parse_geom` if heavy `gbx_st_asmvt` accepts WKT/EWKT too; verify all `gbx_st_*` geom params share the contract.

---

## File Structure

| File | Responsibility |
|---|---|
| `python/geobrix/src/databricks/labs/gbx/pyvx/_geom.py` | **new** — shared `parse_geom` (WKB/EWKB/WKT/EWKT) used by every geom-accepting pyvx function |
| `python/geobrix/src/databricks/labs/gbx/pyvx/_legacy.py` | **new** — decode legacy Mosaic struct → shapely geom (Z + holes preserved) |
| `python/geobrix/src/databricks/labs/gbx/pyvx/_tin.py` | **new** — scipy Delaunay + Sloan constraint recovery + Z-snap + barycentric interp + grid generators (Spark-free) |
| `python/geobrix/src/databricks/labs/gbx/pyvx/functions.py` | **modify** — add `st_legacyaswkb` UDF, 3 TIN `@udtf` classes + wrappers, extend `register()` |
| `python/geobrix/src/databricks/labs/gbx/pyvx/_env.py` | **modify** — add a `assert_tin_available()` guard (scipy + shapely) |
| `python/geobrix/test/pyvx/test_legacy.py` | **new** — Spark-free `_legacy` unit tests |
| `python/geobrix/test/pyvx/test_tin_core.py` | **new** — Spark-free `_tin` unit tests (the Sloan core, hardest) |
| `python/geobrix/test/pyvx/test_legacy_udf.py` | **new** — registered `st_legacyaswkb` via spark fixture |
| `python/geobrix/test/pyvx/test_tin_udtf.py` | **new** — registered TIN UDTFs via spark fixture |
| `python/geobrix/test/pyvx/test_parity_legacy.py` | **new** — light↔heavy parity (JAR-gated) |
| `python/geobrix/test/pyvx/test_parity_tin.py` | **new** — light↔heavy TIN parity (JAR-gated) |
| `src/main/scala/com/databricks/labs/gbx/vectorx/jts/legacy/InternalGeometry.scala` | **modify** — fix dropped-holes TODO |
| `src/main/scala/com/databricks/labs/gbx/vectorx/jts/legacy/expressions/ST_LegacyAsWKB.scala` | **modify** — `toWKB` → `toWKB3` (preserve Z) |
| `src/main/scala/com/databricks/labs/gbx/vectorx/jts/InterpolateElevation.scala` | **modify** — add `mode` (constrained vs conforming) to `triangulate` |
| `src/main/scala/com/databricks/labs/gbx/vectorx/expressions/ST_Triangulate.scala` | **modify** — add trailing `mode` arg (arity 5→5/6) |
| `src/main/scala/com/databricks/labs/gbx/vectorx/expressions/ST_InterpolateElevationBBox.scala` | **modify** — add trailing `mode` arg (arity 12→12/13) |
| `src/main/scala/com/databricks/labs/gbx/vectorx/expressions/ST_InterpolateElevationGeom.scala` | **modify** — add trailing `mode` arg (arity 10→10/11) |
| `python/geobrix/src/databricks/labs/gbx/pyvx/functions.py` (bindings) + `docs/tests/python/api/vectorx_functions_sql.py` + `docs/docs/api/vectorx-functions.mdx` | **modify** — bindings, function-info examples, docs |

---

# PHASE 1 — Legacy (`st_legacyaswkb`), both tiers

Independent, low-risk, establishes the non-MVT scalar-UDF pattern.

## Task 1: Light legacy decode core (`_legacy.py`)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyvx/_legacy.py`
- Test: `python/geobrix/test/pyvx/test_legacy.py`

The input is the legacy Mosaic struct, which arrives in a UDF as a `pyspark.sql.Row` (or a nested dict). Shape:
`{typeId: int, srid: int, boundaries: list[list[list[float]]], holes: list[list[list[list[float]]]]}`.
typeId map: 1 POINT, 2 MULTIPOINT, 3 LINESTRING, 4 MULTILINESTRING, 5 POLYGON, 6 MULTIPOLYGON, 7 LINEARRING, 8 GEOMETRYCOLLECTION (→ raise). Each coordinate is `[x, y]` or `[x, y, z]`.

- [ ] **Step 1: Write the failing tests**

```python
# test/pyvx/test_legacy.py
import pytest

shapely = pytest.importorskip("shapely")
from shapely.geometry import Point, LineString, Polygon, MultiPolygon  # noqa: E402
from shapely import wkb  # noqa: E402

from databricks.labs.gbx.pyvx import _legacy


def _row(type_id, boundaries, holes=None, srid=0):
    return {"typeId": type_id, "srid": srid, "boundaries": boundaries, "holes": holes or []}


def test_point_xy():
    g = _legacy.legacy_to_geom(_row(1, [[[30.0, 10.0]]]))
    assert g.equals(Point(30.0, 10.0))


def test_point_xyz_preserves_z():
    g = _legacy.legacy_to_geom(_row(1, [[[30.0, 10.0, 5.0]]]))
    assert g.has_z and abs(g.z - 5.0) < 1e-9


def test_linestring():
    g = _legacy.legacy_to_geom(_row(3, [[[0.0, 0.0], [1.0, 1.0]]]))
    assert g.equals(LineString([(0, 0), (1, 1)]))


def test_polygon_preserves_holes():
    # outer 0..10 square, one inner hole 2..4 square
    outer = [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]]
    hole = [[2.0, 2.0], [4.0, 2.0], [4.0, 4.0], [2.0, 4.0], [2.0, 2.0]]
    g = _legacy.legacy_to_geom(_row(5, [outer], holes=[[hole]]))
    assert len(g.interiors) == 1
    assert abs(g.area - (100.0 - 4.0)) < 1e-9


def test_multipolygon_preserves_holes():
    sq = lambda o, s: [[o, o], [o + s, o], [o + s, o + s], [o, o + s], [o, o]]
    poly0 = [sq(0.0, 10.0)]
    hole0 = [sq(2.0, 2.0)]
    poly1 = [sq(20.0, 5.0)]
    g = _legacy.legacy_to_geom(_row(6, [poly0[0], poly1[0]], holes=[[hole0[0]], []]))
    assert isinstance(g, MultiPolygon)
    assert sum(len(p.interiors) for p in g.geoms) == 1


def test_geometrycollection_raises():
    with pytest.raises(ValueError, match="GeometryCollection"):
        _legacy.legacy_to_geom(_row(8, []))


def test_aswkb_preserves_z_iso():
    out = _legacy.legacy_to_wkb(_row(1, [[[30.0, 10.0, 5.0]]]))
    assert wkb.loads(out).has_z
```

- [ ] **Step 2: Run to verify they fail**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_legacy.py -v`
Expected: FAIL (`module databricks.labs.gbx.pyvx has no attribute _legacy` / `legacy_to_geom`).

- [ ] **Step 3: Implement `_legacy.py`**

```python
"""Legacy Mosaic geometry decode for the pyvx light tier.

Decodes the legacy internal struct {typeId, srid, boundaries, holes} into a
shapely geometry, preserving Z and polygon holes, then serializes to WKB.
Heavy parity target: databricks.labs.gbx.vectorx.jts.legacy (with the Z-drop
and holes-drop bugs fixed in both tiers).
"""
from typing import Any, List, Optional, Sequence

from shapely import to_wkb
from shapely.geometry import (
    LinearRing,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)

# typeId -> name (mirrors GeometryTypeEnum in jts/legacy)
_POINT, _MULTIPOINT, _LINESTRING, _MULTILINESTRING = 1, 2, 3, 4
_POLYGON, _MULTIPOLYGON, _LINEARRING, _GEOMETRYCOLLECTION = 5, 6, 7, 8


def _field(row: Any, name: str, idx: int) -> Any:
    """Read a field from a Row or dict-like legacy struct."""
    if hasattr(row, "__fields__"):  # pyspark Row
        return row[name]
    if isinstance(row, dict):
        return row.get(name)
    return row[idx]  # positional tuple fallback


def _ring(coords: Sequence[Sequence[float]]) -> List[tuple]:
    return [tuple(float(v) for v in c) for c in coords]


def legacy_to_geom(row: Any):
    """Decode the legacy struct into a shapely geometry (Z + holes preserved)."""
    type_id = int(_field(row, "typeId", 0))
    boundaries = _field(row, "boundaries", 2) or []
    holes = _field(row, "holes", 3) or []

    if type_id == _POINT:
        return Point(*_ring(boundaries[0])[0])
    if type_id == _MULTIPOINT:
        return MultiPoint([_ring(p)[0] for p in boundaries[0]])
    if type_id in (_LINESTRING, _LINEARRING):
        return LineString(_ring(boundaries[0]))
    if type_id == _MULTILINESTRING:
        return MultiLineString([_ring(ls) for ls in boundaries])
    if type_id == _POLYGON:
        shell = _ring(boundaries[0])
        rings = holes[0] if holes else []
        return Polygon(shell, [_ring(h) for h in rings])
    if type_id == _MULTIPOLYGON:
        polys = []
        for i, shell_coords in enumerate(boundaries):
            shell = _ring(shell_coords)
            rings = holes[i] if i < len(holes) and holes[i] else []
            polys.append(Polygon(shell, [_ring(h) for h in rings]))
        return MultiPolygon(polys)
    if type_id == _GEOMETRYCOLLECTION:
        raise ValueError("GeometryCollection is not supported by st_legacyaswkb")
    raise ValueError(f"unknown legacy geometry typeId: {type_id}")


def legacy_to_wkb(row: Any) -> Optional[bytes]:
    """Decode the legacy struct and return ISO WKB (Z preserved when present)."""
    if row is None:
        return None
    geom = legacy_to_geom(row)
    # shapely defaults: flavor="iso", output_dimension=3 -> Z written when present.
    return to_wkb(geom)
```

- [ ] **Step 4: Run to verify they pass**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_legacy.py -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyvx/_legacy.py python/geobrix/test/pyvx/test_legacy.py
git commit -m "feat(pyvx): legacy geometry decode core (Z + holes preserved)

Pure shapely decode of the legacy Mosaic struct; preserves polygon holes and
Z (ISO WKB), which the heavy tier currently drops. Spark-free, TDD."
```

## Task 2: Light `st_legacyaswkb` UDF + register + wrapper

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyvx/functions.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/pyvx/_env.py`
- Test: `python/geobrix/test/pyvx/test_legacy_udf.py`

- [ ] **Step 1: Write the failing test**

```python
# test/pyvx/test_legacy_udf.py
import pytest

shapely = pytest.importorskip("shapely")
from shapely import wkb  # noqa: E402

from databricks.labs.gbx.pyvx import functions as vx


def test_st_legacyaswkb_roundtrips_polygon_with_hole(spark):
    vx.register(spark)
    outer = [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]]
    hole = [[2.0, 2.0], [4.0, 2.0], [4.0, 4.0], [2.0, 4.0], [2.0, 2.0]]
    schema = "g struct<typeId:int,srid:int,boundaries:array<array<array<double>>>,holes:array<array<array<array<double>>>>>"
    df = spark.createDataFrame([({"typeId": 5, "srid": 0, "boundaries": [outer], "holes": [[hole]]},)], schema)
    out = df.selectExpr("gbx_st_legacyaswkb(g) AS wkb").collect()
    geom = wkb.loads(bytes(out[0]["wkb"]))
    assert len(geom.interiors) == 1
    assert abs(geom.area - 96.0) < 1e-9
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_legacy_udf.py --log legacy-udf.log`
Expected: FAIL (`gbx_st_legacyaswkb` not registered).

- [ ] **Step 3: Add the env guard**

In `python/geobrix/src/databricks/labs/gbx/pyvx/_env.py`, add after `assert_mvt_available`:

```python
def assert_tin_available() -> None:
    """Raise a clear ImportError if the TIN/legacy light deps are missing."""
    missing = []
    try:
        import scipy  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("scipy")
    try:
        import shapely  # noqa: F401
    except Exception:  # noqa: BLE001
        missing.append("shapely")
    if missing:
        raise ImportError(
            "pyvx TIN/legacy requires the [light] extra; missing: "
            + ", ".join(missing)
            + ". Install with: pip install 'geobrix[light]'"
        )
```

- [ ] **Step 4: Implement the UDF + register + wrapper in `functions.py`**

Add the import (top, alongside `from . import _env, _mvt`): change to `from . import _env, _mvt, _legacy`.

Add the UDF near `_asmvt_udf`:

```python
def _legacyaswkb_impl(geom):
    """Scalar: decode a legacy Mosaic struct row to ISO WKB (Z + holes)."""
    return _legacy.legacy_to_wkb(geom)
```

In `register(spark)`, after the MVT registrations add:

```python
    _env.assert_tin_available()
    spark.udf.register("gbx_st_legacyaswkb", _legacyaswkb_impl, BinaryType())
```

Add the Column wrapper (bottom of file, mirroring `st_asmvt`):

```python
def st_legacyaswkb(geom: ColLike) -> Column:
    """Decode a legacy Mosaic geometry struct to ISO WKB (Z + holes preserved)."""
    return f.call_function("gbx_st_legacyaswkb", _col(geom))
```

- [ ] **Step 5: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_legacy_udf.py --log legacy-udf.log`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyvx/functions.py python/geobrix/src/databricks/labs/gbx/pyvx/_env.py python/geobrix/test/pyvx/test_legacy_udf.py
git commit -m "feat(pyvx): register gbx_st_legacyaswkb scalar UDF

Decode legacy Mosaic geometry struct to ISO WKB (Z + holes preserved),
Serverless-safe (spark.udf.register only). Adds assert_tin_available guard."
```

## Task 3: Heavy legacy fixes — preserve holes + Z

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/vectorx/jts/legacy/InternalGeometry.scala:21-39`
- Modify: `src/main/scala/com/databricks/labs/gbx/vectorx/jts/legacy/expressions/ST_LegacyAsWKB.scala:33`
- Test: `src/test/scala/com/databricks/labs/gbx/vectorx/jts/legacy/InternalGeometryTest.scala` (create if absent; otherwise add cases)

- [ ] **Step 1: Write the failing Scala test**

Add a test asserting a POLYGON with one hole round-trips through `InternalGeometry(row).toJTS` with `getNumInteriorRing == 1`, and that `ST_LegacyAsWKB.eval` on a Z-valued POINT yields WKB whose `WKBReader` reads a 3-D coordinate (`!coord.getZ.isNaN`).

```scala
// InternalGeometryTest.scala (sketch — use the project's WithExpressionInfo test base + JTS reads)
test("toJTS preserves polygon holes") {
  val ig = InternalGeometry(/* polygon InternalRow with one hole */)
  val poly = ig.toJTS.asInstanceOf[org.locationtech.jts.geom.Polygon]
  assert(poly.getNumInteriorRing == 1)
}
test("ST_LegacyAsWKB preserves Z") {
  val wkb = ST_LegacyAsWKB.eval(/* POINT Z InternalRow */)
  val g = new org.locationtech.jts.io.WKBReader().read(wkb)
  assert(!g.getCoordinate.getZ.isNaN)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-scala.sh --suites 'com.databricks.labs.gbx.vectorx.jts.legacy.InternalGeometryTest' --log legacy-scala.log`
Expected: FAIL (holes dropped → `getNumInteriorRing == 0`; Z dropped → `getZ.isNaN`).

- [ ] **Step 3: Fix `InternalGeometry.toJTS` (holes) — replace the POLYGON/MULTIPOLYGON branches**

```scala
        case GeometryTypeEnum.POLYGON            =>
            val shell = boundaries.head.map(c => c.toCoordinate)
            val rings = if (holes.nonEmpty) holes.head.map(_.map(_.toCoordinate)) else Seq.empty
            JTS.polygonWithHoles(shell, rings)
        case GeometryTypeEnum.MULTIPOLYGON       =>
            val polys = boundaries.indices.map { i =>
                val shell = boundaries(i).map(c => c.toCoordinate)
                val rings = if (i < holes.length && holes(i).nonEmpty) holes(i).map(_.map(_.toCoordinate)) else Seq.empty
                JTS.polygonWithHoles(shell, rings)
            }
            JTS.multiPolygon(polys)
```

If `JTS.polygonWithHoles` / `JTS.multiPolygon(Seq[Polygon])` helpers do not exist, add them to `JTS.scala` (use `geometryFactory.createPolygon(shell, holes.toArray)` / `createMultiPolygon`). Preserve the existing helper-naming style in `JTS.scala`.

- [ ] **Step 4: Fix `ST_LegacyAsWKB.eval` (Z) — `src/.../ST_LegacyAsWKB.scala:33`**

```scala
        JTS.toWKB3(geom)
```
(was `JTS.toWKB(geom)`.)

- [ ] **Step 5: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-scala.sh --suites 'com.databricks.labs.gbx.vectorx.jts.legacy.InternalGeometryTest' --log legacy-scala.log`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects
git add src/main/scala/com/databricks/labs/gbx/vectorx/jts/legacy/ src/test/scala/com/databricks/labs/gbx/vectorx/jts/legacy/
git commit -m "fix(vectorx): legacy decode preserves holes + Z

InternalGeometry.toJTS now keeps polygon/multipolygon interior rings (was a
TODO that dropped them); ST_LegacyAsWKB emits toWKB3 so Z survives. Aligns
heavy with the new pyvx light st_legacyaswkb."
```

## Task 4: Cross-tier legacy parity + bindings + function-info

**Files:**
- Test: `python/geobrix/test/pyvx/test_parity_legacy.py`
- Modify: `docs/tests/python/api/vectorx_functions_sql.py` (the `st_legacyaswkb_sql_example_output`)

- [ ] **Step 1: Write the JAR-gated parity test** (copy the gating block from `test_parity_mvt.py` verbatim — `pytestmark = pytest.mark.integration`, the `_JARS` glob on `parents[2] / "lib"`, the `spark_with_jar` fixture with the active-session skip).

```python
def test_legacy_parity_polygon_with_hole_and_z(spark_with_jar):
    spark = spark_with_jar
    from databricks.labs.gbx.pyvx import functions as vx
    from databricks.labs.gbx.vectorx.jts.legacy import functions as hx
    vx.register(spark)
    hx.register(spark)
    outer = [[0.0, 0.0, 1.0], [10.0, 0.0, 1.0], [10.0, 10.0, 1.0], [0.0, 10.0, 1.0], [0.0, 0.0, 1.0]]
    hole = [[2.0, 2.0, 1.0], [4.0, 2.0, 1.0], [4.0, 4.0, 1.0], [2.0, 4.0, 1.0], [2.0, 2.0, 1.0]]
    schema = "g struct<typeId:int,srid:int,boundaries:array<array<array<double>>>,holes:array<array<array<array<double>>>>>"
    df = spark.createDataFrame([({"typeId": 5, "srid": 0, "boundaries": [outer], "holes": [[hole]]},)], schema)
    light = bytes(df.selectExpr("gbx_st_legacyaswkb(g) AS w").collect()[0]["w"])
    heavy = bytes(df.selectExpr("gbx_st_legacyaswkb(g) AS w").collect()[0]["w"])  # heavy reg overwrote? -> register separately
    from shapely import wkb
    lg, hg = wkb.loads(light), wkb.loads(heavy)
    assert lg.equals(hg)
    assert len(lg.interiors) == 1 and len(hg.interiors) == 1
```
Note: because both tiers register the *same* SQL name `gbx_st_legacyaswkb`, register and collect the light result first, then register heavy and collect — or compare the light WKB against shapely-decoded heavy WKB obtained in a separate session. Keep the assertion: decoded geometries equal, both retain the hole, both retain Z (`lg.has_z`).

- [ ] **Step 2: Run in Docker (JAR present)**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_parity_legacy.py --with-integration --log parity-legacy.log`
Expected: PASS (skips only if no JAR staged).

- [ ] **Step 3: Update the function-info example output** (`docs/tests/python/api/vectorx_functions_sql.py`) so `st_legacyaswkb_sql_example_output` shows a `[BINARY]` table (add it if absent, canonically aligned per the D5 checker — `+` borders sized to max cell width).

- [ ] **Step 4: Add the pyvx binding to the canonical list check**

Run `bash scripts/commands/gbx-test-bindings.sh --log bindings-legacy.log`. `gbx_st_legacyaswkb` already exists in `registered_functions.txt`, Scala, heavy Python, and `function-info.json`; this confirms the light binding doesn't break parity. Fix any failure upstream (not by editing the canonical list).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/pyvx/test_parity_legacy.py docs/tests/python/api/vectorx_functions_sql.py
git commit -m "test(pyvx): light-vs-heavy legacy parity (holes + Z)

Cross-tier decoded-geometry equality for gbx_st_legacyaswkb incl. a holed,
Z-valued polygon; JAR-gated. Refresh the function-info example output."
```

---

# PHASE 2 — Light TIN block

Pure-Python `_tin.py` core (TDD the Sloan recovery hardest), then the 3 `@udtf` functions with the `mode` param.

## Task 5: `_tin.py` — unconstrained triangulation + vertex merge

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pyvx/_tin.py`
- Test: `python/geobrix/test/pyvx/test_tin_core.py`

- [ ] **Step 1: Write the failing tests**

```python
# test/pyvx/test_tin_core.py
import numpy as np
import pytest

pytest.importorskip("scipy")
from databricks.labs.gbx.pyvx import _tin


def test_triangulate_square_gives_two_triangles():
    pts = np.array([[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [1.0, 1.0, 0.0], [0.0, 1.0, 0.0]])
    tris = _tin.triangulate(pts, breaklines=[], merge_tolerance=0.0, snap_tolerance=0.0)
    assert len(tris) == 2  # two triangles cover the unit square
    # each triangle is a (3,3) array of XYZ vertices
    assert all(t.shape == (3, 3) for t in tris)


def test_merge_tolerance_dedups_near_coincident():
    pts = np.array([[0, 0, 0], [1, 0, 0], [1, 1, 0], [0, 1, 0], [1e-9, 1e-9, 0]], dtype=float)
    tris = _tin.triangulate(pts, breaklines=[], merge_tolerance=1e-6, snap_tolerance=0.0)
    assert len(tris) == 2  # the near-duplicate vertex is merged away


def test_empty_or_too_few_points():
    assert _tin.triangulate(np.zeros((0, 3)), [], 0.0, 0.0) == []
    assert _tin.triangulate(np.array([[0.0, 0.0, 0.0], [1.0, 1.0, 0.0]]), [], 0.0, 0.0) == []
```

- [ ] **Step 2: Run to verify they fail**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_tin_core.py -v`
Expected: FAIL (`_tin` missing).

- [ ] **Step 3: Implement the unconstrained core + merge**

```python
"""Pure-Python TIN engine for the pyvx light tier (Serverless-safe).

scipy Delaunay + Sloan constraint recovery (constrained, no Steiner points),
Z-snap to breaklines, barycentric Z interpolation, and grid generators.
Heavy parity target: vectorx.jts.InterpolateElevation (mode="constrained").
"""
from typing import List, Sequence, Tuple

import numpy as np
from scipy.spatial import Delaunay


def _merge_vertices(pts: np.ndarray, tol: float) -> np.ndarray:
    """Snap near-coincident XY vertices (within tol) to a single representative."""
    if tol <= 0.0 or len(pts) == 0:
        return pts
    keys = np.round(pts[:, :2] / tol).astype(np.int64)
    _, idx = np.unique(keys, axis=0, return_index=True)
    return pts[np.sort(idx)]


def triangulate(
    points: np.ndarray,
    breaklines: Sequence[np.ndarray],
    merge_tolerance: float,
    snap_tolerance: float,
) -> List[np.ndarray]:
    """Constrained Delaunay over XYZ points. Returns a list of (3,3) XYZ triangles.

    breaklines: sequence of (N,2|3) constraint polylines whose segments are forced
    as triangle edges (Sloan recovery). Empty -> plain Delaunay.
    """
    pts = _merge_vertices(np.asarray(points, dtype=float), merge_tolerance)
    if len(pts) < 3:
        return []
    tri = Delaunay(pts[:, :2])
    simplices = tri.simplices.copy()
    if breaklines:
        simplices = _recover_constraints(pts[:, :2], simplices, tri, breaklines)
    z = pts[:, 2]
    out = [np.column_stack([pts[s, 0], pts[s, 1], z[s]]) for s in simplices]
    if snap_tolerance > 0.0 and breaklines:
        out = _zsnap(out, breaklines, snap_tolerance)
    return out
```

(`_recover_constraints` and `_zsnap` are defined in Task 6 — for this task, guard the `if breaklines:` branch behind a `raise NotImplementedError` placeholder is NOT allowed; instead implement Task 5 with `breaklines=[]` only paths exercised, and define `_recover_constraints`/`_zsnap` as the real functions in Task 6. To keep Task 5 self-contained and green, temporarily define them as identity/no-op stubs *with a test that only uses empty breaklines*, then replace in Task 6.)

To keep Task 5 green without breakline tests, add minimal definitions:

```python
def _recover_constraints(xy, simplices, tri, breaklines):
    return simplices  # replaced with real Sloan recovery in the next task


def _zsnap(triangles, breaklines, tol):
    return triangles  # replaced in the next task
```

- [ ] **Step 4: Run to verify they pass**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_tin_core.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyvx/_tin.py python/geobrix/test/pyvx/test_tin_core.py
git commit -m "feat(pyvx): TIN unconstrained Delaunay + vertex merge core

scipy Delaunay triangulation with mergeTolerance vertex dedup; constraint
recovery + Z-snap are stubbed pending the next task."
```

## Task 6: `_tin.py` — Sloan constraint recovery + Z-snap (the hard core)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyvx/_tin.py`
- Test: `python/geobrix/test/pyvx/test_tin_core.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_breakline_appears_as_triangle_edges():
    # square + center diagonal breakline that the plain Delaunay might not use
    pts = np.array([[0,0,0],[4,0,0],[4,4,0],[0,4,0],[1,3,0],[3,1,0]], dtype=float)
    bl = [np.array([[1.0, 3.0], [3.0, 1.0]])]
    tris = _tin.triangulate(pts, bl, 0.0, 0.0)
    edges = set()
    for t in tris:
        xy = [tuple(np.round(p[:2], 6)) for p in t]
        for a, b in [(0, 1), (1, 2), (2, 0)]:
            edges.add(frozenset([xy[a], xy[b]]))
    assert frozenset([(1.0, 3.0), (3.0, 1.0)]) in edges


def test_recovery_terminates_on_dense_constraints():
    rng = np.random.default_rng(0)
    pts = np.column_stack([rng.random(40), rng.random(40), np.zeros(40)])
    bl = [np.array([[0.05, 0.05], [0.95, 0.95]])]
    tris = _tin.triangulate(pts, bl, 0.0, 0.0)  # must not hang
    assert len(tris) > 0


def test_zsnap_sets_vertex_z_along_constraint():
    # flat points at z=0, a breakline carrying z=10 at its endpoints
    pts = np.array([[0,0,0],[4,0,0],[4,4,0],[0,4,0]], dtype=float)
    bl = [np.array([[0.0, 2.0, 10.0], [4.0, 2.0, 10.0]])]
    tris = _tin.triangulate(pts, bl, 0.0, 1e-6)
    # any vertex landing on the breakline keeps z≈10 (within snap)
    near = [p[2] for t in tris for p in t if abs(p[1] - 2.0) < 1e-9]
    # at minimum the recovery + snap must not crash and must yield triangles
    assert len(tris) > 0
```

- [ ] **Step 2: Run to verify the breakline test fails**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_tin_core.py::test_breakline_appears_as_triangle_edges -v`
Expected: FAIL (stub returns plain Delaunay; diagonal not guaranteed an edge).

- [ ] **Step 3: Replace the stubs with the real Sloan recovery + Z-snap**

```python
def _orient2d(a, b, c) -> float:
    """>0 if a->b->c is counter-clockwise."""
    return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])


def _segments_intersect(p1, p2, p3, p4) -> bool:
    d1 = _orient2d(p3, p4, p1); d2 = _orient2d(p3, p4, p2)
    d3 = _orient2d(p1, p2, p3); d4 = _orient2d(p1, p2, p4)
    return ((d1 > 0) != (d2 > 0)) and ((d3 > 0) != (d4 > 0))


def _vertex_index(xy: np.ndarray, p, tol=1e-9) -> int:
    d = np.hypot(xy[:, 0] - p[0], xy[:, 1] - p[1])
    i = int(np.argmin(d))
    return i if d[i] <= max(tol, 1e-9) else -1


def _recover_constraints(xy: np.ndarray, simplices: np.ndarray, tri, breaklines):
    """Sloan constraint recovery: force each breakline segment to be an edge.

    Operates on a mutable triangle list with an edge->triangles adjacency map,
    flipping intersected diagonals (only across convex quads) until each
    constraint segment is present as a triangle edge. Bounded flip budget.
    """
    triangles = [list(s) for s in simplices.tolist()]

    def edges_of(t):
        return [frozenset((t[0], t[1])), frozenset((t[1], t[2])), frozenset((t[2], t[0]))]

    def build_adj():
        adj = {}
        for ti, t in enumerate(triangles):
            for e in edges_of(t):
                adj.setdefault(e, []).append(ti)
        return adj

    for bl in breaklines:
        seg_pts = np.asarray(bl, dtype=float)
        for k in range(len(seg_pts) - 1):
            ia = _vertex_index(xy, seg_pts[k]); ib = _vertex_index(xy, seg_pts[k + 1])
            if ia < 0 or ib < 0 or ia == ib:
                continue
            target = frozenset((ia, ib))
            budget = 50 * len(triangles) + 100
            while target not in build_adj() and budget > 0:
                budget -= 1
                adj = build_adj()
                flipped = False
                for e, ts in adj.items():
                    if len(ts) != 2:
                        continue
                    (u, v) = tuple(e)
                    if not _segments_intersect(xy[ia], xy[ib], xy[u], xy[v]):
                        continue
                    t0, t1 = triangles[ts[0]], triangles[ts[1]]
                    w0 = next(x for x in t0 if x not in e)
                    w1 = next(x for x in t1 if x not in e)
                    # convex quad test: diagonal (w0,w1) must cross (u,v)
                    if not _segments_intersect(xy[w0], xy[w1], xy[u], xy[v]):
                        continue
                    triangles[ts[0]] = [w0, w1, u]
                    triangles[ts[1]] = [w0, w1, v]
                    flipped = True
                    break
                if not flipped:
                    break  # cannot recover this segment with convex flips; leave as-is
            if budget <= 0:
                raise RuntimeError("Sloan constraint recovery did not terminate")
    return np.array(triangles, dtype=np.int64)


def _zsnap(triangles, breaklines, tol):
    """Overwrite vertex Z with linear interpolation along any constraint line
    within tol (mirrors heavy LengthIndexedLine post-process)."""
    snapped = []
    for t in triangles:
        t = t.copy()
        for vi in range(3):
            p = t[vi]
            for bl in breaklines:
                bl = np.asarray(bl, dtype=float)
                if bl.shape[1] < 3:
                    continue
                for k in range(len(bl) - 1):
                    a, b = bl[k], bl[k + 1]
                    ab = b[:2] - a[:2]
                    L2 = float(ab @ ab)
                    if L2 == 0.0:
                        continue
                    s = float((p[:2] - a[:2]) @ ab) / L2
                    if 0.0 <= s <= 1.0:
                        proj = a[:2] + s * ab
                        if np.hypot(*(p[:2] - proj)) <= tol:
                            t[vi, 2] = a[2] + s * (b[2] - a[2])
            snapped.append(None)  # placeholder; replaced below
        snapped[-1] = t
    # rebuild cleanly (one entry per triangle)
    return [t for t in (snapped[i] for i in range(len(triangles)))]
```

Note for the implementer: simplify the `_zsnap` accumulation to one appended `t` per triangle (the sketch's placeholder bookkeeping is illustrative — append each finished `t` once). Keep the projection-onto-segment math exactly.

- [ ] **Step 4: Run to verify they pass**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_tin_core.py -v`
Expected: PASS (all 6 tests, incl. breakline-edge + termination + z-snap).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyvx/_tin.py python/geobrix/test/pyvx/test_tin_core.py
git commit -m "feat(pyvx): Sloan constraint recovery + breakline Z-snap

Force breakline segments as triangle edges via convex-quad edge flips (bounded
budget, raises on non-termination); snap vertex Z onto constraint lines within
snapTolerance. Constrained (no Steiner) Delaunay."
```

## Task 7: `_tin.py` — barycentric interpolation + grid generators

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyvx/_tin.py`
- Test: `python/geobrix/test/pyvx/test_tin_core.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_grid_bbox_centers_column_major():
    cells = list(_tin.grid_bbox(0.0, 0.0, 2.0, 2.0, 2, 2))
    # column-major: i (x) slowest, j (y) fastest; centers at 0.5/1.5
    assert cells == [(0.5, 0.5), (0.5, 1.5), (1.5, 0.5), (1.5, 1.5)]


def test_grid_geom_negative_celly():
    cells = list(_tin.grid_geom(0.0, 10.0, 2, 2, 5.0, -5.0))
    assert cells == [(2.5, 7.5), (2.5, 2.5), (7.5, 7.5), (7.5, 2.5)]


def test_interpolate_known_plane_and_outside_hull():
    # plane z = x + y over unit square
    pts = np.array([[0,0,0],[1,0,1],[1,1,2],[0,1,1]], dtype=float)
    tris = _tin.triangulate(pts, [], 0.0, 0.0)
    z = _tin.interpolate_z(tris, 0.5, 0.5)
    assert abs(z - 1.0) < 1e-9
    assert _tin.interpolate_z(tris, 5.0, 5.0) is None  # outside hull -> None (dropped)
```

- [ ] **Step 2: Run to verify they fail**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_tin_core.py -k "grid or interpolate" -v`
Expected: FAIL (`grid_bbox`/`grid_geom`/`interpolate_z` missing).

- [ ] **Step 3: Implement**

```python
def grid_bbox(xmin, ymin, xmax, ymax, width_px, height_px):
    """Yield (x, y) cell centers, column-major (matches heavy pointGridBBox)."""
    xres = (xmax - xmin) / width_px
    yres = (ymax - ymin) / height_px
    for i in range(int(width_px)):
        for j in range(int(height_px)):
            yield (xmin + (i + 0.5) * xres, ymin + (j + 0.5) * yres)


def grid_geom(origin_x, origin_y, cols, rows, cell_x, cell_y):
    """Yield (x, y) cell centers from origin + cell sizes (matches pointGridOrigin).
    cell_y may be negative (y-down)."""
    for i in range(int(cols)):
        for j in range(int(rows)):
            yield (origin_x + (i + 0.5) * cell_x, origin_y + (j + 0.5) * cell_y)


def interpolate_z(triangles: List[np.ndarray], x: float, y: float):
    """Barycentric Z at (x,y) within the TIN. None if outside all triangles."""
    p = np.array([x, y])
    for t in triangles:
        a, b, c = t[0, :2], t[1, :2], t[2, :2]
        d = _orient2d(a, b, c)
        if d == 0.0:
            continue
        l1 = _orient2d(p, b, c) / d
        l2 = _orient2d(a, p, c) / d
        l3 = 1.0 - l1 - l2
        if l1 >= -1e-12 and l2 >= -1e-12 and l3 >= -1e-12:
            z = l1 * t[0, 2] + l2 * t[1, 2] + l3 * t[2, 2]
            return None if np.isnan(z) else float(z)
    return None
```

- [ ] **Step 4: Run to verify they pass**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/pyvx/test_tin_core.py -v`
Expected: PASS (all `_tin` tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyvx/_tin.py python/geobrix/test/pyvx/test_tin_core.py
git commit -m "feat(pyvx): TIN barycentric interpolation + grid generators

Column-major bbox/geom grid centers (matches heavy pointGrid*); barycentric Z
with outside-hull -> None (heavy's silent drop). Negative cell_y supported."
```

## Task 8: Light `st_triangulate` UDTF + `mode`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyvx/functions.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/pyvx/_serde.py` (add `TRIANGLE_SCHEMA`, `ELEVATION_SCHEMA`)
- Test: `python/geobrix/test/pyvx/test_tin_udtf.py`

- [ ] **Step 1: Add output schemas to `_serde.py`**

```python
TRIANGLE_SCHEMA = StructType([StructField("triangle", BinaryType(), False)])
ELEVATION_SCHEMA = StructType([StructField("elevation_point", BinaryType(), False)])
```

- [ ] **Step 2: Write the failing test**

```python
# test/pyvx/test_tin_udtf.py
import pytest

pytest.importorskip("scipy")
shapely = pytest.importorskip("shapely")
from shapely import to_wkb, wkb  # noqa: E402
from shapely.geometry import Point  # noqa: E402

from databricks.labs.gbx.pyvx import functions as vx


def _pts_wkb(coords):
    return [bytearray(to_wkb(Point(*c))) for c in coords]


def test_st_triangulate_emits_triangles(spark):
    vx.register(spark)
    pts = _pts_wkb([(0, 0, 0), (1, 0, 0), (1, 1, 0), (0, 1, 0)])
    df = spark.createDataFrame([(pts, [], 0.0, 0.0, "NONENCROACHING")],
                               "pts array<binary>, bl array<binary>, mt double, st double, spf string")
    rows = df.selectExpr(
        "t.* FROM {df} JOIN LATERAL gbx_st_triangulate(pts, bl, mt, st, spf, 'constrained') t"
    ) if False else spark.sql(
        "SELECT t.triangle FROM v, LATERAL gbx_st_triangulate(pts, bl, mt, st, spf, 'constrained') t"
    )
    df.createOrReplaceTempView("v")
    rows = spark.sql("SELECT t.triangle FROM v, LATERAL gbx_st_triangulate(pts, bl, mt, st, spf, 'constrained') t").collect()
    assert len(rows) == 2
    assert all(wkb.loads(bytes(r["triangle"])).geom_type == "Polygon" for r in rows)


def test_st_triangulate_conforming_raises(spark):
    vx.register(spark)
    pts = _pts_wkb([(0, 0, 0), (1, 0, 0), (1, 1, 0)])
    df = spark.createDataFrame([(pts, [], 0.0, 0.0, "MIDPOINT")],
                               "pts array<binary>, bl array<binary>, mt double, st double, spf string")
    df.createOrReplaceTempView("v2")
    with pytest.raises(Exception, match="conforming"):
        spark.sql("SELECT t.* FROM v2, LATERAL gbx_st_triangulate(pts, bl, mt, st, spf, 'conforming') t").collect()
```

- [ ] **Step 3: Run to verify it fails**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_tin_udtf.py --log tin-udtf.log`
Expected: FAIL (UDTF not registered).

- [ ] **Step 4: Implement the UDTF + helpers + register**

In `functions.py`, add a shared geometry-array decoder and the UDTF:

```python
def _geoms_from_array(arr):
    """Decode an ARRAY<BINARY|STRING> of geometries via the shared parse_geom
    contract (WKB/EWKB/WKT/EWKT)."""
    from ._geom import parse_geom
    out = []
    for g in arr or []:
        geom = parse_geom(g)
        if geom is not None:
            out.append(geom)
    return out


def _validate_mode(mode):
    m = (mode or "constrained").lower()
    if m == "conforming":
        raise NotImplementedError(
            "mode='conforming' (Steiner-point conforming Delaunay) is heavy-only; "
            "use the heavyweight vectorx tier, or mode='constrained' in light."
        )
    if m != "constrained":
        raise ValueError(f"mode must be 'constrained' or 'conforming'; got {mode!r}")
    return m


def _triangulate_schema():
    from ._serde import TRIANGLE_SCHEMA
    return TRIANGLE_SCHEMA


@udtf(returnType=_triangulate_schema())
class _TriangulateUDTF:
    def eval(self, points, breaklines, merge_tolerance, snap_tolerance, split_point_finder, mode=None):
        _validate_mode(mode)
        from shapely import to_wkb
        from shapely.geometry import Polygon
        import numpy as np
        pt_geoms = _geoms_from_array(points)
        if not pt_geoms:
            return
        coords = np.array([[*(c if len(c) == 3 else (c[0], c[1], 0.0))]
                           for g in pt_geoms for c in g.coords], dtype=float)
        bls = [np.array(g.coords, dtype=float) for g in _geoms_from_array(breaklines)]
        for t in _tin.triangulate(coords, bls, float(merge_tolerance), float(snap_tolerance)):
            yield (to_wkb(Polygon([(p[0], p[1]) for p in t])),)  # 2D triangle WKB
```

Add `from . import _env, _mvt, _legacy, _tin` at the top. In `register(spark)` add:

```python
    spark.udtf.register("gbx_st_triangulate", _TriangulateUDTF)
```

Add the Python Column wrapper (SQL-LATERAL-only, like the pyramid):

```python
def st_triangulate(points_geom, breaklines_geom, merge_tolerance, snap_tolerance,
                   split_point_finder, mode: ColLike = "constrained"):
    """Triangulate mass points (constrained Delaunay). Invoke via SQL LATERAL:
    SELECT t.* FROM <df>, LATERAL gbx_st_triangulate(points, breaklines, mt, st, spf, mode) t
    mode='conforming' is heavy-only."""
    raise NotImplementedError(
        "Light st_triangulate has no Python Column form; invoke the registered UDTF via SQL LATERAL."
    )
```

- [ ] **Step 5: Run to verify it passes**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_tin_udtf.py --log tin-udtf.log`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyvx/functions.py python/geobrix/src/databricks/labs/gbx/pyvx/_serde.py python/geobrix/test/pyvx/test_tin_udtf.py
git commit -m "feat(pyvx): gbx_st_triangulate UDTF (constrained mode)

Streaming UDTF emitting one 2D-WKB triangle per row; mode='constrained'
(default) via scipy+Sloan, mode='conforming' raises (heavy-only). SQL LATERAL."
```

## Task 9: Light `st_interpolateelevationbbox` + `st_interpolateelevationgeom` UDTFs

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyvx/functions.py`
- Test: `python/geobrix/test/pyvx/test_tin_udtf.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_interpolateelevationbbox_emits_points(spark):
    vx.register(spark)
    pts = _pts_wkb([(0, 0, 0), (10, 0, 10), (10, 10, 20), (0, 10, 10)])
    df = spark.createDataFrame(
        [(pts, [], 0.0, 0.0, "NONENCROACHING", 0.0, 0.0, 10.0, 10.0, 5, 5, 0)],
        "pts array<binary>, bl array<binary>, mt double, st double, spf string, "
        "xmin double, ymin double, xmax double, ymax double, w int, h int, srid int")
    df.createOrReplaceTempView("vb")
    rows = spark.sql("SELECT t.elevation_point FROM vb, LATERAL "
                     "gbx_st_interpolateelevationbbox(pts, bl, mt, st, spf, xmin, ymin, xmax, ymax, w, h, srid, 'constrained') t").collect()
    assert len(rows) == 25  # 5x5 grid, all inside hull
    from shapely import wkb
    assert wkb.loads(bytes(rows[0]["elevation_point"])).has_z


def test_interpolateelevationgeom_emits_points(spark):
    vx.register(spark)
    pts = _pts_wkb([(0, 0, 0), (10, 0, 10), (10, 10, 20), (0, 10, 10)])
    origin = bytearray(to_wkb(Point(0.0, 10.0)))
    df = spark.createDataFrame(
        [(pts, [], 0.0, 0.0, "NONENCROACHING", origin, 5, 5, 2.0, -2.0)],
        "pts array<binary>, bl array<binary>, mt double, st double, spf string, "
        "origin binary, cols int, rows int, cx double, cy double")
    df.createOrReplaceTempView("vg")
    rows = spark.sql("SELECT t.elevation_point FROM vg, LATERAL "
                     "gbx_st_interpolateelevationgeom(pts, bl, mt, st, spf, origin, cols, rows, cx, cy, 'constrained') t").collect()
    assert len(rows) == 25
```

- [ ] **Step 2: Run to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_tin_udtf.py --log tin-udtf.log`
Expected: FAIL (UDTFs not registered).

- [ ] **Step 3: Implement both UDTFs + register**

```python
def _elevation_schema():
    from ._serde import ELEVATION_SCHEMA
    return ELEVATION_SCHEMA


def _emit_elevation(points, breaklines, mt, st, spf, mode, cell_iter, srid):
    _validate_mode(mode)
    from shapely import to_wkb
    from shapely.geometry import Point
    from shapely import set_srid
    import numpy as np
    pt_geoms = _geoms_from_array(points)
    if not pt_geoms:
        return
    coords = np.array([[*(c if len(c) == 3 else (c[0], c[1], 0.0))]
                       for g in pt_geoms for c in g.coords], dtype=float)
    bls = [np.array(g.coords, dtype=float) for g in _geoms_from_array(breaklines)]
    tris = _tin.triangulate(coords, bls, float(mt), float(st))
    for (x, y) in cell_iter:
        z = _tin.interpolate_z(tris, x, y)
        if z is None:
            continue
        p = Point(x, y, z)
        if srid:
            p = set_srid(p, int(srid))
        yield (to_wkb(p, output_dimension=3),)  # POINT Z


@udtf(returnType=_elevation_schema())
class _InterpElevBBoxUDTF:
    def eval(self, points, breaklines, merge_tolerance, snap_tolerance, split_point_finder,
             xmin, ymin, xmax, ymax, width_px, height_px, srid, mode=None):
        yield from _emit_elevation(
            points, breaklines, merge_tolerance, snap_tolerance, split_point_finder, mode,
            _tin.grid_bbox(float(xmin), float(ymin), float(xmax), float(ymax), int(width_px), int(height_px)),
            int(srid),
        )


@udtf(returnType=_elevation_schema())
class _InterpElevGeomUDTF:
    def eval(self, points, breaklines, merge_tolerance, snap_tolerance, split_point_finder,
             grid_origin, grid_cols, grid_rows, cell_size_x, cell_size_y, mode=None):
        from shapely import get_srid
        from ._geom import parse_geom
        og = parse_geom(grid_origin)  # WKB/EWKB/WKT/EWKT
        ox, oy = (og.x, og.y) if og is not None else (0.0, 0.0)
        srid = get_srid(og) if og is not None else 0
        yield from _emit_elevation(
            points, breaklines, merge_tolerance, snap_tolerance, split_point_finder, mode,
            _tin.grid_geom(ox, oy, int(grid_cols), int(grid_rows), float(cell_size_x), float(cell_size_y)),
            int(srid),
        )
```

In `register(spark)` add:

```python
    spark.udtf.register("gbx_st_interpolateelevationbbox", _InterpElevBBoxUDTF)
    spark.udtf.register("gbx_st_interpolateelevationgeom", _InterpElevGeomUDTF)
```

Add SQL-LATERAL-only Column wrappers `st_interpolateelevationbbox(...)` and `st_interpolateelevationgeom(...)` raising `NotImplementedError` (same pattern as `st_triangulate`), with the full positional signatures + trailing `mode="constrained"`.

- [ ] **Step 4: Run to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_tin_udtf.py --log tin-udtf.log`
Expected: PASS (4 tests).

- [ ] **Step 5: Run the full pyvx suite + Serverless guard**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/ --log pyvx-all.log`
Expected: PASS (MVT + legacy + TIN units; parity tests skip without JAR). `test_serverless_no_spark_config.py` green.

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyvx/functions.py python/geobrix/test/pyvx/test_tin_udtf.py
git commit -m "feat(pyvx): interpolateelevation bbox + geom UDTFs

Barycentric Z over the constrained TIN at column-major grid centers; POINT Z
WKB out, outside-hull cells dropped. SRID from param (bbox) / origin (geom)."
```

---

# PHASE 3 — Heavy `mode` alignment + cross-tier parity + docs

## Task 10: Heavy `mode` param + constrained path (Scala)

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/vectorx/jts/InterpolateElevation.scala`
- Modify: `src/.../expressions/ST_Triangulate.scala`, `ST_InterpolateElevationBBox.scala`, `ST_InterpolateElevationGeom.scala`
- Test: `src/test/scala/com/databricks/labs/gbx/vectorx/expressions/ST_TriangulateTest.scala` (+ existing TIN suites)

This is a **cross-language port** of the Python Sloan core (Tasks 5–7) to Scala. The Python in `_tin.py` is the algorithm reference; reproduce its `triangulate` + `_recover_constraints` + `_zsnap` semantics in JTS.

- [ ] **Step 1: Add a `mode` to `InterpolateElevation.triangulate` + a constrained path**

Extend the signature with `mode: String = "conforming"` and branch:

```scala
def triangulate(
    multiPoint: Geometry,
    breaklines: Seq[Geometry],
    mergeTolerance: Double,
    snapTolerance: Double,
    splitPointFinder: Option[TriangulationSplitPointTypeEnum.Value] = None,
    mode: String = "constrained"
): Seq[Geometry] = mode.toLowerCase match {
    case "conforming" => triangulateConforming(multiPoint, breaklines, mergeTolerance, snapTolerance, splitPointFinder)
    case "constrained" => triangulateConstrained(multiPoint, breaklines, mergeTolerance, snapTolerance)
    case other => throw new IllegalArgumentException(
        s"mode must be 'constrained' or 'conforming'; got '$other'")
}
```

`triangulateConforming` = today's body (verbatim). `triangulateConstrained` = build the initial Delaunay via `DelaunayTriangulationBuilder` (no constraints), then port `_recover_constraints` (edge-flip recovery on a triangle-index structure) + `_zsnap`. Use JTS only for the initial triangulation, geometry construction, and the `LengthIndexedLine` Z-snap.

- [ ] **Step 2: Add the `mode` arg to the three expressions (builder arity arms)**

For `ST_Triangulate` (currently fixed arity 5), follow the `RST_H3_Tessellate` pattern:

```scala
override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
    case 5 => ST_Triangulate(c(0), c(1), c(2), c(3), c(4), Literal("constrained"))
    case 6 => ST_Triangulate(c(0), c(1), c(2), c(3), c(4), c(5))
    case n => throw new IllegalArgumentException(
        s"gbx_st_triangulate takes 5 or 6 arguments (points, breaklines, mergeTol, snapTol, splitPointFinder, [mode]); got $n")
}
```

Add a `modeExpr: Expression` field to the case class; in `eval`, read it (`modeExpr.eval(...).toString`) and pass to `InterpolateElevation.triangulate(..., mode = modeStr)`. Do the same for `ST_InterpolateElevationBBox` (12→12/13) and `ST_InterpolateElevationGeom` (10→10/11).

- [ ] **Step 3: Write + run the Scala tests**

Add `ST_TriangulateTest`: same point set, `mode="constrained"` vs `"conforming"` both produce valid triangle covers; with a breakline, both contain the constraint segment as an edge; unknown mode throws. Run:

`bash scripts/commands/gbx-test-scala.sh --suites 'com.databricks.labs.gbx.vectorx.expressions.ST_TriangulateTest,com.databricks.labs.gbx.vectorx.expressions.ST_InterpolateElevationBBoxTest,com.databricks.labs.gbx.vectorx.expressions.ST_InterpolateElevationGeomTest' --log tin-scala.log`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX .git/objects
git add src/main/scala/com/databricks/labs/gbx/vectorx/ src/test/scala/com/databricks/labs/gbx/vectorx/
git commit -m "feat(vectorx): constrained/conforming mode on TIN functions

Add a trailing mode arg (default 'constrained') to st_triangulate +
interpolateelevation{bbox,geom}; constrained (no Steiner) ports the pyvx
Sloan recovery to JTS, conforming keeps ConformingDelaunayTriangulator."
```

## Task 11: Cross-tier TIN parity tests

**Files:**
- Test: `python/geobrix/test/pyvx/test_parity_tin.py`

- [ ] **Step 1: Write the JAR-gated parity tests** (reuse the `spark_with_jar` fixture pattern).

```python
def test_triangulate_parity_no_breaklines(spark_with_jar):
    # Delaunay ~unique: light constrained == heavy constrained triangle set (within tol)
    ...
    # assert same number of triangles and matching sorted centroid coordinates within 1e-6

def test_interpolate_parity_surface_closeness(spark_with_jar):
    # same points + bbox grid, mode='constrained' both tiers
    # assert per-cell interpolated Z within 1e-6 (no breaklines)

def test_triangulate_breakline_edges_present_both(spark_with_jar):
    # with a breakline: assert the constraint segment is a triangle edge in BOTH tiers
    # (NOT triangle-identity)

def test_conforming_is_heavy_only(spark_with_jar):
    # heavy mode='conforming' returns rows; light mode='conforming' raises
```

Assertion bar per the spec: **no-breakline → near-exact** (triangle set / surface within `1e-6`); **with-breakline → surface-closeness + constraint-edges-present**, not triangle-identity.

- [ ] **Step 2: Run in Docker (JAR present)**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_parity_tin.py --with-integration --log parity-tin.log`
Expected: PASS (skips without JAR).

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/pyvx/test_parity_tin.py
git commit -m "test(pyvx): light-vs-heavy TIN parity (constrained mode)

No-breakline near-exact surface/triangle parity; with-breakline asserts
constraint edges present + surface closeness, not triangle identity; conforming
is heavy-only."
```

## Task 12: Bindings, function-info, docs

**Files:**
- Modify: `docs/tests/python/api/vectorx_functions_sql.py` (add `mode` to the 3 TIN examples)
- Modify: `docs/docs/api/vectorx-functions.mdx`
- Modify: `python/geobrix/src/databricks/labs/gbx/pyvx/functions.py` (ensure all 4 wrappers exist for import-parity)

- [ ] **Step 1: Update function-info examples** to show the `mode` arg, e.g. `gbx_st_triangulate(masspoints, breaklines, 0.01, 0.01, 'NONENCROACHING', 'constrained')`. Regenerate: `bash scripts/commands/gbx-docs-function-info.sh` (or `gbx:test:function-info`). Keep output tables canonically aligned (D5).

- [ ] **Step 2: Update `vectorx-functions.mdx`** — add light/heavy tabs for the 4 functions, the `mode` param, and a "constrained vs conforming" + breakline divergence explainer (defensible-divergence framing, like the H3-Java 3.7.0 note). Legacy section: `st_legacyaswkb` preserves Z + holes; SRID applied separately at ingestion; M out of scope. No "wave N" / internal vocabulary.

- [ ] **Step 3: Geometry-input consistency audit**

Verify every `gbx_st_*` geom-accepting function shares the `parse_geom` contract (WKB/EWKB/WKT/EWKT): the TIN `points`/`breaklines`/`grid_origin` (done in T8/T9) and the existing `st_asmvt` geom input. If heavy `gbx_st_asmvt` accepts WKT/EWKT (not just WKB), update `_asmvt_udf` in `functions.py` to route its geom through `_geom.parse_geom` (decode → re-`to_wkb` for mapbox-vector-tile) and add a test that `st_asmvt` accepts a WKT geom. Document the accepted-encodings contract once on the VectorX page.

- [ ] **Step 4: Build docs + checks**

Run: `cd docs && npm run build` → SUCCESS. `grep -rn -iE "wave [0-9]+" docs/docs/` → empty. `bash scripts/commands/gbx-test-bindings.sh --log bindings-tin.log` → PASS.

- [ ] **Step 5: Full Docker verification**

Run the pyvx suite + the TIN/legacy Scala suites in Docker (mirror the MVT verification): light units green, parity green (JAR staged), Scala green.

- [ ] **Step 6: Commit + update the PR checklist**

```bash
chmod -R u+rwX .git/objects
git add docs/
git commit -m "docs(pyvx): TIN + legacy functions, modes, divergence explainer

Light/heavy tabs + mode param for the 4 VectorX functions; constrained-vs-
conforming + breakline divergence note; legacy Z+holes/SRID framing. Update
function-info examples."
```

Then check the 4 boxes in PR #38's description (legacy + the 3 TIN functions) and mark the PR ready for review.

---

## Self-Review

**Spec coverage:** ✅ Legacy Z+holes (T1–T4, both tiers); ✅ TIN engine scipy+Sloan (T5–T7); ✅ 3 UDTFs (T8–T9); ✅ `mode` both tiers, conforming heavy-only/light-raises (T8, T10); ✅ parity posture no-breakline-exact / breakline-surface (T11); ✅ scipy dep (already present — noted); ✅ Serverless-safe (T9 step 5 guard); ✅ docs + divergence explainer (T12); ✅ phasing legacy→TIN→heavy.

**Placeholder scan:** The only soft spots are the deliberately-staged `_recover_constraints`/`_zsnap` stubs in T5 (replaced with real code in T6 — an intentional TDD sequencing, not a shipped placeholder) and the `_zsnap` accumulation note in T6 (flagged with the exact correction). The Scala constrained port in T10 references the Python core as the algorithm spec (a cross-language port, not a placeholder).

**Type consistency:** schema names (`TRIANGLE_SCHEMA`, `ELEVATION_SCHEMA`, `TILE_SCHEMA`), function names (`triangulate`, `interpolate_z`, `grid_bbox`, `grid_geom`, `legacy_to_geom`, `legacy_to_wkb`, `_validate_mode`, `_geoms_from_array`), and SQL names (`gbx_st_*`) are consistent across tasks.
