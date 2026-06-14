# rst_h3_tessellate covering/centroid Modes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add a `mode` parameter (`"covering"` default / `"centroid"`) to `rst_h3_tessellate` in BOTH tiers, aligning light and heavy on the same per-mode semantics: `covering` = the true overlapping cell set, each clipped to its hexagon (all-touched); `centroid` = pixel-centroid single-assignment partition.

**Architecture:** Light (`pyrx`) uses h3-py 4.4.2 `polygon_to_cells_experimental(contain='overlap')` for covering and per-pixel `latlng_to_cell` for centroid. Heavy (Scala, H3-Java **3.7.0**, no v4 covering primitive) hand-rolls the covering set via a **JTS hexagon∩bbox overlap test** (replacing the nodata keep-test, which is what over-includes a disjoint fringe today) and per-pixel `pointToCellID` for centroid. Parity is enforced by per-mode light-vs-heavy tests, not an identical API call. Backward-compatible: SQL arity 2 (default `covering`) and 3.

**Tech Stack:** Python 3.12 / PySpark UDTF / h3-py 4.4.2 / shapely / rasterio (light); Scala 2.13 / Spark 4 / H3-Java 3.7.0 / JTS / GDAL OGR (heavy), in the `geobrix-dev` Docker container.

**Spec:** `docs/superpowers/specs/2026-06-13-h3-raster-tessellation-modes-design.md` (§6–10).

---

## File Structure

**Light (modify):**
- `python/geobrix/src/databricks/labs/gbx/pyrx/core/tessellate.py` — the H3 cell-selection + chip core (`iter_tessellate_h3` and helpers). Add `mode`; covering→`contain='overlap'`; centroid→per-pixel assignment; fix `all_touched` asymmetry.
- `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` — `_RstH3TessellateUDTF.eval` (+`mode`), the `rst_h3_tessellate` wrapper (`mode: ColLike = "covering"`).
- Tests: `python/geobrix/test/pyrx/` (core + UDTF), and a JAR-gated cross-tier parity test under `python/geobrix/test/pyvx/` or `test/pyrx/`.

**Heavy (modify):**
- `src/main/scala/com/databricks/labs/gbx/rasterx/.../RST_H3_Tessellate.scala` — add `modeExpr`; `FunctionBuilder` arity 2+3.
- `.../RasterTessellate.scala` (`tessellateH3Iter`) — covering (JTS overlap keep-test) + centroid (per-pixel) paths.
- `.../H3.scala` — `getBufferRadius` default arm; any covering helper.
- `.../functions.scala` — Scala API overloads with `mode`.
- `python/geobrix/src/databricks/labs/gbx/rasterx/functions.py` — heavy binding `mode="covering"`.
- Tests: `src/test/scala/com/databricks/labs/gbx/rasterx/.../` (RST_H3_Tessellate / RasterTessellate).

**Docs (create/modify):**
- `docs/docs/` — new H3 explainer page + sidebar wiring.
- `function-info.json` usage example for `gbx_rst_h3_tessellate` (mode arg).

> Implementer note: exact Scala paths/line numbers — locate via `grep -rn "RST_H3_Tessellate\|tessellateH3Iter\|getBufferRadius" src/main/scala`. The behavioral summary in the spec's §3 cites the current logic.

---

## Task 1: Light — `centroid` mode in the core (pixel-centroid partition)

**Files:** Modify `pyrx/core/tessellate.py`; Test `python/geobrix/test/pyrx/test_core_tessellate_modes.py` (create).

- [ ] **Step 1: Failing test** (Spark-free core, real raster):

```python
# python/geobrix/test/pyrx/test_core_tessellate_modes.py
import numpy as np
from rasterio.io import MemoryFile
from databricks.labs.gbx.pyrx.core import tessellate as T
from databricks.labs.gbx.pyrx import _serde


def _tile_4326(size=64, res_deg=0.01, origin=(-0.1, 51.5)):
    data = np.arange(size * size, dtype="float32").reshape(size, size)
    prof = dict(driver="GTiff", height=size, width=size, count=1, dtype="float32",
                crs="EPSG:4326",
                transform=__import__("rasterio").transform.from_origin(origin[0], origin[1], res_deg, res_deg))
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


def test_centroid_mode_partitions_pixels():
    """centroid: every valid pixel assigned to exactly one cell; union == all pixels; no overlap."""
    tile = _tile_4326()
    cells = list(T.iter_tessellate_h3(_serde.open_tile(tile), resolution=9, mode="centroid"))
    # collect the set of valid pixels covered by each cell's chip
    seen = []
    for cell in cells:
        with _serde.open_tile(cell["raster"]) as ds:
            arr = ds.read(1, masked=True)
            seen.append(int((~arr.mask).sum()))
    total_valid = 64 * 64
    assert sum(seen) == total_valid, "centroid chips must partition all pixels exactly once"
```

- [ ] **Step 2: Run, verify FAIL** — `PYSPARK_PYTHON=.venv-pyrx/bin/python .venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_core_tessellate_modes.py::test_centroid_mode_partitions_pixels -v` → FAIL (`iter_tessellate_h3` has no `mode`).

- [ ] **Step 3: Implement centroid path** — read the current `iter_tessellate_h3` in `tessellate.py`; add a `mode: str = "covering"` param with validation (`{"covering","centroid"}` → `ValueError` listing valid values). For `mode == "centroid"`: reproject the raster to 4326 (or assert 4326), then for each valid pixel compute its lon/lat centroid → `h3.latlng_to_cell(lat, lon, resolution)`; group pixels by cell; for each cell emit a chip = a raster with only that cell's pixels (others nodata) — reuse the existing tile-build/serde helpers. Do NOT build the covering set in this path (cells emerge from pixels). Keep the existing covering behavior under `mode == "covering"` for now (Task 2 replaces it).

- [ ] **Step 4: Run, verify PASS** (same command) → PASS.

- [ ] **Step 5: Commit**
```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/tessellate.py python/geobrix/test/pyrx/test_core_tessellate_modes.py
git commit -m "feat(pyrx): h3 tessellate centroid mode (pixel-centroid partition)"
```

## Task 2: Light — `covering` mode via `contain='overlap'` (replace ring+prune)

**Files:** Modify `pyrx/core/tessellate.py`; Test same file as Task 1.

- [ ] **Step 1: Failing test** — covering = the true overlapping set + all-touched chips:

```python
def test_covering_mode_is_overlap_set():
    import h3
    from shapely.geometry import box
    tile = _tile_4326()
    with _serde.open_tile(tile) as ds:
        cells = {c["index"] if "index" in c else c["cellid"] for c in
                 __import__("databricks.labs.gbx.pyrx.core.tessellate", fromlist=["iter_tessellate_h3"]).iter_tessellate_h3(ds, resolution=9, mode="covering")}
    # oracle: h3-py overlap containment over the same 4326 bbox
    shp = h3.geo_to_h3shape(box(-0.1, 51.5 - 0.64, -0.1 + 0.64, 51.5).__geo_interface__)
    oracle = set(h3.polygon_to_cells_experimental(shp, 9, contain="overlap"))
    assert cells == oracle
```

(Adjust the chip's cell-id field name to match the tile schema; adjust the bbox to the test raster's actual 4326 extent.)

- [ ] **Step 2: Run, verify FAIL** (current covering = seed+grid_disk+prune, not exactly the overlap set).

- [ ] **Step 3: Implement** — in the `mode == "covering"` path, replace the `h3shape_to_cells` seed + `grid_disk(1)` ring + prune with a single `h3.polygon_to_cells_experimental(bbox_shape_4326, resolution, contain="overlap")`. Keep the hexagon clip → chip, and make the clip use **`all_touched=True`** (fixing the prune-vs-clip asymmetry: the old prune used True, the clip used False — both must be True now).

- [ ] **Step 4: Run, verify PASS.**

- [ ] **Step 5: Commit**
```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/tessellate.py python/geobrix/test/pyrx/test_core_tessellate_modes.py
git commit -m "feat(pyrx): h3 tessellate covering mode via contain=overlap (+all_touched fix)"
```

## Task 3: Light — wire `mode` through the UDTF + wrapper + SQL

**Files:** Modify `pyrx/functions.py`; Test `python/geobrix/test/pyrx/test_functions_spark.py` (extend).

- [ ] **Step 1: Failing test** — LATERAL call with + without mode:

```python
def test_h3_tessellate_mode_sql(spark):
    from databricks.labs.gbx.pyrx import functions as rx
    rx.register(spark)
    # build a tiny 4326 tile view (reuse existing tile-fixture helper in this test module)
    df = _h3_tile_df(spark)  # existing helper that yields a (tile) column
    df.createOrReplaceTempView("ras")
    n_default = spark.sql("SELECT t.* FROM ras, LATERAL gbx_rst_h3_tessellate(tile, 9) t").count()
    n_cover = spark.sql("SELECT t.* FROM ras, LATERAL gbx_rst_h3_tessellate(tile, 9, 'covering') t").count()
    n_centroid = spark.sql("SELECT t.* FROM ras, LATERAL gbx_rst_h3_tessellate(tile, 9, 'centroid') t").count()
    assert n_default == n_cover and n_cover > 0 and n_centroid > 0
    import pytest
    with pytest.raises(Exception):
        spark.sql("SELECT t.* FROM ras, LATERAL gbx_rst_h3_tessellate(tile, 9, 'bogus') t").count()
```

- [ ] **Step 2: Run, verify FAIL** (`eval` takes only `(tile, resolution)`).

- [ ] **Step 3: Implement** — `_RstH3TessellateUDTF.eval(self, tile, resolution, mode=None)`: default `"covering"` when `mode is None`; validate `{"covering","centroid"}` (ValueError); pass to `iter_tessellate_h3(ds, resolution, mode=...)`. Update the `rst_h3_tessellate` wrapper signature to `(tile, resolution, mode: ColLike = "covering")` (keep the NotImplementedError-LATERAL-guidance body; mention `mode` in the docstring). The UDTF registration is unchanged (positional args).

- [ ] **Step 4: Run, verify PASS**; then the full pyrx suite + Serverless guard:
`PYSPARK_PYTHON=.venv-pyrx/bin/python PYSPARK_DRIVER_PYTHON=.venv-pyrx/bin/python .venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/ -v` (green; note skips).

- [ ] **Step 5: Commit**
```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py python/geobrix/test/pyrx/test_functions_spark.py
git commit -m "feat(pyrx): rst_h3_tessellate mode param (covering default / centroid)"
```

## Task 4: Heavy — `covering` mode: JTS overlap keep-test (fix disjoint fringe)

**Files (Docker):** Modify `RasterTessellate.scala`, `H3.scala`; Test `src/test/scala/.../RST_H3_TessellateTest.scala` (extend or create).

- [ ] **Step 1: Failing Scala test** — covering produces no disjoint cells (every emitted cell's hexagon intersects the tile bbox):

```scala
test("h3 tessellate covering emits only cells whose hexagon overlaps the tile") {
    rasterx.functions.register(spark)
    import rasterx.functions._
    val df = h3TileDf(spark)  // small 4326 raster tile (reuse existing test fixture)
    val cells = df.select(rst_h3_tessellate(col("tile"), lit(9))).collect()  // default covering
    // For each emitted cell, assert its H3 hexagon (JTS) intersects the tile bbox geometry.
    assert(cells.nonEmpty)
    assert(TessTestUtil.allHexagonsOverlapBbox(cells, df))  // helper: no disjoint cell
}
```

Add a `TessTestUtil.allHexagonsOverlapBbox` helper (build each cell's hexagon via `H3.cellIdToGeometry`, the tile bbox via `BoundingBox.bbox`, assert JTS `intersects`).

- [ ] **Step 2: Run, verify FAIL** (current heavy over-includes the disjoint fringe):
`bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.*.RST_H3_TessellateTest' --log h3tess.log` → FAIL.

- [ ] **Step 3: Implement** — in `RasterTessellate.tessellateH3Iter`, for the covering path keep the polyfill+buffer candidate generation but **replace the per-cell nodata keep-test (`RasterAccessors.isEmpty`) with a JTS overlap test**: keep the cell iff `cellHexagon.intersects(bboxGeom4326)` (the hexagon, not its bbox). Give `H3.getBufferRadius` a default match arm (return 0 or a safe default) so non-Polygon inputs don't `MatchError`. (The chip clip via `ClipToGeom` with `CUTLINE_ALL_TOUCHED=TRUE` is unchanged.)

- [ ] **Step 4: Run, verify PASS** (covering test green; existing tessellate tests still green).

- [ ] **Step 5: Commit**
```bash
chmod -R u+rwX .git/objects
git add src/main/scala/com/databricks/labs/gbx/rasterx/ src/test/scala/com/databricks/labs/gbx/rasterx/
git commit -m "fix(rasterx): h3 tessellate covering uses JTS hexagon-overlap keep-test"
```

## Task 5: Heavy — `centroid` mode + `mode` param (arity 2+3) + bindings

**Files (Docker):** `RST_H3_Tessellate.scala`, `RasterTessellate.scala`, `functions.scala`, `rasterx/functions.py`; Test `RST_H3_TessellateTest.scala`.

- [ ] **Step 1: Failing Scala test** — centroid partitions pixels + mode arity:

```scala
test("h3 tessellate centroid partitions pixels; mode arity 2 and 3 both work") {
    rasterx.functions.register(spark)
    import rasterx.functions._
    val df = h3TileDf(spark)
    val cover = df.select(rst_h3_tessellate(col("tile"), lit(9), lit("covering"))).count()
    val cent  = df.select(rst_h3_tessellate(col("tile"), lit(9), lit("centroid"))).count()
    val dflt  = df.select(rst_h3_tessellate(col("tile"), lit(9))).count()  // arity-2 == covering
    assert(dflt == cover && cover > 0 && cent > 0)
    assert(TessTestUtil.centroidPartitionsAllPixels(df, 9))  // helper: each valid pixel in exactly one chip
}
```

- [ ] **Step 2: Run, verify FAIL** (no `mode`; arity strict-2).

- [ ] **Step 3: Implement** — add `modeExpr` to the `RST_H3_Tessellate` case class; `builder()` arity **2** (`Literal("covering")`) and **3** (`c(2)`), else `IllegalArgumentException` ("takes 2 or 3 arguments…"); validate the mode string (`require(Set("covering","centroid").contains(...))`). In `RasterTessellate.tessellateH3Iter`, add the centroid path: per valid pixel → `H3.pointToCellID(lon, lat, res)`; group pixels by cell; emit one chip per cell with only its pixels. Add `functions.scala` overloads (`rst_h3_tessellate(tile, res, mode: String)` + `(tile, Int res, mode: String = "covering")`, mirroring the existing `resolution: Int` overload). Update the heavy Python binding `rasterx/functions.py` `rst_h3_tessellate(tile, resolution, mode: ColLike = "covering")` (mirror `rst_resample`'s string-default handling).

- [ ] **Step 4: Run, verify PASS** — `bash scripts/commands/gbx-test-scala.sh --suite '...RST_H3_TessellateTest' --log h3tess.log` green; `bash scripts/commands/gbx-lint-scalastyle.sh` 0 errors.

- [ ] **Step 5: Commit**
```bash
git add src/main/scala/com/databricks/labs/gbx/ python/geobrix/src/databricks/labs/gbx/rasterx/functions.py src/test/scala/com/databricks/labs/gbx/rasterx/
git commit -m "feat(rasterx): rst_h3_tessellate centroid mode + mode param (arity 2+3)"
```

## Task 6: Cross-tier parity tests (both modes, border tile)

**Files:** `python/geobrix/test/pyvx/test_parity_h3_tessellate.py` (create) — JAR-gated like `test_parity_mvt.py`.

- [ ] **Step 1: Write the parity test** — for a small 4326 raster containing a tile border, for EACH mode assert light and heavy produce the **same cell set** and matching per-cell pixel counts:

```python
import os, pytest
mvt = None  # not needed
pytestmark = pytest.mark.skipif(not os.environ.get("GBX_HEAVY_JAR"),
                                reason="needs heavyweight JAR; run in geobrix-dev Docker")

@pytest.mark.parametrize("mode", ["covering", "centroid"])
def test_light_vs_heavy_h3_tessellate(spark_with_jar, mode):
    from databricks.labs.gbx.pyrx import functions as rx
    from databricks.labs.gbx.rasterx import functions as hx
    rx.register(spark_with_jar); hx.register(spark_with_jar)
    df = _border_tile_df(spark_with_jar); df.createOrReplaceTempView("ras")
    light = {(r["index"]) for r in spark_with_jar.sql(
        f"SELECT t.index FROM ras, LATERAL gbx_rst_h3_tessellate(tile, 9, '{mode}') t").collect()}
    heavy = {r[0] for r in df.select(hx.rst_h3_tessellate(__import__("pyspark.sql.functions", fromlist=['col']).col("tile"), 9, mode)).collect()}
    # heavy returns a tile struct; extract its cell-id field. Adjust accessors to the actual schema.
    assert light == heavy and len(light) > 0
```

(Reuse `test_parity_mvt.py`'s `spark_with_jar` fixture pattern; adjust the cell-id extraction to the tile struct's field. For `centroid`, also assert the partition property holds in both tiers.)

- [ ] **Step 2: Run in Docker (JAR staged)** — `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyvx/test_parity_h3_tessellate.py --log h3-parity.log` → both modes PASS (rebuild the lib JAR first so it has Tasks 4–5).

- [ ] **Step 3: Commit**
```bash
git add python/geobrix/test/pyvx/test_parity_h3_tessellate.py
git commit -m "test: light-vs-heavy rst_h3_tessellate parity (covering + centroid)"
```

## Task 7: Bench — per-mode h3_tessellate leg

**Files:** Modify `python/geobrix/src/databricks/labs/gbx/bench/readers.py` (`run_fanout_udtf` / `_fanout_spec`) + `_CELL_FANOUT` in `cluster.py`.

- [ ] **Step 1: Update the h3_tessellate fan-out leg** to pass `mode` (default `covering`) on both tiers' SQL and compare per-mode; the heavy generator call is `LATERAL VIEW gbx_rst_h3_tessellate(tile, res, 'covering') t AS tile` (no explode). Optionally add a `centroid` variant.
- [ ] **Step 2: Local smoke** (light path) — `run_fanout_udtf(..., api="lightweight", fn="rst_h3_tessellate")` returns ok rows for both modes.
- [ ] **Step 3: Commit** (`feat(bench): h3_tessellate fan-out leg is mode-aware`). (The cluster re-bench runs when a cluster is next up — coordinate with the operator; the parity is already proven by Task 6.)

## Task 8: Docs — H3 explainer page + function-info

**Files:** new `docs/docs/.../h3.mdx` (or per the existing IA), `docs/sidebars.js`, `function-info.json` (or the doc-test source that generates it).

- [ ] **Step 1: Write the H3 explainer page** per spec §9: lineage (Mosaic → `h3_coverash3`/`h3_tessellateaswkb`); the two tessellation modes (`covering` full-coverage/shareable vs `centroid` pixel-centroid partition/de-duped) with a "when to use which" + a border-behavior visual; relationship to `rst_h3_rastertogrid*`; CRS expectations; cross-tier parity (with the H3-Java-3.7.0 mechanism note framed as a defensible divergence). Doc-voice rules (no marketing, no Mosaic-as-rationale, no "wave N"). Wire into `docs/sidebars.js`.
- [ ] **Step 2: Update the `gbx_rst_h3_tessellate` usage example** (function-info source / doc-test) to show the `mode` arg.
- [ ] **Step 3: Build + checks** — `cd docs && npm run build` SUCCESS; `grep -rn -iE "wave [0-9]+" docs/docs/` empty.
- [ ] **Step 4: Commit** (`docs: H3 tessellation explainer page + mode usage`).

---

## Self-Review (against spec §6–9)

- §6.1 mode param (string, default covering, arity 2+3, validation) → Tasks 3, 5. ✓
- §6.2 covering (true overlapping set; light `contain='overlap'`, heavy JTS overlap test; all_touched chip) → Tasks 2, 4. ✓
- §6.3 centroid (pixel-centroid partition, both tiers) → Tasks 1, 5. ✓
- §6.4 CRS internal reproject → preserved (Tasks 1–2, 4–5 keep current reproject). ✓
- §6.5 parity by definition + tests → Task 6. ✓
- §7 impl scope (heavy + light + bindings + function-info) → Tasks 1–5, 8. ✓
- §8 testing (per-mode parity, covering-no-disjoint, centroid-partition) → Tasks 4, 5, 6. ✓
- §9 explainer page → Task 8. ✓

**Known soft spots (acceptable, test-gated):** the heavy Scala steps reference the current code by file/grep + give the target behavior + a Scala test as the gate (verbatim line numbers shift); the cell-id field accessor in the parity/heavy tests must be matched to the actual tile struct schema by the implementer. JAR rebuild precedes Task 6; the cluster re-bench (Task 7) is deferred to the next cluster session (parity already proven locally).
