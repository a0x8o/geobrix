# Design: Wire up and test `gbx_rst_dtmfromgeoms`

**Date:** 2026-05-28
**Status:** Approved (design); implementation pending
**Package:** RasterX (`com.databricks.labs.gbx.rasterx`)

## Problem

`gbx_rst_dtmfromgeoms` is ported from DBLabs Mosaic (`rst_dtmfromgeoms`). It builds a
Digital Terrain Model raster by interpolating elevation (Z) from Z-valued point
geometries and optional breakline geometries using a constrained Delaunay
triangulation (TIN) with barycentric Z-interpolation, bounded by the convex hull.

The implementation exists (`RST_DTMFromGeoms.scala` + `InterpolateElevation.scala`) but
is **not production-wired**:

- `rd.register(RST_DTMFromGeoms)` is commented out in `rasterx/functions.scala` (line ~112).
- Both files are excluded from scoverage (`pom.xml` lines 466, 508).
- `eval` uses the **wrong** `RST_ErrorHandler.safeEval` overload — the tile-array form
  `safeEval(fn, rows: ArrayData, rasterType)`, passing `pointsArray` (geometries) as if
  it were an array of raster tiles. On any error it would try to read point geometries as
  tile structs. This is the `// TODO: this will need fixing` at ~line 109.
- `InterpolateElevation.pointGrid(origin, gridWidthX, gridWidthY, gridSizeX, gridSizeY)` is
  called with cell-size and cell-count arguments in the wrong positions relative to the
  `pointGrid(origin, xCells, yCells, xSize, ySize)` signature — a latent arg-order bug.
- `splitPointFinder` is accepted and parsed (`TriangulationSplitPointTypeEnum`) but never
  passed to the triangulator — a dead parameter.
- There are no tests, no `registered_functions.txt` entry, and no `function-info.json` entry.

**Coverage verdict (why we keep it, not remove it):** the gap is genuine. The closest
registered function, `gbx_rst_gridfrompoints(_agg)`, performs Inverse-Distance-Weighted
(IDW) interpolation — a non-local method with no breakline support and no convex-hull
bounding. TIN/Delaunay surface interpolation with breakline constraints is a distinct,
standard terrain-modeling capability that nothing else in RasterX provides.

## Goals

1. Make `gbx_rst_dtmfromgeoms` a registered, working, tested RasterX function.
2. Ship a streaming aggregator counterpart `gbx_rst_dtmfromgeoms_agg`, mirroring the
   `rst_gridfrompoints` / `rst_gridfrompoints_agg` pairing.
3. Modernize the public signature to the RasterX house style (consistent with
   `rst_gridfrompoints` / `rst_rasterize`), so it composes pixel-for-pixel with the other
   vector→raster functions.
4. Both functions pass the `binding-parity` QC check (Scala name literal + Python binding +
   function-info entry present for each).

## Non-goals (YAGNI)

- No resolution-argument variant and no `grid_mode` discriminator — the documented recipe
  covers resolution-based usage (see API docs below).
- `splitPointFinder` is **not** reinstated.
- No changes to other RasterX functions.
- The aggregator streams **points only**; breaklines are a per-group constant array param
  (not streamed). Rationale below.

## Design decisions (and rationale)

- **Modernize the signature** rather than preserve Mosaic's exactly. RasterX is a
  *successor* to Mosaic raster (only GridX/BNG are mandated to preserve baseline behavior),
  and the project is pre-1.0 beta that breaks APIs to stabilize (CLAUDE.md). The function
  was never registered here, so there are no existing call sites to migrate.
- **Scheme A — bbox + pixel-count** for the grid spec (`xmin, ymin, xmax, ymax, width_px,
  height_px, srid`), matching `rst_gridfrompoints` and `rst_rasterize`. This maximizes
  cross-function consistency and gives free pixel-aligned composability (produce IDW and TIN
  over an identical grid and overlay/diff them). It also avoids the float-resolution rounding
  ambiguity of a resolution-first form. The resolution ergonomic ("I want 10 m cells") is
  recovered via documentation (a one-line conversion), not a second API surface.
- **Provide a streaming aggregator (`_agg`).** Elevation/survey/LiDAR point data lives as one
  row per point. The non-agg form needs all points pre-collected into an `ARRAY` column in a
  single row; the aggregator instead accumulates points directly in a `TypedImperativeAggregate`
  buffer with Spark partial aggregation (map-side `update` + `merge`), avoiding the giant
  `collect_list` array-column materialization. The triangulation itself still holds all points
  in memory at finalization, so the win is the delivery/collection path and `GROUP BY`
  ergonomics, not the core algorithm footprint.
- **Aggregator streams points only; breaklines are a per-group constant array (Option 1).** A
  UDAF aggregates one value per row; breaklines are inherently low-cardinality (a handful of
  ridgelines/rivers per region) while points are high-cardinality (the thing worth streaming).
  Passing breaklines as a group-stable constant array — evaluated against `InternalRow.empty`
  in `eval()`, exactly as `RST_GridFromPointsAgg` handles `xmin`/`srid`/etc. — keeps the input
  shape clean and the buffer small. The rejected alternative (a discriminator column so points
  and lines both stream) forces users to `UNION` mixed geometry types with a boolean flag and
  buys nothing given breakline cardinality.
- **Shared `execute` compute path.** Refactor the triangulate→interpolate→rasterize pipeline
  into a pure `RST_DTMFromGeoms.execute(pointWkbs, breaklineWkbs, mergeTol, snapTol, xmin, ymin,
  xmax, ymax, widthPx, heightPx, srid, noData): InternalRow`. The non-agg `eval` parses its
  arrays and calls it; the aggregator's `eval()` reads the constant breaklines + params and
  calls it with the buffer's accumulated points. Mirrors `RST_GridFromPoints.execute` shared by
  both grid functions.

## 1. Public API

```
gbx_rst_dtmfromgeoms(
  points_geom     ARRAY<BINARY|STRING>,  -- Z-valued points (WKB or WKT)
  breaklines_geom ARRAY<BINARY|STRING>,  -- breakline LineStrings; pass empty array for none
  merge_tolerance DOUBLE,                -- Delaunay segment-merge tolerance
  snap_tolerance  DOUBLE,                -- vertex-to-breakline snap tolerance
  xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE,
  width_px INT, height_px INT,
  srid INT,
  no_data DOUBLE                         -- optional, default -9999.0
) -> tile   -- single-band Float64 GTiff, width_px x height_px
```

- Output is a tile row `(index_id LONG, raster BINARY, metadata MAP<STRING,STRING>)` holding
  a single-band Float64 GTiff of exactly `width_px x height_px`.
- Builder accepts **11 args** (`no_data` defaulted to `-9999.0`) and **12 args** (explicit
  `no_data`), mirroring `RST_GridFromPoints`' arg-count flexibility.
- **Resolution recipe (documented).** To get N-unit cells over a known extent:
  `width_px = round((xmax - xmin) / N)`, `height_px = round((ymax - ymin) / N)`.
  Example: a 1 km² extent in EPSG:27700 at 10 m cells ⇒ `width_px = height_px = 100`:
  `gbx_rst_dtmfromgeoms(pts, lines, 0.0, 0.01, 530000, 180000, 531000, 181000, 100, 100, 27700)`.
  This recipe MUST appear in the function description and the SQL doc example.

### Aggregator form

```
gbx_rst_dtmfromgeoms_agg(
  point_geom      BINARY|STRING,         -- AGGREGATED per row: one Z-valued point (WKB or WKT)
  breaklines_geom ARRAY<BINARY|STRING>,  -- per-group CONSTANT array of breakline LineStrings
  merge_tolerance DOUBLE,
  snap_tolerance  DOUBLE,
  xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE,
  width_px INT, height_px INT,
  srid INT,
  no_data DOUBLE                         -- optional, default -9999.0
) -> tile   -- single-band Float64 GTiff, width_px × height_px
```

- `point_geom` is the only aggregated (per-row) input; every other argument is a per-group
  constant (same value for all rows in the group; read once in `eval()`).
- Typical usage: `GROUP BY <extent_key>`, pass the per-row point column and per-group literal
  extent/tolerance/breakline params.
- Produces the **same** DTM as the non-agg form over the same grid (verified by test).
- Builder accepts 11 args (`no_data` defaulted) and 12 args (explicit).

## 2. Internals & bug fixes

- **Error handling (TODO fix):** use `RST_ErrorHandler.safeEval(() => {...}, null, BinaryType,
  conf)` — the no-raster-input overload used by `RST_GridFromPoints`. Wrap with
  `Option(...).map(_.asInstanceOf[InternalRow]).orNull` per the sibling pattern.
- **PySpark support:** provide both an `Int`-args and a `Long`-args `eval` entry point (PySpark
  passes Python ints as `Long`), each delegating to a shared private `doInvoke`. Replace the
  current single packed-tuple `eval`.
- **Grid generation:** refactor `InterpolateElevation.pointGrid` (or add a bbox variant) to take
  `(xmin, ymin, xmax, ymax, width_px, height_px, srid)` and emit cell-center points at
  `x = xmin + (i + 0.5) * x_res`, `y = ymin + (j + 0.5) * y_res`, where
  `x_res = (xmax - xmin) / width_px`, `y_res = (ymax - ymin) / height_px`. This removes the
  latent arg-order bug.
- **TIN core unchanged:** keep the working constrained-Delaunay + barycentric Z-interpolation
  in `InterpolateElevation` (`triangulate`, `interpolate`, `postProcessTriangulation`).
- **Rasterization (chosen approach — direct pixel-fill):** write the interpolated cell-center
  Z values directly into a row-major Float64 pixel grid, `no_data` for cells outside the
  triangulated hull (or with NaN Z), then emit a GTiff with geotransform
  `(xmin, x_res, 0, ymax, 0, -y_res)` and the given `srid`. This is exact (the TIN already
  produced Z at each cell center) and avoids a second rasterization pass. `RST_DTMFromGeoms`
  will **no longer call** `GDALRasterize.executeRasterize`; the shared `GDALRasterize` util
  itself is untouched (other functions may use it).
- **Validation:** `require()` guards — `width_px > 0`, `height_px > 0`, `xmax > xmin`,
  `ymax > ymin`, points array non-empty — with `rst_dtmfromgeoms:`-prefixed messages.
- **NaN interpolation:** today `interpolate` throws if any cell's Z is NaN. For a grid that
  extends beyond the convex hull this is expected for some cells. Change: cells with no
  containing triangle (or NaN Z) become `no_data` rather than throwing.
- **Shared `execute`:** extract a pure
  `RST_DTMFromGeoms.execute(pointWkbs: Seq[Array[Byte]], breaklineWkbs: Seq[Array[Byte]],
  mergeTol, snapTol, xmin, ymin, xmax, ymax, widthPx, heightPx, srid, noData): InternalRow`
  containing triangulate → interpolate → direct-fill rasterize. The non-agg `eval` and the
  aggregator both call it. WKB/WKT decoding of input geometries happens before `execute`
  (reusing the `geomsFromArrayData` WKB/WKT pattern from `RST_GridFromPoints`).
- **Aggregator (`RST_DTMFromGeomsAgg`):** a `TypedImperativeAggregate[DTMFromGeomsAcc]` mirroring
  `RST_GridFromPointsAgg`:
  - Buffer `DTMFromGeomsAcc` accumulates point WKB byte arrays only; `serialize`/`deserialize`
    for partial aggregation across partitions; `merge` concatenates buffers.
  - `update(buffer, row)`: evaluate `point_geom`, normalize WKT→WKB, append (skip nulls).
  - `eval(buffer)`: evaluate the per-group constants (breaklines array, tolerances, bbox,
    width_px, height_px, srid, no_data) against `InternalRow.empty` via Int/Long-tolerant
    readers (mirror `evalDouble`/`evalInt`), decode the breakline array to WKBs, then call the
    shared `RST_DTMFromGeoms.execute(...)` with `buffer.points`.
  - `dataType` = the same tile `StructType` as the non-agg output.
  - Companion overrides `name = "gbx_rst_dtmfromgeoms_agg"` and a `builder()` accepting 11/12
    args (defaulting `no_data`).

## 3. Registration & metadata

- Uncomment `rd.register(RST_DTMFromGeoms)` **and add** `rd.register(RST_DTMFromGeomsAgg)` in
  `rasterx/functions.scala` (the `_agg` registration goes with the other aggregators).
- Remove the two scoverage `excludedFiles` entries (`pom.xml` lines 466, 508) covering
  `RST_DTMFromGeoms.scala` and `InterpolateElevation.scala`. (`RST_DTMFromGeomsAgg` is a new
  file, not excluded.)
- Add **both** `gbx_rst_dtmfromgeoms` and `gbx_rst_dtmfromgeoms_agg` to
  `docs/tests-function-info/registered_functions.txt`.
- Add a `*_sql_example()` for **each** in `docs/tests/python/api/rasterx_functions_sql.py`, then
  regenerate `function-info.json` via `gbx:docs:function-info`. No hand-edited `ExpressionInfo`
  — usage/example flow from the doc-test single-source pipeline (matching `RST_GridFromPoints`,
  which overrides only `name` and `builder`).

## 4. Testing

- **Scala unit test** (`src/test/scala/.../rasterx/`): construct Z-valued points sampling a
  **known tilted plane** `z = a*x + b*y + c`. Because linear (barycentric) TIN interpolation of
  a planar surface is exact, assert interpolated pixel values equal the plane within a small
  tolerance. Assert out-of-hull cells equal `no_data`. Assert output is a valid single-band
  Float64 GTiff of the requested dimensions. Include one case **with a breakline** to prove
  constraints are honored. Mix in `SilenceProjError` if non-EPSG warnings appear; release GDAL
  datasets in `try/finally`.
- **Scala aggregator test** (`src/test/scala/.../rasterx/`): feed the **same** known-plane
  Z-valued points as a one-row-per-point DataFrame, `groupBy` a constant extent key, call
  `gbx_rst_dtmfromgeoms_agg` with the breaklines as a literal array + the extent params, and
  assert the resulting raster is **byte-for-byte (or pixel-for-pixel within tolerance)
  equivalent** to the non-agg `gbx_rst_dtmfromgeoms` over the identical grid. Include the
  breakline case. This is the key correctness guarantee: agg ≡ non-agg.
- **Python binding tests** (`python/geobrix/test/rasterx/`): `rst_dtmfromgeoms` and
  `rst_dtmfromgeoms_agg` wrappers calling their respective `call_function(...)` with inline
  points; assert a tile row is returned and the raster opens. The agg test uses a row-per-point
  DataFrame + `groupBy`.
- **SQL doc tests** (`docs/tests/.../sql`): inline-constructed Z-valued points (deterministic,
  real code, not mocked) for **both** functions; double as the `function-info` examples. The
  `_agg` example demonstrates the `GROUP BY` row-per-point workflow.
- **binding-parity:** `bash scripts/commands/gbx-test-bindings.sh` passes with **both**
  `gbx_rst_dtmfromgeoms` and `gbx_rst_dtmfromgeoms_agg` present in Scala (name literals), Python
  (`functions.py`), and `function-info.json`.

Test inputs are inline-constructed Z-valued geometries (deterministic), not sample-data files —
appropriate because the assertions need a known surface with a predictable interpolation result.

## 5. Affected files

| File | Change |
|---|---|
| `src/main/scala/.../rasterx/expressions/RST_DTMFromGeoms.scala` | Rework signature (bbox+pixels), Int+Long eval, safeEval fix, validation, drop splitPointFinder, extract shared `execute`, header comment |
| `src/main/scala/.../rasterx/expressions/RST_DTMFromGeomsAgg.scala` | **New** — `TypedImperativeAggregate` aggregator + `DTMFromGeomsAcc` buffer; delegates to `RST_DTMFromGeoms.execute` |
| `src/main/scala/.../rasterx/operations/InterpolateElevation.scala` | bbox-based `pointGrid`; out-of-hull/NaN → no_data instead of throw; header comment |
| `src/main/scala/.../rasterx/functions.scala` | Uncomment `rd.register(RST_DTMFromGeoms)`; add `rd.register(RST_DTMFromGeomsAgg)` |
| `pom.xml` | Remove 2 scoverage `excludedFiles` entries |
| `docs/tests-function-info/registered_functions.txt` | Add `gbx_rst_dtmfromgeoms` and `gbx_rst_dtmfromgeoms_agg` |
| `docs/tests/python/api/rasterx_functions_sql.py` | Add a `*_sql_example()` for each function |
| `src/main/resources/.../function-info.json` | Regenerated |
| `python/geobrix/src/databricks/labs/gbx/rasterx/functions.py` | Add `rst_dtmfromgeoms` and `rst_dtmfromgeoms_agg` wrappers |
| `src/test/scala/.../rasterx/` | New Scala tests (non-agg known-plane + breakline; agg ≡ non-agg) |
| `python/geobrix/test/rasterx/` | New Python binding tests (both functions) |
| `docs/tests/.../sql` | New SQL doc tests (both functions) |

## Verification

- `gbx:test:scala --suite '*RST_DTMFromGeoms*'` (or the rasterx suite) green — includes the
  agg-equals-non-agg assertion.
- `gbx:test:python --path python/geobrix/test/rasterx/` green for both new tests.
- `gbx:test:bindings` green (parity for both functions).
- `gbx:test:function-info` green (every registered function has a non-empty example).
- Doc tests for the new SQL examples green (in Docker).
