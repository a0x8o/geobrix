# VectorX TIN functions: st_triangulate + st_interpolateelevation{bbox,geom}

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add three VectorX geometry-generator functions that expose the constrained-Delaunay TIN pipeline (already used internally by `gbx_rst_dtmfromgeoms`) as first-class geometry output:
- `gbx_st_triangulate` — emits the TIN triangles as polygons.
- `gbx_st_interpolateelevationbbox` — interpolates Z onto a bbox+pixels grid, emits Z-valued points.
- `gbx_st_interpolateelevationgeom` — same, but the grid is given as an origin point + cell counts + cell sizes.

**Why these (utility, not parity):**
- **st_triangulate** — the triangulated irregular network is currently locked inside the raster DTM path. Exposing the triangles as geometries lets users *inspect/visualize the mesh*, validate that breaklines were honored, and feed downstream mesh/contour/QC workflows. Useful on its own.
- **st_interpolateelevationbbox** — interpolating onto an extent+pixel grid returns elevation as **vector points** you can join, aggregate, or grid-index — and the bbox+pixels parameterization is **consistent with the rest of GeoBrix's grid functions** (`rst_dtmfromgeoms`, `rst_gridfrompoints`, `rst_rasterize`), so the same grid composes pixel-aligned across vector and raster.
- **st_interpolateelevationgeom** — lets users define the grid the way terrain practitioners think: an **origin corner + explicit cell size** ("10 m cells starting here"), instead of computing pixel counts from an extent. Resolution-first ergonomics; a distinct, genuinely useful convenience that coexists with the extent-first bbox form. Kept as a **separate clearly-named function** (not an overloaded signature) so each call site is unambiguous.
- **split_point_finder** — tunes how the conforming-Delaunay triangulation handles constraint (breakline) encroachment (`MIDPOINT` vs `NONENCROACHING`), trading triangle quality against constraint fidelity. A real quality knob for breakline-heavy terrain; the underlying builder already supports it (`JTSConformingDelaunayTriangulationBuilder.setSplitPointFinder`), it just isn't wired through yet.

**Architecture:** The pure-JTS TIN math (`triangulate`, `interpolate`, `postProcessTriangulation`, grid helpers) is GDAL-free and conceptually pure geometry, so it moves from `rasterx.operations.InterpolateElevation` to `vectorx.jts` (the three new VectorX functions and the existing `rst_dtmfromgeoms` both consume it; `rasterx` already depends on `vectorx.jts`, so this removes a would-be `vectorx→rasterx` cycle). `split_point_finder` is threaded through as an optional param (default = current behavior, so `rst_dtmfromgeoms` is unchanged). The three functions are `CollectionGenerator` expressions (one input row → many geometry rows), mirroring `vectorx/expressions/ST_AsMvtPyramid`.

**Tech Stack:** Scala 2.13 / Spark 4.0 Catalyst `CollectionGenerator`, JTS (`ConformingDelaunayTriangulationBuilder`, `Triangle.interpolateZ`), PySpark `call_function`. Builds/tests run in the `geobrix-dev` Docker container via `gbx:*`.

**Conventions:** Run Scala/Python tests via `gbx:*` IN THE FOREGROUND, wait for `BUILD SUCCESS/FAILURE` + `Tests: succeeded N`. Never host `mvn`. Rebuild JAR after Scala changes before Python tests. ASCII-only source. `gh auth switch --user mjohns-databricks` before push. Encode/decode geometries with `JTS.fromWKB`/`fromWKT` and `JTS.toWKB3` (Z-preserving — `JTS.toWKB` strips Z). PySpark sends Python ints as `Long` → readers for int args must accept Int **or** Long.

**Implementation reference:** the constrained-Delaunay + barycentric-Z algorithm already lives in our `InterpolateElevation` (`triangulate`/`interpolate`); the new work is mostly *exposing* it via generator expressions, not new algorithm code.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/main/scala/.../vectorx/jts/InterpolateElevation.scala` (MOVED from `rasterx/operations/`) | Pure-JTS TIN math; `triangulate`/`interpolate` gain optional `splitPointFinder`; add `pointGridOrigin`. |
| `src/main/scala/.../rasterx/expressions/RST_DTMFromGeoms.scala` (edit imports) | Now imports `InterpolateElevation` from `vectorx.jts`. |
| `src/test/scala/.../vectorx/jts/InterpolateElevationTest.scala` (MOVED) | Follows the object. |
| `src/main/scala/.../vectorx/expressions/ST_Triangulate.scala` (new) | Generator → triangle polygons. |
| `src/main/scala/.../vectorx/expressions/ST_InterpolateElevationBBox.scala` (new) | Generator → Z-points, bbox+pixels grid. |
| `src/main/scala/.../vectorx/expressions/ST_InterpolateElevationGeom.scala` (new) | Generator → Z-points, origin+cell-size grid. |
| `src/main/scala/.../vectorx/functions.scala` (edit) | Register the three. |
| `docs/tests-function-info/registered_functions.txt` | Add 3 names. |
| `docs/tests/python/api/vectorx_functions_sql.py` | 3 `*_sql_example()`. |
| `src/main/resources/.../function-info.json` | Regenerated. |
| `python/.../vectorx/functions.py` | 3 wrappers. |
| `src/test/scala/.../vectorx/expressions/ST_*Test.scala` (new) | per-function tests. |
| `python/geobrix/test/vectorx/test_tin_functions.py` (new) | binding smoke tests. |

---

## Task 1: Move TIN math to `vectorx.jts` + thread optional `split_point_finder`

**Files:** move `src/main/scala/com/databricks/labs/gbx/rasterx/operations/InterpolateElevation.scala` → `src/main/scala/com/databricks/labs/gbx/vectorx/jts/InterpolateElevation.scala` (package `com.databricks.labs.gbx.vectorx.jts`); move its test similarly; edit `RST_DTMFromGeoms.scala` import.

- [ ] **Step 1: Read** the current `InterpolateElevation.scala` (rasterx/operations), `RST_DTMFromGeoms.scala` (its `import ...operations.InterpolateElevation` + call sites: `triangulate`, `pointGridBBox`, `interpolate`), and `vectorx/jts/JTSConformingDelaunayTriangulationBuilder.scala` (the `setSplitPointFinder(TriangulationSplitPointTypeEnum.Value)` + `TriangulationSplitPointTypeEnum.fromString` API). Grep for any other references to `rasterx.operations.InterpolateElevation`.

- [ ] **Step 2: Move + repackage.** Move the file to `vectorx/jts/InterpolateElevation.scala`, change its `package` to `com.databricks.labs.gbx.vectorx.jts`. It already imports `JTS` and `JTSConformingDelaunayTriangulationBuilder` from this package (now same-package). Move the test `InterpolateElevationTest.scala` to `src/test/scala/com/databricks/labs/gbx/vectorx/jts/` and update its `package`.

- [ ] **Step 3: Thread optional `splitPointFinder`** (behavior-preserving). Change:
```scala
def triangulate(multiPoint: Geometry, breaklines: Seq[Geometry],
                mergeTolerance: Double, snapTolerance: Double,
                splitPointFinder: Option[TriangulationSplitPointTypeEnum.Value] = None): Seq[Geometry] = {
    ...
    val triangulator = JTSConformingDelaunayTriangulationBuilder(multiPoint)
    if (breaklines.nonEmpty) triangulator.setConstraints(multiLineString)
    triangulator.setTolerance(mergeTolerance)
    splitPointFinder.foreach(triangulator.setSplitPointFinder)   // only set when provided
    ...
}
```
and forward it through `interpolate`:
```scala
def interpolate(multipoint: MultiPoint, breaklines: Seq[LineString], gridPoints: MultiPoint,
                mergeTolerance: Double, snapTolerance: Double,
                splitPointFinder: Option[TriangulationSplitPointTypeEnum.Value] = None): Seq[Point] = {
    val triangles = triangulate(multipoint, breaklines, mergeTolerance, snapTolerance, splitPointFinder)
    ...
}
```
The `= None` defaults mean `RST_DTMFromGeoms`'s existing 4-arg calls compile unchanged and behave identically (no `setSplitPointFinder` call). Import `TriangulationSplitPointTypeEnum` (same package now).

- [ ] **Step 4: Add `pointGridOrigin`** (for the geom-form function in Task 4):
```scala
/** Grid of cell-center points from an origin corner + cell counts + per-cell sizes.
 *  Centers: x = originX + (i + 0.5)*cellSizeX, y = originY + (j + 0.5)*cellSizeY.
 *  cellSizeY is typically negative (y-down). Column-major (x slowest, y fastest).
 */
def pointGridOrigin(originX: Double, originY: Double, cols: Int, rows: Int,
                    cellSizeX: Double, cellSizeY: Double, srid: Int): MultiPoint = {
    val pts = for (i <- 0 until cols; j <- 0 until rows) yield {
        val p = JTS.point(new Coordinate(originX + (i + 0.5) * cellSizeX, originY + (j + 0.5) * cellSizeY))
        p.setSRID(srid); p
    }
    val mp = JTS.multiPoint(pts.toArray); mp.setSRID(srid); mp
}
```

- [ ] **Step 5: Update `RST_DTMFromGeoms.scala`** import from `...rasterx.operations.InterpolateElevation` to `...vectorx.jts.InterpolateElevation`. Fix any other references found in Step 1.

- [ ] **Step 6: Verify no regression** (FOREGROUND, wait): run BOTH the moved unit test and the dtmfromgeoms suite:
```
gbx:test:scala --suites 'com.databricks.labs.gbx.vectorx.jts.InterpolateElevationTest,com.databricks.labs.gbx.rasterx.expressions.RST_DTMFromGeomsTest' --log tin-move.log
```
Expect all pass (InterpolateElevation tests + the 8 dtmfromgeoms tests). This proves the move + default-param threading didn't change dtmfromgeoms behavior.

- [ ] **Step 7: Commit** `git commit -m "refactor(vectorx): move TIN math to vectorx.jts; optional split_point_finder; add pointGridOrigin"`

---

## Task 2: `gbx_st_triangulate`

**Files:** `src/main/scala/.../vectorx/expressions/ST_Triangulate.scala` (new); test `src/test/scala/.../vectorx/expressions/ST_TriangulateTest.scala` (new).

**Utility:** exposes the TIN triangles as polygons so users can inspect/validate/visualize the mesh.

- [ ] **Step 1: Read** `src/main/scala/com/databricks/labs/gbx/vectorx/expressions/ST_AsMvtPyramid.scala` (the `CollectionGenerator with CodegenFallback` pattern: `elementSchema`, `eval(input): IterableOnce[InternalRow]`, `children`, `withNewChildrenInternal`, companion `name`/`builder`), and how a VectorX expression is registered in `vectorx/functions.scala` + how geometries are decoded/encoded (`JTS.fromWKB`/`fromWKT`, `JTS.toWKB`). Read `InterpolateElevation.triangulate` (now in vectorx.jts) and `TriangulationSplitPointTypeEnum.fromString`.

- [ ] **Step 2: Write the failing test.** `ST_TriangulateTest.scala` (AnyFunSuite + Matchers): build 4 corner points of a square (e.g. (0,0),(10,0),(0,10),(10,10)) as a geometry array, empty breaklines; construct `ST_Triangulate(...)` with `Literal` children; call `.eval(InternalRow)` and assert it yields **2 triangle rows** (Delaunay of a square = 2 triangles), each a valid Polygon WKB (parse via `JTS.fromWKB`, assert `.getNumPoints == 4` ring / `isValid`). Add a case with a breakline asserting it still triangulates (count > 0).

- [ ] **Step 3: Run, verify FAIL** (FOREGROUND, wait): `gbx:test:scala --suite 'com.databricks.labs.gbx.vectorx.expressions.ST_TriangulateTest' --log st-triangulate.log`.

- [ ] **Step 4: Implement `ST_Triangulate`.** `CollectionGenerator with CodegenFallback`, 5 children `(pointsArray, breaklinesArray, mergeTolerance, snapTolerance, splitPointFinder)`. `elementSchema = StructType(StructField("triangle", BinaryType) :: Nil)`. `eval`: decode arrays (WKB/WKT) to JTS geoms (mirror `RST_DTMFromGeoms.geomsFromArrayData`), build `JTS.multiPoint(points)`, parse `splitPointFinder` String via `TriangulationSplitPointTypeEnum.fromString`, call `InterpolateElevation.triangulate(mp, lines.map(_.asInstanceOf[LineString]), mergeTol, snapTol, Some(finder))`, map each triangle polygon to `InternalRow(JTS.toWKB(poly))` (triangles are 2D rings — `toWKB` is correct here; no Z needed). Companion `name = "gbx_st_triangulate"`, builder requiring 5 args. Register-ready (registration in Task 5).

- [ ] **Step 5: Run, verify PASS** (FOREGROUND, wait).

- [ ] **Step 6: Commit** `git commit -m "feat(vectorx): gbx_st_triangulate generator (TIN triangles as polygons)"`

---

## Task 3: `gbx_st_interpolateelevationbbox`

**Files:** `.../vectorx/expressions/ST_InterpolateElevationBBox.scala` (new); test (new).

**Utility:** Z interpolation onto an extent+pixel grid, returned as vector points; bbox+pixels parameterization composes with the rest of GeoBrix's grid functions.

- [ ] **Step 1: Read** `InterpolateElevation.{pointGridBBox, interpolate}` and the `ST_Triangulate` you just wrote (for the generator + decode pattern). Note PySpark Long handling for `width_px`/`height_px`/`srid`.

- [ ] **Step 2: Write the failing test.** Known tilted plane `z = 2x + 3y + 5` at 4 corners of a 100×100 extent; grid 10×10 over (0,0)-(100,100); assert the generator yields Z-valued points whose Z equals `2x+3y+5` (within 1e-6) at each emitted point's (x,y); assert count = number of in-hull cells (100 for a fully-covered square). Construct via `Literal` children, `.eval(InternalRow)`, collect rows, parse each WKB point, check Z.

- [ ] **Step 3: Run, verify FAIL** (FOREGROUND, wait): suite `com.databricks.labs.gbx.vectorx.expressions.ST_InterpolateElevationBBoxTest`.

- [ ] **Step 4: Implement.** `CollectionGenerator`, 12 children `(points, breaklines, mergeTol, snapTol, splitPointFinder, xmin, ymin, xmax, ymax, widthPx, heightPx, srid)`. Read `widthPx/heightPx/srid` Int-or-Long tolerant (mirror `RST_DTMFromGeomsAgg.evalInt` style, but these are direct children so eval against the input row). `eval`: decode points/lines, `grid = InterpolateElevation.pointGridBBox(xmin,ymin,xmax,ymax,widthPx,heightPx,srid)`, `pts = InterpolateElevation.interpolate(mp, lines, grid, mergeTol, snapTol, Some(finder))`, emit `InternalRow(JTS.toWKB3(p))` per point (**toWKB3** — Z must be preserved). `elementSchema = StructType(StructField("elevation_point", BinaryType) :: Nil)`. Companion `name = "gbx_st_interpolateelevationbbox"`, builder requiring 12 args.

- [ ] **Step 5: Run, verify PASS** (FOREGROUND, wait).

- [ ] **Step 6: Commit** `git commit -m "feat(vectorx): gbx_st_interpolateelevationbbox generator (bbox+pixels grid)"`

---

## Task 4: `gbx_st_interpolateelevationgeom`

**Files:** `.../vectorx/expressions/ST_InterpolateElevationGeom.scala` (new); test (new).

**Utility:** define the grid by origin corner + cell counts + cell sizes (resolution-first), the natural way to ask for "N-metre cells starting here."

- [ ] **Step 1: Read** `InterpolateElevation.pointGridOrigin` (added in Task 1) and the `ST_InterpolateElevationBBox` you just wrote.

- [ ] **Step 2: Write the failing test.** Same plane. Pick an origin + cell sizes that yield the SAME grid as a bbox case (e.g. origin (0,0), cols=10, rows=10, cell_size_x=10.0, cell_size_y=10.0 → centers at 5,15,...,95 — matching `pointGridBBox(0,0,100,100,10,10)`). Assert the emitted Z-points match `z=2x+3y+5` at their (x,y). Add an **equivalence assertion**: the set of (x,y,z) emitted by geom-form equals the set emitted by `ST_InterpolateElevationBBox` over the equivalent extent (sort both, compare) — proving the two functions are consistent. (Use positive cell_size_y here with origin at min-corner so centers match pointGridBBox; document that negative cell_size_y is y-down.)

- [ ] **Step 3: Run, verify FAIL** (FOREGROUND, wait): suite `...ST_InterpolateElevationGeomTest`.

- [ ] **Step 4: Implement.** `CollectionGenerator`, 10 children `(points, breaklines, mergeTol, snapTol, splitPointFinder, gridOrigin, gridCols, gridRows, cellSizeX, cellSizeY)`. `eval`: decode points/lines; decode `gridOrigin` geometry (WKB/WKT) → a JTS Point; `originX = origin.getX`, `originY = origin.getY`, `srid = origin.getSRID` (if 0, that's acceptable — document that origin should carry SRID); `gridCols/gridRows` Int-or-Long tolerant; `grid = InterpolateElevation.pointGridOrigin(originX, originY, cols, rows, cellSizeX, cellSizeY, srid)`; `pts = InterpolateElevation.interpolate(mp, lines, grid, mergeTol, snapTol, Some(finder))`; emit `InternalRow(JTS.toWKB3(p))`. `elementSchema = StructType(StructField("elevation_point", BinaryType) :: Nil)`. Companion `name = "gbx_st_interpolateelevationgeom"`, builder requiring 10 args.

- [ ] **Step 5: Run, verify PASS** (FOREGROUND, wait) — including the bbox/geom equivalence assertion.

- [ ] **Step 6: Commit** `git commit -m "feat(vectorx): gbx_st_interpolateelevationgeom generator (origin+cell-size grid)"`

---

## Task 5: Register all three + rebuild JAR

- [ ] **Step 1:** In `src/main/scala/com/databricks/labs/gbx/vectorx/functions.scala`, add `rd.register(ST_Triangulate)`, `rd.register(ST_InterpolateElevationBBox)`, `rd.register(ST_InterpolateElevationGeom)` near the other `ST_*` registrations; add imports if the file imports expressions individually (mirror existing style; check how `ST_AsMvtPyramid` is imported/registered).
- [ ] **Step 2: Rebuild** (FOREGROUND, wait): `gbx:docker:exec "mvn clean package -PskipScoverage -DskipTests"` → BUILD SUCCESS.
- [ ] **Step 3: Commit** `git commit -m "feat(vectorx): register st_triangulate + st_interpolateelevation{bbox,geom}"`

---

## Task 6: registered_functions.txt + SQL examples + function-info

- [ ] **Step 1:** Add `gbx_st_triangulate`, `gbx_st_interpolateelevationbbox`, `gbx_st_interpolateelevationgeom` to `docs/tests-function-info/registered_functions.txt`.
- [ ] **Step 2:** Add a `*_sql_example()` + `_output` for each to `docs/tests/python/api/vectorx_functions_sql.py` (find it; mirror existing `st_*` example style — placeholder tables OK, display+structural-validation only). Examples should show the streaming/generator usage (`SELECT gbx_st_triangulate(masspoints, breaklines, 0.01, 0.01, 'NONENCROACHING') FROM survey` etc.). Use clear inline values; for the geom form show `ST_Point(...)` origin + cell sizes.
- [ ] **Step 3: Regenerate** (FOREGROUND, wait): `gbx:docs:function-info`; confirm all three in `function-info.json`.
- [ ] **Step 4: Verify coverage** (FOREGROUND, wait): `gbx:test:function-info --log tin-fninfo.log` — `test_full_coverage_against_registered_list` passes (pre-existing `databricks`-module errors, if any, are baseline noise — confirm no NEW failure for the three).
- [ ] **Step 5: Commit** `git commit -m "docs: function-info examples for st_triangulate + st_interpolateelevation{bbox,geom}"`

---

## Task 7: Python bindings + tests

- [ ] **Step 1: Write failing tests** mirroring an existing vectorx python test's session header. `python/geobrix/test/vectorx/test_tin_functions.py`: for each function, build a small DataFrame of Z-valued point WKT/WKB (a square + corners), `select`/`lateral`-explode the generator, assert non-empty rows of geometry. (For generators in PySpark, the call returns multiple rows — use the generator in a `select` and `.collect()`; confirm how existing generator bindings like `st_asmvt_pyramid` are tested.)
- [ ] **Step 2: Run, verify FAIL** (FOREGROUND, wait): `gbx:test:python --path python/geobrix/test/vectorx/test_tin_functions.py --log tin-py.log`.
- [ ] **Step 3: Add wrappers** to `python/geobrix/src/databricks/labs/gbx/vectorx/functions.py`:
  - `st_triangulate(points_geom, breaklines_geom, merge_tolerance, snap_tolerance, split_point_finder)`
  - `st_interpolateelevationbbox(points_geom, breaklines_geom, merge_tolerance, snap_tolerance, split_point_finder, xmin, ymin, xmax, ymax, width_px, height_px, srid)`
  - `st_interpolateelevationgeom(points_geom, breaklines_geom, merge_tolerance, snap_tolerance, split_point_finder, grid_origin, grid_cols, grid_rows, cell_size_x, cell_size_y)`
  Each `return f.call_function("gbx_...", _col(...), ...)`. Match the existing vectorx wrapper style + docstrings (utility-framed, no Mosaic references).
- [ ] **Step 4: Run, verify PASS** (FOREGROUND, wait).
- [ ] **Step 5: Commit** `git commit -m "feat(python): bindings + tests for st_triangulate + st_interpolateelevation{bbox,geom}"`

---

## Task 8: Full verification + push

- [ ] **Step 1: binding-parity** — `bash scripts/commands/gbx-test-bindings.sh --log tin-parity.log` → all three present in Scala/Python/function-info; parity green (count 147).
- [ ] **Step 2: Scala suites** (FOREGROUND/bg, wait): `gbx:test:scala --suites 'com.databricks.labs.gbx.vectorx.*,com.databricks.labs.gbx.rasterx.*'` → 0 failures (rasterx included because the TIN math moved — confirms dtmfromgeoms still green).
- [ ] **Step 3: Python suites:** `gbx:test:python --path python/geobrix/test/vectorx/` and `--path python/geobrix/test/rasterx/` → pass.
- [ ] **Step 4: scalastyle:** `gbx:lint:scalastyle` → 0 errors (ASCII-only).
- [ ] **Step 5: function-info coverage** → pass.
- [ ] **Step 6: Push** (`gh auth switch --user mjohns-databricks` first): `git push origin beta/0.4.0`. QC `binding-parity` gates the three.

---

## Self-review notes (author)
- **Rationale framing:** every function justified by user utility; no "Mosaic-faithful"/parity framing in plan, examples, docstrings, or function-info.
- **Coverage:** TIN extraction + split_point_finder threading (T1, behavior-preserving for dtmfromgeoms, re-verified); three generators (T2-4) with per-function tests incl. the bbox/geom **equivalence** test; registration (T5); function-info (T6); Python (T7); full verification incl. rasterx regression since TIN moved (T8).
- **Type consistency:** `InterpolateElevation.triangulate`/`interpolate` gain `splitPointFinder: Option[...] = None` (dtmfromgeoms calls unchanged); generators pass `Some(fromString(...))`; `toWKB3` used for Z-points, `toWKB` for triangle polygons; Int/Long tolerance on count/srid args.
- **Risk:** T1 moves shipped code — mitigated by re-running the dtmfromgeoms suite in T1 Step 6 and the rasterx suite in T8.
