# Custom Grid (gbx_custom_*) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add a *bring-your-own regular cell-index grid* to GridX: a user defines a grid by bounds + root cell size + split factor (in any projected CRS), and gets the core cell-index vocabulary on it — `point→cellId`, `cellId→polygon/centroid`, `polyfill`, `kRing`.

**Why (utility):** GeoBrix's built-in grids are each fixed — BNG (UK EPSG:27700 only), QuadBin/H3 (WGS84). None lets a user index into *their own* regular grid in *their own* CRS at *their own* cell size. Custom gridding enables spatial binning / aggregation / tiling on an arbitrary regular grid (e.g. a national grid in its native CRS, or a study-area analysis grid), reusing the same grid-op vocabulary as BNG. Distinct from raster/point gridding (`rst_gridfrompoints`, `st_interpolateelevation*`) which produce rasters/points, not a reusable cell index.

**Architecture:** The complete core math already exists, commented out, in `gridx/grid/{GridConf.scala, CustomGridSystem.scala}` (it's correct and slightly *ahead* of the reference — `cellIdToBoundary`/`cellIdToCenter` are implemented). Work = uncomment + fix the one broken import, then add bespoke `gbx_custom_*` Spark expressions (GridX has no shared IndexSystem trait — BNG/QuadBin are bespoke; mirror that). The grid spec is passed per call as a **struct** built by a `gbx_custom_grid(...)` constructor, so op signatures stay small (`op(operand, grid[, res])`). Each op decodes the struct → `GridConf` → `CustomGridSystem` → method.

**Polyfill semantic (clarification):** the existing core `polyfill` is a correct, standard **centroid-containment** polyfill — a cell is included iff its center falls inside the geometry (same semantic as H3 polyfill). This is NOT a bug; ship it as-is with the semantic documented + tested. (A BNG-style *intersects*-coverage flood-fill is a different semantic and an explicit future option, not this scope.)

**Tech Stack:** Scala 2.13 / Spark 4.0 Catalyst expressions, JTS. Builds/tests in the `geobrix-dev` Docker container via `gbx:*`.

**Conventions:** Run Scala/Python tests via `gbx:*` IN THE FOREGROUND, wait for `BUILD SUCCESS/FAILURE` + `Tests: succeeded N`. Never host `mvn`. Rebuild JAR after Scala changes before Python tests. ASCII-only source. `gh auth switch --user mjohns-databricks` before push. **Before pushing python changes run `gbx:lint:python --check`.** PySpark sends ints as Long → readers for int args (resolution, k, splits, sizes) must accept Int **or** Long.

---

## File Structure

| File | Responsibility |
|---|---|
| `gridx/grid/GridConf.scala` | Uncomment the `GridConf` case class (no logic change). |
| `gridx/grid/CustomGridSystem.scala` | Uncomment; fix `import JTS` → `com.databricks.labs.gbx.vectorx.jts.JTS`; the rest is correct. |
| `gridx/custom/Custom_GridSpec.scala` (new) | Shared: the grid-struct `StructType` schema + `gridConfFromRow(InternalRow): GridConf` decoder + Int/Long readers. |
| `gridx/custom/Custom_Grid.scala` (new) | `gbx_custom_grid(...)` constructor expression → grid struct (with validation). |
| `gridx/custom/Custom_PointAsCell.scala` (new) | `gbx_custom_pointascell(point_geom, grid, res) -> BIGINT` |
| `gridx/custom/Custom_AsWKB.scala`, `Custom_AsWKT.scala`, `Custom_Centroid.scala` (new) | `cellId, grid -> polygon WKB / polygon WKT / centroid-point WKB` |
| `gridx/custom/Custom_Polyfill.scala` (new) | `gbx_custom_polyfill(geom, grid, res) -> ARRAY<BIGINT>` |
| `gridx/custom/Custom_KRing.scala` (new) | `gbx_custom_kring(cell, grid, k) -> ARRAY<BIGINT>` |
| `gridx/custom/functions.scala` (new) | `register(spark)` for all `gbx_custom_*`; wired into GridX registration. |
| `gridx/functions.scala` (or wherever GridX aggregates registration) | Call custom `register`. |
| `docs/tests-function-info/registered_functions.txt` | Add the 7 names. |
| `docs/tests/python/api/gridx_functions_sql.py` | `*_sql_example()` for each. |
| `src/main/resources/.../function-info.json` | Regenerated. |
| `python/.../gridx/custom/functions.py` (new) | 7 wrappers. |
| `src/test/scala/.../gridx/...` | core math test + per-op tests. |
| `python/geobrix/test/gridx/custom/test_custom_grid.py` (new) | binding tests. |

---

## Task 1: Uncomment + fix the core (GridConf + CustomGridSystem) + core unit test

**Files:** `gridx/grid/GridConf.scala`, `gridx/grid/CustomGridSystem.scala`; test `src/test/scala/com/databricks/labs/gbx/gridx/grid/CustomGridSystemTest.scala` (new).

- [ ] **Step 1: Write the failing test** — `CustomGridSystemTest.scala` (AnyFunSuite + Matchers). Use a known grid `GridConf(0, 100, 0, 100, cellSplits = 2, rootCellSizeX = 10, rootCellSizeY = 10, crsID = Some(32633))` and `val g = CustomGridSystem(conf)`. Assert:
  - `g.pointToCellID(5.0, 5.0, 0)` returns a Long whose `g.getCellResolution(id) == 0` and whose `cellIdToGeometry(id)` is the rectangle `[0,10]×[0,10]` (check envelope min/max). Point (5,5) at res 0 (10×10 root cells) → cell (0,0).
  - `g.pointToCellID(15.0, 25.0, 0)` → cell (1,2): envelope `[10,20]×[20,30]`.
  - At res 1 (cellSplits=2 → 5×5 cells over the 10-unit root? NO: cellWidth(1) = 10/2^1 = 5; totalCellsX(1) = rootCellCountX * 2^1 = 10*2 = 20): `g.pointToCellID(2.5, 2.5, 1)` → cell width 5 → cell (0,0) envelope `[0,5]×[0,5]`.
  - `cellIdToCenter` of the (0,0) res-0 cell ≈ (5,5).
  - `g.polyfill(<a polygon covering [0,30]×[0,30]>, 0)` returns the 9 cells whose centers (5,15,25 × 5,15,25) fall inside — assert size 9 (centroid semantic).
  - `g.kRing(<center cell at res 0>, 1)` returns the 3×3 (or clipped) neighbourhood.
  - Build the polygon for polyfill via `JTS.fromWKT("POLYGON ((0 0, 30 0, 30 30, 0 30, 0 0))")`.

- [ ] **Step 2: Run, verify FAIL** (FOREGROUND, wait): `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.grid.CustomGridSystemTest' --log custom-core.log` — expect compile-fail (GridConf/CustomGridSystem are commented out).

- [ ] **Step 3: Uncomment the core.** In `GridConf.scala`: uncomment the `case class GridConf(...)` block (remove the leading `//` on lines 4-34). No logic change. In `CustomGridSystem.scala`: uncomment everything (remove leading `//`), and FIX the broken import on (commented) line 5: `import JTS` → `import com.databricks.labs.gbx.vectorx.jts.JTS`. Keep `import org.apache.spark.unsafe.types.UTF8String`, `import org.locationtech.jts.geom.{Coordinate, Geometry}`, `import scala.util.{Success, Try}`. Verify `JTS.point(Double, Double)` and `JTS.polygonFromXYs(Array[(Double,Double)])` are used (they exist in JTS) — no change needed.

- [ ] **Step 4: Run, verify PASS** (FOREGROUND, wait). Expect all core tests pass. If a cell-position/envelope assertion is off, re-derive the expected cell by hand from the formulas (`cellWidth(res) = rootCellSizeX / cellSplits^res`, `cellPosX = floor((x - boundXMin)/cellWidth)`, `totalCellsX(res) = rootCellCountX * cellSplits^res`) and correct the TEST's expected value (the core math is the reference) — do not change the core unless a real bug surfaces.

- [ ] **Step 5: Commit** `git commit -m "feat(gridx): enable CustomGridSystem core (uncomment GridConf + CustomGridSystem, fix import)"`

---

## Task 2: Grid-spec struct + `gbx_custom_grid` constructor

**Files:** `gridx/custom/Custom_GridSpec.scala` (new, shared helpers), `gridx/custom/Custom_Grid.scala` (new constructor); test `src/test/scala/.../gridx/custom/Custom_GridTest.scala`.

**Struct schema** (the grid spec carried between functions):
```
StructType(Seq(
  StructField("bound_x_min", LongType, false),
  StructField("bound_x_max", LongType, false),
  StructField("bound_y_min", LongType, false),
  StructField("bound_y_max", LongType, false),
  StructField("cell_splits", IntegerType, false),
  StructField("root_cell_size_x", IntegerType, false),
  StructField("root_cell_size_y", IntegerType, false),
  StructField("srid", IntegerType, false)   // -1 == no CRS (Option None)
))
```

- [ ] **Step 1: Read** `gridx/bng/BNG_PointAsCell.scala` + `gridx/bng/functions.scala` for the expression base class + registration pattern, and `gridx/grid/CustomGridSystem.scala` (now uncommented) for `GridConf`/`CustomGridSystem`.

- [ ] **Step 2: Write `Custom_GridSpec.scala`** (an object with shared helpers; no expression):
```scala
package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

object Custom_GridSpec {
    /** Schema of the grid-spec struct produced by gbx_custom_grid and consumed by all ops. */
    val gridStructType: StructType = StructType(Seq(
        StructField("bound_x_min", LongType, nullable = false),
        StructField("bound_x_max", LongType, nullable = false),
        StructField("bound_y_min", LongType, nullable = false),
        StructField("bound_y_max", LongType, nullable = false),
        StructField("cell_splits", IntegerType, nullable = false),
        StructField("root_cell_size_x", IntegerType, nullable = false),
        StructField("root_cell_size_y", IntegerType, nullable = false),
        StructField("srid", IntegerType, nullable = false)
    ))

    /** Decode the grid-spec struct InternalRow into a CustomGridSystem. */
    def systemFromRow(row: InternalRow): CustomGridSystem = {
        require(row != null, "gbx_custom: grid spec must not be null")
        val srid = row.getInt(7)
        CustomGridSystem(GridConf(
            boundXMin = row.getLong(0), boundXMax = row.getLong(1),
            boundYMin = row.getLong(2), boundYMax = row.getLong(3),
            cellSplits = row.getInt(4),
            rootCellSizeX = row.getInt(5), rootCellSizeY = row.getInt(6),
            crsID = if (srid < 0) None else Some(srid)
        ))
    }

    /** Int-or-Long tolerant read (PySpark sends Long). */
    def asInt(v: Any, label: String): Int = v match {
        case i: Int => i
        case l: Long => l.toInt
        case null => throw new IllegalArgumentException(s"gbx_custom: $label must not be null")
        case o => throw new IllegalArgumentException(s"gbx_custom: $label must be INT or LONG; got ${o.getClass.getName}")
    }
}
```

- [ ] **Step 3: Write the failing constructor test** — `Custom_GridTest.scala`: build `Custom_Grid` with `Literal` args (0L,100L,0L,100L,2,10,10,32633), eval against `InternalRow.empty`, assert the returned `InternalRow` has the 8 fields with those values; assert `Custom_GridSpec.systemFromRow(result)` yields a `CustomGridSystem` whose `conf.maxResolution > 0`. Assert validation: `xmax<=xmin` (e.g. 100,0) throws; `cell_splits < 2` throws; `root_cell_size_x <= 0` throws.

- [ ] **Step 4: Run, verify FAIL** (FOREGROUND, wait): suite `com.databricks.labs.gbx.gridx.custom.Custom_GridTest`.

- [ ] **Step 5: Implement `Custom_Grid.scala`** — a Catalyst expression (extend `Expression with CodegenFallback`, or the simplest base that returns a struct; mirror how an existing GeoBrix expression returns a StructType — check BNG which returns chip structs). 8 children (bound_x_min..srid), `dataType = Custom_GridSpec.gridStructType`, `nullable = false`. `eval(input)`: read the 8 args (Long bounds via `asLong`-tolerant; Int splits/sizes/srid via `Custom_GridSpec.asInt`), validate (`xmax > xmin`, `ymax > ymin`, `cell_splits >= 2`, `root_cell_size_x > 0`, `root_cell_size_y > 0`), return `InternalRow(xmin, xmax, ymin, ymax, splits, rootX, rootY, srid)`. Companion `extends WithExpressionInfo`: `name = "gbx_custom_grid"`, builder accepting 7 args (srid defaulted to `Literal(-1)`) or 8 args. `withNewChildrenInternal` copies children.

- [ ] **Step 6: Run, verify PASS** (FOREGROUND, wait).

- [ ] **Step 7: Commit** `git commit -m "feat(gridx): gbx_custom_grid grid-spec constructor + shared decoder"`

---

## Task 3: Cell-identity ops — `pointascell`, `aswkb`, `aswkt`, `centroid`

**Files:** `gridx/custom/Custom_PointAsCell.scala`, `Custom_AsWKB.scala`, `Custom_AsWKT.scala`, `Custom_Centroid.scala` (new); test `Custom_OpsTest.scala`.

Each op: read the grid struct via `Custom_GridSpec.systemFromRow`, then call the matching `CustomGridSystem` method.

- [ ] **Step 1: Write the failing test** — `Custom_OpsTest.scala`: grid `gbx_custom_grid(0,100,0,100,2,10,10,32633)` (build the struct via `Custom_Grid` eval, or directly an `InternalRow` of the 8 fields). Construct each op expression with `Literal` children + the grid struct literal, eval, assert:
  - `Custom_PointAsCell(point WKB at (5,5), grid, res=0)` → a Long; feeding that Long back to `Custom_AsWKB(cell, grid)` → polygon WKB whose envelope is `[0,10]×[0,10]`.
  - `Custom_AsWKT(cell, grid)` → WKT string starting `POLYGON`.
  - `Custom_Centroid(cell, grid)` → point WKB at ≈(5,5).
  Build the input point via `JTS.toWKB(JTS.point(5.0, 5.0))`.

- [ ] **Step 2: Run, verify FAIL** (FOREGROUND, wait): suite `com.databricks.labs.gbx.gridx.custom.Custom_OpsTest`.

- [ ] **Step 3: Implement the four ops.** Mirror a BNG op's expression base. Decode geometry inputs with the typed pattern (`getBinary`/`getUTF8String` by declared element type, or `JTS.fromWKB`/`fromWKT` on the value — these are scalar geometry args, not arrays, so `geom.eval(input)` → `Array[Byte]`/`UTF8String` → `JTS.fromWKB`/`fromWKT`). Specs:
  - **Custom_PointAsCell**(geomExpr, gridExpr, resExpr): `dataType = LongType`. eval: `sys = systemFromRow(gridExpr.eval(input).asInstanceOf[InternalRow])`; decode point geom → `c = geom.getCoordinate`; `res = asInt(resExpr.eval(input), "resolution")`; return `sys.pointToCellID(c.x, c.y, res)`. name `gbx_custom_pointascell`, 3-arg builder.
  - **Custom_AsWKB**(cellExpr, gridExpr): `dataType = BinaryType`. eval: `sys.cellIdToGeometry(cell)` → `JTS.toWKB(_)`. name `gbx_custom_cellaswkb`, 2-arg.
  - **Custom_AsWKT**(cellExpr, gridExpr): `dataType = StringType`. → `UTF8String.fromString(JTS.toWKT(sys.cellIdToGeometry(cell)))`. name `gbx_custom_cellaswkt`, 2-arg.
  - **Custom_Centroid**(cellExpr, gridExpr): `dataType = BinaryType`. → `c = sys.cellIdToCenter(cell)`; `JTS.toWKB(JTS.point(c))`. name `gbx_custom_centroid`, 2-arg.
  `cell` args are `Long` (read via `asInt`-style but Long: `cellExpr.eval(input).asInstanceOf[Long]`). Guard null grid/cell.

- [ ] **Step 4: Run, verify PASS** (FOREGROUND, wait).

- [ ] **Step 5: Commit** `git commit -m "feat(gridx): custom-grid cell-identity ops (pointascell, cellaswkb, cellaswkt, centroid)"`

---

## Task 4: Coverage ops — `polyfill`, `kring` (array-returning)

**Files:** `gridx/custom/Custom_Polyfill.scala`, `Custom_KRing.scala` (new); test `Custom_CoverageTest.scala`.

- [ ] **Step 1: Write the failing test** — `Custom_CoverageTest.scala`, same grid:
  - `Custom_Polyfill(POLYGON((0 0,30 0,30 30,0 30,0 0)) WKB, grid, res=0)` → `ARRAY<BIGINT>` of size 9 (centroid-containment: the 9 cells with centers at {5,15,25}×{5,15,25}). Assert size 9 and that each returned cell's `cellIdToGeometry` envelope lies within `[0,30]×[0,30]`.
  - `Custom_KRing(centerCell, grid, k=1)` for the (1,1) res-0 cell → the 3×3 = 9 neighbourhood (or clipped at the grid edge); assert it contains the center and its 8 neighbours' ids.

- [ ] **Step 2: Run, verify FAIL** (FOREGROUND, wait): suite `com.databricks.labs.gbx.gridx.custom.Custom_CoverageTest`.

- [ ] **Step 3: Implement.** Mirror `BNG_Polyfill`/`BNG_KRing` (array-returning) for the result encoding (`ArrayData`/`GenericArrayData` of Long). 
  - **Custom_Polyfill**(geomExpr, gridExpr, resExpr): `dataType = ArrayType(LongType, false)`. eval: decode geom → `sys.polyfill(geom, res)` → `ArrayData.toArrayData(seq.toArray)` (mirror how BNG_Polyfill builds its array result). name `gbx_custom_polyfill`, 3-arg. Scaladoc: documents **centroid-containment** semantic (cell included iff its center is inside the geometry).
  - **Custom_KRing**(cellExpr, gridExpr, kExpr): `dataType = ArrayType(LongType, false)`. eval: `sys.kRing(cell, asInt(k))` → array. name `gbx_custom_kring`, 3-arg.

- [ ] **Step 4: Run, verify PASS** (FOREGROUND, wait).

- [ ] **Step 5: Commit** `git commit -m "feat(gridx): custom-grid coverage ops (polyfill centroid-containment, kring)"`

---

## Task 5: Register all + rebuild JAR

**Files:** `gridx/custom/functions.scala` (new), GridX registration aggregator.

- [ ] **Step 1: Write `gridx/custom/functions.scala`** mirroring `gridx/bng/functions.scala`: an object with `def register(spark: SparkSession): Unit` that builds a `RegistryDelegate` and `rd.register(Custom_Grid)`, `rd.register(Custom_PointAsCell)`, `rd.register(Custom_AsWKB)`, `rd.register(Custom_AsWKT)`, `rd.register(Custom_Centroid)`, `rd.register(Custom_Polyfill)`, `rd.register(Custom_KRing)`. Match BNG's RegistryDelegate construction (prefix handling — note BNG names already include `gbx_bng_`; here companions' `name` already include `gbx_custom_*`, so follow BNG's exact prefix convention).

- [ ] **Step 2: Wire into GridX registration.** Find where GridX registers grids (the top-level gridx registration, or how `bng`/`quadbin` `register` are called) and add a call to `custom.functions.register(spark)`. Mirror exactly.

- [ ] **Step 3: Rebuild** (FOREGROUND, wait): `gbx:docker:exec "mvn clean package -PskipScoverage -DskipTests"` → BUILD SUCCESS.

- [ ] **Step 4: Commit** `git commit -m "feat(gridx): register gbx_custom_* functions"`

---

## Task 6: registered_functions.txt + SQL examples + function-info

- [ ] **Step 1:** Add to `docs/tests-function-info/registered_functions.txt`: `gbx_custom_grid`, `gbx_custom_pointascell`, `gbx_custom_cellaswkb`, `gbx_custom_cellaswkt`, `gbx_custom_centroid`, `gbx_custom_polyfill`, `gbx_custom_kring`.
- [ ] **Step 2:** Add a `*_sql_example()` + `_output` for each to `docs/tests/python/api/gridx_functions_sql.py` (mirror the `quadbin_*` example style; placeholder tables OK — display + structural validation). Show the grid-spec usage, e.g.:
  `SELECT gbx_custom_pointascell(geom, gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000), 5) AS cell FROM points;`
  Descriptions framed by utility (no Mosaic references).
- [ ] **Step 3: Regenerate** (FOREGROUND, wait): `gbx:docs:function-info`; confirm all 7 in `function-info.json`.
- [ ] **Step 4: Verify coverage** (FOREGROUND, wait): `gbx:test:function-info --log custom-fninfo.log` — `test_full_coverage_against_registered_list` passes; the DESCRIBE step also validates the 7 register cleanly.
- [ ] **Step 5: Commit** `git commit -m "docs: function-info examples for gbx_custom_* grid functions"`

---

## Task 7: Python bindings + tests

- [ ] **Step 1: Write failing tests** — `python/geobrix/test/gridx/custom/test_custom_grid.py` (mirror an existing gridx python test's session header). Build a grid via `gbx_custom_grid`, then: point→cell (assert a BIGINT), cell→wkb (assert binary), polyfill (assert array of cells), kring (assert array). Use the wrappers.
- [ ] **Step 2: Run, verify FAIL** (FOREGROUND, wait): `gbx:test:python --path python/geobrix/test/gridx/custom/test_custom_grid.py --log custom-py.log`.
- [ ] **Step 3: Add wrappers** in `python/geobrix/src/databricks/labs/gbx/gridx/custom/functions.py` (new; mirror the quadbin functions.py module + add to package exports as needed):
  - `custom_grid(bound_x_min, bound_x_max, bound_y_min, bound_y_max, cell_splits, root_cell_size_x, root_cell_size_y, srid=None)`
  - `custom_pointascell(geom, grid, resolution)`, `custom_cellaswkb(cell, grid)`, `custom_cellaswkt(cell, grid)`, `custom_centroid(cell, grid)`, `custom_polyfill(geom, grid, resolution)`, `custom_kring(cell, grid, k)`
  Each `return f.call_function("gbx_custom_...", _col(...), ...)`; `custom_grid` defaults srid to `f.lit(-1)` when None. Docstrings utility-framed.
- [ ] **Step 4: Run, verify PASS** (FOREGROUND, wait).
- [ ] **Step 5: Commit** `git commit -m "feat(python): gbx_custom_* grid bindings + tests"`

---

## Task 8: Full verification + push

- [ ] **Step 1: binding-parity** — `bash scripts/commands/gbx-test-bindings.sh --log custom-parity.log` → all 7 present in Scala/Python/function-info (count 154).
- [ ] **Step 2: Scala** (FOREGROUND/bg, wait): `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.*'` → 0 failures.
- [ ] **Step 3: Python:** `gbx:test:python --path python/geobrix/test/gridx/` → pass.
- [ ] **Step 4: Lint:** `gbx:lint:scalastyle` (0 errors) AND `gbx:lint:python --check` (clean — isort/black/flake8).
- [ ] **Step 5: function-info coverage** → pass.
- [ ] **Step 6: Push** (`gh auth switch --user mjohns-databricks` first): `git push origin beta/0.4.0`. QC binding-parity gates the 7.
- [ ] **Step 7:** Update `docs/docs/limitations.mdx` — remove/flip the "Custom Gridding - Not fully ported" line (now ported). Commit + (it'll go in the push, or a follow-up commit). Run `grep -rn "wave" docs/docs/` style internals-leak check is N/A; just ensure the limitations edit is utility-framed.

---

## Self-review notes (author)
- **Rationale:** utility-framed (bring-your-own grid in any CRS); no Mosaic-parity framing in plan/examples/docstrings/limitations.
- **Polyfill:** centroid-containment semantic shipped as-is (correct + standard), documented + tested; NOT rewritten to flood-fill (that's a different semantic, out of scope).
- **Coverage:** core uncomment+test (T1); struct + constructor (T2); 4 identity ops (T3); 2 coverage ops (T4); register (T5); function-info (T6); python (T7); verify incl. both lints + limitations-doc update (T8).
- **Type consistency:** all int args (resolution, k, splits, sizes, srid) read Int-or-Long tolerant via `Custom_GridSpec.asInt`; grid struct schema is the single source `Custom_GridSpec.gridStructType`; ops decode via `Custom_GridSpec.systemFromRow`.
- **Risk:** core math is pre-written/correct; main new surface is the struct-spec plumbing + op expressions (mirror BNG). The `gbx_custom_grid` struct-return expression is the least-templated piece — T2 builds + tests it first so later ops rely on a verified spec.
