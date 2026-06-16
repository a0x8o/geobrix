# gbx_rst_dtmfromgeoms (+ _agg) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire up, fix, and test the ported `gbx_rst_dtmfromgeoms` (Delaunay-TIN DTM from Z-valued points + breaklines) and ship its streaming aggregator `gbx_rst_dtmfromgeoms_agg`.

**Architecture:** A pure `RST_DTMFromGeoms.execute(points, breaklines, …)` compute path (triangulate → interpolate Z at bbox grid cell-centers → direct-fill Float64 GTiff) is shared by the non-agg expression and the `TypedImperativeAggregate` aggregator. The non-agg parses array inputs; the aggregator streams point geometries into a serializable buffer and reads breaklines/extent as per-group constants. Mirrors the existing `RST_GridFromPoints` / `RST_GridFromPointsAgg` pairing exactly.

**Tech Stack:** Scala 2.13 / Spark 4.0 Catalyst expressions, JTS (`ConformingDelaunayTriangulationBuilder`), GDAL Java bindings, PySpark `call_function` bindings. All build/test runs happen inside the `geobrix-dev` Docker container via `gbx:*` commands.

**Spec:** `docs/superpowers/specs/2026-05-28-rst-dtmfromgeoms-wireup-design.md`

**Conventions reminder:**
- Run Scala/Python/doc tests via `gbx:*` commands inside Docker (never `mvn`/`pytest` on the host).
- Long-running suites (`gbx:test:scala`, builds) should be dispatched as background work.
- After any change to Scala source, the assembly JAR is stale — `gbx:test:python` will warn; rebuild with `gbx:docker:exec "mvn clean package -PskipScoverage -DskipTests"` before the Python/doc tests.
- `gh auth switch --user mjohns-databricks` before any push.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/main/scala/com/databricks/labs/gbx/rasterx/operations/InterpolateElevation.scala` | TIN math: triangulation, Z-interpolation, **bbox-based** grid generation. NaN/out-of-hull cells skipped (not thrown). |
| `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeoms.scala` | Non-agg expression: modern bbox+pixels signature, Int+Long eval, correct `safeEval`, builder; **owns the shared pure `execute`** + `tileRow`. |
| `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeomsAgg.scala` | **New.** `TypedImperativeAggregate` streaming points; breaklines/extent are per-group constants; delegates to `RST_DTMFromGeoms.execute`. |
| `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/DTMFromGeomsAcc.scala` | **New.** Serializable point-WKB accumulation buffer for the aggregator. |
| `src/main/scala/com/databricks/labs/gbx/rasterx/functions.scala` | Register both functions. |
| `pom.xml` | Remove the two scoverage `excludedFiles` entries. |
| `docs/tests-function-info/registered_functions.txt` | Add both canonical names. |
| `docs/tests/python/api/rasterx_functions_sql.py` | A `*_sql_example()` + `_output` for each. |
| `src/main/resources/com/databricks/labs/gbx/function-info.json` | Regenerated. |
| `python/geobrix/src/databricks/labs/gbx/rasterx/functions.py` | `rst_dtmfromgeoms` + `rst_dtmfromgeoms_agg` wrappers. |
| `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeomsTest.scala` | **New.** Known-plane, breakline, out-of-hull, validation, agg≡non-agg, buffer roundtrip. |
| `python/geobrix/test/rasterx/test_dtmfromgeoms.py` | **New.** Python binding smoke tests for both. |
| `docs/tests/python/api/` SQL doc test wiring | New SQL doc examples execute under Docker. |

---

## Task 1: bbox grid + non-throwing interpolation in `InterpolateElevation`

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/operations/InterpolateElevation.scala`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/operations/InterpolateElevationTest.scala` (create)

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/databricks/labs/gbx/rasterx/operations/InterpolateElevationTest.scala`:

```scala
package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, LineString}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class InterpolateElevationTest extends AnyFunSuite {

    private val gf = new GeometryFactory()

    /** z = 2*x + 3*y + 5 sampled at the 4 corners of a 100x100 extent. */
    private def planePoints() = Seq(
        JTS.point(new Coordinate(0.0,   0.0,   2 * 0.0   + 3 * 0.0   + 5)),
        JTS.point(new Coordinate(100.0, 0.0,   2 * 100.0 + 3 * 0.0   + 5)),
        JTS.point(new Coordinate(0.0,   100.0, 2 * 0.0   + 3 * 100.0 + 5)),
        JTS.point(new Coordinate(100.0, 100.0, 2 * 100.0 + 3 * 100.0 + 5))
    )

    test("pointGridBBox emits widthPx*heightPx cell centers inside the extent") {
        val grid = InterpolateElevation.pointGridBBox(0.0, 0.0, 100.0, 100.0, 10, 10, 32633)
        grid.getNumGeometries shouldBe 100
        // first cell center is at (xmin + xRes/2, ymin + yRes/2) = (5, 5)
        val p0 = grid.getGeometryN(0)
        p0.getCoordinate.x shouldBe 5.0 +- 1e-9
        p0.getCoordinate.y shouldBe 5.0 +- 1e-9
    }

    test("interpolate reproduces a planar surface exactly (linear TIN)") {
        val mp = JTS.multiPoint(planePoints().toArray)
        val grid = InterpolateElevation.pointGridBBox(0.0, 0.0, 100.0, 100.0, 10, 10, 32633)
        val out = InterpolateElevation.interpolate(mp, Seq.empty[LineString], grid, 0.0, 0.0)
        out should not be empty
        out.foreach { p =>
            val expected = 2 * p.getX + 3 * p.getY + 5
            p.getCoordinate.getZ shouldBe expected +- 1e-6
        }
    }

    test("interpolate skips (does not throw on) points outside the convex hull") {
        val mp = JTS.multiPoint(planePoints().toArray)
        // Grid extends well beyond the 100x100 point hull; outer cells have no triangle.
        val grid = InterpolateElevation.pointGridBBox(-50.0, -50.0, 150.0, 150.0, 20, 20, 32633)
        noException should be thrownBy {
            InterpolateElevation.interpolate(mp, Seq.empty[LineString], grid, 0.0, 0.0)
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Dispatch (background, Docker):
```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.operations.InterpolateElevationTest' --log dtm-interp.log
```
Expected: FAIL — `pointGridBBox` does not exist (compile error) / current `interpolate` throws on NaN.

- [ ] **Step 3: Add `pointGridBBox` and make `interpolate` skip NaN**

In `InterpolateElevation.scala`, add the bbox grid method (keep the existing `pointGrid` for now or remove it — it is only used by the old `eval`, which Task 3 rewrites; remove it in Task 3):

```scala
    /** Regular grid of cell-center points over a bbox, row-major by column then row.
     *  Cell size is derived: xRes = (xmax-xmin)/widthPx, yRes = (ymax-ymin)/heightPx.
     *  Centers: x = xmin + (i + 0.5)*xRes, y = ymin + (j + 0.5)*yRes.
     */
    def pointGridBBox(
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, srid: Int
    ): MultiPoint = {
        val xRes = (xmax - xmin) / widthPx
        val yRes = (ymax - ymin) / heightPx
        val pts = for (i <- 0 until widthPx; j <- 0 until heightPx) yield {
            val x = xmin + (i + 0.5) * xRes
            val y = ymin + (j + 0.5) * yRes
            val p = JTS.point(new Coordinate(x, y))
            p.setSRID(srid)
            p
        }
        JTS.multiPoint(pts.toArray)
    }
```

Change the tail of `interpolate` from a throwing `.map` to a skipping `.flatMap`:

```scala
            .flatMap({ case (point: Point, poly: Polygon) =>
                val polyCoords = poly.getCoordinates
                val tri = new Triangle(polyCoords(0), polyCoords(1), polyCoords(2))
                val z = tri.interpolateZ(point.getCoordinate)
                if (z.isNaN) {
                    None // cell with degenerate triangle -> caller treats as no_data
                } else {
                    val ip = JTS.point(new Coordinate(point.getX, point.getY, z))
                    ip.setSRID(multipoint.getSRID)
                    Some(ip)
                }
            })
            .toSeq
```

(Replace the existing `.map({ case (point, poly) => … }).toSeq` block; the `if (z.isNaN) { throw … }` line is removed.)

- [ ] **Step 4: Run test to verify it passes**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.operations.InterpolateElevationTest' --log dtm-interp.log
```
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/operations/InterpolateElevation.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/operations/InterpolateElevationTest.scala
git commit -m "feat(rasterx): bbox grid + non-throwing interpolation in InterpolateElevation"
```

---

## Task 2: Shared `RST_DTMFromGeoms.execute` (direct-fill rasterize)

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeoms.scala`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeomsTest.scala` (create)

This task adds the pure `execute` + `tileRow` to the companion object. The expression-class rework (signature, eval entry points) is Task 3 — keep this task focused on the compute path so it can be tested in isolation by direct call.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeomsTest.scala`:

```scala
package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.gdal.gdal.gdal
import org.locationtech.jts.geom.{Coordinate, Geometry, LineString}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

class RST_DTMFromGeomsTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    /** z = 2*x + 3*y + 5 sampled at the 4 corners of a 100x100 extent (EPSG:32633). */
    private def planePoints(): Seq[Geometry] = Seq(
        JTS.point(new Coordinate(0.0,   0.0,   5.0)),
        JTS.point(new Coordinate(100.0, 0.0,   205.0)),
        JTS.point(new Coordinate(0.0,   100.0, 305.0)),
        JTS.point(new Coordinate(100.0, 100.0, 505.0))
    )

    /** Read a single pixel value (col,row) from the GTiff bytes in a tile row. */
    private def pixel(row: InternalRow, col: Int, r: Int): Double = {
        val bytes = row.getBinary(1)
        bytes should not be null
        val tmp = s"/vsimem/dtm_readback_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        gdal.FileFromMemBuffer(tmp, bytes)
        val ds = gdal.Open(tmp)
        try {
            val buf = new Array[Double](1)
            ds.GetRasterBand(1).ReadRaster(col, r, 1, 1, buf)
            buf(0)
        } finally { ds.delete(); gdal.Unlink(tmp) }
    }

    test("execute reproduces the planar surface at cell centers") {
        val row = RST_DTMFromGeoms.execute(
            planePoints(), Seq.empty[LineString],
            mergeTolerance = 0.0, snapTolerance = 0.0,
            xmin = 0.0, ymin = 0.0, xmax = 100.0, ymax = 100.0,
            widthPx = 10, heightPx = 10, srid = 32633, noData = -9999.0
        )
        row should not be null
        // Pixel (col=0,row=0) is the top-left cell. Its center is x=5, y=95 (row 0 = max y).
        // Expected z = 2*5 + 3*95 + 5 = 300.
        pixel(row, 0, 0) shouldBe 300.0 +- 1e-3
        // Pixel (col=9,row=9): center x=95, y=5 -> z = 2*95 + 3*5 + 5 = 210.
        pixel(row, 9, 9) shouldBe 210.0 +- 1e-3
    }

    test("execute writes no_data for cells outside the point hull") {
        val row = RST_DTMFromGeoms.execute(
            planePoints(), Seq.empty[LineString],
            0.0, 0.0,
            xmin = -100.0, ymin = -100.0, xmax = 200.0, ymax = 200.0,
            widthPx = 30, heightPx = 30, srid = 32633, noData = -9999.0
        )
        // top-left corner cell center (~ -95, 195) is far outside the 0..100 hull.
        pixel(row, 0, 0) shouldBe -9999.0 +- 1e-6
    }

    test("execute honors a breakline without throwing") {
        val bl = JTS.fromWKT("LINESTRING (0 50, 100 50)").asInstanceOf[LineString]
        noException should be thrownBy {
            RST_DTMFromGeoms.execute(
                planePoints(), Seq(bl), 0.0, 0.01,
                0.0, 0.0, 100.0, 100.0, 10, 10, 32633, -9999.0)
        }
    }

    test("execute rejects degenerate extents and non-positive dims") {
        an[IllegalArgumentException] should be thrownBy {
            RST_DTMFromGeoms.execute(planePoints(), Seq.empty, 0.0, 0.0, 0.0, 0.0, 0.0, 100.0, 10, 10, 32633, -9999.0)
        }
        an[IllegalArgumentException] should be thrownBy {
            RST_DTMFromGeoms.execute(planePoints(), Seq.empty, 0.0, 0.0, 0.0, 0.0, 100.0, 100.0, 0, 10, 32633, -9999.0)
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_DTMFromGeomsTest' --log dtm-exec.log
```
Expected: FAIL — `RST_DTMFromGeoms.execute` does not exist (compile error).

- [ ] **Step 3: Add `execute` + `tileRow` to the companion**

In `RST_DTMFromGeoms.scala`, add these imports if missing:

```scala
import com.databricks.labs.gbx.rasterx.util.VectorRasterBridge
import com.databricks.labs.gbx.util.SerializationUtil
import org.locationtech.jts.geom.Geometry
```

Add to `object RST_DTMFromGeoms`:

```scala
    /** Pure compute path shared by the non-agg expression and the aggregator.
     *  Builds a constrained-Delaunay TIN from `points` (+ optional `breaklines`),
     *  interpolates Z at the bbox cell centers, and writes a single-band Float64
     *  GTiff tile. Cells outside the triangulated hull are `noData`.
     */
    def execute(
        points: Seq[Geometry],
        breaklines: Seq[LineString],
        mergeTolerance: Double,
        snapTolerance: Double,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, srid: Int,
        noData: Double
    ): InternalRow = {
        require(widthPx > 0,  s"rst_dtmfromgeoms: width_px must be positive; got $widthPx")
        require(heightPx > 0, s"rst_dtmfromgeoms: height_px must be positive; got $heightPx")
        require(xmax > xmin,  s"rst_dtmfromgeoms: xmax ($xmax) must be > xmin ($xmin)")
        require(ymax > ymin,  s"rst_dtmfromgeoms: ymax ($ymax) must be > ymin ($ymin)")
        require(points.nonEmpty, "rst_dtmfromgeoms: at least one point is required")

        val mp = JTS.multiPoint(points.toArray)
        mp.setSRID(srid)
        val grid = InterpolateElevation.pointGridBBox(xmin, ymin, xmax, ymax, widthPx, heightPx, srid)
        val interpolated = InterpolateElevation.interpolate(mp, breaklines, grid, mergeTolerance, snapTolerance)

        val ds = VectorRasterBridge.buildEmptyRaster(xmin, ymin, xmax, ymax, widthPx, heightPx, srid, noData)
        try {
            val xRes = (xmax - xmin) / widthPx
            val yRes = (ymax - ymin) / heightPx
            val arr = Array.fill[Double](widthPx * heightPx)(noData)
            interpolated.foreach { p =>
                val col = math.floor((p.getX - xmin) / xRes).toInt
                val r   = math.floor((ymax - p.getY) / yRes).toInt
                if (col >= 0 && col < widthPx && r >= 0 && r < heightPx) {
                    arr(r * widthPx + col) = p.getCoordinate.getZ
                }
            }
            ds.GetRasterBand(1).WriteRaster(0, 0, widthPx, heightPx, arr)
            ds.FlushCache()
            tileRow(VectorRasterBridge.toGTiffBytes(ds))
        } finally {
            ds.delete()
        }
    }

    /** Build the (index_id, raster, metadata) tile row downstream serializers expect. */
    def tileRow(bytes: Array[Byte]): InternalRow = {
        val mtd = Map(
            "driver" -> "GTiff",
            "extension" -> "tif",
            "size" -> bytes.length.toString,
            "parentPath" -> "",
            "all_parents" -> "",
            "last_command" -> "gbx_rst_dtmfromgeoms"
        )
        InternalRow.fromSeq(Seq(0L, bytes, SerializationUtil.toMapData[String, String](mtd)))
    }
```

- [ ] **Step 4: Run test to verify it passes**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_DTMFromGeomsTest' --log dtm-exec.log
```
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeoms.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeomsTest.scala
git commit -m "feat(rasterx): shared RST_DTMFromGeoms.execute with direct-fill rasterize"
```

---

## Task 3: Rework the `RST_DTMFromGeoms` expression (signature, eval, builder)

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeoms.scala`
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/operations/InterpolateElevation.scala` (remove now-dead old `pointGrid`)
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeomsTest.scala` (extend)

- [ ] **Step 1: Write the failing test** (append to `RST_DTMFromGeomsTest.scala`)

```scala
    test("builder accepts 11 args (no_data defaulted) and 12 args") {
        val lit = (v: Any) => org.apache.spark.sql.catalyst.expressions.Literal(v)
        val base = Seq[org.apache.spark.sql.catalyst.expressions.Expression](
            lit(null), lit(null), lit(0.0), lit(0.0),
            lit(0.0), lit(0.0), lit(100.0), lit(100.0),
            lit(10), lit(10), lit(32633)
        )
        // 11 args -> no_data defaulted, builds without error.
        RST_DTMFromGeoms.builder()(base) shouldBe a[RST_DTMFromGeoms]
        // 12 args -> explicit no_data.
        RST_DTMFromGeoms.builder()(base :+ lit(-1.0)) shouldBe a[RST_DTMFromGeoms]
        // wrong arity -> error.
        an[IllegalArgumentException] should be thrownBy { RST_DTMFromGeoms.builder()(base.take(5)) }
    }
```

- [ ] **Step 2: Run test to verify it fails**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_DTMFromGeomsTest' --log dtm-exec.log
```
Expected: FAIL — current builder takes the old 11-positional shape and there is no `no_data` default.

- [ ] **Step 3: Replace the case class and companion `eval`/`builder`**

Replace the whole `case class RST_DTMFromGeoms(...)` and the `eval`/`builder`/`name` parts of the companion with the modern form (keep the `execute`/`tileRow` from Task 2). Use `RST_GridFromPoints` as the structural template.

Case class:

```scala
case class RST_DTMFromGeoms(
    pointsArray: Expression,
    breaklinesArray: Expression,
    mergeTolerance: Expression,
    snapTolerance: Expression,
    xminExpr: Expression,
    yminExpr: Expression,
    xmaxExpr: Expression,
    ymaxExpr: Expression,
    widthPxExpr: Expression,
    heightPxExpr: Expression,
    sridExpr: Expression,
    noDataExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        pointsArray, breaklinesArray, mergeTolerance, snapTolerance,
        xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthPxExpr, heightPxExpr, sridExpr, noDataExpr,
        ExpressionConfigExpr()
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(BinaryType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_DTMFromGeoms.name
    override def replacement: Expression = invoke(RST_DTMFromGeoms)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9), nc(10), nc(11))
}
```

Companion `eval` (two arity-on-int entry points) + `doInvoke` + `builder` + `name`:

```scala
    import org.apache.spark.sql.catalyst.expressions.Literal

    /** Default no-data sentinel (matches RST_GridFromPoints). */
    val DefaultNoData: Double = -9999.0

    // Int-args entry (Catalyst / SQL literals).
    def eval(
        pointsArray: ArrayData, breaklinesArray: ArrayData,
        mergeTolerance: Double, snapTolerance: Double,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, srid: Int, noData: Double,
        conf: UTF8String
    ): InternalRow = doInvoke(
        pointsArray, breaklinesArray, mergeTolerance, snapTolerance,
        xmin, ymin, xmax, ymax, widthPx, heightPx, srid, noData, conf)

    // Long-args entry (PySpark passes Python ints as Long).
    def eval(
        pointsArray: ArrayData, breaklinesArray: ArrayData,
        mergeTolerance: Double, snapTolerance: Double,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Long, heightPx: Long, srid: Long, noData: Double,
        conf: UTF8String
    ): InternalRow = doInvoke(
        pointsArray, breaklinesArray, mergeTolerance, snapTolerance,
        xmin, ymin, xmax, ymax, widthPx.toInt, heightPx.toInt, srid.toInt, noData, conf)

    private def doInvoke(
        pointsArray: ArrayData, breaklinesArray: ArrayData,
        mergeTolerance: Double, snapTolerance: Double,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, srid: Int, noData: Double,
        conf: UTF8String
    ): InternalRow =
        Option(
            RST_ErrorHandler.safeEval(
                () => {
                    val exprConf = ExpressionConfig.fromB64(conf.toString)
                    RST_ExpressionUtil.init(exprConf)
                    if (pointsArray == null) return null
                    val pts = JTS.fromArrayData(pointsArray, pointsArray.getClass; ???)
                    null // replaced below
                },
                null, BinaryType, conf
            )
        ).map(_.asInstanceOf[InternalRow]).orNull
```

> NOTE: decoding `ArrayData` needs the element `DataType`, which the expression knows from
> `pointsArray.dataType` / `breaklinesArray.dataType` (the case-class fields), not the companion.
> So decode in the **case class** (where the field types are available) and pass decoded
> sequences down, OR pass the element types into `doInvoke`. Use the latter to keep `execute`
> reuse clean. Concretely, change the case class to compute element types and the companion
> `eval` to receive them is awkward; instead decode in `doInvoke` using `JTS.fromArrayData`
> with the element type carried via the array's own struct. The original code used
> `JTS.fromArrayData(pointsArray, pdt)` where `pdt` came from the expression. Mirror that by
> having the **expression** override `eval`-routing through `invoke` with the element types
> appended — but simpler: decode using the WKB/WKT element inspection helper below, which needs
> no external DataType.

Replace the `doInvoke` body's decoding with a self-describing decoder (no external DataType needed), mirroring `RST_GridFromPoints.geomsFromArrayData`:

```scala
    private def doInvoke(
        pointsArray: ArrayData, breaklinesArray: ArrayData,
        mergeTolerance: Double, snapTolerance: Double,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, srid: Int, noData: Double,
        conf: UTF8String
    ): InternalRow =
        Option(
            RST_ErrorHandler.safeEval(
                () => {
                    val exprConf = ExpressionConfig.fromB64(conf.toString)
                    RST_ExpressionUtil.init(exprConf)
                    if (pointsArray == null) return null
                    val pts = geomsFromArrayData(pointsArray).toSeq
                    val lines = (if (breaklinesArray == null) Seq.empty[Geometry]
                                 else geomsFromArrayData(breaklinesArray).toSeq)
                        .map(_.asInstanceOf[LineString])
                    execute(pts, lines, mergeTolerance, snapTolerance,
                        xmin, ymin, xmax, ymax, widthPx, heightPx, srid, noData)
                },
                null, BinaryType, conf
            )
        ).map(_.asInstanceOf[InternalRow]).orNull

    /** Decode an ARRAY of point/line geometries; element may be BINARY (WKB) or STRING (WKT). */
    private def geomsFromArrayData(data: ArrayData): Array[Geometry] = {
        val n = data.numElements()
        val out = new Array[Geometry](n)
        var i = 0
        while (i < n) {
            if (!data.isNullAt(i)) {
                out(i) = data.get(i, null) match {
                    case b: Array[Byte] => JTS.fromWKB(b)
                    case s: UTF8String  => JTS.fromWKT(s.toString)
                    case other          => throw new IllegalArgumentException(
                        "rst_dtmfromgeoms: geometry array element must be BINARY (WKB) or STRING (WKT); " +
                        s"got ${if (other == null) "null" else other.getClass.getName}")
                }
            }
            i += 1
        }
        out.filter(_ != null)
    }

    override def name: String = "gbx_rst_dtmfromgeoms"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 11 => RST_DTMFromGeoms(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10),
            Literal(DefaultNoData))
        case 12 => RST_DTMFromGeoms(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_dtmfromgeoms takes 11 or 12 arguments (points, breaklines, merge_tolerance, " +
            s"snap_tolerance, xmin, ymin, xmax, ymax, width_px, height_px, srid, [no_data]); got $n")
    }
```

Remove the old single packed-tuple `eval`, the `firstElementType`/`secondElementType` helpers, the `splitPointFinder`/`gridOrigin`/`gridWidth*`/`gridSize*` fields, and the unused imports (`ArrayData` stays; remove `UTF8String`-only-for-origin usages as needed — keep what compiles). Update the header comment to describe the registered modern signature (drop "Not yet implemented for production").

In `InterpolateElevation.scala`, delete the now-unused old `def pointGrid(origin: Point, …)` (superseded by `pointGridBBox`). The `TriangulationSplitPointTypeEnum` object is also now unused — remove it.

- [ ] **Step 4: Run test to verify it passes**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_DTMFromGeomsTest' --log dtm-exec.log
```
Expected: PASS (5 tests incl. builder arity).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeoms.scala \
        src/main/scala/com/databricks/labs/gbx/rasterx/operations/InterpolateElevation.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeomsTest.scala
git commit -m "feat(rasterx): modern bbox+pixels signature, Int/Long eval, safeEval fix for rst_dtmfromgeoms"
```

---

## Task 4: Aggregator `RST_DTMFromGeomsAgg` + `DTMFromGeomsAcc`

**Files:**
- Create: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/DTMFromGeomsAcc.scala`
- Create: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeomsAgg.scala`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeomsTest.scala` (extend)

- [ ] **Step 1: Write the failing test** (append to `RST_DTMFromGeomsTest.scala`)

```scala
    test("DTMFromGeomsAcc serialize/deserialize roundtrips point WKBs") {
        val buf = DTMFromGeomsAcc.empty
        planePoints().foreach(p => buf.add(JTS.toWKB(p)))
        val restored = DTMFromGeomsAcc.deserialize(buf.serialize)
        restored.points.length shouldBe 4
        restored.points.zip(buf.points).foreach { case (a, b) => a shouldBe b }
    }

    test("RST_DTMFromGeomsAgg produces the same raster as the non-agg execute") {
        val lit = (v: Any) => org.apache.spark.sql.catalyst.expressions.Literal(v)
        val buf = DTMFromGeomsAcc.empty
        planePoints().foreach(p => buf.add(JTS.toWKB(p)))
        val agg = RST_DTMFromGeomsAgg(
            pointExpr = null,
            breaklinesExpr = lit(null),
            mergeToleranceExpr = lit(0.0), snapToleranceExpr = lit(0.0),
            xminExpr = lit(0.0), yminExpr = lit(0.0), xmaxExpr = lit(100.0), ymaxExpr = lit(100.0),
            widthPxExpr = lit(10), heightPxExpr = lit(10), sridExpr = lit(32633),
            noDataExpr = lit(-9999.0)
        )
        val aggRow = agg.eval(buf).asInstanceOf[InternalRow]
        val nonAggRow = RST_DTMFromGeoms.execute(
            planePoints(), Seq.empty[LineString], 0.0, 0.0,
            0.0, 0.0, 100.0, 100.0, 10, 10, 32633, -9999.0)
        pixel(aggRow, 0, 0) shouldBe pixel(nonAggRow, 0, 0) +- 1e-9
        pixel(aggRow, 9, 9) shouldBe pixel(nonAggRow, 9, 9) +- 1e-9
    }
```

- [ ] **Step 2: Run test to verify it fails**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_DTMFromGeomsTest' --log dtm-agg.log
```
Expected: FAIL — `DTMFromGeomsAcc` / `RST_DTMFromGeomsAgg` do not exist.

- [ ] **Step 3a: Create `DTMFromGeomsAcc.scala`**

```scala
package com.databricks.labs.gbx.rasterx.expressions

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import scala.collection.mutable.ArrayBuffer

/** Mutable aggregation buffer for [[RST_DTMFromGeomsAgg]]: accumulates point WKB byte
 *  arrays (Z carried in the geometry). Shipped between executors via serialize/deserialize.
 */
final class DTMFromGeomsAcc(
    val points: ArrayBuffer[Array[Byte]] = ArrayBuffer.empty,
    private var byteSize: Long = 0L
) extends Serializable {

    def add(wkb: Array[Byte]): DTMFromGeomsAcc = {
        if (wkb != null && wkb.length > 0) {
            points += wkb
            byteSize += wkb.length.toLong
            DTMFromGeomsAcc.guardSize(byteSize)
        }
        this
    }

    def merge(other: DTMFromGeomsAcc): DTMFromGeomsAcc = {
        points ++= other.points
        byteSize += other.byteSize
        DTMFromGeomsAcc.guardSize(byteSize)
        this
    }

    def serialize: Array[Byte] = {
        val bos = new ByteArrayOutputStream()
        val out = new DataOutputStream(bos)
        out.writeInt(points.length)
        for (wkb <- points) { out.writeInt(wkb.length); out.write(wkb) }
        bos.toByteArray
    }
}

object DTMFromGeomsAcc {

    /** Hard cap on accumulated WKB bytes per buffer (guards memory blow-ups). */
    val MAX_BUFFER_BYTES: Long = 200L * 1024L * 1024L

    def empty: DTMFromGeomsAcc = new DTMFromGeomsAcc()

    def deserialize(bytes: Array[Byte]): DTMFromGeomsAcc = {
        val in = new DataInputStream(new ByteArrayInputStream(bytes))
        val n = in.readInt()
        val buf = ArrayBuffer.empty[Array[Byte]]
        var total = 0L
        var i = 0
        while (i < n) {
            val len = in.readInt()
            val wkb = new Array[Byte](len)
            if (len > 0) in.readFully(wkb)
            buf += wkb
            total += len.toLong
            i += 1
        }
        new DTMFromGeomsAcc(buf, total)
    }

    private[expressions] def guardSize(currentBytes: Long): Unit = {
        if (currentBytes > MAX_BUFFER_BYTES) {
            throw new IllegalStateException(
                s"rst_dtmfromgeoms_agg buffer exceeded ${MAX_BUFFER_BYTES / (1024 * 1024)} MiB " +
                s"(current = ${currentBytes / (1024 * 1024)} MiB). Tile the workload by extent.")
        }
    }
}
```

- [ ] **Step 3b: Create `RST_DTMFromGeomsAgg.scala`** (mirror `RST_GridFromPointsAgg`)

```scala
package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Geometry, LineString}

/** UDAF: `gbx_rst_dtmfromgeoms_agg(point, breaklines, merge_tolerance, snap_tolerance,
 *  xmin, ymin, xmax, ymax, width_px, height_px, srid, [no_data])`.
 *
 *  Streams one Z-valued `point` per row into a buffer; every other argument is a
 *  per-group constant (read once in `eval`). Breaklines arrive as a constant ARRAY.
 *  Delegates to [[RST_DTMFromGeoms.execute]] so the result equals the non-agg form.
 */
final case class RST_DTMFromGeomsAgg(
    pointExpr: Expression,
    breaklinesExpr: Expression,
    mergeToleranceExpr: Expression,
    snapToleranceExpr: Expression,
    xminExpr: Expression, yminExpr: Expression, xmaxExpr: Expression, ymaxExpr: Expression,
    widthPxExpr: Expression, heightPxExpr: Expression, sridExpr: Expression,
    noDataExpr: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[DTMFromGeomsAcc] {

    import RST_DTMFromGeomsAgg.{evalDouble, evalInt, evalExpr, geomsFromArrayData}

    override lazy val deterministic: Boolean = true
    override val nullable: Boolean = true
    override val dataType: DataType = StructType(Seq(
        StructField("index_id", LongType, nullable = true),
        StructField("raster", BinaryType, nullable = true),
        StructField("metadata", MapType(StringType, StringType), nullable = true)
    ))
    override def prettyName: String = RST_DTMFromGeomsAgg.name

    override def children: Seq[Expression] = Seq(
        pointExpr, breaklinesExpr, mergeToleranceExpr, snapToleranceExpr,
        xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthPxExpr, heightPxExpr, sridExpr, noDataExpr)

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): RST_DTMFromGeomsAgg = {
        require(nc.length == 12, s"RST_DTMFromGeomsAgg expects 12 children; got ${nc.length}")
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9), nc(10), nc(11))
    }

    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate = copy(mutableAggBufferOffset = n)
    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate = copy(inputAggBufferOffset = n)

    override def createAggregationBuffer(): DTMFromGeomsAcc = DTMFromGeomsAcc.empty

    override def update(buffer: DTMFromGeomsAcc, input: InternalRow): DTMFromGeomsAcc = {
        val pt = evalExpr(pointExpr, input)
        if (pt == null) return buffer
        val wkb = pt match {
            case b: Array[Byte] => b
            case s: UTF8String  => JTS.toWKB(JTS.fromWKT(s.toString))
            case other          => throw new IllegalArgumentException(
                s"rst_dtmfromgeoms_agg: point column must be BINARY (WKB) or STRING (WKT); got ${other.getClass.getName}")
        }
        buffer.add(wkb)
    }

    override def merge(a: DTMFromGeomsAcc, b: DTMFromGeomsAcc): DTMFromGeomsAcc = a.merge(b)

    override def eval(buffer: DTMFromGeomsAcc): Any = {
        val empty = InternalRow.empty
        val breaklines: Seq[LineString] = evalExpr(breaklinesExpr, empty) match {
            case null              => Seq.empty
            case ad: ArrayData     => geomsFromArrayData(ad).map(_.asInstanceOf[LineString]).toSeq
            case other             => throw new IllegalArgumentException(
                s"rst_dtmfromgeoms_agg: breaklines must be an ARRAY of geometries; got ${other.getClass.getName}")
        }
        val points: Seq[Geometry] = buffer.points.toSeq.map(JTS.fromWKB)
        RST_DTMFromGeoms.execute(
            points, breaklines,
            evalDouble(mergeToleranceExpr, empty, "merge_tolerance"),
            evalDouble(snapToleranceExpr, empty, "snap_tolerance"),
            evalDouble(xminExpr, empty, "xmin"), evalDouble(yminExpr, empty, "ymin"),
            evalDouble(xmaxExpr, empty, "xmax"), evalDouble(ymaxExpr, empty, "ymax"),
            evalInt(widthPxExpr, empty, "width_px"), evalInt(heightPxExpr, empty, "height_px"),
            evalInt(sridExpr, empty, "srid"),
            evalDouble(noDataExpr, empty, "no_data"))
    }

    override def serialize(b: DTMFromGeomsAcc): Array[Byte] = b.serialize
    override def deserialize(bytes: Array[Byte]): DTMFromGeomsAcc = DTMFromGeomsAcc.deserialize(bytes)
}

object RST_DTMFromGeomsAgg extends WithExpressionInfo {

    override def name: String = "gbx_rst_dtmfromgeoms_agg"

    private[expressions] def evalExpr(e: Expression, row: InternalRow): Any = e.eval(row)

    private[expressions] def geomsFromArrayData(data: ArrayData): Array[Geometry] = {
        val n = data.numElements()
        val out = scala.collection.mutable.ArrayBuffer.empty[Geometry]
        var i = 0
        while (i < n) {
            if (!data.isNullAt(i)) {
                out += (data.get(i, null) match {
                    case b: Array[Byte] => JTS.fromWKB(b)
                    case s: UTF8String  => JTS.fromWKT(s.toString)
                    case other          => throw new IllegalArgumentException(
                        s"rst_dtmfromgeoms_agg: breakline element must be BINARY/STRING; got ${other.getClass.getName}")
                })
            }
            i += 1
        }
        out.toArray
    }

    private[expressions] def evalDouble(e: Expression, row: InternalRow, label: String): Double =
        evalExpr(e, row) match {
            case null => throw new IllegalArgumentException(s"rst_dtmfromgeoms_agg: $label must not be null")
            case d: Double => d
            case f: Float => f.toDouble
            case i: Int => i.toDouble
            case l: Long => l.toDouble
            case dec: org.apache.spark.sql.types.Decimal => dec.toDouble
            case o => throw new IllegalArgumentException(s"rst_dtmfromgeoms_agg: $label must be numeric; got ${o.getClass.getName}")
        }

    private[expressions] def evalInt(e: Expression, row: InternalRow, label: String): Int =
        evalExpr(e, row) match {
            case null => throw new IllegalArgumentException(s"rst_dtmfromgeoms_agg: $label must not be null")
            case i: Int => i
            case l: Long => l.toInt
            case o => throw new IllegalArgumentException(s"rst_dtmfromgeoms_agg: $label must be INT or LONG; got ${o.getClass.getName}")
        }

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 11 => RST_DTMFromGeomsAgg(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10),
            Literal(RST_DTMFromGeoms.DefaultNoData))
        case 12 => RST_DTMFromGeomsAgg(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11))
        case n => throw new IllegalArgumentException(
            s"$name takes 11 or 12 arguments (point, breaklines, merge_tolerance, snap_tolerance, " +
            s"xmin, ymin, xmax, ymax, width_px, height_px, srid, [no_data]); got $n")
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_DTMFromGeomsTest' --log dtm-agg.log
```
Expected: PASS (7 tests incl. agg≡non-agg + buffer roundtrip).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/DTMFromGeomsAcc.scala \
        src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeomsAgg.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeomsTest.scala
git commit -m "feat(rasterx): streaming RST_DTMFromGeomsAgg aggregator (agg == non-agg)"
```

---

## Task 5: Register both functions; remove scoverage exclusions

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/functions.scala`
- Modify: `pom.xml`

- [ ] **Step 1: Uncomment + add registrations**

In `functions.scala`, replace the line `//        rd.register(RST_DTMFromGeoms)` with:

```scala
        rd.register(RST_DTMFromGeoms)
```

Add the aggregator registration alongside the other aggregators (near `RST_DerivedBandAgg` / the agg grouping):

```scala
        rd.register(RST_DTMFromGeomsAgg)
```

Both expressions are in package `...rasterx.expressions`; add imports if the file imports expressions individually (follow the existing import style in `functions.scala`).

- [ ] **Step 2: Remove scoverage exclusions**

In `pom.xml`, in **both** `<excludedFiles>` entries (lines ~466 and ~508), remove the
`.*RST_DTMFromGeoms\.scala;.*InterpolateElevation\.scala` portions. If they are the only
entries, set the element to empty (`<excludedFiles></excludedFiles>`); if combined with others
via `;`, remove just these two patterns and their separators.

- [ ] **Step 3: Build to verify registration compiles and resolves**

Rebuild the JAR (this also refreshes the stale JAR for later Python/doc tests):
```
gbx:docker:exec "mvn clean package -PskipScoverage -DskipTests"
```
Expected: BUILD SUCCESS.

- [ ] **Step 4: Quick registration smoke test (optional but cheap)**

```
gbx:docker:exec "echo 'spark not needed'"   # registration is exercised by function-info in Task 6
```
(There is no standalone registration unit test in this repo; Task 6's `gbx:test:function-info` is the registration gate.)

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/functions.scala pom.xml
git commit -m "feat(rasterx): register rst_dtmfromgeoms + _agg; drop scoverage exclusions"
```

---

## Task 6: registered_functions.txt + SQL doc examples + regenerate function-info

**Files:**
- Modify: `docs/tests-function-info/registered_functions.txt`
- Modify: `docs/tests/python/api/rasterx_functions_sql.py`
- Regenerated: `src/main/resources/com/databricks/labs/gbx/function-info.json`

- [ ] **Step 1: Add the two canonical names**

Add to `docs/tests-function-info/registered_functions.txt` (place near the other `gbx_rst_*`
operations / aggregators; exact position is not significant — the parity check is set-based):

```
gbx_rst_dtmfromgeoms
gbx_rst_dtmfromgeoms_agg
```

- [ ] **Step 2: Add SQL doc examples**

Append to `docs/tests/python/api/rasterx_functions_sql.py`:

```python
def rst_dtmfromgeoms_sql_example():
    """DTM via Delaunay-TIN interpolation from Z-valued points (+ optional breaklines)."""
    return """
-- TIN interpolation from arrays of Z-valued point WKB and breakline WKB.
-- Output is a 100 x 100 Float64 GTiff over the extent. For N-metre cells set
-- width_px = round((xmax-xmin)/N): here a 1000 m extent at 10 m cells -> 100 px.
SELECT gbx_rst_dtmfromgeoms(
    points_wkb_array, breaklines_wkb_array,
    0.0, 0.01,
    0.0, 0.0, 1000.0, 1000.0,
    100, 100, 32633
) AS dtm
FROM survey_points;
"""


rst_dtmfromgeoms_sql_example_output = """
+---+
|dtm|
+---+
|...|
+---+
"""


def rst_dtmfromgeoms_agg_sql_example():
    """DTM aggregator - one Z-valued point per row, grouped by extent key."""
    return """
-- Stream survey points per region into one TIN DTM tile. Breaklines are a
-- per-group constant array; for 10 m cells over a 1000 m extent use 100 px.
SELECT region_id,
    gbx_rst_dtmfromgeoms_agg(
        point_wkb, breaklines_wkb_array,
        0.0, 0.01,
        bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax,
        100, 100, 32633
    ) AS dtm
FROM survey_points
GROUP BY region_id;
"""


rst_dtmfromgeoms_agg_sql_example_output = """
+---------+---+
|region_id|dtm|
+---------+---+
|...      |...|
+---------+---+
"""
```

- [ ] **Step 3: Regenerate function-info.json**

```
gbx:docs:function-info
```
Expected: regenerates `function-info.json`; both `gbx_rst_dtmfromgeoms` and
`gbx_rst_dtmfromgeoms_agg` now appear as keys with non-empty usage.

- [ ] **Step 4: Verify function-info coverage**

```
gbx:test:function-info --log dtm-fninfo.log
```
Expected: PASS — every registered function (incl. the two new ones) has a non-empty example.

- [ ] **Step 5: Commit**

```bash
git add docs/tests-function-info/registered_functions.txt \
        docs/tests/python/api/rasterx_functions_sql.py \
        src/main/resources/com/databricks/labs/gbx/function-info.json
git commit -m "docs(rasterx): register rst_dtmfromgeoms(+_agg) in function-info + examples"
```

---

## Task 7: Python bindings + binding tests

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/rasterx/functions.py`
- Test: `python/geobrix/test/rasterx/test_dtmfromgeoms.py` (create)

- [ ] **Step 1: Write the failing Python test**

Create `python/geobrix/test/rasterx/test_dtmfromgeoms.py` (mirror the session/import pattern of the
existing rasterx tests — copy the `JAR`/`SparkSession`/`register` boilerplate header from
`python/geobrix/test/rasterx/test_vector_raster_bridge.py`, then):

```python
def test_rst_dtmfromgeoms_returns_tile(spark):
    from databricks.labs.gbx.rasterx import functions as F
    from pyspark.sql import functions as f

    # Four Z-valued corner points of a 100x100 extent, as WKT (z = 2x+3y+5).
    pts = [
        "POINT Z (0 0 5)", "POINT Z (100 0 205)",
        "POINT Z (0 100 305)", "POINT Z (100 100 505)",
    ]
    df = spark.createDataFrame([(pts, [])], ["points", "breaklines"])
    out = df.select(
        F.rst_dtmfromgeoms(
            f.col("points"), f.col("breaklines"),
            f.lit(0.0), f.lit(0.0),
            f.lit(0.0), f.lit(0.0), f.lit(100.0), f.lit(100.0),
            f.lit(10), f.lit(10), f.lit(32633),
        ).alias("dtm")
    ).collect()
    assert out[0]["dtm"] is not None
    assert out[0]["dtm"]["raster"] is not None


def test_rst_dtmfromgeoms_agg_returns_tile(spark):
    from databricks.labs.gbx.rasterx import functions as F
    from pyspark.sql import functions as f

    rows = [
        (1, "POINT Z (0 0 5)"), (1, "POINT Z (100 0 205)"),
        (1, "POINT Z (0 100 305)"), (1, "POINT Z (100 100 505)"),
    ]
    df = spark.createDataFrame(rows, ["region", "pt"])
    out = (
        df.groupBy("region")
        .agg(
            F.rst_dtmfromgeoms_agg(
                f.col("pt"), f.array().cast("array<string>"),
                f.lit(0.0), f.lit(0.0),
                f.lit(0.0), f.lit(0.0), f.lit(100.0), f.lit(100.0),
                f.lit(10), f.lit(10), f.lit(32633),
            ).alias("dtm")
        )
        .collect()
    )
    assert out[0]["dtm"] is not None
    assert out[0]["dtm"]["raster"] is not None
```

- [ ] **Step 2: Run test to verify it fails**

(JAR was rebuilt in Task 5; if Scala changed since, rebuild first.)
```
gbx:test:python --path python/geobrix/test/rasterx/test_dtmfromgeoms.py --log dtm-py.log
```
Expected: FAIL — `functions` has no attribute `rst_dtmfromgeoms` / `rst_dtmfromgeoms_agg`.

- [ ] **Step 3: Add the two wrappers** to `python/geobrix/src/databricks/labs/gbx/rasterx/functions.py`

```python
def rst_dtmfromgeoms(
    points: ColLike,
    breaklines: ColLike,
    merge_tolerance: ColLike,
    snap_tolerance: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
    no_data: ColLike = None,
) -> Column:
    """DTM from Z-valued points + optional breaklines via Delaunay-TIN interpolation.

    Output is a single-band Float64 GTiff of ``width_px x height_px`` over the bbox.
    For N-unit cells set ``width_px = round((xmax-xmin)/N)``,
    ``height_px = round((ymax-ymin)/N)`` (e.g. a 1000 m extent at 10 m cells -> 100 px).

    Args:
        points: Array column of Z-valued point geometries (WKB binary or WKT string).
        breaklines: Array column of breakline LineString geometries; pass an empty array for none.
        merge_tolerance: Delaunay segment-merge tolerance.
        snap_tolerance: Vertex-to-breakline snap tolerance.
        xmin, ymin, xmax, ymax: Output raster extent.
        width_px, height_px: Output raster size in pixels.
        srid: EPSG SRID.
        no_data: No-data sentinel (default -9999.0).

    Returns:
        Raster tile column.
    """
    nd = f.lit(-9999.0) if no_data is None else _col(no_data)
    return f.call_function(
        "gbx_rst_dtmfromgeoms",
        _col(points), _col(breaklines),
        _col(merge_tolerance), _col(snap_tolerance),
        _col(xmin), _col(ymin), _col(xmax), _col(ymax),
        _col(width_px), _col(height_px), _col(srid), nd,
    )


def rst_dtmfromgeoms_agg(
    point: ColLike,
    breaklines: ColLike,
    merge_tolerance: ColLike,
    snap_tolerance: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
    no_data: ColLike = None,
) -> Column:
    """DTM aggregator - one Z-valued ``point`` per row, grouped by extent key.

    Aggregator counterpart of :func:`rst_dtmfromgeoms`. ``point`` is the only
    aggregated (per-row) input; ``breaklines`` and all extent/tolerance args are
    per-group constants. Produces the same DTM as the non-agg form over the same grid.

    Returns:
        Raster tile column.
    """
    nd = f.lit(-9999.0) if no_data is None else _col(no_data)
    return f.call_function(
        "gbx_rst_dtmfromgeoms_agg",
        _col(point), _col(breaklines),
        _col(merge_tolerance), _col(snap_tolerance),
        _col(xmin), _col(ymin), _col(xmax), _col(ymax),
        _col(width_px), _col(height_px), _col(srid), nd,
    )
```

- [ ] **Step 4: Run test to verify it passes**

```
gbx:test:python --path python/geobrix/test/rasterx/test_dtmfromgeoms.py --log dtm-py.log
```
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/rasterx/functions.py \
        python/geobrix/test/rasterx/test_dtmfromgeoms.py
git commit -m "feat(python): rst_dtmfromgeoms + rst_dtmfromgeoms_agg bindings + tests"
```

---

## Task 8: SQL doc tests execute under Docker

**Files:**
- Verify: the SQL examples added in Task 6 run as doc tests.

- [ ] **Step 1: Run the SQL doc tests**

```
gbx:test:sql-docs --log dtm-sqldocs.log
```
Expected: PASS — the new `gbx_rst_dtmfromgeoms` / `_agg` SQL examples execute against real data
without error. If the example references a non-existent table (`survey_points`), adjust the
example to construct inline points via `VALUES` + `ST_*`/WKT (deterministic; matches how other
examples build inputs) so it actually executes, then re-run.

- [ ] **Step 2: Commit any example adjustments**

```bash
git add docs/tests/python/api/rasterx_functions_sql.py \
        src/main/resources/com/databricks/labs/gbx/function-info.json
git commit -m "test(docs): executable SQL doc examples for rst_dtmfromgeoms(+_agg)"
```

(Re-run `gbx:docs:function-info` if the example text changed, so function-info stays in sync; include the regenerated JSON in the commit.)

---

## Task 9: Full verification

- [ ] **Step 1: Binding parity**

```
bash scripts/commands/gbx-test-bindings.sh --log dtm-parity.log
```
Expected: PASS — both `gbx_rst_dtmfromgeoms` and `gbx_rst_dtmfromgeoms_agg` present in Scala
(name literals), Python (`functions.py`), and `function-info.json`; no missing-binding failures.

- [ ] **Step 2: Full rasterx Scala suite** (background)

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.*' --log dtm-scala-all.log
```
Expected: PASS, including `RST_DTMFromGeomsTest` and `InterpolateElevationTest`.

- [ ] **Step 3: Python rasterx suite**

```
gbx:test:python --path python/geobrix/test/rasterx/ --log dtm-py-all.log
```
Expected: PASS.

- [ ] **Step 4: function-info coverage**

```
gbx:test:function-info --log dtm-fninfo.log
```
Expected: PASS.

- [ ] **Step 5: Push** (after `gh auth switch --user mjohns-databricks`)

The QC judge runs on push, including the `binding-parity` check (which now also covers the two
new functions). Address any findings; do not blind-override.

```bash
gh auth switch --user mjohns-databricks
git push origin beta/0.4.0
```

---

## Self-review notes (author)

- **Spec coverage:** signature modernization (Task 3) ✓; bbox+pixels Scheme A (Tasks 1-3) ✓;
  safeEval fix (Task 3) ✓; pointGrid arg-order bug eliminated via `pointGridBBox` (Task 1) ✓;
  out-of-hull/NaN → no_data (Tasks 1-2) ✓; splitPointFinder dropped (Task 3) ✓; shared `execute`
  (Task 2) ✓; `_agg` with streamed points + constant-array breaklines (Task 4) ✓; register both +
  remove scoverage exclusions (Task 5) ✓; registered_functions.txt + function-info via SQL examples
  (Task 6) ✓; Python bindings (Task 7) ✓; Scala/Python/SQL doc tests + agg≡non-agg (Tasks 2,4,7,8) ✓;
  binding-parity + verification (Task 9) ✓.
- **Type consistency:** `RST_DTMFromGeoms.execute(points: Seq[Geometry], breaklines: Seq[LineString], …)`
  is called identically from `doInvoke` (Task 3) and the aggregator `eval` (Task 4);
  `DTMFromGeomsAcc.points: ArrayBuffer[Array[Byte]]` with `add(wkb)` / `serialize` / `deserialize`
  used consistently in Task 4 tests and impl; `DefaultNoData` defined once on `RST_DTMFromGeoms`
  and reused by the agg builder.
- **Known follow-up flagged in Task 8:** the SQL example may need inline `VALUES`-built points to
  be executable; resolved within the task rather than left as a placeholder.
