package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.gridx.grid.{BNG, H3, Quadbin}
import com.databricks.labs.gbx.rasterx.expressions.grid.{
    GridReprojection,
    RasterToGridGeneric,
    RST_BNG_RasterToGrid,
    RST_H3_RasterToGridAvg, RST_H3_RasterToGridCount, RST_H3_RasterToGridMax,
    RST_H3_RasterToGridMedian, RST_H3_RasterToGridMin, RST_H3_RasterToGridStddev,
    RST_H3_RasterToGridSum, RST_H3_RasterToGridVariance,
    RST_Quadbin_RasterToGrid
}
import com.databricks.labs.gbx.rasterx.parity.GridRasterParityBaseline
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.util.RasterSerializationUtil
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{ArrayType, BinaryType, DoubleType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.{SpatialReference, osrConstants}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files
import scala.collection.mutable

class RST_GridExecuteTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _
    // Tiny synthetic raster + coarse resolution for the covering-path tests (see makeCoveringRaster).
    private var covDs: Dataset = _
    private val covRes = 7

    // Task-1 stub: covering path (fAggW) throws in Task 1; provide a typed stub so Scala 2
    // can resolve the overloaded `execute` without a "missing parameter type" error on `_ => 0.0`.
    private val fAggWStub: mutable.ArrayBuffer[(Double, Double)] => Double = _ => 0.0

    // Task-2 covering stubs: fAgg is not used in the covering path but must be typed for overload
    // resolution under Scala 2.
    private val fAggStub: mutable.ArrayBuffer[Double] => Double = _ => 0.0

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        // Grid `eval` tests route through RST_ExpressionUtil.init → NodeFileManager.init,
        // which expects the checkpoint root to exist (mirrors RST_H3_RasterizeAggTest).
        Files.createDirectories(NodeFilePathUtil.rootPath)
        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString.replace("file:/", "/")
        ds = gdal.Open(tifPath)
        covDs = makeCoveringRaster()
    }

    override def afterAll(): Unit = {
        if (ds != null) ds.delete()
        if (covDs != null) covDs.delete()
    }

    test("RST_H3_RasterToGridAvg should produce average cells") {
        val result = RST_H3_RasterToGridAvg.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0.0
        }
    }

    test("RST_H3_RasterToGridCount should produce count cells") {
        val result = RST_H3_RasterToGridCount.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0
        }
    }

    test("RST_H3_RasterToGridMax should produce max cells") {
        val result = RST_H3_RasterToGridMax.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0.0
        }
    }

    test("RST_H3_RasterToGridMin should produce min cells") {
        val result = RST_H3_RasterToGridMin.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0.0
        }
    }

    test("RST_H3_RasterToGridMedian should produce median cells") {
        val result = RST_H3_RasterToGridMedian.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0.0
        }
    }

    test("RST_H3_RasterToGridSum should produce sum cells consistent with avg*count") {
        val sumRes = RST_H3_RasterToGridSum.execute(ds, 2)
        sumRes.length shouldBe 1
        sumRes(0).length should be > 0
        sumRes(0).take(5).foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0.0
        }
        // sum == avg * count per cell (same summation-order class as avg).
        val avgByCell = RST_H3_RasterToGridAvg.execute(ds, 2)(0).toMap
        val cntByCell = RST_H3_RasterToGridCount.execute(ds, 2)(0).toMap
        sumRes(0).foreach { case (cellID, sumVal) =>
            val expected = avgByCell(cellID) * cntByCell(cellID)
            sumVal shouldBe (expected +- 1e-6)
        }
    }

    test("generic execute matches per-grid avg for all three grids") {
        val fAvg: mutable.ArrayBuffer[Double] => Double = b => b.sum / b.size

        // H3: generic should match the per-grid path end-to-end (reprojection + aggregation)
        val h3Gen = RasterToGridGeneric.execute(H3, ds, 2, fAvg)
        val h3Cur = RST_H3_RasterToGridAvg.execute(ds, 2)
        h3Gen.map(_.map { case (c, v) => (c.toString, v) }).map(_.toMap) shouldBe
            h3Cur.map(_.map { case (c, v) => (c.toString, v) }).map(_.toMap)

        // Quadbin: same check at resolution 10
        val qbGen = RasterToGridGeneric.execute(Quadbin, ds, 10, fAvg)
        val qbCur = RST_Quadbin_RasterToGrid.execute(ds, 10, fAvg)
        qbGen.map(_.map { case (c, v) => (c.toString, v) }).map(_.toMap) shouldBe
            qbCur.map(_.map { case (c, v) => (c.toString, v) }).map(_.toMap)

        // BNG: generic called with BNG.isValid filter must match the per-grid path
        val bngGen = RasterToGridGeneric.execute(BNG, ds, 3, fAvg, BNG.isValid)
        val bngCur = RST_BNG_RasterToGrid.execute(ds, 3, fAvg)
        bngGen.map(_.map { case (c, v) => (c.toString, v) }).map(_.toMap) shouldBe
            bngCur.map(_.map { case (c, v) => (c.toString, v) }).map(_.toMap)
    }

    test("execute sparse+centroid reproduces Stage-1 H3 avg res-2 digest") {
        val out = RasterToGridGeneric.execute[Double](
            H3, ds, 2, "sparse", "centroid",
            fAgg = b => b.sum / b.length, fAggW = fAggWStub, emptyValue = None)
        // Some(v) unwrap → same (cell,value) shape the Stage-1 baseline hashed:
        val flat = out.map(_.collect { case (c, Some(v)) => (c.toString, v) })
        GridRasterParityBaseline.digest(flat) shouldBe GridRasterParityBaseline.H3_AVG_RES2
    }

    test("execute complete adds covered-but-empty cells as None (avg) beyond sparse") {
        // Pre-typed fAvg required: Scala 2 can't infer `b` type for positional-arg overloaded calls.
        val fAvg: mutable.ArrayBuffer[Double] => Double = b => b.sum / b.length
        val sparse = RasterToGridGeneric.execute[Double](H3, ds, 2, "sparse", "centroid",
            fAvg, fAggWStub, None)
        val complete = RasterToGridGeneric.execute[Double](H3, ds, 2, "complete", "centroid",
            fAvg, fAggWStub, None)
        val sparseCells = sparse.head.map(_._1).toSet
        val completeCells = complete.head.map(_._1).toSet
        completeCells should contain allElementsOf sparseCells // superset
        complete.head.collect { case (c, None) => c }.toSet shouldBe (completeCells -- sparseCells) // extras are None
    }

    test("RST_H3_RasterToGridVariance is population variance (>=0) and stddev == its sqrt") {
        val varRes = RST_H3_RasterToGridVariance.execute(ds, 2)
        val stdRes = RST_H3_RasterToGridStddev.execute(ds, 2)
        varRes.length shouldBe 1
        varRes(0).length should be > 0
        // Population variance is always >= 0; at least one multi-pixel cell in a
        // real MODIS tile carries nonzero spread (guard against a vacuous pass).
        val varByCell = varRes(0).toMap
        varByCell.values.foreach { v => v should be >= 0.0 }
        varByCell.values.exists(_ > 0.0) shouldBe true
        // stddev == sqrt(variance) per cell, exact cell-set match.
        val stdByCell = stdRes(0).toMap
        stdByCell.keySet shouldBe varByCell.keySet
        stdByCell.foreach { case (cellID, stdVal) =>
            stdVal shouldBe (math.sqrt(varByCell(cellID)) +- 1e-9)
        }
        // No NaN leaks from the two-pass math.
        varByCell.values.foreach { v => v.isNaN shouldBe false }
    }

    // ── Task-2 tests: covering assignment ─────────────────────────────────────

    test("covering assignment conserves mass for sum") {
        // Typed vals required: Scala 2 can't infer lambda types for overloaded execute.
        // fAggStub is unused by the covering path but is required to resolve the overload.
        val fAggWSum: mutable.ArrayBuffer[(Double, Double)] => Double =
            pairs => pairs.map { case (v, w) => v * w }.sum
        val out = RasterToGridGeneric.execute[Double](H3, covDs, covRes, "sparse", "covering",
            fAgg = fAggStub, fAggW = fAggWSum, emptyValue = None)
        val cellSum     = out.head.collect { case (_, Some(v)) => v }.sum
        val rasterTotal = band1ValidSum(covDs, H3.crsSrid)
        // Fractional tolerance: robust against accumulated FP error on the v*w products.
        cellSum shouldBe (rasterTotal +- math.max(1e-6, math.abs(rasterTotal) * 1e-9))
    }

    test("covering+complete: extra cells are None only when all-NoData") {
        // Typed vals required: Scala 2 can't infer lambda types for overloaded execute.
        val fAggWAvg: mutable.ArrayBuffer[(Double, Double)] => Double = pairs => {
            val sw = pairs.map(_._2).sum
            pairs.map { case (v, w) => v * w }.sum / sw
        }
        // Sparse run gives the has-data cell set; complete run must be a strict superset.
        val sparse = RasterToGridGeneric.execute[Double](H3, covDs, covRes, "sparse", "covering",
            fAgg = fAggStub, fAggW = fAggWAvg, emptyValue = None)
        val sparseKeys: Set[Any] = sparse.head.map(_._1).toSet

        val out = RasterToGridGeneric.execute[Double](H3, covDs, covRes, "complete", "covering",
            fAgg = fAggStub, fAggW = fAggWAvg, emptyValue = None)
        // Positive direction: every Some(v) is a valid (non-NaN) double.
        out.head.foreach {
            case (_, Some(v)) => v.isNaN shouldBe false
            case (_, None)    => ()
        }
        // Negative direction: every None entry is an extra covered-but-empty cell,
        // never a has-data cell that was silently dropped.
        out.head.collect { case (k, None) => k }.foreach { k =>
            sparseKeys should not contain k
        }
    }

    // ── Task-3-H3 tests: coverage / assignment threaded through companions ──────

    private def encodedEmpty(): UTF8String = {
        val cfg = new ExpressionConfig(
            Map.empty[String, String],
            new SerializableConfiguration(new org.apache.hadoop.conf.Configuration()))
        UTF8String.fromString(cfg.toB64)
    }

    test("companion 4-arg execute: sparse+centroid all Some; complete+centroid adds None extras") {
        val sparse   = RST_H3_RasterToGridAvg.execute(ds, 2, "sparse", "centroid")
        val complete = RST_H3_RasterToGridAvg.execute(ds, 2, "complete", "centroid")
        sparse.head.forall { case (_, m) => m.isDefined } shouldBe true
        complete.head.length should be > sparse.head.length
        val sparseKeys = sparse.head.map(_._1).toSet
        val extras = complete.head.filterNot { case (c, _) => sparseKeys.contains(c) }
        extras should not be empty
        extras.foreach { case (_, m) => m shouldBe None }
        // the has-data subset of complete is exactly the sparse cell set
        complete.head.collect { case (c, Some(_)) => c }.toSet shouldBe sparseKeys
    }

    test("companion 2-arg execute shim preserves sparse+centroid has-data cells/values") {
        val shim   = RST_H3_RasterToGridAvg.execute(ds, 2)                    // Array[Array[(Long, Double)]]
        val sparse = RST_H3_RasterToGridAvg.execute(ds, 2, "sparse", "centroid")
        shim.head.toMap shouldBe sparse.head.collect { case (c, Some(v)) => (c, v) }.toMap
    }

    test("Count measure dataType is DoubleType and complete extras emit Some(0.0)") {
        val expr = RST_H3_RasterToGridCount(
            Literal.create(null, BinaryType), Literal(2), Literal("complete"), Literal("centroid"))
        val measureType = expr.dataType
            .asInstanceOf[ArrayType].elementType
            .asInstanceOf[ArrayType].elementType
            .asInstanceOf[StructType]("measure").dataType
        measureType shouldBe DoubleType

        val sparse   = RST_H3_RasterToGridCount.execute(ds, 2, "sparse", "centroid")
        val complete = RST_H3_RasterToGridCount.execute(ds, 2, "complete", "centroid")
        val sparseKeys = sparse.head.map(_._1).toSet
        val extras = complete.head.filterNot { case (c, _) => sparseKeys.contains(c) }
        extras should not be empty
        extras.foreach { case (_, m) => m shouldBe Some(0.0) }
        // has-data count cells carry n.toDouble (>= 1.0)
        sparse.head.foreach { case (_, m) => m.get should be >= 1.0 }
    }

    test("builder injects defaults (complete, centroid), passes 4 through, rejects <2/>4") {
        val tile = Literal.create(null, BinaryType)
        val b2 = RST_H3_RasterToGridAvg.builder()(Seq(tile, Literal(2)))
            .asInstanceOf[RST_H3_RasterToGridAvg]
        b2.children.length shouldBe 5
        b2.coverage.asInstanceOf[Literal].value.toString shouldBe "complete"
        b2.assignment.asInstanceOf[Literal].value.toString shouldBe "centroid"

        val b3 = RST_H3_RasterToGridAvg.builder()(Seq(tile, Literal(2), Literal("sparse")))
            .asInstanceOf[RST_H3_RasterToGridAvg]
        b3.coverage.asInstanceOf[Literal].value.toString shouldBe "sparse"
        b3.assignment.asInstanceOf[Literal].value.toString shouldBe "centroid"

        val b4 = RST_H3_RasterToGridAvg.builder()(Seq(tile, Literal(2), Literal("sparse"), Literal("covering")))
            .asInstanceOf[RST_H3_RasterToGridAvg]
        b4.assignment.asInstanceOf[Literal].value.toString shouldBe "covering"

        an[IllegalArgumentException] should be thrownBy RST_H3_RasterToGridAvg.builder()(Seq(tile))
    }

    test("covering avg via companion execute produces finite values") {
        val cov = RST_H3_RasterToGridAvg.execute(covDs, covRes, "sparse", "covering")
        cov.head.length should be > 0
        cov.head.foreach { case (_, m) => m.foreach { v => v.isNaN shouldBe false } }
        cov.head.exists { case (_, m) => m.isDefined } shouldBe true
    }

    test("weighted median (fAggW) matches hand-computed cases") {
        // half-weight lands exactly on the 2|3 boundary → interpolate (2+3)/2 = 2.5
        RST_H3_RasterToGridMedian.fAggW(mutable.ArrayBuffer((1.0, 1.0), (2.0, 1.0), (3.0, 2.0))) shouldBe (2.5 +- 1e-12)
        // heavy weight on 20 → weighted median is 20
        RST_H3_RasterToGridMedian.fAggW(mutable.ArrayBuffer((10.0, 1.0), (20.0, 3.0))) shouldBe (20.0 +- 1e-12)
        // unit weights reduce to the ordinary median (odd n)
        RST_H3_RasterToGridMedian.fAggW(mutable.ArrayBuffer((5.0, 1.0), (1.0, 1.0), (3.0, 1.0))) shouldBe (3.0 +- 1e-12)
        // unit weights reduce to the ordinary median (even n) → (2+3)/2 = 2.5
        RST_H3_RasterToGridMedian.fAggW(mutable.ArrayBuffer((4.0, 1.0), (1.0, 1.0), (3.0, 1.0), (2.0, 1.0))) shouldBe (2.5 +- 1e-12)
    }

    test("weighted population variance and stddev (fAggW) match hand-computed cases") {
        // equal weights → ordinary population variance of {1,3} = 1.0
        RST_H3_RasterToGridVariance.fAggW(mutable.ArrayBuffer((1.0, 1.0), (3.0, 1.0))) shouldBe (1.0 +- 1e-12)
        // weighted: mean = (0*1 + 10*3)/4 = 7.5 ; var = (1*7.5^2 + 3*2.5^2)/4 = 18.75
        RST_H3_RasterToGridVariance.fAggW(mutable.ArrayBuffer((0.0, 1.0), (10.0, 3.0))) shouldBe (18.75 +- 1e-12)
        RST_H3_RasterToGridStddev.fAggW(mutable.ArrayBuffer((0.0, 1.0), (10.0, 3.0))) shouldBe (math.sqrt(18.75) +- 1e-12)
    }

    test("companion eval threads coverage/assignment to object eval and nulls empty measures") {
        val row  = RasterSerializationUtil.tileToRow((0L, ds, Map.empty[String, String]), BinaryType, null)
        val conf = encodedEmpty()

        // complete+centroid: some measures must be null (covered-but-empty cells).
        val outC = RST_H3_RasterToGridAvg.eval(
            row, 2, UTF8String.fromString("complete"), UTF8String.fromString("centroid"), conf, BinaryType)
        outC should not be null
        val bandC = outC.getArray(0)
        var sawNull = false; var sawVal = false; var i = 0
        while (i < bandC.numElements()) {
            val s = bandC.getStruct(i, 2)
            if (s.isNullAt(1)) sawNull = true else { s.getDouble(1); sawVal = true }
            i += 1
        }
        sawNull shouldBe true
        sawVal shouldBe true

        // sparse+centroid: no null measures.
        val outS = RST_H3_RasterToGridAvg.eval(
            row, 2, UTF8String.fromString("sparse"), UTF8String.fromString("centroid"), conf, BinaryType)
        val bandS = outS.getArray(0)
        var j = 0
        while (j < bandS.numElements()) {
            bandS.getStruct(j, 2).isNullAt(1) shouldBe false
            j += 1
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Tiny in-memory EPSG:4326 raster for the covering-path tests: a few thousand pixels so the
      * O(pixels) area-weighted split runs sub-second, yet still exercises real weighting. Pixels
      * (~0.05°) are larger than res-7 H3 cells, so each pixel straddles several cells and produces
      * area-fraction weights < 1. An interior NoData block guarantees covered-but-empty (None)
      * cells under `complete` coverage. Grid CRS == raster CRS (4326), so no reprojection occurs
      * and `band1ValidSum` is an exact mass-conservation reference. */
    private def makeCoveringRaster(): Dataset = {
        val w = 40; val h = 40; val nodata = -9999.0
        val mem = gdal.GetDriverByName("MEM").Create("", w, h, 1, gdalconstConstants.GDT_Float64)
        // origin lon -0.5, lat 52.0; 0.05° pixels → extent lon[-0.5,1.5], lat[52.0,50.0]
        mem.SetGeoTransform(Array(-0.5, 0.05, 0.0, 52.0, 0.0, -0.05))
        val sr = new SpatialReference()
        sr.ImportFromEPSG(4326)
        sr.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        mem.SetProjection(sr.ExportToWkt())
        sr.delete()
        val buf = Array.tabulate(w * h) { i =>
            val x = i % w; val y = i / w
            if (x >= 14 && x < 26 && y >= 14 && y < 26) nodata // interior all-NoData block
            else 1.0 + x + y                                    // positive ramp, all distinct-ish
        }
        val band = mem.GetRasterBand(1)
        band.SetNoDataValue(nodata)
        band.WriteRaster(0, 0, w, h, buf)
        mem.FlushCache()
        mem
    }

    /** Sum of valid (non-masked) pixel values in band 1 of the reprojected dataset.
      * Reprojects `rawDs` to `gridSrid` using the same path as `execute`, so the total
      * matches what `executeOnCovering` accumulates before the area-weight product.
      */
    private def band1ValidSum(rawDs: Dataset, gridSrid: Int): Double = {
        val (workDs, reprojected) = GridReprojection.toGridCrs(rawDs, gridSrid)
        try {
            val xSize = workDs.getRasterXSize
            val ySize = workDs.getRasterYSize
            val nPix  = xSize * ySize
            val buf   = new Array[Double](nPix)
            val mask  = new Array[Byte](nPix)
            val b     = workDs.GetRasterBand(1)
            b.ReadRaster(0, 0, xSize, ySize, buf)
            b.GetMaskBand().ReadRaster(0, 0, xSize, ySize, mask)
            var sum = 0.0
            var i   = 0
            while (i < nPix) { if (mask(i) != 0) sum += buf(i); i += 1 }
            sum
        } finally {
            if (reprojected) RasterDriver.releaseDataset(workDs)
        }
    }

}
