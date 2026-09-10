package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf, GridSystem}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.locationtech.jts.geom.Geometry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files
import scala.collection.mutable

/**
  * Task 15 (Stage-3 Phase 5): guards the O(boundary) interior fast-path in
  * [[RasterToGridGeneric]]'s covering-assignment path.
  *
  * Two invariants:
  *  1. RESULT-IDENTITY — the production covering path must produce the SAME per-cell
  *     weighted measures as a verbatim reimplementation of the OLD per-pixel
  *     candidate+intersection algorithm ([[refCovering]]). This is the result-neutrality
  *     guard: the refactor is pure performance and MUST NOT change results.
  *  2. PERF-PROOF — a pixel whose four corners all map to the same cell id is fully
  *     interior and must SKIP `coveringCandidateCells`/`intersection`. Instrumented via a
  *     [[CountingGrid]] decorator: `coveringCandidateCells` invocations must be
  *     `<= boundaryPixelCount`. This FAILS before the fast-path (every valid pixel enters
  *     the candidate path) and PASSES after (only boundary pixels do).
  *
  * Fixture: a varying-value EPSG:27700 raster whose pixel grid is intentionally NOT aligned
  * to the custom grid's cell edges, so cell boundaries cut through pixels — guaranteeing a
  * mix of interior AND boundary pixels (both counts asserted > 0).
  */
class RasterToGridCoveringPerfTest extends AnyFunSuite with BeforeAndAfterAll {

    /** Custom grid: [0, 1_000_000]^2 in EPSG:27700; 100 km root cells, split by 2. */
    private val conf = GridConf(
        boundXMin     = 0,
        boundXMax     = 1000000,
        boundYMin     = 0,
        boundYMax     = 1000000,
        cellSplits    = 2,
        rootCellSizeX = 100000,
        rootCellSizeY = 100000,
        crsID         = Some(27700)
    )
    private val baseGrid = CustomGridSystem(conf)
    private val res      = 3 // cell size = 100000 / 2^3 = 12_500 m

    /** fAggW used by both the production path and the reference path: sum of value*weight. */
    private val fAggW: mutable.ArrayBuffer[(Double, Double)] => Double =
        _.foldLeft(0.0) { case (acc, (v, w)) => acc + v * w }
    /** Centroid-path aggregator is unused on the covering path; a typed stub for overload resolution. */
    private val fAggStub: mutable.ArrayBuffer[Double] => Double = _ => 0.0

    /**
      * 37x37 EPSG:27700 raster, extent 495_000–515_000 E/N (20 km square), varying values.
      * Pixel size 20000/37 ≈ 540.5 m does NOT divide the 12_500 m cell size, and the extent
      * crosses the cell boundaries at 500_000 and 512_500 on both axes — so many pixels straddle
      * a cell edge (boundary) while the bulk sit wholly inside a cell (interior).
      */
    private var ds: Dataset = _
    private val size = 37

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)

        val (minX, minY) = (495000.0, 495000.0)
        val (w, h)       = (20000.0, 20000.0)
        val drv          = gdal.GetDriverByName("MEM")
        ds = drv.Create("/vsimem/covering_perf", size, size, 1, gdalconstConstants.GDT_Float64)
        ds.SetGeoTransform(Array(minX, w / size, 0.0, minY + h, 0.0, -(h / size)))
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(27700)
        ds.SetProjection(sr.ExportToWkt())
        sr.delete()
        val band = ds.GetRasterBand(1)
        band.SetNoDataValue(-9999.0)
        // Varying field so per-cell weighted sums are distinctive (a constant field would
        // hide a mis-weighting on the boundary path).
        val vals = new Array[Double](size * size)
        var i = 0
        while (i < vals.length) { vals(i) = 1.0 + (i % 13) + (i / size) * 0.5; i += 1 }
        band.WriteRaster(0, 0, size, size, vals)
        band.FlushCache(); ds.FlushCache()
    }

    override def afterAll(): Unit = {
        if (ds != null) ds.delete()
    }

    /**
      * Verbatim reimplementation of the OLD `executeOnCovering` inner loop: candidate
      * enumeration + JTS intersection for EVERY valid pixel (no interior fast-path). Also
      * tallies interior vs boundary pixels (four-corner cell agreement) for the perf assertion.
      *
      * @return (per-cell weighted measure keyed by renderCellId, validPixels, interiorPixels, boundaryPixels)
      */
    private def refCovering(g: GridSystem, d: Dataset): (Map[Any, Double], Int, Int, Int) = {
        val gt     = d.GetGeoTransform
        val xSize  = d.getRasterXSize
        val ySize  = d.getRasterYSize
        val nPix   = xSize * ySize
        val bandBuf = new Array[Double](nPix)
        val maskBuf = new Array[Byte](nPix)
        val b = d.GetRasterBand(1); val m = b.GetMaskBand()
        b.ReadRaster(0, 0, xSize, ySize, bandBuf)
        m.ReadRaster(0, 0, xSize, ySize, maskBuf)

        val accW = new mutable.LongMap[mutable.ArrayBuffer[(Double, Double)]]()
        var valid = 0; var interior = 0; var boundary = 0
        var y = 0; var idx = 0
        while (y < ySize) {
            var x = 0
            while (x < xSize) {
                if (maskBuf(idx) != 0) {
                    valid += 1
                    val value = bandBuf(idx)
                    val x0 = gt(0) + x       * gt(1) + y       * gt(2)
                    val y0 = gt(3) + x       * gt(4) + y       * gt(5)
                    val x1 = gt(0) + (x + 1) * gt(1) + y       * gt(2)
                    val y1 = gt(3) + (x + 1) * gt(4) + y       * gt(5)
                    val x2 = gt(0) + (x + 1) * gt(1) + (y + 1) * gt(2)
                    val y2 = gt(3) + (x + 1) * gt(4) + (y + 1) * gt(5)
                    val x3 = gt(0) + x       * gt(1) + (y + 1) * gt(2)
                    val y3 = gt(3) + x       * gt(4) + (y + 1) * gt(5)

                    // Interior/boundary tally (independent of the accumulation).
                    val c0 = g.pointToCellID(x0, y0, res)
                    val c1 = g.pointToCellID(x1, y1, res)
                    val c2 = g.pointToCellID(x2, y2, res)
                    val c3 = g.pointToCellID(x3, y3, res)
                    if (c0 == c1 && c1 == c2 && c2 == c3) interior += 1 else boundary += 1

                    // OLD algorithm: candidate enumeration + intersection for EVERY pixel.
                    val pixelRect: Geometry = JTS.polygonFromXYs(
                        Array((x0, y0), (x1, y1), (x2, y2), (x3, y3), (x0, y0))
                    )
                    val pxArea = pixelRect.getArea
                    g.coveringCandidateCells(pixelRect, res).foreach { c =>
                        val inter = g.cellIdToGeometry(c).intersection(pixelRect)
                        if (inter != null && inter.getArea > 0) {
                            accW.getOrElseUpdate(c, new mutable.ArrayBuffer) += ((value, inter.getArea / pxArea))
                        }
                    }
                }
                idx += 1; x += 1
            }
            y += 1
        }
        val out = accW.map { case (cell, buf) => (g.renderCellId(cell), fAggW(buf)) }.toMap
        (out, valid, interior, boundary)
    }

    /** Runs the production covering path and returns per-cell weighted measures keyed by renderCellId. */
    private def prodCovering(g: GridSystem, d: Dataset): Map[Any, Double] = {
        val bands = RasterToGridGeneric.execute[Double](
            g, d, res, "sparse", "covering", fAggStub, fAggW, Option.empty[Double], (_: Long) => true
        )
        bands(0).map { case (k, opt) => (k, opt.get) }.toMap
    }

    test("result-identity: production covering equals the old per-pixel candidate+intersection path") {
        val (refMap, valid, interior, boundary) = refCovering(baseGrid, ds)
        val prodMap = prodCovering(baseGrid, ds)

        // Fixture sanity: the tile genuinely exercises BOTH paths.
        withClue("fixture must contain interior pixels: ")(interior should be > 0)
        withClue("fixture must contain boundary pixels: ")(boundary should be > 0)
        valid shouldBe (size * size)

        // Same cell set.
        prodMap.keySet shouldBe refMap.keySet

        // Per-cell weighted measure identity. Byte-identity is the strong claim; if JTS area
        // arithmetic on axis-aligned rectangles differs from an exact 1.0 by a sub-ULP epsilon,
        // this reports the actual max diff (which must remain well within the 1e-9 parity bar).
        val maxDiff = refMap.map { case (k, rv) => math.abs(rv - prodMap(k)) }.max
        info(s"result-identity maxAbsDiff = $maxDiff (interior=$interior boundary=$boundary valid=$valid cells=${refMap.size})")
        maxDiff shouldBe 0.0

        // Mass conservation for a sum measure: Σ weighted contributions == Σ valid pixel values.
        val gt = ds.GetGeoTransform; val nPix = size * size
        val bandBuf = new Array[Double](nPix); val maskBuf = new Array[Byte](nPix)
        val b = ds.GetRasterBand(1); val m = b.GetMaskBand()
        b.ReadRaster(0, 0, size, size, bandBuf); m.ReadRaster(0, 0, size, size, maskBuf)
        var pixelTotal = 0.0; var i = 0
        while (i < nPix) { if (maskBuf(i) != 0) pixelTotal += bandBuf(i); i += 1 }
        val prodTotal = prodMap.values.sum
        prodTotal shouldBe (pixelTotal +- 1e-6)
    }

    test("perf-proof: interior pixels skip candidate enumeration (coveringCandidateCells calls <= boundary pixels)") {
        val (_, _, interior, boundary) = refCovering(baseGrid, ds)
        interior should be > 0 // otherwise the assertion below is vacuous

        val counting = new CountingGrid(baseGrid)
        prodCovering(counting, ds)
        info(s"coveringCandidateCells calls = ${counting.candidateCalls}; boundaryPixels = $boundary; interiorPixels = $interior")
        counting.candidateCalls should be <= boundary
    }

    /**
      * GridSystem decorator that counts `coveringCandidateCells` invocations (the candidate
      * enumeration that gates every JTS intersection). All other members delegate unchanged.
      * In-process, single-threaded use — a plain var counter suffices.
      */
    private final class CountingGrid(u: GridSystem) extends GridSystem {
        var candidateCalls = 0
        def name: String = u.name
        def crsSrid: Int = u.crsSrid
        def resolutions: Set[Int] = u.resolutions
        def pointToCellID(x: Double, y: Double, resolution: Int): Long = u.pointToCellID(x, y, resolution)
        def cellIdToGeometry(cellID: Long): Geometry = u.cellIdToGeometry(cellID)
        def polyfill(geometry: Geometry, resolution: Int): Seq[Long] = u.polyfill(geometry, resolution)
        def coveringCandidateCells(bbox: Geometry, resolution: Int): Seq[Long] = {
            candidateCalls += 1
            u.coveringCandidateCells(bbox, resolution)
        }
        def kRing(cellID: Long, k: Int): Seq[Long] = u.kRing(cellID, k)
        def kLoop(cellID: Long, k: Int): Seq[Long] = u.kLoop(cellID, k)
        override def renderCellId(cellID: Long): Any = u.renderCellId(cellID)
    }
}
