package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.gridx.grid.{BNG, CustomGridSystem, GridConf, GridSystem, H3, Quadbin}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.{SpatialReference, osrConstants}
import org.locationtech.jts.geom.Geometry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files
import scala.collection.mutable

/**
  * Task 15 (Stage-3 Phase 5): guards the covering-assignment interior fast-path in
  * [[RasterToGridGeneric]] for RESULT-NEUTRALITY (the refactor MUST NOT change results) and for
  * the O(boundary) PERF win, PER GRID.
  *
  * Result-neutrality is grid-dependent because the fast-path assigns an interior pixel weight
  * exactly 1.0 instead of the JTS `intersection.getArea / pixelArea` the old path computed. That
  * substitution is exact ONLY when a grid's point-partition (`pointToCellID`) coincides with its
  * cell polygon (`cellIdToGeometry`):
  *  - Custom / Quadbin / BNG — analytic-square cells: `pointToCellID` floor-bins to the same
  *    square `cellIdToGeometry` draws. The fast-path is BYTE-IDENTICAL to the old path (asserted).
  *  - H3 — `pointToCellID` is `geoToH3` (the true partition) while `cellIdToGeometry` is the
  *    `h3ToGeoBoundary` CHORD polygon that under-shoots the geodesic edge. An interior pixel with
  *    a sliver in the chord gap would get 1.0 (fast) vs slightly <1.0 (old) — a real, tiny change.
  *    So H3 declares `coveringFastPathExact = false` and keeps the old path. This suite MEASURES
  *    that hypothetical gap and confirms production H3 output is byte-identical to the old path.
  */
class RasterToGridCoveringPerfTest extends AnyFunSuite with BeforeAndAfterAll {

    /** fAggW used by production, reference, and forced-fast paths: sum of value*weight. */
    private val fAggW: mutable.ArrayBuffer[(Double, Double)] => Double =
        _.foldLeft(0.0) { case (acc, (v, w)) => acc + v * w }
    private val fAggStub: mutable.ArrayBuffer[Double] => Double = _ => 0.0
    private val acceptAll: Long => Boolean = _ => true

    // ── fixtures: per grid, pixels finer than a cell so both interior AND boundary pixels occur,
    //    and pixel edges intentionally unaligned to cell edges so cells cut through pixels. ──
    private var customDs: Dataset = _ // EPSG:27700, custom grid
    private var qbDs: Dataset = _     // EPSG:4326,  quadbin
    private var bngDs: Dataset = _    // EPSG:27700, BNG
    private var h3Ds: Dataset = _     // EPSG:4326,  H3 (fine res)
    private var h3CoarseDs: Dataset = _ // EPSG:4326, H3 (coarse res, where the chord gap is large)

    private val customGrid = CustomGridSystem(GridConf(
        boundXMin = 0, boundXMax = 1000000, boundYMin = 0, boundYMax = 1000000,
        cellSplits = 2, rootCellSizeX = 100000, rootCellSizeY = 100000, crsID = Some(27700)))
    private val customRes = 3 // 12_500 m cells
    private val qbRes      = 12
    private val bngRes     = 3  // 1 km cells
    private val h3Res       = 9
    private val h3CoarseRes = 3

    private val size = 37

    /** Builds a `size`x`size` in-memory raster with a varying value field, at the given origin,
      * pixel size, and SRID (traditional GIS axis order). North-up (negative y pixel height). */
    private def makeRaster(minX: Double, minY: Double, pxW: Double, pxH: Double, srid: Int): Dataset = {
        val mem = gdal.GetDriverByName("MEM").Create("", size, size, 1, gdalconstConstants.GDT_Float64)
        mem.SetGeoTransform(Array(minX, pxW, 0.0, minY + pxH * size, 0.0, -pxH))
        val sr = new SpatialReference()
        sr.ImportFromEPSG(srid)
        sr.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        mem.SetProjection(sr.ExportToWkt())
        sr.delete()
        val vals = Array.tabulate(size * size)(i => 1.0 + (i % 13) + (i / size) * 0.5)
        val band = mem.GetRasterBand(1)
        band.SetNoDataValue(-9999.0)
        band.WriteRaster(0, 0, size, size, vals)
        band.FlushCache(); mem.FlushCache()
        mem
    }

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)

        // Custom: 20 km raster over 12.5 km cells (pixel ~541 m), crosses cell edges at 500/512.5 km.
        customDs = makeRaster(495000.0, 495000.0, 20000.0 / size, 20000.0 / size, 27700)
        // Quadbin res-12 cells ~0.088deg; pixels 0.02deg over 0.74deg extent.
        qbDs = makeRaster(-0.5, 51.5, 0.02, 0.02, 4326)
        // BNG res-3 = 1 km cells; pixels 250 m over 9.25 km extent.
        bngDs = makeRaster(500000.0, 200000.0, 250.0, 250.0, 27700)
        // H3 res-9 hexes ~348 m across; pixels ~0.001deg (~90 m) over 0.037deg extent.
        h3Ds = makeRaster(-0.5, 51.5, 0.001, 0.001, 4326)
        // H3 res-3 hexes are ~100+ km across; pixels 0.1deg (~11 km) so the chord-vs-geodesic
        // sag between a hex's straight chord polygon and its true geoToH3 boundary is material.
        h3CoarseDs = makeRaster(-2.0, 40.0, 0.1, 0.1, 4326)
    }

    override def afterAll(): Unit = {
        Seq(customDs, qbDs, bngDs, h3Ds, h3CoarseDs).foreach(d => if (d != null) d.delete())
    }

    /** Reads band-1 values + validity mask of `d` into parallel arrays plus the geotransform. */
    private def read(d: Dataset): (Array[Double], Array[Byte], Array[Double], Int, Int) = {
        val gt = d.GetGeoTransform; val xSize = d.getRasterXSize; val ySize = d.getRasterYSize
        val nPix = xSize * ySize
        val bandBuf = new Array[Double](nPix); val maskBuf = new Array[Byte](nPix)
        val b = d.GetRasterBand(1); val m = b.GetMaskBand()
        b.ReadRaster(0, 0, xSize, ySize, bandBuf); m.ReadRaster(0, 0, xSize, ySize, maskBuf)
        (bandBuf, maskBuf, gt, xSize, ySize)
    }

    private def corners(gt: Array[Double], x: Int, y: Int): Array[(Double, Double)] = Array(
        (gt(0) + x       * gt(1) + y       * gt(2), gt(3) + x       * gt(4) + y       * gt(5)),
        (gt(0) + (x + 1) * gt(1) + y       * gt(2), gt(3) + (x + 1) * gt(4) + y       * gt(5)),
        (gt(0) + (x + 1) * gt(1) + (y + 1) * gt(2), gt(3) + (x + 1) * gt(4) + (y + 1) * gt(5)),
        (gt(0) + x       * gt(1) + (y + 1) * gt(2), gt(3) + x       * gt(4) + (y + 1) * gt(5))
    )

    /**
      * Verbatim reimplementation of the OLD `executeOnCovering` inner loop: candidate enumeration +
      * JTS intersection for EVERY valid pixel (no interior fast-path). Also tallies interior vs
      * boundary pixels (four-corner cell agreement).
      *
      * @return (per-cell weighted measure keyed by renderCellId, validPixels, interiorPixels, boundaryPixels)
      */
    private def refCovering(g: GridSystem, d: Dataset, res: Int, isCellValid: Long => Boolean)
        : (Map[Any, Double], Int, Int, Int) = {
        val (bandBuf, maskBuf, gt, xSize, ySize) = read(d)
        val accW = new mutable.LongMap[mutable.ArrayBuffer[(Double, Double)]]()
        var valid = 0; var interior = 0; var boundary = 0
        var y = 0; var idx = 0
        while (y < ySize) {
            var x = 0
            while (x < xSize) {
                if (maskBuf(idx) != 0) {
                    valid += 1
                    val value = bandBuf(idx)
                    val cs = corners(gt, x, y)
                    val cids = cs.map { case (cx, cy) => g.pointToCellID(cx, cy, res) }
                    if (cids.forall(_ == cids(0))) interior += 1 else boundary += 1
                    val pixelRect: Geometry = JTS.polygonFromXYs(cs :+ cs(0))
                    val pxArea = pixelRect.getArea
                    g.coveringCandidateCells(pixelRect, res).foreach { c =>
                        if (isCellValid(c)) {
                            val inter = g.cellIdToGeometry(c).intersection(pixelRect)
                            if (inter != null && inter.getArea > 0) {
                                accW.getOrElseUpdate(c, new mutable.ArrayBuffer) += ((value, inter.getArea / pxArea))
                            }
                        }
                    }
                }
                idx += 1; x += 1
            }
            y += 1
        }
        (accW.map { case (cell, buf) => (g.renderCellId(cell), fAggW(buf)) }.toMap, valid, interior, boundary)
    }

    /**
      * The interior fast-path applied UNCONDITIONALLY (ignoring `coveringFastPathExact`): an
      * interior pixel gets weight exactly 1.0. Used only to MEASURE what the fast-path WOULD do on
      * a non-exact grid (H3) vs the exact old path — it is not the production behaviour for H3.
      */
    private def forcedFastCovering(g: GridSystem, d: Dataset, res: Int, isCellValid: Long => Boolean): Map[Any, Double] = {
        val (bandBuf, maskBuf, gt, xSize, ySize) = read(d)
        val accW = new mutable.LongMap[mutable.ArrayBuffer[(Double, Double)]]()
        var y = 0; var idx = 0
        while (y < ySize) {
            var x = 0
            while (x < xSize) {
                if (maskBuf(idx) != 0) {
                    val value = bandBuf(idx)
                    val cs = corners(gt, x, y)
                    val cids = cs.map { case (cx, cy) => g.pointToCellID(cx, cy, res) }
                    if (cids.forall(_ == cids(0))) {
                        if (isCellValid(cids(0))) accW.getOrElseUpdate(cids(0), new mutable.ArrayBuffer) += ((value, 1.0))
                    } else {
                        val pixelRect: Geometry = JTS.polygonFromXYs(cs :+ cs(0))
                        val pxArea = pixelRect.getArea
                        g.coveringCandidateCells(pixelRect, res).foreach { c =>
                            if (isCellValid(c)) {
                                val inter = g.cellIdToGeometry(c).intersection(pixelRect)
                                if (inter != null && inter.getArea > 0) {
                                    accW.getOrElseUpdate(c, new mutable.ArrayBuffer) += ((value, inter.getArea / pxArea))
                                }
                            }
                        }
                    }
                }
                idx += 1; x += 1
            }
            y += 1
        }
        accW.map { case (cell, buf) => (g.renderCellId(cell), fAggW(buf)) }.toMap
    }

    /** Runs the production covering path; returns per-cell weighted measures keyed by renderCellId. */
    private def prodCovering(g: GridSystem, d: Dataset, res: Int, isCellValid: Long => Boolean): Map[Any, Double] =
        RasterToGridGeneric.execute[Double](g, d, res, "sparse", "covering", fAggStub, fAggW, Option.empty[Double], isCellValid)(0)
            .map { case (k, opt) => (k, opt.get) }.toMap

    private def maxAbsDiff(a: Map[Any, Double], b: Map[Any, Double]): Double = {
        (a.keySet ++ b.keySet).foldLeft(0.0) { (mx, k) =>
            math.max(mx, math.abs(a.getOrElse(k, 0.0) - b.getOrElse(k, 0.0)))
        }
    }

    // ── exact (analytic-square) grids: production fast-path must be BYTE-IDENTICAL to the old path ──
    private val exactCases: Seq[(String, GridSystem, () => Dataset, Int)] = Seq(
        ("Custom",  customGrid, () => customDs, customRes),
        ("Quadbin", Quadbin,    () => qbDs,     qbRes),
        ("BNG",     BNG,        () => bngDs,    bngRes)
    )

    exactCases.foreach { case (name, grid, dsF, res) =>
        test(s"$name: covering interior fast-path is byte-identical to the old intersection path") {
            withClue(s"$name should declare coveringFastPathExact: ")(grid.coveringFastPathExact shouldBe true)
            val d = dsF()
            val (refMap, valid, interior, boundary) = refCovering(grid, d, res, acceptAll)
            val prodMap = prodCovering(grid, d, res, acceptAll)
            val diff = maxAbsDiff(prodMap, refMap)
            info(s"$name result-identity maxAbsDiff = $diff (interior=$interior boundary=$boundary valid=$valid cells=${refMap.size})")
            withClue(s"$name fixture must exercise the interior fast-path: ")(interior should be > 0)
            withClue(s"$name fixture must exercise the boundary path: ")(boundary should be > 0)
            prodMap.keySet shouldBe refMap.keySet
            diff shouldBe 0.0
        }
    }

    // ── H3: fast-path disabled; production == old path at every resolution. The hypothetical
    //    forced-fast gap is MEASURED at a fine AND a coarse resolution (reported, not asserted:
    //    the sliver is sub-ULP at fine res but grows with cell size — the reason H3 is restricted). ──
    test("H3: fast-path is disabled and covering output is byte-identical to the old path (gap measured)") {
        H3.coveringFastPathExact shouldBe false

        // Fine resolution (res-9).
        val (refFine, validF, interF, boundF) = refCovering(H3, h3Ds, h3Res, acceptAll)
        val prodFine = prodCovering(H3, h3Ds, h3Res, acceptAll)
        withClue("H3 fine fixture must contain interior-binned pixels: ")(interF should be > 0)
        withClue("H3 fine fixture must contain boundary pixels: ")(boundF should be > 0)
        prodFine.keySet shouldBe refFine.keySet
        val prodFineDiff = maxAbsDiff(prodFine, refFine)
        info(s"H3 res-9 production-vs-old maxAbsDiff = $prodFineDiff (interior=$interF boundary=$boundF valid=$validF)")
        prodFineDiff shouldBe 0.0 // production uses the old path -> byte-identical, the guarantee that matters
        val forcedFineDiff = maxAbsDiff(forcedFastCovering(H3, h3Ds, h3Res, acceptAll), refFine)
        info(s"H3 res-9 forced-fast-path-vs-old maxAbsDiff = $forcedFineDiff (near-straight fine edges: sub-ULP)")

        // Coarse resolution (res-3): the chord-vs-geodesic sag is large, so the forced fast-path
        // would diverge here — concrete evidence that enabling it for H3 breaks result-neutrality.
        val (refCoarse, validC, interC, boundC) = refCovering(H3, h3CoarseDs, h3CoarseRes, acceptAll)
        withClue("H3 coarse fixture must contain interior-binned pixels: ")(interC should be > 0)
        withClue("H3 coarse fixture must contain boundary pixels: ")(boundC should be > 0)
        val prodCoarseDiff = maxAbsDiff(prodCovering(H3, h3CoarseDs, h3CoarseRes, acceptAll), refCoarse)
        info(s"H3 res-3 production-vs-old maxAbsDiff = $prodCoarseDiff (interior=$interC boundary=$boundC valid=$validC)")
        prodCoarseDiff shouldBe 0.0 // production still byte-identical (old path) at coarse res
        val forcedCoarseDiff = maxAbsDiff(forcedFastCovering(H3, h3CoarseDs, h3CoarseRes, acceptAll), refCoarse)
        info(s"H3 res-3 forced-fast-path-vs-old maxAbsDiff = $forcedCoarseDiff (chord-gap materialises at coarse res)")
    }

    // ── perf: exact grids skip candidate enumeration on interior pixels; H3 does not ──
    test("perf-proof: exact grids call coveringCandidateCells == boundary pixels; H3 calls it for every valid pixel") {
        exactCases.foreach { case (name, grid, dsF, res) =>
            val d = dsF()
            val (_, _, _, boundary) = refCovering(grid, d, res, acceptAll)
            val counting = new CountingGrid(grid)
            prodCovering(counting, d, res, acceptAll)
            info(s"$name coveringCandidateCells calls = ${counting.candidateCalls}; boundaryPixels = $boundary")
            withClue(s"$name interior fast-path must remove ALL interior candidate enumerations: ")(
                counting.candidateCalls shouldBe boundary)
        }
        val (_, valid, _, _) = refCovering(H3, h3Ds, h3Res, acceptAll)
        val h3Counting = new CountingGrid(H3)
        prodCovering(h3Counting, h3Ds, h3Res, acceptAll)
        info(s"H3 coveringCandidateCells calls = ${h3Counting.candidateCalls}; validPixels = $valid (fast-path not taken)")
        h3Counting.candidateCalls shouldBe valid
    }

    // ── isCellValid false-branch on the fast-path: a rejected interior cell contributes nothing ──
    test("fast-path honours isCellValid: a rejected interior cell is dropped, matching the old path") {
        // Reject exactly one cell that receives interior pixels, to exercise the fast-path's
        // `if (isCellValid(c0))` false branch. Pick the cell with the most contributions.
        val (fullMap, _, interior, _) = refCovering(customGrid, customDs, customRes, acceptAll)
        interior should be > 0
        val rejected = fullMap.keys.maxBy(k => fullMap(k)) // some real, populated cell (Long key)
        val rejectedId = rejected.asInstanceOf[Long]
        val pred: Long => Boolean = _ != rejectedId

        val (refMap, _, _, _) = refCovering(customGrid, customDs, customRes, pred)
        val prodMap = prodCovering(customGrid, customDs, customRes, pred)
        withClue("rejected cell must be absent from both paths: ") {
            refMap.contains(rejected) shouldBe false
            prodMap.contains(rejected) shouldBe false
        }
        prodMap.keySet shouldBe refMap.keySet
        maxAbsDiff(prodMap, refMap) shouldBe 0.0
    }

    test("coveringFastPathExact capability is wired: true for analytic-square grids, false for H3") {
        customGrid.coveringFastPathExact shouldBe true
        Quadbin.coveringFastPathExact shouldBe true
        BNG.coveringFastPathExact shouldBe true
        H3.coveringFastPathExact shouldBe false
    }

    /**
      * GridSystem decorator counting `coveringCandidateCells` invocations (the enumeration gating
      * every JTS intersection). Delegates everything else — crucially `coveringFastPathExact`, so
      * wrapping does not change which path the production code takes.
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
        override def coveringFastPathExact: Boolean = u.coveringFastPathExact
    }
}
