package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/**
  * End-to-end tests for the 8 raster->custom-grid aggregator expressions.
  *
  * Uses a synthetic in-memory EPSG:27700 raster fully contained within a large custom grid
  * (bounds [0, 1_000_000] x [0, 1_000_000]).  Custom cell keys are Long; the raster->grid path
  * is fully grid-generic (via RasterToGridGeneric) with `isCellValid = _ => true`.
  */
class RST_Custom_RasterToGridTest extends AnyFunSuite with BeforeAndAfterAll {

    /** Custom grid: [0, 1_000_000] x [0, 1_000_000] in EPSG:27700; 100 km root cells, split by 2. */
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
    private val grid = CustomGridSystem(conf)
    private val res  = 3 // cell width/height = 100000 / 2^3 = 12_500 m

    /** A constant 42.0 raster, 40x40 px, extent 490_000–510_000 E/N (20 km square), EPSG:27700. */
    var constDs: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)

        val (minX, minY) = (490000.0, 490000.0)
        val (w, h)       = (20000.0, 20000.0)
        val size         = 40
        val drv          = gdal.GetDriverByName("MEM")
        constDs = drv.Create("/vsimem/custom_r2g_const", size, size, 1, gdalconstConstants.GDT_Float64)
        constDs.SetGeoTransform(Array(minX, w / size, 0.0, minY + h, 0.0, -(h / size)))
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(27700)
        constDs.SetProjection(sr.ExportToWkt())
        sr.delete()
        val band = constDs.GetRasterBand(1)
        band.SetNoDataValue(-9999.0)
        band.WriteRaster(0, 0, size, size, Array.fill(size * size)(42.0))
        band.FlushCache(); constDs.FlushCache()
    }

    override def afterAll(): Unit = {
        if (constDs != null) constDs.delete()
    }

    test("Avg over a constant raster yields one band; every cell measure == the constant value") {
        val result = RST_Custom_RasterToGridAvg.execute(grid, constDs, res, "sparse", "centroid")
        result.length shouldBe 1
        result(0).length should be > 0
        result(0).foreach { case (cellId, measure) =>
            grid.getCellResolution(cellId) shouldBe res
            measure shouldBe Some(42.0)
        }
    }

    test("Count (centroid) sums to the total valid pixel count across cells") {
        val result = RST_Custom_RasterToGridCount.execute(grid, constDs, res, "sparse", "centroid")
        result.length shouldBe 1
        val total = result(0).flatMap(_._2).sum
        total shouldBe 1600.0 // 40x40 pixels, no NoData
    }

    test("complete coverage emits covered-but-NULL cells beyond the sparse (has-data) set") {
        val sparse   = RST_Custom_RasterToGridAvg.execute(grid, constDs, res, "sparse",   "centroid")(0)
        val complete = RST_Custom_RasterToGridAvg.execute(grid, constDs, res, "complete", "centroid")(0)

        val sparseKeys   = sparse.map(_._1).toSet
        val completeMap  = complete.toMap

        // complete is a superset of sparse; every sparse cell keeps its Some(42.0) measure.
        complete.length should be >= sparse.length
        sparseKeys.foreach(k => completeMap(k) shouldBe Some(42.0))

        // The cells complete adds (covered-but-empty) carry a NULL (None) measure.
        val extraKeys = completeMap.keySet.diff(sparseKeys)
        extraKeys.foreach(k => completeMap(k) shouldBe None)
    }

    test("centroid AND covering both produce non-empty results with the constant value") {
        val centroid = RST_Custom_RasterToGridAvg.execute(grid, constDs, res, "sparse", "centroid")(0)
        val covering = RST_Custom_RasterToGridAvg.execute(grid, constDs, res, "sparse", "covering")(0)

        centroid should not be empty
        covering should not be empty
        // Constant raster: both the centroid mean and the area-weighted (covering) mean are 42.0.
        centroid.foreach { case (_, m) => m.get shouldBe 42.0 +- 1e-9 }
        covering.foreach { case (_, m) => m.get shouldBe 42.0 +- 1e-9 }
    }

    test("R3: a grid whose bounds do NOT contain the raster extent fails UP FRONT with a clear error") {
        val tinyConf = conf.copy(boundXMax = 15000, boundYMax = 15000) // raster is at 490_000–510_000
        val tinyGrid = CustomGridSystem(tinyConf)
        val ex = intercept[IllegalArgumentException] {
            RST_Custom_RasterToGridAvg.execute(tinyGrid, constDs, res, "sparse", "centroid")
        }
        // Names the offending bound / axis, not a mid-aggregation "X coordinate out of bounds".
        ex.getMessage.toLowerCase should include("bound")
    }
}
