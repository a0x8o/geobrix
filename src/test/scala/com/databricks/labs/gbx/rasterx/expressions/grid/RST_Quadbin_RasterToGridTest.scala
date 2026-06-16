package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.gridx.grid.Quadbin
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/**
  * End-to-end tests for the 5 raster->quadbin aggregator expressions.
  *
  * Uses a synthetic in-memory raster in EPSG:4326 so cell IDs and measure
  * values can be hand-verified.
  */
class RST_Quadbin_RasterToGridTest extends AnyFunSuite with BeforeAndAfterAll {

    /** A small 4x4 raster centered over (0, 0) — pixels 0.25 deg wide. */
    var constDs: Dataset = _

    /** A 4x4 raster with a non-uniform value field. */
    var rangeDs: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()

        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)

        val drv = gdal.GetDriverByName("MEM")

        // Raster 1: constant 7.0 over (-0.5, -0.5) -> (0.5, 0.5), 4x4 pixels, EPSG:4326.
        constDs = drv.Create("/vsimem/quadbin_const", 4, 4, 1, gdalconstConstants.GDT_Float64)
        constDs.SetGeoTransform(Array(-0.5, 0.25, 0.0, 0.5, 0.0, -0.25))
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(4326)
        constDs.SetProjection(sr.ExportToWkt())
        val cBand = constDs.GetRasterBand(1)
        cBand.WriteRaster(0, 0, 4, 4, Array.fill(16)(7.0))
        cBand.FlushCache()

        // Raster 2: same footprint, values = 1..16 in raster order.
        rangeDs = drv.Create("/vsimem/quadbin_range", 4, 4, 1, gdalconstConstants.GDT_Float64)
        rangeDs.SetGeoTransform(Array(-0.5, 0.25, 0.0, 0.5, 0.0, -0.25))
        rangeDs.SetProjection(sr.ExportToWkt())
        val rBand = rangeDs.GetRasterBand(1)
        rBand.WriteRaster(0, 0, 4, 4, (1 to 16).map(_.toDouble).toArray)
        rBand.FlushCache()
    }

    override def afterAll(): Unit = {
        if (constDs != null) constDs.delete()
        if (rangeDs != null) rangeDs.delete()
    }

    test("Avg returns one band of cells with measure = constant value") {
        val result = RST_Quadbin_RasterToGridAvg.execute(constDs, resolution = 6)
        result.length shouldBe 1
        result(0).length should be > 0
        result(0).foreach { case (cellId, avg) =>
            Quadbin.resolution(cellId) shouldBe 6
            avg shouldBe 7.0 +- 1e-9
        }
    }

    test("Count sums to total valid pixel count across cells") {
        val result = RST_Quadbin_RasterToGridCount.execute(constDs, resolution = 6)
        result.length shouldBe 1
        val total = result(0).map(_._2).sum
        total shouldBe 16L // 4x4 pixels, no NoData
    }

    test("Max >= Avg >= Min for every cell on the range raster") {
        val avgRes = RST_Quadbin_RasterToGridAvg.execute(rangeDs, resolution = 6)(0).toMap
        val maxRes = RST_Quadbin_RasterToGridMax.execute(rangeDs, resolution = 6)(0).toMap
        val minRes = RST_Quadbin_RasterToGridMin.execute(rangeDs, resolution = 6)(0).toMap

        avgRes.keySet should not be empty
        avgRes.keySet shouldBe maxRes.keySet
        avgRes.keySet shouldBe minRes.keySet

        avgRes.foreach { case (cell, avg) =>
            val mx = maxRes(cell)
            val mn = minRes(cell)
            mn should be <= avg
            avg should be <= mx
            mn should be >= 1.0
            mx should be <= 16.0
        }
    }

    test("Median falls between min and max for every cell") {
        val medRes = RST_Quadbin_RasterToGridMedian.execute(rangeDs, resolution = 6)(0).toMap
        val maxRes = RST_Quadbin_RasterToGridMax.execute(rangeDs, resolution = 6)(0).toMap
        val minRes = RST_Quadbin_RasterToGridMin.execute(rangeDs, resolution = 6)(0).toMap

        medRes.keySet shouldBe maxRes.keySet
        medRes.foreach { case (cell, med) =>
            minRes(cell) should be <= med
            med should be <= maxRes(cell)
        }
    }

    test("Resolution guard rejects values outside [0, 20]") {
        an[IllegalArgumentException] should be thrownBy
            RST_Quadbin_RasterToGridAvg.execute(constDs, resolution = 21)
        an[IllegalArgumentException] should be thrownBy
            RST_Quadbin_RasterToGridAvg.execute(constDs, resolution = -1)
    }

}
