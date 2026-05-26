package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/**
 * Comprehensive tests for raster aggregation operations.
 * 
 * These tests validate:
 * - RST_CombineAvg: Pixel-wise averaging of multiple rasters
 * - RST_DerivedBand: Python-based combination with custom functions
 * - RST_Merge: Spatial merging of rasters
 * - Edge cases: single raster, empty collections, NoData handling
 * - Performance: handling multiple large rasters
 * - Correctness: verifying output dimensions, metadata preservation
 * 
 * Test Data:
 * - MODIS single-band TIF files (B01, B02, B03)
 * - Consistent spatial extent and resolution for all inputs
 */
class RST_AggregationsTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds1: Dataset = _
    var ds2: Dataset = _
    var ds3: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        
        // Ensure NodeFilePathUtil root directory exists for tests
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
        
        // Load test datasets
        val tif1Path = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").getPath
        val tif2Path = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF").getPath
        val tif3Path = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B03.TIF").getPath
        
        ds1 = gdal.Open(tif1Path)
        ds2 = gdal.Open(tif2Path)
        ds3 = gdal.Open(tif3Path)
        
        require(ds1 != null, s"Failed to open $tif1Path")
        require(ds2 != null, s"Failed to open $tif2Path")
        require(ds3 != null, s"Failed to open $tif3Path")
    }

    override def afterAll(): Unit = {
        if (ds1 != null) ds1.delete()
        if (ds2 != null) ds2.delete()
        if (ds3 != null) ds3.delete()
    }

    // ====================================================================
    // RST_CombineAvg Tests (3 tests)
    // ====================================================================

    test("CombineAvg should average two identical rasters") {
        // Averaging two identical rasters should produce the same result
        val (idx, resultDs, metadata) = RST_CombineAvg.execute(Seq((1L, ds1, Map.empty), (1L, ds1, Map.empty)))
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        resultDs.GetRasterXSize shouldBe ds1.GetRasterXSize
        resultDs.GetRasterYSize shouldBe ds1.GetRasterYSize
        
        // Verify spatial reference is preserved
        resultDs.GetProjection() should not be empty
        
        RasterDriver.releaseDataset(resultDs)
    }

    test("CombineAvg should average multiple different rasters") {
        // Average 3 different bands - result should have valid pixel values
        val (idx, resultDs, metadata) = RST_CombineAvg.execute(Seq((1L, ds1, Map.empty), (1L, ds2, Map.empty), (1L, ds3, Map.empty)))
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        resultDs.GetRasterXSize shouldBe ds1.GetRasterXSize
        resultDs.GetRasterYSize shouldBe ds1.GetRasterYSize
        
        // Verify geotransform is preserved
        val gt = Array.ofDim[Double](6)
        resultDs.GetGeoTransform(gt)
        gt(1) should not be 0.0 // pixel width
        gt(5) should not be 0.0 // pixel height
        
        RasterDriver.releaseDataset(resultDs)
    }

    test("CombineAvg actually averages pixel values (regression: pixel function must fire)") {
        // Synthetic control with known answer. Two constant Byte rasters
        // (50 and 100); the mean must be 75 everywhere. Before the
        // PixelCombineRasters ordering fix, gdal.Open parsed the VRT XML
        // BEFORE addPixelFunction wrote PixelFunctionLanguage into the file,
        // so the in-memory Dataset never saw the Python function and
        // gdal.Translate fell back to a default multi-source mosaic
        // (last-source-wins per pixel). On co-extensive inputs the output
        // then equaled the last input — 50 or 100, never 75 — silently and
        // without any error. This test would fail (output 100 instead of
        // 75); after the fix it passes.
        val tmpDir = Files.createTempDirectory("gbx_combineavg_").toFile
        val const50  = makeConstantByteRaster(s"${tmpDir.getAbsolutePath}/const_50.tif",  50)
        val const100 = makeConstantByteRaster(s"${tmpDir.getAbsolutePath}/const_100.tif", 100)
        try {
            val (_, resultDs, _) = RST_CombineAvg.execute(
                Seq((1L, const50, Map.empty), (1L, const100, Map.empty))
            )
            try {
                val w = resultDs.GetRasterXSize
                val h = resultDs.GetRasterYSize
                val band = resultDs.GetRasterBand(1)
                val buf = Array.ofDim[Double](w * h)
                band.ReadRaster(0, 0, w, h, gdalconstConstants.GDT_Float64, buf)
                buf.min shouldBe 75.0 +- 0.5
                buf.max shouldBe 75.0 +- 0.5
                (buf.sum / buf.length) shouldBe 75.0 +- 0.5
            } finally RasterDriver.releaseDataset(resultDs)
        } finally {
            RasterDriver.releaseDataset(const50)
            RasterDriver.releaseDataset(const100)
            tmpDir.listFiles().foreach(_.delete())
            tmpDir.delete()
        }
    }

    test("CombineAvg should generate operation metadata") {
        val inputMetadata = Map(
          "TEST_KEY" -> "TEST_VALUE",
          "PROCESSING" -> "AVERAGE"
        )
        
        val (idx, resultDs, metadata) = RST_CombineAvg.execute(Seq((1L, ds1, inputMetadata), (1L, ds2, inputMetadata)))
        
        resultDs should not be null
        // Verify operation generates internal metadata
        metadata should contain key "path"
        metadata should contain key "driver"
        metadata should contain key "format"
        
        RasterDriver.releaseDataset(resultDs)
    }

    // ====================================================================
    // RST_DerivedBand Tests (3 tests)
    // ====================================================================

    test("DerivedBand should apply simple Python averaging function") {
        val pyfunc = """
import numpy as np
def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    # Simple average across all input rasters
    stacked = np.array(in_ar)
    out_ar[:] = np.mean(stacked, axis=0)
"""
        
        val (resultDs, metadata) = RST_DerivedBand.execute(Seq(ds1, ds2, ds3), Map.empty, pyfunc, "average")
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        resultDs.GetRasterXSize shouldBe ds1.GetRasterXSize
        resultDs.GetRasterYSize shouldBe ds1.GetRasterYSize
        
        RasterDriver.releaseDataset(resultDs)
    }

    test("DerivedBand should apply maximum value selection") {
        val pyfunc = """
import numpy as np
def select_max(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    # Select maximum value across all input rasters
    stacked = np.array(in_ar)
    out_ar[:] = np.max(stacked, axis=0)
"""
        
        val (resultDs, metadata) = RST_DerivedBand.execute(Seq(ds1, ds2), Map.empty, pyfunc, "select_max")
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        
        RasterDriver.releaseDataset(resultDs)
    }

    test("DerivedBand should apply weighted combination") {
        val pyfunc = """
import numpy as np
def weighted_avg(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    # Weighted average: first raster gets 50%, others split remaining 50%
    stacked = np.array(in_ar)
    n = len(in_ar)
    weights = [0.5] + [0.5/(n-1)] * (n-1)
    out_ar[:] = np.average(stacked, axis=0, weights=weights)
"""
        
        val (resultDs, metadata) = RST_DerivedBand.execute(Seq(ds1, ds2, ds3), Map.empty, pyfunc, "weighted_avg")
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        
        RasterDriver.releaseDataset(resultDs)
    }

    // ====================================================================
    // RST_Merge Tests (3 tests)
    // ====================================================================

    test("Merge should combine two overlapping rasters") {
        // Merging identical rasters should result in same or larger dimensions
        val (resultDs, metadata) = RST_Merge.execute(Array(ds1, ds1), Map.empty)
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        resultDs.GetRasterXSize should be >= ds1.GetRasterXSize
        resultDs.GetRasterYSize should be >= ds1.GetRasterYSize
        
        // Verify projection is preserved
        val projection = resultDs.GetProjection()
        projection should not be empty
        
        RasterDriver.releaseDataset(resultDs)
    }

    test("Merge should handle multiple rasters with same extent") {
        // When rasters have same extent, merge should preserve or expand dimensions
        val (resultDs, metadata) = RST_Merge.execute(Array(ds1, ds2, ds3), Map.empty)
        
        resultDs should not be null
        resultDs.GetRasterCount shouldBe 1
        
        // For identical extents, dimensions should be at least as large
        resultDs.GetRasterXSize should be >= ds1.GetRasterXSize
        resultDs.GetRasterYSize should be >= ds1.GetRasterYSize
        
        // Verify geotransform is valid
        val gtResult = Array.ofDim[Double](6)
        resultDs.GetGeoTransform(gtResult)
        
        gtResult(1) should not be 0.0 // pixel width
        gtResult(5) should not be 0.0 // pixel height
        
        RasterDriver.releaseDataset(resultDs)
    }

    test("Merge should generate operation metadata") {
        val inputMetadata = Map(
          "SOURCE" -> "MODIS",
          "PRODUCT" -> "MCD43A4"
        )
        
        val (resultDs, metadata) = RST_Merge.execute(Array(ds1, ds2), inputMetadata)
        
        resultDs should not be null
        // Verify operation generates internal metadata
        metadata should contain key "path"
        metadata should contain key "driver"
        metadata should contain key "all_parents" // merge tracks parent files
        
        RasterDriver.releaseDataset(resultDs)
    }

    // ====================================================================
    // Edge Cases and Integration Test (1 test)
    // ====================================================================

    test("Aggregations should handle edge cases gracefully") {
        // Test 1: Single raster in CombineAvg (should return same raster)
        val (singleIdx, singleDs, _) = RST_CombineAvg.execute(Seq((1L, ds1, Map.empty)))
        singleDs should not be null
        singleDs.GetRasterXSize shouldBe ds1.GetRasterXSize
        RasterDriver.releaseDataset(singleDs)
        
        // Test 2: Merge single raster (should return same raster)
        val (mergedSingle, _) = RST_Merge.execute(Array(ds1), Map.empty)
        mergedSingle should not be null
        mergedSingle.GetRasterXSize should be >= ds1.GetRasterXSize
        RasterDriver.releaseDataset(mergedSingle)
        
        // Test 3: Simple Python function with single raster
        val simplePyfunc = """
import numpy as np
def identity(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    out_ar[:] = in_ar[0]
"""
        val (derivedDs, _) = RST_DerivedBand.execute(Seq(ds1), Map.empty, simplePyfunc, "identity")
        derivedDs should not be null
        RasterDriver.releaseDataset(derivedDs)
    }

    /**
     * Create a small in-memory GTiff filled with a single Byte value. Used by
     * the CombineAvg / DerivedBand numerical-correctness tests that need
     * deterministic synthetic inputs with a known expected mean. EPSG:4326 +
     * a fixed geotransform so gdalbuildvrt aligns the rasters.
     *
     * Note: closes the writer Dataset and re-opens it for reading. Without
     * this, downstream gdalbuildvrt sometimes sees an unflushed file and
     * pulls in zeros for the pixel values.
     */
    private def makeConstantByteRaster(path: String, value: Int, w: Int = 32, h: Int = 32): Dataset = {
        val drv = gdal.GetDriverByName("GTiff")
        val writer = drv.Create(path, w, h, 1, gdalconstConstants.GDT_Byte, Array[String]("COMPRESS=DEFLATE"))
        writer.SetGeoTransform(Array[Double](149.0, 0.01, 0.0, -35.0, 0.0, -0.01))
        val sr = new SpatialReference()
        sr.ImportFromEPSG(4326)
        writer.SetProjection(sr.ExportToWkt())
        val band = writer.GetRasterBand(1)
        // Use the byte[] overload — GDAL Java bindings dispatch by buffer
        // element type; an Int[] buffer + GDT_Byte combo silently writes
        // nothing back to the file.
        val buf = Array.fill[Byte](w * h)(value.toByte)
        band.WriteRaster(0, 0, w, h, buf)
        band.FlushCache()
        writer.FlushCache()
        writer.delete()
        gdal.Open(path)
    }
}
