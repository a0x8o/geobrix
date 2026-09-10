package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.CombineStats
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
  * Cross-raster combine family (`gbx_rst_combine{min,max,median,sum,stddev,count}`).
  *
  * Three grid-aligned single-band Float64 tiles with a known NoData hole, so every
  * per-pixel statistic and the all-NoData -> NoData propagation is checked against
  * hand-computed values. Conventions under test: median = mean-of-two-middle on even
  * count; stddev = population (ddof=0); count = number of valid inputs.
  */
class RST_CombineFamilyTest extends AnyFunSuite with BeforeAndAfterAll {

    private val ND = -9999.0

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    /** 2x2 Float64 /vsimem GTiff (EPSG:4326, georeferenced), NoData=-9999, pixels in row-major order. */
    private def tile(values: Array[Double], epsg: Int = 4326, size: Int = 2,
                     gt: Array[Double] = Array(0.0, 1.0, 0.0, 2.0, 0.0, -1.0)): Dataset = {
        val path = s"/vsimem/combine_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val d = gdal.GetDriverByName("GTiff").Create(path, size, size, 1, gdalconstConstants.GDT_Float64)
        d.SetGeoTransform(gt)
        val srs = new org.gdal.osr.SpatialReference(); srs.ImportFromEPSG(epsg)
        d.SetProjection(srs.ExportToWkt()); srs.delete()
        val band = d.GetRasterBand(1)
        band.SetNoDataValue(ND)
        band.WriteRaster(0, 0, size, size, values)
        band.FlushCache(); d.FlushCache(); band.delete()
        d
    }

    private def readBand(ds: Dataset): Array[Double] = {
        val w = ds.GetRasterXSize; val h = ds.GetRasterYSize
        val buf = Array.ofDim[Double](w * h)
        ds.GetRasterBand(1).ReadRaster(0, 0, w, h, buf)
        buf
    }

    // Pixel-major stacks across three aligned tiles:
    //   p0: [1,  2,  3 ]  all valid
    //   p1: [10, ND, 20]  one NoData
    //   p2: [ND, ND, ND]  all NoData  -> NoData out
    //   p3: [4,  4,  4 ]  all valid
    private def stack(): Array[Dataset] = Array(
        tile(Array(1.0, 10.0, ND, 4.0)),
        tile(Array(2.0, ND, ND, 4.0)),
        tile(Array(3.0, 20.0, ND, 4.0))
    )

    private def runStat(stat: String): Array[Double] = {
        val rasters = stack()
        val (resultDs, _) = CombineStats.compute(rasters, Map.empty, stat)
        try {
            resultDs should not be null
            resultDs.GetRasterXSize shouldBe 2
            resultDs.GetRasterYSize shouldBe 2
            // NoData sentinel is stamped on the output band.
            val ndArr = Array.ofDim[java.lang.Double](1)
            resultDs.GetRasterBand(1).GetNoDataValue(ndArr)
            ndArr(0) shouldBe ND
            readBand(resultDs)
        } finally {
            RasterDriver.releaseDataset(resultDs)
            rasters.foreach(RasterDriver.releaseDataset)
        }
    }

    private val tol = 1e-6

    test("combine min excludes NoData; all-NoData pixel -> NoData") {
        val r = runStat("min")
        r(0) shouldBe 1.0 +- tol
        r(1) shouldBe 10.0 +- tol
        r(2) shouldBe ND +- tol
        r(3) shouldBe 4.0 +- tol
    }

    test("combine max excludes NoData; all-NoData pixel -> NoData") {
        val r = runStat("max")
        r(0) shouldBe 3.0 +- tol
        r(1) shouldBe 20.0 +- tol
        r(2) shouldBe ND +- tol
        r(3) shouldBe 4.0 +- tol
    }

    test("combine sum of valid values; all-NoData pixel -> NoData") {
        val r = runStat("sum")
        r(0) shouldBe 6.0 +- tol
        r(1) shouldBe 30.0 +- tol
        r(2) shouldBe ND +- tol
        r(3) shouldBe 12.0 +- tol
    }

    test("combine count of valid inputs; all-NoData pixel -> NoData") {
        val r = runStat("count")
        r(0) shouldBe 3.0 +- tol
        r(1) shouldBe 2.0 +- tol
        r(2) shouldBe ND +- tol
        r(3) shouldBe 3.0 +- tol
    }

    test("combine median (mean-of-two on even count); all-NoData pixel -> NoData") {
        val r = runStat("median")
        r(0) shouldBe 2.0 +- tol        // median(1,2,3)
        r(1) shouldBe 15.0 +- tol       // median(10,20) even -> (10+20)/2
        r(2) shouldBe ND +- tol
        r(3) shouldBe 4.0 +- tol
    }

    test("combine stddev (population, ddof=0); all-NoData pixel -> NoData") {
        val r = runStat("stddev")
        r(0) shouldBe math.sqrt(2.0 / 3.0) +- tol   // population std of {1,2,3}
        r(1) shouldBe 5.0 +- tol                    // population std of {10,20}
        r(2) shouldBe ND +- tol
        r(3) shouldBe 0.0 +- tol
    }

    test("misaligned inputs (different dimensions) raise a clear error pointing at rst_align_to") {
        val a = tile(Array(1.0, 2.0, 3.0, 4.0))
        val b = gdal.GetDriverByName("GTiff").Create(
            s"/vsimem/mis_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif", 3, 3, 1, gdalconstConstants.GDT_Float64)
        b.SetGeoTransform(Array(0.0, 1.0, 0.0, 3.0, 0.0, -1.0))
        val srs = new org.gdal.osr.SpatialReference(); srs.ImportFromEPSG(4326)
        b.SetProjection(srs.ExportToWkt()); srs.delete()
        b.GetRasterBand(1).WriteRaster(0, 0, 3, 3, Array.fill[Double](9)(1.0))
        b.FlushCache()
        try {
            val ex = intercept[IllegalArgumentException] {
                CombineStats.compute(Array(a, b), Map.empty, "sum")
            }
            ex.getMessage.toLowerCase should include("align")
        } finally {
            RasterDriver.releaseDataset(a); RasterDriver.releaseDataset(b)
        }
    }

    test("misaligned inputs (different CRS) raise a clear error") {
        val a = tile(Array(1.0, 2.0, 3.0, 4.0), epsg = 4326)
        val b = tile(Array(1.0, 2.0, 3.0, 4.0), epsg = 3857)
        try {
            val ex = intercept[IllegalArgumentException] {
                CombineStats.compute(Array(a, b), Map.empty, "min")
            }
            ex.getMessage.toLowerCase should include("align")
        } finally {
            RasterDriver.releaseDataset(a); RasterDriver.releaseDataset(b)
        }
    }

    test("RST_CombineSum expression execute wires through to the operations reducer") {
        val rasters = stack()
        val (_, resultDs, _) = RST_CombineSum.execute(rasters.map(d => (1L, d, Map.empty[String, String])).toSeq)
        try {
            val r = readBand(resultDs)
            r(0) shouldBe 6.0 +- tol
            r(3) shouldBe 12.0 +- tol
        } finally {
            RasterDriver.releaseDataset(resultDs)
            rasters.foreach(RasterDriver.releaseDataset)
        }
    }
}
