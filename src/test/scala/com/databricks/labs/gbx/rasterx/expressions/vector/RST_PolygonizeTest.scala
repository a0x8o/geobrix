package com.databricks.labs.gbx.rasterx.expressions.vector

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Direct-execute tests for [[RST_Polygonize]].
 *
 *  Builds a tiny 8x8 in-memory raster with two distinct value regions and
 *  checks that polygonize emits one feature per region carrying the expected
 *  burn value.
 */
class RST_PolygonizeTest extends AnyFunSuite with BeforeAndAfterAll {

    private var srcDs: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)

        // 8x8 EPSG:4326 raster covering (0, 0) -> (8, 8).
        // Left half = value 1.0, right half = value 2.0.
        val drv = gdal.GetDriverByName("MEM")
        srcDs = drv.Create("", 8, 8, 1, gdalconstConstants.GDT_Float64)
        srcDs.SetGeoTransform(Array(0.0, 1.0, 0.0, 8.0, 0.0, -1.0))
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(4326)
        srcDs.SetProjection(sr.ExportToWkt())
        sr.delete()
        val band = srcDs.GetRasterBand(1)
        val pixels = (0 until 64).map { i =>
            val col = i % 8
            if (col < 4) 1.0 else 2.0
        }.toArray
        band.WriteRaster(0, 0, 8, 8, pixels)
        band.FlushCache()
    }

    override def afterAll(): Unit = {
        if (srcDs != null) srcDs.delete()
    }

    test("RST_Polygonize.execute emits one polygon per value region with the correct value") {
        val result = RST_Polygonize.execute(srcDs, 1, 4)
        result should not be null
        val n = result.numElements()
        n shouldBe 2

        val values = (0 until n).map(i => result.getStruct(i, 2).getDouble(1)).toSet
        values shouldBe Set(1.0, 2.0)

        // Each feature's geometry must be non-empty WKB.
        (0 until n).foreach { i =>
            val wkb = result.getStruct(i, 2).getBinary(0)
            wkb should not be null
            wkb.length should be > 0
        }
    }

    test("RST_Polygonize.execute rejects invalid band / connectedness") {
        an[IllegalArgumentException] should be thrownBy {
            RST_Polygonize.execute(srcDs, 5, 4)
        }
        an[IllegalArgumentException] should be thrownBy {
            RST_Polygonize.execute(srcDs, 1, 7)
        }
    }

}
