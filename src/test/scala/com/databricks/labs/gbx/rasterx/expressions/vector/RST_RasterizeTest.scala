package com.databricks.labs.gbx.rasterx.expressions.vector

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.rasterx.util.VectorRasterBridge
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.gdal
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Direct-execute tests for [[RST_Rasterize]] and [[VectorRasterBridge]].
 *
 *  We exercise `execute(...)` directly (the GDAL/Spark integration boundary)
 *  on a small 32x32 EPSG:4326 extent. That avoids a full Spark session bootstrap
 *  and keeps wall-clock under a second.
 */
class RST_RasterizeTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    private def squareWkb(): Array[Byte] = {
        val gf = new GeometryFactory()
        val poly = gf.createPolygon(Array(
            new Coordinate(0.0, 0.0),
            new Coordinate(10.0, 0.0),
            new Coordinate(10.0, 10.0),
            new Coordinate(0.0, 10.0),
            new Coordinate(0.0, 0.0)
        ))
        JTS.toWKB(poly)
    }

    test("VectorRasterBridge.buildEmptyRaster rejects degenerate extents") {
        an[IllegalArgumentException] should be thrownBy {
            VectorRasterBridge.buildEmptyRaster(0, 0, 0, 10, 32, 32, 4326)
        }
        an[IllegalArgumentException] should be thrownBy {
            VectorRasterBridge.buildEmptyRaster(0, 0, 10, 10, 0, 32, 4326)
        }
    }

    test("RST_Rasterize.execute burns the value into a covered raster cell and returns GTiff metadata") {
        // 32x32 raster covering (0,0) -> (10,10); the square covers the whole extent.
        val row = RST_Rasterize.execute(
            squareWkb(), 42.0,
            0.0, 0.0, 10.0, 10.0,
            32, 32, 4326,
            ExpressionConfigTestUtil.encodedEmpty()
        )
        row should not be null

        // tile row = (cellid:Long, raster:Binary, metadata:Map)
        val bytes = row.getBinary(1)
        bytes should not be null
        bytes.length should be > 0

        // GTiff magic: "II*\0" (little-endian) or "MM\0*" (big-endian).
        val isLE = bytes(0) == 'I'.toByte && bytes(1) == 'I'.toByte
        val isBE = bytes(0) == 'M'.toByte && bytes(1) == 'M'.toByte
        (isLE || isBE) shouldBe true

        // Sanity-check on read-back: open the bytes, read a pixel from the center.
        val tmpPath = s"/vsimem/test_rasterize_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        gdal.FileFromMemBuffer(tmpPath, bytes)
        val ds = gdal.Open(tmpPath)
        try {
            ds should not be null
            val band = ds.GetRasterBand(1)
            val buf = new Array[Double](1)
            // Pixel at (16, 16) is inside the burned polygon.
            band.ReadRaster(16, 16, 1, 1, buf)
            buf(0) shouldBe 42.0
        } finally {
            ds.delete()
            gdal.Unlink(tmpPath)
        }
    }

}

/** Tiny helper to build the b64-encoded empty ExpressionConfig used by direct-execute tests. */
private object ExpressionConfigTestUtil {
    import com.databricks.labs.gbx.expressions.ExpressionConfig
    import org.apache.hadoop.conf.Configuration
    import org.apache.spark.unsafe.types.UTF8String
    import org.apache.spark.util.SerializableConfiguration

    def encodedEmpty(): UTF8String = {
        val cfg = new ExpressionConfig(Map.empty[String, String], new SerializableConfiguration(new Configuration()))
        val baos = new java.io.ByteArrayOutputStream()
        val oos = new java.io.ObjectOutputStream(baos)
        oos.writeObject(cfg)
        oos.close()
        UTF8String.fromString(java.util.Base64.getEncoder.encodeToString(baos.toByteArray))
    }
}
