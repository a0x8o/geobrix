package com.databricks.labs.gbx.vectorx.ds.ogr

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.ogr.{FieldDefn, ogr, ogrConstants}
import org.gdal.osr.SpatialReference
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Contract tests for the OGR reader:
  *   - bare .shp file path reads correctly (sidecar-bundle staging fix)
  *   - multi-stem divergent-schema directory raises IllegalArgumentException (B4)
  *   - multi-stem same-schema directory reads without error (B4)
  */
class OgrReaderContractTest extends PlanTest with SilentSparkSession {

    // ---------------------------------------------------------------------------
    // Helper: write a minimal ESRI Shapefile to a temp dir under a given stem.
    // Fields describe the schema; the file will have one POINT feature.
    // ---------------------------------------------------------------------------
    private def writeShapefile(dir: java.io.File, stem: String, fields: Seq[(String, Int)]): Unit = {
        GDALManager.initOgr()
        val drv = ogr.GetDriverByName("ESRI Shapefile")
        require(drv != null, "ESRI Shapefile driver not available")
        val shpPath = new java.io.File(dir, s"$stem.shp").getAbsolutePath
        val srs = new SpatialReference()
        srs.ImportFromEPSG(4326)
        val ds = drv.CreateDataSource(shpPath)
        require(ds != null, s"Could not create datasource at $shpPath")
        val layer = ds.CreateLayer(stem, srs, ogrConstants.wkbPoint)
        require(layer != null, s"Could not create layer in $shpPath")
        fields.foreach { case (name, ftype) => layer.CreateField(new FieldDefn(name, ftype)) }
        val fdef = layer.GetLayerDefn
        val feat = new org.gdal.ogr.Feature(fdef)
        feat.SetGeometryDirectly(ogr.CreateGeometryFromWkt("POINT (0 0)"))
        layer.CreateFeature(feat)
        ds.FlushCache()
        ds.delete()
    }

    // ---------------------------------------------------------------------------
    // Pre-existing contract tests (B3)
    // ---------------------------------------------------------------------------

    test("shapefile_ogr must read a bare .shp path (sidecar bundle staging)") {
        val shpPath = this.getClass
            .getResource("/binary/shapefile/map.shp")
            .toString
            .replace("file:", "")

        val df = spark.read
            .format("shapefile_ogr")
            .load(shpPath)

        df.count() should be > 0L
    }

    test("shapefile_ogr must read a bare .shp path for the elevation fixture") {
        val shpPath = this.getClass
            .getResource("/binary/elevation/sd46_dtm_breakline.shp")
            .toString
            .replace("file:", "")

        val df = spark.read
            .format("shapefile_ogr")
            .load(shpPath)

        df.count() should be > 0L
    }

    // ---------------------------------------------------------------------------
    // B4: schema-divergence guard
    // ---------------------------------------------------------------------------

    test("shapefile_ogr must raise on a directory with differing-schema shapefiles") {
        val tmpDir = Files.createTempDirectory("gbx_b4_diverge_").toFile
        // roads: one string field 'name'; rivers: one real field 'width' -- different schemas
        writeShapefile(tmpDir, "roads", Seq("name" -> ogrConstants.OFTString))
        writeShapefile(tmpDir, "rivers", Seq("width" -> ogrConstants.OFTReal))

        val ex = intercept[IllegalArgumentException] {
            spark.read.format("shapefile_ogr").load(tmpDir.getAbsolutePath)
        }
        val msg = ex.getMessage
        msg should include("differing schemas")
        msg should include("load them separately")
        // Both stems appear in the message
        (msg.contains("roads") || msg.contains("rivers")) shouldBe true
    }

    test("shapefile_ogr must NOT raise on a directory with same-schema shapefiles") {
        val tmpDir = Files.createTempDirectory("gbx_b4_same_").toFile
        // Both shapefiles have the same field list — union read should proceed normally
        val fields = Seq("name" -> ogrConstants.OFTString, "length" -> ogrConstants.OFTReal)
        writeShapefile(tmpDir, "roads", fields)
        writeShapefile(tmpDir, "rivers", fields)

        noException shouldBe thrownBy {
            val df = spark.read.format("shapefile_ogr").load(tmpDir.getAbsolutePath)
            df.count()
        }
    }

}
