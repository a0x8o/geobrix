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

    // ---------------------------------------------------------------------------
    // C1 (revised): write-guard — read-only OGR formats must fail writes with a clear message.
    // Mechanism: OGR_DataSource.supportsExternalMetadata=true routes writes through
    // OGR_Table.newWriteBuilder, which throws immediately. inferSchema is NOT called on writes,
    // so reads are completely unaffected.
    // ---------------------------------------------------------------------------

    /** Walk the exception chain to find the root cause. */
    private def rootCause(t: Throwable): Throwable =
        if (t.getCause == null) t else rootCause(t.getCause)

    /** Assert that a write of the given format to the given path produces a clear read-only error. */
    private def assertWriteRejected(fmt: String, path: String, altName: String): Unit = {
        val df = spark.range(1).toDF("id")
        val ex = intercept[Exception] {
            df.write.format(fmt).mode("overwrite").save(path)
        }
        val root = rootCause(ex)
        root.getMessage should include("read-only")
        root.getMessage should include(altName)
        root.getClass.getSimpleName should not include "NoSuchFileException"
    }

    // shapefile_ogr: new (non-existent) path
    test("shapefile_ogr must reject writes to a new path with a read-only error naming shapefile_gbx") {
        val tmpPath = Files.createTempDirectory("gbx_c1_shp_").toFile.getAbsolutePath
        assertWriteRejected("shapefile_ogr", tmpPath + "/nonexistent_output", "shapefile_gbx")
    }

    // shapefile_ogr: existing path — guard must fire for existing paths too
    test("shapefile_ogr must reject writes to an existing path with a read-only error naming shapefile_gbx") {
        val existingDir = Files.createTempDirectory("gbx_c1_shp_exist_").toFile.getAbsolutePath
        assertWriteRejected("shapefile_ogr", existingDir, "shapefile_gbx")
    }

    // gpkg_ogr: new path
    test("gpkg_ogr must reject writes to a new path with a read-only error naming gpkg_gbx") {
        val tmpPath = Files.createTempDirectory("gbx_c1_gpkg_").toFile.getAbsolutePath
        assertWriteRejected("gpkg_ogr", tmpPath + "/nonexistent_output", "gpkg_gbx")
    }

    // gpkg_ogr: existing path
    test("gpkg_ogr must reject writes to an existing path with a read-only error naming gpkg_gbx") {
        val existingDir = Files.createTempDirectory("gbx_c1_gpkg_exist_").toFile.getAbsolutePath
        assertWriteRejected("gpkg_ogr", existingDir, "gpkg_gbx")
    }

    // file_gdb_ogr: new path
    test("file_gdb_ogr must reject writes to a new path with a read-only error naming file_gdb_gbx") {
        val tmpPath = Files.createTempDirectory("gbx_c1_gdb_").toFile.getAbsolutePath
        assertWriteRejected("file_gdb_ogr", tmpPath + "/nonexistent_output", "file_gdb_gbx")
    }

    // file_gdb_ogr: existing path
    test("file_gdb_ogr must reject writes to an existing path with a read-only error naming file_gdb_gbx") {
        val existingDir = Files.createTempDirectory("gbx_c1_gdb_exist_").toFile.getAbsolutePath
        assertWriteRejected("file_gdb_ogr", existingDir, "file_gdb_gbx")
    }

    // geojson_ogr: new path
    test("geojson_ogr must reject writes to a new path with a read-only error naming geojson_gbx") {
        val tmpPath = Files.createTempDirectory("gbx_c1_gjson_").toFile.getAbsolutePath
        assertWriteRejected("geojson_ogr", tmpPath + "/nonexistent_output", "geojson_gbx")
    }

    // geojson_ogr: existing path
    test("geojson_ogr must reject writes to an existing path with a read-only error naming geojson_gbx") {
        val existingDir = Files.createTempDirectory("gbx_c1_gjson_exist_").toFile.getAbsolutePath
        assertWriteRejected("geojson_ogr", existingDir, "geojson_gbx")
    }

    // generic ogr: new path
    test("ogr must reject writes to a new path with a read-only error listing _gbx alternatives") {
        val tmpPath = Files.createTempDirectory("gbx_c1_ogr_").toFile.getAbsolutePath
        assertWriteRejected("ogr", tmpPath + "/nonexistent_output", "shapefile_gbx")
    }

    // generic ogr: existing path
    test("ogr must reject writes to an existing path with a read-only error listing _gbx alternatives") {
        val existingDir = Files.createTempDirectory("gbx_c1_ogr_exist_").toFile.getAbsolutePath
        assertWriteRejected("ogr", existingDir, "shapefile_gbx")
    }

    // Regression: a read of a NONEXISTENT path must NOT show the write-guard message.
    // Previously (a94d05b), inferSchema had a path-existence check that showed "read-only" even
    // for reads of typo paths. This test confirms that consequence is gone.
    test("shapefile_ogr read of a nonexistent path must NOT produce a read-only error message") {
        val ex = intercept[Exception] {
            spark.read.format("shapefile_ogr").load("/tmp/does_not_exist_gbx_c1_regression.shp").count()
        }
        // The error must be a not-found / no-files error, not the write-guard message
        val root = rootCause(ex)
        root.getMessage should not include "read-only"
        root.getMessage should not include "shapefile_gbx"
    }

    // Regression: existing reads must still work after the mechanism change
    test("shapefile_ogr read must NOT be affected by the write guard") {
        val shpPath = this.getClass
            .getResource("/binary/shapefile/map.shp")
            .toString
            .replace("file:", "")

        val df = spark.read.format("shapefile_ogr").load(shpPath)
        df.count() should be > 0L
    }

}
