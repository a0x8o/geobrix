package com.databricks.labs.gbx.vectorx.ds.ogr

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

/** Contract tests for the OGR reader: verifies that a bare .shp file path (without a containing
  * directory) can be read via shapefile_ogr. Prior to the fix, GDAL threw "Unable to open .shx"
  * because stageHeadForSchemaSpark staged only the .shp — the parent-dir sidecar discovery was
  * never invoked when the caller passed a single-file path. */
class OgrReaderContractTest extends PlanTest with SilentSparkSession {

    test("shapefile_ogr must read a bare .shp path (sidecar bundle staging)") {
        // Use the test fixture that has map.shp + map.shx + map.dbf alongside it.
        // Passing the bare .shp path triggers the sidecar-discovery fix: listDataFilesSpark
        // returns [map.shp] only, so stageHeadForSchemaSpark must discover map.shx / map.dbf
        // from the parent directory.
        val shpPath = this.getClass
            .getResource("/binary/shapefile/map.shp")
            .toString
            .replace("file:", "")

        val df = spark.read
            .format("shapefile_ogr")
            .load(shpPath)

        val count = df.count()
        count should be > 0L
    }

    test("shapefile_ogr must read a bare .shp path for the elevation fixture") {
        val shpPath = this.getClass
            .getResource("/binary/elevation/sd46_dtm_breakline.shp")
            .toString
            .replace("file:", "")

        val df = spark.read
            .format("shapefile_ogr")
            .load(shpPath)

        val count = df.count()
        count should be > 0L
    }

}
