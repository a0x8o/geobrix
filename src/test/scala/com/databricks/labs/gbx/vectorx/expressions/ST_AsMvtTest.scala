package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.gdal.gdal
import org.gdal.ogr.ogr
import org.gdal.ogr.ogrConstants
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

import java.util.{Vector => JVector}
import scala.util.Try

/**
 * Test helper: re-read MVT protobuf bytes through the OGR MVT driver and return the OGR
 * field type of each requested attribute. Used to assert native value typing (int/double),
 * not the legacy all-`OFTString` behavior.
 *
 * The OGR MVT driver opens a standalone single-tile `.pbf` directly via the `MVT:` prefix —
 * no directory layout or `metadata.json` needed. We stage the bytes under `/vsimem/<uuid>.pbf`
 * and open `MVT:<path>`. OGR/GDAL is registered only via the synchronized `GDALManager`
 * guards (per the repo thread-safety rule). Note the driver injects a synthetic `mvt_id`
 * field (OFTInteger64) into every layer — we enumerate fields by name and ignore it.
 */
object MvtTestUtil {

    /** Returns the OGR field type (e.g. `OFTInteger`, `OFTReal`, `OFTString`) for each name. */
    def readFieldTypes(
        mvtBytes: Array[Byte],
        layerName: String,
        fieldNames: Seq[String]
    ): Map[String, Int] = {
        GDALManager.initOgr()
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val path = s"/vsimem/gbx_mvt_read_$uuid.pbf"
        gdal.FileFromMemBuffer(path, mvtBytes)

        val ds = ogr.Open(s"MVT:$path", false)
        try {
            require(ds != null, s"OGR MVT driver failed to open $path: ${gdal.GetLastErrorMsg()}")
            val layer = ds.GetLayerByName(layerName)
            require(layer != null, s"layer '$layerName' not found in re-read MVT")
            val defn = layer.GetLayerDefn()
            val byName: Map[String, Int] =
                (0 until defn.GetFieldCount()).map { i =>
                    val fd = defn.GetFieldDefn(i)
                    fd.GetName() -> fd.GetType()
                }.toMap
            fieldNames.map { n =>
                n -> byName.getOrElse(n, throw new AssertionError(
                    s"field '$n' not present in re-read MVT; present: ${byName.keys.mkString(",")}"))
            }.toMap
        } finally {
            if (ds != null) ds.delete()
            Try(gdal.Unlink(path))
        }
    }
}

class ST_AsMvtTest extends PlanTest with SilentSparkSession {

    test("st_asmvt should encode a single point feature into a non-empty MVT blob") {
        spark.sparkContext.setLogLevel("ERROR")
        vectorx.functions.register(spark)
        import vectorx.functions._

        val gf = new GeometryFactory()
        val pt = gf.createPoint(new Coordinate(0.5, 0.5))
        val df = spark.createDataFrame(Seq(
            (JTS.toWKB(pt), "alpha", 1L)
        )).toDF("geom_wkb", "name", "id")

        val out = df
            .agg(st_asmvt(col("geom_wkb"), struct(col("name"), col("id")), lit("layer1")).as("mvt"))
            .collect()

        assert(out.length == 1)
        val mvtBytes = out.head.getAs[Array[Byte]]("mvt")
        assert(mvtBytes != null && mvtBytes.nonEmpty)
        assert((mvtBytes(0) & 0xff) == 0x1a)
    }

    test("st_asmvt should aggregate multiple features into a single MVT blob") {
        spark.sparkContext.setLogLevel("ERROR")
        vectorx.functions.register(spark)
        import vectorx.functions._

        val gf = new GeometryFactory()
        val features = Seq(
            (JTS.toWKB(gf.createPoint(new Coordinate(0.1, 0.1))), "a", 1L),
            (JTS.toWKB(gf.createPoint(new Coordinate(0.5, 0.5))), "b", 2L),
            (JTS.toWKB(gf.createPoint(new Coordinate(0.9, 0.9))), "c", 3L)
        )
        val df = spark.createDataFrame(features).toDF("geom_wkb", "name", "id")

        val mvt = df.agg(st_asmvt(col("geom_wkb"), struct(col("name"), col("id")), lit("points")).as("mvt"))
            .collect().head.getAs[Array[Byte]]("mvt")

        assert(mvt != null && mvt.length > 0)
        val asStr = new String(mvt, "UTF-8")
        assert(asStr.contains("points"))
    }

    test("st_asmvt should drop null WKB rows in update") {
        spark.sparkContext.setLogLevel("ERROR")
        vectorx.functions.register(spark)
        import vectorx.functions._

        val gf = new GeometryFactory()
        val pt = gf.createPoint(new Coordinate(0.5, 0.5))
        // One real WKB and one null — the null row must be dropped without raising.
        val df = spark.createDataFrame(Seq(
            (JTS.toWKB(pt), "alpha", 1L),
            (null, "ignored", 99L)
        )).toDF("geom_wkb", "name", "id")

        val mvt = df.agg(st_asmvt(col("geom_wkb"), struct(col("name"), col("id")), lit("mixed")).as("mvt"))
            .collect().head.getAs[Array[Byte]]("mvt")
        assert(mvt != null && mvt.nonEmpty)
    }

    test("st_asmvt should produce a non-null MVT for an empty group") {
        spark.sparkContext.setLogLevel("ERROR")
        vectorx.functions.register(spark)
        import vectorx.functions._

        val df = spark.createDataFrame(Seq.empty[(Array[Byte], String, Long)])
            .toDF("geom_wkb", "name", "id")

        val out = df.agg(st_asmvt(col("geom_wkb"), struct(col("name"), col("id")), lit("empty")).as("mvt"))
            .collect()

        assert(out.length == 1)
        val mvt = out.head.getAs[Array[Byte]]("mvt")
        assert(mvt != null)
    }

    test("st_asmvt encodes numeric attributes with native MVT value types") {
        spark.sparkContext.setLogLevel("ERROR")
        vectorx.functions.register(spark)
        import vectorx.functions._

        val gf = new GeometryFactory()
        val pt = gf.createPoint(new Coordinate(0.5, 0.5))
        val df = spark.createDataFrame(Seq(
            (JTS.toWKB(pt), 42, 3.5)
        )).toDF("geom_wkb", "pop", "h")

        val mvtBytes = df.agg(
            st_asmvt(col("geom_wkb"), struct(col("pop"), col("h")), lit("layer1")).as("mvt")
        ).collect().head.getAs[Array[Byte]]("mvt")
        assert(mvtBytes != null && mvtBytes.nonEmpty)

        val types = MvtTestUtil.readFieldTypes(mvtBytes, "layer1", Seq("pop", "h"))
        assert(
          types("pop") == ogrConstants.OFTInteger || types("pop") == ogrConstants.OFTInteger64,
          s"expected pop to be integer, got OGR type ${types("pop")}"
        )
        assert(
          types("h") == ogrConstants.OFTReal,
          s"expected h to be real, got OGR type ${types("h")}"
        )
    }

}
