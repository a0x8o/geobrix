package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.vectorx
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

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

    test("st_asmvt string-overload should accept a plain layer name") {
        spark.sparkContext.setLogLevel("ERROR")
        vectorx.functions.register(spark)
        // Idempotent second call exercises the early-return branch in register().
        vectorx.functions.register(spark)
        import vectorx.functions._

        val gf = new GeometryFactory()
        val pt = gf.createPoint(new Coordinate(0.5, 0.5))
        val df = spark.createDataFrame(Seq(
            (JTS.toWKB(pt), "alpha", 1L)
        )).toDF("geom_wkb", "name", "id")

        val mvt = df.agg(st_asmvt(col("geom_wkb"), struct(col("name"), col("id")), "layer_str").as("mvt"))
            .collect().head.getAs[Array[Byte]]("mvt")
        assert(mvt != null && mvt.nonEmpty)
        assert(new String(mvt, "UTF-8").contains("layer_str"))
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

    test("MvtAcc serialize/deserialize should round-trip null attrs") {
        // Drives the `attrs == null → writeInt(-1)` branch in MvtAcc.serialize and the
        // mirroring `aLen < 0 → null` branch in MvtAcc.deserialize.
        val gf = new GeometryFactory()
        val wkb = JTS.toWKB(gf.createPoint(new Coordinate(0.0, 0.0)))
        val acc = MvtAcc.empty("L")
        acc.add(wkb, null)             // null attrs path
        acc.add(wkb, Array[Byte](1, 2, 3)) // non-null attrs path
        val bytes = acc.serialize
        val round = MvtAcc.deserialize(bytes)
        assert(round.layerName == "L")
        assert(round.features.length == 2)
        assert(round.features(0)._2 == null)
        assert(round.features(1)._2.sameElements(Array[Byte](1, 2, 3)))
    }

    test("MvtAcc.add should be a no-op for empty WKB and null WKB") {
        val acc = MvtAcc.empty("L")
        acc.add(null, Array[Byte](1))
        acc.add(Array.emptyByteArray, Array[Byte](1))
        assert(acc.features.isEmpty)
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

}
