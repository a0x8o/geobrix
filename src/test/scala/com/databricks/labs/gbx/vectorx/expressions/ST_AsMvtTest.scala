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

}
