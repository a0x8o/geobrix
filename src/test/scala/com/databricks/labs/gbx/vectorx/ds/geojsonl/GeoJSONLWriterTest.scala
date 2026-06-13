package com.databricks.labs.gbx.vectorx.ds.geojsonl

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.Files

/**
  * Tests for the heavyweight multi-file `geojsonl` vector writer.
  *
  * Like the lightweight `geojsonl_gbx`, it writes a DIRECTORY of newline-delimited GeoJSONL
  * shards — one per partition, NO driver merge — with an optional `maxRecordsPerFile` that splits
  * a partition into several shards. Round-trips with the `geojson_ogr` (`multi=true`) directory
  * reader.
  */
class GeoJSONLWriterTest extends PlanTest with SilentSparkSession {

    private val gf = new GeometryFactory()

    /** A DataFrame in the *_ogr reader's writer shape: geom_0 (WKB) + srid + proj + attrs. */
    private def wkbDf(n: Int) = {
        val rows = (0 until n).map { i =>
            (s"$i", i, JTS.toWKB(gf.createPoint(new Coordinate(i.toDouble / 10.0, 40.0))), "4326", "")
        }
        spark.createDataFrame(rows)
            .toDF("name", "pop", "geom_0", "geom_0_srid", "geom_0_srid_proj")
    }

    private def shards(dir: java.io.File): Array[String] =
        Option(dir.list()).getOrElse(Array.empty)
            .filter(n => n.endsWith(".geojsonl") || n.endsWith(".geojsons"))

    test("geojsonl writer emits exactly one shard per partition and round-trips") {
        spark.sparkContext.setLogLevel("ERROR")
        val out = Files.createTempDirectory("gbx_geojsonl_out_").toFile
        out.delete() // let the writer create it
        val df = wkbDf(6).repartition(3)
        val nparts = df.rdd.getNumPartitions

        df.write.format("geojsonl").mode("overwrite").save(out.getAbsolutePath)

        out.isDirectory shouldBe true
        val sh = shards(out)
        sh.length shouldEqual nparts

        val back = spark.read.format("geojson_ogr").option("multi", "true").load(out.getAbsolutePath)
        back.count() shouldEqual 6L
        // The reader type-infers attribute columns, so 'name' may come back as a numeric type;
        // compare by string value rather than assuming String.
        val names = back.collect().map(_.get(back.schema.fieldIndex("name")).toString).toSet
        names shouldEqual (0 until 6).map(_.toString).toSet
        // geometry column round-trips as a parseable point
        val gcol = back.schema.fields.map(_.name).find(_.endsWith("_srid")).get.dropRight("_srid".length)
        val wkbs = back.select(gcol).collect().map(_.getAs[Array[Byte]](0))
        wkbs.foreach(b => JTS.fromWKB(b).getGeometryType shouldEqual "Point")
    }

    test("maxRecordsPerFile splits one partition into ceil(M/k) shards") {
        spark.sparkContext.setLogLevel("ERROR")
        val out = Files.createTempDirectory("gbx_geojsonl_split_").toFile
        out.delete()
        val (m, k) = (10, 3) // ceil(10/3) == 4
        wkbDf(m).repartition(1).write.format("geojsonl").mode("overwrite")
            .option("maxRecordsPerFile", k.toString).save(out.getAbsolutePath)

        val expected = (m + k - 1) / k // ceil(m/k) = ceil(10/3) = 4
        shards(out).length shouldEqual expected

        val back = spark.read.format("geojson_ogr").option("multi", "true").load(out.getAbsolutePath)
        back.count() shouldEqual m.toLong
    }

    test("overwrite clears prior shards") {
        spark.sparkContext.setLogLevel("ERROR")
        val out = Files.createTempDirectory("gbx_geojsonl_ow_").toFile
        out.delete()
        wkbDf(8).repartition(4).write.format("geojsonl").mode("overwrite").save(out.getAbsolutePath)
        val first = shards(out).toSet
        wkbDf(2).repartition(1).write.format("geojsonl").mode("overwrite").save(out.getAbsolutePath)
        val second = shards(out).toSet
        assert(first.intersect(second).isEmpty, "overwrite left stale shards behind")
        val back = spark.read.format("geojson_ogr").option("multi", "true").load(out.getAbsolutePath)
        back.count() shouldEqual 2L
    }

    test("append mode is rejected") {
        spark.sparkContext.setLogLevel("ERROR")
        val out = Files.createTempDirectory("gbx_geojsonl_append_").toFile
        out.delete()
        wkbDf(4).repartition(2).write.format("geojsonl").mode("overwrite").save(out.getAbsolutePath)
        val ex = intercept[Exception] {
            wkbDf(4).write.format("geojsonl").mode("append").save(out.getAbsolutePath)
        }
        // unwrap to the root message
        val msg = Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null)
            .map(_.getMessage).filter(_ != null).mkString(" | ")
        assert(msg.toLowerCase.contains("append"), s"expected an 'append' rejection, got: $msg")
    }
}
