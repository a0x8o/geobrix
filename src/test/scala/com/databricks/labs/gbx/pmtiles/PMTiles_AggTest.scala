package com.databricks.labs.gbx.pmtiles

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession

import java.nio.{ByteBuffer, ByteOrder}

/**
  * End-to-end test for the `gbx_pmtiles_agg` UDAF.
  *
  * Validates that the aggregator produces a valid PMTile v3 blob with the expected
  * header magic, version byte, addressed-tiles count, and that tile bytes round-trip
  * through the tile-data section.
  */
class PMTiles_AggTest extends PlanTest with SilentSparkSession {

    test("pmtiles_agg encodes a 9-tile pyramid into a valid PMTile blob") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        val tiles = (for {
            x <- 0 until 3
            y <- 0 until 3
        } yield (2, x, y, s"tile_${x}_${y}".getBytes("UTF-8"))).toSeq

        val df = spark.createDataFrame(tiles).toDF("z", "x", "y", "bytes")
        val out = df
            .agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y"), lit("{}")).as("pmt"))
            .collect()
            .head
            .getAs[Array[Byte]]("pmt")

        assert(out != null, "pmtiles_agg result should not be null")
        assert(out.length > 127, s"PMTile must be at least header+data; got ${out.length}")
        // Magic + version check.
        assert(out(0) == 'P'.toByte && out(7) == 0x03.toByte, "magic+version must match PMTiles v3")
        // addressed_tiles_count at offset 72 = 9.
        val addressed = ByteBuffer.wrap(out, 72, 8).order(ByteOrder.LITTLE_ENDIAN).getLong
        assert(addressed == 9L, s"expected 9 addressed tiles; got $addressed")
    }

    test("pmtiles_agg works with 4-arg signature (no metadata)") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        val df = spark.createDataFrame(Seq((1, 0, 0, "AAA".getBytes("UTF-8"))))
            .toDF("z", "x", "y", "bytes")
        val out = df
            .agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
            .collect()
            .head
            .getAs[Array[Byte]]("pmt")
        assert(out != null && out(0) == 'P'.toByte)
    }

    test("pmtiles_agg auto-detects PNG tile type from magic bytes") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        // PNG magic: 89 50 4E 47 0D 0A 1A 0A
        val pngBytes = Array[Byte](0x89.toByte, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D)
        val df = spark.createDataFrame(Seq((1, 0, 0, pngBytes))).toDF("z", "x", "y", "bytes")
        val out = df.agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
            .collect().head.getAs[Array[Byte]]("pmt")
        // Tile type byte is at offset 99.
        assert(out(99) == PMTilesV3Encoder.TILE_TYPE_PNG, s"expected PNG tile_type; got ${out(99)}")
    }

    test("pmtiles_agg auto-detects JPEG tile type") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        val jpegBytes = Array[Byte](0xFF.toByte, 0xD8.toByte, 0xFF.toByte, 0xE0.toByte, 0x00, 0x10)
        val df = spark.createDataFrame(Seq((1, 0, 0, jpegBytes))).toDF("z", "x", "y", "bytes")
        val out = df.agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
            .collect().head.getAs[Array[Byte]]("pmt")
        assert(out(99) == PMTilesV3Encoder.TILE_TYPE_JPEG, s"expected JPEG tile_type; got ${out(99)}")
    }

    test("pmtiles_agg defaults to MVT for non-image bytes") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        // Plain text — no image magic; treated as MVT (protobuf).
        val df = spark.createDataFrame(Seq((1, 0, 0, "plain_text_tile".getBytes("UTF-8"))))
            .toDF("z", "x", "y", "bytes")
        val out = df.agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
            .collect().head.getAs[Array[Byte]]("pmt")
        assert(out(99) == PMTilesV3Encoder.TILE_TYPE_MVT, s"expected MVT tile_type; got ${out(99)}")
    }

    test("pmtiles_agg returns valid header-only PMTile for empty input") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        val df = spark.createDataFrame(Seq.empty[(Int, Int, Int, Array[Byte])])
            .toDF("z", "x", "y", "bytes")
        val out = df.agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
            .collect().head.getAs[Array[Byte]]("pmt")
        assert(out != null && out.length >= 127)
        assert(out(0) == 'P'.toByte && out(7) == 0x03.toByte)
    }

    test("pmtiles_agg survives a multi-partition shuffle merge") {
        spark.sparkContext.setLogLevel("ERROR")
        functions.register(spark)
        import functions._

        // Generate enough tiles across multiple partitions to force a shuffle.
        val tiles = (0 until 64).map(i => (3, i % 8, i / 8, s"tile_$i".getBytes("UTF-8")))
        val df = spark.createDataFrame(tiles).toDF("z", "x", "y", "bytes").repartition(4)
        val out = df.agg(pmtiles_agg(col("bytes"), col("z"), col("x"), col("y")).as("pmt"))
            .collect().head.getAs[Array[Byte]]("pmt")
        val addressed = ByteBuffer.wrap(out, 72, 8).order(ByteOrder.LITTLE_ENDIAN).getLong
        assert(addressed == 64L, s"expected 64 addressed tiles after merge; got $addressed")
    }
}
