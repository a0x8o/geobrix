package com.databricks.labs.gbx.pmtiles

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession

import java.io.IOException
import java.nio.file.{Files, Path => JPath, Paths}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

/**
  * End-to-end tests for the `pmtiles` DataSource writer.
  *
  * Covers single-partition write, multi-partition shuffle + commit, header byte integrity,
  * graceful schema validation, and the "read not supported" guard.
  */
class PMTiles_DataSourceTest extends PlanTest with SilentSparkSession {

    private def tmpFile(prefix: String): String = {
        val dir = Files.createTempDirectory(s"pmtiles-test-$prefix-")
        // Use a unique filename inside a fresh temp dir so scratch _part_* files don't collide
        // with parallel test runs (each suite-class has its own per-test tempdir).
        s"${dir.toAbsolutePath.toString}/out-${UUID.randomUUID()}.pmtiles"
    }

    private def deleteRecursively(p: JPath): Unit = {
        if (!Files.exists(p)) return
        if (Files.isDirectory(p)) {
            val it = Files.list(p)
            try it.forEach(child => deleteRecursively(child)) finally it.close()
        }
        try Files.delete(p) catch { case _: IOException => () }
    }

    test("DataSource writes a single PMTile file from 100 tiles across 4 partitions") {
        spark.sparkContext.setLogLevel("ERROR")
        val tiles = (for {
            x <- 0 until 10
            y <- 0 until 10
        } yield (5, x, y, s"tile_${x}_${y}_payload".getBytes("UTF-8")))
        val df = spark.createDataFrame(tiles).toDF("z", "x", "y", "bytes").repartition(4)

        val outPath = tmpFile("multi")
        try {
            df.write.format("pmtiles").mode("overwrite").save(outPath)

            // Verify the canonical single-file output exists and is well-formed.
            assert(Files.exists(Paths.get(outPath)), s"output $outPath does not exist")
            val bytes = Files.readAllBytes(Paths.get(outPath))
            assert(bytes.length >= 127, s"PMTile must include 127-byte header; got ${bytes.length}")
            // Magic + version.
            assert(bytes(0) == 'P'.toByte, "first byte must be 'P'")
            assert(bytes(7) == 0x03.toByte, "version byte must be 3")
            // addressed_tiles_count at offset 72 — must be at least 100 (RLE may reduce entries
            // but addressed count is the actual tile count before RLE).
            val addressed = ByteBuffer.wrap(bytes, 72, 8).order(ByteOrder.LITTLE_ENDIAN).getLong
            assert(addressed >= 100L, s"expected >= 100 addressed tiles; got $addressed")
            // No leftover scratch files in the same directory.
            val parent = Paths.get(outPath).getParent
            val scratch = Files.list(parent)
            try {
                val remainingScratch = scala.collection.mutable.ArrayBuffer.empty[String]
                scratch.forEach(p => {
                    val name = p.getFileName.toString
                    if (name.startsWith("_part_") || name.endsWith(".tdata") || name.endsWith(".entries")) {
                        remainingScratch += name
                    }
                })
                assert(remainingScratch.isEmpty, s"scratch files left behind: ${remainingScratch.mkString(", ")}")
            } finally scratch.close()
        } finally {
            deleteRecursively(Paths.get(outPath).getParent)
        }
    }

    test("DataSource writes from a single-partition DataFrame") {
        spark.sparkContext.setLogLevel("ERROR")
        val df = spark.createDataFrame(Seq(
            (1, 0, 0, "AAA".getBytes("UTF-8")),
            (1, 0, 1, "BBB".getBytes("UTF-8")),
            (1, 1, 0, "CCC".getBytes("UTF-8")),
            (1, 1, 1, "DDD".getBytes("UTF-8"))
        )).toDF("z", "x", "y", "bytes").coalesce(1)

        val outPath = tmpFile("single")
        try {
            df.write.format("pmtiles").mode("overwrite").save(outPath)
            val bytes = Files.readAllBytes(Paths.get(outPath))
            assert(bytes(0) == 'P'.toByte && bytes(7) == 0x03.toByte)
            val addressed = ByteBuffer.wrap(bytes, 72, 8).order(ByteOrder.LITTLE_ENDIAN).getLong
            assert(addressed == 4L)
        } finally deleteRecursively(Paths.get(outPath).getParent)
    }

    test("DataSource rejects wrong schema with a friendly error (missing column)") {
        spark.sparkContext.setLogLevel("ERROR")
        // Missing the `bytes` column — Spark's analyzer surfaces this with INCOMPATIBLE_DATA_FOR_TABLE
        // before reaching our validator; check that we still get a clear column-name error.
        val df = spark.createDataFrame(Seq((1, 0, 0))).toDF("z", "x", "y")
        val outPath = tmpFile("wrong-schema")
        try {
            val ex = intercept[Exception] {
                df.write.format("pmtiles").mode("overwrite").save(outPath)
            }
            val msg = Iterator
                .iterate[Throwable](ex)(_.getCause)
                .takeWhile(_ != null)
                .map(t => Option(t.getMessage).getOrElse(""))
                .mkString(" | ") + " " + Option(ex.getMessage).getOrElse("")
            // Either Spark's own analyzer error names the missing column, or our validator does.
            assert(msg.toLowerCase.contains("bytes"),
                s"expected an error naming the missing 'bytes' column; got: $msg")
        } finally deleteRecursively(Paths.get(outPath).getParent)
    }

    test("DataSource rejects wrong column type via PMTiles_DataSource.validateWriteSchema") {
        // Direct unit test of the schema-validator helper — covers the path Spark itself can't
        // catch (column present but wrong dtype). Tests the helper directly rather than going
        // through Spark since the Spark analyzer also coerces ints across some type boundaries.
        import org.apache.spark.sql.types._
        val badType = StructType(Array(
            StructField("z", IntegerType, nullable = false),
            StructField("x", IntegerType, nullable = false),
            StructField("y", IntegerType, nullable = false),
            // bytes as STRING instead of BINARY.
            StructField("bytes", StringType, nullable = true)
        ))
        val ex = intercept[IllegalArgumentException] {
            PMTiles_DataSource.validateWriteSchema(badType)
        }
        assert(ex.getMessage.contains("`bytes`"),
            s"expected an error naming the wrong-typed `bytes` column; got: ${ex.getMessage}")
        assert(ex.getMessage.toLowerCase.contains("binary"),
            s"expected error to mention BINARY; got: ${ex.getMessage}")
    }

    test("validateWriteSchema rejects extra columns") {
        import org.apache.spark.sql.types._
        val extra = StructType(Array(
            StructField("z", IntegerType, nullable = false),
            StructField("x", IntegerType, nullable = false),
            StructField("y", IntegerType, nullable = false),
            StructField("bytes", BinaryType, nullable = true),
            StructField("ext", StringType, nullable = true)
        ))
        val ex = intercept[IllegalArgumentException] {
            PMTiles_DataSource.validateWriteSchema(extra)
        }
        assert(ex.getMessage.contains("(z INT, x INT, y INT, bytes BINARY)"),
            s"expected error to reference the canonical schema; got: ${ex.getMessage}")
        assert(ex.getMessage.contains("ext"),
            s"expected error to name the extra `ext` column; got: ${ex.getMessage}")
    }

    test("DataSource passes metadataJson option through to the encoded archive") {
        spark.sparkContext.setLogLevel("ERROR")
        val df = spark.createDataFrame(Seq((1, 0, 0, "X".getBytes("UTF-8"))))
            .toDF("z", "x", "y", "bytes")
        val outPath = tmpFile("meta")
        try {
            df.write.format("pmtiles").mode("overwrite").option("metadataJson", "{\"name\":\"test\"}").save(outPath)
            val bytes = Files.readAllBytes(Paths.get(outPath))
            // metadata_offset at 24..31, metadata_length at 32..39.
            val metaOff = ByteBuffer.wrap(bytes, 24, 8).order(ByteOrder.LITTLE_ENDIAN).getLong
            val metaLen = ByteBuffer.wrap(bytes, 32, 8).order(ByteOrder.LITTLE_ENDIAN).getLong
            val metaSlice = bytes.slice(metaOff.toInt, (metaOff + metaLen).toInt)
            val metaString = new String(metaSlice, "UTF-8")
            assert(metaString == "{\"name\":\"test\"}", s"metadata round-trip failed: '$metaString'")
        } finally deleteRecursively(Paths.get(outPath).getParent)
    }

    test("read is not supported in v0.4.0 — surfaces our friendly error, not class-not-found") {
        spark.sparkContext.setLogLevel("ERROR")
        // .load() returns a DataFrame; the scan is only built when we touch the rows.
        val ex = intercept[Exception] {
            spark.read.format("pmtiles").load("/tmp/does-not-matter").collect()
        }
        val msg = Iterator
            .iterate[Throwable](ex)(_.getCause)
            .takeWhile(_ != null)
            .map(t => Option(t.getMessage).getOrElse(""))
            .mkString(" | ")
        // Specifically expect our message — not Spark's generic ClassNotFound or
        // "is not a valid Spark SQL Data Source".
        assert(msg.contains("Reading PMTiles archives is not supported"),
            s"expected our 'Reading PMTiles archives is not supported in GeoBrix 0.4.0' error; got: $msg")
        assert(msg.contains("0.4.0"), s"expected message to name the version; got: $msg")
        assert(msg.contains("write-only"),
            s"expected message to call out write-only; got: $msg")
    }
}
