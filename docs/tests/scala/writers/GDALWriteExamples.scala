package tests.docs.scala.writers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{concat, lit, monotonically_increasing_id}
import tests.docs.scala.SampleDataPath

/**
 * GDAL Writer Examples - Single Source of Truth
 *
 * All Scala code examples shown in docs/docs/writers/gdal.mdx are defined here.
 * Uses payload-only pattern: object constants for docs display, methods for test validation.
 */
object GDALWriteExamples {

    // Display constants (payload only) - shown in documentation

    val WRITE_GDAL: String =
        """// Read, (optionally transform), then write back as raster files.
          |// Keep the reader's full schema (source, tile): the writer looks up both by name.
          |spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
          |    .write
          |      .format("gdal")
          |      .mode("append")           // required -- other modes are not supported
          |      .option("ext", "tif")     // file extension (default: 'tif')
          |    .save("/Volumes/main/default/geobrix_samples/geobrix-examples/out/writer-docs-example")""".stripMargin

    val WRITE_GDAL_output: String =
        """(.save() returns Unit; list the output directory to inspect files)
          |$ ls /Volumes/.../out/writer-docs-example
          |946817315_0_0.tif
          |...""".stripMargin

    val WRITE_WITH_NAMECOL: String =
        """// Overwrite the reader's 'source' column with your desired filename prefix,
          |// then point nameCol at it. The writer needs the fixed (source, tile) schema,
          |// so replacing an existing column is the only way to inject a name.
          |import org.apache.spark.sql.functions.{concat, lit, monotonically_increasing_id}
          |
          |spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
          |    .withColumn("source", concat(lit("tile_"), monotonically_increasing_id()))
          |    .write
          |      .format("gdal")
          |      .mode("append")
          |      .option("nameCol", "source")   // 'source' now carries the filename
          |      .option("ext", "tif")
          |    .save("/Volumes/main/default/geobrix_samples/geobrix-examples/out/writer-docs-example")""".stripMargin

    // Test methods (validate logic) - used by ScalaTest

    def writeGDAL(spark: SparkSession, inPath: String = SampleDataPath.nycSentinel2, outDir: String): Unit = {
        spark.read.format("gdal").load(inPath)
            .write
            .format("gdal")
            .mode("append")
            .option("ext", "tif")
            .save(outDir)
    }

    def writeWithNameCol(spark: SparkSession, inPath: String = SampleDataPath.nycSentinel2, outDir: String): Unit = {
        spark.read.format("gdal").load(inPath)
            .withColumn("source", concat(lit("tile_"), monotonically_increasing_id()))
            .write
            .format("gdal")
            .mode("append")
            .option("nameCol", "source")
            .option("ext", "tif")
            .save(outDir)
    }

    def readBack(spark: SparkSession, outDir: String): DataFrame = {
        spark.read.format("gtiff_gdal").load(outDir)
    }
}
