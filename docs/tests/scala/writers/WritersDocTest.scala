package tests.docs.scala.writers

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.gdal.gdal.gdal
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import tests.docs.scala.SampleDataPath

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._

/**
 * Tests for writer code examples in documentation.
 *
 * These tests verify that documented writer patterns produce valid GDAL output.
 * Outputs are written to a per-test scratch directory under /tmp and cleaned
 * up afterwards, so nothing persists to sample-data Volumes.
 */
class WritersDocTest extends AnyFunSuite with BeforeAndAfterAll {

    var spark: SparkSession = _

    override def beforeAll(): Unit = {
        super.beforeAll()
        spark = SparkSession.builder()
            .appName("Writers Doc Test")
            .master("local[*]")
            .getOrCreate()
    }

    override def afterAll(): Unit = {
        if (spark != null) spark.stop()
        super.afterAll()
    }

    private def makeScratch(): String = {
        val p = Files.createTempDirectory("gdal_write_docs_")
        p.toString
    }

    private def cleanup(outDir: String): Unit = {
        val p = Paths.get(outDir)
        if (Files.exists(p)) {
            Files.list(p).iterator().asScala.foreach(Files.deleteIfExists)
            Files.deleteIfExists(p)
        }
    }

    private def samplePresent(): Boolean = {
        val path = new Path(SampleDataPath.nycSentinel2)
        path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path)
    }

    private def assertValidTifs(outDir: String): Unit = {
        val files = Files.list(Paths.get(outDir))
            .iterator().asScala
            .filter(p => !p.toString.endsWith(".crc"))
            .toList
        files should not be empty
        files.foreach { f =>
            val ds = gdal.Open(f.toString)
            ds should not be null
            ds.GetRasterXSize should be > 0
            ds.GetRasterYSize should be > 0
            ds.delete()
        }
    }

    test("gdal writer: basic append write produces valid tifs") {
        assume(samplePresent(), "Raster sample not present; add nyc/sentinel2 or set GBX_SAMPLE_DATA_ROOT")
        val out = makeScratch()
        try {
            GDALWriteExamples.writeGDAL(spark, outDir = out)
            assertValidTifs(out)
        } finally cleanup(out)
    }

    test("gdal writer: nameCol option controls filenames") {
        assume(samplePresent(), "Raster sample not present; add nyc/sentinel2 or set GBX_SAMPLE_DATA_ROOT")
        val out = makeScratch()
        try {
            GDALWriteExamples.writeWithNameCol(spark, outDir = out)
            assertValidTifs(out)
            val tifs = Files.list(Paths.get(out)).iterator().asScala
                .filter(_.toString.endsWith(".tif")).toList
            tifs.exists(_.getFileName.toString.startsWith("tile_")) shouldBe true
        } finally cleanup(out)
    }

    test("writer: output round-trips through the reader") {
        assume(samplePresent(), "Raster sample not present; add nyc/sentinel2 or set GBX_SAMPLE_DATA_ROOT")
        val out = makeScratch()
        try {
            GDALWriteExamples.writeGDAL(spark, outDir = out)
            val df = GDALWriteExamples.readBack(spark, out)
            df.columns should contain("tile")
            df.count() should be > 0L
        } finally cleanup(out)
    }

    test("writer: doc-display constants are defined") {
        GDALWriteExamples.WRITE_GDAL should not be empty
        GDALWriteExamples.WRITE_GDAL_output should not be empty
        GDALWriteExamples.WRITE_WITH_NAMECOL should not be empty
    }
}
