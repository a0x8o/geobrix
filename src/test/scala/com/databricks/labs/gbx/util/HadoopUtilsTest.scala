package com.databricks.labs.gbx.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/**
  * Tests that HadoopUtils directory enumeration skips Hadoop marker/hidden files (names starting
  * with `_` or `.`, e.g. `_SUCCESS`, `_committed_*`). Without this, a reader pointed at a writer's
  * output directory would pick up `_SUCCESS` and fail opening it as a dataset (the GeoJSONL/OGR
  * round-trip regression this guards against).
  */
class HadoopUtilsTest extends AnyFunSuite with BeforeAndAfterEach {

    private var tempDir: java.nio.file.Path = _
    private var hconf: SerializableConfiguration = _

    override def beforeEach(): Unit = {
        tempDir = Files.createTempDirectory("HadoopUtilsTest")
        hconf = new SerializableConfiguration(new Configuration())
    }

    override def afterEach(): Unit = {
        if (tempDir != null) {
            try {
                Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
            } catch { case _: Exception => () }
        }
    }

    private def write(name: String): Unit = {
        Files.write(tempDir.resolve(name), "x".getBytes)
        ()
    }

    /** The directory as a file: URI string (cleanPath keeps file:/ paths as-is). */
    private def dirUri: String = tempDir.toUri.toString

    private def names(paths: Seq[String]): Set[String] = paths.map(p => new Path(p).getName).toSet

    test("listHadoopFiles skips marker and hidden files") {
        write("a.geojsonl")
        write("b.geojsonl")
        write("_SUCCESS")
        write("_committed_123")
        write(".hidden.geojsonl")

        names(HadoopUtils.listHadoopFiles(dirUri, hconf)) shouldBe Set("a.geojsonl", "b.geojsonl")
    }

    test("getFirstFile returns a data file, not the _SUCCESS marker") {
        write("_SUCCESS")
        write("shard.geojsonl")

        new Path(HadoopUtils.getFirstFile(dirUri, hconf)).getName shouldBe "shard.geojsonl"
    }

    test("getFirstFile throws when the directory has only marker files") {
        write("_SUCCESS")
        write("_committed_0")

        an[IllegalArgumentException] should be thrownBy HadoopUtils.getFirstFile(dirUri, hconf)
    }
}
