package com.databricks.labs.gbx.pmtiles

import com.databricks.labs.gbx.util.HadoopUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.funsuite.AnyFunSuite

import java.io.IOException
import java.nio.file.{Files, Path => JPath, Paths}
import java.util.UUID

/**
  * Tests for the single-file naming contract wired into the `pmtiles` DataSource.
  *
  * Covers:
  *  - `HadoopUtils.completeExt` (unit, no FS)
  *  - `HadoopUtils.resolveSingleFileOutput` (local FS cases)
  *  - End-to-end write: stem path → `<stem>.pmtiles`, `fileName` option, existing-dir, wrong-ext
  */
class PMTilesNamingTest extends AnyFunSuite {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private def hConf: SerializableConfiguration =
        new SerializableConfiguration(new Configuration())

    private def tmpDir(prefix: String): JPath =
        Files.createTempDirectory(s"gbx-pmtiles-naming-$prefix-")

    private def deleteRecursively(p: JPath): Unit = {
        if (!Files.exists(p)) return
        if (Files.isDirectory(p)) {
            val it = Files.list(p)
            try it.forEach(child => deleteRecursively(child)) finally it.close()
        }
        try Files.delete(p) catch { case _: IOException => () }
    }

    // -------------------------------------------------------------------------
    // completeExt — unit tests (no filesystem)
    // -------------------------------------------------------------------------

    test("completeExt: name already ends with ext → unchanged") {
        assert(HadoopUtils.completeExt("out.pmtiles", ".pmtiles") == "out.pmtiles")
        assert(HadoopUtils.completeExt("OUT.PMTILES", ".pmtiles") == "OUT.PMTILES")
    }

    test("completeExt: name without ext → appends ext") {
        assert(HadoopUtils.completeExt("out", ".pmtiles") == "out.pmtiles")
        assert(HadoopUtils.completeExt("my-tiles", ".pmtiles") == "my-tiles.pmtiles")
    }

    test("completeExt: wrong recognized geo extension → clear error") {
        val ex = intercept[IllegalArgumentException] {
            HadoopUtils.completeExt("data.gpkg", ".pmtiles")
        }
        assert(ex.getMessage.contains(".gpkg"), s"error should name the wrong ext; got: ${ex.getMessage}")
        assert(ex.getMessage.contains(".pmtiles"), s"error should name the expected ext; got: ${ex.getMessage}")
    }

    test("completeExt: unrecognized extension → appends ext (no rejection)") {
        // A name like 'data.csv' has no recognized geo extension; just append .pmtiles
        assert(HadoopUtils.completeExt("data.csv", ".pmtiles") == "data.csv.pmtiles")
    }

    // -------------------------------------------------------------------------
    // resolveSingleFileOutput — local filesystem cases
    // -------------------------------------------------------------------------

    test("resolveSingleFileOutput case 1: fileName given → placed under path dir") {
        val dir = tmpDir("case1")
        try {
            val resolved = HadoopUtils.resolveSingleFileOutput(
                dir.toAbsolutePath.toString, Some("tiles"), ".pmtiles", hConf)
            assert(resolved == s"${dir.toAbsolutePath}/tiles.pmtiles",
                s"unexpected resolved path: $resolved")
        } finally deleteRecursively(dir)
    }

    test("resolveSingleFileOutput case 1: fileName already has .pmtiles → not double-appended") {
        val dir = tmpDir("case1-ext")
        try {
            val resolved = HadoopUtils.resolveSingleFileOutput(
                dir.toAbsolutePath.toString, Some("tiles.pmtiles"), ".pmtiles", hConf)
            assert(resolved.endsWith("tiles.pmtiles") && !resolved.endsWith("tiles.pmtiles.pmtiles"),
                s"unexpected resolved path: $resolved")
        } finally deleteRecursively(dir)
    }

    test("resolveSingleFileOutput case 1: fileName with wrong ext → error") {
        val dir = tmpDir("case1-badext")
        try {
            intercept[IllegalArgumentException] {
                HadoopUtils.resolveSingleFileOutput(
                    dir.toAbsolutePath.toString, Some("data.gpkg"), ".pmtiles", hConf)
            }
        } finally deleteRecursively(dir)
    }

    test("resolveSingleFileOutput case 2: existing dir → output named after dir, placed under it") {
        val dir = tmpDir("my-atlas")
        try {
            val dirName = dir.getFileName.toString
            val resolved = HadoopUtils.resolveSingleFileOutput(
                dir.toAbsolutePath.toString, None, ".pmtiles", hConf)
            assert(resolved == s"${dir.toAbsolutePath}/$dirName.pmtiles",
                s"unexpected resolved path: $resolved")
        } finally deleteRecursively(dir)
    }

    test("resolveSingleFileOutput case 3: non-existing file-like path → ext completed, parent created") {
        val dir = tmpDir("case3-parent")
        try {
            val stem = s"${dir.toAbsolutePath}/subdir/my-tiles"
            val resolved = HadoopUtils.resolveSingleFileOutput(stem, None, ".pmtiles", hConf)
            assert(resolved == s"$stem.pmtiles", s"unexpected resolved path: $resolved")
            // Parent directory must have been created.
            assert(Files.isDirectory(Paths.get(s"${dir.toAbsolutePath}/subdir")),
                "parent directory was not created")
        } finally deleteRecursively(dir)
    }

    test("resolveSingleFileOutput case 3: stem already ends with .pmtiles → not double-appended") {
        val dir = tmpDir("case3-hasext")
        try {
            val stemWithExt = s"${dir.toAbsolutePath}/already.pmtiles"
            val resolved = HadoopUtils.resolveSingleFileOutput(stemWithExt, None, ".pmtiles", hConf)
            assert(resolved == stemWithExt, s"should not double-append; got: $resolved")
        } finally deleteRecursively(dir)
    }
}

/**
  * End-to-end write tests for the fileName naming contract (requires Spark).
  */
class PMTilesNamingWriteTest extends PlanTest with SilentSparkSession {

    private def smallDf() = spark
        .createDataFrame(Seq((1, 0, 0, "A".getBytes("UTF-8")), (1, 0, 1, "B".getBytes("UTF-8"))))
        .toDF("z", "x", "y", "bytes")
        .coalesce(1)

    private def tmpDir(prefix: String): JPath =
        Files.createTempDirectory(s"gbx-pmtiles-naming-e2e-$prefix-")

    private def deleteRecursively(p: JPath): Unit = {
        if (!Files.exists(p)) return
        if (Files.isDirectory(p)) {
            val it = Files.list(p)
            try it.forEach(child => deleteRecursively(child)) finally it.close()
        }
        try Files.delete(p) catch { case _: IOException => () }
    }

    test("write to stem path → output lands at <stem>.pmtiles") {
        spark.sparkContext.setLogLevel("ERROR")
        val dir = tmpDir("stem")
        try {
            val stem = s"${dir.toAbsolutePath}/my-tiles"
            smallDf().write.format("pmtiles").mode("overwrite").save(stem)
            val out = Paths.get(s"$stem.pmtiles")
            assert(Files.exists(out), s"expected $stem.pmtiles to exist")
            assert(Files.size(out) >= 127L, "output must have at least a 127-byte header")
        } finally deleteRecursively(dir)
    }

    test("write with fileName option → <dir>/tiles.pmtiles") {
        spark.sparkContext.setLogLevel("ERROR")
        val dir = tmpDir("filename-opt")
        try {
            smallDf().write
                .format("pmtiles")
                .mode("overwrite")
                .option("fileName", "tiles")
                .save(dir.toAbsolutePath.toString)
            val out = dir.resolve("tiles.pmtiles")
            assert(Files.exists(out), s"expected tiles.pmtiles under $dir")
            assert(Files.size(out) >= 127L, "output must have at least a 127-byte header")
        } finally deleteRecursively(dir)
    }

    test("write to existing directory → output named after the directory") {
        spark.sparkContext.setLogLevel("ERROR")
        val dir = tmpDir("my-atlas")
        try {
            val dirName = dir.getFileName.toString
            smallDf().write.format("pmtiles").mode("overwrite").save(dir.toAbsolutePath.toString)
            val out = dir.resolve(s"$dirName.pmtiles")
            assert(Files.exists(out), s"expected $dirName.pmtiles under $dir")
            assert(Files.size(out) >= 127L, "output must have at least a 127-byte header")
        } finally deleteRecursively(dir)
    }

    test("write with wrong-ext fileName → clear error before writing") {
        spark.sparkContext.setLogLevel("ERROR")
        val dir = tmpDir("wrong-ext")
        try {
            val ex = intercept[Exception] {
                smallDf().write
                    .format("pmtiles")
                    .mode("overwrite")
                    .option("fileName", "data.gpkg")
                    .save(dir.toAbsolutePath.toString)
            }
            val msg = Iterator
                .iterate[Throwable](ex)(_.getCause)
                .takeWhile(_ != null)
                .map(t => Option(t.getMessage).getOrElse(""))
                .mkString(" | ")
            assert(msg.contains(".gpkg") || msg.contains("geo extension"),
                s"expected a clear wrong-ext error; got: $msg")
        } finally deleteRecursively(dir)
    }
}
