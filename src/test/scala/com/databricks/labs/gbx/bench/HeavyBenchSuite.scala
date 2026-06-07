package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.gdal.gdal

class HeavyBenchSuite extends PlanTest with SilentSparkSession {

  test("run heavyweight benchmark from system properties") {
    val corpusRoot = sys.props.getOrElse("gbx.bench.corpus", "")
    val outPath = sys.props.getOrElse("gbx.bench.out", "")
    assume(corpusRoot.nonEmpty && outPath.nonEmpty,
      "gbx.bench.corpus and gbx.bench.out must be set; skipping (on-demand suite)")

    val modes = sys.props.getOrElse("gbx.bench.modes", "both")
    val warmup = sys.props.getOrElse("gbx.bench.warmup", "2").toInt
    val measured = sys.props.getOrElse("gbx.bench.measured", "5").toInt
    val runId = sys.props.getOrElse("gbx.bench.runId", "local")
    val rowCounts = sys.props.getOrElse("gbx.bench.rowCounts", "10,100,1000,10000")
      .split(",").filter(_.nonEmpty).map(_.trim.toInt).toSeq
    val fnsProp = sys.props.getOrElse("gbx.bench.functions", "")
    val fns = if (fnsProp.isEmpty) BenchDispatch.all
              else fnsProp.split(",").filter(_.nonEmpty).map(_.trim).toSeq

    GDALManager.loadSharedObjects(Iterable.empty[String])
    GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
    gdal.AllRegister()

    val corpus = BenchManifest.read(s"$corpusRoot/corpus.json")
    // Stream each row to disk (truncate-on-open, fsync per row) so a later native
    // crash leaves a partial-but-valid shard rather than voiding the whole run.
    val writer = BenchIO.appendWriter(outPath)
    val rows = scala.collection.mutable.ArrayBuffer.empty[BenchRow]
    try {
      if (modes == "pure-core" || modes == "both")
        rows ++= HeavyRunner.runPureCore(
          corpusRoot, corpus, fns, runId, warmup, measured, Map.empty, writer.append)
      if (modes == "spark-path" || modes == "both")
        rows ++= HeavyRunner.runSparkPath(
          spark, corpusRoot, corpus, fns, runId, rowCounts, warmup, measured, Map.empty, writer.append)
    } finally {
      writer.close()
    }
    info(s"wrote ${rows.length} heavyweight rows -> $outPath")
    assert(rows.nonEmpty)
  }
}
