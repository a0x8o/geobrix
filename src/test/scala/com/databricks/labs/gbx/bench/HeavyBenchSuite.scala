package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession

class HeavyBenchSuite extends PlanTest with SilentSparkSession {

  test("run heavyweight benchmark from system properties", OnDemand) {
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

    // Drive GDAL init through the PRODUCT'S locked, idempotent GDALManager.init so
    // GDALManager.isEnabled is flipped true on the driver BEFORE any Spark task runs.
    // The spark-path's raster work executes inside expression eval (RST_*.eval ->
    // RST_ExpressionUtil.init -> GDALManager.init) on multiple local[N] task threads
    // in this same JVM. If the driver only ran a raw gdal.AllRegister() (leaving
    // isEnabled=false), every first-touch task thread would re-enter AllRegister();
    // GDAL's GDALDriverManager::AutoSkipDrivers() mutates a process-global, non-
    // thread-safe driver list and SIGSEGVs when called concurrently. Routing through
    // GDALManager.init means tasks see isEnabled=true and short-circuit -> the
    // driver list is registered exactly once, never concurrently.
    GDALManager.init(ExpressionConfig(spark))

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
