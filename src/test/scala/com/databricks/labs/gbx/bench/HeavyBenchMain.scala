package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.SparkSession
import org.gdal.gdal.gdal

/** py4j-callable cluster entry: invoked from a Python notebook via
 *  spark._jvm.com.databricks.labs.gbx.bench.HeavyBenchMain.run(spark._jsparkSession, ...).
 *  Writes a heavyweight JSONL shard (same schema as the lightweight runner). */
object HeavyBenchMain {
  def run(spark: SparkSession, corpusRoot: String, outPath: String, fnsCsv: String,
          modes: String, rowCountsCsv: String, warmup: Int, measured: Int, runId: String): Unit = {
    // Ensure the GDAL JNI is loaded + drivers registered on THIS JVM (idempotent).
    // Pure-core opens rasters directly (gdal.Open on the driver) without going through
    // functions.register, so registration must be guaranteed here. Deliberately do NOT
    // call GDALManager.configureGDAL: on a cluster, GDAL_DATA/PROJ are set by the
    // heavyweight init script and must not be overridden.
    try {
      GDALManager.loadSharedObjects(Iterable.empty[String])
      gdal.AllRegister()
    } catch {
      case _: Throwable => () // already loaded / unavailable — runPureCore will surface a clear error
    }
    val fns =
      if (fnsCsv == null || fnsCsv.trim.isEmpty) BenchDispatch.all
      else fnsCsv.split(",").map(_.trim).filter(_.nonEmpty).toSeq
    val rowCounts = rowCountsCsv.split(",").map(_.trim).filter(_.nonEmpty).map(_.toInt).toSeq
    val corpus = BenchManifest.read(s"$corpusRoot/corpus.json")
    val rows = scala.collection.mutable.ArrayBuffer.empty[BenchRow]
    if (modes == "pure-core" || modes == "both")
      rows ++= HeavyRunner.runPureCore(corpusRoot, corpus, fns, runId, warmup, measured, Map.empty)
    if (modes == "spark-path" || modes == "both")
      rows ++= HeavyRunner.runSparkPath(spark, corpusRoot, corpus, fns, runId, rowCounts, warmup, measured, Map.empty)
    BenchIO.writeJsonl(rows.toSeq, outPath)
  }
}
