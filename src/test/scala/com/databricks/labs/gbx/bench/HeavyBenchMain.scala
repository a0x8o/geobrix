package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.SparkSession
import org.gdal.gdal.gdal

/** py4j-callable cluster entry: invoked from a Python notebook via
 *  spark._jvm.com.databricks.labs.gbx.bench.HeavyBenchMain.run(spark._jsparkSession, ...).
 *  Writes a heavyweight JSONL shard (same schema as the lightweight runner). */
object HeavyBenchMain {
  // localCorpusRoot: a LOCAL (driver-disk) copy of the corpus for the pure-core path,
  // which opens tiles via GDAL (can't read a UC Volume). runSparkPath keeps corpusRoot
  // (the Volume) because it reads tiles via Spark binaryFile (UC-aware) on executors.
  // Pass localCorpusRoot == corpusRoot when not on a Volume (e.g. local Docker runs).
  def run(spark: SparkSession, corpusRoot: String, localCorpusRoot: String, outPath: String,
          fnsCsv: String, modes: String, rowCountsCsv: String, warmup: Int, measured: Int,
          runId: String): Unit = {
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
    // Stream each row to disk (truncate-on-open, fsync per row) so a later native
    // crash leaves a partial-but-valid shard rather than voiding the whole run.
    val writer = BenchIO.appendWriter(outPath)
    try {
      if (modes == "pure-core" || modes == "both")
        // pure-core opens tiles via GDAL on the driver -> read from the LOCAL corpus copy.
        HeavyRunner.runPureCore(
          localCorpusRoot, corpus, fns, runId, warmup, measured, Map.empty, writer.append)
      if (modes == "spark-path" || modes == "both")
        HeavyRunner.runSparkPath(
          spark, corpusRoot, corpus, fns, runId, rowCounts, warmup, measured, Map.empty, writer.append)
    } finally {
      writer.close()
    }
  }
}
