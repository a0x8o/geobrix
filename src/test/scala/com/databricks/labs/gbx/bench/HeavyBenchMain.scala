package com.databricks.labs.gbx.bench

import org.apache.spark.sql.SparkSession

/** py4j-callable cluster entry: invoked from a Python notebook via
 *  spark._jvm.com.databricks.labs.gbx.bench.HeavyBenchMain.run(spark._jsparkSession, ...).
 *  Writes a heavyweight JSONL shard (same schema as the lightweight runner). */
object HeavyBenchMain {
  def run(spark: SparkSession, corpusRoot: String, outPath: String, fnsCsv: String,
          modes: String, rowCountsCsv: String, warmup: Int, measured: Int, runId: String): Unit = {
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
