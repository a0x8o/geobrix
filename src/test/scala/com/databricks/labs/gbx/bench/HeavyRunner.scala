package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.functions
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.gdal.gdal.{Dataset, gdal}
import java.nio.file.Paths

object HeavyRunner {

  private def resolve(corpusRoot: String, path: String): String =
    if (Paths.get(path).isAbsolute) path else Paths.get(corpusRoot, path).toString

  private def env(where: String): Map[String, String] = Map(
    "env_arch" -> System.getProperty("os.arch", "unknown"),
    "env_cpu_model" -> "jvm",
    "env_os" -> System.getProperty("os.name", "unknown"),
    "env_gdal_version" -> gdal.VersionInfo("RELEASE_NAME"),
    "env_gbx_version" -> "0.4.0",
    "env_runtime_version" -> ("jvm" + System.getProperty("java.version", "")),
    "env_where" -> where
  )

  private def mpix(tilePx: Int, bands: Int, rows: Int): Double =
    (tilePx.toLong * tilePx.toLong * bands * rows) / 1e6

  private def timeIters(body: () => Unit, warmup: Int, measured: Int): (Double, Double, Double) = {
    var i = 0
    while (i < warmup) { body(); i += 1 }
    val samples = Array.ofDim[Double](measured)
    i = 0
    while (i < measured) {
      val t0 = System.nanoTime(); body(); samples(i) = (System.nanoTime() - t0) / 1e6; i += 1
    }
    val sorted = samples.sorted
    val median = if (sorted.length % 2 == 1) sorted(sorted.length / 2)
                 else (sorted(sorted.length / 2 - 1) + sorted(sorted.length / 2)) / 2.0
    val p90idx = math.min(sorted.length - 1, math.round(0.9 * (sorted.length - 1)).toInt)
    (median, sorted.head, sorted(p90idx))
  }

  private def row(e: Map[String, String], runId: String, fn: String, mode: String,
                  tilePx: Int, bands: Int, dtype: String, srid: Int, rows: Int, ndf: Double,
                  warmup: Int, measured: Int, median: Double, mn: Double, p90: Double,
                  mpixS: Double, rowsS: Double, status: String, note: String, fp: String): BenchRow =
    BenchRow(runId, "heavyweight", fn, BenchDispatch.category(fn), mode, tilePx, bands, dtype,
      srid, rows, ndf, warmup, measured, median, mn, p90, mpixS, rowsS, 0.0, status, note,
      e("env_arch"), e("env_cpu_model"), 0, e("env_os"), e("env_gbx_version"),
      e("env_gdal_version"), e("env_runtime_version"), e("env_where"), fp)

  /** `sink` is invoked for every row as soon as it is produced, so a caller can
   *  flush each row to disk immediately (crash-resilient shard). Defaults to a
   *  no-op, preserving the in-memory-only behavior for callers that don't pass one. */
  def runPureCore(corpusRoot: String, corpus: Corpus, fns: Seq[String], runId: String,
                  warmup: Int, measured: Int, argsByFn: Map[String, Map[String, String]],
                  sink: BenchRow => Unit = _ => ()): Seq[BenchRow] = {
    val e = env("docker")
    val out = scala.collection.mutable.ArrayBuffer.empty[BenchRow]
    def emit(r: BenchRow): Unit = { out += r; sink(r) }
    for (fn <- fns; te <- corpus.size_sweep) {
      val a = argsByFn.getOrElse(fn, Map.empty)
      if (te.bands < BenchDispatch.minBands(fn)) {
        emit(row(e, runId, fn, "pure-core", te.tile_px, te.bands, te.dtype, te.srid, 1,
          te.nodata_frac, warmup, 0, 0, 0, 0, 0, 0, "na_by_design",
          s"requires >= ${BenchDispatch.minBands(fn)} bands", ""))
      } else {
        val path = resolve(corpusRoot, te.path)
        var ds: Dataset = null
        try {
          ds = gdal.Open(path)
          val fp = BenchDispatch.pureCore(fn, ds, a)  // untimed fingerprint capture
          val (median, mn, p90) = timeIters(() => BenchDispatch.pureCore(fn, ds, a), warmup, measured)
          val mpixS = if (median > 0) mpix(te.tile_px, te.bands, 1) / (median / 1000.0) else 0.0
          emit(row(e, runId, fn, "pure-core", te.tile_px, te.bands, te.dtype, te.srid, 1,
            te.nodata_frac, warmup, measured, median, mn, p90, mpixS,
            if (median > 0) 1.0 / (median / 1000.0) else 0.0, "ok", "", fp))
        } catch {
          case ex: Throwable =>
            emit(row(e, runId, fn, "pure-core", te.tile_px, te.bands, te.dtype, te.srid, 1,
              te.nodata_frac, warmup, 0, 0, 0, 0, 0, 0, "error",
              Option(ex.getMessage).getOrElse(ex.toString).take(300), ""))
        } finally {
          if (ds != null) ds.delete()
        }
      }
    }
    out.toSeq
  }

  def runSparkPath(spark: SparkSession, corpusRoot: String, corpus: Corpus, fns: Seq[String],
                   runId: String, rowCounts: Seq[Int], warmup: Int, measured: Int,
                   argsByFn: Map[String, Map[String, String]],
                   sink: BenchRow => Unit = _ => ()): Seq[BenchRow] = {
    functions.register(spark)
    val e = env("docker")
    val pool = corpus.row_pool
    val maxRows = rowCounts.max
    val paths = pool.tiles.take(maxRows).map(t => resolve(corpusRoot, t.path))
    val dfAll = spark.read.format("binaryFile").load(paths: _*)
      .withColumn("raster", functions.rst_fromcontent(col("content"), lit("GTiff")))
      .select(col("raster"))
      .cache()
    dfAll.count()
    // throwaway materialized job so JVM/Spark spin-up isn't timed.
    // Wrapped + band-aware so a band-math head fn on a low-band pool can't abort the run.
    val warmFn = fns.find(f => BenchDispatch.minBands(f) <= pool.bands).orElse(fns.headOption)
    warmFn.foreach { wf =>
      try {
        dfAll.limit(1).select(BenchDispatch.column(wf, col("raster"), Map.empty).alias("o"))
          .write.format("noop").mode("overwrite").save()
      } catch { case _: Throwable => () }  // warm-up failures must never abort timing
    }
    val out = scala.collection.mutable.ArrayBuffer.empty[BenchRow]
    def emit(r: BenchRow): Unit = { out += r; sink(r) }
    for (fn <- fns; n <- rowCounts.sorted) {
      val a = argsByFn.getOrElse(fn, Map.empty)
      try {
        val df = dfAll.limit(n)
        val (median, mn, p90) = timeIters(() => {
          df.select(BenchDispatch.column(fn, col("raster"), a).alias("o"))
            .write.format("noop").mode("overwrite").save()
        }, warmup, measured)
        val mpixS = if (median > 0) mpix(pool.tile_px, pool.bands, n) / (median / 1000.0) else 0.0
        emit(row(e, runId, fn, "spark-path", pool.tile_px, pool.bands, pool.dtype, 0, n, 0.0,
          warmup, measured, median, mn, p90, mpixS,
          if (median > 0) n / (median / 1000.0) else 0.0, "ok", "", ""))
      } catch {
        case ex: Throwable =>
          emit(row(e, runId, fn, "spark-path", pool.tile_px, pool.bands, pool.dtype, 0, n, 0.0,
            warmup, 0, 0, 0, 0, 0, 0, "error", Option(ex.getMessage).getOrElse(ex.toString).take(300), ""))
      }
    }
    dfAll.unpersist()
    out.toSeq
  }
}
