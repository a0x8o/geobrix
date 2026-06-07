package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.functions
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.gdal.gdal.{Dataset, gdal}
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest

object HeavyRunner {

  private def resolve(corpusRoot: String, path: String): String =
    if (Paths.get(path).isAbsolute) path else Paths.get(corpusRoot, path).toString

  // Deterministic synth output dir, mirroring bench.synth.synth_dir on the pyrx
  // side EXACTLY (sha1(tile_rel_path)[:12]) so the heavy runner reads the SAME
  // files the pyrx runner wrote (write-once-read-both cross-engine identity).
  private def synthDir(corpusRoot: String, tileRelPath: String, recipe: String): Path = {
    val sha = MessageDigest.getInstance("SHA-1").digest(tileRelPath.getBytes("UTF-8"))
    val stem = sha.map("%02x".format(_)).mkString.take(12)
    Paths.get(corpusRoot, "_synth", recipe, stem)
  }

  // The synthesized file paths for a tile_array fn, in consumption order. The
  // pyrx runner writes them; here we only resolve the deterministic filenames so
  // both engines read identical bytes. Filenames mirror bench.synth exactly.
  private def synthPaths(corpusRoot: String, tileRelPath: String, fn: String): Seq[String] = {
    val recipe = BenchDispatch.synthRecipe(fn)
    val dir = synthDir(corpusRoot, tileRelPath, recipe)
    val names = recipe match {
      case "frombands" =>
        // one single-band tile per source band; determined by opening the source.
        val src = resolve(corpusRoot, tileRelPath)
        val ds = gdal.Open(src)
        val n = if (ds != null) ds.GetRasterCount() else 0
        if (ds != null) ds.delete()
        (1 to n).map(b => f"band_$b%02d.tif")
      case "combineavg" => Seq("copy_0.tif", "copy_1.tif")
      case "merge"      => Seq("part_0.tif", "part_1.tif")
      case other        => throw new IllegalArgumentException(s"no synth recipe: $other")
    }
    names.map(nm => dir.resolve(nm).toString)
  }

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
        // input_kind adapter (mirrors the pyrx runner): "bytes"/"path" reader
        // fns are NOT handed an open Dataset -- the dispatch opens the bytes/path
        // itself. "tile" (default) opens the dataset here, as before.
        val kind = BenchDispatch.inputKind(fn)
        var ds: Dataset = null
        try {
          val (fp, body): (String, () => Unit) = kind match {
            case "bytes" =>
              val bytes = Files.readAllBytes(Paths.get(path))
              (BenchDispatch.pureCoreBytes(fn, bytes, a),
                () => BenchDispatch.pureCoreBytes(fn, bytes, a))
            case "path" =>
              (BenchDispatch.pureCorePath(fn, path, a),
                () => BenchDispatch.pureCorePath(fn, path, a))
            case "tile_array" =>
              // Read the SAME synthesized files the pyrx runner wrote (the bench
              // synthesizes once, both engines read identical bytes). Open them
              // fresh inside the helper per call so each timed iteration owns its
              // datasets (the dispatch releases its output, not these inputs).
              val paths = synthPaths(corpusRoot, te.path, fn)
              def callArr(): String = {
                val arr = paths.map(p => gdal.Open(p)).toArray
                try BenchDispatch.pureCoreTileArray(fn, arr, a)
                finally arr.foreach(d => if (d != null) d.delete())
              }
              (callArr(), () => callArr())
            case _ =>
              ds = gdal.Open(path)
              (BenchDispatch.pureCore(fn, ds, a),
                () => BenchDispatch.pureCore(fn, ds, a))
          }
          val (median, mn, p90) = timeIters(body, warmup, measured)
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
    // tile_array adapter (spark-path): the multi-tile fns consume an ARRAY<tile>
    // column. Build a CONSTANT array literal from the SAME synthesized files the
    // pure-core path reads (write-once-read-both), broadcast across every row. The
    // representative source is the first row_pool tile (matches the pyrx runner).
    import org.apache.spark.sql.functions.array
    val arrayRoot = pool.tiles.headOption.map(_.path).getOrElse("")
    def synthArrayCol(fn: String): Column = {
      val tileCols = synthPaths(corpusRoot, arrayRoot, fn).map { p =>
        functions.rst_fromcontent(lit(Files.readAllBytes(Paths.get(p))), lit("GTiff"))
      }
      array(tileCols: _*)
    }
    def inputCol(fn: String): Column =
      if (BenchDispatch.inputKind(fn) == "tile_array") synthArrayCol(fn) else col("raster")
    // throwaway materialized job so JVM/Spark spin-up isn't timed.
    // Wrapped + band-aware so a band-math head fn on a low-band pool can't abort the run.
    val warmFn = fns.find(f => BenchDispatch.minBands(f) <= pool.bands).orElse(fns.headOption)
    warmFn.foreach { wf =>
      try {
        dfAll.limit(1).select(BenchDispatch.column(wf, inputCol(wf), Map.empty).alias("o"))
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
          df.select(BenchDispatch.column(fn, inputCol(fn), a).alias("o"))
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
