package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.gdal.gdal.{Dataset, gdal}
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest

object HeavyRunner {

  private def resolve(corpusRoot: String, path: String): String =
    if (Paths.get(path).isAbsolute) path else Paths.get(corpusRoot, path).toString

  // GDAL needs random access; UC Volumes (cloud object storage) don't support it, so the
  // JVM can't gdal.Open a /Volumes tile directly. Stage it to a LOCAL temp via the
  // Volume-native Hadoop read (BenchIO.readBytes), open that, then clean up. Same reason
  // BenchIO.readBytes reads through the Hadoop FileSystem rather than java.io.
  private def withVolumeDataset[T](path: String)(f: Dataset => T): T = {
    val tmp = java.io.File.createTempFile("heavy_gdal_", ".tif")
    try {
      Files.write(tmp.toPath, BenchIO.readBytes(path))
      val ds = gdal.Open(tmp.getAbsolutePath)
      try f(ds) finally if (ds != null) ds.delete()
    } finally tmp.delete()
  }

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
        val n = withVolumeDataset(src)(ds => if (ds != null) ds.GetRasterCount() else 0)
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
    // Geometry corpus: geometry-in fns read the tile's GeometrySet from the
    // geometry.json written alongside corpus.json (write-once-read-both; the SAME
    // base64 WKB the pyrx tier reads). Loaded once; None for older corpora.
    val geomCorpus = BenchManifest.readGeometry(Paths.get(corpusRoot, "geometry.json").toString)
    // The *_agg aggregators are spark-path-only (no single-row pure-core UDAF
    // analogue); BenchDispatch.pureCore throws "unknown bench fn" for them. Skip
    // them here so pure-core stays SYMMETRIC with the lightweight run_pure_core
    // (which skips any fn whose FnSpec.modes lack "pure-core"). Without this, a
    // pure-core run over a set that includes aggregators emitted heavy error rows
    // the lightweight side never produced -> unmatched comparison rows.
    val pureCoreFns = fns.filterNot(f => BenchDispatch.inputKind(f).endsWith("aggregate"))
    for (fn <- pureCoreFns; te <- corpus.size_sweep) {
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
              val bytes = BenchIO.readBytes(path)  // FUSE-safe (Volume tile bytes)
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
            case "geometry" =>
              // Open the tile (for extent/size/srid) and resolve its GeometrySet
              // from the geometry corpus (by source_tile path, else by srid). Both
              // engines read identical WKB bytes -> byte-identical geometry input.
              ds = gdal.Open(path)
              val gset = geomCorpus
                .flatMap(_.setFor(te.path, te.srid))
                .getOrElse(throw new IllegalStateException(
                  s"no geometry set for tile ${te.path} (srid ${te.srid}); " +
                    "geometry.json missing or stale"))
              (BenchDispatch.pureCoreGeometry(fn, ds, a, gset),
                () => BenchDispatch.pureCoreGeometry(fn, ds, a, gset))
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
    // The *_agg aggregators hold whole rasters in memory while aggregating: the
    // ObjectHash aggregate map buffers per-group state and the raster UDAFs decode
    // every input tile (a 1024x1024x4 float32 tile is ~16MB, plus GDAL dataset
    // overhead) before producing the group's output tile. Two knobs keep the forked
    // 4G test JVM from OOMing as the row ladder grows, while preserving a real
    // distributed groupBy().agg():
    //   1. fallbackThreshold=1 forces the spilling sort-based aggregate immediately,
    //      so at most one group's raster state is buffered per task at a time.
    //   2. shuffle.partitions=2 caps how many heavy groups aggregate concurrently
    //      (8 simultaneous GDAL-decoding tasks each holding multi-MB rasters
    //      overruns the heap regardless of spill). Fewer tasks process keys
    //      sequentially, bounding peak in-flight raster memory.
    spark.conf.set("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "1")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    val e = env("docker")
    val pool = corpus.row_pool
    // Record the row tiles' real srid (they share one CRS) so heavy spark-path rows
    // match the lightweight rows in compare_cells. Hardcoding 0 here made every cell
    // mismatch the light side's real srid -> compared=0.
    val poolSrid = pool.tiles.headOption.map(_.srid).getOrElse(0)
    val maxRows = rowCounts.max
    val paths = pool.tiles.take(maxRows).map(t => resolve(corpusRoot, t.path))
    // coalesce(1): the rst_fromcontent map (and every downstream spark-path job) opens
    // rasters through GDAL. In the bench's local[N] mode all task threads share one JVM;
    // the first GDAL-touching task on each thread re-enters GDALDriverManager::
    // AutoSkipDrivers(), which mutates a process-global, non-thread-safe driver list and
    // SIGSEGVs when called concurrently. One partition serializes GDAL to a single task
    // thread -> no concurrent AllRegister -> no crash. Real clusters run distributed
    // across separate executor JVMs and are unaffected; this only removes local-mode
    // parallelism (timing is single-thread, documented on the aggregate path below).
    val dfAll = spark.read.format("binaryFile").load(paths: _*)
      .coalesce(1)
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
        functions.rst_fromcontent(lit(BenchIO.readBytes(p)), lit("GTiff"))  // FUSE-safe
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
    // bucket A: the 7 *_agg aggregators run a real df.groupBy(key).agg(...). They are
    // spark-path-only (no single-row pure-core UDAF analogue). Handle them in the
    // dedicated aggregate branch (consistency fingerprint + scaled perf timing).
    val geomCorpusSp = BenchManifest.readGeometry(
      Paths.get(corpusRoot, "geometry.json").toString)
    for (fn <- fns if BenchDispatch.inputKind(fn).endsWith("aggregate"))
      runAggregate(spark, corpusRoot, corpus, fn, runId, rowCounts, warmup, measured,
        argsByFn.getOrElse(fn, Map.empty), e, geomCorpusSp, emit)
    // Geometry-in fns (input_kind == "geometry") are pure-core-only: the tile
    // DataFrame carries no geometry column, so there is no spark-path column form.
    // Skip them + the aggregators (handled above) here.
    val sparkFns = fns.filterNot(f =>
      BenchDispatch.inputKind(f) == "geometry" ||
        BenchDispatch.inputKind(f).endsWith("aggregate"))
    for (fn <- sparkFns; n <- rowCounts.sorted) {
      val a = argsByFn.getOrElse(fn, Map.empty)
      try {
        val df = dfAll.limit(n)
        val (median, mn, p90) = timeIters(() => {
          df.select(BenchDispatch.column(fn, inputCol(fn), a).alias("o"))
            .write.format("noop").mode("overwrite").save()
        }, warmup, measured)
        val mpixS = if (median > 0) mpix(pool.tile_px, pool.bands, n) / (median / 1000.0) else 0.0
        emit(row(e, runId, fn, "spark-path", pool.tile_px, pool.bands, pool.dtype, poolSrid, n, 0.0,
          warmup, measured, median, mn, p90, mpixS,
          if (median > 0) n / (median / 1000.0) else 0.0, "ok", "", ""))
      } catch {
        case ex: Throwable =>
          emit(row(e, runId, fn, "spark-path", pool.tile_px, pool.bands, pool.dtype, poolSrid, n, 0.0,
            warmup, 0, 0, 0, 0, 0, 0, "error", Option(ex.getMessage).getOrElse(ex.toString).take(300), ""))
      }
    }
    dfAll.unpersist()
    out.toSeq
  }

  // (xmin, ymin, xmax, ymax, widthPx, heightPx, srid) from an open Dataset -- the
  // per-group extent constants a geometry aggregator burns/interpolates into. Mirrors
  // the pyrx _tile_extent_size_srid: dataset bounds + size + EPSG.
  private def extentOf(ds: Dataset): (Double, Double, Double, Double, Int, Int, Int) = {
    val gt = ds.GetGeoTransform()
    val w = ds.GetRasterXSize(); val h = ds.GetRasterYSize()
    val x0 = gt(0); val y0 = gt(3)
    val x1 = gt(0) + w * gt(1); val y1 = gt(3) + h * gt(5)
    val (xmin, xmax) = (math.min(x0, x1), math.max(x0, x1))
    val (ymin, ymax) = (math.min(y0, y1), math.max(y0, y1))
    val srid = try {
      val sr = ds.GetSpatialRef()
      if (sr != null && sr.GetAuthorityCode(null) != null) sr.GetAuthorityCode(null).toInt else 0
    } catch { case _: Throwable => 0 }
    (xmin, ymin, xmax, ymax, w, h, srid)
  }

  /** bucket A: run a *_agg aggregator as a real df.groupBy(key).agg(...). Emits a
    * CONSISTENCY row (a fixed deterministic single group -> ONE out tile ->
    * BenchFingerprint.ofDataset, on the smallest-N row) and the PERF timing (the
    * scaled groupBy, one fixed group replicated per key). The fixed group is
    * byte-identical to the pyrx tier (the tile aggregators read the SAME synthesized
    * tiles; the geometry aggregators read the SAME geometry.json WKB). */
  private def runAggregate(spark: SparkSession, corpusRoot: String, corpus: Corpus,
                           fn: String, runId: String, rowCounts: Seq[Int],
                           warmup: Int, measured: Int, a: Map[String, String],
                           e: Map[String, String], geomCorpus: Option[GeometryCorpus],
                           emit: BenchRow => Unit): Unit = {
    import org.apache.spark.sql.functions.{col, lit, struct}
    val pool = corpus.row_pool
    // Real srid of the row tiles (see runSparkPath) so agg rows match the light side.
    val poolSrid = pool.tiles.headOption.map(_.srid).getOrElse(0)
    val sorted = rowCounts.sorted
    val arrayRoot = pool.tiles.headOption.map(_.path).getOrElse("")
    def errAll(msg: String): Unit = sorted.foreach { n =>
      emit(row(e, runId, fn, "spark-path", pool.tile_px, pool.bands, pool.dtype, poolSrid, n,
        0.0, warmup, 0, 0, 0, 0, 0, 0, "error", msg.take(300), ""))
    }
    // Serialize the heavy aggregate spark-path to a SINGLE partition so GDAL is
    // touched by at most one task thread at a time. In the bench's local[N] mode
    // every task thread shares one JVM; the first GDAL-touching task on each thread
    // re-enters GDALDriverManager::AutoSkipDrivers(), which mutates a process-global,
    // non-thread-safe driver list and SIGSEGVs when called concurrently. One reduce
    // partition (shuffle.partitions=1 + keyParts=1 + coalesce(1)) means the groupBy's
    // single task runs alone -> no concurrent AllRegister -> no crash. This trades
    // distributed timing for the heavy aggregate (real clusters run distributed across
    // separate executor JVMs and are unaffected); consistency + single-thread heavy-vs-
    // light timing are still obtained.
    val savedShuffle = spark.conf.get("spark.sql.shuffle.partitions")
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    try {
      val kind = BenchDispatch.inputKind(fn)
      // Build the fixed group DataFrame + (for geometry aggregators) extent.
      var ext: (Double, Double, Double, Double, Int, Int, Int) = (0, 0, 0, 0, 0, 0, 0)
      val groupDf: org.apache.spark.sql.DataFrame = if (kind == "tile_aggregate") {
        val recipe = BenchDispatch.aggSynthRecipe(fn)
        val dir = synthDir(corpusRoot, arrayRoot, recipe)
        val src = resolve(corpusRoot, arrayRoot)
        val names = recipe match {
          case "frombands" =>
            val n = withVolumeDataset(src)(ds => if (ds != null) ds.GetRasterCount() else 0)
            (1 to n).map(b => f"band_$b%02d.tif")
          case "combineavg" => Seq("copy_0.tif", "copy_1.tif")
          case "merge"      => Seq("part_0.tif", "part_1.tif")
        }
        val rows = names.zipWithIndex.map { case (nm, i) =>
          val bytes = BenchIO.readBytes(dir.resolve(nm).toString)  // FUSE-safe
          (0L, bytes, i)
        }
        import spark.implicits._
        spark.createDataFrame(rows).toDF("cellid", "rasterBytes", "band_index")
          .withColumn("tile", functions.rst_fromcontent(col("rasterBytes"), lit("GTiff")))
          .select(col("tile"), col("band_index"))
      } else {
        // geometry aggregate: rows of (geom_wkb, value) from the per-tile GeometrySet.
        val gset = geomCorpus.flatMap(_.setFor(arrayRoot, pool.tiles.headOption.map(_.srid).getOrElse(0)))
          .getOrElse(throw new IllegalStateException(
            s"no geometry set for $arrayRoot; geometry.json missing or stale"))
        ext = withVolumeDataset(resolve(corpusRoot, arrayRoot))(extentOf)
        val pairs: Seq[(Array[Byte], Double)] = fn match {
          case "rst_dtmfromgeoms_agg"   => gset.zpointWkbs.map(b => (b, 0.0))
          case "rst_gridfrompoints_agg" => gset.pointPairs
          case _                        => gset.boxPairs  // rst_rasterize_agg
        }
        import spark.implicits._
        spark.createDataFrame(pairs).toDF("geom_wkb", "value")
      }
      // coalesce(1): the group DataFrame's tile column is built by rst_fromcontent
      // (tile_aggregate) which opens rasters through GDAL; keep its materialization on
      // a single task thread too, for the same local-mode AutoSkipDrivers reason.
      val cached = groupDf.coalesce(1).cache()
      cached.count()
      // CONSISTENCY: one group -> one out tile -> raster fingerprint.
      val fp = {
        val one = cached.withColumn("key", lit(0))
        val aggCol = BenchDispatch.aggregateColumn(fn, one, ext, a)
        val collected = one.groupBy("key").agg(aggCol.alias("out")).collect()
        if (collected.isEmpty || collected(0).isNullAt(collected(0).fieldIndex("out")))
          BenchFingerprint.empty
        else {
          val tile = collected(0).getStruct(collected(0).fieldIndex("out"))
          val rasterBytes = tile.getAs[Array[Byte]]("raster")
          if (rasterBytes == null || rasterBytes.isEmpty) BenchFingerprint.empty
          else {
            val ds = RasterDriver.readFromBytes(rasterBytes, Map.empty)
            try BenchFingerprint.ofDataset(ds) finally RasterDriver.releaseDataset(ds)
          }
        }
      }
      // PERF: time the scaled groupBy (the fixed group replicated across N keys).
      sorted.foreach { n =>
        try {
          // Single partition end-to-end (see the shuffle.partitions=1 note above):
          // one key partition, the scaled crossJoin coalesced to one partition, so the
          // groupBy's map and reduce sides run on ONE task thread. GDAL is therefore
          // touched serially -> no concurrent AutoSkipDrivers -> no local-mode SIGSEGV.
          // The N row copies still flow through a real groupBy().agg(); only the
          // parallelism is removed (timing is now single-thread, documented above).
          val keys = spark.range(n).select(col("id").alias("key")).repartition(1)
          val scaled = keys.crossJoin(org.apache.spark.sql.functions.broadcast(cached))
            .coalesce(1)
          val aggCol = BenchDispatch.aggregateColumn(fn, scaled, ext, a)
          val (median, mn, p90) = timeIters(() => {
            scaled.groupBy("key").agg(aggCol.alias("out"))
              .write.format("noop").mode("overwrite").save()
          }, warmup, measured)
          val mpixS = if (median > 0) mpix(pool.tile_px, pool.bands, n) / (median / 1000.0) else 0.0
          val emitFp = if (n == sorted.head) fp else ""
          emit(row(e, runId, fn, "spark-path", pool.tile_px, pool.bands, pool.dtype, poolSrid, n,
            0.0, warmup, measured, median, mn, p90, mpixS,
            if (median > 0) n / (median / 1000.0) else 0.0, "ok", "", emitFp))
        } catch {
          case ex: Throwable =>
            emit(row(e, runId, fn, "spark-path", pool.tile_px, pool.bands, pool.dtype, poolSrid, n,
              0.0, warmup, 0, 0, 0, 0, 0, 0, "error",
              Option(ex.getMessage).getOrElse(ex.toString).take(300), ""))
        }
      }
      cached.unpersist()
    } catch {
      case ex: Throwable => errAll(Option(ex.getMessage).getOrElse(ex.toString))
    } finally {
      spark.conf.set("spark.sql.shuffle.partitions", savedShuffle)
    }
  }
}
