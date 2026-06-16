package com.databricks.labs.gbx.bench

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.io.{FileInputStream, FileOutputStream, OutputStreamWriter}
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.SparkSession

/** Mirrors the Python ResultRow (results.py) field-for-field (snake_case = JSON keys). */
case class BenchRow(
    run_id: String,
    api: String,
    fn: String,
    category: String,
    mode: String,
    tile_px: Int,
    bands: Int,
    dtype: String,
    srid: Int,
    rows: Int,
    nodata_frac: Double,
    warmup_iters: Int,
    measured_iters: Int,
    median_ms: Double,
    min_ms: Double,
    p90_ms: Double,
    throughput_mpix_s: Double,
    throughput_rows_s: Double,
    peak_rss_mb: Double,
    status: String,
    note: String,
    env_arch: String,
    env_cpu_model: String,
    env_cpu_count: Int,
    env_os: String,
    env_gbx_version: String,
    env_gdal_version: String,
    env_runtime_version: String,
    env_where: String,
    output_fingerprint: String,
    // Wall clock over the measured iterations: total (sum) + avg (mean). Mirrors the
    // Python ResultRow fields; default 0.0 so error/skip rows stay valid.
    total_wall_clock_ms: Double = 0.0,
    avg_wall_clock_ms: Double = 0.0
)

object BenchIO {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def toJson(row: BenchRow): String = mapper.writeValueAsString(row)

  /** Volume-native byte read for the JVM. UC Volumes are cloud object storage: the JVM
    * cannot read a `/Volumes` file via java.io / java.nio (EPERM -- no random access) NOR
    * via the Hadoop FileSystem directly (UC_VOLUMES_NOT_SUPPORTED). The only UC-aware
    * reader available to the JVM is Spark itself, so read the file's bytes through the
    * binaryFile data source (the same path the row-tile reader uses). This is the JVM
    * equivalent of Python's `Path.read_bytes()` on a Volume. Works on-cluster (UC
    * Volume) and in local tests (local file) alike. */
  def readBytes(path: String): Array[Byte] = {
    // Dual-mode: a LOCAL file (e.g. a pure-core corpus staged to local disk) reads via
    // plain java.io; a UC Volume path can't be opened by the JVM (EPERM), so fall back
    // to Spark's binaryFile (the only UC-aware reader). FileInputStream on a /Volumes
    // path raises IOException -> Spark fallback.
    try {
      val is = new FileInputStream(path)
      try is.readAllBytes()
      finally is.close()
    } catch {
      case _: java.io.IOException =>
        SparkSession.active.read
          .format("binaryFile")
          .load(path)
          .select("content")
          .head()
          .getAs[Array[Byte]](0)
    }
  }

  def writeJsonl(rows: Seq[BenchRow], path: String): Unit = {
    val p = Paths.get(path)
    Option(p.getParent).foreach(Files.createDirectories(_))
    val sb = new StringBuilder
    rows.foreach(r => sb.append(toJson(r)).append("\n"))
    Files.write(p, sb.toString.getBytes(StandardCharsets.UTF_8))
  }

  /** Incremental, crash-resilient JSONL sink. Truncates `path` on open; each
   *  `append` writes one row's serialized line (byte-identical to `writeJsonl`)
   *  and fsyncs to disk so rows survive a later native JVM crash. */
  final class JsonlAppender private[bench] (path: String) extends AutoCloseable {
    private val p = Paths.get(path)
    Option(p.getParent).foreach(Files.createDirectories(_))
    // append=false → truncate at start, matching writeJsonl's "w" semantics.
    private val fos = new FileOutputStream(p.toFile, false)
    private val writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8)

    def append(row: BenchRow): Unit = {
      writer.write(toJson(row))
      writer.write("\n")
      writer.flush()       // push out of the JVM writer buffer
      // fsync so completed rows survive a native crash. Best-effort: the dbfs
      // /Volumes FUSE mount can reject fsync ("Operation not permitted"); the row
      // is already flushed to the OS, so skip durability rather than abort the run.
      try fos.getFD.sync()
      catch { case _: java.io.IOException => () }
    }

    override def close(): Unit = {
      writer.flush()
      writer.close()
    }
  }

  def appendWriter(path: String): JsonlAppender = new JsonlAppender(path)
}
