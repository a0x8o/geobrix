package com.databricks.labs.gbx.bench

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.io.{FileOutputStream, OutputStreamWriter}
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

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
    output_fingerprint: String
)

object BenchIO {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def toJson(row: BenchRow): String = mapper.writeValueAsString(row)

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
      fos.getFD.sync()     // fsync so completed rows survive a native crash
    }

    override def close(): Unit = {
      writer.flush()
      writer.close()
    }
  }

  def appendWriter(path: String): JsonlAppender = new JsonlAppender(path)
}
