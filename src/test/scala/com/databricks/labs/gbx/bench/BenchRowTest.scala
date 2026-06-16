package com.databricks.labs.gbx.bench

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.Files

class BenchRowTest extends AnyFunSuite {
  private def sampleRow(fn: String, median: Double) = BenchRow(
    run_id = "r", api = "heavyweight", fn = fn, category = "accessor", mode = "pure-core",
    tile_px = 256, bands = 1, dtype = "Float32", srid = 4326, rows = 1, nodata_frac = 0.0,
    warmup_iters = 1, measured_iters = 2, median_ms = median, min_ms = median, p90_ms = median,
    throughput_mpix_s = 1.0, throughput_rows_s = 1.0, peak_rss_mb = 0.0,
    status = "ok", note = "", env_arch = "x86_64", env_cpu_model = "c", env_cpu_count = 8,
    env_os = "Linux", env_gbx_version = "0.4.0", env_gdal_version = "3.12.1",
    env_runtime_version = "jvm17", env_where = "docker", output_fingerprint = "{\"kind\":\"scalar\",\"value\":256}")

  test("writeJsonl emits one JSON object per line with the Python schema keys") {
    val tmp = Files.createTempFile("bench", ".jsonl")
    BenchIO.writeJsonl(Seq(sampleRow("rst_width", 1.5), sampleRow("rst_avg", 2.0)), tmp.toString)
    val lines = Files.readAllLines(tmp).toArray.map(_.toString)
    assert(lines.length == 2)
    val node = new ObjectMapper().readTree(lines(0))
    for (k <- Seq("run_id","api","fn","mode","tile_px","median_ms","throughput_mpix_s",
                  "status","env_arch","env_gdal_version","output_fingerprint")) {
      assert(node.has(k), s"missing key $k")
    }
    assert(node.get("api").asText() == "heavyweight")
    assert(node.get("fn").asText() == "rst_width")
  }
}
