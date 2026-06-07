package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.gdal
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import java.nio.file.Files

class HeavyRunnerTest extends AnyFunSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    GDALManager.loadSharedObjects(Iterable.empty[String])
    GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
    gdal.AllRegister()
  }

  test("pure-core run over a tiny corpus yields ok heavyweight rows with fingerprints") {
    val tif = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
      .toString.replace("file:/", "/")
    val dir = Files.createTempDirectory("corpus")
    val json =
      s"""{"seed":1,"size_sweep":[
         | {"path":"$tif","cellid":0,"srid":4326,"dtype":"float32","bands":1,"tile_px":1200,"nodata_frac":0.0}],
         | "row_pool":{"tile_px":1200,"bands":1,"dtype":"float32","tiles":[]}}""".stripMargin
    Files.write(dir.resolve("corpus.json"), json.getBytes)

    val rows = HeavyRunner.runPureCore(
      corpusRoot = dir.toString,
      corpus = BenchManifest.read(dir.resolve("corpus.json").toString),
      fns = Seq("rst_width", "rst_avg", "rst_ndvi"),
      runId = "t", warmup = 1, measured = 2, argsByFn = Map.empty)

    assert(rows.nonEmpty)
    assert(rows.forall(_.api == "heavyweight"))
    val byFn = rows.groupBy(_.fn)
    assert(byFn("rst_width").head.status == "ok")
    assert(byFn("rst_width").head.output_fingerprint.contains("scalar"))
    assert(byFn("rst_ndvi").head.status == "na_by_design")  // 1-band tile, ndvi needs 2
  }
}
