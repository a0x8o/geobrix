package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.fasterxml.jackson.databind.ObjectMapper
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class BenchDispatchTest extends AnyFunSuite with BeforeAndAfterAll {
  var ds: Dataset = _
  override def beforeAll(): Unit = {
    GDALManager.loadSharedObjects(Iterable.empty[String])
    GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
    gdal.AllRegister()
    val tif = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
      .toString.replace("file:/", "/")
    ds = gdal.Open(tif)
  }
  override def afterAll(): Unit = if (ds != null) ds.delete()

  test("registry covers the ds-in functions with categories + min_bands") {
    assert(BenchDispatch.all.toSet.contains("rst_width"))
    // 19 representative + 15 Task 2 scalar + 7 Task 3 coord + 6 Task 4 map/struct
    // + 13 Task 5 tile-out scalar-args + 10 Task 6 tile-out complex-args
    // + 6 bucket-C C1/C2 (readers + buildoverviews + subdataset fns)
    // + 3 bucket-C C3 (multi-tile: frombands/combineavg/merge)
    assert(BenchDispatch.all.size == 79)
    assert(BenchDispatch.minBands("rst_ndvi") == 2)
    assert(BenchDispatch.minBands("rst_band") == 2)
    assert(BenchDispatch.minBands("rst_evi") == 2)
    assert(BenchDispatch.minBands("rst_index") == 2)
    assert(BenchDispatch.category("rst_proximity") == "analysis")
    assert(BenchDispatch.minBands("rst_width") == 1)
    assert(BenchDispatch.category("rst_resample") == "resample")
    assert(BenchDispatch.category("rst_slope") == "terrain")
    assert(BenchDispatch.category("rst_srid") == "accessor")
  }

  test("pureCore runs an accessor and a terrain op, returning a fingerprint") {
    val wfp = new ObjectMapper().readTree(BenchDispatch.pureCore("rst_width", ds, Map.empty))
    assert(wfp.get("kind").asText() == "scalar")
    val sfp = new ObjectMapper().readTree(BenchDispatch.pureCore("rst_slope", ds, Map.empty))
    assert(sfp.get("kind").asText() == "raster")
  }
}
