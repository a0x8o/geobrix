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

  test("registry covers the 19 ds-in functions with categories + min_bands") {
    assert(BenchDispatch.all.toSet.contains("rst_width"))
    assert(BenchDispatch.all.size == 19)
    assert(BenchDispatch.minBands("rst_ndvi") == 2)
    assert(BenchDispatch.minBands("rst_width") == 1)
    assert(BenchDispatch.category("rst_slope") == "terrain")
  }

  test("pureCore runs an accessor and a terrain op, returning a fingerprint") {
    val wfp = new ObjectMapper().readTree(BenchDispatch.pureCore("rst_width", ds, Map.empty))
    assert(wfp.get("kind").asText() == "scalar")
    val sfp = new ObjectMapper().readTree(BenchDispatch.pureCore("rst_slope", ds, Map.empty))
    assert(sfp.get("kind").asText() == "raster")
  }
}
