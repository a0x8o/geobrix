package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.fasterxml.jackson.databind.ObjectMapper
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class BenchFingerprintTest extends AnyFunSuite with BeforeAndAfterAll {
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

  test("ofDataset emits raster fingerprint with per-band stats") {
    val node = new ObjectMapper().readTree(BenchFingerprint.ofDataset(ds))
    assert(node.get("kind").asText() == "raster")
    val b0 = node.get("bands").get(0)
    for (k <- Seq("shape", "dtype", "nodata_count", "min", "max", "mean", "std")) assert(b0.has(k))
    assert(b0.get("shape").get(0).asInt() == ds.GetRasterYSize())
    assert(b0.get("shape").get(1).asInt() == ds.GetRasterXSize())
  }

  test("ofScalar and ofArray match the python kinds") {
    assert(new ObjectMapper().readTree(BenchFingerprint.ofScalar(256)).get("kind").asText() == "scalar")
    val arr = new ObjectMapper().readTree(BenchFingerprint.ofArray(Array(1.0, 2.0)))
    assert(arr.get("kind").asText() == "scalar_list")
    assert(arr.get("values").get(1).asDouble() == 2.0)
  }
}
