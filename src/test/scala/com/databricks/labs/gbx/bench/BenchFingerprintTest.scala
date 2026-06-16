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

  test("ofDggsGrid emits dggs_grid fingerprint with count, hash, sorted ids, agg") {
    val cells = Seq(Array((10L, 1.0), (5L, 3.0)), Array((7L, 2.0)))
    val node = new ObjectMapper().readTree(BenchFingerprint.ofDggsGrid(cells))
    assert(node.get("kind").asText() == "dggs_grid")
    assert(node.get("count").asInt() == 3)
    assert(node.get("cells_hash").asText().length == 64)
    // ids sorted ascending; agg over measures.
    assert(node.get("cell_ids").get(0).asLong() == 5L)
    assert(node.get("agg").get("min").asDouble() == 1.0)
    assert(node.get("agg").get("max").asDouble() == 3.0)
  }

  test("ofDggsGrid hash is order-independent") {
    val a = BenchFingerprint.ofDggsGrid(Seq(Array((5L, 1.0), (10L, 2.0))))
    val b = BenchFingerprint.ofDggsGrid(Seq(Array((10L, 2.0), (5L, 1.0))))
    assert(a == b)
  }

  test("ofVector lines uses length; polygons use area") {
    import com.databricks.labs.gbx.vectorx.jts.JTS
    val line = JTS.lineStringXYs(scala.collection.mutable.Buffer((0.0, 0.0), (3.0, 4.0)))
    val lineFp = new ObjectMapper().readTree(BenchFingerprint.ofVector(Seq((line, 1.0))))
    assert(lineFp.get("kind").asText() == "vector")
    assert(lineFp.get("count").asInt() == 1)
    assert(math.abs(lineFp.get("measure").asDouble() - 5.0) < 1e-9)

    val poly = JTS.polygonFromXYs(Array((0.0, 0.0), (0.0, 2.0), (2.0, 2.0), (2.0, 0.0), (0.0, 0.0)))
    val polyFp = new ObjectMapper().readTree(BenchFingerprint.ofVector(Seq((poly, 5.0))))
    assert(math.abs(polyFp.get("measure").asDouble() - 4.0) < 1e-9)
    assert(polyFp.get("attr_agg").get("max").asDouble() == 5.0)
  }
}
