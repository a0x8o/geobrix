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
    // + 5 bucket-C C4 (tiling: maketiles/retile/tooverlappingtiles/
    //   separatebands/xyzpyramid -> raster_collection fingerprint)
    // + 11 bucket-B B-grid (DGGS: h3_tessellate + 10 {h3,quadbin}
    //   rastertogrid{avg,count,max,median,min} -> dggs_grid fingerprint)
    // + 2 bucket-B B-vec (contour, polygonize -> vector fingerprint)
    // + 3 bucket-D geometry-in (rasterize/gridfrompoints/dtmfromgeoms -> raster fp)
    assert(BenchDispatch.all.size == 100)
    assert(BenchDispatch.category("rst_h3_tessellate") == "dggs")
    assert(BenchDispatch.inputKind("rst_rasterize") == "geometry")
    assert(BenchDispatch.inputKind("rst_gridfrompoints") == "geometry")
    assert(BenchDispatch.inputKind("rst_dtmfromgeoms") == "geometry")
    assert(BenchDispatch.category("rst_rasterize") == "vector")
    assert(BenchDispatch.category("rst_quadbin_rastertogridavg") == "dggs")
    assert(BenchDispatch.category("rst_contour") == "vector")
    assert(BenchDispatch.category("rst_polygonize") == "vector")
    assert(BenchDispatch.category("rst_xyzpyramid") == "format")
    assert(BenchDispatch.category("rst_separatebands") == "format")
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

  // The heavy runner passes an empty args map, so the default in BenchDispatch IS the
  // value compared cross-engine. It must equal the authoritative pyrx FnSpec default
  // (bench/spec.py rst_tooverlappingtiles overlap=25). A mismatched default (was 32)
  // produced different overlapping-window positions and a ~3% pooled-pixel divergence.
  test("tooverlappingtiles default overlap matches the pyrx spec (25, not 32)") {
    val mapper = new ObjectMapper()
    val deflt = mapper.readTree(BenchDispatch.pureCore("rst_tooverlappingtiles", ds, Map.empty))
    val ov25 = mapper.readTree(
      BenchDispatch.pureCore("rst_tooverlappingtiles", ds, Map("overlap" -> "25")))
    val ov32 = mapper.readTree(
      BenchDispatch.pureCore("rst_tooverlappingtiles", ds, Map("overlap" -> "32")))
    // Default must agree with the spec value (25) and differ from the old default (32).
    assert(deflt.get("agg").get("mean").asDouble() == ov25.get("agg").get("mean").asDouble())
    assert(deflt.get("agg").get("std").asDouble() == ov25.get("agg").get("std").asDouble())
    assert(deflt.get("agg").get("mean").asDouble() != ov32.get("agg").get("mean").asDouble())
  }
}
