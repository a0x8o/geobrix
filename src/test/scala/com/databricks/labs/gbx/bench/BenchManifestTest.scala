package com.databricks.labs.gbx.bench

import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.Files

class BenchManifestTest extends AnyFunSuite {
  test("reads a corpus.json into typed case classes") {
    val json =
      """{"seed":7,
        | "size_sweep":[{"path":"size/t0.tif","cellid":0,"srid":4326,"dtype":"float32","bands":2,"tile_px":256,"nodata_frac":0.0}],
        | "row_pool":{"tile_px":1024,"bands":4,"dtype":"float32",
        |   "tiles":[{"path":"rows/r0.tif","cellid":1,"srid":3857,"dtype":"float32","bands":4,"tile_px":1024,"nodata_frac":0.0}]}}""".stripMargin
    val dir = Files.createTempDirectory("corpus")
    val f = dir.resolve("corpus.json")
    Files.write(f, json.getBytes)
    val c = BenchManifest.read(f.toString)
    assert(c.seed == 7)
    assert(c.size_sweep.length == 1)
    assert(c.size_sweep.head.path == "size/t0.tif")
    assert(c.size_sweep.head.tile_px == 256 && c.size_sweep.head.bands == 2)
    assert(c.row_pool.tiles.head.srid == 3857)
  }
}
