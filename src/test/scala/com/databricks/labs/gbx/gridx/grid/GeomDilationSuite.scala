// src/test/scala/com/databricks/labs/gbx/gridx/grid/GeomDilationSuite.scala
package com.databricks.labs.gbx.gridx.grid

import org.scalatest.funsuite.AnyFunSuite
import org.locationtech.jts.io.WKTReader

class GeomDilationSuite extends AnyFunSuite {
  private val wkt = new WKTReader()
  // Quadbin is a concrete GridSystem with kLoop/polyfill/cellIdToGeometry.
  private val grid = Quadbin
  private val res = 12

  test("boundary-out ring is filled (keeps core)") {
    val g = wkt.read("POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))")
    val cls = GeomDilation.classify(grid, g, res)
    val ring = GeomDilation.expand("ring", 1, "boundary-out", grid, g, res)
    assert(cls.pCore.subsetOf(ring))
    assert(cls.pCover.subsetOf(ring))
  }

  test("hole modes empty when no holes") {
    val g = wkt.read("POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))")
    assert(GeomDilation.expand("ring", 3, "hole-in", grid, g, res).isEmpty)
  }

  test("loop k equals ring(k) minus ring(k-1)") {
    val g = wkt.read("POLYGON((-2 -2, -2 2, 2 2, 2 -2, -2 -2))")
    val r3 = GeomDilation.expand("ring", 3, "boundary-out", grid, g, res)
    val r2 = GeomDilation.expand("ring", 2, "boundary-out", grid, g, res)
    val l3 = GeomDilation.expand("loop", 3, "boundary-out", grid, g, res)
    assert(l3 == (r3 diff r2))
  }

  test("boundary-in stays inside geom and respects a hole") {
    val g = wkt.read("POLYGON((-3 -3,-3 3,3 3,3 -3,-3 -3),(-1 -1,-1 1,1 1,1 -1,-1 -1))")
    val cls = GeomDilation.classify(grid, g, res)
    val r = GeomDilation.expand("ring", 2, "boundary-in", grid, g, res)
    assert(r.subsetOf(cls.pCore union cls.pBorder))
    assert(r.intersect(cls.hCore).isEmpty)
  }

  test("unknown mode throws") {
    val g = wkt.read("POLYGON((0 0,0 1,1 1,1 0,0 0))")
    assertThrows[IllegalArgumentException](GeomDilation.expand("ring", 1, "sideways", grid, g, res))
  }
}
