package com.databricks.labs.gbx.gridx.grid

import org.locationtech.jts.geom.Geometry
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GridSystemTest extends AnyFunSuite {
  test("GridSystem exposes the members the raster refactor needs") {
    val members = classOf[GridSystem].getMethods.map(_.getName).toSet
    Set("name", "crsSrid", "resolutions", "pointToCellID", "cellIdToGeometry",
        "polyfill", "renderCellId").subsetOf(members) shouldBe true
  }
}
