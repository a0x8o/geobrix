package com.databricks.labs.gbx.gridx.grid

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.locationtech.jts.geom.Coordinate
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CustomGridSystemTest extends AnyFunSuite with Matchers {

    val conf = GridConf(
      boundXMin = 0,
      boundXMax = 100,
      boundYMin = 0,
      boundYMax = 100,
      cellSplits = 2,
      rootCellSizeX = 10,
      rootCellSizeY = 10,
      crsID = Some(32633)
    )
    val g = CustomGridSystem(conf)

    // res 0: cellWidth=10, totalCellsX=10, rootCellCountX=ceil(100/10)=10

    test("pointToCellID res0 at (5,5) has resolution 0 and envelope [0,10]x[0,10]") {
        val id = g.pointToCellID(5.0, 5.0, 0)
        g.getCellResolution(id) shouldBe 0
        val env = g.cellIdToGeometry(id).getEnvelopeInternal
        env.getMinX shouldBe 0.0 +- 1e-9
        env.getMaxX shouldBe 10.0 +- 1e-9
        env.getMinY shouldBe 0.0 +- 1e-9
        env.getMaxY shouldBe 10.0 +- 1e-9
    }

    test("pointToCellID res0 at (15,25) has envelope [10,20]x[20,30]") {
        val id = g.pointToCellID(15.0, 25.0, 0)
        val env = g.cellIdToGeometry(id).getEnvelopeInternal
        env.getMinX shouldBe 10.0 +- 1e-9
        env.getMaxX shouldBe 20.0 +- 1e-9
        env.getMinY shouldBe 20.0 +- 1e-9
        env.getMaxY shouldBe 30.0 +- 1e-9
    }

    test("pointToCellID res1 at (2.5,2.5) has resolution 1 and envelope [0,5]x[0,5]") {
        // res1: cellWidth=10/2^1=5
        val id = g.pointToCellID(2.5, 2.5, 1)
        g.getCellResolution(id) shouldBe 1
        val env = g.cellIdToGeometry(id).getEnvelopeInternal
        env.getMinX shouldBe 0.0 +- 1e-9
        env.getMaxX shouldBe 5.0 +- 1e-9
        env.getMinY shouldBe 0.0 +- 1e-9
        env.getMaxY shouldBe 5.0 +- 1e-9
    }

    test("cellIdToCenter at res0 (5,5) is approximately (5,5)") {
        val id = g.pointToCellID(5.0, 5.0, 0)
        val center: Coordinate = g.cellIdToCenter(id)
        center.x shouldBe 5.0 +- 1e-9
        center.y shouldBe 5.0 +- 1e-9
    }

    test("polyfill POLYGON((0 0, 30 0, 30 30, 0 30, 0 0)) at res0 returns 9 cells all within [0,30]x[0,30]") {
        val poly = JTS.fromWKT("POLYGON ((0 0, 30 0, 30 30, 0 30, 0 0))")
        val cells = g.polyfill(poly, 0)
        cells should have size 9
        cells.foreach { cellId =>
            val env = g.cellIdToGeometry(cellId).getEnvelopeInternal
            env.getMinX should be >= 0.0
            env.getMaxX should be <= 30.0
            env.getMinY should be >= 0.0
            env.getMaxY should be <= 30.0
        }
    }

    test("kRing at res0 (15,15) with k=1 returns 9 entries including center") {
        // cellPosX=1, cellPosY=1 => interior 3x3 ring
        val center = g.pointToCellID(15.0, 15.0, 0)
        val ring = g.kRing(center, 1)
        ring should have size 9
        ring should contain(center)
    }

    test("pointToCellID rejects a NaN X coordinate via the NaN guard") {
        val ex = the[IllegalStateException] thrownBy g.pointToCellID(Double.NaN, 5.0, 0)
        ex.getMessage should include("NaN coordinates are not supported")
    }

    test("pointToCellID rejects a NaN Y coordinate via the NaN guard") {
        // Resolved decision 3: the guard formerly read `!x.isNaN && !x.isNaN`, so a NaN Y
        // was never caught by the NaN guard. Pre-fix a NaN Y is only (incidentally) rejected
        // by the later Y-bounds `require` (NaN comparisons are false), surfacing the wrong
        // message. The fix makes the NaN guard itself catch a NaN Y -> assert that message.
        val ex = the[IllegalStateException] thrownBy g.pointToCellID(5.0, Double.NaN, 0)
        ex.getMessage should include("NaN coordinates are not supported")
    }

}
