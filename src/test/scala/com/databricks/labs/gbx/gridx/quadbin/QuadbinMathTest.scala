package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.gridx.grid.Quadbin
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class QuadbinMathTest extends AnyFunSuite {

    test("pointToCell at z=0 returns the single root cell (header + zoom + zero Morton)") {
        val cell = Quadbin.pointToCell(0.0, 0.0, 0)
        Quadbin.resolution(cell) shouldBe 0
        val (x, y) = Quadbin.cellXY(cell)
        x shouldBe 0L
        y shouldBe 0L
        // CARTO header: bit 62 set + mode = 1 in bits 59..61
        ((cell >>> 62) & 0x1L) shouldBe 1L
        ((cell >>> 59) & 0x7L) shouldBe 1L
    }

    test("pointToCell round-trip — bbox(pointToCell(lon, lat, z)) contains (lon, lat)") {
        val points = Seq(
          (-122.4194, 37.7749),  // San Francisco
          (0.0, 0.0),
          (151.2093, -33.8688),  // Sydney
          (-180.0, 85.0),
          (179.99, -84.99)
        )
        val zooms = Seq(0, 5, 10, 15, 20, 26)
        for { (lon, lat) <- points; z <- zooms } {
            val cell = Quadbin.pointToCell(lon, lat, z)
            val (xmin, ymin, xmax, ymax) = Quadbin.cellBbox(cell)
            assert(lon >= xmin - 1e-6 && lon <= xmax + 1e-6, s"lon=$lon not in [$xmin, $xmax] for cell at z=$z")
            assert(lat >= ymin - 1e-6 && lat <= ymax + 1e-6, s"lat=$lat not in [$ymin, $ymax] for cell at z=$z")
        }
    }

    test("resolution bit extraction matches input z for every supported zoom") {
        for (z <- 0 to Quadbin.MAX_RESOLUTION) {
            val cell = Quadbin.pointToCell(0.0, 0.0, z)
            Quadbin.resolution(cell) shouldBe z
        }
    }

    test("encode + cellXY round-trip preserves (x, y)") {
        for (z <- Seq(0, 1, 5, 10, 20, 26)) {
            val n = if (z == 0) 1L else 1L << z
            val samples = Seq((0L, 0L), (n - 1L, n - 1L), (n / 2L, n / 3L))
            for ((x, y) <- samples) {
                val cell = Quadbin.encode(z, x, y)
                val (rx, ry) = Quadbin.cellXY(cell)
                Quadbin.resolution(cell) shouldBe z
                rx shouldBe x
                ry shouldBe y
            }
        }
    }

    test("cellDistance — same cell is 0; adjacent cell is 1; require same resolution") {
        val c = Quadbin.pointToCell(0.0, 0.0, 10)
        Quadbin.cellDistance(c, c) shouldBe 0
        val ring = Quadbin.kRing(c, 1)
        val neighbour = ring.find(_ != c).get
        Quadbin.cellDistance(c, neighbour) shouldBe 1

        val other = Quadbin.pointToCell(0.0, 0.0, 9)
        intercept[IllegalArgumentException] { Quadbin.cellDistance(c, other) }
    }

    test("kRing returns 9 cells for an interior cell at k=1, 25 at k=2") {
        // Interior cell at z=10 (lon=0, lat=0)
        val c = Quadbin.pointToCell(0.0, 0.0, 10)
        Quadbin.kRing(c, 0) should have length 1
        Quadbin.kRing(c, 1) should have length 9
        Quadbin.kRing(c, 2) should have length 25
    }

    test("polyfillBbox covers a small region and respects maxCells guard") {
        // Small bbox near (0, 0) at z=8 → small number of cells
        val cells = Quadbin.polyfillBbox((-1.0, -1.0, 1.0, 1.0), 8)
        cells.length should be > 0
        cells.foreach(c => Quadbin.resolution(c) shouldBe 8)
        // Cell-count guard
        intercept[IllegalArgumentException] {
            Quadbin.polyfillBbox((-180.0, -85.0, 180.0, 85.0), 20, maxCells = 1000)
        }
    }
}
