package com.databricks.labs.gbx.rasterx.tile

import org.scalatest.funsuite.AnyFunSuite

/** Pure-logic unit tests for TileMath. Web-mercator XYZ tile bbox math is
 *  deterministic and CRS-only — no GDAL needed.
 */
class TileMathTest extends AnyFunSuite {

    test("tileBboxWebMerc at z=0 covers the full web-mercator extent") {
        val (xmin, ymin, xmax, ymax) = TileMath.tileBboxWebMerc(0, 0, 0)
        assert(math.abs(xmin - -20037508.342789244) < 1.0)
        assert(math.abs(xmax - 20037508.342789244) < 1.0)
        assert(math.abs(ymin - -20037508.342789244) < 1.0)
        assert(math.abs(ymax - 20037508.342789244) < 1.0)
    }

    test("tileBboxWebMerc z=1 four tiles tile the world") {
        // At z=1, 4 tiles tile the world. Their union extent must equal z=0.
        val tiles = for (x <- 0 to 1; y <- 0 to 1) yield TileMath.tileBboxWebMerc(1, x, y)
        val minX = tiles.map(_._1).min
        val maxX = tiles.map(_._3).max
        val minY = tiles.map(_._2).min
        val maxY = tiles.map(_._4).max
        assert(math.abs(minX - -20037508.342789244) < 1.0)
        assert(math.abs(maxX - 20037508.342789244) < 1.0)
        assert(math.abs(minY - -20037508.342789244) < 1.0)
        assert(math.abs(maxY - 20037508.342789244) < 1.0)
    }

    test("intersectingTiles returns ≥1 tile for a small bbox around (0,0) at z=10") {
        val tiles = TileMath.intersectingTiles(-0.001, -0.001, 0.001, 0.001, 10)
        assert(tiles.length >= 1 && tiles.length <= 4)
        tiles.foreach { case (z, x, y) =>
            assert(z == 10)
            assert(x >= 0 && x < (1 << 10))
            assert(y >= 0 && y < (1 << 10))
        }
    }

    test("tileBboxWebMerc validates out-of-range tile coords") {
        intercept[IllegalArgumentException](TileMath.tileBboxWebMerc(0, 1, 0))
        intercept[IllegalArgumentException](TileMath.tileBboxWebMerc(-1, 0, 0))
    }
}
