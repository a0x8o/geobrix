package com.databricks.labs.gbx.vectorx.jts

import org.locationtech.jts.geom.{Coordinate, LineString}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class InterpolateElevationTest extends AnyFunSuite {

    /** z = 2*x + 3*y + 5 sampled at the 4 corners of a 100x100 extent. */
    private def planePoints() = Seq(
        JTS.point(new Coordinate(0.0,   0.0,   2 * 0.0   + 3 * 0.0   + 5)),
        JTS.point(new Coordinate(100.0, 0.0,   2 * 100.0 + 3 * 0.0   + 5)),
        JTS.point(new Coordinate(0.0,   100.0, 2 * 0.0   + 3 * 100.0 + 5)),
        JTS.point(new Coordinate(100.0, 100.0, 2 * 100.0 + 3 * 100.0 + 5))
    )

    test("pointGridBBox emits widthPx*heightPx cell centers inside the extent") {
        val grid = InterpolateElevation.pointGridBBox(0.0, 0.0, 100.0, 100.0, 10, 10, 32633)
        grid.getNumGeometries shouldBe 100
        val p0 = grid.getGeometryN(0)
        p0.getCoordinate.x shouldBe 5.0 +- 1e-9
        p0.getCoordinate.y shouldBe 5.0 +- 1e-9
    }

    test("interpolate reproduces a planar surface exactly (linear TIN)") {
        val mp = JTS.multiPoint(planePoints().toArray)
        val grid = InterpolateElevation.pointGridBBox(0.0, 0.0, 100.0, 100.0, 10, 10, 32633)
        val out = InterpolateElevation.interpolate(mp, Seq.empty[LineString], grid, 0.0, 0.0)
        out should not be empty
        out.foreach { p =>
            val expected = 2 * p.getX + 3 * p.getY + 5
            p.getCoordinate.getZ shouldBe expected +- 1e-6
        }
    }

    test("interpolate skips (does not throw on) points outside the convex hull") {
        val mp = JTS.multiPoint(planePoints().toArray)
        val grid = InterpolateElevation.pointGridBBox(-50.0, -50.0, 150.0, 150.0, 20, 20, 32633)
        val out = InterpolateElevation.interpolate(mp, Seq.empty[LineString], grid, 0.0, 0.0)
        out.size should be > 0  // interior (in-hull) cells still interpolate; out-of-hull skipped, not thrown
    }
}
