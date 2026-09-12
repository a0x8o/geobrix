package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.gridx.grid.{GeomDilation, Quadbin}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Unit tests for Quadbin_GeometryKRing, Quadbin_GeometryKLoop, and their Explode variants.
  *
  * Fixture: a small box around NYC (-73.99, 40.71 → -73.95, 40.75) at resolution 12.
  * At z=12 each cell is ~0.044° wide; the 4-cell-wide box covers ~16 polyfill cells.
  * For the holed-polygon tests: outer box (-73.99, 40.71 → -73.95, 40.75), inner hole
  * (-73.975, 40.725 → -73.965, 40.735) at res 13 (cells ~0.022° wide, <4 cells/hole).
  */
class Quadbin_GeometrySuite extends AnyFunSuite {

    private val NYC_WKT  = "POLYGON((-73.99 40.71, -73.95 40.71, -73.95 40.75, -73.99 40.75, -73.99 40.71))"
    private val NYC_WKB  = JTS.toWKB(JTS.fromWKT(NYC_WKT))
    private val RES      = 12

    // Holed polygon: NYC box with a small interior hole
    private val HOLED_WKT = "POLYGON((-73.99 40.71, -73.95 40.71, -73.95 40.75, -73.99 40.75, -73.99 40.71), " +
                            "(-73.975 40.725, -73.965 40.725, -73.965 40.735, -73.975 40.735, -73.975 40.725))"
    private val HOLED_WKB = JTS.toWKB(JTS.fromWKT(HOLED_WKT))
    private val HOLED_RES = 13  // smaller cells to ensure multi-cell hole

    test("Quadbin_GeometryKRing — k=0 == polyfill (execute)") {
        val geom   = JTS.fromWKB(NYC_WKB)
        val k0     = Quadbin_GeometryKRing.execute(geom, RES, 0, GeomDilation.DEFAULT_MODE)
        val fill   = Quadbin.polyfillBbox(
            (geom.getEnvelopeInternal.getMinX, geom.getEnvelopeInternal.getMinY,
             geom.getEnvelopeInternal.getMaxX, geom.getEnvelopeInternal.getMaxY), RES)
            .filter(c => Quadbin.cellIdToGeometry(c).intersects(geom))
            .toSet
        k0 shouldBe fill
    }

    test("Quadbin_GeometryKRing — filled superset of polyfill") {
        val geom = JTS.fromWKB(NYC_WKB)
        val k1   = Quadbin_GeometryKRing.execute(geom, RES, 1, GeomDilation.DEFAULT_MODE)
        val fill = Quadbin.polyfillBbox(
            (geom.getEnvelopeInternal.getMinX, geom.getEnvelopeInternal.getMinY,
             geom.getEnvelopeInternal.getMaxX, geom.getEnvelopeInternal.getMaxY), RES)
            .filter(c => Quadbin.cellIdToGeometry(c).intersects(geom))
            .toSet
        fill.subsetOf(k1) shouldBe true
        k1.size should be > fill.size
    }

    test("Quadbin_GeometryKLoop — loop == ring diff") {
        val geom = JTS.fromWKB(NYC_WKB)
        val r2   = Quadbin_GeometryKRing.execute(geom, RES, 2, GeomDilation.DEFAULT_MODE)
        val r1   = Quadbin_GeometryKRing.execute(geom, RES, 1, GeomDilation.DEFAULT_MODE)
        val loop = Quadbin_GeometryKLoop.execute(geom, RES, 2, GeomDilation.DEFAULT_MODE)
        loop shouldBe (r2 diff r1)
    }

    test("Quadbin_GeometryKRing — returns LongType cell ids (all at correct resolution)") {
        val geom  = JTS.fromWKB(NYC_WKB)
        val cells = Quadbin_GeometryKRing.execute(geom, RES, 1, GeomDilation.DEFAULT_MODE)
        cells.size should be > 0
        cells.foreach(c => Quadbin.resolution(c) shouldBe RES)
    }

    test("Quadbin_GeometryKRing — eval(WKB) non-null and consistent with execute") {
        val geom   = JTS.fromWKB(NYC_WKB)
        val expect = Quadbin_GeometryKRing.execute(geom, RES, 1, GeomDilation.DEFAULT_MODE).toArray.sorted
        val arr    = Quadbin_GeometryKRing.eval(NYC_WKB, RES, 1)
        arr should not be null
        arr.toObjectArray(org.apache.spark.sql.types.LongType).map(_.asInstanceOf[Long]).sorted shouldBe expect
    }

    test("Quadbin_GeometryKLoop — eval(WKB) non-null and consistent with execute") {
        val geom   = JTS.fromWKB(NYC_WKB)
        val expect = Quadbin_GeometryKLoop.execute(geom, RES, 1, GeomDilation.DEFAULT_MODE).toArray.sorted
        val arr    = Quadbin_GeometryKLoop.eval(NYC_WKB, RES, 1)
        arr should not be null
        arr.toObjectArray(org.apache.spark.sql.types.LongType).map(_.asInstanceOf[Long]).sorted shouldBe expect
    }

    test("Quadbin_GeometryKRing — all 6 modes produce non-empty or empty results consistently (no exception)") {
        val geom = JTS.fromWKT(HOLED_WKT)
        GeomDilation.MODES.foreach { mode =>
            val cells = Quadbin_GeometryKRing.execute(geom, HOLED_RES, 1, mode)
            // No exception is the primary assertion; check all are valid Longs at correct res
            cells.foreach(c => Quadbin.resolution(c) shouldBe HOLED_RES)
        }
    }

    test("Quadbin_GeometryKLoop — all 6 modes on holed polygon: no exception") {
        val geom = JTS.fromWKT(HOLED_WKT)
        GeomDilation.MODES.foreach { mode =>
            val cells = Quadbin_GeometryKLoop.execute(geom, HOLED_RES, 1, mode)
            cells.foreach(c => Quadbin.resolution(c) shouldBe HOLED_RES)
        }
    }

    test("Quadbin_GeometryKRing — unknown mode raises") {
        val geom = JTS.fromWKB(NYC_WKB)
        assertThrows[IllegalArgumentException] {
            Quadbin_GeometryKRing.execute(geom, RES, 1, "BOGUS")
        }
    }

    test("Quadbin_GeometryKLoop — unknown mode raises") {
        val geom = JTS.fromWKB(NYC_WKB)
        assertThrows[IllegalArgumentException] {
            Quadbin_GeometryKLoop.execute(geom, RES, 1, "BOGUS")
        }
    }

    test("Quadbin_GeometryKRing — null geom eval returns null") {
        val arr = Quadbin_GeometryKRing.eval(null.asInstanceOf[Array[Byte]], RES, 1)
        arr shouldBe null
    }

    test("Quadbin_GeometryKLoop — null geom eval returns null") {
        val arr = Quadbin_GeometryKLoop.eval(null.asInstanceOf[Array[Byte]], RES, 1)
        arr shouldBe null
    }

}
