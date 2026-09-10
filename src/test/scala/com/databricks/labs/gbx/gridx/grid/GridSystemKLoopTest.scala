package com.databricks.labs.gbx.gridx.grid

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for Quadbin.kLoop and GridSystem trait polymorphism (kRing/kLoop). */
class GridSystemKLoopTest extends AnyFunSuite {

    test("Quadbin.kLoop is the hollow ring and composes with kRing") {
        val c = Quadbin.pointToCellID(-0.1, 51.5, 15)
        assert(Quadbin.kLoop(c, 0) == Seq(c))
        val ring1 = Quadbin.kLoop(c, 1)
        assert(!ring1.contains(c) && ring1.toSet == (Quadbin.kRing(c, 1).toSet - c))
        assert((0 to 2).flatMap(Quadbin.kLoop(c, _)).toSet == Quadbin.kRing(c, 2).toSet)
    }

    test("kLoop is reachable through the GridSystem trait") {
        val g: GridSystem = Quadbin
        assert(g.kLoop(Quadbin.pointToCellID(-0.1, 51.5, 15), 1).nonEmpty)
    }

    test("kRing is reachable through the GridSystem trait for H3") {
        val g: GridSystem = H3
        val cell = H3.pointToCellID(-0.1, 51.5, 8)
        assert(g.kRing(cell, 1).nonEmpty)
    }

    test("Quadbin.kLoop(cell, k) size is (2k+1)^2 - (2(k-1)+1)^2 for interior cells") {
        // For an interior cell at zoom 15, ring 1 has (3^2 - 1^2) = 8, ring 2 has (5^2 - 3^2) = 16
        val c = Quadbin.pointToCellID(-0.1, 51.5, 15)
        Quadbin.kLoop(c, 1) should have size 8
        Quadbin.kLoop(c, 2) should have size 16
    }

    test("Quadbin.kLoop(cell, k) does not overlap with kLoop(cell, k-1)") {
        val c = Quadbin.pointToCellID(-0.1, 51.5, 15)
        val r1 = Quadbin.kLoop(c, 1).toSet
        val r2 = Quadbin.kLoop(c, 2).toSet
        r1.intersect(r2) shouldBe empty
    }

}
