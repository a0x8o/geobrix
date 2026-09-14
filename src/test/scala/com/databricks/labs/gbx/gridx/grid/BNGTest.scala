package com.databricks.labs.gbx.gridx.grid

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.locationtech.jts.io.WKTReader
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Conformance tests: BNG implements GridSystem correctly. */
class BNGTest extends AnyFunSuite {

    test("BNG conforms to GridSystem and renders cell ids as strings") {
        val g: GridSystem = BNG
        g.name shouldBe "BNG"
        g.crsSrid shouldBe 27700
        val cell = g.pointToCellID(530000.0, 180000.0, BNG.getResolution("1km"))
        g.renderCellId(cell) shouldBe a[String]
        g.renderCellId(cell) shouldBe BNG.format(cell)
        g.polyfill(g.cellIdToGeometry(cell), BNG.getResolution("1km")) should not be empty
    }

    test("bng geometryKRing boundary-out (mode) equals legacy no-mode") {
        val wkt = new WKTReader()
        val g = wkt.read("POLYGON((529000 179000,529000 182000,532000 182000,532000 179000,529000 179000))")
        assert(BNG.geometryKRing(g, 3, 1, "boundary-out") == BNG.geometryKRing(g, 3, 1))
    }

    test("bng geometryKRing boundary-in stays inside the covering set and is disjoint from boundary-out") {
        val wkt = new WKTReader()
        val g = wkt.read("POLYGON((528000 178000,528000 184000,534000 184000,534000 178000,528000 178000))")
        val cls = GeomDilation.classify(BNG, g, 3)
        val out = BNG.geometryKRing(g, 3, 1, "boundary-out")
        val inn = BNG.geometryKRing(g, 3, 1, "boundary-in")
        // LOCKED design: boundary-out returns the OUTWARD band (geom excluded); boundary-in returns
        // the INWARD band inside the covering set. They are now DISJOINT (previously boundary-out
        // included the geom, so boundary-in was a subset of it).
        assert(inn.nonEmpty, "boundary-in must be non-empty")
        assert(inn.subsetOf(cls.pCover), "boundary-in must stay within the covering set")
        assert(inn.intersect(out).isEmpty, "inward band must be disjoint from the outward band")
    }

    test("bng geometryKRing boundary-out expands outward for aligned geom (alignment-robustness)") {
        val wkt = new WKTReader()
        // 3 km × 3 km box with corners exactly on 1 km BNG grid lines → no straddling cells.
        // The old straddling-cell (pBorder) seed produced no outward expansion for such geoms.
        // With the perimeter fix boundary-out expands; and under the LOCKED design the geom is
        // EXCLUDED, so the k=1 band is non-empty and disjoint from pCover.
        val g   = wkt.read("POLYGON((529000 179000,529000 182000,532000 182000,532000 179000,529000 179000))")
        val cls = GeomDilation.classify(BNG, g, 3)
        assert(cls.pBorder.isEmpty, "test precondition: aligned polygon must have no straddling cells (pBorder empty)")
        val ring = BNG.geometryKRing(g, 3, 1)
        assert(ring.nonEmpty,
          "boundary-out must expand outward even when pBorder is empty (alignment-robustness)")
        assert(ring.intersect(cls.pCover).isEmpty, "boundary-out outward band must exclude the geom")
    }

    test("bng geometryKLoop boundary-out outward shell for aligned geom") {
        val wkt = new WKTReader()
        val g   = wkt.read("POLYGON((529000 179000,529000 182000,532000 182000,532000 179000,529000 179000))")
        val r1  = BNG.geometryKRing(g, 3, 1)
        val r0  = BNG.geometryKRing(g, 3, 0)
        val l1  = BNG.geometryKLoop(g, 3, 1)
        // kLoop must equal ring(k) \ ring(k-1) for the BNG standalone methods
        assert(l1 == r1.diff(r0), "bng geometryKLoop must equal ring(1) diff ring(0)")
    }

    test("BNG.coveringCandidateCells returns non-empty Seq[Long] for a GB bbox") {
        val g: GridSystem = BNG
        // 2 km x 2 km bbox around central London in EPSG:27700
        val bbox = JTS.polygonFromXYs(
          Array(
            (529000.0, 179000.0),
            (531000.0, 179000.0),
            (531000.0, 181000.0),
            (529000.0, 181000.0),
            (529000.0, 179000.0)
          )
        )
        bbox.setSRID(27700)
        val candidates = g.coveringCandidateCells(bbox, BNG.getResolution("1km"))
        candidates should not be empty
        // All returned ids are Long (not strings)
        val centerCell = BNG.pointToCellID(530000.0, 180000.0, BNG.getResolution("1km"))
        candidates should contain(centerCell)
    }
}
