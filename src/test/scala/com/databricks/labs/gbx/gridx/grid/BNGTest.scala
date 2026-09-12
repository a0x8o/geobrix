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

    test("bng geometryKRing boundary-in is a strict inward subset") {
        val wkt = new WKTReader()
        val g = wkt.read("POLYGON((528000 178000,528000 184000,534000 184000,534000 178000,528000 178000))")
        val out = BNG.geometryKRing(g, 3, 1, "boundary-out")
        val inn = BNG.geometryKRing(g, 3, 1, "boundary-in")
        assert(inn.subsetOf(out) && inn.size < out.size)
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
