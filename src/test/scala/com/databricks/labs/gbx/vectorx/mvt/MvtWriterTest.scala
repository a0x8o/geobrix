package com.databricks.labs.gbx.vectorx.mvt

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite

/**
  * Direct unit tests for [[MvtWriter]] — exercise edge-case branches that aren't
  * easily reached through the full Spark UDAF path (null WKB filtering, invalid WKB,
  * null attrs map, null individual field values, empty input).
  */
class MvtWriterTest extends AnyFunSuite {

    private val gf = new GeometryFactory()

    test("encode should return empty Array[Byte] for an empty feature list") {
        val out = MvtWriter.encode("empty", 4096, Seq.empty)
        assert(out != null)
        assert(out.isEmpty)
    }

    test("encode should skip null and empty WKB rows but still write good ones") {
        val wkb = JTS.toWKB(gf.createPoint(new Coordinate(0.5, 0.5)))
        val features = Seq(
            (null.asInstanceOf[Array[Byte]], Map[String, Any]("name" -> "skip-me")),
            (Array.emptyByteArray, Map[String, Any]("name" -> "also-skip")),
            (wkb, Map[String, Any]("name" -> "alpha"))
        )
        val out = MvtWriter.encode("layer1", 4096, features)
        assert(out != null && out.nonEmpty)
        assert(new String(out, "UTF-8").contains("layer1"))
    }

    test("encode should skip invalid WKB bytes (CreateGeometryFromWkb returns null)") {
        val good = JTS.toWKB(gf.createPoint(new Coordinate(0.5, 0.5)))
        val features = Seq(
            (Array[Byte](0, 1, 2, 3), Map[String, Any]("name" -> "bad-wkb")),
            (good, Map[String, Any]("name" -> "ok"))
        )
        val out = MvtWriter.encode("layer1", 4096, features)
        assert(out != null && out.nonEmpty)
        assert(new String(out, "UTF-8").contains("layer1"))
    }

    test("encode should handle null attrs map (no per-field iteration)") {
        val wkb = JTS.toWKB(gf.createPoint(new Coordinate(0.5, 0.5)))
        // Schema is derived from the FIRST non-null attrs map, so include one such row
        // and one with a null map to drive the `if (attrs != null)` false branch.
        val features = Seq(
            (wkb, Map[String, Any]("name" -> "first")),
            (wkb, null.asInstanceOf[Map[String, Any]])
        )
        val out = MvtWriter.encode("layer1", 4096, features)
        assert(out != null && out.nonEmpty)
    }

    test("encode should skip individual null field values inside an attrs map") {
        val wkb = JTS.toWKB(gf.createPoint(new Coordinate(0.5, 0.5)))
        val features = Seq(
            (wkb, Map[String, Any]("name" -> "alpha", "extra" -> null))
        )
        val out = MvtWriter.encode("layer1", 4096, features)
        assert(out != null && out.nonEmpty)
    }

    test("encode should return empty when all features have null/empty WKB") {
        // No good geometries → MVT driver writes no .pbf → empty Array[Byte].
        val features = Seq(
            (null.asInstanceOf[Array[Byte]], Map[String, Any]("name" -> "n1")),
            (Array.emptyByteArray, Map[String, Any]("name" -> "n2"))
        )
        val out = MvtWriter.encode("empty-layer", 4096, features)
        assert(out != null)
        assert(out.isEmpty)
    }

}
