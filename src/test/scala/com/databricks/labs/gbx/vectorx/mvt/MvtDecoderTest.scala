package com.databricks.labs.gbx.vectorx.mvt

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite

/**
  * Unit tests for [[MvtDecoder]].
  *
  * Encodes real MVT blobs via [[MvtWriter]] (the write side) and verifies that
  * `MvtDecoder.decode` recovers the expected layer name, geometry, and attributes.
  * The round-trip exercises the same OGR MVT driver in both directions.
  */
class MvtDecoderTest extends AnyFunSuite {

    private val gf = new GeometryFactory()

    /** Encode a tile-local polygon as a real MVT blob via MvtWriter. */
    private def encodePolygon(layerName: String, id: Int, x0: Int, y0: Int): Array[Byte] = {
        val ring = gf.createLinearRing(Array(
            new Coordinate(x0, y0),
            new Coordinate(x0 + 100, y0),
            new Coordinate(x0 + 100, y0 + 100),
            new Coordinate(x0, y0 + 100),
            new Coordinate(x0, y0)
        ))
        val poly = gf.createPolygon(ring)
        val wkb = JTS.toWKB(poly)
        MvtWriter.encode(layerName, 4096, Seq((wkb, Map[String, Any]("id" -> id))))
    }

    test("MvtDecoder round-trips a real polygon MVT blob") {
        val blob = encodePolygon("bldg", 42, 100, 100)
        assert(blob.nonEmpty, "MvtWriter produced empty blob")
        val features = MvtDecoder.decode(blob)
        assert(features.nonEmpty, "MvtDecoder returned no features")
        val (layerName, geomWkb, attrs) = features.head
        assert(layerName == "bldg", s"expected layer 'bldg'; got '$layerName'")
        assert(
            attrs.get("id").contains(42) || attrs.get("id").exists(_.toString == "42"),
            s"expected id=42; got attrs=$attrs"
        )
        assert(geomWkb != null && geomWkb.nonEmpty, "geomWkb is empty")
    }

    test("MvtDecoder returns empty Seq for empty byte array") {
        assert(MvtDecoder.decode(Array.emptyByteArray).isEmpty)
    }

    test("MvtDecoder returns empty Seq for null input") {
        assert(MvtDecoder.decode(null).isEmpty)
    }

    test("MvtDecoder decodes geometry as non-empty WKB parseable by JTS") {
        val blob = encodePolygon("roads", 7, 200, 200)
        val features = MvtDecoder.decode(blob)
        assert(features.nonEmpty)
        val (_, wkb, _) = features.head
        val geom = JTS.fromWKB(wkb)
        assert(geom != null && !geom.isEmpty, "decoded WKB did not parse to a valid geometry")
    }
}
