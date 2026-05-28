package com.databricks.labs.gbx.vectorx.mvt

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite

/** Direct unit tests for [[MvtPyramidBuilder]] — exercises the helper without a Spark session.
 *
 *  Tests pin: (1) zoom-range guards, (2) per-tile clipping yields the expected tile count for
 *  a feature that straddles a tile boundary, and (3) per-tile output decodes to non-empty MVT
 *  bytes carrying the configured layer name.
 */
class MvtPyramidBuilderTest extends AnyFunSuite {

    private val gf = new GeometryFactory()

    test("guards reject invalid zoom ranges (negative, inverted, above MAX_ZOOM)") {
        val features = Seq((JTS.toWKB(gf.createPoint(new Coordinate(0.0, 0.0))), Map.empty[String, Any]))
        assertThrows[IllegalArgumentException] {
            MvtPyramidBuilder.build(features, minZ = -1, maxZ = 0, "layer", 4096)
        }
        assertThrows[IllegalArgumentException] {
            MvtPyramidBuilder.build(features, minZ = 5, maxZ = 4, "layer", 4096)
        }
        assertThrows[IllegalArgumentException] {
            MvtPyramidBuilder.build(features, minZ = 0, maxZ = 21, "layer", 4096)
        }
    }

    test("a point near the prime meridian yields one z=4 tile with the layer name") {
        // (0.5, 0.5) lon/lat is inside a single z=4 tile.
        val pt = gf.createPoint(new Coordinate(0.5, 0.5))
        val features = Seq((JTS.toWKB(pt), Map[String, Any]("name" -> "p1")))
        val tiles = MvtPyramidBuilder.build(features, minZ = 4, maxZ = 4, "points", 4096)
        assert(tiles.length == 1, s"expected 1 tile, got ${tiles.length}")
        val (z, _, _, bytes) = tiles.head
        assert(z == 4)
        assert(bytes != null && bytes.nonEmpty)
        assert(new String(bytes, "UTF-8").contains("points"))
    }

    test("a polygon spanning two z=2 tiles emits two non-empty MVT rows") {
        // Rectangle from lon=-30 to lon=+30, lat=10 to lat=20. At z=2 the world is split into 4
        // longitudinal tiles each spanning 90 deg; the rect straddles the 0-meridian (tiles x=1
        // and x=2 at the y=1 row). Polygons clip cleanly along tile boundaries (line-on-boundary
        // collapses to a near-zero-area polygon that the MVT driver still encodes).
        val coords = Array(
            new Coordinate(-30.0, 10.0),
            new Coordinate(30.0, 10.0),
            new Coordinate(30.0, 20.0),
            new Coordinate(-30.0, 20.0),
            new Coordinate(-30.0, 10.0)
        )
        val poly = gf.createPolygon(coords)
        val features = Seq((JTS.toWKB(poly), Map[String, Any]("kind" -> "region")))
        val tiles = MvtPyramidBuilder.build(features, minZ = 2, maxZ = 2, "regions", 4096)
        assert(tiles.length == 2, s"expected 2 tiles, got ${tiles.length}")
        tiles.foreach { case (z, _, _, bytes) =>
            assert(z == 2)
            assert(bytes != null && bytes.nonEmpty)
            assert(new String(bytes, "UTF-8").contains("regions"))
        }
    }
}
