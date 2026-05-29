package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Coordinate, Point}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Unit tests for [[ST_InterpolateElevationGeom]] -- expression-level (no Spark session required).
 *
 *  Tilted plane: z = 2x + 3y + 5, sampled at the 4 corners of a 100x100 extent.
 *  Origin = POINT(0 0) with SRID 32633, grid_cols=10, grid_rows=10, cell_size_x=10.0, cell_size_y=10.0.
 *  Centers: x = 0 + (i+0.5)*10, y = 0 + (j+0.5)*10 => 5,15,...,95 on each axis == pointGridBBox(0,0,100,100,10,10).
 */
class ST_InterpolateElevationGeomTest extends AnyFunSuite {

    /** z = 2x + 3y + 5 */
    private def planeZ(x: Double, y: Double): Double = 2.0 * x + 3.0 * y + 5.0

    /** Build a Literal wrapping an ARRAY<BINARY> of WKB-encoded geometries. */
    private def geomArrayLit(wkbs: Array[Byte]*): Literal = {
        val data = new GenericArrayData(wkbs.toArray.asInstanceOf[Array[Any]])
        Literal.create(data, ArrayType(BinaryType, containsNull = false))
    }

    /** Empty ARRAY<BINARY> literal. */
    private def emptyArrayLit: Literal =
        Literal.create(new GenericArrayData(Array.empty[Any]), ArrayType(BinaryType, containsNull = false))

    /** 4 corners of a 100x100 square with Z from the tilted plane. */
    private def cornerPoints: Seq[Array[Byte]] = {
        val corners = Seq((0.0, 0.0), (100.0, 0.0), (0.0, 100.0), (100.0, 100.0))
        corners.map { case (x, y) =>
            JTS.toWKB3(JTS.point(new Coordinate(x, y, planeZ(x, y))))
        }
    }

    /** Build the grid-origin POINT(0 0) with SRID 32633 as a BINARY literal. */
    private def originLit: Literal = {
        val originPt = JTS.point(new Coordinate(0.0, 0.0))
        originPt.setSRID(32633)
        Literal.create(JTS.toWKB3(originPt), BinaryType)
    }

    /** Invoke the geom generator and collect all emitted rows. */
    private def evalGeomExpr(expr: ST_InterpolateElevationGeom): Seq[InternalRow] =
        expr.eval(InternalRow.empty).iterator.toSeq

    /** Invoke the bbox generator and collect all emitted rows. */
    private def evalBBoxExpr(expr: ST_InterpolateElevationBBox): Seq[InternalRow] =
        expr.eval(InternalRow.empty).iterator.toSeq

    // -----------------------------------------------------------------------
    // Test 1: 10x10 grid over tilted plane => exactly 100 points with correct Z
    // -----------------------------------------------------------------------
    test("st_interpolateelevationgeom emits 100 points with correct Z for tilted plane") {
        val pts = cornerPoints
        val expr = ST_InterpolateElevationGeom(
            geomArrayLit(pts: _*),
            emptyArrayLit,
            Literal(0.0),                                                        // merge_tolerance
            Literal(0.01),                                                       // snap_tolerance
            Literal.create(UTF8String.fromString("NONENCROACHING"), StringType), // split_point_finder
            originLit,                                                           // grid_origin BINARY
            Literal(10),                                                         // grid_cols  (Int)
            Literal(10),                                                         // grid_rows  (Int)
            Literal(10.0),                                                       // cell_size_x
            Literal(10.0)                                                        // cell_size_y
        )

        val rows = evalGeomExpr(expr)
        rows.length shouldBe 100

        rows.foreach { row =>
            val wkb = row.getBinary(0)
            wkb should not be null
            val geom = JTS.fromWKB(wkb)
            geom shouldBe a[Point]
            val pt = geom.asInstanceOf[Point]
            val expectedZ = planeZ(pt.getX, pt.getY)
            pt.getCoordinate.getZ should be(expectedZ +- 1e-6)
        }
    }

    // -----------------------------------------------------------------------
    // Test 2: geom and bbox generators produce identical (x, y, z) triples
    // -----------------------------------------------------------------------
    test("st_interpolateelevationgeom matches st_interpolateelevationbbox over equivalent grid") {
        val pts = cornerPoints

        val geomExpr = ST_InterpolateElevationGeom(
            geomArrayLit(pts: _*),
            emptyArrayLit,
            Literal(0.0),
            Literal(0.01),
            Literal.create(UTF8String.fromString("NONENCROACHING"), StringType),
            originLit,
            Literal(10),
            Literal(10),
            Literal(10.0),
            Literal(10.0)
        )

        val bboxExpr = ST_InterpolateElevationBBox(
            geomArrayLit(pts: _*),
            emptyArrayLit,
            Literal(0.0),
            Literal(0.01),
            Literal.create(UTF8String.fromString("NONENCROACHING"), StringType),
            Literal(0.0),   // xmin
            Literal(0.0),   // ymin
            Literal(100.0), // xmax
            Literal(100.0), // ymax
            Literal(10),    // width_px
            Literal(10),    // height_px
            Literal(32633)  // srid
        )

        def toTriples(rows: Seq[InternalRow]): Seq[(Double, Double, Double)] =
            rows.map { row =>
                val pt = JTS.fromWKB(row.getBinary(0)).asInstanceOf[Point]
                (pt.getX, pt.getY, pt.getCoordinate.getZ)
            }.sortBy(t => (t._1, t._2))

        val geomTriples = toTriples(evalGeomExpr(geomExpr))
        val bboxTriples = toTriples(evalBBoxExpr(bboxExpr))

        geomTriples.length shouldBe bboxTriples.length
        geomTriples.zip(bboxTriples).foreach { case ((gx, gy, gz), (bx, by, bz)) =>
            gx should be(bx +- 1e-6)
            gy should be(by +- 1e-6)
            gz should be(bz +- 1e-6)
        }
    }

    // -----------------------------------------------------------------------
    // Test 3: builder arity guard -- 10 args ok; wrong count throws
    // -----------------------------------------------------------------------
    test("ST_InterpolateElevationGeom.builder rejects wrong number of arguments") {
        val lit = Literal(0.0)
        an[IllegalArgumentException] should be thrownBy {
            ST_InterpolateElevationGeom.builder()(Seq(lit, lit, lit))
        }
    }

    test("ST_InterpolateElevationGeom.builder accepts exactly 10 arguments") {
        val pts = cornerPoints
        noException should be thrownBy {
            ST_InterpolateElevationGeom.builder()(Seq(
                geomArrayLit(pts: _*),
                emptyArrayLit,
                Literal(0.0),
                Literal(0.01),
                Literal.create(UTF8String.fromString("NONENCROACHING"), StringType),
                originLit,
                Literal(10),
                Literal(10),
                Literal(10.0),
                Literal(10.0)
            ))
        }
    }
}
