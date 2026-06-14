package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Coordinate, Polygon}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Unit tests for [[ST_Triangulate]] -- expression-level (no Spark session required).
 *
 *  Array inputs are encoded as [[GenericArrayData]] of WKB byte arrays (BinaryType elements),
 *  which mirrors how Catalyst delivers ARRAY<BINARY> columns to expression eval.
 */
class ST_TriangulateTest extends AnyFunSuite {

    /** Build a Literal wrapping an ARRAY<BINARY> of WKB-encoded geometries. */
    private def geomArrayLit(wkbs: Array[Byte]*): Literal = {
        val data = new GenericArrayData(wkbs.toArray.asInstanceOf[Array[Any]])
        Literal.create(data, ArrayType(BinaryType, containsNull = false))
    }

    /** Empty ARRAY<BINARY> literal. */
    private def emptyArrayLit: Literal =
        Literal.create(new GenericArrayData(Array.empty[Any]), ArrayType(BinaryType, containsNull = false))

    /** A STRING-typed mode literal. */
    private def modeLit(m: String): Literal =
        Literal.create(UTF8String.fromString(m), StringType)

    private val finderLit: Literal =
        Literal.create(UTF8String.fromString("NONENCROACHING"), StringType)

    /** Invoke the generator and collect all emitted rows. */
    private def evalTriangulate(expr: ST_Triangulate): Seq[InternalRow] =
        expr.eval(InternalRow.empty).iterator.toSeq

    /** True iff some emitted triangle has (ia, ib) as one of its edges. */
    private def hasEdge(rows: Seq[InternalRow], a: Coordinate, b: Coordinate): Boolean = {
        def sameXY(c: Coordinate, d: Coordinate): Boolean =
            math.abs(c.x - d.x) < 1e-6 && math.abs(c.y - d.y) < 1e-6
        rows.exists { row =>
            val coords = JTS.fromWKB(row.getBinary(0)).getCoordinates
            // ring of 4 (closing repeat); 3 edges
            val edges = Seq((coords(0), coords(1)), (coords(1), coords(2)), (coords(2), coords(0)))
            edges.exists { case (p, q) =>
                (sameXY(p, a) && sameXY(q, b)) || (sameXY(p, b) && sameXY(q, a))
            }
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: 4-corner square => exactly 2 Delaunay triangles
    // -----------------------------------------------------------------------
    test("st_triangulate emits exactly 2 triangles for a unit square (4 non-collinear points)") {
        // 4 corners of a 10x10 square with Z=0 -- non-collinear => exactly 2 Delaunay triangles
        val p00 = JTS.toWKB3(JTS.point(new Coordinate(0.0,  0.0,  0.0)))
        val p10 = JTS.toWKB3(JTS.point(new Coordinate(10.0, 0.0,  0.0)))
        val p01 = JTS.toWKB3(JTS.point(new Coordinate(0.0,  10.0, 0.0)))
        val p11 = JTS.toWKB3(JTS.point(new Coordinate(10.0, 10.0, 0.0)))

        val expr = ST_Triangulate(
            geomArrayLit(p00, p10, p01, p11),
            emptyArrayLit,
            Literal(0.01),
            Literal(0.01),
            finderLit,
            modeLit("constrained")
        )

        val rows = evalTriangulate(expr)
        rows.length shouldBe 2

        rows.foreach { row =>
            val wkb = row.getBinary(0)
            wkb should not be null
            wkb should not be empty
            val geom = JTS.fromWKB(wkb)
            geom shouldBe a[Polygon]
            val poly = geom.asInstanceOf[Polygon]
            poly.isValid shouldBe true
            // A triangle ring has 4 coordinates (3 distinct + closing repeat)
            poly.getExteriorRing.getCoordinates.length shouldBe 4
        }
    }

    // -----------------------------------------------------------------------
    // Test 2: 5 points + 1 breakline => > 0 triangles, no exception
    // -----------------------------------------------------------------------
    test("st_triangulate emits at least one triangle for 5 points with a breakline") {
        val p00 = JTS.toWKB3(JTS.point(new Coordinate(0.0,  0.0,  0.0)))
        val p10 = JTS.toWKB3(JTS.point(new Coordinate(10.0, 0.0,  0.0)))
        val p01 = JTS.toWKB3(JTS.point(new Coordinate(0.0,  10.0, 0.0)))
        val p11 = JTS.toWKB3(JTS.point(new Coordinate(10.0, 10.0, 0.0)))
        val p55 = JTS.toWKB3(JTS.point(new Coordinate(5.0,  5.0,  1.0)))

        val breakline = JTS.toWKB(JTS.fromWKT("LINESTRING (0 5, 10 5)"))

        val expr = ST_Triangulate(
            geomArrayLit(p00, p10, p01, p11, p55),
            geomArrayLit(breakline),
            Literal(0.01),
            Literal(0.01),
            finderLit,
            modeLit("constrained")
        )

        val rows = evalTriangulate(expr)
        rows.length should be > 0
        rows.foreach { row =>
            val wkb = row.getBinary(0)
            wkb should not be null
            JTS.fromWKB(wkb) shouldBe a[Polygon]
        }
    }

    // -----------------------------------------------------------------------
    // Test 3: builder rejects wrong arity
    // -----------------------------------------------------------------------
    test("ST_Triangulate.builder rejects wrong number of arguments") {
        val lit = Literal(0.0)
        an[IllegalArgumentException] should be thrownBy {
            ST_Triangulate.builder()(Seq(lit, lit, lit))
        }
    }

    // -----------------------------------------------------------------------
    // Test 4: conforming mode also produces a valid non-empty cover
    // -----------------------------------------------------------------------
    test("st_triangulate mode=conforming emits a valid non-empty triangle cover") {
        val p00 = JTS.toWKB3(JTS.point(new Coordinate(0.0,  0.0,  0.0)))
        val p10 = JTS.toWKB3(JTS.point(new Coordinate(10.0, 0.0,  0.0)))
        val p01 = JTS.toWKB3(JTS.point(new Coordinate(0.0,  10.0, 0.0)))
        val p11 = JTS.toWKB3(JTS.point(new Coordinate(10.0, 10.0, 0.0)))

        val expr = ST_Triangulate(
            geomArrayLit(p00, p10, p01, p11),
            emptyArrayLit,
            Literal(0.01),
            Literal(0.01),
            finderLit,
            modeLit("conforming")
        )

        val rows = evalTriangulate(expr)
        rows.length should be > 0
        rows.foreach { row =>
            val poly = JTS.fromWKB(row.getBinary(0)).asInstanceOf[Polygon]
            poly.isValid shouldBe true
        }
    }

    // -----------------------------------------------------------------------
    // Test 5: constrained recovery -- a breakline whose endpoints are NOT
    // mass points becomes a triangle edge (the C1 no-Steiner behavior).
    // -----------------------------------------------------------------------
    test("st_triangulate mode=constrained forces a breakline (non-mass-point) as a triangle edge") {
        // 4 corner mass points of a 10x10 square; the breakline endpoints (0,5)-(10,5)
        // are NOT among them. Under constrained recovery they are added as sites and the
        // segment is forced as an edge.
        val p00 = JTS.toWKB3(JTS.point(new Coordinate(0.0,  0.0,  0.0)))
        val p10 = JTS.toWKB3(JTS.point(new Coordinate(10.0, 0.0,  0.0)))
        val p01 = JTS.toWKB3(JTS.point(new Coordinate(0.0,  10.0, 0.0)))
        val p11 = JTS.toWKB3(JTS.point(new Coordinate(10.0, 10.0, 0.0)))

        val a = new Coordinate(0.0, 5.0)
        val b = new Coordinate(10.0, 5.0)
        val breakline = JTS.toWKB(JTS.fromWKT("LINESTRING (0 5, 10 5)"))

        val expr = ST_Triangulate(
            geomArrayLit(p00, p10, p01, p11),
            geomArrayLit(breakline),
            Literal(0.0),
            Literal(0.0),
            finderLit,
            modeLit("constrained")
        )

        val rows = evalTriangulate(expr)
        rows.length should be > 0
        hasEdge(rows, a, b) shouldBe true
    }

    // -----------------------------------------------------------------------
    // Test 6: unknown mode throws
    // -----------------------------------------------------------------------
    test("st_triangulate rejects an unknown mode") {
        val p00 = JTS.toWKB3(JTS.point(new Coordinate(0.0,  0.0,  0.0)))
        val p10 = JTS.toWKB3(JTS.point(new Coordinate(10.0, 0.0,  0.0)))
        val p01 = JTS.toWKB3(JTS.point(new Coordinate(0.0,  10.0, 0.0)))
        val p11 = JTS.toWKB3(JTS.point(new Coordinate(10.0, 10.0, 0.0)))

        val expr = ST_Triangulate(
            geomArrayLit(p00, p10, p01, p11),
            emptyArrayLit,
            Literal(0.01),
            Literal(0.01),
            finderLit,
            modeLit("bogus")
        )
        an[IllegalArgumentException] should be thrownBy evalTriangulate(expr)
    }

    // -----------------------------------------------------------------------
    // Test 7: 5-arg builder call defaults to constrained
    // -----------------------------------------------------------------------
    test("ST_Triangulate.builder defaults the 5-arg call to constrained mode") {
        val p00 = JTS.toWKB3(JTS.point(new Coordinate(0.0,  0.0,  0.0)))
        val p10 = JTS.toWKB3(JTS.point(new Coordinate(10.0, 0.0,  0.0)))
        val p01 = JTS.toWKB3(JTS.point(new Coordinate(0.0,  10.0, 0.0)))
        val p11 = JTS.toWKB3(JTS.point(new Coordinate(10.0, 10.0, 0.0)))

        val built = ST_Triangulate.builder()(Seq(
            geomArrayLit(p00, p10, p01, p11),
            emptyArrayLit,
            Literal(0.01),
            Literal(0.01),
            finderLit
        )).asInstanceOf[ST_Triangulate]

        built.modeExpr shouldBe Literal("constrained")

        val rows = evalTriangulate(built)
        rows.length shouldBe 2
    }

    // -----------------------------------------------------------------------
    // Test 8: 6-arg builder call passes mode through
    // -----------------------------------------------------------------------
    test("ST_Triangulate.builder accepts a 6th mode argument") {
        val lit = Literal(0.0)
        noException should be thrownBy {
            ST_Triangulate.builder()(Seq(
                emptyArrayLit, emptyArrayLit, lit, lit, finderLit, modeLit("conforming")))
        }
    }
}
