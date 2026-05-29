package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType}
import org.scalatest.matchers.should.Matchers._

class Custom_CoverageTest extends PlanTest with SilentSparkSession {

    // ------------------------------------------------------------------
    // Grid: (0,100,0,100,2,10,10,32633) — 10x10 cells at resolution 0
    // ------------------------------------------------------------------
    private val gridConf = GridConf(
        boundXMin     = 0L,
        boundXMax     = 100L,
        boundYMin     = 0L,
        boundYMax     = 100L,
        cellSplits    = 2,
        rootCellSizeX = 10,
        rootCellSizeY = 10,
        crsID         = Some(32633)
    )

    private def buildGridLit(): Literal = {
        val gridExpr = Custom_Grid(
            Literal(0L,    LongType),
            Literal(100L,  LongType),
            Literal(0L,    LongType),
            Literal(100L,  LongType),
            Literal(2,     IntegerType),
            Literal(10,    IntegerType),
            Literal(10,    IntegerType),
            Literal(32633, IntegerType)
        )
        val gridRow = gridExpr.eval(InternalRow.empty).asInstanceOf[InternalRow]
        Literal.create(gridRow, Custom_GridSpec.gridStructType)
    }

    // ------------------------------------------------------------------
    // Custom_Polyfill: POLYGON ((0 0, 30 0, 30 30, 0 30, 0 0))
    //   Centroid-containment at resolution 0: cell centers {5,15,25}x{5,15,25} = 9
    // ------------------------------------------------------------------
    test("Custom_Polyfill should return 9 cells for a 30x30 polygon at resolution 0") {
        val gridLit = buildGridLit()
        val sys     = CustomGridSystem(gridConf)

        val polyWkb = JTS.toWKB(JTS.fromWKT("POLYGON ((0 0, 30 0, 30 30, 0 30, 0 0))"))
        val polyLit = Literal.create(polyWkb, BinaryType)
        val resLit  = Literal(0, IntegerType)

        val result = Custom_Polyfill(polyLit, gridLit, resLit).eval(InternalRow.empty)
        result.asInstanceOf[AnyRef] should not be null

        val arr  = result.asInstanceOf[ArrayData]
        arr.numElements() shouldBe 9

        val cells = arr.toLongArray()
        cells should have length 9

        // Every cell's geometry envelope must lie within [0,30]x[0,30]
        cells.foreach { cell =>
            val env = sys.cellIdToGeometry(cell).getEnvelopeInternal
            env.getMinX should be >= 0.0
            env.getMaxX should be <= 30.0 + 1e-9
            env.getMinY should be >= 0.0
            env.getMaxY should be <= 30.0 + 1e-9
        }
    }

    // ------------------------------------------------------------------
    // Custom_KRing: k=1 around cell (1,1) — all 9 interior cells of a
    //   10x10 grid produce a full 3x3 ring since no edge clamping fires.
    //   centerCell is the cell at grid position (1,1) via pointToCellID(15,15,0).
    // ------------------------------------------------------------------
    test("Custom_KRing k=1 around center cell should return 9 cells and include the center") {
        val gridLit  = buildGridLit()
        val sys      = CustomGridSystem(gridConf)

        val centerCell = sys.pointToCellID(15.0, 15.0, 0)
        val cellLit    = Literal(centerCell)
        val gridLit2   = buildGridLit()
        val kLit       = Literal(1, IntegerType)

        val result = Custom_KRing(cellLit, gridLit2, kLit).eval(InternalRow.empty)
        result.asInstanceOf[AnyRef] should not be null

        val arr   = result.asInstanceOf[ArrayData]
        arr.numElements() shouldBe 9

        val cells = arr.toLongArray()
        cells should have length 9
        cells should contain(centerCell)
    }

}
