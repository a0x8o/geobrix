package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.scalatest.matchers.should.Matchers._

/** Tests for Custom_Distance (gbx_custom_distance) expression.
  * gbx_custom_distance returns the grid distance between two custom cells.
  */
class Custom_DistanceTest extends PlanTest with SilentSparkSession {

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
    private val sys = CustomGridSystem(gridConf)

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

    test("gbx_custom_distance: distance of a cell to itself is 0") {
        val gridLit = buildGridLit()
        val cell    = sys.pointToCellID(15.0, 15.0, 0)

        val result = Custom_Distance(Literal(cell), gridLit, Literal(cell)).eval(InternalRow.empty)
        assert(result != null)
        result.asInstanceOf[java.lang.Long].longValue shouldBe 0L
    }

    test("gbx_custom_distance: adjacent cells have distance 1") {
        val gridLit = buildGridLit()
        // cell at (15,15,0) pos=(1,1); adjacent at (25,15,0) pos=(2,1) -> dx=1,dy=0 -> Manhattan=1
        val cellA = sys.pointToCellID(15.0, 15.0, 0)
        val cellB = sys.pointToCellID(25.0, 15.0, 0)

        val result = Custom_Distance(Literal(cellA), gridLit, Literal(cellB)).eval(InternalRow.empty)
        assert(result != null)
        result.asInstanceOf[java.lang.Long].longValue shouldBe 1L
    }

    test("gbx_custom_distance: distance is symmetric") {
        val gridLit  = buildGridLit()
        val gridLit2 = buildGridLit()
        val cellA = sys.pointToCellID(15.0, 15.0, 0)
        val cellB = sys.pointToCellID(45.0, 35.0, 0)  // dx=3, dy=2 -> Manhattan=5

        val dAB = Custom_Distance(Literal(cellA), gridLit,  Literal(cellB)).eval(InternalRow.empty).asInstanceOf[java.lang.Long].longValue
        val dBA = Custom_Distance(Literal(cellB), gridLit2, Literal(cellA)).eval(InternalRow.empty).asInstanceOf[java.lang.Long].longValue
        dAB shouldBe dBA
    }

    test("gbx_custom_distance: registers and runs as SQL function via functions.register") {
        functions.register(spark)
        val cell = sys.pointToCellID(15.0, 15.0, 0)
        val row  = spark.sql(
            s"""SELECT gbx_custom_distance(
               |  ${cell}L,
               |  gbx_custom_grid(0L, 100L, 0L, 100L, 2, 10, 10, 32633),
               |  ${cell}L
               |)""".stripMargin
        ).head()
        row.getLong(0) shouldBe 0L
    }

}
