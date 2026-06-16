package com.databricks.labs.gbx.gridx.custom

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.scalatest.matchers.should.Matchers._

class Custom_GridTest extends PlanTest with SilentSparkSession {

    // ------------------------------------------------------------------
    // Helper: build and eval a Custom_Grid expression
    // ------------------------------------------------------------------
    private def evalGrid(
        xMin: Long, xMax: Long, yMin: Long, yMax: Long,
        splits: Int, rootX: Int, rootY: Int, srid: Int
    ): InternalRow = {
        val expr = Custom_Grid(
            Literal(xMin,   LongType),
            Literal(xMax,   LongType),
            Literal(yMin,   LongType),
            Literal(yMax,   LongType),
            Literal(splits, IntegerType),
            Literal(rootX,  IntegerType),
            Literal(rootY,  IntegerType),
            Literal(srid,   IntegerType)
        )
        expr.eval(InternalRow.empty).asInstanceOf[InternalRow]
    }

    // ------------------------------------------------------------------
    // Happy-path: round-trip through all 8 fields
    // ------------------------------------------------------------------
    test("Custom_Grid should produce a correct 8-field grid-spec struct") {
        val result = evalGrid(0L, 100L, 0L, 100L, 2, 10, 10, 32633)

        result.getLong(0)    shouldBe 0L
        result.getLong(1)    shouldBe 100L
        result.getLong(2)    shouldBe 0L
        result.getLong(3)    shouldBe 100L
        result.getInt(4)     shouldBe 2
        result.getInt(5)     shouldBe 10
        result.getInt(6)     shouldBe 10
        result.getInt(7)     shouldBe 32633
    }

    // ------------------------------------------------------------------
    // systemFromRow: reconstruct CustomGridSystem and verify maxResolution
    // ------------------------------------------------------------------
    test("Custom_GridSpec.systemFromRow should produce a valid CustomGridSystem") {
        val row = evalGrid(0L, 100L, 0L, 100L, 2, 10, 10, 32633)
        val system = Custom_GridSpec.systemFromRow(row)

        system.conf.maxResolution should be > 0
        system.conf.crsID         shouldBe Some(32633)
    }

    // ------------------------------------------------------------------
    // 7-arg builder: srid defaults to -1 -> crsID == None
    // ------------------------------------------------------------------
    test("Custom_Grid companion builder should accept 7 args (srid defaults to -1)") {
        val children = Seq(
            Literal(0L,  LongType),
            Literal(100L, LongType),
            Literal(0L,  LongType),
            Literal(100L, LongType),
            Literal(2,   IntegerType),
            Literal(10,  IntegerType),
            Literal(10,  IntegerType)
        )
        val expr   = Custom_Grid.builder()(children)
        val result = expr.eval(InternalRow.empty).asInstanceOf[InternalRow]

        result.getInt(7) shouldBe -1   // defaulted srid

        val system = Custom_GridSpec.systemFromRow(result)
        system.conf.crsID shouldBe None
    }

    // ------------------------------------------------------------------
    // 8-arg builder
    // ------------------------------------------------------------------
    test("Custom_Grid companion builder should accept 8 args") {
        val children = Seq(
            Literal(0L,  LongType),
            Literal(100L, LongType),
            Literal(0L,  LongType),
            Literal(100L, LongType),
            Literal(2,   IntegerType),
            Literal(10,  IntegerType),
            Literal(10,  IntegerType),
            Literal(4326, IntegerType)
        )
        val expr   = Custom_Grid.builder()(children)
        val result = expr.eval(InternalRow.empty).asInstanceOf[InternalRow]
        result.getInt(7) shouldBe 4326
    }

    // ------------------------------------------------------------------
    // Wrong arity -> IllegalArgumentException
    // ------------------------------------------------------------------
    test("Custom_Grid companion builder should reject wrong arity") {
        an[IllegalArgumentException] should be thrownBy {
            Custom_Grid.builder()(Seq(Literal(0L, LongType), Literal(1L, LongType)))
        }
    }

    // ------------------------------------------------------------------
    // Validation: xmax <= xmin
    // ------------------------------------------------------------------
    test("Custom_Grid should throw when xmax <= xmin") {
        an[IllegalArgumentException] should be thrownBy {
            evalGrid(100L, 0L, 0L, 100L, 2, 10, 10, -1)
        }
    }

    // ------------------------------------------------------------------
    // Validation: ymax <= ymin
    // ------------------------------------------------------------------
    test("Custom_Grid should throw when ymax <= ymin") {
        an[IllegalArgumentException] should be thrownBy {
            evalGrid(0L, 100L, 100L, 0L, 2, 10, 10, -1)
        }
    }

    // ------------------------------------------------------------------
    // Validation: cell_splits < 2
    // ------------------------------------------------------------------
    test("Custom_Grid should throw when cell_splits < 2") {
        an[IllegalArgumentException] should be thrownBy {
            evalGrid(0L, 100L, 0L, 100L, 1, 10, 10, -1)
        }
    }

    // ------------------------------------------------------------------
    // Validation: root_cell_size_x <= 0
    // ------------------------------------------------------------------
    test("Custom_Grid should throw when root_cell_size_x <= 0") {
        an[IllegalArgumentException] should be thrownBy {
            evalGrid(0L, 100L, 0L, 100L, 2, 0, 10, -1)
        }
    }

    // ------------------------------------------------------------------
    // Validation: root_cell_size_y <= 0
    // ------------------------------------------------------------------
    test("Custom_Grid should throw when root_cell_size_y <= 0") {
        an[IllegalArgumentException] should be thrownBy {
            evalGrid(0L, 100L, 0L, 100L, 2, 10, 0, -1)
        }
    }

}
