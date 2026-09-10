package com.databricks.labs.gbx.gridx

import com.databricks.labs.gbx.gridx.bng.BNG_CellFill
import com.databricks.labs.gbx.gridx.custom.{Custom_CellFill, Custom_GridSpec}
import com.databricks.labs.gbx.gridx.grid.{BNG, CustomGridSystem, GridConf, GridSystem, H3, Quadbin}
import com.databricks.labs.gbx.gridx.h3.H3_CellFill
import com.databricks.labs.gbx.gridx.quadbin.Quadbin_CellFill
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Grid-generic cell-space fill: [[CellFill.fill]] semantics across all four grid systems,
 *  plus name/arity/round-trip checks for the four `gbx_<grid>_cellfill` aggregator wrappers.
 *  Drives the core and `update`/`eval` directly (no Spark session).
 */
class CellFillTest extends AnyFunSuite {

    private val customConf = GridConf(0, 1000000, 0, 1000000, 2, 100000, 100000, Some(27700))
    private val custom     = CustomGridSystem(customConf)

    /** (label, grid, centerCellId) for a well-interior cell in each grid. */
    private val grids: Seq[(String, GridSystem, Long)] = Seq(
        ("H3",      H3,      H3.pointToCellID(-0.1, 51.5, 8)),
        ("QUADBIN", Quadbin, Quadbin.pointToCellID(-0.1, 51.5, 10)),
        ("BNG",     BNG,     BNG.pointToCellID(530000.0, 180000.0, 4)),
        ("CUSTOM",  custom,  custom.pointToCellID(500000.0, 500000.0, 3))
    )

    test("mean k=1 fills a NULL cell with the neighbour mean (all four grids)") {
        grids.foreach { case (label, grid, center) =>
            val ring1 = grid.kLoop(center, 1)
            withClue(s"$label ring1: ") { ring1.length should be >= 2 }
            val cells = (Map(center -> Option.empty[Double]) ++ ring1.map(_ -> Some(5.0))).toMap
            val out   = CellFill.fill(grid, cells, k = 1, method = "mean", power = 2.0).toMap
            withClue(s"$label filled center: ") { out(center) shouldBe Some(5.0) }
            withClue(s"$label neighbour unchanged: ") { ring1.foreach(nb => out(nb) shouldBe Some(5.0)) }
        }
    }

    test("a NULL cell with no valid neighbour stays NULL (all four grids)") {
        grids.foreach { case (label, grid, center) =>
            val cells = Map(center -> Option.empty[Double])
            val out   = CellFill.fill(grid, cells, k = 3, method = "mean", power = 2.0).toMap
            withClue(s"$label: ") { out(center) shouldBe None }
        }
    }

    test("idw k=2 power=2 weights closer rings higher; exact value (all four grids)") {
        grids.foreach { case (label, grid, center) =>
            val nb1 = grid.kLoop(center, 1).head // ring distance 1
            val nb2 = grid.kLoop(center, 2).head // ring distance 2 (hollow, disjoint from ring 1)
            withClue(s"$label rings disjoint: ") { nb1 should not equal nb2 }
            val cells = Map(center -> Option.empty[Double], nb1 -> Some(10.0), nb2 -> Some(20.0))

            // mean k=2 = (10 + 20) / 2 = 15.0
            CellFill.fill(grid, cells, 2, "mean", 2.0).toMap.apply(center) shouldBe Some(15.0)

            // idw k=2, power=2 = (10*1^-2 + 20*2^-2) / (1^-2 + 2^-2) = (10 + 5) / 1.25 = 12.0
            withClue(s"$label idw: ") {
                CellFill.fill(grid, cells, 2, "idw", 2.0).toMap.apply(center) shouldBe Some(12.0)
            }
        }
    }

    test("k=1 idw equals mean (all four grids)") {
        grids.foreach { case (label, grid, center) =>
            val ring1 = grid.kLoop(center, 1)
            ring1.length should be >= 2
            val cells = Map(center -> Option.empty[Double], ring1(0) -> Some(10.0), ring1(1) -> Some(20.0))
            val meanC = CellFill.fill(grid, cells, 1, "mean", 2.0).toMap.apply(center)
            val idwC  = CellFill.fill(grid, cells, 1, "idw", 2.0).toMap.apply(center)
            withClue(s"$label: ") {
                meanC shouldBe Some(15.0)
                idwC shouldBe meanC
            }
        }
    }

    test("valid (non-NULL) cells pass through unchanged (all four grids)") {
        grids.foreach { case (label, grid, center) =>
            val ring1 = grid.kLoop(center, 1)
            val cells = (Map(center -> Some(7.0)) ++ ring1.map(_ -> Some(5.0))).toMap
            withClue(s"$label mean: ") {
                CellFill.fill(grid, cells, 2, "mean", 2.0).toMap.apply(center) shouldBe Some(7.0)
            }
            withClue(s"$label idw: ") {
                CellFill.fill(grid, cells, 2, "idw", 2.0).toMap.apply(center) shouldBe Some(7.0)
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Aggregator wrappers: canonical name + builder arity.
    // -------------------------------------------------------------------------------------------

    test("canonical names") {
        H3_CellFill.name shouldBe "gbx_h3_cellfill"
        Quadbin_CellFill.name shouldBe "gbx_quadbin_cellfill"
        BNG_CellFill.name shouldBe "gbx_bng_cellfill"
        Custom_CellFill.name shouldBe "gbx_custom_cellfill"
    }

    test("h3/quadbin/bng builder accepts arities 2..5 (cellid, value, [k], [method], [power])") {
        def args(n: Int): Seq[Expression] = (0 until n).map(_ => Literal(1L))
        for (n <- 2 to 5) {
            H3_CellFill.builder()(args(n)) shouldBe a[H3_CellFill]
            Quadbin_CellFill.builder()(args(n)) shouldBe a[Quadbin_CellFill]
            BNG_CellFill.builder()(args(n)) shouldBe a[BNG_CellFill]
        }
        an[IllegalArgumentException] should be thrownBy H3_CellFill.builder()(args(1))
        an[IllegalArgumentException] should be thrownBy H3_CellFill.builder()(args(6))
    }

    test("custom builder accepts arities 3..6 (cellid, value, grid, [k], [method], [power])") {
        def args(n: Int): Seq[Expression] = (0 until n).map(_ => Literal(1L))
        for (n <- 3 to 6) {
            Custom_CellFill.builder()(args(n)) shouldBe a[Custom_CellFill]
        }
        an[IllegalArgumentException] should be thrownBy Custom_CellFill.builder()(args(2))
        an[IllegalArgumentException] should be thrownBy Custom_CellFill.builder()(args(7))
    }

    // -------------------------------------------------------------------------------------------
    // Aggregator wrappers: update/eval round-trip through the shared core.
    // -------------------------------------------------------------------------------------------

    /** Decode `array<struct<cellid:LONG, value:DOUBLE?>>` into a cellId -> Option[Double] map. */
    private def decodeLong(res: Any): Map[Long, Option[Double]] = {
        val arr = res.asInstanceOf[ArrayData]
        (0 until arr.numElements()).map { i =>
            val row = arr.getStruct(i, 2)
            row.getLong(0) -> (if (row.isNullAt(1)) None else Some(row.getDouble(1)))
        }.toMap
    }

    test("H3_CellFill update/eval round-trip fills the NULL center with the neighbour mean") {
        val center = H3.pointToCellID(-0.1, 51.5, 8)
        val ring1  = H3.kLoop(center, 1)
        val agg = H3_CellFill(
            Literal.create(null, LongType), Literal.create(null, DoubleType),
            Literal(1), Literal("mean"), Literal(2.0))
        val buf = agg.createAggregationBuffer()
        agg.update(buf, center, None)
        ring1.foreach(nb => agg.update(buf, nb, Some(5.0)))
        val out = decodeLong(agg.eval(buf))
        out.size shouldBe (1 + ring1.length)
        out(center) shouldBe Some(5.0)
    }

    test("BNG_CellFill eval renders cell ids as BNG strings") {
        val center = BNG.pointToCellID(530000.0, 180000.0, 4)
        val ring1  = BNG.kLoop(center, 1)
        val agg = BNG_CellFill(
            Literal.create(null, StringType), Literal.create(null, DoubleType),
            Literal(1), Literal("mean"), Literal(2.0))
        val buf = agg.createAggregationBuffer()
        agg.update(buf, center, None)
        ring1.foreach(nb => agg.update(buf, nb, Some(5.0)))

        val arr = agg.eval(buf).asInstanceOf[ArrayData]
        arr.numElements() shouldBe (1 + ring1.length)
        val decoded = (0 until arr.numElements()).map { i =>
            val row = arr.getStruct(i, 2)
            row.getUTF8String(0).toString -> (if (row.isNullAt(1)) None else Some(row.getDouble(1)))
        }.toMap
        decoded(BNG.format(center)) shouldBe Some(5.0)
    }

    test("Custom_CellFill eval fills through the reconstructed grid struct") {
        val center = custom.pointToCellID(500000.0, 500000.0, 3)
        val ring1  = custom.kLoop(center, 1)
        val gridExpr = com.databricks.labs.gbx.gridx.custom.Custom_Grid(
            Literal(0L), Literal(1000000L), Literal(0L), Literal(1000000L),
            Literal(2), Literal(100000), Literal(100000), Literal(27700))
        val agg = Custom_CellFill(
            Literal.create(null, LongType), Literal.create(null, DoubleType), gridExpr,
            Literal(1), Literal("mean"), Literal(2.0))
        val buf = agg.createAggregationBuffer()
        agg.update(buf, center, None)
        ring1.foreach(nb => agg.update(buf, nb, Some(5.0)))
        val out = decodeLong(agg.eval(buf))
        out(center) shouldBe Some(5.0)
    }

    test("empty buffer evaluates to null") {
        val agg = H3_CellFill(
            Literal.create(null, LongType), Literal.create(null, DoubleType),
            Literal(1), Literal("mean"), Literal(2.0))
        agg.eval(agg.createAggregationBuffer()).asInstanceOf[AnyRef] shouldBe null
    }
}
