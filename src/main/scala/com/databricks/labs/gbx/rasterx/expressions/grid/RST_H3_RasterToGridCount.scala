package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.{ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.collection.mutable.ArrayBuffer

/** Returns the number of pixels aggregated into the grid cell.
  *
  * The measure is a Double (not Int): centroid count is `n.toDouble`; covering count is the sum of
  * area-fraction weights (`Σw`), a mass-conserving fractional count. Covered-but-empty cells added
  * by `complete` coverage report `0.0`.
  */
case class RST_H3_RasterToGridCount(
    tile: Expression,
    resolution: Expression,
    coverage: Expression,
    assignment: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, resolution, coverage, assignment, ExpressionConfigExpr())
    override def dataType: DataType =
        ArrayType(ArrayType(StructType(Seq(
            StructField("cellID", LongType),
            StructField("measure", DoubleType, nullable = true)))))
    override def nullable: Boolean = true
    override def prettyName: String = RST_H3_RasterToGridCount.name
    override def replacement: Expression = invoke(RST_H3_RasterToGridCount)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_H3_RasterToGridCount extends WithExpressionInfo {

    /** Centroid reducer: pixel count as a Double. */
    val fAgg: ArrayBuffer[Double] => Double = values => values.length.toDouble
    /** Covering reducer: sum of area-fraction weights (fractional, mass-conserving count). */
    val fAggW: ArrayBuffer[(Double, Double)] => Double = pairs => pairs.map(_._2).sum

    def eval(row: InternalRow, resolution: Int, coverage: UTF8String, assignment: UTF8String,
             conf: UTF8String): ArrayData =
        eval(row, resolution, coverage, assignment, conf, BinaryType)

    def eval(row: InternalRow, resolution: Int, coverage: UTF8String, assignment: UTF8String,
             conf: UTF8String, rdt: DataType): ArrayData =
        Option(RST_ErrorHandler.safeEval(() =>
            RST_H3_RasterToGrid.eval[Double](
                row, resolution, coverage.toString, assignment.toString, conf, rdt, this.execute),
            row, rdt, conf))
            .map(_.asInstanceOf[ArrayData])
            .orNull

    /** Canonical Stage-2 path: Double measure, `Some(0.0)` for covered-but-empty cells. */
    def execute(ds: Dataset, resolution: Int, coverage: String, assignment: String): Array[Array[(Long, Option[Double])]] =
        RST_H3_RasterToGrid.execute[Double](ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue = Some(0.0))

    /** Legacy sparse+centroid shim returning an Int count (pre-Stage-2 return type) for existing
      * direct-execute callers (integration/bench tests). Emits only has-data cells. */
    def execute(ds: Dataset, resolution: Int): Array[Array[(Long, Int)]] =
        RST_H3_RasterToGrid.execute[Int](ds, resolution, "sparse", "centroid",
            values => values.length, _ => 0, emptyValue = None)
            .map(_.collect { case (c, Some(v)) => (c, v) })

    override def name: String = "gbx_rst_h3_rastertogridcount"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => new RST_H3_RasterToGridCount(c(0), c(1), Literal("complete"), Literal("centroid"))
        case 3 => new RST_H3_RasterToGridCount(c(0), c(1), c(2), Literal("centroid"))
        case 4 => new RST_H3_RasterToGridCount(c(0), c(1), c(2), c(3))
        case n => throw new IllegalArgumentException(
            s"$name expects 2-4 args (tile, resolution, [coverage], [assignment]); got $n")
    }

}
