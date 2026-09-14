package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.{ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.collection.mutable.ArrayBuffer

/** Returns the median raster value in each BNG grid cell. */
case class RST_BNG_RasterToGridMedian(
    tile: Expression,
    resolution: Expression,
    coverage: Expression,
    assignment: Expression
) extends InvokedExpression {
    override def children: Seq[Expression] = Seq(tile, resolution, coverage, assignment, ExpressionConfigExpr())
    override def dataType: DataType =
        ArrayType(ArrayType(StructType(Seq(
            StructField("cellID", StringType),
            StructField("measure", DoubleType, nullable = true)))))
    override def nullable: Boolean = true
    override def prettyName: String = RST_BNG_RasterToGridMedian.name
    override def replacement: Expression = invoke(RST_BNG_RasterToGridMedian)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2), nc(3))
}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_BNG_RasterToGridMedian extends WithExpressionInfo {

    /** Centroid reducer: ordinary median (even n averages the two middle values). */
    val fAgg: ArrayBuffer[Double] => Double = values => {
        val sorted = values.sorted
        val mid = sorted.length / 2
        if (sorted.length % 2 == 0) (sorted(mid - 1) + sorted(mid)) / 2.0
        else sorted(mid)
    }

    /** Covering reducer: weighted median. Sort by value, accumulate weight; return the value where
      * the running weight first reaches `Σw/2`. If the half-weight lands exactly on a value
      * boundary, interpolate (average) the two straddling values. Reduces to the ordinary median
      * when all weights are equal. */
    val fAggW: ArrayBuffer[(Double, Double)] => Double = pairs => {
        val sorted = pairs.sortBy(_._1)
        val totalW = sorted.map(_._2).sum
        val half = totalW / 2.0
        var cum = 0.0
        var idx = 0
        while (idx < sorted.length && cum + sorted(idx)._2 < half) {
            cum += sorted(idx)._2
            idx += 1
        }
        // cum = Σ weight before idx; sorted(idx) is the crossing element.
        if (idx < sorted.length - 1 && cum + sorted(idx)._2 == half)
            (sorted(idx)._1 + sorted(idx + 1)._1) / 2.0
        else
            sorted(idx)._1
    }

    // New Stage-2 eval: resolution Int/Long/UTF8String + coverage + assignment.
    def eval(row: InternalRow, resolution: Int, coverage: UTF8String, assignment: UTF8String,
             conf: UTF8String): ArrayData =
        eval(row, resolution, coverage, assignment, conf, BinaryType)

    // Long overload — PySpark sends Python ints as LongType.
    def eval(row: InternalRow, resolution: Long, coverage: UTF8String, assignment: UTF8String,
             conf: UTF8String): ArrayData =
        eval(row, resolution.toInt, coverage, assignment, conf)

    // BNG string-key resolution ("1km" etc.) — PySpark may send a UTF8String.
    def eval(row: InternalRow, resolution: UTF8String, coverage: UTF8String, assignment: UTF8String,
             conf: UTF8String): ArrayData =
        eval(row, BNG.getResolution(resolution), coverage, assignment, conf)

    def eval(row: InternalRow, resolution: Int, coverage: UTF8String, assignment: UTF8String,
             conf: UTF8String, rdt: DataType): ArrayData =
        Option(RST_ErrorHandler.safeEval(() =>
            RST_BNG_RasterToGrid.eval[Double](
                row, resolution, coverage.toString, assignment.toString, conf, rdt, this.execute),
            row, rdt, conf))
            .map(_.asInstanceOf[ArrayData])
            .orNull

    def execute(ds: Dataset, resolution: Int, coverage: String, assignment: String): Array[Array[(String, Option[Double])]] =
        RST_BNG_RasterToGrid.execute[Double](ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue = None)

    def execute(ds: Dataset, resolution: Int): Array[Array[(String, Double)]] =
        execute(ds, resolution, "sparse", "centroid").map(_.collect { case (c, Some(v)) => (c, v) })

    override def name: String = "gbx_rst_bng_rastertogridmedian"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => new RST_BNG_RasterToGridMedian(c(0), c(1), Literal("complete"), Literal("centroid"))
        case 3 => new RST_BNG_RasterToGridMedian(c(0), c(1), c(2), Literal("centroid"))
        case 4 => new RST_BNG_RasterToGridMedian(c(0), c(1), c(2), c(3))
        case n => throw new IllegalArgumentException(
            s"$name expects 2-4 args (tile, resolution, [coverage], [assignment]); got $n")
    }

}
