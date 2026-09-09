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

/** Returns the population variance of the raster values within the grid cell. */
case class RST_H3_RasterToGridVariance(
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
    override def prettyName: String = RST_H3_RasterToGridVariance.name
    override def replacement: Expression = invoke(RST_H3_RasterToGridVariance)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_H3_RasterToGridVariance extends WithExpressionInfo {

    /** Centroid reducer: population variance (two-pass; matches numpy ddof=0; see spec 3.2). */
    val fAgg: ArrayBuffer[Double] => Double = values => {
        val n = values.length
        val m = values.sum / n
        values.map(v => { val d = v - m; d * d }).sum / n
    }

    /** Covering reducer: weighted population variance.
      * `mean = Σ(v*w)/Σw ; variance = Σ(w*(v-mean)^2)/Σw`. */
    val fAggW: ArrayBuffer[(Double, Double)] => Double = pairs => {
        val sw = pairs.map(_._2).sum
        val mean = pairs.map { case (v, w) => v * w }.sum / sw
        pairs.map { case (v, w) => w * { val d = v - mean; d * d } }.sum / sw
    }

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

    def execute(ds: Dataset, resolution: Int, coverage: String, assignment: String): Array[Array[(Long, Option[Double])]] =
        RST_H3_RasterToGrid.execute[Double](ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue = None)

    def execute(ds: Dataset, resolution: Int): Array[Array[(Long, Double)]] =
        execute(ds, resolution, "sparse", "centroid").map(_.collect { case (c, Some(v)) => (c, v) })

    override def name: String = "gbx_rst_h3_rastertogridvariance"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => new RST_H3_RasterToGridVariance(c(0), c(1), Literal("complete"), Literal("centroid"))
        case 3 => new RST_H3_RasterToGridVariance(c(0), c(1), c(2), Literal("centroid"))
        case 4 => new RST_H3_RasterToGridVariance(c(0), c(1), c(2), c(3))
        case n => throw new IllegalArgumentException(
            s"$name expects 2-4 args (tile, resolution, [coverage], [assignment]); got $n")
    }

}
