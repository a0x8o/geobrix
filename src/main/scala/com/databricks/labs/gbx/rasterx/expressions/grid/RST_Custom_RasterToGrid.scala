package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.custom.Custom_GridSpec
import com.databricks.labs.gbx.gridx.grid.CustomGridSystem
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.CustomGridBounds
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** Shared custom-grid raster→grid pixel aggregation.
  *
  * Delegates to the grid-generic [[RasterToGridGeneric.execute]] with `isCellValid = _ => true`
  * ([[CustomGridSystem]] has no per-cell validity guard; the R3 bounds-contain-extent check runs UP
  * FRONT in [[execute]] via [[CustomGridBounds.requireBoundsContainRaster]]) and narrows the `Any`
  * cell key to `Long` ([[CustomGridSystem]] uses the base Long `renderCellId`). Per-grid reducer
  * bodies (`fAgg`/`fAggW`) are grid-independent and copied verbatim from the H3 family — this
  * duplication is the established repo pattern for H3/quadbin/BNG.
  */
object RST_Custom_RasterToGrid {

    /** Run a custom raster→grid aggregation over `ds`.
      *
      * The R3 guard (bounds must contain the raster extent) runs first so an out-of-bounds pixel
      * centroid can never throw mid-aggregation. `fAgg`/`fAggW` are only ever called on NON-EMPTY
      * buffers; covered-but-empty cells (added under `complete`) materialise as `emptyValue`/`None`.
      *
      * @return per-band `(cellId: Long, Option[T])` — `None` marks a covered-but-empty cell.
      */
    def execute[T](
        grid: CustomGridSystem,
        ds: Dataset,
        resolution: Int,
        coverage: String,
        assignment: String,
        fAgg: mutable.ArrayBuffer[Double] => T,
        fAggW: mutable.ArrayBuffer[(Double, Double)] => T,
        emptyValue: Option[T]
    ): Array[Array[(Long, Option[T])]] = {
        CustomGridBounds.requireBoundsContainRaster(grid, ds)
        RasterToGridGeneric.execute(grid, ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue, _ => true)
            .map(_.map { case (c, v) => (c.asInstanceOf[Long], v) })
    }

    /** Runs a custom raster→grid `execute` over the tile in `row` (with the grid decoded from the
      * grid-spec struct in `gridRow`) and packs the result into the `Array[Array[Struct(cellID, measure)]]`
      * return shape. A `None` measure is emitted as SQL `null`; the measure StructField is nullable.
      */
    def eval[T](
        row: InternalRow,
        gridRow: InternalRow,
        resolution: Int,
        coverage: String,
        assignment: String,
        conf: UTF8String,
        rdt: DataType,
        execute: (CustomGridSystem, Dataset, Int, String, String) => Array[Array[(Long, Option[T])]]
    ): ArrayData = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val gridSys = Custom_GridSpec.systemFromRow(gridRow)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val result = execute(gridSys, ds, resolution, coverage, assignment)
        RasterDriver.releaseDataset(ds)
        ArrayData.toArrayData(
          result.map(band =>
              ArrayData.toArrayData(
                band.map { case (cellId, measure) =>
                    val m: Any = measure match {
                        case Some(v) => v
                        case None    => null
                    }
                    InternalRow.fromSeq(Seq(cellId, m))
                }
              )
          )
        )
    }

}

/** Shared plumbing for the 8 custom raster→grid stat expressions.
  *
  * SQL arg order: `gbx_rst_custom_rastertogrid<stat>(tile, grid, resolution, [coverage], [assignment])`
  * — the `grid` struct (from `gbx_custom_grid(...)`) sits immediately after `tile`, mirroring the
  * `gbx_custom_*` grid-op arg convention, with the H3 optional `[coverage]`,`[assignment]` trailing.
  */
private[grid] abstract class RST_Custom_RasterToGridBase extends InvokedExpression {
    def tile: Expression
    def grid: Expression
    def resolution: Expression
    def coverage: Expression
    def assignment: Expression

    override def children: Seq[Expression] = Seq(tile, grid, resolution, coverage, assignment, ExpressionConfigExpr())
    override def dataType: DataType =
        ArrayType(ArrayType(StructType(Seq(
            StructField("cellID", LongType),
            StructField("measure", DoubleType, nullable = true)))))
    override def nullable: Boolean = true
}

/** Shared companion helpers: the 3-to-5-arg builder factory and the invoke-facing eval bridge. */
private[grid] object RST_Custom_RasterToGridBase {

    /** Builds the 3-to-5-arg `FunctionBuilder` shared by every custom stat expression.
      * `make(tile, grid, resolution, coverage, assignment)` constructs the concrete case class.
      */
    def builder(name: String)(
        make: (Expression, Expression, Expression, Expression, Expression) => Expression
    ): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 3 => make(c(0), c(1), c(2), Literal("complete"), Literal("centroid"))
        case 4 => make(c(0), c(1), c(2), c(3), Literal("centroid"))
        case 5 => make(c(0), c(1), c(2), c(3), c(4))
        case n => throw new IllegalArgumentException(
            s"$name expects 3-5 args (tile, grid, resolution, [coverage], [assignment]); got $n")
    }

    /** Invoke-facing eval bridge: wraps [[RST_Custom_RasterToGrid.eval]] in the shared error handler. */
    def eval[T](
        row: InternalRow, gridRow: InternalRow, resolution: Int,
        coverage: UTF8String, assignment: UTF8String, conf: UTF8String, rdt: DataType,
        execute: (CustomGridSystem, Dataset, Int, String, String) => Array[Array[(Long, Option[T])]]
    ): ArrayData =
        Option(RST_ErrorHandler.safeEval(() =>
            RST_Custom_RasterToGrid.eval[T](
                row, gridRow, resolution, coverage.toString, assignment.toString, conf, rdt, execute),
            row, rdt, conf))
            .map(_.asInstanceOf[ArrayData])
            .orNull
}

// -------------------------------------------------------------------------------------------------
// The 8 stat expressions. Each mirrors its RST_H3_RasterToGrid<stat> counterpart, with the reducer
// bodies copied verbatim (they are grid-independent) and the custom `grid` struct arg inserted after
// `tile`. Cell keys are Long; isCellValid = _ => true (R3 bounds check is up front, not per-cell).
// -------------------------------------------------------------------------------------------------

/** Returns the average value of the raster within the grid cell. */
case class RST_Custom_RasterToGridAvg(
    tile: Expression, grid: Expression, resolution: Expression, coverage: Expression, assignment: Expression
) extends RST_Custom_RasterToGridBase {
    override def prettyName: String = RST_Custom_RasterToGridAvg.name
    override def replacement: Expression = invoke(RST_Custom_RasterToGridAvg)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4))
}

object RST_Custom_RasterToGridAvg extends WithExpressionInfo {
    val fAgg: ArrayBuffer[Double] => Double = values => values.sum / values.length
    val fAggW: ArrayBuffer[(Double, Double)] => Double = pairs => {
        val sw = pairs.map(_._2).sum
        pairs.map { case (v, w) => v * w }.sum / sw
    }
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String): ArrayData =
        eval(row, grid, resolution, coverage, assignment, conf, BinaryType)
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String, rdt: DataType): ArrayData =
        RST_Custom_RasterToGridBase.eval[Double](row, grid, resolution, coverage, assignment, conf, rdt, this.execute)
    def execute(grid: CustomGridSystem, ds: Dataset, resolution: Int, coverage: String,
                assignment: String): Array[Array[(Long, Option[Double])]] =
        RST_Custom_RasterToGrid.execute[Double](grid, ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue = None)
    override def name: String = "gbx_rst_custom_rastertogridavg"
    override def builder(): FunctionBuilder =
        RST_Custom_RasterToGridBase.builder(name)(new RST_Custom_RasterToGridAvg(_, _, _, _, _))
}

/** Returns the number of pixels aggregated into the grid cell (Double; covering count is `Σw`). */
case class RST_Custom_RasterToGridCount(
    tile: Expression, grid: Expression, resolution: Expression, coverage: Expression, assignment: Expression
) extends RST_Custom_RasterToGridBase {
    override def prettyName: String = RST_Custom_RasterToGridCount.name
    override def replacement: Expression = invoke(RST_Custom_RasterToGridCount)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4))
}

object RST_Custom_RasterToGridCount extends WithExpressionInfo {
    val fAgg: ArrayBuffer[Double] => Double = values => values.length.toDouble
    val fAggW: ArrayBuffer[(Double, Double)] => Double = pairs => pairs.map(_._2).sum
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String): ArrayData =
        eval(row, grid, resolution, coverage, assignment, conf, BinaryType)
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String, rdt: DataType): ArrayData =
        RST_Custom_RasterToGridBase.eval[Double](row, grid, resolution, coverage, assignment, conf, rdt, this.execute)
    def execute(grid: CustomGridSystem, ds: Dataset, resolution: Int, coverage: String,
                assignment: String): Array[Array[(Long, Option[Double])]] =
        RST_Custom_RasterToGrid.execute[Double](grid, ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue = Some(0.0))
    override def name: String = "gbx_rst_custom_rastertogridcount"
    override def builder(): FunctionBuilder =
        RST_Custom_RasterToGridBase.builder(name)(new RST_Custom_RasterToGridCount(_, _, _, _, _))
}

/** Returns the maximum value of the raster in the grid cell. */
case class RST_Custom_RasterToGridMax(
    tile: Expression, grid: Expression, resolution: Expression, coverage: Expression, assignment: Expression
) extends RST_Custom_RasterToGridBase {
    override def prettyName: String = RST_Custom_RasterToGridMax.name
    override def replacement: Expression = invoke(RST_Custom_RasterToGridMax)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4))
}

object RST_Custom_RasterToGridMax extends WithExpressionInfo {
    val fAgg: ArrayBuffer[Double] => Double = values => values.max
    val fAggW: ArrayBuffer[(Double, Double)] => Double = pairs => pairs.map(_._1).max
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String): ArrayData =
        eval(row, grid, resolution, coverage, assignment, conf, BinaryType)
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String, rdt: DataType): ArrayData =
        RST_Custom_RasterToGridBase.eval[Double](row, grid, resolution, coverage, assignment, conf, rdt, this.execute)
    def execute(grid: CustomGridSystem, ds: Dataset, resolution: Int, coverage: String,
                assignment: String): Array[Array[(Long, Option[Double])]] =
        RST_Custom_RasterToGrid.execute[Double](grid, ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue = None)
    override def name: String = "gbx_rst_custom_rastertogridmax"
    override def builder(): FunctionBuilder =
        RST_Custom_RasterToGridBase.builder(name)(new RST_Custom_RasterToGridMax(_, _, _, _, _))
}

/** Returns the minimum value of the raster in the grid cell. */
case class RST_Custom_RasterToGridMin(
    tile: Expression, grid: Expression, resolution: Expression, coverage: Expression, assignment: Expression
) extends RST_Custom_RasterToGridBase {
    override def prettyName: String = RST_Custom_RasterToGridMin.name
    override def replacement: Expression = invoke(RST_Custom_RasterToGridMin)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4))
}

object RST_Custom_RasterToGridMin extends WithExpressionInfo {
    val fAgg: ArrayBuffer[Double] => Double = values => values.min
    val fAggW: ArrayBuffer[(Double, Double)] => Double = pairs => pairs.map(_._1).min
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String): ArrayData =
        eval(row, grid, resolution, coverage, assignment, conf, BinaryType)
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String, rdt: DataType): ArrayData =
        RST_Custom_RasterToGridBase.eval[Double](row, grid, resolution, coverage, assignment, conf, rdt, this.execute)
    def execute(grid: CustomGridSystem, ds: Dataset, resolution: Int, coverage: String,
                assignment: String): Array[Array[(Long, Option[Double])]] =
        RST_Custom_RasterToGrid.execute[Double](grid, ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue = None)
    override def name: String = "gbx_rst_custom_rastertogridmin"
    override def builder(): FunctionBuilder =
        RST_Custom_RasterToGridBase.builder(name)(new RST_Custom_RasterToGridMin(_, _, _, _, _))
}

/** Returns the median value of the raster. */
case class RST_Custom_RasterToGridMedian(
    tile: Expression, grid: Expression, resolution: Expression, coverage: Expression, assignment: Expression
) extends RST_Custom_RasterToGridBase {
    override def prettyName: String = RST_Custom_RasterToGridMedian.name
    override def replacement: Expression = invoke(RST_Custom_RasterToGridMedian)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4))
}

object RST_Custom_RasterToGridMedian extends WithExpressionInfo {
    val fAgg: ArrayBuffer[Double] => Double = values => {
        val sorted = values.sorted
        val mid = sorted.length / 2
        if (sorted.length % 2 == 0) (sorted(mid - 1) + sorted(mid)) / 2.0
        else sorted(mid)
    }
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
        if (idx < sorted.length - 1 && cum + sorted(idx)._2 == half)
            (sorted(idx)._1 + sorted(idx + 1)._1) / 2.0
        else
            sorted(idx)._1
    }
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String): ArrayData =
        eval(row, grid, resolution, coverage, assignment, conf, BinaryType)
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String, rdt: DataType): ArrayData =
        RST_Custom_RasterToGridBase.eval[Double](row, grid, resolution, coverage, assignment, conf, rdt, this.execute)
    def execute(grid: CustomGridSystem, ds: Dataset, resolution: Int, coverage: String,
                assignment: String): Array[Array[(Long, Option[Double])]] =
        RST_Custom_RasterToGrid.execute[Double](grid, ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue = None)
    override def name: String = "gbx_rst_custom_rastertogridmedian"
    override def builder(): FunctionBuilder =
        RST_Custom_RasterToGridBase.builder(name)(new RST_Custom_RasterToGridMedian(_, _, _, _, _))
}

/** Returns the sum of the raster values within the grid cell. */
case class RST_Custom_RasterToGridSum(
    tile: Expression, grid: Expression, resolution: Expression, coverage: Expression, assignment: Expression
) extends RST_Custom_RasterToGridBase {
    override def prettyName: String = RST_Custom_RasterToGridSum.name
    override def replacement: Expression = invoke(RST_Custom_RasterToGridSum)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4))
}

object RST_Custom_RasterToGridSum extends WithExpressionInfo {
    val fAgg: ArrayBuffer[Double] => Double = values => values.sum
    val fAggW: ArrayBuffer[(Double, Double)] => Double = pairs => pairs.map { case (v, w) => v * w }.sum
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String): ArrayData =
        eval(row, grid, resolution, coverage, assignment, conf, BinaryType)
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String, rdt: DataType): ArrayData =
        RST_Custom_RasterToGridBase.eval[Double](row, grid, resolution, coverage, assignment, conf, rdt, this.execute)
    def execute(grid: CustomGridSystem, ds: Dataset, resolution: Int, coverage: String,
                assignment: String): Array[Array[(Long, Option[Double])]] =
        RST_Custom_RasterToGrid.execute[Double](grid, ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue = None)
    override def name: String = "gbx_rst_custom_rastertogridsum"
    override def builder(): FunctionBuilder =
        RST_Custom_RasterToGridBase.builder(name)(new RST_Custom_RasterToGridSum(_, _, _, _, _))
}

/** Returns the population variance of the raster values within the grid cell. */
case class RST_Custom_RasterToGridVariance(
    tile: Expression, grid: Expression, resolution: Expression, coverage: Expression, assignment: Expression
) extends RST_Custom_RasterToGridBase {
    override def prettyName: String = RST_Custom_RasterToGridVariance.name
    override def replacement: Expression = invoke(RST_Custom_RasterToGridVariance)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4))
}

object RST_Custom_RasterToGridVariance extends WithExpressionInfo {
    val fAgg: ArrayBuffer[Double] => Double = values => {
        val n = values.length
        val m = values.sum / n
        values.map(v => { val d = v - m; d * d }).sum / n
    }
    val fAggW: ArrayBuffer[(Double, Double)] => Double = pairs => {
        val sw = pairs.map(_._2).sum
        val mean = pairs.map { case (v, w) => v * w }.sum / sw
        pairs.map { case (v, w) => w * { val d = v - mean; d * d } }.sum / sw
    }
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String): ArrayData =
        eval(row, grid, resolution, coverage, assignment, conf, BinaryType)
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String, rdt: DataType): ArrayData =
        RST_Custom_RasterToGridBase.eval[Double](row, grid, resolution, coverage, assignment, conf, rdt, this.execute)
    def execute(grid: CustomGridSystem, ds: Dataset, resolution: Int, coverage: String,
                assignment: String): Array[Array[(Long, Option[Double])]] =
        RST_Custom_RasterToGrid.execute[Double](grid, ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue = None)
    override def name: String = "gbx_rst_custom_rastertogridvariance"
    override def builder(): FunctionBuilder =
        RST_Custom_RasterToGridBase.builder(name)(new RST_Custom_RasterToGridVariance(_, _, _, _, _))
}

/** Returns the population standard deviation of the raster values within the grid cell. */
case class RST_Custom_RasterToGridStddev(
    tile: Expression, grid: Expression, resolution: Expression, coverage: Expression, assignment: Expression
) extends RST_Custom_RasterToGridBase {
    override def prettyName: String = RST_Custom_RasterToGridStddev.name
    override def replacement: Expression = invoke(RST_Custom_RasterToGridStddev)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4))
}

object RST_Custom_RasterToGridStddev extends WithExpressionInfo {
    val fAgg: ArrayBuffer[Double] => Double = values => {
        val n = values.length
        val m = values.sum / n
        math.sqrt(values.map(v => { val d = v - m; d * d }).sum / n)
    }
    val fAggW: ArrayBuffer[(Double, Double)] => Double = pairs =>
        math.sqrt(RST_Custom_RasterToGridVariance.fAggW(pairs))
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String): ArrayData =
        eval(row, grid, resolution, coverage, assignment, conf, BinaryType)
    def eval(row: InternalRow, grid: InternalRow, resolution: Int, coverage: UTF8String,
             assignment: UTF8String, conf: UTF8String, rdt: DataType): ArrayData =
        RST_Custom_RasterToGridBase.eval[Double](row, grid, resolution, coverage, assignment, conf, rdt, this.execute)
    def execute(grid: CustomGridSystem, ds: Dataset, resolution: Int, coverage: String,
                assignment: String): Array[Array[(Long, Option[Double])]] =
        RST_Custom_RasterToGrid.execute[Double](grid, ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue = None)
    override def name: String = "gbx_rst_custom_rastertogridstddev"
    override def builder(): FunctionBuilder =
        RST_Custom_RasterToGridBase.builder(name)(new RST_Custom_RasterToGridStddev(_, _, _, _, _))
}
