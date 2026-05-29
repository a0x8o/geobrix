package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.DataType

/** Catalyst expression that packs grid parameters into the grid-spec STRUCT consumed by all
  * gbx_custom_* operations.  Accepts 7 or 8 arguments; the optional 8th is the SRID
  * (default -1 meaning no CRS).
  *
  * Arguments (all INT or LONG):
  *   boundXMin, boundXMax, boundYMin, boundYMax, cellSplits, rootCellSizeX, rootCellSizeY[, srid]
  */
case class Custom_Grid(
    boundXMinExpr:  Expression,
    boundXMaxExpr:  Expression,
    boundYMinExpr:  Expression,
    boundYMaxExpr:  Expression,
    cellSplitsExpr: Expression,
    rootCellSizeXExpr: Expression,
    rootCellSizeYExpr: Expression,
    sridExpr:       Expression
) extends Expression with CodegenFallback {

    override def children: Seq[Expression] =
        Seq(boundXMinExpr, boundXMaxExpr, boundYMinExpr, boundYMaxExpr,
            cellSplitsExpr, rootCellSizeXExpr, rootCellSizeYExpr, sridExpr)

    override def dataType: DataType  = Custom_GridSpec.gridStructType
    override def nullable: Boolean   = false
    override def foldable: Boolean   = children.forall(_.foldable)

    override def eval(input: InternalRow): Any = {
        val xMin   = Custom_GridSpec.asLong(boundXMinExpr.eval(input),    "bound_x_min")
        val xMax   = Custom_GridSpec.asLong(boundXMaxExpr.eval(input),    "bound_x_max")
        val yMin   = Custom_GridSpec.asLong(boundYMinExpr.eval(input),    "bound_y_min")
        val yMax   = Custom_GridSpec.asLong(boundYMaxExpr.eval(input),    "bound_y_max")
        val splits = Custom_GridSpec.asInt(cellSplitsExpr.eval(input),    "cell_splits")
        val rootX  = Custom_GridSpec.asInt(rootCellSizeXExpr.eval(input), "root_cell_size_x")
        val rootY  = Custom_GridSpec.asInt(rootCellSizeYExpr.eval(input), "root_cell_size_y")
        val srid   = Custom_GridSpec.asInt(sridExpr.eval(input),          "srid")

        require(xMax > xMin,
            s"gbx_custom_grid: bound_x_max ($xMax) must be greater than bound_x_min ($xMin)")
        require(yMax > yMin,
            s"gbx_custom_grid: bound_y_max ($yMax) must be greater than bound_y_min ($yMin)")
        require(splits >= 2,
            s"gbx_custom_grid: cell_splits must be >= 2; got $splits")
        require(rootX > 0,
            s"gbx_custom_grid: root_cell_size_x must be > 0; got $rootX")
        require(rootY > 0,
            s"gbx_custom_grid: root_cell_size_y must be > 0; got $rootY")

        InternalRow(xMin, xMax, yMin, yMax, splits, rootX, rootY, srid)
    }

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7))

}

/** Companion: SQL name gbx_custom_grid, 7- or 8-arg builder. */
object Custom_Grid extends WithExpressionInfo {

    override def name: String = "gbx_custom_grid"

    override def builder(): FunctionBuilder = {
        case c if c.length == 7 =>
            Custom_Grid(c(0), c(1), c(2), c(3), c(4), c(5), c(6), Literal(-1))
        case c if c.length == 8 =>
            Custom_Grid(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7))
        case c =>
            throw new IllegalArgumentException(
                s"gbx_custom_grid requires 7 or 8 arguments; got ${c.length}")
    }

}
