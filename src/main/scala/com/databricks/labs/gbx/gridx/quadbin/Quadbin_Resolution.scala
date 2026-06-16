package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.Quadbin
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

/** Expression that returns the resolution (z, 0..26) of a quadbin cell. Argument: cell (BIGINT). */
case class Quadbin_Resolution(
    cell: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cell)
    override def dataType: DataType = IntegerType
    override def nullable: Boolean = true
    override def prettyName: String = Quadbin_Resolution.name
    override def replacement: Expression = invoke(Quadbin_Resolution)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name gbx_quadbin_resolution, builder. */
object Quadbin_Resolution extends WithExpressionInfo {

    def execute(cell: Long): Int = Quadbin.resolution(cell)

    def eval(cell: Long): Int = execute(cell)

    override def name: String = "gbx_quadbin_resolution"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new Quadbin_Resolution(c(0))
}
