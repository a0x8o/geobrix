package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.Quadbin
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

/** Expression that returns the Chebyshev distance between two quadbin cells at the same resolution.
  * Arguments: cellA (BIGINT), cellB (BIGINT). */
case class Quadbin_Distance(
    cellA: Expression,
    cellB: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cellA, cellB)
    override def dataType: DataType = IntegerType
    override def nullable: Boolean = true
    override def prettyName: String = Quadbin_Distance.name
    override def replacement: Expression = invoke(Quadbin_Distance)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name gbx_quadbin_distance, builder. */
object Quadbin_Distance extends WithExpressionInfo {

    def execute(a: Long, b: Long): Int = Quadbin.cellDistance(a, b)

    def eval(a: Long, b: Long): Int = execute(a, b)

    override def name: String = "gbx_quadbin_distance"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new Quadbin_Distance(c(0), c(1))
}
