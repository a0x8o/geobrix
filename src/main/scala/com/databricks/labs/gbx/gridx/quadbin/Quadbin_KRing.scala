package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.Quadbin
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

/** Expression that returns all quadbin cells within Chebyshev distance k of `cell` (inclusive).
  * Arguments: cell (BIGINT), k (int). */
case class Quadbin_KRing(
    cell: Expression,
    k: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cell, k)
    override def dataType: DataType = ArrayType(LongType)
    override def nullable: Boolean = true
    override def prettyName: String = Quadbin_KRing.name
    override def replacement: Expression = invoke(Quadbin_KRing)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name gbx_quadbin_kring, builder. */
object Quadbin_KRing extends WithExpressionInfo {

    def execute(cell: Long, k: Int): Array[Long] = Quadbin.kRing(cell, k)

    def eval(cell: Long, k: Int): ArrayData = ArrayData.toArrayData(execute(cell, k))

    override def name: String = "gbx_quadbin_kring"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new Quadbin_KRing(c(0), c(1))
}
