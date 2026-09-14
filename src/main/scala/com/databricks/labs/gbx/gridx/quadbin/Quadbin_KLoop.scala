package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.Quadbin
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

/** Expression that returns all quadbin cells at EXACTLY Chebyshev distance k of `cell`
  * (hollow ring). k=0 returns an array containing only the center cell.
  * Arguments: cell (BIGINT), k (INT). */
case class Quadbin_KLoop(
    cell: Expression,
    k: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cell, k)
    override def dataType: DataType = ArrayType(LongType)
    override def nullable: Boolean = true
    override def prettyName: String = Quadbin_KLoop.name
    override def replacement: Expression = invoke(Quadbin_KLoop)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name gbx_quadbin_kloop, builder. */
object Quadbin_KLoop extends WithExpressionInfo {

    def execute(cell: Long, k: Int): Array[Long] = Quadbin.kLoop(cell, k).toArray

    def eval(cell: Long, k: Int): ArrayData = ArrayData.toArrayData(execute(cell, k))
    def eval(cell: Long, k: Long): ArrayData = eval(cell, k.toInt)

    override def name: String = "gbx_quadbin_kloop"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new Quadbin_KLoop(c(0), c(1))
}
