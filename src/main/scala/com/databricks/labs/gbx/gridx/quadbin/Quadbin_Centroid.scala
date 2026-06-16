package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.Quadbin
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

/** Expression that returns the quadbin cell centroid as EWKB (SRID=4326) POINT bytes.
  * Argument: cell (BIGINT). */
case class Quadbin_Centroid(
    cell: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cell)
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = Quadbin_Centroid.name
    override def replacement: Expression = invoke(Quadbin_Centroid)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name gbx_quadbin_centroid, builder. */
object Quadbin_Centroid extends WithExpressionInfo {

    /** Build the cell-centroid Point as EWKB bytes with SRID=4326. */
    def execute(cell: Long): Array[Byte] = {
        val (lon, lat) = Quadbin.cellCenter(cell)
        val pt = JTS.point(lon, lat)
        pt.setSRID(4326)
        JTS.toEWKB(pt)
    }

    def eval(cell: Long): Array[Byte] = execute(cell)

    override def name: String = "gbx_quadbin_centroid"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new Quadbin_Centroid(c(0))
}
