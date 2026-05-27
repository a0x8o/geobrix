package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.Quadbin
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

/** Expression that returns the quadbin cell footprint as EWKB (SRID=4326) polygon bytes.
  * Argument: cell (BIGINT). */
case class Quadbin_AsWKB(
    cell: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cell)
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = Quadbin_AsWKB.name
    override def replacement: Expression = invoke(Quadbin_AsWKB)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name gbx_quadbin_aswkb, builder. */
object Quadbin_AsWKB extends WithExpressionInfo {

    /** Build the cell polygon as EWKB bytes with SRID=4326. */
    def execute(cell: Long): Array[Byte] = {
        val (lonMin, latMin, lonMax, latMax) = Quadbin.cellBbox(cell)
        val ring = Array(
          (lonMin, latMin),
          (lonMax, latMin),
          (lonMax, latMax),
          (lonMin, latMax),
          (lonMin, latMin)
        )
        val poly = JTS.polygonFromXYs(ring)
        poly.setSRID(4326)
        JTS.toEWKB(poly)
    }

    def eval(cell: Long): Array[Byte] = execute(cell)

    override def name: String = "gbx_quadbin_aswkb"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new Quadbin_AsWKB(c(0))
}
