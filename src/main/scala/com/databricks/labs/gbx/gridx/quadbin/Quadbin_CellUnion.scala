package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.operation.union.CascadedPolygonUnion

import scala.jdk.CollectionConverters._

/** Expression that unions an ARRAY<BIGINT> of quadbin cells into a single MultiPolygon (EWKB SRID=4326).
  * Argument: cells (ArrayType(LongType)). */
case class Quadbin_CellUnion(
    cells: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(cells)
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = Quadbin_CellUnion.name
    override def replacement: Expression = invoke(Quadbin_CellUnion)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name gbx_quadbin_cellunion, builder. */
object Quadbin_CellUnion extends WithExpressionInfo {

    def execute(cells: Array[Long]): Array[Byte] = {
        if (cells == null || cells.isEmpty) return null
        val polys: java.util.List[Geometry] = cells
            .map(Quadbin_AsWKB.execute)
            .map(JTS.fromWKB)
            .toList
            .asJava
        val unioned: Geometry = CascadedPolygonUnion.union(polys)
        if (unioned == null) null
        else {
            unioned.setSRID(4326)
            JTS.toEWKB(unioned)
        }
    }

    def eval(cellsArr: ArrayData): Array[Byte] = {
        val arr = cellsArr.toLongArray()
        execute(arr)
    }

    override def name: String = "gbx_quadbin_cellunion"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new Quadbin_CellUnion(c(0))
}
