package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

/** Expression that tessellates a geometry into quadbin cells (chip structs (cell, geom) per cell).
  * Arguments: geom (WKB or WKT), resolution (int, 0..20 enforced via Quadbin_Polyfill.MAX_POLYFILL_RES). */
case class Quadbin_Tessellate(
    geom: Expression,
    resolution: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(geom, resolution)
    override def dataType: DataType = ArrayType(Quadbin_Tessellate.chipType)
    override def nullable: Boolean = true
    override def prettyName: String = Quadbin_Tessellate.name
    override def replacement: Expression = invoke(Quadbin_Tessellate)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name gbx_quadbin_tessellate, builder. */
object Quadbin_Tessellate extends WithExpressionInfo {

    /** Chip struct returned per cell: cell BIGINT + intersected polygon EWKB. */
    val chipType: StructType = StructType(
      Array(
        StructField("cell", LongType, nullable = false),
        StructField("geom", BinaryType, nullable = true)
      )
    )

    def execute(geom: Geometry, resolution: Int): Array[(Long, Array[Byte])] = {
        val cells = Quadbin_Polyfill.execute(geom, resolution)
        cells.flatMap { cell =>
            val cellGeomBytes = Quadbin_AsWKB.execute(cell)
            val cellGeom = JTS.fromWKB(cellGeomBytes)
            try {
                val inter = cellGeom.intersection(geom)
                if (inter == null || inter.isEmpty) None
                else {
                    inter.setSRID(4326)
                    Some((cell, JTS.toEWKB(inter)))
                }
            } catch {
                case _: Throwable => None
            }
        }
    }

    private def toInternalRows(chips: Array[(Long, Array[Byte])]): Array[InternalRow] =
        chips.map { case (cell, bytes) => InternalRow.fromSeq(Seq(cell, bytes)) }

    def eval(wkb: Array[Byte], resolution: Int): ArrayData = {
        val geom = JTS.fromWKB(wkb)
        ArrayData.toArrayData(toInternalRows(execute(geom, resolution)))
    }

    def eval(wkb: Array[Byte], resolution: Long): ArrayData = eval(wkb, resolution.toInt)

    def eval(wkt: UTF8String, resolution: Int): ArrayData = {
        val geom = JTS.fromWKT(wkt.toString)
        ArrayData.toArrayData(toInternalRows(execute(geom, resolution)))
    }

    def eval(wkt: UTF8String, resolution: Long): ArrayData = eval(wkt, resolution.toInt)

    override def name: String = "gbx_quadbin_tessellate"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new Quadbin_Tessellate(c(0), c(1))
}
