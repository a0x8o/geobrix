package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.expressions.{InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.Quadbin
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

/** Expression that returns the quadbin cells covering the geometry's envelope at the given resolution.
  * Arguments: geom (WKB or WKT) and resolution (int, 0..20 enforced for cell-count safety). */
case class Quadbin_Polyfill(
    geom: Expression,
    resolution: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(geom, resolution)
    override def dataType: DataType = ArrayType(LongType)
    override def nullable: Boolean = true
    override def prettyName: String = Quadbin_Polyfill.name
    override def replacement: Expression = invoke(Quadbin_Polyfill)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name gbx_quadbin_polyfill, builder. */
object Quadbin_Polyfill extends WithExpressionInfo {

    /** Max resolution accepted by polyfill (cell-count safety guard, parallel to plan spec). */
    val MAX_POLYFILL_RES: Int = 20

    def execute(geom: Geometry, resolution: Int): Array[Long] = {
        require(
          resolution >= 0 && resolution <= MAX_POLYFILL_RES,
          s"quadbin_polyfill: resolution must be in [0, $MAX_POLYFILL_RES]; got $resolution"
        )
        val env = geom.getEnvelopeInternal
        Quadbin.polyfillBbox((env.getMinX, env.getMinY, env.getMaxX, env.getMaxY), resolution)
    }

    def eval(wkb: Array[Byte], resolution: Int): ArrayData = {
        val geom = JTS.fromWKB(wkb)
        ArrayData.toArrayData(execute(geom, resolution))
    }

    def eval(wkt: UTF8String, resolution: Int): ArrayData = {
        val geom = JTS.fromWKT(wkt.toString)
        ArrayData.toArrayData(execute(geom, resolution))
    }

    override def name: String = "gbx_quadbin_polyfill"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new Quadbin_Polyfill(c(0), c(1))
}
