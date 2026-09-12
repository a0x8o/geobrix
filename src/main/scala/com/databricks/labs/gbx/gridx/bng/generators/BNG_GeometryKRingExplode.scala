package com.databricks.labs.gbx.gridx.bng.generators

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler
import com.databricks.labs.gbx.gridx.grid.{BNG, GeomDilation}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Generator that explodes the k-ring geometry of a BNG cell (from geom at resolution) into one row per cell.
  * Arguments: geom, resolution, k[, mode]. mode is optional (default: boundary-out). */
case class BNG_GeometryKRingExplode(
    geom: Expression,
    resolution: Expression,
    k: Expression,
    mode: Expression
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    override def position: Boolean = false
    override def inline: Boolean = false
    override def children: Seq[Expression] = Seq(geom, resolution, k, mode)

    // noinspection DuplicatedCode
    override def eval(input: InternalRow): IterableOnce[InternalRow] = {
        val geometryRaw = geom.eval(input)
        val resolutionRaw = resolution.eval(input)
        val kRaw = k.eval(input)
        val modeRaw = mode.eval(input)
        if (geometryRaw == null || resolutionRaw == null || kRaw == null) {
            Seq.empty
        } else {
            val resolutionVal = resolution.dataType match {
                case StringType  => BNG.resolutionMap(resolutionRaw.asInstanceOf[UTF8String].toString)
                case IntegerType => resolutionRaw.asInstanceOf[Int]
            }
            val kVal = kRaw.asInstanceOf[Int]
            val modeStr = if (modeRaw == null) GeomDilation.DEFAULT_MODE else modeRaw.asInstanceOf[UTF8String].toString

            GridErrorHandler.safeEval[IterableOnce[InternalRow]](Iterator.empty) {
                val geometryVal = geom.dataType match {
                    case StringType => JTS.fromWKT(geometryRaw.asInstanceOf[UTF8String].toString)
                    case BinaryType => JTS.fromWKB(geometryRaw.asInstanceOf[Array[Byte]])
                }
                BNG.geometryKRing(geometryVal, resolutionVal, kVal, modeStr)
                    .map(row => InternalRow.fromSeq(Seq(UTF8String.fromString(BNG.format(row)))))
            }
        }
    }

    override def elementSchema: StructType = StructType(Seq(StructField("cellid", StringType)))

    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1), nc(2), nc(3))

}

/** Companion: SQL name gbx_bng_geomkringexplode, builder. */
object BNG_GeometryKRingExplode extends WithExpressionInfo {

    override def name: String = "gbx_bng_geomkringexplode"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 3 => new BNG_GeometryKRingExplode(c(0), c(1), c(2), Literal(GeomDilation.DEFAULT_MODE))
        case 4 => new BNG_GeometryKRingExplode(c(0), c(1), c(2), c(3))
    }

}
