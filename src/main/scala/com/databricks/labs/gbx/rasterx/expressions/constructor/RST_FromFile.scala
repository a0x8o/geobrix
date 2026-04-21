package com.databricks.labs.gbx.rasterx.expressions.constructor

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.GDAL
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil}
import com.databricks.labs.gbx.util.{HadoopUtils, SerializationUtil}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Build a raster tile by reading the bytes at `path` into the tile's raster field (BinaryType).
  * Loading bytes up-front makes the tile a self-contained payload, so downstream expressions
  * (rst_clip, rst_transform, ...) carry content through the plan instead of stringly-typed paths.
  */
case class RST_FromFile(
    rasterPathExpr: Expression,
    driverExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(rasterPathExpr, driverExpr, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(BinaryType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_FromFile.name
    override def replacement: Expression = invoke(RST_FromFile)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name, builder, and evaluator for building a binary-content tile from a path. */
object RST_FromFile extends WithExpressionInfo {

    def eval(path: UTF8String, driver: UTF8String, conf: UTF8String): InternalRow =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val hPath = new Path(HadoopUtils.cleanPath(path.toString))
                val fs = hPath.getFileSystem(exprConf.hConf.value)
                val content = HadoopUtils.readContent(fs, fs.getFileStatus(hPath))
                val mtd = Map(
                  "driver" -> driver.toString,
                  "extension" -> GDAL.getExtension(driver.toString),
                  "size" -> content.length.toString
                )
                val mapData = SerializationUtil.toMapData[String, String](mtd)
                InternalRow.fromSeq(Seq(null, content, mapData))
            },
            null,
            BinaryType,
            conf
          )
        ).map(_.asInstanceOf[InternalRow]).orNull

    override def name: String = "gbx_rst_fromfile"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_FromFile(c(0), c(1))

}
