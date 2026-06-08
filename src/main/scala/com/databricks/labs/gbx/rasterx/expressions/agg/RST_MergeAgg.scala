package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.MergeRasters
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.collection.mutable.ArrayBuffer

/** Merges rasters into a single raster. */
//noinspection DuplicatedCode
case class RST_MergeAgg(
    tileExpr: Expression,
    exprConfExpr: Expression = ExpressionConfigExpr(),
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[ArrayBuffer[Any]]
      with UnaryLike[Expression] {

    override lazy val deterministic: Boolean = true
    override val child: Expression = tileExpr
    override val nullable: Boolean = false
    lazy val rasterType: DataType = RST_ExpressionUtil.rasterType(tileExpr)
    override lazy val dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    override def prettyName: String = RST_MergeAgg.name

    private lazy val projection = UnsafeProjection.create(Array[DataType](ArrayType(elementType = dataType, containsNull = false)))
    private lazy val row = new UnsafeRow(1)

    def update(buffer: ArrayBuffer[Any], input: InternalRow): ArrayBuffer[Any] = {
        val value = child.eval(input)
        buffer += InternalRow.copyValue(value)
        buffer
    }

    def merge(buffer: ArrayBuffer[Any], input: ArrayBuffer[Any]): ArrayBuffer[Any] = {
        buffer ++= input
    }

    override def createAggregationBuffer(): ArrayBuffer[Any] = ArrayBuffer.empty

    override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = newInputAggBufferOffset)

    override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = newMutableAggBufferOffset)

    override def eval(buffer: ArrayBuffer[Any]): Any = {
        val exprConf = ExpressionConfig.fromExpr(exprConfExpr)
        RST_ExpressionUtil.init(exprConf)

        if (buffer.isEmpty) {
            null
        } else if (buffer.size == 1) {
            buffer.head
        } else {

            // A groupBy().agg() does not guarantee the order tiles reach the aggregator,
            // so a last-wins mosaic would otherwise pick a different overlap winner from
            // run to run. Sort by the tile's geotransform origin (originX, originY) -- a
            // stable key intrinsic to the georef -- so the highest-origin tile reliably
            // wins the overlap regardless of arrival order. (GetDescription, the previous
            // key, is a per-open /vsimem/<uuid> path for in-memory tiles and so sorted
            // nondeterministically.) GetDescription is kept only as a final tie-break.
            val tiles = buffer
                .map(row => RasterSerializationUtil.rowToTile(row.asInstanceOf[InternalRow], rasterType))
                .sortBy { case (_, ds, _) =>
                    val gt = ds.GetGeoTransform()
                    (gt(0), gt(3), ds.GetDescription())
                }

            // If merging multiple index rasters, the index value is dropped
            val idx: Long = if (tiles.map(_._1).groupBy(identity).size == 1) tiles.head._1 else -1L
            val (res, resMtd) = MergeRasters.merge(tiles.map(_._2).toArray, tiles.head._3)

            val resRow = RasterSerializationUtil.tileToRow((idx, res, resMtd), rasterType, exprConf.hConf)

            tiles.foreach(t => RasterDriver.releaseDataset(t._2))
            RasterDriver.releaseDataset(res)

            resRow
        }
    }

    override def serialize(obj: ArrayBuffer[Any]): Array[Byte] = {
        val array = new GenericArrayData(obj.toArray)
        projection.apply(InternalRow.apply(array)).getBytes
    }

    override def deserialize(bytes: Array[Byte]): ArrayBuffer[Any] = {
        val buffer = createAggregationBuffer()
        row.pointTo(bytes, bytes.length)
        row.getArray(0).foreach(dataType, (_, x: Any) => buffer += x)
        buffer
    }

    override protected def withNewChildInternal(newChild: Expression): RST_MergeAgg = copy(tileExpr = newChild)

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_MergeAgg extends WithExpressionInfo {

    override def name: String = "gbx_rst_merge_agg"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => RST_MergeAgg(c(0))

}
