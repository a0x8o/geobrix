package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.collection.mutable

/** Shared helper for `RST_BNG_RasterToGrid*` expressions.
  *
  * Delegates the pixel-accumulation loop to [[RasterToGridGeneric]], which reprojects the
  * raster to EPSG:27700 (nearest-neighbour) before binning. Two BNG-specific guards are
  * applied here, before the generic path:
  *
  *  1. Resolution validation — must be a member of [[BNG.resolutions]].
  *  2. Per-pixel cell validity — [[BNG.isValid]] rejects pixels whose projected centroid
  *     falls outside the British National Grid coordinate extent; those pixels are silently
  *     discarded (same behaviour as before the generic refactor).
  *
  * Cell ids are Long internally; [[BNG.renderCellId]] converts them to the user-facing
  * formatted String (e.g. "TL3098") at the output boundary.
  */
object RST_BNG_RasterToGrid {

    def execute[T](
        ds: Dataset,
        resolution: Int,
        fAgg: mutable.ArrayBuffer[Double] => T
    ): Array[Array[(String, T)]] = {
        require(
          BNG.resolutions.contains(resolution),
          s"raster→bng: resolution must be one of ${BNG.resolutions.toSeq.sorted.mkString(", ")}; got $resolution"
        )
        RasterToGridGeneric.execute(BNG, ds, resolution, fAgg, BNG.isValid)
            .map(_.map { case (c, v) => (c.asInstanceOf[String], v) })
    }

    def eval[T](
        row: InternalRow,
        resolution: Int,
        conf: UTF8String,
        rdt: DataType,
        execute: (Dataset, Int) => Array[Array[(String, T)]]
    ): ArrayData = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val result = execute(ds, resolution)
        RasterDriver.releaseDataset(ds)
        ArrayData.toArrayData(
          result.map(band =>
              ArrayData.toArrayData(
                band.map { case (cellId, measure) =>
                    InternalRow.fromSeq(Seq(UTF8String.fromString(cellId), measure))
                }
              )
          )
        )
    }
}
