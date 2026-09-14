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

    /** Stage-2 path: coverage/assignment-aware, nullable-measure output.
      * Passes [[BNG.isValid]] as the per-cell validity predicate.
      *
      * @param coverage   `"sparse"` (only cells with pixels) or `"complete"` (also emit
      *                   covered-but-empty cells carrying `emptyValue`)
      * @param assignment `"centroid"` (bin each pixel by its centroid, `fAgg`) or
      *                   `"covering"` (area-weighted distribution, `fAggW`)
      * @param emptyValue measure for covered-but-empty cells added by `complete`
      *
      * `fAgg`/`fAggW` are only ever called on NON-EMPTY buffers — a cell with no pixels never
      * reaches a reducer; it is materialised (under `complete`) as `emptyValue`/`None` instead.
      * Reducers may therefore assume at least one element. Copy-forward note for Quadbin.
      * @return per-band `(cellId: String, Option[T])` — `None` marks a covered-but-empty cell
      */
    def execute[T](
        ds: Dataset,
        resolution: Int,
        coverage: String,
        assignment: String,
        fAgg: mutable.ArrayBuffer[Double] => T,
        fAggW: mutable.ArrayBuffer[(Double, Double)] => T,
        emptyValue: Option[T]
    ): Array[Array[(String, Option[T])]] = {
        require(
          BNG.resolutions.contains(resolution),
          s"raster→bng: resolution must be one of ${BNG.resolutions.toSeq.sorted.mkString(", ")}; got $resolution"
        )
        RasterToGridGeneric.execute(BNG, ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue, BNG.isValid)
            .map(_.map { case (c, v) => (c.asInstanceOf[String], v) })
    }

    /** Legacy Stage-1 path: sparse+centroid only, non-Option output.
      * Retained for direct callers (GridRasterParityBaseline, RST_BNG_RasterToGridTest, bench).
      */
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

    /** Stage-2 eval: coverage/assignment-aware, unwraps `Option[T]` → `Some(v)=>v`, `None=>null`
      * into the nullable measure StructField. */
    def eval[T](
        row: InternalRow,
        resolution: Int,
        coverage: String,
        assignment: String,
        conf: UTF8String,
        rdt: DataType,
        execute: (Dataset, Int, String, String) => Array[Array[(String, Option[T])]]
    ): ArrayData = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val result = execute(ds, resolution, coverage, assignment)
        RasterDriver.releaseDataset(ds)
        ArrayData.toArrayData(
          result.map(band =>
              ArrayData.toArrayData(
                band.map { case (cellId, measure) =>
                    val m: Any = measure match {
                        case Some(v) => v
                        case None    => null
                    }
                    InternalRow.fromSeq(Seq(UTF8String.fromString(cellId), m))
                }
              )
          )
        )
    }

}
