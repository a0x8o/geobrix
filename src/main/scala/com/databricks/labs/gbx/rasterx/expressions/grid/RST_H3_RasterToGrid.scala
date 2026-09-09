package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.H3
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.collection.mutable

object RST_H3_RasterToGrid {

    /** Shared H3 raster→grid pixel aggregation. Delegates to the generic 9-param
      * [[RasterToGridGeneric.execute]] (H3 has no per-cell validity guard, so the default
      * `isCellValid` accepts all cells) and narrows the `Any` cell key to `Long`.
      *
      * @param coverage   `"sparse"` (only cells with pixels) or `"complete"` (also emit
      *                   covered-but-empty cells carrying `emptyValue`)
      * @param assignment `"centroid"` (bin each pixel by its centroid, `fAgg`) or
      *                   `"covering"` (area-weighted distribution, `fAggW`)
      * @param emptyValue measure for covered-but-empty cells added by `complete`
      *
      * `fAgg`/`fAggW` are only ever called on NON-EMPTY buffers — a cell with no pixels never
      * reaches a reducer; it is materialised (under `complete`) as `emptyValue`/`None` instead.
      * Reducers may therefore assume at least one element (e.g. `.min`, `Σw > 0`). Copy-forward
      * note for BNG/Quadbin.
      * @return per-band `(cellId: Long, Option[T])` — `None` marks a covered-but-empty cell
      *         (only under `complete` coverage with `emptyValue == None`)
      */
    def execute[T](
        ds: Dataset,
        resolution: Int,
        coverage: String,
        assignment: String,
        fAgg: mutable.ArrayBuffer[Double] => T,
        fAggW: mutable.ArrayBuffer[(Double, Double)] => T,
        emptyValue: Option[T]
    ): Array[Array[(Long, Option[T])]] =
        RasterToGridGeneric.execute(H3, ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue)
            .map(_.map { case (c, v) => (c.asInstanceOf[Long], v) })

    /** Runs an H3 raster→grid `execute` over the tile in `row` and packs the result into the
      * `Array[Array[Struct(cellID, measure)]]` return shape. A `None` measure (covered-but-empty
      * cell) is emitted as SQL `null`; a `Some(v)` as `v`. The measure StructField is nullable.
      */
    def eval[T](
        row: InternalRow,
        resolution: Int,
        coverage: String,
        assignment: String,
        conf: UTF8String,
        rdt: DataType,
        execute: (Dataset, Int, String, String) => Array[Array[(Long, Option[T])]]
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
                    InternalRow.fromSeq(Seq(cellId, m))
                }
              )
          )
        )
    }

}
