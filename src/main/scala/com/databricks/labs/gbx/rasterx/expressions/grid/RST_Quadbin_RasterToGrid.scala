package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.Quadbin
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.collection.mutable

/** Shared helper for `RST_Quadbin_RasterToGrid*` expressions — mirrors `RST_H3_RasterToGrid`
  * but applies the `MAX_AGG_RESOLUTION` guard before delegating to the generic execute.
  *
  * The geotransform interprets the raster as EPSG:4326 lon/lat; a differently-CRS'd raster
  * is auto-reprojected to 4326 (nearest-neighbour) up front via [[RasterToGridGeneric]],
  * so easting/northing are never read as lon/lat. A CRS-less raster is assumed already-4326.
  *
  * Resolution range: [0, 20]. Capped well below the CARTO v0 max of 26 because the
  * per-band cell count at z>=21 over a continental raster (~10^6) is dominated by GDAL I/O
  * and easily OOMs.
  */
object RST_Quadbin_RasterToGrid {

    /** Maximum quadbin resolution permitted for raster→grid aggregation. */
    val MAX_AGG_RESOLUTION: Int = 20

    /** Stage-2 path: coverage/assignment-aware, nullable-measure output.
      * Applies MAX_AGG_RESOLUTION guard, then delegates to the generic 9-param
      * [[RasterToGridGeneric.execute]] (Quadbin uses the default `isCellValid`, same as H3)
      * and narrows the `Any` cell key to `Long`.
      *
      * @param coverage   `"sparse"` (only cells with pixels) or `"complete"` (also emit
      *                   covered-but-empty cells carrying `emptyValue`)
      * @param assignment `"centroid"` (bin each pixel by its centroid, `fAgg`) or
      *                   `"covering"` (area-weighted distribution, `fAggW`)
      * @param emptyValue measure for covered-but-empty cells added by `complete`
      *
      * `fAgg`/`fAggW` are only ever called on NON-EMPTY buffers — a cell with no pixels never
      * reaches a reducer; it is materialised (under `complete`) as `emptyValue`/`None` instead.
      * Reducers may therefore assume at least one element (e.g. `.min`, `Σw > 0`).
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
    ): Array[Array[(Long, Option[T])]] = {
        require(
          resolution >= 0 && resolution <= MAX_AGG_RESOLUTION,
          s"raster→quadbin: resolution must be in [0, $MAX_AGG_RESOLUTION]; got $resolution"
        )
        RasterToGridGeneric.execute(Quadbin, ds, resolution, coverage, assignment, fAgg, fAggW, emptyValue)
            .map(_.map { case (c, v) => (c.asInstanceOf[Long], v) })
    }

    /** Legacy Stage-1 path: sparse+centroid only, non-Option output.
      * Retained for direct callers (GridRasterParityBaseline, RST_Quadbin*Test, bench).
      */
    def execute[T](
        ds: Dataset,
        resolution: Int,
        fAgg: mutable.ArrayBuffer[Double] => T
    ): Array[Array[(Long, T)]] = {
        require(
          resolution >= 0 && resolution <= MAX_AGG_RESOLUTION,
          s"raster→quadbin: resolution must be in [0, $MAX_AGG_RESOLUTION]; got $resolution"
        )
        RasterToGridGeneric.execute(Quadbin, ds, resolution, fAgg)
            .map(_.map { case (c, v) => (c.asInstanceOf[Long], v) })
    }

    /** Runs a Quadbin raster→grid `execute` over the tile in `row` and packs the result into the
      * `Array[Array[Struct(cellID, measure)]]` return shape. A `None` measure (covered-but-empty
      * cell) is emitted as SQL `null`; a `Some(v)` as `v`. The measure StructField is nullable.
      *
      * Only ONE `eval` overload here to avoid Scala 2 batch-compile type-inference failures
      * (same pattern as RST_BNG_RasterToGrid after Stage-2 migration).
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
