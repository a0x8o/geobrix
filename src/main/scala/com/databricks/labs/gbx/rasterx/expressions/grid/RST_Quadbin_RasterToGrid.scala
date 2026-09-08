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
  * but delegates per-pixel cell math to [[Quadbin.pointToCell]] (CARTO quadbin v0).
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

    def eval[T](
        row: InternalRow,
        resolution: Int,
        conf: UTF8String,
        rdt: DataType,
        execute: (Dataset, Int) => Array[Array[(Long, T)]]
    ): ArrayData = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val result = execute(ds, resolution)
        RasterDriver.releaseDataset(ds)
        ArrayData.toArrayData(
          result.map(band =>
              ArrayData.toArrayData(
                band.map { case (cellId, measure) => InternalRow.fromSeq(Seq(cellId, measure)) }
              )
          )
        )
    }

}
