package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.gridx.grid.GridSystem
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import org.gdal.gdal.Dataset

import scala.collection.mutable

/** Generic raster→grid pixel aggregation for any [[GridSystem]].
  *
  * Reprojects `ds` to the grid's native CRS (via [[GridReprojection]]), bins each valid
  * (non-NoData) pixel centroid to a cell id with `grid.pointToCellID`, optionally filters
  * cells with `isCellValid` (for grids like BNG that have coordinate-extent constraints),
  * accumulates pixel values per cell, and applies `fAgg`. Cells with no valid pixels are
  * dropped (sparse output). The caller owns `ds`; any internally reprojected dataset is
  * released before return.
  *
  * Output cell keys are produced by [[GridSystem.renderCellId]]: Long for H3/Quadbin,
  * String for BNG.
  */
object RasterToGridGeneric {

    /** Run pixel aggregation over `ds` for `grid`.
      *
      * @param grid        the grid system supplying CRS, per-pixel cell index, and cell render
      * @param ds          source raster (any CRS; reprojected internally if needed)
      * @param resolution  grid resolution
      * @param fAgg        reduces per-cell pixel values to an aggregated `T`
      * @param isCellValid optional per-cell validity predicate (default: accept all)
      * @return per-band array of `(cellKey, T)` pairs, where `cellKey` is
      *         `grid.renderCellId(cellID)` (Long for H3/Quadbin, String for BNG)
      */
    def execute[T](
        grid: GridSystem,
        ds: Dataset,
        resolution: Int,
        fAgg: mutable.ArrayBuffer[Double] => T,
        isCellValid: Long => Boolean = _ => true
    ): Array[Array[(Any, T)]] = {
        val (workDs, reprojected) = GridReprojection.toGridCrs(ds, grid.crsSrid)
        try executeOn(grid, workDs, resolution, fAgg, isCellValid)
        finally if (reprojected) RasterDriver.releaseDataset(workDs)
    }

    private def executeOn[T](
        grid: GridSystem,
        ds: Dataset,
        resolution: Int,
        fAgg: mutable.ArrayBuffer[Double] => T,
        isCellValid: Long => Boolean
    ): Array[Array[(Any, T)]] = {

        val gt     = ds.GetGeoTransform
        val xSize  = ds.getRasterXSize
        val ySize  = ds.getRasterYSize
        val nPix   = xSize * ySize
        val bands  = ds.getRasterCount

        val bandBuf = new Array[Double](nPix)
        val maskBuf = new Array[Byte](nPix)

        (1 to bands).iterator.map { bi =>
            val b = ds.GetRasterBand(bi)
            val m = b.GetMaskBand()
            b.ReadRaster(0, 0, xSize, ySize, bandBuf)
            m.ReadRaster(0, 0, xSize, ySize, maskBuf)

            // Count valid pixels for LongMap pre-sizing.
            var valid = 0; var i = 0
            while (i < nPix) { if (maskBuf(i) != 0) valid += 1; i += 1 }

            val acc = new mutable.LongMap[mutable.ArrayBuffer[Double]](valid)
            var y = 0; var idx = 0
            while (y < ySize) {
                var x = 0
                while (x < xSize) {
                    if (maskBuf(idx) != 0) {
                        val xOff = 0.5 + x
                        val yOff = 0.5 + y
                        val cell = grid.pointToCellID(
                            gt(0) + xOff * gt(1) + yOff * gt(2),
                            gt(3) + xOff * gt(4) + yOff * gt(5),
                            resolution
                        )
                        if (isCellValid(cell)) {
                            val buf = acc.getOrElseUpdate(cell, new mutable.ArrayBuffer)
                            buf += bandBuf(idx)
                        }
                    }
                    idx += 1; x += 1
                }
                y += 1
            }

            val out = new Array[(Any, T)](acc.size)
            var j = 0
            acc.foreach { case (cell, buf) => out(j) = (grid.renderCellId(cell), fAgg(buf)); j += 1 }
            out
        }.toArray
    }
}
