package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.gridx.grid.GridSystem
import com.databricks.labs.gbx.rasterx.operations.OSRTransformGeometry
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference

/** Shared pixel-centroid burn loop for the per-grid `RST_*_RasterizeAgg` UDAFs.
 *
 * For each output pixel, computes its geographic coordinate in the raster's `srid`
 * via the dataset's affine GeoTransform. When `srid` differs from `grid.crsSrid`,
 * each pixel centre is reprojected to the grid's native CRS before the cell
 * membership lookup. Pixels with no entry in `lut` are written as -9999.0 (NoData).
 *
 * The three per-grid aggregators (H3, Quadbin, BNG) delegate their inner burn body
 * to this object, passing their respective [[GridSystem]] singleton. No per-grid
 * filtering or value transformation is applied here: the only grid-specific behaviour
 * is the choice of `pointToCellID` implementation and the `crsSrid` bypass check.
 *
 * '''BNG shortcut''': for BNG, `srid` is always `BNG.crsID` (27700) and
 * `BNG.crsSrid == 27700`, so `needsReproject` is always false and no GDAL
 * spatial-reference objects are allocated on the BNG path.
 */
object RasterizeBurn {

    /** NoData sentinel, matching `cellraster._NODATA` in the lightweight tier. */
    val NoData: Double = -9999.0

    /** Burn a cell-value lookup table into `rasterDs`, writing NoData for absent cells.
     *
     * @param grid       grid system providing `pointToCellID` and `crsSrid`
     * @param lut        cell id to value (Long-keyed; last-wins overlap already resolved
     *                   by the caller sorting `(cellId, value)` before inserting)
     * @param srid       CRS of `rasterDs`; when it differs from `grid.crsSrid`, each
     *                   pixel centre is reprojected to the grid's native CRS before indexing
     * @param resolution grid resolution passed through to `grid.pointToCellID`
     * @param rasterDs   pre-built GDAL in-memory dataset (mutated in-place; the caller
     *                   retains ownership and is responsible for `.delete()`)
     * @param width      raster width in pixels
     * @param height     raster height in pixels
     */
    def burn(
        grid:       GridSystem,
        lut:        scala.collection.mutable.LongMap[Double],
        srid:       Int,
        resolution: Int,
        rasterDs:   Dataset,
        width:      Int,
        height:     Int
    ): Unit = {
        val gt     = rasterDs.GetGeoTransform
        val band   = rasterDs.GetRasterBand(1)
        val rowBuf = new Array[Double](width)

        val needsReproject = srid != grid.crsSrid
        val srcSR: SpatialReference = if (needsReproject) {
            val s = new SpatialReference(); s.ImportFromEPSG(srid); s
        } else null
        val dstSR: SpatialReference = if (needsReproject) {
            val d = new SpatialReference(); d.ImportFromEPSG(grid.crsSrid); d
        } else null

        try {
            var py = 0
            while (py < height) {
                var px = 0
                while (px < width) {
                    val xOffset = 0.5 + px
                    val yOffset = 0.5 + py
                    val xGeo = gt(0) + xOffset * gt(1) + yOffset * gt(2)
                    val yGeo = gt(3) + xOffset * gt(4) + yOffset * gt(5)
                    val (cx, cy) =
                        if (needsReproject) {
                            val pt = JTS.point(
                                new org.locationtech.jts.geom.Coordinate(xGeo, yGeo))
                            val tp = OSRTransformGeometry.transform(pt, srcSR, dstSR)
                            val c  = tp.getCoordinate
                            (c.x, c.y)
                        } else (xGeo, yGeo)
                    val cellId = grid.pointToCellID(cx, cy, resolution)
                    rowBuf(px) = lut.getOrElse(cellId, NoData)
                    px += 1
                }
                band.WriteRaster(0, py, width, 1, rowBuf)
                py += 1
            }
        } finally {
            if (needsReproject) {
                srcSR.delete()
                dstSR.delete()
            }
        }
    }
}
