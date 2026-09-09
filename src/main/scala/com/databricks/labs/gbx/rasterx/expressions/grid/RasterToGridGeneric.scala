package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.gridx.grid.GridSystem
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.{BoundingBox, GridOverlap}
import org.gdal.gdal.Dataset
import org.gdal.osr.{SpatialReference, osrConstants}

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

    /** Run pixel aggregation over `ds` for `grid`. Convenience overload: accepts all cells.
      * See the 5-param variant for the BNG use-case (where `BNG.isValid` must be passed).
      *
      * @param grid       the grid system supplying CRS, per-pixel cell index, and cell render
      * @param ds         source raster (any CRS; reprojected internally if needed)
      * @param resolution grid resolution
      * @param fAgg       reduces per-cell pixel values to an aggregated `T`
      * @return per-band array of `(cellKey, T)` pairs, where `cellKey` is
      *         `grid.renderCellId(cellID)` (Long for H3/Quadbin, String for BNG)
      */
    def execute[T](
        grid: GridSystem,
        ds: Dataset,
        resolution: Int,
        fAgg: mutable.ArrayBuffer[Double] => T
    ): Array[Array[(Any, T)]] =
        execute(grid, ds, resolution, fAgg, _ => true)

    /** Run pixel aggregation over `ds` for `grid` with explicit per-cell validity.
      *
      * @param grid        the grid system supplying CRS, per-pixel cell index, and cell render
      * @param ds          source raster (any CRS; reprojected internally if needed)
      * @param resolution  grid resolution
      * @param fAgg        reduces per-cell pixel values to an aggregated `T`
      * @param isCellValid per-cell validity predicate (use `BNG.isValid` for BNG, `_ => true` otherwise)
      * @return per-band array of `(cellKey, T)` pairs, where `cellKey` is
      *         `grid.renderCellId(cellID)` (Long for H3/Quadbin, String for BNG)
      */
    def execute[T](
        grid: GridSystem,
        ds: Dataset,
        resolution: Int,
        fAgg: mutable.ArrayBuffer[Double] => T,
        isCellValid: Long => Boolean
    ): Array[Array[(Any, T)]] = {
        val (workDs, reprojected) = GridReprojection.toGridCrs(ds, grid.crsSrid)
        try executeOn(grid, workDs, resolution, fAgg, isCellValid)
        finally if (reprojected) RasterDriver.releaseDataset(workDs)
    }

    /** Run pixel aggregation over `ds` for `grid` with explicit coverage, assignment, and
      * per-cell validity params. This is the ONLY overload that defines a default argument
      * (`isCellValid`) — Scala 2 permits at most one overloaded variant to have defaults.
      *
      * @param grid        the grid system supplying CRS, per-pixel cell index, and cell render
      * @param ds          source raster (any CRS; reprojected internally if needed)
      * @param resolution  grid resolution
      * @param coverage    `"sparse"` — emit only cells with pixels (Stage-1 behaviour);
      *                    `"complete"` — additionally emit cells that overlap the raster bbox
      *                    but have no pixel centroids, using `emptyValue` as their measure
      * @param assignment  `"centroid"` — bin each valid pixel to the cell containing its centroid
      *                    (existing path). `"covering"` — not yet implemented (throws).
      * @param fAgg        reduces per-cell pixel values to `T` (centroid path)
      * @param fAggW       reduces per-cell `(value, areaWeight)` pairs to `T` (covering path —
      *                    accepted here but only used in Task 2; currently unused)
      * @param emptyValue  measure for cells added by `complete` coverage that have no pixels
      * @param isCellValid per-cell validity predicate; default accepts all cells (use `BNG.isValid` for BNG)
      * @return per-band array of `(cellKey, Option[T])` pairs:
      *         `Some(fAgg(buf))` for data cells; `emptyValue` for covered-but-empty cells
      */
    def execute[T](
        grid: GridSystem,
        ds: Dataset,
        resolution: Int,
        coverage: String,
        assignment: String,
        fAgg: mutable.ArrayBuffer[Double] => T,
        fAggW: mutable.ArrayBuffer[(Double, Double)] => T,
        emptyValue: Option[T],
        isCellValid: Long => Boolean = _ => true
    ): Array[Array[(Any, Option[T])]] = {
        require(Set("sparse", "complete").contains(coverage),
            s"coverage must be 'sparse' or 'complete'; got '$coverage'")
        if (assignment == "covering")
            throw new IllegalArgumentException("covering assignment implemented in Task 2")
        require(assignment == "centroid",
            s"assignment must be 'centroid' or 'covering'; got '$assignment'")

        val (workDs, reprojected) = GridReprojection.toGridCrs(ds, grid.crsSrid)
        try {
            // Run the existing centroid accumulation — numerics unchanged from Stage 1.
            val sparseOut = executeOn(grid, workDs, resolution, fAgg, isCellValid)

            if (coverage == "sparse") {
                // Wrap T in Some. For sparse output every cell has a pixel value, so
                // emptyValue is never needed and the flattened result is byte-identical
                // to Stage 1 after Option unwrapping.
                sparseOut.map(_.map { case (c, v) => (c, Option(v)) })
            } else {
                // coverage == "complete": find cells that overlap the raster bbox but
                // received no pixel centroids, and emit them with emptyValue.
                val gridSr = buildGridSR(grid.crsSrid)
                val bboxGeom = BoundingBox.bbox(workDs, gridSr)
                val candidates = grid.coveringCandidateCells(bboxGeom, resolution)

                sparseOut.map { band =>
                    // Keys of cells already populated by the centroid loop.
                    val sparseRendered: Set[Any] = band.map(_._1).toSet

                    // Cells that overlap the bbox but have no pixels: keep only those that
                    // (a) pass the validity guard, (b) have positive-area overlap with the
                    // bbox (same keep-test used by RasterTessellate's covering path), and
                    // (c) are not already keyed in the sparse output.
                    val extraCells: Array[(Any, Option[T])] = candidates
                        .filter { c =>
                            !sparseRendered.contains(grid.renderCellId(c)) &&
                            isCellValid(c) &&
                            GridOverlap.hasPositiveAreaOverlap(grid.cellIdToGeometry(c), bboxGeom)
                        }
                        .map { c => (grid.renderCellId(c): Any, emptyValue) }
                        .toArray

                    band.map { case (c, v) => (c, Option(v)): (Any, Option[T]) } ++ extraCells
                }
            }
        } finally {
            if (reprojected) RasterDriver.releaseDataset(workDs)
        }
    }

    /** Builds a SpatialReference for the given EPSG code with traditional (easting/northing
      * before northing/easting) axis order, matching the convention used by [[BoundingBox]] and
      * [[com.databricks.labs.gbx.rasterx.operations.RasterTessellate]].
      */
    private def buildGridSR(crsSrid: Int): SpatialReference = {
        val sr = new SpatialReference()
        sr.ImportFromEPSG(crsSrid)
        sr.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        sr
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
