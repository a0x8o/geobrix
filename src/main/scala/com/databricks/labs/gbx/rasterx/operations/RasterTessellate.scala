package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.gridx.grid.{BNG, GridSystem, H3, Quadbin}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.GDALWarp
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.Dataset
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.{CoordinateTransformation, SpatialReference}
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Tessellates a raster into grid cells: clips by cell geometry and yields (cellId, Dataset, metadata) per cell. */
object RasterTessellate {

    /** Supported tessellation modes. `covering` (default) keeps every cell whose polygon overlaps the
      * raster bbox (chips may share pixels). `centroid` single-assigns each valid pixel to the one cell
      * whose polygon contains its centroid (chips partition the valid pixels). */
    val Modes: Set[String] = Set("covering", "centroid")

    /**
      * Covering keep-test shared by all three grids (H3, quadbin, BNG): a cell is emitted iff its geometry
      * has POSITIVE-AREA overlap with the raster bbox — mere boundary touch (a shared edge line or corner
      * point) is NOT enough. On a grid-aligned tile (raster edges land exactly on cell boundaries) a fringe
      * cell just outside the data shares only a 1-D boundary with the raster: `intersects == true` but
      * `intersection(bbox).getArea == 0.0` and it holds ZERO source pixels, so clipping it yields a spurious
      * empty all-NoData chip. This test drops those (matches the light tier). It KEEPS cells with real areal
      * overlap even if they clip to all-NoData (e.g. a cloud hole or the raster's own NoData) — those fill
      * their position in covering mode and must not be dropped, else white gaps are punched into the mosaic.
      */
    private def hasPositiveAreaOverlap(cellGeom: Geometry, bbox: Geometry): Boolean = {
        if (!cellGeom.intersects(bbox)) return false
        val inter = cellGeom.intersection(bbox)
        inter != null && !inter.isEmpty && inter.getArea > 0.0
    }

    /**
      * Clips ds to the H3 cell geometry and returns (cellId, clipped Dataset, metadata); returns null if the
      * cell hexagon does NOT geometrically overlap the raster bbox.
      *
      * The covering set is defined geometrically: keep the cell iff its H3 hexagon (WGS84, same CRS as `bbox`)
      * intersects the raster bbox. This replaces an earlier nodata-mask keep-test (`RasterAccessors.isEmpty`
      * on the bbox-snapped warp), which over-included a fringe of cells whose hexagons sit just outside the
      * raster (zero geometric overlap). Matches the light tier's `contain='overlap'` covering set.
      */
    def getTile(
        ds: Dataset,
        options: Map[String, String],
        cell: Long,
        bbox: Geometry
    ): (Long, Dataset, Map[String, String]) = {
        val cellGeom = H3.cellIdToGeometry(cell)
        if (!hasPositiveAreaOverlap(cellGeom, bbox)) return null
        val (resDs, resMtd) = ClipToGeom.clip(ds, options, cellGeom, GDAL.WSG84)
        if (resDs == null) return null
        resDs.SetMetadataItem("RASTERX_CELL_ID", cell.toString)
        resDs.FlushCache()
        (cell, resDs, resMtd)
    }

    /**
      * Clips ds to the quadbin cell geometry and returns (cellId, clipped Dataset, metadata); returns null if the
      * cell tile does NOT geometrically overlap the raster bbox. Clone of [[getTile]] for quadbin: the cell polygon
      * is built from `Quadbin.cellBbox` (EPSG:4326 lon/lat, same CRS as `bbox`) and the same intersect keep-test /
      * clip is applied.
      */
    def getQuadbinTile(
        ds: Dataset,
        options: Map[String, String],
        cell: Long,
        bbox: Geometry
    ): (Long, Dataset, Map[String, String]) = {
        val cellGeom = quadbinCellGeometry(cell)
        if (!hasPositiveAreaOverlap(cellGeom, bbox)) return null
        val (resDs, resMtd) = ClipToGeom.clip(ds, options, cellGeom, GDAL.WSG84)
        if (resDs == null) return null
        resDs.SetMetadataItem("RASTERX_CELL_ID", cell.toString)
        resDs.FlushCache()
        (cell, resDs, resMtd)
    }

    /** Quadbin cell -> JTS polygon (EPSG:4326, SRID 4326), built from `Quadbin.cellBbox` = (lonMin,latMin,lonMax,latMax). */
    private def quadbinCellGeometry(cell: Long): Geometry = {
        val (lonMin, latMin, lonMax, latMax) = Quadbin.cellBbox(cell)
        val geom = JTS.polygonFromXYs(
          Array((lonMin, latMin), (lonMax, latMin), (lonMax, latMax), (lonMin, latMax), (lonMin, latMin))
        )
        geom.setSRID(4326)
        geom
    }

    /**
      * Clips ds to the BNG cell geometry and returns (cellId string, clipped Dataset, metadata); returns null
      * if the cell polygon does NOT geometrically overlap the raster bbox, or the cell is outside GB. Clone of
      * [[getTile]] / [[getQuadbinTile]] for BNG: the cell polygon is built from `BNG.cellIdToGeometry` (EPSG:27700,
      * same CRS as `bbox`), out-of-GB cells are dropped via `BNG.isValid`, and the clip targets the 27700 SRS.
      * `ds` is assumed already reprojected to EPSG:27700 by the caller.
      */
    def getBngTile(
        ds: Dataset,
        options: Map[String, String],
        cell: Long,
        bbox: Geometry
    ): (String, Dataset, Map[String, String]) = {
        if (!BNG.isValid(cell)) return null
        val cellGeom = BNG.cellIdToGeometry(cell)
        if (!hasPositiveAreaOverlap(cellGeom, bbox)) return null
        val (resDs, resMtd) = ClipToGeom.clip(ds, options, cellGeom, BngSR)
        if (resDs == null) return null
        val cellStr = BNG.format(cell)
        resDs.SetMetadataItem("RASTERX_CELL_ID", cellStr)
        resDs.FlushCache()
        (cellStr, resDs, resMtd)
    }

    // ------------------------------------------------------------------------------------------------
    // BNG-specific helpers shared by the generic tessellate path.
    // ------------------------------------------------------------------------------------------------

    /** EPSG:27700 (British National Grid) spatial reference, traditional (easting, northing) axis order. */
    private val BngSR: SpatialReference = {
        val sr = new SpatialReference()
        sr.ImportFromEPSG(27700)
        sr.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        sr
    }

    /**
      * Reproject `ds` to EPSG:27700 (nearest-neighbour) unless it is already 27700; returns
      * `(workDs, reprojected)`. When `reprojected` is true the caller owns `workDs` and must release it.
      * Mirrors `RST_BNG_RasterToGrid`'s warp-up-front behaviour (BNG has no lon/lat input path).
      */
    private def warpToBng(ds: Dataset): (Dataset, Boolean) = {
        val srcWkt = ds.GetProjection()
        val alreadyBng = srcWkt != null && srcWkt.nonEmpty && {
            val s = new SpatialReference()
            s.ImportFromWkt(srcWkt)
            val same = s.IsSame(BngSR) == 1
            s.delete()
            same
        }
        if (alreadyBng) (ds, false)
        else {
            val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
            val driver = ds.GetDriver()
            val extension = GDAL.getExtension(driver.getShortName)
            val resultPath = s"/vsimem/raster_bng_tess_$uuid.$extension"
            val (result, _) = GDALWarp.executeWarp(
              resultPath,
              Array(ds),
              Map.empty[String, String],
              command = "gdalwarp -t_srs EPSG:27700 -r near"
            )
            (result, true)
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Generic tessellation over GridSystem.
    // ------------------------------------------------------------------------------------------------

    /** Returns the native spatial reference for the grid (the CRS its cell geometries live in). */
    private def srForGrid(grid: GridSystem): SpatialReference = grid.crsSrid match {
        case 27700 => BngSR
        case _     => GDAL.WSG84
    }

    /** Validity guard: BNG rejects out-of-GB cells; all other grids accept every cell returned by pointToCellID. */
    private def isCellValid(grid: GridSystem, cellId: Long): Boolean =
        if (grid.crsSrid == 27700) BNG.isValid(cellId) else true

    /**
      * Generic tessellation of a raster over any [[GridSystem]]. The per-grid enumeration differences
      * are captured in [[GridSystem.coveringCandidateCells]] (buffered polyfill for H3/BNG; raw bbox
      * lookup for quadbin), so the shared clip+keep-test logic here is grid-agnostic.
      *
      * Returns an `Iterator[(cellKey, Dataset, metadata)]` where `cellKey` is a `Long` for H3 and
      * quadbin, and a `String` for BNG (via [[GridSystem.renderCellId]]). The three named wrapper
      * methods ([[tessellateH3Iter]], [[tessellateQuadbinIter]], [[tessellateBngIter]]) delegate here
      * and cast the result to their specific return type.
      *
      * Caller must release each emitted Dataset. The iterator is AutoCloseable and releases the working
      * dataset when exhausted or explicitly closed.
      */
    def tessellate(
        grid: GridSystem,
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        mode: String = "covering"
    ): Iterator[(Any, Dataset, Map[String, String])] = {
        require(
          Modes.contains(mode),
          s"${grid.name.toLowerCase}_tessellate mode must be one of ${Modes.mkString(", ")}; got '$mode'"
        )
        if (mode == "centroid") tessellateGenericCentroidIter(grid, ds, options, resolution)
        else tessellateGenericCoveringIter(grid, ds, options, resolution)
    }

    /**
      * Generic covering tessellation. Candidate cells come from [[GridSystem.coveringCandidateCells]],
      * which encodes each grid's buffering strategy (H3/BNG buffer; quadbin does not). The shared
      * positive-area keep-test and [[ClipToGeom.clip]] are then applied identically for every grid.
      */
    private def tessellateGenericCoveringIter(
        grid: GridSystem,
        ds: Dataset,
        options: Map[String, String],
        resolution: Int
    ): Iterator[(Any, Dataset, Map[String, String])] = {
        val gridSR = srForGrid(grid)
        // BNG requires a pre-warp to EPSG:27700; 4326-native grids (H3/quadbin) use ds as-is.
        val (workDs, reprojected) = if (grid.crsSrid == 27700) warpToBng(ds) else (ds, false)
        val bbox = BoundingBox.bbox(workDs, gridSR)
        val cells = grid.coveringCandidateCells(bbox, resolution).toArray

        new Iterator[(Any, Dataset, Map[String, String])] with AutoCloseable {
            private var closed   = false
            private var fetched  = false
            private var _ds      = workDs
            private var cc       = 0
            private var nextTile: (Any, Dataset, Map[String, String]) = _

            private def advance(): Unit = {
                fetched = true
                nextTile = null
                while (cc < cells.length && nextTile == null) {
                    val cellId = cells(cc)
                    cc += 1
                    val cellGeom = grid.cellIdToGeometry(cellId)
                    if (hasPositiveAreaOverlap(cellGeom, bbox)) {
                        val (resDs, resMtd) = ClipToGeom.clip(_ds, options, cellGeom, gridSR)
                        if (resDs != null) {
                            val rendered = grid.renderCellId(cellId)
                            resDs.SetMetadataItem("RASTERX_CELL_ID", rendered.toString)
                            resDs.FlushCache()
                            nextTile = (rendered, resDs, resMtd)
                        }
                    }
                }
                if (cc >= cells.length && nextTile == null) close()
            }

            override def hasNext: Boolean = {
                if (!fetched && !closed) advance()
                !closed && nextTile != null
            }

            override def next(): (Any, Dataset, Map[String, String]) = {
                if (!fetched && !closed) advance()
                fetched = false
                nextTile
            }

            override def close(): Unit = {
                if (!closed) {
                    closed = true
                    if (reprojected) RasterDriver.releaseDataset(_ds) else RasterAccessors.unlink(_ds)
                    _ds = null
                }
            }
        }
    }

    /**
      * Generic centroid tessellation. Reads all pixel values up front, assigns each valid pixel to
      * the cell returned by [[GridSystem.pointToCellID]], then emits one chip per cell holding only
      * its assigned pixels (the rest set to nodata). BNG warps the raster to EPSG:27700 before
      * reading so that pixel coordinates are already in the grid's native CRS. 4326-native grids
      * (H3/quadbin) optionally reproject pixel centroids per-pixel when the raster CRS differs
      * from 4326.
      */
    private def tessellateGenericCentroidIter(
        grid: GridSystem,
        ds: Dataset,
        options: Map[String, String],
        resolution: Int
    ): Iterator[(Any, Dataset, Map[String, String])] = {
        // BNG requires a pre-warp so pixel coordinates are in EPSG:27700 (the grid's native CRS).
        val (workDs, reprojected) = if (grid.crsSrid == 27700) warpToBng(ds) else (ds, false)

        val xSize     = workDs.getRasterXSize
        val ySize     = workDs.getRasterYSize
        val nPix      = xSize * ySize
        val bandCount = workDs.getRasterCount
        val dtype     = workDs.GetRasterBand(1).getDataType
        val gt        = workDs.GetGeoTransform

        // Capture projection WKT and source path before potentially releasing workDs (BNG case).
        val projWkt = workDs.GetProjection()
        val sourcePath = Option(workDs.GetFileList())
            .flatMap(_.asScala.headOption.map(_.toString))
            .getOrElse("unknown source path")

        // For 4326-native grids: set up per-pixel reprojection if the raster CRS is not already 4326.
        // For 27700-native grids (BNG): no per-pixel reprojection — the warp already puts coords in 27700.
        val tf: CoordinateTransformation = if (grid.crsSrid != 27700) {
            val srcSR = workDs.GetSpatialRef
            val needReproject = srcSR != null && srcSR.IsSame(GDAL.WSG84) != 1
            if (needReproject) new CoordinateTransformation(srcSR, GDAL.WSG84) else null
        } else null

        // Read every band's values + mask once; assign each valid pixel (by flat index) to its cell.
        val bandVals   = new Array[Array[Double]](bandCount)
        val bandMask   = new Array[Array[Byte]](bandCount)
        val bandNoData = new Array[Double](bandCount)
        var bi = 0
        while (bi < bandCount) {
            val band = workDs.GetRasterBand(bi + 1)
            val vals = new Array[Double](nPix)
            val mask = new Array[Byte](nPix)
            band.ReadRaster(0, 0, xSize, ySize, vals)
            band.GetMaskBand().ReadRaster(0, 0, xSize, ySize, mask)
            bandVals(bi) = vals
            bandMask(bi) = mask
            val nd = new Array[java.lang.Double](1)
            band.GetNoDataValue(nd)
            bandNoData(bi) = if (nd(0) != null) nd(0).doubleValue() else sentinelNoData(dtype)
            bi += 1
        }

        // cell -> flat pixel indices; all valid pixels assigned to exactly one cell.
        val cellPixels = new mutable.LongMap[mutable.ArrayBuffer[Int]]()
        var y = 0
        var idx = 0
        while (y < ySize) {
            var x = 0
            while (x < xSize) {
                var anyValid = false
                var b = 0
                while (b < bandCount && !anyValid) { if (bandMask(b)(idx) != 0) anyValid = true; b += 1 }
                if (anyValid) {
                    val xOff = 0.5 + x
                    val yOff = 0.5 + y
                    val xGeo = gt(0) + xOff * gt(1) + yOff * gt(2)
                    val yGeo = gt(3) + xOff * gt(4) + yOff * gt(5)
                    val (cx, cy) = if (tf != null) {
                        val p = tf.TransformPoint(xGeo, yGeo)
                        (p(0), p(1))
                    } else (xGeo, yGeo)
                    val cellId = grid.pointToCellID(cx, cy, resolution)
                    if (isCellValid(grid, cellId))
                        cellPixels.getOrElseUpdate(cellId, new mutable.ArrayBuffer[Int]) += idx
                }
                idx += 1
                x += 1
            }
            y += 1
        }

        // For BNG the working dataset is a temporary warp; release it once all pixels are read.
        if (reprojected) RasterDriver.releaseDataset(workDs)

        val gridName = grid.name.toLowerCase
        val cellIter = cellPixels.iterator

        new Iterator[(Any, Dataset, Map[String, String])] with AutoCloseable {
            private var closed = false

            override def hasNext: Boolean = !closed && cellIter.hasNext

            override def next(): (Any, Dataset, Map[String, String]) = {
                val (cellId, pixIdx) = cellIter.next()
                val tile = buildGenericCentroidChip(
                  grid, gridName, projWkt, sourcePath, options, cellId, pixIdx,
                  xSize, ySize, bandCount, dtype, gt, bandVals, bandNoData
                )
                if (!cellIter.hasNext) close()
                tile
            }

            override def close(): Unit = { closed = true }
        }
    }

    /** Builds one full-extent chip for centroid tessellation: every pixel starts as nodata, then the
      * `pixIdx` pixels are restored to their source values.  Works for H3, quadbin, and BNG because the
      * grid-specific rendering ([[GridSystem.renderCellId]]) and name are supplied as parameters. */
    private def buildGenericCentroidChip(
        grid: GridSystem,
        gridName: String,
        projWkt: String,
        sourcePath: String,
        options: Map[String, String],
        cellId: Long,
        pixIdx: mutable.ArrayBuffer[Int],
        xSize: Int,
        ySize: Int,
        bandCount: Int,
        dtype: Int,
        gt: Array[Double],
        bandVals: Array[Array[Double]],
        bandNoData: Array[Double]
    ): (Any, Dataset, Map[String, String]) = {
        val rendered   = grid.renderCellId(cellId)
        val uuid       = java.util.UUID.randomUUID().toString.replace("-", "_")
        val rasterPath = s"/vsimem/${gridName}_centroid_${cellId}_$uuid.tif"
        val drv = GDALManager.gtiffDriver()
        val out = drv.Create(rasterPath, xSize, ySize, bandCount, dtype)
        out.SetGeoTransform(gt)
        out.SetProjection(projWkt)

        val nPix = xSize * ySize
        var b = 0
        while (b < bandCount) {
            val nd  = bandNoData(b)
            val src = bandVals(b)
            val buf = new Array[Double](nPix)
            java.util.Arrays.fill(buf, nd) // blank everything to nodata...
            var i = 0
            while (i < pixIdx.length) { val p = pixIdx(i); buf(p) = src(p); i += 1 } // ...then restore assigned pixels
            val db = out.GetRasterBand(b + 1)
            db.SetNoDataValue(nd)
            // Implicit Float64 buffer overload: GDAL converts double[] to the band's native dtype on write.
            db.WriteRaster(0, 0, xSize, ySize, buf)
            b += 1
        }
        out.SetMetadataItem("RASTERX_CELL_ID", rendered.toString)
        out.FlushCache()

        val meta = Map(
          "path"         -> rasterPath,
          "parentPath"   -> options.getOrElse("path", sourcePath),
          "driver"       -> "GTiff",
          "format"       -> "GTiff",
          "last_command" -> s"${gridName}_centroid_tessellate cell=$rendered",
          "last_error"   -> "",
          "all_parents"  -> s"$sourcePath;${options.getOrElse("all_parents", "")}",
          "size"         -> "-1",
          "compression"  -> options.getOrElse("compression", "DEFLATE"),
          "isZipped"     -> "false",
          "isSubset"     -> "false"
        )
        (rendered, out, meta)
    }

    // ------------------------------------------------------------------------------------------------
    // Named wrappers — thin delegates to `tessellate`, preserving the exact per-grid return types
    // so all current callers (RST_H3_Tessellate, RST_Quadbin_Tessellate, RST_BNG_Tessellate) and
    // tests compile and run unchanged.
    // ------------------------------------------------------------------------------------------------

    /**
      * Iterator of (cellId Long, Dataset, metadata) per emitted H3 cell at resolution. Caller must release each
      * Dataset; iterator is AutoCloseable.
      *
      *  - `covering` (default): one chip per cell whose hexagon overlaps the raster bbox.
      *  - `centroid`: pixel-centroid single-assignment — each valid source pixel lands in exactly one chip.
      */
    def tessellateH3Iter(
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        mode: String = "covering"
    ): Iterator[(Long, Dataset, Map[String, String])] =
        tessellate(H3, ds, options, resolution, mode)
            .asInstanceOf[Iterator[(Long, Dataset, Map[String, String])]]

    /**
      * Iterator of (cellId Long, Dataset, metadata) per emitted quadbin cell at `resolution` (zoom z).
      * Caller must release each Dataset; iterator is AutoCloseable. Parallel to [[tessellateH3Iter]].
      */
    def tessellateQuadbinIter(
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        mode: String = "covering"
    ): Iterator[(Long, Dataset, Map[String, String])] =
        tessellate(Quadbin, ds, options, resolution, mode)
            .asInstanceOf[Iterator[(Long, Dataset, Map[String, String])]]

    /**
      * Iterator of (BNG cellId String, Dataset, metadata) per emitted BNG cell at `resolution`.
      * Caller must release each Dataset; iterator is AutoCloseable.
      *
      * The raster is reprojected to EPSG:27700 first (skipped if already 27700). Cells are enumerated
      * via [[GridSystem.coveringCandidateCells]] (buffered polyfill) and geometrised via
      * [[GridSystem.cellIdToGeometry]] — the vector `bng_tessellate` codepath is never reached.
      */
    def tessellateBngIter(
        ds: Dataset,
        options: Map[String, String],
        resolution: Int,
        mode: String = "covering"
    ): Iterator[(String, Dataset, Map[String, String])] =
        tessellate(BNG, ds, options, resolution, mode)
            .asInstanceOf[Iterator[(String, Dataset, Map[String, String])]]

    // ------------------------------------------------------------------------------------------------
    // Utility.
    // ------------------------------------------------------------------------------------------------

    /** A nodata sentinel for bands lacking an explicit nodata, by data type (used only to blank unassigned pixels). */
    private def sentinelNoData(dtype: Int): Double = {
        // Float types: NaN is the natural sentinel. Integer types: 0 (chips for centroid mode set it as nodata
        // so the mask treats it as invalid; collisions with real 0-valued data are acceptable for blanking only
        // when no explicit nodata exists, which is rare for the rasters this path serves).
        if (dtype == gdalconstConstants.GDT_Float32 || dtype == gdalconstConstants.GDT_Float64) Double.NaN else 0.0
    }

}
