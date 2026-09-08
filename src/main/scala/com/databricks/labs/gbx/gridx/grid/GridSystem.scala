package com.databricks.labs.gbx.gridx.grid

import org.locationtech.jts.geom.Geometry

/**
 * Common interface for GeoBrix discrete grid systems (H3, BNG, quadbin, custom).
 * Instance-based: custom carries its GridConf; H3/BNG/Quadbin are singleton-backed
 * instances. Stage 1 defines only the members the raster tessellation/aggregation
 * paths consume; neighbourhood/formatting members are added in Stage 3.
 */
trait GridSystem extends Serializable {
  /** Stable grid name, e.g. "H3", "BNG", "QUADBIN", "CUSTOM". */
  def name: String
  /** SRID the grid's cell geometries are expressed in (4326 for H3/quadbin, 27700 for BNG). */
  def crsSrid: Int
  /** Valid resolution keys for this grid. */
  def resolutions: Set[Int]
  /** Index a point (in the grid's native CRS) to a cell id. */
  def pointToCellID(x: Double, y: Double, resolution: Int): Long
  /** Cell id -> its polygon in `crsSrid`. */
  def cellIdToGeometry(cellID: Long): Geometry
  /** Cells (by id) whose geometry the input geometry covers, at `resolution`. */
  def polyfill(geometry: Geometry, resolution: Int): Seq[Long]
  /**
   * Candidate cells for COVERING tessellation of a raster bbox — the enumeration step
   * BEFORE the positive-area keep-test. Grid-specific by necessity: H3 must buffer the bbox
   * (hex centroids can fall outside a tight bbox while the hex still overlaps) whereas quadbin
   * and BNG (rectangular) enumerate the bbox directly. Each grid lifts its CURRENT enumeration
   * verbatim (Task 0 spike confirmed quadbin's is already `polyfillBbox(env)`); the generic
   * tessellate (Task 8) applies one shared keep-test to whatever this returns.
   */
  def coveringCandidateCells(bbox: Geometry, resolution: Int): Seq[Long]
  /**
   * How this grid renders a cell id in raster->grid OUTPUT.
   * H3/quadbin emit the Long; BNG emits its formatted string. Default: the Long.
   */
  def renderCellId(cellID: Long): Any = cellID
}
