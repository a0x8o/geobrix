package com.databricks.labs.gbx.gridx.grid

import org.locationtech.jts.geom.Geometry

/**
 * Common interface for GeoBrix discrete grid systems (H3, BNG, quadbin, custom).
 * Instance-based so a custom grid can carry its own configuration; H3/BNG/Quadbin
 * are singleton-backed instances. Defines the members consumed by raster
 * tessellation/aggregation and neighbourhood/formatting paths.
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
   * BEFORE the positive-area keep-test. Grid-specific by necessity: quadbin enumerates
   * the bbox directly (its two-corner tile lookup already includes every overlapping cell);
   * H3 and BNG buffer the bbox because their polyfill is centroid-based and would otherwise
   * miss cells that overlap but whose centroid lies outside. The generic tessellate path
   * applies one shared keep-test to whatever this method returns.
   */
  def coveringCandidateCells(bbox: Geometry, resolution: Int): Seq[Long]
  /**
   * How this grid renders a cell id in raster->grid OUTPUT.
   * H3/quadbin emit the Long; BNG emits its formatted string. Default: the Long.
   */
  def renderCellId(cellID: Long): Any = cellID
}
