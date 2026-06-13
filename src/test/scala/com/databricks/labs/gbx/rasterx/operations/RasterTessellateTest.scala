package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.gridx.grid.H3
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver}
import org.gdal.gdal.{Dataset, gdal}
import org.locationtech.jts.geom.Geometry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
  * Covers the H3 covering path of [[RasterTessellate.tessellateH3Iter]]: the emitted cell set must be
  * exactly the cells whose H3 hexagon geometrically overlaps the raster bbox (no disjoint fringe).
  */
class RasterTessellateTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        val tifPath = this.getClass
            .getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
            .toString
            .replace("file:/", "/")
        ds = gdal.Open(tifPath)
    }

    override def afterAll(): Unit = {
        if (ds != null) ds.delete()
    }

    /** Collects the emitted cell IDs from the covering tessellation, releasing each chip dataset. */
    private def tessellateCells(resolution: Int): Seq[Long] = {
        val iter = RasterTessellate.tessellateH3Iter(ds, Map.empty, resolution)
        try {
            iter.map { case (cell, resDs, _) =>
                RasterDriver.releaseDataset(resDs)
                cell
            }.toList
        } finally iter match {
            case ac: AutoCloseable => ac.close()
            case _                 =>
        }
    }

    test("tessellateH3Iter covering emits only cells whose hexagon overlaps the raster bbox") {
        // MODIS tile footprint in WGS84 is roughly lon [-85..-71], lat [10..20] (see ClipToGeomTest).
        // Resolution 3 yields a handful of cells including border cells, so the old nodata keep-test
        // over-includes a disjoint fringe just outside the raster.
        val resolution = 3

        // Capture the bbox BEFORE consuming the iterator: tessellateH3Iter.close() unlinks `ds`,
        // after which BoundingBox.bbox(ds, ...) would read a dead dataset and return a degenerate
        // (0 0 ...) polygon.
        val bboxGeom: Geometry = BoundingBox.bbox(ds, GDAL.WSG84)
        bboxGeom.isValid shouldBe true
        bboxGeom.getArea should be > 0.0

        val cells = tessellateCells(resolution)
        cells should not be empty

        val disjoint = cells.filterNot { cell =>
            val hex = H3.cellIdToGeometry(cell)
            hex.intersects(bboxGeom)
        }

        withClue(
          s"${disjoint.length} of ${cells.length} emitted H3 cells are geometrically disjoint from the " +
              s"raster bbox (covering must emit only overlapping hexagons): ${disjoint.take(10).mkString(",")} "
        ) {
            disjoint shouldBe empty
        }
    }

}
