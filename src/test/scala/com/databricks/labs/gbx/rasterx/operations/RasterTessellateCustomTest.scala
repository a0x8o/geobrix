package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
  * Tests for [[RasterTessellate.tessellateCustomIter]] — the Long-keyed custom-grid tessellate
  * wrapper. Exercises both the centroid and covering paths at a custom SRID (27700), and the
  * R3 up-front bounds-contain-extent guard.
  */
class RasterTessellateCustomTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    private val conf = GridConf(
        boundXMin     = 0,
        boundXMax     = 1000000,
        boundYMin     = 0,
        boundYMax     = 1000000,
        cellSplits    = 2,
        rootCellSizeX = 100000,
        rootCellSizeY = 100000,
        crsID         = Some(27700)
    )
    private val grid = CustomGridSystem(conf)

    /** A 32x32 EPSG:27700 Float32 raster filled with 42.0; extent 490_000–510_000 E/N. */
    private def raster27700(): Dataset = {
        val (minX, minY) = (490000.0, 490000.0)
        val (w, h)       = (20000.0, 20000.0)
        val size         = 32
        val path = s"/vsimem/custom_tess_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val drv  = gdal.GetDriverByName("GTiff")
        val d = drv.Create(path, size, size, 1, org.gdal.gdalconst.gdalconstConstants.GDT_Float32)
        d.SetGeoTransform(Array(minX, w / size, 0.0, minY + h, 0.0, -(h / size)))
        val srs = new org.gdal.osr.SpatialReference()
        srs.ImportFromEPSG(27700)
        d.SetProjection(srs.ExportToWkt())
        srs.delete()
        val band = d.GetRasterBand(1)
        band.SetNoDataValue(-9999.0)
        band.WriteRaster(0, 0, size, size, Array.fill[Double](size * size)(42.0))
        band.FlushCache(); d.FlushCache(); band.delete()
        d
    }

    private def drain(iter: Iterator[(Long, Dataset, Map[String, String])]): List[(Long, Dataset, Map[String, String])] = {
        val cells = try iter.toList finally iter match {
            case ac: AutoCloseable => ac.close()
            case _                 =>
        }
        cells.foreach { case (_, chip, _) => RasterDriver.releaseDataset(chip) }
        cells
    }

    test("centroid tessellation over a custom grid at 27700 emits Long-keyed chips") {
        val ds = raster27700()
        val cells = drain(
            RasterTessellate.tessellateCustomIter(grid, ds, Map.empty, resolution = 3,
                assignment = "centroid", coverage = "sparse"))
        RasterDriver.releaseDataset(ds)

        cells should not be empty
        cells.foreach { case (k, _, _) => k shouldBe a[java.lang.Long] }
    }

    test("covering tessellation over a custom grid at 27700 emits Long-keyed chips") {
        val ds = raster27700()
        val cells = drain(
            RasterTessellate.tessellateCustomIter(grid, ds, Map.empty, resolution = 3,
                assignment = "covering", coverage = "complete"))
        RasterDriver.releaseDataset(ds)

        // Requires the custom-grid SR (27700) in the generic tessellate path: with a WGS84 SR the
        // raster bbox and the 27700 cell geometries would never overlap and the set would be empty.
        cells should not be empty
        cells.foreach { case (k, _, _) => k shouldBe a[java.lang.Long] }
    }

    test("R3: a grid whose bounds do NOT contain the raster extent fails UP FRONT") {
        val tinyGrid = CustomGridSystem(conf.copy(boundXMax = 15000, boundYMax = 15000))
        val ds = raster27700()
        val ex = intercept[IllegalArgumentException] {
            RasterTessellate.tessellateCustomIter(tinyGrid, ds, Map.empty, resolution = 3,
                assignment = "centroid", coverage = "sparse").toList
        }
        RasterDriver.releaseDataset(ds)
        ex.getMessage.toLowerCase should include("bound")
    }
}
