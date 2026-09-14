package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
  * `gbx_rst_align_to(tile, reference_tile)` — warp a tile onto a reference tile's grid
  * (CRS + extent + pixel size), nearest-neighbour. Checks that the output exactly matches
  * the reference grid (dimensions, geotransform, CRS) across a 4326 -> 27700 warp and back.
  */
class RST_AlignToTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    private def raster(epsg: Int, gt: Array[Double], w: Int, h: Int, fill: Double): Dataset = {
        val path = s"/vsimem/align_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val d = gdal.GetDriverByName("GTiff").Create(path, w, h, 1, gdalconstConstants.GDT_Float64)
        d.SetGeoTransform(gt)
        val srs = new org.gdal.osr.SpatialReference(); srs.ImportFromEPSG(epsg)
        d.SetProjection(srs.ExportToWkt()); srs.delete()
        val band = d.GetRasterBand(1)
        band.WriteRaster(0, 0, w, h, Array.fill[Double](w * h)(fill))
        band.FlushCache(); d.FlushCache(); band.delete()
        d
    }

    private def gtOf(ds: Dataset): Array[Double] = { val g = Array.ofDim[Double](6); ds.GetGeoTransform(g); g }
    private def epsgOf(ds: Dataset): String = {
        val sr = ds.GetSpatialRef
        sr should not be null
        sr.GetAuthorityCode(null)
    }

    // London: BNG easting/northing ~ (530000, 180000) == lon/lat ~ (-0.104, 51.507).
    // Reference grid: EPSG:27700, 1km x 1km window over 10x10 100m pixels.
    private def refGt = Array(530000.0, 100.0, 0.0, 180000.0, 0.0, -100.0)
    // Source tile in EPSG:4326 covering the same ground, finer pixels.
    private def srcGt = Array(-0.12, 0.002, 0.0, 51.52, 0.0, -0.002)

    test("align warps a 4326 tile onto a 27700 reference: matches dimensions, CRS, geotransform") {
        val src = raster(4326, srcGt, 20, 20, 42.0)
        val ref = raster(27700, refGt, 10, 10, 0.0)
        val (out, _) = RST_AlignTo.execute(src, ref, Map.empty)
        try {
            out should not be null
            out.GetRasterXSize shouldBe 10
            out.GetRasterYSize shouldBe 10
            epsgOf(out) shouldBe "27700"
            val g = gtOf(out); val rg = gtOf(ref)
            g(0) shouldBe rg(0) +- 1e-3   // origin X
            g(3) shouldBe rg(3) +- 1e-3   // origin Y
            g(1) shouldBe rg(1) +- 1e-3   // pixel width
            g(5) shouldBe rg(5) +- 1e-3   // pixel height
        } finally {
            RasterDriver.releaseDataset(out)
            RasterDriver.releaseDataset(src); RasterDriver.releaseDataset(ref)
        }
    }

    test("align round-trips: warping back onto a 4326 reference restores that grid") {
        val src = raster(4326, srcGt, 20, 20, 42.0)
        val ref = raster(27700, refGt, 10, 10, 0.0)
        val (aligned, _) = RST_AlignTo.execute(src, ref, Map.empty)
        val (back, _) = RST_AlignTo.execute(aligned, src, Map.empty)
        try {
            back should not be null
            back.GetRasterXSize shouldBe src.GetRasterXSize
            back.GetRasterYSize shouldBe src.GetRasterYSize
            epsgOf(back) shouldBe "4326"
            val g = gtOf(back); val sg = gtOf(src)
            g(0) shouldBe sg(0) +- 1e-6
            g(3) shouldBe sg(3) +- 1e-6
            g(1) shouldBe sg(1) +- 1e-9
            g(5) shouldBe sg(5) +- 1e-9
        } finally {
            RasterDriver.releaseDataset(back); RasterDriver.releaseDataset(aligned)
            RasterDriver.releaseDataset(src); RasterDriver.releaseDataset(ref)
        }
    }
}
