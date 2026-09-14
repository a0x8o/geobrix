package com.databricks.labs.gbx.rasterx.parity

import com.databricks.labs.gbx.gridx.grid.H3
import com.databricks.labs.gbx.rasterx.expressions.grid.{RST_BNG_RasterToGrid, RST_H3_RasterToGridAvg, RST_Quadbin_RasterToGrid, RasterToGridGeneric}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.collection.mutable

/** Golden parity baseline for raster-to-grid functions.
  *
  * These digests are captured against the CURRENT implementation (before any GridSystem-trait
  * refactor) and frozen as constants. Every later refactor task runs this suite to prove
  * behavioral neutrality — the digest must not change.
  *
  * BNG note: the MODIS h10v07 tile is nominally over Southeast Asia, but RST_BNG_RasterToGrid
  * produces a non-empty cell set on it (frozen digest 4962e13f…). Whether those cells represent
  * geographically valid BNG positions is an open question this baseline does not assert — it
  * only freezes whatever the current implementation produces so that any refactor can prove
  * it changed nothing.
  */
class GridRasterParityBaselineTest extends AnyFunSuite with BeforeAndAfterAll {

  private var ds: Dataset = _

  override def beforeAll(): Unit = {
    GDALManager.loadSharedObjects(Iterable.empty[String])
    GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
    gdal.AllRegister()
    val tif = getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
      .toString.replace("file:/", "/")
    ds = gdal.Open(tif)
  }

  override def afterAll(): Unit = if (ds != null) ds.delete()

  test("H3 rastertogridavg digest is stable at res 2") {
    val out = GridRasterParityBaseline.h3Grid(ds, 2)
    GridRasterParityBaseline.digest(out.map(_.map { case (c, v) => (c.toString, v) })) shouldBe
      GridRasterParityBaseline.H3_AVG_RES2
  }

  test("BNG rastertogrid avg digest is stable at resolution 3 (1km)") {
    val out = GridRasterParityBaseline.bngGrid(ds, 3)
    GridRasterParityBaseline.digest(out) shouldBe GridRasterParityBaseline.BNG_AVG_RES3
  }

  test("Quadbin rastertogrid avg digest is stable at resolution 10") {
    val out = GridRasterParityBaseline.quadbinGrid(ds, 10)
    GridRasterParityBaseline.digest(out.map(_.map { case (c, v) => (c.toString, v) })) shouldBe
      GridRasterParityBaseline.QUADBIN_AVG_RES10
  }

  test("sparse+centroid execute preserves H3 avg res-2 digest (Stage-2 gate)") {
    val fAvg: mutable.ArrayBuffer[Double] => Double = b => b.sum / b.length
    // Typed stub required: Scala 2 can't infer `_ => 0.0` for fAggW in an overloaded context.
    val fAggWStub: mutable.ArrayBuffer[(Double, Double)] => Double = _ => 0.0
    val out = RasterToGridGeneric.execute[Double](H3, ds, 2, "sparse", "centroid",
      fAvg, fAggWStub, None)
    val flat = out.map(_.collect { case (c, Some(v)) => (c.toString, v) })
    GridRasterParityBaseline.digest(flat) shouldBe GridRasterParityBaseline.H3_AVG_RES2
  }
}

object GridRasterParityBaseline {

  /** Thin wrapper: H3 average aggregation. Returns per-band `(cellId: Long, avg: Double)`. */
  def h3Grid(ds: Dataset, resolution: Int): Array[Array[(Long, Double)]] =
    RST_H3_RasterToGridAvg.execute(ds, resolution)

  /** Thin wrapper: BNG average aggregation. Returns per-band `(cellId: String, avg: Double)`. */
  def bngGrid(ds: Dataset, resolution: Int): Array[Array[(String, Double)]] = {
    val fAvg: mutable.ArrayBuffer[Double] => Double = buf => buf.sum / buf.length
    RST_BNG_RasterToGrid.execute(ds, resolution, fAvg)
  }

  /** Thin wrapper: Quadbin average aggregation. Returns per-band `(cellId: Long, avg: Double)`. */
  def quadbinGrid(ds: Dataset, resolution: Int): Array[Array[(Long, Double)]] = {
    val fAvg: mutable.ArrayBuffer[Double] => Double = buf => buf.sum / buf.length
    RST_Quadbin_RasterToGrid.execute(ds, resolution, fAvg)
  }

  /** Deterministic digest of a raster-to-grid result.
    *
    * Cells in each band are sorted by their string key before hashing, so output
    * ordering from the per-band LongMap (arbitrary) does not affect the digest.
    * Values are rounded to 1e-9 precision so tiny floating-point variance across
    * JVM runs does not produce false failures.
    */
  def digest(result: Array[Array[(String, Double)]]): String = {
    val md = java.security.MessageDigest.getInstance("SHA-256")
    result.foreach { band =>
      band.sortBy(_._1).foreach { case (c, v) =>
        md.update(c.getBytes("UTF-8"))
        md.update(java.lang.Double.toString(math.rint(v * 1e9) / 1e9).getBytes("UTF-8"))
      }
      md.update(0.toByte) // band separator
    }
    md.digest().map("%02x".format(_)).mkString
  }

  // Captured from the first run against main (before GridSystem-trait refactor) and frozen:
  val H3_AVG_RES2: String       = "7e7a803cee4bf1b34d16ad4e36c5a6996f116a75040b979114c5ed6586bd43db"
  val BNG_AVG_RES3: String      = "4962e13f5e2a73784e5a24d360455ba0a167965262bce96bb6546e5839a45800"
  val QUADBIN_AVG_RES10: String = "94a119f9a7595916463ceb3c635c6edb33275d01aaee3c21d002d2c1a18ea712"
}
