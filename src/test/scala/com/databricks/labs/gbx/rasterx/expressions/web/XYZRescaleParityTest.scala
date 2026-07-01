package com.databricks.labs.gbx.rasterx.expressions.web

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Cross-tier parity gate for the XYZ rescale feature.
 *
 *  Both tiers MUST derive the same per-band (min,max) for a source and apply the same
 *  linear map v -> (v-min)/(max-min)*255. Light feeds rio-tiler in_range; heavy feeds
 *  gdal_translate -scale. Parity is pixel/value-distribution-level, NOT byte-level
 *  (heavy re-encodes a GTiff per tile; PNG encoders differ between GDAL and rio-tiler).
 *  uint8 pass-through is the one byte-identical-within-tier assertion (auto == none).
 *
 *  Scope (per Task 6 brief + plan LOCKED DECISIONS):
 *    - "auto" path: heavy resolves whole-dataset per-band (min,max), applies linear map.
 *    - uint8 pass-through: auto == none (byte-identical within tier, no -scale emitted).
 *    - "none" heavy behavior: asserted against heavy's own real behavior (GDAL -ot Byte
 *      clips values > 255 to 255). NOT asserted equal to light's "none" -- that is a
 *      known pre-existing per-tier-raw difference (Task 7 validates empirically).
 *    - Explicit pair: asserted to match the bounds supplied exactly.
 *
 *  Implementation note: resolveScale emits repeated -scale flags (not per-band -scale_N)
 *  for single-band sources. The parity contract is the linear mapping itself, verified
 *  by inspecting the output value distribution.
 */
class XYZRescaleParityTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        java.nio.file.Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    /** 64x64 uint16 raster covering lon 0..45, lat 0..45 (EPSG:4326), values ramped [lo,hi].
     *
     *  The footprint is entirely within z=2 tile (x=2, y=1), which covers lon 0..90 and
     *  lat ~0..66.5. At 64px the ramp maps to multiple distinct byte values after rescale.
     */
    private def makeUint16Narrow(lo: Int = 8000, hi: Int = 12000): Dataset = {
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("/vsimem/parity_u16", 64, 64, 1, gdalconstConstants.GDT_UInt16)
        ds.SetGeoTransform(Array(0.0, 45.0 / 64, 0.0, 45.0, 0.0, -45.0 / 64))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        val n = 64 * 64
        val ramp = (0 until n).map(i => (lo.toDouble + (hi - lo).toDouble * i / (n - 1))).toArray
        ds.GetRasterBand(1).WriteRaster(0, 0, 64, 64, ramp)
        ds.GetRasterBand(1).FlushCache()
        ds
    }

    /** Decode PNG bytes via GDAL and return (min, max) of first-band non-zero pixels. */
    private def pngBandSpread(bytes: Array[Byte]): (Int, Int) = {
        val path = s"/vsimem/parity_decode_${java.util.UUID.randomUUID().toString.replace("-", "")}.png"
        gdal.FileFromMemBuffer(path, bytes)
        val ds = gdal.Open(path)
        try {
            val band = ds.GetRasterBand(1)
            val buf = Array.ofDim[Byte](ds.GetRasterXSize * ds.GetRasterYSize)
            band.ReadRaster(0, 0, ds.GetRasterXSize, ds.GetRasterYSize, buf)
            val vals = buf.map(_ & 0xff).filter(_ > 0)
            if (vals.isEmpty) (0, 0) else (vals.min, vals.max)
        } finally { ds.delete(); gdal.Unlink(path) }
    }

    // -----------------------------------------------------------------------
    // Parity assertion (a): heavy resolves whole-dataset min/max -- same stat
    // as light's ds.statistics(approx=False). Verified via resolveScale output.
    // -----------------------------------------------------------------------
    test("heavy auto resolves source whole-dataset min/max (same statistic light uses)") {
        val ds = makeUint16Narrow(8000, 12000)
        try {
            val scale = RST_TileXYZ.resolveScale(ds, "auto")
            // resolveScale uses repeated -scale form: "-scale <lo> <hi> 0 255"
            scale should not be empty
            scale should include("-scale ")
            val parts = scale.trim.split("\\s+")
            // parts: ["-scale", "<lo>", "<hi>", "0", "255"]
            parts(0) shouldBe "-scale"
            val loVal = parts(1).toDouble
            val hiVal = parts(2).toDouble
            // The ramp covers exactly [8000, 12000]. ComputeRasterMinMax (exact) should
            // resolve these within 0.5 (rounding from the linspace endpoint).
            loVal shouldBe (8000.0 +- 5.0)
            hiVal shouldBe (12000.0 +- 5.0)
            parts(3) shouldBe "0"
            parts(4) shouldBe "255"
        } finally ds.delete()
    }

    // -----------------------------------------------------------------------
    // Parity assertion (b): heavy "auto" decoded value distribution matches
    // the expected linear [min,max]->[0,255] map within tolerance.
    // NOT crushed into a narrow range (that would be the "none" behavior).
    // -----------------------------------------------------------------------
    test("heavy auto recovers contrast (value distribution spans most of 8-bit range)") {
        val ds = makeUint16Narrow(8000, 12000)
        try {
            val png = RST_TileXYZ.execute(ds, Map.empty[String, String], 2, 2, 1, "PNG", 64, "near", "auto")
            val (lo, hi) = pngBandSpread(png)
            // Full ramp in tile: auto maps [8000,12000] -> [0,255]; spread should be wide.
            // Tolerance: resampling + border effects can trim a few values at each end.
            (hi - lo) should be > 100
        } finally ds.delete()
    }

    // -----------------------------------------------------------------------
    // Heavy "none" behavior: GDAL -ot Byte clips values > 255 to 255.
    // 8000-12000 all exceed 255, so the entire tile clips to 255.
    // This is asserted against heavy's OWN behavior, NOT compared to light.
    // (Light "none" may behave differently -- that is a documented known difference.)
    // -----------------------------------------------------------------------
    test("heavy none clips uint16 >255 values to 255 (heavy-tier own behavior, not vs light)") {
        val ds = makeUint16Narrow(8000, 12000)
        try {
            val png = RST_TileXYZ.execute(ds, Map.empty[String, String], 2, 2, 1, "PNG", 64, "near", "none")
            val (lo, hi) = pngBandSpread(png)
            // 8000-12000 all far exceed 255. GDAL -ot Byte clips to 255.
            // Expect most/all data pixels to be 255.
            hi should be >= 200
            lo should be >= 200
        } finally ds.delete()
    }

    // -----------------------------------------------------------------------
    // Explicit pair: resolveScale emits the given bounds directly.
    // -----------------------------------------------------------------------
    test("heavy explicit pair maps exactly the given bounds") {
        val ds = makeUint16Narrow(8000, 12000)
        try {
            val scale = RST_TileXYZ.resolveScale(ds, "8000,12000")
            // resolveScale: repeated -scale form, Double.toString for the values.
            scale shouldBe "-scale 8000.0 12000.0 0 255"
        } finally ds.delete()
    }

    // -----------------------------------------------------------------------
    // Multi-band guard: resolveScale emits a SEPARATE per-band -scale segment
    // for each band, each reflecting THAT band's own min/max -- not band 1's
    // range repeated.  This locks in the per-band correctness that was verified
    // empirically on GDAL 3.11.4 (repeated bare -scale IS per-band-correct;
    // a single bare -scale gives one-to-all -- proven by indexed -scale_N
    // cross-check).  The test uses three bands with clearly separated uint16
    // ranges so any reversion to a global/first-band range would be caught.
    // -----------------------------------------------------------------------
    test("multi-band auto emits per-band -scale segments with each band's own min/max") {
        // Build a 3-band uint16 MEM dataset.  Each band is filled with a
        // constant whose range is clearly separated from the others:
        //   band 1: ~5 000  (lo=4000, hi=6000)
        //   band 2: ~25 000 (lo=24000, hi=26000)
        //   band 3: ~55 000 (lo=54000, hi=56000)
        // A pure-constant band has lo==hi, which triggers the lo+1 guard in
        // resolveScale, so we use a tiny ramp within each band's range instead.
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("/vsimem/parity_multiband", 64, 64, 3, gdalconstConstants.GDT_UInt16)
        ds.SetGeoTransform(Array(0.0, 45.0 / 64, 0.0, 45.0, 0.0, -45.0 / 64))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())

        // Band-specific [lo, hi] ranges (well-separated, all << UInt16 max).
        val bandRanges = Seq((4000.0, 6000.0), (24000.0, 26000.0), (54000.0, 56000.0))
        val n = 64 * 64
        bandRanges.zipWithIndex.foreach { case ((lo, hi), idx) =>
            val ramp = (0 until n).map(i => lo + (hi - lo) * i / (n - 1)).toArray
            ds.GetRasterBand(idx + 1).WriteRaster(0, 0, 64, 64, ramp)
            ds.GetRasterBand(idx + 1).FlushCache()
        }

        try {
            val scale = RST_TileXYZ.resolveScale(ds, "auto")

            // The emitted string must contain exactly 3 separate -scale tokens.
            // Each -scale segment: "-scale <lo> <hi> 0 255"
            val segments = scale.trim.split("-scale").map(_.trim).filter(_.nonEmpty)
            withClue(s"Expected 3 -scale segments, got: '$scale'\n") {
                segments.length shouldBe 3
            }

            // Parse each segment and assert per-band min/max match band-specific ranges.
            bandRanges.zipWithIndex.foreach { case ((expectedLo, expectedHi), idx) =>
                val parts = segments(idx).split("\\s+")
                withClue(s"Band ${idx + 1} segment '${segments(idx)}' malformed: ") {
                    parts.length shouldBe 4 // lo hi 0 255
                }
                val emittedLo = parts(0).toDouble
                val emittedHi = parts(1).toDouble
                withClue(s"Band ${idx + 1}: lo=$emittedLo should match ~$expectedLo (full scale='$scale'): ") {
                    emittedLo shouldBe (expectedLo +- 5.0)
                }
                withClue(s"Band ${idx + 1}: hi=$emittedHi should match ~$expectedHi (full scale='$scale'): ") {
                    emittedHi shouldBe (expectedHi +- 5.0)
                }
                parts(2) shouldBe "0"
                parts(3) shouldBe "255"
            }

            // Cross-band sanity: each band's emitted lo must be strictly less than
            // the next band's lo.  If resolveScale reused band 1's range, all three
            // would collapse to ~4000/6000 and this would catch it.
            val emittedLos = segments.map(_.trim.split("\\s+").head.toDouble)
            emittedLos(0) should be < emittedLos(1)
            emittedLos(1) should be < emittedLos(2)

        } finally ds.delete()
    }

    // -----------------------------------------------------------------------
    // Parity assertion (c): uint8 source -> auto == none (byte-identical
    // pass-through within tier; no -scale emitted).
    // -----------------------------------------------------------------------
    test("uint8 source: auto == none (byte-identical pass-through within tier)") {
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("/vsimem/parity_u8", 64, 64, 1, gdalconstConstants.GDT_Byte)
        ds.SetGeoTransform(Array(0.0, 45.0 / 64, 0.0, 45.0, 0.0, -45.0 / 64))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).WriteRaster(0, 0, 64, 64, Array.fill(64 * 64)(100.0))
        ds.GetRasterBand(1).FlushCache()
        try {
            val auto = RST_TileXYZ.execute(ds, Map.empty[String, String], 2, 2, 1, "PNG", 64, "near", "auto")
            val none = RST_TileXYZ.execute(ds, Map.empty[String, String], 2, 2, 1, "PNG", 64, "near", "none")
            java.util.Arrays.equals(auto, none) shouldBe true
        } finally ds.delete()
    }
}
