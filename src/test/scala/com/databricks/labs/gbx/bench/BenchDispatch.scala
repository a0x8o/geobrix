package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.expressions.RST_NDVI
import com.databricks.labs.gbx.rasterx.expressions.RST_Transform
import com.databricks.labs.gbx.rasterx.expressions.accessors._
import com.databricks.labs.gbx.rasterx.expressions.dem._
import com.databricks.labs.gbx.rasterx.expressions.spectral._
import com.databricks.labs.gbx.rasterx.expressions.web.RST_ToWebMercator
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.apache.spark.sql.Column
import org.gdal.gdal.Dataset

import java.nio.file.Files

/** Maps a bench fn name to its pure-core call (-> fingerprint) and its Spark Column. */
object BenchDispatch {
  private def argS(a: Map[String, String], k: String, d: String) = a.getOrElse(k, d)
  private def argD(a: Map[String, String], k: String, d: Double) = a.get(k).map(_.toDouble).getOrElse(d)
  private def argI(a: Map[String, String], k: String, d: Int) = a.get(k).map(_.toInt).getOrElse(d)
  private def argB(a: Map[String, String], k: String, d: Boolean) = a.get(k).map(_.toBoolean).getOrElse(d)

  private val ACC = "accessor"; private val TER = "terrain"
  private val BM = "band-math"; private val WARP = "warp"

  private val cats: Map[String, String] = Map(
    "rst_width" -> ACC, "rst_height" -> ACC, "rst_numbands" -> ACC, "rst_avg" -> ACC,
    "rst_min" -> ACC, "rst_max" -> ACC, "rst_median" -> ACC, "rst_pixelcount" -> ACC,
    "rst_slope" -> TER, "rst_aspect" -> TER, "rst_hillshade" -> TER,
    "rst_tri" -> TER, "rst_tpi" -> TER, "rst_roughness" -> TER,
    "rst_ndvi" -> BM, "rst_ndwi" -> BM, "rst_nbr" -> BM,
    "rst_transform" -> WARP, "rst_to_webmercator" -> WARP
  )

  def all: Seq[String] = cats.keys.toSeq.sorted
  def category(fn: String): String = cats(fn)
  def minBands(fn: String): Int = if (cats(fn) == BM) 2 else 1

  def pureCore(fn: String, ds: Dataset, a: Map[String, String]): String = {
    // gdal_calc/warp-backed functions write to NodeFilePathUtil.rootPath, a per-JVM scratch dir
    // that the Spark file-lock copy path normally mkdirs. Pure-core opens tiles directly via
    // gdal.Open and bypasses that path, so ensure the dir exists or gdal_calc fails with
    // "No such file or directory" and returns a null dataset.
    Files.createDirectories(NodeFilePathUtil.rootPath)
    fn match {
    case "rst_width"      => BenchFingerprint.ofScalar(RST_Width.execute(ds))
    case "rst_height"     => BenchFingerprint.ofScalar(RST_Height.execute(ds))
    case "rst_numbands"   => BenchFingerprint.ofScalar(RST_NumBands.execute(ds))
    case "rst_avg"        => BenchFingerprint.ofArray(RST_Avg.execute(ds))
    case "rst_min"        => BenchFingerprint.ofArray(RST_Min.execute(ds))
    case "rst_max"        => BenchFingerprint.ofArray(RST_Max.execute(ds))
    case "rst_median"     => BenchFingerprint.ofArray(RST_Median.execute(ds, Map.empty))
    case "rst_pixelcount" => BenchFingerprint.ofArray(RST_PixelCount.execute(ds).map(_.toDouble))
    case "rst_slope"      => fpDerived(RST_Slope.execute(ds, argS(a, "unit", "degrees"), argD(a, "scale", Double.NaN)))
    case "rst_aspect"     => fpDerived(RST_Aspect.execute(ds, argB(a, "trigonometric", false), argB(a, "zero_for_flat", false)))
    case "rst_hillshade"  => fpDerived(RST_Hillshade.execute(ds, argD(a, "azimuth", 315.0), argD(a, "altitude", 45.0), argD(a, "z_factor", 1.0)))
    case "rst_tri"        => fpDerived(RST_TRI.execute(ds))
    case "rst_tpi"        => fpDerived(RST_TPI.execute(ds))
    case "rst_roughness"  => fpDerived(RST_Roughness.execute(ds))
    case "rst_ndvi"       => fpDerived(RST_NDVI.execute(ds, argI(a, "red_band", 1), argI(a, "nir_band", 2), Map.empty))
    case "rst_ndwi"       => fpDerived(RST_NDWI.execute(ds, argI(a, "green_idx", 1), argI(a, "nir_idx", 2)))
    case "rst_nbr"        => fpDerived(RST_NBR.execute(ds, argI(a, "nir_idx", 1), argI(a, "swir_idx", 2)))
    case "rst_transform"  => fpDerived(RST_Transform.execute(ds, Map.empty, argI(a, "target_srid", 3857)))
    case "rst_to_webmercator" => fpDerived(RST_ToWebMercator.execute(ds, Map.empty, argS(a, "resampling", "bilinear")))
    case other            => throw new IllegalArgumentException(s"unknown bench fn: $other")
    }
  }

  private def fpDerived(res: (Dataset, Map[String, String])): String = {
    val out = res._1
    try BenchFingerprint.ofDataset(out)
    finally RasterDriver.releaseDataset(out)
  }

  def column(fn: String, tile: Column, a: Map[String, String]): Column = {
    import functions._
    fn match {
      case "rst_width"      => rst_width(tile)
      case "rst_height"     => rst_height(tile)
      case "rst_numbands"   => rst_numbands(tile)
      case "rst_avg"        => rst_avg(tile)
      case "rst_min"        => rst_min(tile)
      case "rst_max"        => rst_max(tile)
      case "rst_median"     => rst_median(tile)
      case "rst_pixelcount" => rst_pixelcount(tile)
      case "rst_slope"      => rst_slope(tile, argS(a, "unit", "degrees"))
      case "rst_aspect"     => rst_aspect(tile)
      case "rst_hillshade"  => rst_hillshade(tile)
      case "rst_tri"        => rst_tri(tile)
      case "rst_tpi"        => rst_tpi(tile)
      case "rst_roughness"  => rst_roughness(tile)
      case "rst_ndvi"       => rst_ndvi(tile, argI(a, "red_band", 1), argI(a, "nir_band", 2))
      case "rst_ndwi"       => rst_ndwi(tile, argI(a, "green_idx", 1), argI(a, "nir_idx", 2))
      case "rst_nbr"        => rst_nbr(tile, argI(a, "nir_idx", 1), argI(a, "swir_idx", 2))
      case "rst_transform"  => rst_transform(tile, argI(a, "target_srid", 3857))
      case "rst_to_webmercator" => rst_to_webmercator(tile, argS(a, "resampling", "bilinear"))
      case other            => throw new IllegalArgumentException(s"unknown bench fn: $other")
    }
  }
}
