package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.expressions.RST_IsEmpty
import com.databricks.labs.gbx.rasterx.expressions.RST_NDVI
import com.databricks.labs.gbx.rasterx.expressions.RST_RasterToWorldCoord
import com.databricks.labs.gbx.rasterx.expressions.RST_RasterToWorldCoordX
import com.databricks.labs.gbx.rasterx.expressions.RST_RasterToWorldCoordY
import com.databricks.labs.gbx.rasterx.expressions.RST_Transform
import com.databricks.labs.gbx.rasterx.expressions.RST_WorldToRasterCoord
import com.databricks.labs.gbx.rasterx.expressions.RST_WorldToRasterCoordX
import com.databricks.labs.gbx.rasterx.expressions.RST_WorldToRasterCoordY
import com.databricks.labs.gbx.rasterx.expressions.accessors._
import com.databricks.labs.gbx.rasterx.expressions.dem._
import com.databricks.labs.gbx.rasterx.expressions.spectral._
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_Histogram
import com.databricks.labs.gbx.rasterx.expressions.web.RST_TileXYZ
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
    "rst_transform" -> WARP, "rst_to_webmercator" -> WARP,
    // scalar accessors (Task 2)
    "rst_srid" -> ACC, "rst_pixelwidth" -> ACC, "rst_pixelheight" -> ACC,
    "rst_upperleftx" -> ACC, "rst_upperlefty" -> ACC, "rst_scalex" -> ACC,
    "rst_scaley" -> ACC, "rst_skewx" -> ACC, "rst_skewy" -> ACC,
    "rst_rotation" -> ACC, "rst_isempty" -> ACC, "rst_getnodata" -> ACC,
    "rst_format" -> ACC, "rst_type" -> ACC, "rst_memsize" -> ACC,
    // coordinate / index accessors (Task 3)
    "rst_rastertoworldcoord" -> ACC, "rst_rastertoworldcoordx" -> ACC,
    "rst_rastertoworldcoordy" -> ACC, "rst_worldtorastercoord" -> ACC,
    "rst_worldtorastercoordx" -> ACC, "rst_worldtorastercoordy" -> ACC,
    "rst_tilexyz" -> ACC,
    // map / struct accessors (Task 4): timing-only
    "rst_metadata" -> ACC, "rst_bandmetadata" -> ACC, "rst_georeference" -> ACC,
    "rst_boundingbox" -> ACC, "rst_summary" -> ACC, "rst_histogram" -> ACC
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
    // scalar accessors (Task 2)
    case "rst_srid"        => BenchFingerprint.ofScalar(RST_SRID.execute(ds))
    case "rst_pixelwidth"  => BenchFingerprint.ofScalar(RST_PixelWidth.execute(ds))
    case "rst_pixelheight" => BenchFingerprint.ofScalar(RST_PixelHeight.execute(ds))
    case "rst_upperleftx"  => BenchFingerprint.ofScalar(RST_UpperLeftX.execute(ds))
    case "rst_upperlefty"  => BenchFingerprint.ofScalar(RST_UpperLeftY.execute(ds))
    case "rst_scalex"      => BenchFingerprint.ofScalar(RST_ScaleX.execute(ds))
    case "rst_scaley"      => BenchFingerprint.ofScalar(RST_ScaleY.execute(ds))
    case "rst_skewx"       => BenchFingerprint.ofScalar(RST_SkewX.execute(ds))
    case "rst_skewy"       => BenchFingerprint.ofScalar(RST_SkewY.execute(ds))
    case "rst_rotation"    => BenchFingerprint.ofScalar(RST_Rotation.execute(ds))
    // bool -> 1.0/0.0 to match the pyrx core_fn's numeric coercion
    case "rst_isempty"     => BenchFingerprint.ofScalar(if (RST_IsEmpty.execute(ds)) 1.0 else 0.0)
    case "rst_getnodata"   => BenchFingerprint.ofArray(RST_GetNoData.execute(ds))
    case "rst_format"      => BenchFingerprint.ofScalar(RST_Format.execute(ds))
    // timing-only (per-band string array; no cross-engine fingerprint)
    case "rst_type"        => { RST_Type.execute(ds); BenchFingerprint.empty }
    // timing-only (file size vs in-memory size; not comparable)
    case "rst_memsize"     => { RST_MemSize.execute(ds); BenchFingerprint.empty }
    // coordinate / index accessors (Task 3).
    // raster->world: forward affine; X = pair._1, Y = pair._2; pair as scalar_list.
    case "rst_rastertoworldcoordx" =>
      BenchFingerprint.ofScalar(RST_RasterToWorldCoordX.execute(ds, argI(a, "x", 64), argI(a, "y", 64))._1)
    case "rst_rastertoworldcoordy" =>
      BenchFingerprint.ofScalar(RST_RasterToWorldCoordY.execute(ds, argI(a, "x", 64), argI(a, "y", 64))._2)
    case "rst_rastertoworldcoord" =>
      val p = RST_RasterToWorldCoord.execute(ds, argI(a, "x", 64), argI(a, "y", 64))
      BenchFingerprint.ofArray(Array(p._1, p._2))
    // world->raster: timing-only (CRS-dependent inverse-affine index; not comparable).
    case "rst_worldtorastercoordx" =>
      RST_WorldToRasterCoordX.execute(ds, argD(a, "x", -73.985), argD(a, "y", 40.745)); BenchFingerprint.empty
    case "rst_worldtorastercoordy" =>
      RST_WorldToRasterCoordY.execute(ds, argD(a, "x", -73.985), argD(a, "y", 40.745)); BenchFingerprint.empty
    case "rst_worldtorastercoord" =>
      RST_WorldToRasterCoord.execute(ds, argD(a, "x", -73.985), argD(a, "y", 40.745)); BenchFingerprint.empty
    // rst_tilexyz: timing-only (render/encode-dependent bytes; not comparable).
    case "rst_tilexyz" =>
      RST_TileXYZ.execute(ds, Map.empty, argI(a, "z", 12), argI(a, "x", 1205),
        argI(a, "y", 1539), argS(a, "format", "PNG"), argI(a, "size", 256), argS(a, "resampling", "bilinear"))
      BenchFingerprint.empty
    // map / struct accessors (Task 4): timing-only (maps, structs, CRS/JSON bytes).
    case "rst_metadata"      => { RST_MetaData.execute(ds); BenchFingerprint.empty }
    case "rst_bandmetadata"  => { RST_BandMetaData.execute(ds.GetRasterBand(1)); BenchFingerprint.empty }
    case "rst_georeference"  => { RST_GeoReference.execute(ds); BenchFingerprint.empty }
    case "rst_boundingbox"   => { RST_BoundingBox.execute(ds); BenchFingerprint.empty }
    case "rst_summary"       => { RST_Summary.execute(ds); BenchFingerprint.empty }
    case "rst_histogram"     => { RST_Histogram.execute(ds, 256, None, None, false); BenchFingerprint.empty }
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
      // scalar accessors (Task 2)
      case "rst_srid"        => rst_srid(tile)
      case "rst_pixelwidth"  => rst_pixelwidth(tile)
      case "rst_pixelheight" => rst_pixelheight(tile)
      case "rst_upperleftx"  => rst_upperleftx(tile)
      case "rst_upperlefty"  => rst_upperlefty(tile)
      case "rst_scalex"      => rst_scalex(tile)
      case "rst_scaley"      => rst_scaley(tile)
      case "rst_skewx"       => rst_skewx(tile)
      case "rst_skewy"       => rst_skewy(tile)
      case "rst_rotation"    => rst_rotation(tile)
      case "rst_isempty"     => rst_isempty(tile)
      case "rst_getnodata"   => rst_getnodata(tile)
      case "rst_format"      => rst_format(tile)
      case "rst_type"        => rst_type(tile)
      case "rst_memsize"     => rst_memsize(tile)
      // coordinate / index accessors (Task 3)
      case "rst_rastertoworldcoordx" => rst_rastertoworldcoordx(tile, argI(a, "x", 64), argI(a, "y", 64))
      case "rst_rastertoworldcoordy" => rst_rastertoworldcoordy(tile, argI(a, "x", 64), argI(a, "y", 64))
      case "rst_rastertoworldcoord"  => rst_rastertoworldcoord(tile, argI(a, "x", 64), argI(a, "y", 64))
      // world->raster + tilexyz are pure-core-only; the column form is here only
      // to keep the match exhaustive (the spark-path runner filters by modes).
      case "rst_worldtorastercoordx" => rst_worldtorastercoordx(tile, argD(a, "x", -73.985), argD(a, "y", 40.745))
      case "rst_worldtorastercoordy" => rst_worldtorastercoordy(tile, argD(a, "x", -73.985), argD(a, "y", 40.745))
      case "rst_worldtorastercoord"  => rst_worldtorastercoord(tile, argD(a, "x", -73.985), argD(a, "y", 40.745))
      case "rst_tilexyz"             => rst_tilexyz(tile, argI(a, "z", 12), argI(a, "x", 1205), argI(a, "y", 1539))
      // map / struct accessors (Task 4) are pure-core-only; the column form is here
      // only to keep the match exhaustive (the spark-path runner filters by modes).
      case "rst_metadata"      => rst_metadata(tile)
      case "rst_bandmetadata"  => rst_bandmetadata(tile, 1)
      case "rst_georeference"  => rst_georeference(tile)
      case "rst_boundingbox"   => rst_boundingbox(tile)
      case "rst_summary"       => rst_summary(tile)
      case "rst_histogram"     => rst_histogram(tile)
      case other            => throw new IllegalArgumentException(s"unknown bench fn: $other")
    }
  }
}
