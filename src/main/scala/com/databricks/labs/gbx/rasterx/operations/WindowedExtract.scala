package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager}
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * Extracts an axis-aligned pixel window from a raster directly, without shelling out to
  * `gdal.Translate -srcwin`. This is the fast path under [[ReTile.getTile]] (shared by
  * `rst_tooverlappingtiles`, `rst_retile`, and `rst_maketiles`).
  *
  * The downstream serializer ([[com.databricks.labs.gbx.rasterx.gdal.RasterDriver.writeToBytes]])
  * re-encodes the returned Dataset with its own creation options, so compression/byte-layout
  * parity with `gdal.Translate` is moot. What MUST match: pixels, geotransform, SRS, NoData,
  * band structure, and the per-band/per-dataset attributes copied below.
  *
  * Correctness over speed: the fast path runs only when [[simpleEnough]] holds. Anything it
  * cannot faithfully reproduce (mixed dtype, real mask bands, GCPs, RPC/GEOLOCATION
  * georeferencing) FALLS BACK to the proven [[GDALTranslate.executeTranslate]] with the exact
  * same `gdal_translate -srcwin ...` command. Worst case is "not faster," never "wrong."
  */
private[rasterx] object WindowedExtract {

    /**
      * Extract the axis-aligned window `(xStart, yStart, xOffset, yOffset)` from `ds`.
      *
      * Returns `(Dataset, metadata)` with the SAME metadata-map keys/shape as
      * [[GDALTranslate.executeTranslate]] so downstream `writeToBytes` behaves identically.
      * Caller must release the returned Dataset (and unlink the `"path"` /vsimem file).
      */
    def extract(
        ds: Dataset,
        options: Map[String, String],
        xStart: Int,
        yStart: Int,
        xOffset: Int,
        yOffset: Int
    ): (Dataset, Map[String, String]) = {
        if (!simpleEnough(ds)) {
            return fallback(ds, options, xStart, yStart, xOffset, yOffset)
        }

        val bandCount = ds.getRasterCount
        val dtype = ds.GetRasterBand(1).getDataType
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val rasterPath = s"/vsimem/retile_$uuid.tif"
        val drv = GDALManager.gtiffDriver()
        val out = drv.Create(rasterPath, xOffset, yOffset, bandCount, dtype)

        // Dataset-level: window-shifted geotransform (full formula incl. rotation terms),
        // projection, and default-domain metadata.
        val gt = new Array[Double](6)
        ds.GetGeoTransform(gt)
        out.SetGeoTransform(
          Array(
            gt(0) + xStart * gt(1) + yStart * gt(2),
            gt(1),
            gt(2),
            gt(3) + xStart * gt(4) + yStart * gt(5),
            gt(4),
            gt(5)
          )
        )
        out.SetProjection(ds.GetProjection())
        Option(ds.GetMetadata_Dict()).foreach(md => if (!md.isEmpty) out.SetMetadata(md))

        val pixBytes = gdal.GetDataTypeSize(dtype) / 8
        val buf = new Array[Byte](xOffset * yOffset * pixBytes)
        var b = 1
        while (b <= bandCount) {
            val sb = ds.GetRasterBand(b)
            val db = out.GetRasterBand(b)
            // Raw bytes at the band's native dtype: no resample, no type-convert.
            sb.ReadRaster(xStart, yStart, xOffset, yOffset, xOffset, yOffset, dtype, buf)
            db.WriteRaster(0, 0, xOffset, yOffset, xOffset, yOffset, dtype, buf)

            val nd = new Array[java.lang.Double](1)
            sb.GetNoDataValue(nd)
            if (nd(0) != null) db.SetNoDataValue(nd(0).doubleValue()) // handles NaN-nodata too

            db.SetColorInterpretation(sb.GetColorInterpretation())

            val ct = sb.GetColorTable()
            if (ct != null) db.SetColorTable(ct)

            val scale = new Array[java.lang.Double](1)
            sb.GetScale(scale)
            if (scale(0) != null) db.SetScale(scale(0).doubleValue())

            val offset = new Array[java.lang.Double](1)
            sb.GetOffset(offset)
            if (offset(0) != null) db.SetOffset(offset(0).doubleValue())

            val unit = sb.GetUnitType()
            if (unit != null && unit.nonEmpty) db.SetUnitType(unit)

            Option(sb.GetMetadata_Dict()).foreach(md => if (!md.isEmpty) db.SetMetadata(md))
            b += 1
        }
        out.FlushCache()

        val sourcePath = Option(ds.GetFileList())
            .flatMap(_.asScala.headOption.map(_.toString))
            .getOrElse("unknown source path")
        val meta = Map(
          "path" -> rasterPath,
          "sourcePath" -> sourcePath,
          "driver" -> "GTiff",
          "format" -> "GTiff",
          "last_command" -> s"windowed_extract -srcwin $xStart $yStart $xOffset $yOffset",
          "last_error" -> "",
          "all_parents" -> s"$sourcePath;${options.getOrElse("all_parents", "")}",
          "size" -> "-1",
          "compression" -> options.getOrElse("compression", "DEFLATE"),
          "isZipped" -> "false",
          "isSubset" -> "false"
        )
        (out, meta)
    }

    /**
      * True when the fast path can faithfully reproduce a `gdal.Translate -srcwin` of `ds`.
      *
      * Requires ALL of: uniform per-band data type ([[org.gdal.gdal.Driver.Create]] takes one
      * dtype); no real mask bands (every band's `GetMaskFlags()` is only `GMF_ALL_VALID` or
      * `GMF_NODATA` — same predicate as [[BandAccessors.isEmpty]] treats as "no separate mask");
      * no GCPs; no `RPC`/`GEOLOCATION` metadata domain. Anything else => fall back.
      */
    private def simpleEnough(ds: Dataset): Boolean = {
        val bandCount = ds.getRasterCount
        if (bandCount <= 0) return false
        val dtype0 = ds.GetRasterBand(1).getDataType

        var b = 1
        while (b <= bandCount) {
            val band = ds.GetRasterBand(b)
            if (band.getDataType != dtype0) return false
            // A real (per-dataset/alpha/separate) mask band is anything beyond all-valid or
            // the nodata-derived mask, which Create+WriteRaster + SetNoDataValue reproduces.
            val flags = band.GetMaskFlags()
            val onlyImplicit =
                (flags & gdalconstConstants.GMF_ALL_VALID) != 0 ||
                    (flags & gdalconstConstants.GMF_NODATA) != 0
            if (!onlyImplicit) return false
            b += 1
        }

        if (ds.GetGCPCount() != 0) return false

        val domains = Option(ds.GetMetadataDomainList())
            .map(_.asScala.map(_.toString).toSet)
            .getOrElse(Set.empty[String])
        if (domains.contains("RPC") || domains.contains("GEOLOCATION")) return false

        true
    }

    /**
      * Exact-semantics fallback to `gdal.Translate -srcwin` for non-simple datasets.
      *
      * Identical to the original [[ReTile.getTile]] behaviour: the output path takes the SOURCE
      * driver's extension so the format round-trips faithfully. A GTiff target would be wrong
      * here — the very cases that reach the fallback (e.g. a mixed-dtype VRT) cannot be written
      * to GTiff at all ("different datatypes per different bands"), so the source format must be
      * preserved.
      */
    private def fallback(
        ds: Dataset,
        options: Map[String, String],
        xStart: Int,
        yStart: Int,
        xOffset: Int,
        yOffset: Int
    ): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val extension = GDAL.getExtension(ds.GetDriver.getShortName)
        val rasterPath = s"/vsimem/retile_$uuid.$extension"
        GDALTranslate.executeTranslate(
          rasterPath,
          ds,
          command = s"gdal_translate -srcwin $xStart $yStart $xOffset $yOffset",
          options
        )
    }

}
