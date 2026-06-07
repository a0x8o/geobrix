package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.{GDALBuildVRT, GDALTranslate}
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.gdal.gdal.Dataset

import java.io.File
import java.nio.file.{Files, Paths}
import scala.xml.{Elem, UnprefixedAttribute, XML}

/** Combines multiple rasters with a Python pixel function (e.g. average) via VRT and gdal_translate. */
object PixelCombineRasters {

    /** Builds VRT, injects Python pixel function, translates to raster; returns (Dataset, metadata). Caller must release. */
    def combine(
        dss: Array[Dataset],
        options: Map[String, String],
        pythonFunc: String,
        pythonFuncName: String
    ): (Dataset, Map[String, String]) = {
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val outShortName = dss.head.GetDriver().getShortName
        val extension = GDAL.getExtension(outShortName)
        // Ensure the per-JVM staging dir exists. gdal.BuildVRT silently
        // produces an unwritten Dataset if the parent dir is missing, and
        // the subsequent RasterDriver.read(vrtPath) then throws
        // "No such file or directory". Only ClipToGeom previously created
        // this dir, leaving combineavg / derivedband broken if they were
        // the first op to hit a fresh JVM.
        Files.createDirectories(NodeFilePathUtil.rootPath)
        val vrtPath = s"${NodeFilePathUtil.rootPath}/combine_rasters_vrt_$uuid.vrt"
        val rasterPath = s"/vsimem/combine_rasters_$uuid.$extension"

        // `-separate` stacks every band of every input dataset as a distinct
        // VRT band/source. Without it, gdalbuildvrt overlays inputs band-by-band
        // and preserves the input band count, so a single multi-band input tile
        // yields a multi-band VRT — and the pixel function would then be applied
        // per band, emitting N output bands instead of the documented single
        // derived band. `-separate` + the single-band collapse in
        // addPixelFunction guarantee exactly one derived output band, whether the
        // input is N single-band tiles (combineavg / *_agg) or one N-band tile.
        val vrtRaster = GDALBuildVRT.executeVRT(
          vrtPath,
          dss,
          options,
          command = s"gdalbuildvrt -resolution highest -separate"
        )
        vrtRaster._1.delete()

        // Inject the pixel function BEFORE re-opening the VRT. gdal.Open
        // parses VRT XML into an in-memory band structure at Open time, so
        // any mutation of the on-disk file performed after Open is invisible
        // to the Dataset handle passed to gdal.Translate — Translate then
        // runs a default multi-source mosaic (last-source-wins per pixel)
        // instead of evaluating the pixel function, silently returning one
        // of the inputs.
        addPixelFunction(vrtPath, pythonFunc, pythonFuncName)

        // GDAL evaluates <PixelFunctionLanguage>Python</...> during VRT read-back and translate.
        val result = GDALManager.withVrtPython {
            val vrtRefreshed = RasterDriver.read(vrtPath, vrtRaster._2)

            GDALTranslate.executeTranslate(
              rasterPath,
              vrtRefreshed,
              command = s"gdal_translate",
              options
            )
        }

        Files.deleteIfExists(Paths.get(vrtPath))

        result
    }

    /**
      * Collapses every band of the VRT into a SINGLE derived `VRTRasterBand`
      * driven by the supplied Python pixel function.
      *
      * The VRT is built with `gdalbuildvrt -separate`, so each input band is its
      * own `VRTRasterBand` with one source. We gather the source elements
      * (`SimpleSource` / `ComplexSource` / `AveragedSource`) from every band into
      * a single derived band so the pixel function receives all input bands in
      * `in_ar` and produces exactly one output band — matching the documented and
      * unit-tested `rst_derivedband` / `rst_combineavg` contract (1 output band)
      * regardless of whether the input is N single-band tiles or one N-band tile.
      */
    def addPixelFunction(vrtPath: String, pixFuncCode: String, pixFuncName: String): Unit = {
        val pixFuncTypeEl = <PixelFunctionType>{pixFuncName}</PixelFunctionType>
        val pixFuncLangEl = <PixelFunctionLanguage>Python</PixelFunctionLanguage>
        val pixFuncCodeEl = <PixelFunctionCode>
            {scala.xml.Unparsed(s"<![CDATA[$pixFuncCode]]>")}
        </PixelFunctionCode>

        val sourceLabels = Set("SimpleSource", "ComplexSource", "AveragedSource")

        val vrtContent = XML.loadFile(new File(vrtPath))
        val vrtWithPixFunc = vrtContent match {
            case body @ Elem(_, _, _, _, child @ _*) =>
                val rasterBands = child.collect {
                    case el @ Elem(_, "VRTRasterBand", _, _, _*) => el.asInstanceOf[Elem]
                }
                // Pull every source element across all bands into one derived band.
                val allSources = rasterBands.flatMap { band =>
                    band.child.filter {
                        case Elem(_, label, _, _, _*) => sourceLabels.contains(label)
                        case _                        => false
                    }
                }
                // Base the merged band on the first band's attributes; force band="1".
                val baseBand = rasterBands.headOption.getOrElse(
                    <VRTRasterBand dataType="Float64" band="1"/>
                )
                val mergedBand = baseBand.copy(
                  child = Seq(pixFuncTypeEl, pixFuncLangEl, pixFuncCodeEl) ++ allSources,
                  attributes = baseBand.attributes
                      .remove("band")
                      .append(new UnprefixedAttribute("band", "1", scala.xml.Null))
                      .append(new UnprefixedAttribute("subClass", "VRTDerivedRasterBand", scala.xml.Null))
                )
                // Drop all original VRTRasterBand children; insert the single merged band
                // in their place (preserving non-band metadata like SRS / GeoTransform).
                var inserted = false
                val newChild = child.flatMap {
                    case Elem(_, "VRTRasterBand", _, _, _*) =>
                        if (inserted) Seq.empty
                        else { inserted = true; Seq(mergedBand) }
                    case other => Seq(other)
                }
                body.copy(child = newChild)
        }

        XML.save(vrtPath, vrtWithPixFunc)

    }

}
