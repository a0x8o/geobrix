package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import org.gdal.gdal.{Dataset, gdal}

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.{Failure, Try}

/** Dataset-level accessors: subdatasets metadata, in-memory size, empty check, and vsimem unlink. */
object RasterAccessors {

    /** Returns the SUBDATASETS metadata as a name->description map, or empty if none. */
    def subdatasetsMap(ds: Dataset): Map[String, String] = {
        Try(ds.GetMetadata_Dict("SUBDATASETS")) match {
            case Failure(_) => Map.empty[String, String]
            case _          =>
                val dict = ds.GetMetadata_Dict("SUBDATASETS").asScala.toMap.asInstanceOf[Map[String, String]]
                dict
        }
    }

    /** Returns the size in bytes (file size or vsimem buffer length), or -1 if not determinable.
      *
      * Uses `startsWith("/vsimem/")` — a loose `contains` would match subdataset selectors like
      * `NetCDF:/vsimem/xxx.nc:prAdjust`, where `GetMemFileBuffer` returns null and `.length` NPEs.
      * For selectors and other non-filesystem descriptions, returns -1 (caller's failure sentinel). */
    def memSize(ds: Dataset): Long = {
        val srcPath = ds.GetDescription()
        if (srcPath.startsWith("/vsimem/")) {
            val buf = gdal.GetMemFileBuffer(srcPath)
            if (buf == null) -1L else buf.length.toLong
        } else if (srcPath.startsWith("/")) {
            Try(Files.size(Paths.get(srcPath))).getOrElse(-1L)
        } else {
            -1L
        }
    }

    /** Returns true if the dataset is null, has no size, or all bands have no valid pixels. */
    def isEmpty(ds: Dataset): Boolean = {
        if (ds == null || ds.GetRasterXSize <= 0 || ds.GetRasterYSize <= 0) return true
        val n = ds.GetRasterCount; if (n <= 0) return true
        (1 to n).forall(i => BandAccessors.isEmpty(ds.GetRasterBand(i)))
    }

    /** Releases the dataset and, if vsimem, unlinks the buffer; otherwise delegates to RasterDriver.
      *
      * Uses `startsWith("/vsimem/")` — a loose `contains` would match subdataset selectors like
      * `NetCDF:/vsimem/xxx.nc:prAdjust`, where `Unlink(selector)` is a silent no-op and the real
      * underlying vsimem file leaks. Falling through to `releaseDataset` for that case walks
      * `GetFileList` and unlinks the actual file(s). */
    def unlink(ds: Dataset): Unit = {
        // TODO: move to RasterDriver
        if (ds == null) return
        val srcPath = ds.GetDescription()
        if (srcPath.startsWith("/vsimem/")) {
            ds.delete() // release the dataset
            gdal.Unlink(srcPath)
        } else {
            RasterDriver.releaseDataset(ds)
        }
    }

}
