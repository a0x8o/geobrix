package com.databricks.labs.gbx.rasterx.operations

import org.gdal.gdal.Dataset

/**
  * Per-pixel/per-band cross-raster statistics over a stack of ALIGNED tiles, mirroring
  * [[CombineAVG]] but swapping the reduction. Backs the six expressions
  * `gbx_rst_combine{min,max,median,sum,stddev,count}`.
  *
  * NoData handling is identical to [[CombineAVG]]: each input's declared NoData (baked into
  * the VRT pixel function as a literal list) is excluded from the statistic, and a pixel that
  * is NoData in ALL inputs becomes NoData in the output (the chosen sentinel is stamped on every
  * output band so downstream consumers can detect it with `GetNoDataValue`). Band count is
  * preserved: K aligned N-band tiles yield an N-band result where output band j reduces band j
  * across all inputs.
  *
  * Conventions (the light tier mirrors these for parity):
  *   - `median` : middle of the sorted valid values; on an EVEN count the MEAN of the two middle
  *                values (numpy `np.ma.median` default; matches `RST_*_RasterToGridMedian`).
  *   - `stddev` : POPULATION standard deviation, ddof=0 (numpy `.std()` default; matches
  *                `RST_*_RasterToGridStddev` and combineavg's plain-mean convention).
  *   - `count`  : number of valid (non-NoData) inputs at the pixel.
  *
  * Output dtype follows [[CombineAVG]] (no new schema): the result keeps the input band's dtype,
  * and when that dtype is integer the statistic is rounded to nearest before the cast — so a
  * fractional statistic (median/stddev/mean count) over an integer stack is rounded to the input
  * dtype exactly as combineavg's mean is.
  *
  * MEMORY: `median` and `stddev` require the FULL stack of valid values to be materialised per
  * pixel block inside the VRT pixel function (numpy holds the stacked array), so peak executor
  * RAM scales with (stack depth x block size). On Serverless (~1 GB/task) this bounds how many /
  * how large the tiles in one combine call can be. `min`/`max`/`sum`/`count` are streaming-friendly
  * in principle but use the same stacked implementation here for a single code path.
  */
object CombineStats {

    /** The supported statistics; the SQL function name is `gbx_rst_combine<stat>`. */
    val Stats: Set[String] = Set("min", "max", "median", "sum", "stddev", "count")

    /** numpy expression reducing the masked stack (`masked`, axis=0) to a per-pixel result array. */
    private def reduceExpr(stat: String): String = stat match {
        case "min"    => "masked.min(axis=0)"
        case "max"    => "masked.max(axis=0)"
        case "sum"    => "masked.sum(axis=0)"
        case "median" => "np.ma.median(masked, axis=0)"
        case "stddev" => "masked.std(axis=0)"          // ddof=0 -> population std
        case "count"  => "counts.astype(np.float64)"
        case other    => throw new IllegalArgumentException(
            s"CombineStats: unsupported stat '$other'; allowed: ${Stats.toSeq.sorted.mkString(", ")}")
    }

    /**
      * Reduce `rasters` (all aligned) to a single tile whose band j is the per-pixel `stat`
      * of band j across every input, excluding NoData. Caller must release the returned Dataset.
      *
      * @throws IllegalArgumentException if `stat` is unknown or the inputs are not grid-aligned.
      */
    def compute(rasters: Array[Dataset], options: Map[String, String], stat: String): (Dataset, Map[String, String]) = {
        require(Stats.contains(stat),
            s"CombineStats: unsupported stat '$stat'; allowed: ${Stats.toSeq.sorted.mkString(", ")}")
        RasterAlignment.requireAligned(rasters.toSeq, s"gbx_rst_combine$stat")

        val sourceNoData: Array[Option[Double]] = rasters.map { ds =>
            val v = BandAccessors.getNoDataValue(ds.GetRasterBand(1))
            if (v.isNaN) None else Some(v)
        }
        val nodataListLiteral = sourceNoData
            .map(_.map(d => f"$d%s").getOrElse("None"))
            .mkString("[", ", ", "]")
        val fallback: Double = sourceNoData.collectFirst { case Some(v) => v }.getOrElse(0.0)
        val fallbackLiteral = f"$fallback%s"
        val fname = s"combine_$stat"
        val reduce = reduceExpr(stat)

        // Self-contained pixel function: depends only on numpy + the two injected literals.
        // `stat` is filled from valid values; all-NoData columns (counts==0) resolve to FALLBACK.
        val pythonFunc =
            s"""
               |import numpy as np
               |
               |NODATA = $nodataListLiteral
               |FALLBACK = $fallbackLiteral
               |
               |def $fname(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
               |    stacked = np.asarray(in_ar, dtype=np.float64)
               |    valid = np.ones(stacked.shape, dtype=bool)
               |    for i, nd in enumerate(NODATA):
               |        if nd is not None:
               |            valid[i] = stacked[i] != nd
               |    counts = valid.sum(axis=0)
               |    masked = np.ma.masked_array(stacked, mask=~valid)
               |    stat = $reduce
               |    filled = np.ma.filled(np.ma.asarray(stat, dtype=np.float64), FALLBACK)
               |    result = np.where(counts > 0, filled, FALLBACK)
               |    if np.issubdtype(out_ar.dtype, np.integer):
               |        np.copyto(out_ar, np.rint(result), casting='unsafe')
               |    else:
               |        np.copyto(out_ar, result, casting='unsafe')
               |""".stripMargin

        // PRESERVE band count (per-band reduce, same as combineavg): all inputs are aligned so
        // the first input's band count is N for the whole stack.
        val bandCount = math.max(rasters.head.GetRasterCount, 1)
        val (resultDs, resultMeta) =
            PixelCombineRasters.combine(rasters, options, pythonFunc, fname, collapseBands = false, bandCount = bandCount)

        // Stamp the chosen NoData onto every output band so callers can detect all-NoData pixels.
        // Only when at least one input declared NoData — otherwise we'd invent an absent sentinel.
        if (sourceNoData.exists(_.isDefined) && resultDs != null) {
            scala.util.Try {
                (1 to resultDs.GetRasterCount).foreach(b => resultDs.GetRasterBand(b).SetNoDataValue(fallback))
                resultDs.FlushCache()
            }
        }

        (resultDs, resultMeta)
    }

}
