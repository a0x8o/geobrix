package com.databricks.labs.gbx.rasterx.expressions.web

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operator.{GDALTranslate, GDALWarp}
import com.databricks.labs.gbx.rasterx.tile.TileMath
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}

import scala.util.Try

/** Render a single web-mercator XYZ tile from a raster.
 *
 *  Returns BINARY bytes of the PNG / JPEG / WEBP tile at `(z, x, y)`. Per-tile primitive:
 *
 *    1. Compute the EPSG:3857 bbox of the tile via [[TileMath.tileBboxWebMerc]].
 *    2. `gdal.Warp` the source into a `size × size` raster covering exactly that bbox
 *       (`-te xmin ymin xmax ymax -t_srs EPSG:3857 -ts size size -r <resampling>`).
 *    3. `gdal.Translate -of <format>` to materialize PNG / JPEG / WEBP bytes.
 *    4. Read the bytes back from `/vsimem/`.
 *
 *  Out-of-extent tiles return a transparent PNG (alpha=0) of the requested size — NOT
 *  null. Slippy-map tile servers expect a 200-status non-zero body even outside source
 *  coverage; returning null would surface as a 404 in the publishing pipeline.
 *
 *  Defaults: `format = "PNG"`, `size = 256`, `resampling = "bilinear"`.
 */
case class RST_TileXYZ(
    tileExpr: Expression,
    zExpr: Expression,
    xExpr: Expression,
    yExpr: Expression,
    formatExpr: Expression,
    sizeExpr: Expression,
    resamplingExpr: Expression
) extends InvokedExpression {

    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] =
        Seq(tileExpr, zExpr, xExpr, yExpr, formatExpr, sizeExpr, resamplingExpr, ExpressionConfigExpr())
    override def dataType: DataType = BinaryType
    override def nullable: Boolean = true
    override def prettyName: String = RST_TileXYZ.name
    override def replacement: Expression = rstInvoke(RST_TileXYZ, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6))
}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_TileXYZ extends WithExpressionInfo {

    /** GDAL drivers that can act as XYZ tile output formats. */
    private val AllowedFormats: Set[String] = Set("PNG", "JPEG", "WEBP")

    /** Allowed GDAL warp resampling algorithms. */
    private val AllowedResampling: Set[String] = Set(
        "near", "bilinear", "cubic", "cubicspline", "lanczos",
        "average", "mode", "max", "min", "med", "q1", "q3"
    )

    // Spark sends Python ints as LongType — we accept both Int and Long overloads. Int
    // overloads are needed for SQL literal default args; Long overloads cover the
    // PySpark-from-notebook case (Wave 3 found this in Quadbin_PointAsCell).
    def evalBinary(row: InternalRow, z: Int, x: Int, y: Int, format: UTF8String, size: Int, resampling: UTF8String, conf: UTF8String): Array[Byte] =
        doInvoke(row, z, x, y, format, size, resampling, conf, BinaryType)
    def evalBinary(row: InternalRow, z: Long, x: Long, y: Long, format: UTF8String, size: Long, resampling: UTF8String, conf: UTF8String): Array[Byte] =
        doInvoke(row, z.toInt, x.toInt, y.toInt, format, size.toInt, resampling, conf, BinaryType)
    def evalPath(row: InternalRow, z: Int, x: Int, y: Int, format: UTF8String, size: Int, resampling: UTF8String, conf: UTF8String): Array[Byte] =
        doInvoke(row, z, x, y, format, size, resampling, conf, StringType)
    def evalPath(row: InternalRow, z: Long, x: Long, y: Long, format: UTF8String, size: Long, resampling: UTF8String, conf: UTF8String): Array[Byte] =
        doInvoke(row, z.toInt, x.toInt, y.toInt, format, size.toInt, resampling, conf, StringType)

    private def doInvoke(
        row: InternalRow,
        z: Int, x: Int, y: Int,
        format: UTF8String, size: Int, resampling: UTF8String,
        conf: UTF8String, dt: DataType
    ): Array[Byte] = {
        val safe: () => Array[Byte] = () => {
            val exprConf = ExpressionConfig.fromB64(conf.toString)
            RST_ExpressionUtil.init(exprConf)
            val fmtStr = if (format == null) "PNG" else format.toString
            val resampleStr = if (resampling == null) "bilinear" else resampling.toString
            // scalastyle:off caselocale
            val fmt = fmtStr.toUpperCase
            val resampleLower = resampleStr.toLowerCase
            // scalastyle:on caselocale
            require(AllowedFormats.contains(fmt), s"rst_tilexyz: format must be one of ${AllowedFormats.mkString(", ")}; got '$fmtStr'")
            require(AllowedResampling.contains(resampleLower),
                s"rst_tilexyz: unsupported resampling '$resampleStr'; allowed: ${AllowedResampling.toSeq.sorted.mkString(", ")}")
            require(size > 0 && size <= 4096, s"rst_tilexyz: size must be in (0, 4096]; got $size")
            val (_, ds, options) = RasterSerializationUtil.rowToTile(row, dt)
            try execute(ds, options, z, x, y, fmt, size, resampleLower)
            finally RasterDriver.releaseDataset(ds)
        }
        // safeEval wraps Throwables → null; for BinaryType callers want bytes, never null.
        // On hard failure we still want a transparent PNG, so wrap the safe-eval ourselves.
        val result = Try(safe()).toOption.flatMap(Option(_))
        result.getOrElse(transparentPng(size))
    }

    /** Render the tile by warping `ds` to the (z,x,y) bbox + translating to bytes.
     *  If the tile bbox does not overlap the source extent, return a transparent PNG.
     */
    def execute(
        ds: Dataset,
        options: Map[String, String],
        z: Int, x: Int, y: Int,
        format: String, size: Int, resampling: String
    ): Array[Byte] = {
        val (xmin, ymin, xmax, ymax) = TileMath.tileBboxWebMerc(z, x, y)
        if (!datasetIntersectsWebMercBbox(ds, xmin, ymin, xmax, ymax)) {
            return transparentPng(size)
        }
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val warpPath = s"/vsimem/tilexyz_warp_$uuid.tif"
        val (warpedDs, warpedOpts) = GDALWarp.executeWarp(
          warpPath,
          Array(ds),
          // Inject format=GTiff into the intermediate so OperatorOptions.appendOptions
          // does not stamp PNG-specific flags on the warp step (we translate to PNG below).
          options ++ Map("format" -> "GTiff"),
          command = s"gdalwarp -t_srs EPSG:3857 -te $xmin $ymin $xmax $ymax -ts $size $size -r $resampling"
        )
        try {
            val extension = format.toLowerCase match {
                case "png"  => "png"
                case "jpeg" => "jpg"
                case "webp" => "webp"
                case other  => throw new IllegalArgumentException(s"rst_tilexyz: unknown format $other")
            }
            val translatePath = s"/vsimem/tilexyz_out_$uuid.$extension"
            val (resDs, _) = GDALTranslate.executeTranslate(
              translatePath,
              warpedDs,
              command = "gdal_translate",
              warpedOpts ++ Map("format" -> format, "extension" -> extension)
            )
            Try(resDs.FlushCache())
            Try(resDs.delete())
            val bytes = gdal.GetMemFileBuffer(translatePath)
            gdal.Unlink(translatePath)
            if (bytes == null || bytes.isEmpty) transparentPng(size) else bytes
        } finally {
            RasterDriver.releaseDataset(warpedDs)
        }
    }

    /** Cheap intersection test against the dataset's web-mercator extent. We assume the
     *  source has been warped to EPSG:3857 OR has a known SRS; if neither, fall back to
     *  the WGS84 world-bbox transform (i.e. assume coverage).
     */
    private def datasetIntersectsWebMercBbox(
        ds: Dataset, xmin: Double, ymin: Double, xmax: Double, ymax: Double
    ): Boolean = Try {
        val gt = ds.GetGeoTransform()
        val w = ds.GetRasterXSize.toDouble
        val h = ds.GetRasterYSize.toDouble
        val srcXmin = math.min(gt(0), gt(0) + w * gt(1) + h * gt(2))
        val srcXmax = math.max(gt(0), gt(0) + w * gt(1) + h * gt(2))
        val srcYmax = math.max(gt(3), gt(3) + w * gt(4) + h * gt(5))
        val srcYmin = math.min(gt(3), gt(3) + w * gt(4) + h * gt(5))
        // If source is not in EPSG:3857, this comparison is approximate — but it's only
        // used to short-circuit when there's clearly no overlap (e.g. the user asked
        // for a tile half a world away). For ambiguous cases we just warp and let GDAL
        // produce an empty raster — the bytes check at the end catches that.
        val srs = ds.GetSpatialRef
        if (srs != null && srs.GetAuthorityCode(null) == "3857") {
            !(srcXmax < xmin || srcXmin > xmax || srcYmax < ymin || srcYmin > ymax)
        } else {
            // Source not in 3857 — be permissive (let GDAL try; empty output ⇒ transparent).
            true
        }
    }.getOrElse(true)

    /** Returns a minimal RGBA transparent PNG of `size × size`. */
    private def transparentPng(size: Int): Array[Byte] = {
        val drv = gdal.GetDriverByName("MEM")
        val src = drv.Create("", size, size, 4, org.gdal.gdalconst.gdalconstConstants.GDT_Byte)
        // All bands are already zero-initialized — alpha=0 ⇒ fully transparent.
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val outPath = s"/vsimem/tilexyz_empty_$uuid.png"
        val (resDs, _) = GDALTranslate.executeTranslate(
          outPath,
          src,
          command = "gdal_translate",
          Map("format" -> "PNG", "extension" -> "png")
        )
        Try(resDs.FlushCache())
        Try(resDs.delete())
        val bytes = gdal.GetMemFileBuffer(outPath)
        gdal.Unlink(outPath)
        Try(src.delete())
        bytes
    }

    override def name: String = "gbx_rst_tilexyz"

    /** Builder: 4 to 7 args (tile, z, x, y, [format, [size, [resampling]]]). */
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => {
        c.length match {
            case 4 => RST_TileXYZ(c(0), c(1), c(2), c(3), Literal("PNG"), Literal(256), Literal("bilinear"))
            case 5 => RST_TileXYZ(c(0), c(1), c(2), c(3), c(4), Literal(256), Literal("bilinear"))
            case 6 => RST_TileXYZ(c(0), c(1), c(2), c(3), c(4), c(5), Literal("bilinear"))
            case 7 => RST_TileXYZ(c(0), c(1), c(2), c(3), c(4), c(5), c(6))
            case n => throw new IllegalArgumentException(
                s"gbx_rst_tilexyz takes 4 to 7 arguments (tile, z, x, y, [format, [size, [resampling]]]); got $n"
            )
        }
    }
}
