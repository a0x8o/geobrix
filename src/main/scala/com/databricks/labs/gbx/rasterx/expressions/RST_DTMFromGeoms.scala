package com.databricks.labs.gbx.rasterx.expressions

/** DTM from points and breaklines (Delaunay interpolation + rasterize).
 * Not yet implemented for production: expression is not registered in functions.
 * Excluded from scoverage (see pom.xml excludedFiles).
 */
import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.{GDALRasterize, InterpolateElevation}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil, VectorRasterBridge}
import com.databricks.labs.gbx.util.SerializationUtil
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Geometry, LineString}

case class RST_DTMFromGeoms(
    pointsArray: Expression,
    linesArray: Expression,
    mergeTolerance: Expression,
    snapTolerance: Expression,
    splitPointFinder: Expression,
    gridOrigin: Expression,
    gridWidthX: Expression,
    gridWidthY: Expression,
    gridSizeX: Expression,
    gridSizeY: Expression,
    noData: Expression
) extends InvokedExpression {

    def firstElementType: DataType = pointsArray.dataType.asInstanceOf[ArrayType].elementType
    def secondElementType: DataType = linesArray.dataType.asInstanceOf[ArrayType].elementType

    override def children: Seq[Expression] =
        Seq(
          pointsArray,
          linesArray,
          mergeTolerance,
          snapTolerance,
          splitPointFinder,
          gridOrigin,
          gridWidthX,
          gridWidthY,
          gridSizeX,
          gridSizeY,
          noData,
          ExpressionConfigExpr()
        )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(BinaryType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_DTMFromGeoms.name
    override def replacement: Expression = invoke(RST_DTMFromGeoms)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9), nc(10))

}

object RST_DTMFromGeoms extends WithExpressionInfo {

    def eval(
        pointsArray: ArrayData,
        linesArray: ArrayData,
        mergeTolerance: Double,
        snapTolerance: Double,
        splitPointFinder: UTF8String,
        gridOrigin: Any,
        gridWindow: (Int, Int, Double, Double),
        noData: Double,
        conf: UTF8String,
        dts: (DataType, DataType, DataType)
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (pdt, ldt, odt) = dts
              val (gridWidthX, gridWidthY, gridSizeX, gridSizeY) = gridWindow
              val geomPoints = JTS.fromArrayData(pointsArray, pdt)
              val geomLines = JTS.fromArrayData(linesArray, ldt).map(_.asInstanceOf[LineString])
              val multiPointGeom = JTS.multiPoint(geomPoints)
              val origin = (odt match {
                  case StringType => JTS.fromWKT(gridOrigin.asInstanceOf[UTF8String].toString)
                  case BinaryType => JTS.fromWKB(gridOrigin.asInstanceOf[Array[Byte]])
              }).getCentroid

              val gridPoints = InterpolateElevation.pointGrid(origin, gridWidthX, gridWidthY, gridSizeX, gridSizeY)
              val interpolatedPoints = InterpolateElevation
                  .interpolate(multiPointGeom, geomLines, gridPoints, mergeTolerance, snapTolerance)

              val outputRaster = GDALRasterize.executeRasterize(
                interpolatedPoints,
                None,
                origin,
                gridWidthX,
                gridWidthY,
                gridSizeX,
                gridSizeY,
                noData,
                Map.empty
              )

              val res = RasterSerializationUtil.tileToRow((0L, outputRaster._1, outputRaster._2), BinaryType, exprConf.hConf)
              RasterDriver.releaseDataset(outputRaster._1)
              res
          },
          pointsArray, // TODO: this will need fixing
          StringType
        )

    /** Pure compute path shared by the non-agg expression and the aggregator.
     *  Builds a constrained-Delaunay TIN from `points` (+ optional `breaklines`),
     *  interpolates Z at the bbox cell centers, and writes a single-band Float64
     *  GTiff tile. Cells outside the triangulated hull are `noData`.
     */
    def execute(
        points: Seq[Geometry],
        breaklines: Seq[LineString],
        mergeTolerance: Double,
        snapTolerance: Double,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, srid: Int,
        noData: Double
    ): InternalRow = {
        // Materialize rootPath defensively
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        java.nio.file.Files.createDirectories(NodeFilePathUtil.rootPath)
        require(widthPx > 0,  s"rst_dtmfromgeoms: width_px must be positive; got $widthPx")
        require(heightPx > 0, s"rst_dtmfromgeoms: height_px must be positive; got $heightPx")
        require(xmax > xmin,  s"rst_dtmfromgeoms: xmax ($xmax) must be > xmin ($xmin)")
        require(ymax > ymin,  s"rst_dtmfromgeoms: ymax ($ymax) must be > ymin ($ymin)")
        require(points.nonEmpty, "rst_dtmfromgeoms: at least one point is required")

        val mp = JTS.multiPoint(points.toArray)
        mp.setSRID(srid)
        val grid = InterpolateElevation.pointGridBBox(xmin, ymin, xmax, ymax, widthPx, heightPx, srid)
        val interpolated = InterpolateElevation.interpolate(mp, breaklines, grid, mergeTolerance, snapTolerance)

        val ds = VectorRasterBridge.buildEmptyRaster(xmin, ymin, xmax, ymax, widthPx, heightPx, srid, noData)
        try {
            val xRes = (xmax - xmin) / widthPx
            val yRes = (ymax - ymin) / heightPx
            val arr = Array.fill[Double](widthPx * heightPx)(noData)
            interpolated.foreach { p =>
                val col = math.floor((p.getX - xmin) / xRes).toInt
                val r   = math.floor((ymax - p.getY) / yRes).toInt
                if (col >= 0 && col < widthPx && r >= 0 && r < heightPx) {
                    arr(r * widthPx + col) = p.getCoordinate.getZ
                }
            }
            ds.GetRasterBand(1).WriteRaster(0, 0, widthPx, heightPx, arr)
            ds.FlushCache()
            tileRow(VectorRasterBridge.toGTiffBytes(ds))
        } finally {
            ds.delete()
        }
    }

    /** Build the (index_id, raster, metadata) tile row downstream serializers expect. */
    def tileRow(bytes: Array[Byte]): InternalRow = {
        val mtd = Map(
            "driver" -> "GTiff",
            "extension" -> "tif",
            "size" -> bytes.length.toString,
            "parentPath" -> "",
            "all_parents" -> "",
            "last_command" -> "gbx_rst_dtmfromgeoms"
        )
        InternalRow.fromSeq(Seq(0L, bytes, SerializationUtil.toMapData[String, String](mtd)))
    }

    override def name: String = "gbx_rst_dtmfromgeoms"

    override def builder(): FunctionBuilder =
        (c: Seq[Expression]) => new RST_DTMFromGeoms(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10))

}
