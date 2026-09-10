package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.custom.Custom_Grid
import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.gdal
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Name/arity + burn round-trip tests for the custom-grid `RST_Custom_RasterizeAgg` UDAF.
 *  Mirrors [[RST_GridRasterizeAggTest]]; drives `update`/`eval` directly (no Spark session).
 */
class RST_Custom_RasterizeAggTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    private def encodedEmpty(): UTF8String = {
        val cfg = new ExpressionConfig(
            Map.empty[String, String],
            new SerializableConfiguration(new org.apache.hadoop.conf.Configuration()))
        val baos = new java.io.ByteArrayOutputStream()
        val oos  = new java.io.ObjectOutputStream(baos)
        oos.writeObject(cfg); oos.close()
        UTF8String.fromString(java.util.Base64.getEncoder.encodeToString(baos.toByteArray))
    }

    private case class RasterRead(gt: Array[Double], width: Int, height: Int, data: Array[Double])

    private def readRaster(tileRow: Any): RasterRead = {
        val ir    = tileRow.asInstanceOf[InternalRow]
        val bytes = ir.getBinary(1)
        bytes should not be null
        val tmp = s"/vsimem/custom_ragg_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        gdal.FileFromMemBuffer(tmp, bytes)
        val ds = gdal.Open(tmp)
        try {
            val w = ds.getRasterXSize
            val h = ds.getRasterYSize
            val buf = new Array[Double](w * h)
            ds.GetRasterBand(1).ReadRaster(0, 0, w, h, buf)
            RasterRead(ds.GetGeoTransform, w, h, buf)
        } finally {
            ds.delete()
            gdal.Unlink(tmp)
        }
    }

    // Custom grid [0, 1_000_000] x [0, 1_000_000] in EPSG:27700; res 3 => 12_500 m cells.
    private val conf = GridConf(0, 1000000, 0, 1000000, 2, 100000, 100000, Some(27700))
    private val grid = CustomGridSystem(conf)
    private val res  = 3

    private def gridExpr: Expression =
        Custom_Grid(Literal(0L), Literal(1000000L), Literal(0L), Literal(1000000L),
            Literal(2), Literal(100000), Literal(100000), Literal(27700))

    // Three well-separated EPSG:27700 points -> distinct res-3 cells.
    private val points = Seq((500000.0, 500000.0), (525000.0, 525000.0), (475000.0, 475000.0))

    /** Explicit-extent aggregator (xmin..height supplied) so the burn grid is deterministic. */
    private def makeCustomAgg(): RST_Custom_RasterizeAgg =
        RST_Custom_RasterizeAgg(
            cellidExpr    = Literal.create(null, LongType),
            valueExpr     = Literal(0.0),
            gridExpr      = gridExpr,
            outSridExpr   = Literal(27700),
            pixelSizeExpr = Literal.create(null, DoubleType),
            xminExpr      = Literal(460000.0),
            yminExpr      = Literal(460000.0),
            xmaxExpr      = Literal(540000.0),
            ymaxExpr      = Literal(540000.0),
            widthExpr     = Literal(80),
            heightExpr    = Literal(80),
            modeExpr      = Literal("centroids"),
            kringPadExpr  = Literal(1),
            exprConfExpr  = Literal.create(encodedEmpty(), StringType)
        )

    test("custom rasterize_agg canonical name + 13-arg builder") {
        assert(RST_Custom_RasterizeAgg.name == "gbx_rst_custom_rasterize_agg")
        val args = (0 until 13).map(i => Literal(i)).toSeq
        assert(RST_Custom_RasterizeAgg.builder()(args).isInstanceOf[RST_Custom_RasterizeAgg])
    }

    test("custom burn: every covered pixel maps back into the input cell set") {
        val cellIds = points.map { case (x, y) => grid.pointToCellID(x, y, res) }
        val cellSet = cellIds.toSet

        val agg = makeCustomAgg()
        val buf = agg.createAggregationBuffer()
        cellIds.zipWithIndex.foreach { case (c, i) => agg.update(buf, c, (i + 1).toDouble) }

        val result: AnyRef = agg.eval(buf).asInstanceOf[AnyRef]
        result should not be null

        val r = readRaster(result)
        var covered = 0
        var py = 0
        while (py < r.height) {
            var px = 0
            while (px < r.width) {
                val v = r.data(py * r.width + px)
                if (v != -9999.0) {
                    covered += 1
                    val xOffset = 0.5 + px
                    val yOffset = 0.5 + py
                    val cx = r.gt(0) + xOffset * r.gt(1) + yOffset * r.gt(2)
                    val cy = r.gt(3) + xOffset * r.gt(4) + yOffset * r.gt(5)
                    cellSet should contain(grid.pointToCellID(cx, cy, res))
                }
                px += 1
            }
            py += 1
        }
        covered should be >= cellSet.size
    }

    test("custom burn: absent cells are written as -9999.0 NoData") {
        val cellId = grid.pointToCellID(500000.0, 500000.0, res)
        val agg    = makeCustomAgg()
        val buf    = agg.createAggregationBuffer()
        agg.update(buf, cellId, 42.0)

        val r = readRaster(agg.eval(buf).asInstanceOf[AnyRef])
        r.data.count(_ == -9999.0) should be > 0
    }
}
