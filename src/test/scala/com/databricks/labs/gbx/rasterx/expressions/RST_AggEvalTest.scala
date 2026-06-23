package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.udfs
import com.databricks.labs.gbx.udfs.st_buffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.matchers.should.Matchers._

class RST_AggEvalTest extends PlanTest with SilentSparkSession {

    /**
     * Valid GDAL VRT Python pixel-function signature: (in_ar, out_ar, xoff,
     * yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt,
     * **kwargs). The previous test used `def myfunc(x): return x*2`, which
     * GDAL silently rejected; the surrounding `noException` only checked that
     * nothing threw, so the malformed pyfunc went unnoticed for as long as
     * the underlying PixelCombineRasters bug prevented any pyfunc from
     * actually firing.
     */
    private val doublePyFunc =
        """
          |import numpy as np
          |def myfunc(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
          |    out_ar[:] = np.mean(np.asarray(in_ar, dtype=np.float64), axis=0) * 2
          |""".stripMargin

    test("RST_AggEvalTest should evaluate expressions on raster columns") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        def runQuery(df: DataFrame): Unit = {
            df
                .withColumn("bbox", rst_boundingbox(col("raster")))
                .withColumn("clipper", st_buffer(col("bbox"), lit(-500000.0))) // projection in meters 1 px is ~470m
                .withColumn("raster", rst_clip(col("raster"), col("clipper"), lit(true)))
                .groupBy(lit(1))
                .agg(
                  rst_combineavg_agg(col("raster")),
                  rst_derivedband_agg(col("raster"), doublePyFunc, "myfunc"),
                  rst_merge_agg(col("raster"))
                )
                .collect()
        }

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"),
          (2, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF"),
          (3, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B03.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))

        noException should be thrownBy runQuery(df)

        val df2 = spark.read
            .format("binaryFile")
            .load(tifPath)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))

        noException should be thrownBy runQuery(df2)

    }

    test("rst_combineavg on a single tile column raises a friendly error pointing at the _agg form") {
        // Regression for the user-reported notebook error:
        //   .selectExpr("gbx_rst_combineavg(tile) AS tile")
        // The non-agg form expects ARRAY<tile>; passing a single tile struct
        // previously produced a raw ClassCastException from inside Catalyst
        // analysis. After RST_ExpressionUtil.arrayOfTileRasterType is in
        // place we should see an IllegalArgumentException with a message that
        // names the function, the actual type received, and the aggregator
        // companion that the user likely wanted.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString
        val df = Seq(
          s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
        ).toDF("path").withColumn("tile", udfs.rasterFromPath(col("path")))

        val thrown = intercept[Throwable] {
            df.selectExpr("gbx_rst_combineavg(tile) AS tile").collect()
        }
        // Spark wraps analysis-time IllegalArgumentException so the actual
        // class can be either IllegalArgumentException or one of Spark's
        // catalyst-wrapper types — what matters is that our diagnostic
        // message survives in the chain.
        val joined = LazyList
            .iterate(Option(thrown))(_.flatMap(t => Option(t.getCause)))
            .takeWhile(_.isDefined)
            .flatMap(_.map(_.getMessage).filter(_ != null))
            .mkString(" || ")
        joined should include ("gbx_rst_combineavg expects ARRAY<tile>")
        joined should include ("gbx_rst_combineavg_agg")
    }

    test("rst_derivedband_agg actually transforms pixel values (parity with combineavg_agg fix)") {
        // End-to-end Spark aggregation: three constant Byte tiles (10, 20, 30)
        // averaged then doubled by the pyfunc should yield 40 everywhere
        // (mean(10,20,30)=20, *2 = 40). Before the PixelCombineRasters
        // ordering fix this returned one of the inputs unchanged through the
        // aggregator path, so the output would be 10 / 20 / 30 — not 40.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tmpDir = java.nio.file.Files.createTempDirectory("gbx_derivedband_agg_").toFile

        val w = 8; val h = 8
        // Inline byte raster writer; can't reuse RST_AggregationsTest's helper
        // from a separate suite without lifting it to a shared location and
        // this is the only place outside that suite that needs it.
        def writeByteConst(p: String, v: Int): Unit = {
            val drv = gdal.GetDriverByName("GTiff")
            val ds = drv.Create(p, w, h, 1, gdalconstConstants.GDT_Byte, Array[String]("COMPRESS=DEFLATE"))
            ds.SetGeoTransform(Array[Double](149.0, 0.01, 0.0, -35.0, 0.0, -0.01))
            val sr = new org.gdal.osr.SpatialReference()
            sr.ImportFromEPSG(4326)
            ds.SetProjection(sr.ExportToWkt())
            val band = ds.GetRasterBand(1)
            band.WriteRaster(0, 0, w, h, Array.fill[Byte](w * h)(v.toByte))
            band.FlushCache()
            ds.FlushCache()
            ds.delete()
        }
        val paths = Seq(10, 20, 30).map { v =>
            val p = s"${tmpDir.getAbsolutePath}/const_$v.tif"
            writeByteConst(p, v)
            p
        }

        try {
            val agg = paths.toDF("path")
                .withColumn("tile", udfs.rasterFromPath(col("path")))
                .groupBy(lit(1).alias("g"))
                .agg(rst_derivedband_agg(col("tile"), doublePyFunc, "myfunc").alias("out"))
                .select(col("out.raster").alias("raster"))

            val bytes = agg.collect().head.getAs[Array[Byte]]("raster")
            // Decode in-memory GTiff bytes; verify uniform 40.
            val mem = s"/vsimem/derivedband_agg_check_${java.util.UUID.randomUUID()}.tif"
            gdal.FileFromMemBuffer(mem, bytes)
            val ds = gdal.Open(mem)
            try {
                val buf = Array.ofDim[Double](ds.GetRasterXSize * ds.GetRasterYSize)
                ds.GetRasterBand(1).ReadRaster(0, 0, ds.GetRasterXSize, ds.GetRasterYSize, gdalconstConstants.GDT_Float64, buf)
                buf.min shouldBe 40.0 +- 0.5
                buf.max shouldBe 40.0 +- 0.5
            } finally {
                RasterDriver.releaseDataset(ds)
                gdal.Unlink(mem)
            }
        } finally {
            tmpDir.listFiles().foreach(_.delete())
            tmpDir.delete()
        }
    }

    test("rst_merge_agg overlap winner is deterministic regardless of row order") {
        // Two same-size tiles whose extents overlap (origins differ by half a tile),
        // filled with distinct constants. The mosaic is last-wins; the aggregator now
        // orders tiles by their raw serialized content, so the same tile reliably wins
        // the overlap regardless of the order rows reach the aggregator.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tmpDir = java.nio.file.Files.createTempDirectory("gbx_merge_agg_det_").toFile
        val w = 8; val h = 8
        // part_0: origin (149.0, -35.0); part_1: origin shifted +0.04 east / -0.04 south
        // (half the 8 px * 0.01 extent) so the two extents overlap in their inner corner.
        def writeByteConst(p: String, v: Int, ox: Double, oy: Double): Unit = {
            val drv = gdal.GetDriverByName("GTiff")
            val ds = drv.Create(p, w, h, 1, gdalconstConstants.GDT_Byte, Array[String]("COMPRESS=DEFLATE"))
            ds.SetGeoTransform(Array[Double](ox, 0.01, 0.0, oy, 0.0, -0.01))
            val sr = new org.gdal.osr.SpatialReference()
            sr.ImportFromEPSG(4326)
            ds.SetProjection(sr.ExportToWkt())
            val band = ds.GetRasterBand(1)
            band.WriteRaster(0, 0, w, h, Array.fill[Byte](w * h)(v.toByte))
            band.FlushCache(); ds.FlushCache(); ds.delete()
        }
        val p0 = s"${tmpDir.getAbsolutePath}/part_0.tif"  // origin 149.00, value 10
        val p1 = s"${tmpDir.getAbsolutePath}/part_1.tif"  // origin 149.04, value 20 (wins overlap)
        writeByteConst(p0, 10, 149.0, -35.0)
        writeByteConst(p1, 20, 149.04, -35.04)

        def mergeMean(order: Seq[String]): Double = {
            val bytes = order.toDF("path")
                .withColumn("tile", udfs.rasterFromPath(col("path")))
                .groupBy(lit(1).alias("g"))
                .agg(rst_merge_agg(col("tile")).alias("out"))
                .select(col("out.raster").alias("raster"))
                .collect().head.getAs[Array[Byte]]("raster")
            val mem = s"/vsimem/merge_agg_det_${java.util.UUID.randomUUID()}.tif"
            gdal.FileFromMemBuffer(mem, bytes)
            val ds = gdal.Open(mem)
            try {
                val n = ds.GetRasterXSize * ds.GetRasterYSize
                val buf = Array.ofDim[Double](n)
                ds.GetRasterBand(1).ReadRaster(0, 0, ds.GetRasterXSize, ds.GetRasterYSize,
                    gdalconstConstants.GDT_Float64, buf)
                buf.sum / n
            } finally { RasterDriver.releaseDataset(ds); gdal.Unlink(mem) }
        }

        try {
            // Same group, both row orders -> identical mosaic (content sort, not arrival).
            val meanAB = mergeMean(Seq(p0, p1))
            val meanBA = mergeMean(Seq(p1, p0))
            meanAB shouldBe meanBA +- 1e-9
        } finally {
            tmpDir.listFiles().foreach(_.delete()); tmpDir.delete()
        }
    }

    test("rst_merge_agg same-origin overlap winner is deterministic for in-memory tiles") {
        // The residual nondeterminism hole the content-key fix closes: two tiles sharing
        // the SAME geotransform origin but different content fully overlap. The previous
        // key (origin, GetDescription) tied on origin and fell back to GetDescription --
        // for BinaryType (rst_fromcontent) tiles that is a per-open /vsimem/<uuid> path,
        // i.e. random -- so the last-wins winner varied run to run. Sorting on raw content
        // gives a total order with no tie, so the winner is fixed regardless of row order.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val w = 8; val h = 8
        // Two byte rasters, IDENTICAL georef/origin, distinct constant fills -> they fully
        // overlap; the canonical content order alone decides the winner.
        def byteConstBytes(v: Int): Array[Byte] = {
            val mem = s"/vsimem/merge_agg_sameorigin_src_${java.util.UUID.randomUUID()}.tif"
            val drv = gdal.GetDriverByName("GTiff")
            val ds = drv.Create(mem, w, h, 1, gdalconstConstants.GDT_Byte, Array[String]("COMPRESS=DEFLATE"))
            ds.SetGeoTransform(Array[Double](149.0, 0.01, 0.0, -35.0, 0.0, -0.01))
            val sr = new org.gdal.osr.SpatialReference()
            sr.ImportFromEPSG(4326)
            ds.SetProjection(sr.ExportToWkt())
            val band = ds.GetRasterBand(1)
            band.WriteRaster(0, 0, w, h, Array.fill[Byte](w * h)(v.toByte))
            band.FlushCache(); ds.FlushCache(); ds.delete()
            val buf = gdal.GetMemFileBuffer(mem)
            gdal.Unlink(mem)
            buf
        }
        val a = byteConstBytes(10)
        val b = byteConstBytes(20)

        def mergeMean(order: Seq[Array[Byte]]): Double = {
            val bytes = order.toDF("content")
                .withColumn("tile", rst_fromcontent(col("content"), lit("GTiff")))
                .groupBy(lit(1).alias("g"))
                .agg(rst_merge_agg(col("tile")).alias("out"))
                .select(col("out.raster").alias("raster"))
                .collect().head.getAs[Array[Byte]]("raster")
            val mem = s"/vsimem/merge_agg_sameorigin_${java.util.UUID.randomUUID()}.tif"
            gdal.FileFromMemBuffer(mem, bytes)
            val ds = gdal.Open(mem)
            try {
                val n = ds.GetRasterXSize * ds.GetRasterYSize
                val buf = Array.ofDim[Double](n)
                ds.GetRasterBand(1).ReadRaster(0, 0, ds.GetRasterXSize, ds.GetRasterYSize,
                    gdalconstConstants.GDT_Float64, buf)
                buf.sum / n
            } finally { RasterDriver.releaseDataset(ds); gdal.Unlink(mem) }
        }

        // Both row orders must yield the SAME constant mosaic (one tile wins everywhere).
        val meanAB = mergeMean(Seq(a, b))
        val meanBA = mergeMean(Seq(b, a))
        meanAB shouldBe meanBA +- 1e-9
        // The winner is one of the two inputs (10 or 20), uniform across the tile.
        (meanAB === 10.0 +- 1e-9 || meanAB === 20.0 +- 1e-9) shouldBe true
    }

}
