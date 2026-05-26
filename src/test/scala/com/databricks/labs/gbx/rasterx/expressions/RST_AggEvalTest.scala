package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
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
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))

        noException should be thrownBy runQuery(df)

        val df2 = spark.read
            .format("binaryFile")
            .load(tifPath)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))

        noException should be thrownBy runQuery(df2)

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
                .withColumn("tile", rst_fromfile(col("path"), lit("GTiff")))
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

}
