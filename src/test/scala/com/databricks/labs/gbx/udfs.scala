package com.databricks.labs.gbx

import com.databricks.labs.gbx.rasterx.functions.rst_fromcontent
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{lit, udf}

import scala.util.Try

object udfs {

    // ---- raster test fixtures --------------------------------------------------
    // RST_FromFile (the Scala gbx_rst_fromfile expression) was removed: the function is
    // lightweight-only (a pyrx Python UDF), because the JVM cannot read UC Volumes (see
    // rasterx/functions.scala register / issue #34). Scala tests that loaded LOCAL test
    // rasters via rst_fromfile read the bytes JVM-side here and build the tile with
    // rst_fromcontent -- identical decoded pixels, drop-in per-row semantics.

    /** Read a local file path (bare or `file:`-scheme URI) into raw bytes. */
    def readLocalBytes: UserDefinedFunction =
        udf((p: String) => {
            val uri = new java.net.URI(p)
            val path =
                if (uri.getScheme != null) java.nio.file.Paths.get(uri)
                else java.nio.file.Paths.get(p)
            java.nio.file.Files.readAllBytes(path)
        })

    /** Drop-in replacement for the removed `rst_fromfile` in tests: a raster tile
      * `Column` built from a local file-path `Column`. */
    def rasterFromPath(pathCol: Column, driver: String = "GTiff"): Column =
        rst_fromcontent(readLocalBytes(pathCol), lit(driver))

    def st_aswkb: UserDefinedFunction =
        udf((wkt: String) => {
            JTS.toWKB(JTS.fromWKT(wkt))
        })

    def st_aswkt: UserDefinedFunction =
        udf((wkb: Array[Byte]) => {
            JTS.toWKT(JTS.fromWKB(wkb))
        })

    def st_buffer: UserDefinedFunction =
        udf((wkb: Array[Byte], distance: Double) => {
            JTS.toWKB(JTS.fromWKB(wkb).buffer(distance))
        })

    def st_area: UserDefinedFunction =
        udf((wkb: Array[Byte]) => {
            Try(JTS.fromWKB(wkb).getArea).getOrElse(0.0)
        })

    def st_type: UserDefinedFunction = {
        udf((wkb: Array[Byte]) => {
            JTS.fromWKB(wkb).getGeometryType
        })
    }

}
