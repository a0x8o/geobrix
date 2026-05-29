package com.databricks.labs.gbx.vectorx

import com.databricks.labs.gbx.expressions.RegistryDelegate
import com.databricks.labs.gbx.vectorx.expressions.{ST_AsMvt, ST_AsMvtPyramid, ST_InterpolateElevationBBox, ST_InterpolateElevationGeom, ST_Triangulate}
import com.databricks.labs.gbx.vectorx.mvt.MvtWriter
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

/**
  * VectorX API entry point: register expression-level vector SQL functions and provide
  * Column-based helpers.
  *
  * Call `functions.register(spark)` once per session to make `gbx_st_*` expression
  * functions available in SQL. (VectorX data sources are registered separately via
  * `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.)
  *
  * As of v0.4.0 this package exposes the `gbx_st_asmvt` MVT aggregator (see [[ST_AsMvt]])
  * and the `gbx_st_asmvt_pyramid` generator (see [[ST_AsMvtPyramid]]); subsequent waves
  * add more.
  */
object functions extends Serializable {

    val flag = "com.databricks.labs.gbx.vectorx.registered"

    /** Register all VectorX expressions with Spark; idempotent per session. */
    def register(spark: SparkSession): Unit = {
        val sc = spark.sparkContext
        if (sc.getConf.get(flag, "false") == "true") return

        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)

        // Aggregators
        rd.register(ST_AsMvt)

        // Generators
        rd.register(ST_AsMvtPyramid)
        rd.register(ST_Triangulate)
        rd.register(ST_InterpolateElevationBBox)
        rd.register(ST_InterpolateElevationGeom)

        sc.getConf.set(flag, "true")
    }

    /**
      * Aggregator: encode a group of features into a Mapbox Vector Tile (MVT) protobuf blob.
      *
      * @param geomWkb   per-row geometry in WKB (BINARY) in tile-local coordinates
      * @param attrs     per-row attribute struct (all fields stringified in v0.4.0)
      * @param layerName constant Column holding the MVT layer name
      */
    def st_asmvt(geomWkb: Column, attrs: Column, layerName: Column): Column =
        ColumnAdapter(ST_AsMvt.name, Seq(geomWkb, attrs, layerName))

    /** Convenience overload - pass a plain string as the layer name. */
    def st_asmvt(geomWkb: Column, attrs: Column, layerName: String): Column =
        st_asmvt(geomWkb, attrs, lit(layerName))

    /**
      * Generator: explode one `(geom_wkb, attrs)` row into one row per intersecting
      * `(z, x, y)` tile in `[min_z, max_z]`, encoded as MVT bytes. Geometry assumed
      * EPSG:4326. Output column is a single struct `tile: STRUCT<z, x, y, mvt_bytes>`.
      *
      * @param geomWkb   per-feature geometry in WKB (BINARY); EPSG:4326 lon/lat
      * @param attrs     per-feature attribute struct (all fields stringified in v0.4.0)
      * @param minZ      inclusive minimum zoom level
      * @param maxZ      inclusive maximum zoom level (<= 20)
      * @param layerName constant Column holding the MVT layer name
      * @param extent    MVT tile extent in pixels (default 4096)
      */
    def st_asmvt_pyramid(
        geomWkb: Column, attrs: Column, minZ: Column, maxZ: Column,
        layerName: Column, extent: Column
    ): Column =
        ColumnAdapter(ST_AsMvtPyramid.name, Seq(geomWkb, attrs, minZ, maxZ, layerName, extent))

    /** Convenience overload - extent defaults to the MVT v2 standard (4096). */
    def st_asmvt_pyramid(
        geomWkb: Column, attrs: Column, minZ: Column, maxZ: Column, layerName: Column
    ): Column =
        ColumnAdapter(
            ST_AsMvtPyramid.name,
            Seq(geomWkb, attrs, minZ, maxZ, layerName, lit(MvtWriter.DefaultExtent))
        )

    /** Convenience overload - Int zooms, String layer name (auto-lit-wrapped). */
    def st_asmvt_pyramid(
        geomWkb: Column, attrs: Column, minZ: Int, maxZ: Int, layerName: String
    ): Column =
        st_asmvt_pyramid(geomWkb, attrs, lit(minZ), lit(maxZ), lit(layerName))

    /** Convenience overload - Int zooms + extent, String layer name (auto-lit-wrapped). */
    def st_asmvt_pyramid(
        geomWkb: Column, attrs: Column, minZ: Int, maxZ: Int, layerName: String, extent: Int
    ): Column =
        st_asmvt_pyramid(geomWkb, attrs, lit(minZ), lit(maxZ), lit(layerName), lit(extent))

}
