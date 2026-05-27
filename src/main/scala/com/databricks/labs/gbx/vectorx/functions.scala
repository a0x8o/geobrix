package com.databricks.labs.gbx.vectorx

import com.databricks.labs.gbx.expressions.RegistryDelegate
import com.databricks.labs.gbx.vectorx.expressions.ST_AsMvt
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
  * As of v0.4.0 this package contains a single expression-level function — the
  * `gbx_st_asmvt` MVT aggregator (see [[ST_AsMvt]]). Subsequent waves add more.
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

    /** Convenience overload — pass a plain string as the layer name. */
    def st_asmvt(geomWkb: Column, attrs: Column, layerName: String): Column =
        st_asmvt(geomWkb, attrs, lit(layerName))

}
