package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.expressions.RegistryDelegate
import org.apache.spark.sql.SparkSession

/**
  * GridX Custom Grid API entry point: register all custom-grid SQL functions.
  *
  * Call `functions.register(spark)` once per session to make `gbx_custom_*` functions available
  * (grid spec, point-as-cell, cell geometry, k-ring, polyfill, etc.).
  */
object functions extends Serializable {

    val flag = "com.databricks.labs.gbx.gridx.custom.registered"

    /** Register all custom-grid expressions with Spark; idempotent per session. */
    def register(spark: SparkSession): Unit = {
        val sc = spark.sparkContext
        if (sc.getConf.get(flag, "false") == "true") return

        val registry = spark.sessionState.functionRegistry
        val rd = RegistryDelegate(registry)

        rd.register(Custom_Grid)
        rd.register(Custom_PointAsCell)
        rd.register(Custom_AsWKB)
        rd.register(Custom_AsWKT)
        rd.register(Custom_Centroid)
        rd.register(Custom_Polyfill)
        rd.register(Custom_KRing)

        sc.getConf.set(flag, "true")
    }

}
