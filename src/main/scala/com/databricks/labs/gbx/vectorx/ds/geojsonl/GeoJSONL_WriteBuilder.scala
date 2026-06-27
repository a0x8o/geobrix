package com.databricks.labs.gbx.vectorx.ds.geojsonl

import com.databricks.labs.gbx.util.HadoopUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsTruncate, Write, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
  * WriteBuilder for the `geojsonl_ogr` DataSource.
  *
  * Requires `.mode("overwrite")`: Spark calls [[truncate]] for overwrite writes and `build()`
  * directly for append/default writes, so a write that reaches `build()` without `truncate()`
  * having been called is an append and is rejected (matching the lightweight writer and the
  * other vector writers in v0.4.0).
  *
  * On overwrite, the target directory is cleared ONCE here on the driver (the WriteBuilder is
  * constructed once on the driver, before any task runs) so stale shards from a prior write are
  * gone before the executors land new ones.
  */
class GeoJSONL_WriteBuilder(schema: StructType, options: Map[String, String])
    extends WriteBuilder with SupportsTruncate {

    private var truncated: Boolean = false

    /** Spark calls truncate() for `.mode("overwrite")`; record it so build() knows it's not append. */
    override def truncate(): WriteBuilder = {
        truncated = true
        this
    }

    /** Builds the Write; rejects append, clears the target dir on overwrite. */
    override def build(): Write = {
        val path = options.getOrElse("path",
            throw new IllegalArgumentException(
                "geojsonl_ogr DataSource requires a path option (use .save(path))."))
        if (!truncated) {
            throw new IllegalArgumentException(
                "geojsonl_ogr does not support append; use .mode(\"overwrite\").")
        }
        val spark = SparkSession.builder().getOrCreate()
        val hConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())

        // Clear the target directory ONCE on the driver, then recreate it empty so executors
        // can copy shards in. Recursive delete (no rename) is FUSE-safe on DBFS/Volumes.
        val outPath = new Path(HadoopUtils.cleanPath(path))
        val fs = outPath.getFileSystem(hConf.value)
        if (fs.exists(outPath)) fs.delete(outPath, true)
        fs.mkdirs(outPath)

        new Write {
            override def toBatch: BatchWrite = new GeoJSONL_BatchWrite(schema, path, options, hConf)
        }
    }
}
