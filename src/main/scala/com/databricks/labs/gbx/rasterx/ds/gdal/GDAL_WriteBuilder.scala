package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.util.HadoopUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsTruncate, Write, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/** WriteBuilder for GDAL: produces a Write whose toBatch is GDAL_BatchWrite.
  *
  * Supports `.mode("overwrite")` via SupportsTruncate: Spark calls [[truncate]] for overwrite
  * writes and `build()` directly for append writes. On overwrite, the target directory is cleared
  * ONCE here on the driver (the WriteBuilder is constructed once on the driver, before any task
  * runs) so stale rasters from a prior write are gone before executors land new ones. Recursive
  * delete (no rename) is FUSE-safe on DBFS/Volumes. Append leaves existing files in place. The
  * default ErrorIfExists save mode is not a valid DataSource-V2 batch mode — use overwrite/append.
  */
class GDAL_WriteBuilder(schema: StructType, options: Map[String, String])
    extends WriteBuilder with SupportsTruncate {

    private var truncated: Boolean = false

    /** Spark calls truncate() for `.mode("overwrite")`; record it so build() clears the target. */
    override def truncate(): WriteBuilder = {
        truncated = true
        this
    }

    /** Builds a Write that uses GDAL_BatchWrite; on overwrite, clears the target dir first. */
    override def build(): Write = {
        val spark = SparkSession.builder().getOrCreate()
        val ec = ExpressionConfig(spark)
        if (truncated) {
            val path = options.getOrElse("path",
                throw new IllegalArgumentException(
                    "gdal/gtiff_gdal writer requires a path option (use .save(path))."))
            val hConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
            val outPath = new Path(HadoopUtils.cleanPath(path))
            val fs = outPath.getFileSystem(hConf.value)
            if (fs.exists(outPath)) fs.delete(outPath, true)
            fs.mkdirs(outPath)
        }
        new Write {
            /** Overrides Write.toBatch: returns GDAL_BatchWrite for this schema/options/config. */
            override def toBatch: BatchWrite = new GDAL_BatchWrite(schema, options, ec)
        }
    }
}
