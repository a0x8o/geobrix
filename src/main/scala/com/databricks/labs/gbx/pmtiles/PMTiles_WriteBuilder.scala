package com.databricks.labs.gbx.pmtiles

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsTruncate, Write, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
  * WriteBuilder for the `pmtiles` DataSource. Produces a `Write` whose `toBatch` is a
  * [[PMTiles_BatchWrite]] that performs the two-phase partitioned commit (per-task scratch
  * files → single commit task that concatenates and prepends the v3 header).
  *
  * Implements `SupportsTruncate` so that `df.write.format("pmtiles").save(path)` works
  * without an explicit `.mode(...)` — the writer always produces a single file, and
  * "append" semantics don't apply to a binary container. `.mode("overwrite")` is the
  * canonical mode; we silently accept the default (ErrorIfExists) by treating it as
  * truncate when the user-provided path doesn't yet exist.
  */
class PMTiles_WriteBuilder(schema: StructType, options: Map[String, String])
    extends WriteBuilder with SupportsTruncate {

    /** Default Spark mode is ErrorIfExists; truncate flips it to overwrite for the binary blob. */
    override def truncate(): WriteBuilder = this

    /** Builds a Write whose batch is a PMTiles_BatchWrite carrying schema, options, and hConf. */
    override def build(): Write = {
        val path = options.getOrElse("path",
            throw new IllegalArgumentException(
                "pmtiles DataSource requires a path option (use .save(path))"))
        val spark = SparkSession.builder().getOrCreate()
        val hConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
        new Write {
            override def toBatch: BatchWrite = new PMTiles_BatchWrite(schema, path, options, hConf)
        }
    }
}
