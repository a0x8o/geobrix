package com.databricks.labs.gbx.vectorx.ds.geojsonl

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
  * Factory that creates one [[GeoJSONL_RowWriter]] per (partitionId, taskId). The output directory
  * (the user's `.save(path)`) is handed to each row writer, which writes `part-<uuid>.geojsonl`
  * shards into it and reports their paths back through commit messages.
  */
class GeoJSONL_DataWriterFactory(
    schema: StructType,
    path: String,
    options: Map[String, String],
    hConf: SerializableConfiguration
) extends DataWriterFactory with Serializable {

    /** Overrides DataWriterFactory.createWriter: returns a per-task GeoJSONL_RowWriter. */
    override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
        new GeoJSONL_RowWriter(schema, path, options, hConf)
}
