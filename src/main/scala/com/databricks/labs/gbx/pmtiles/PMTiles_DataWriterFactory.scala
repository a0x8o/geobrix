package com.databricks.labs.gbx.pmtiles

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
  * Factory that creates one [[PMTiles_RowWriter]] per (partitionId, taskId).
  *
  * `path` is the final output file path (passed through from the user's `.save(path)`); the
  * factory hands it to each row writer, which writes `_part_<taskAttemptId>` scratch files in
  * the parent directory and reports them back through commit messages.
  */
class PMTiles_DataWriterFactory(
    schema: StructType,
    path: String,
    options: Map[String, String],
    hConf: SerializableConfiguration
) extends DataWriterFactory with Serializable {

    /**
      * Overrides DataWriterFactory.createWriter: returns a per-task PMTiles_RowWriter.
      *
      * The (partitionId, taskId) tuple is encoded into the scratch filenames so multiple
      * attempts of the same partition (e.g. speculative execution) don't collide, and the
      * commit phase only consumes the scratch files for committed task attempts.
      */
    override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
        new PMTiles_RowWriter(schema, path, partitionId, taskId, options, hConf)
    }
}
