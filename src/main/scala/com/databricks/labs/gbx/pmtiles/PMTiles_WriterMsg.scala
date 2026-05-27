package com.databricks.labs.gbx.pmtiles

import org.apache.spark.sql.connector.write.WriterCommitMessage

/**
  * Commit message from a [[PMTiles_RowWriter]]: tells the driver-side commit phase how to find
  * this task's scratch files and how much tile data it wrote.
  *
  * @param partitionId       Spark partition id; used to deterministically order the per-task
  *                          tile-data segments in the final blob.
  * @param tdataScratchName  Basename of the tile-data scratch file (relative to the parent of
  *                          the user-supplied output path).
  * @param entriesScratchName Basename of the entries scratch file (parallel to tdata).
  * @param tileDataBytes     Cumulative length of this task's tile data — drives the global
  *                          offset arithmetic in [[PMTiles_BatchWrite.commit]].
  */
final case class PMTiles_WriterMsg(
    partitionId: Int,
    tdataScratchName: String,
    entriesScratchName: String,
    tileDataBytes: Long
) extends WriterCommitMessage
