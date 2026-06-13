package com.databricks.labs.gbx.vectorx.ds.geojsonl

import org.apache.spark.sql.connector.write.WriterCommitMessage

/**
  * Commit message from a [[GeoJSONL_RowWriter]]: lists the shard files this task published into the
  * output directory. Unlike the PMTiles writer there is no driver-side merge — the shards ARE the
  * dataset — so the only thing the driver needs from each task is the set of shard paths it wrote
  * (used by `abort` for best-effort cleanup).
  *
  * @param shardPaths Absolute (Hadoop-cleaned) paths of the `.geojsonl` shards this task wrote.
  */
final case class GeoJSONL_WriterMsg(shardPaths: Seq[String]) extends WriterCommitMessage
