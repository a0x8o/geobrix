package com.databricks.labs.gbx.vectorx.ds.geojsonl

import com.databricks.labs.gbx.util.HadoopUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
  * BatchWrite for the `geojsonl_ogr` DataSource.
  *
  * Per-partition (executor side, [[GeoJSONL_RowWriter]]): buffer the partition's rows, then on
  * commit encode one (or several, with `maxRecordsPerFile`) `GeoJSONSeq` shard(s) to worker-local
  * temp via OGR and copy each into the output directory as `part-<uuid>.geojsonl`.
  *
  * Commit (driver side, this method): NO merge — the shards ARE the dataset. Best-effort write of
  * a `_SUCCESS` marker into the directory.
  *
  * Abort: best-effort delete of any shards named in the commit messages.
  */
class GeoJSONL_BatchWrite(
    schema: StructType,
    path: String,
    options: Map[String, String],
    hConf: SerializableConfiguration
) extends BatchWrite {

    /** Builds the per-task data-writer factory; passes the output directory + options + hConf. */
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
        new GeoJSONL_DataWriterFactory(schema, path, options, hConf)

    /** NO merge: just finalize. Best-effort `_SUCCESS` marker (advisory; never fails the commit). */
    override def commit(messages: Array[WriterCommitMessage]): Unit = {
        val outPath = new Path(HadoopUtils.cleanPath(path))
        val fs = outPath.getFileSystem(hConf.value)
        try {
            if (!fs.exists(outPath)) fs.mkdirs(outPath)
            val marker = fs.create(new Path(outPath, "_SUCCESS"), true)
            try marker.write(Array.emptyByteArray) finally marker.close()
        } catch {
            case _: Throwable => () // marker is advisory
        }
    }

    /** Best-effort delete of any shards that did land. */
    override def abort(messages: Array[WriterCommitMessage]): Unit = {
        val anyPath = new Path(HadoopUtils.cleanPath(path))
        val fs = anyPath.getFileSystem(hConf.value)
        messages
            .filter(_ != null)
            .collect { case m: GeoJSONL_WriterMsg => m }
            .foreach(_.shardPaths.foreach { shard =>
                try fs.delete(new Path(shard), false) catch { case _: Throwable => () }
            })
    }
}
