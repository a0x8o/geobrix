package com.databricks.labs.gbx.pmtiles

import com.databricks.labs.gbx.util.HadoopUtils
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.io.{ByteArrayOutputStream, DataOutputStream}
import scala.collection.mutable

/**
  * Per-task data writer for the `pmtiles` DataSource.
  *
  * Behavior:
  *   - On each row, append the tile bytes to a streaming scratch file
  *     `{parent}/_part_{partitionId}_{taskId}.tdata` and record the corresponding
  *     `(tileId, offsetWithinPartition, length)` triple in an in-memory list.
  *   - Within a partition, deduplicate by content-hash so that repeat tiles in the same task
  *     share a single tile-data blob (consecutive identical tile_ids get RLE-merged).
  *   - On commit, write the entries-table sidecar `{parent}/_part_{partitionId}_{taskId}.entries`
  *     and emit a [[PMTiles_WriterMsg]] for the driver.
  *   - On abort, delete both scratch files.
  *
  * Cross-task content dedup is not attempted in v0.4.0 — each task is independent so a
  * tile that appears in multiple tasks will be stored multiple times in the final file. This
  * keeps the per-task path branch-free; future work could shuffle by tile_id to dedup.
  */
class PMTiles_RowWriter(
    schema: StructType,
    outPath: String,
    partitionId: Int,
    taskId: Long,
    options: Map[String, String],
    hConf: SerializableConfiguration
) extends DataWriter[InternalRow] {

    private val zIdx = schema.fieldIndex("z")
    private val xIdx = schema.fieldIndex("x")
    private val yIdx = schema.fieldIndex("y")
    private val bytesIdx = schema.fieldIndex("bytes")

    private val cleanOut = HadoopUtils.cleanPath(outPath)
    private val outHadoopPath = new Path(cleanOut)
    private val parentPath: Path = Option(outHadoopPath.getParent)
        .getOrElse(new Path(".")) // defensive; in practice .save(path) always has a parent
    private val fs: FileSystem = outHadoopPath.getFileSystem(hConf.value)

    // Make sure the work directory exists.
    fs.mkdirs(parentPath)

    private val baseName = s"_part_${partitionId}_$taskId"
    private val tdataScratch: Path = new Path(parentPath, s"$baseName.tdata")
    private val entriesScratch: Path = new Path(parentPath, s"$baseName.entries")

    private val tdataStream: FSDataOutputStream = fs.create(tdataScratch, true)
    private var bytesWritten: Long = 0L

    // Content hash → (offset, length) for in-task dedup. Hash key is a SHA-256 hex string of
    // the payload; we trade the small hash cost for the savings on repeat blank tiles.
    private val contentToBlob = mutable.HashMap.empty[String, (Long, Int)]
    // Buffered entries in insertion order; the driver-side commit will resort by tileId after merging.
    private val entries = mutable.ArrayBuffer.empty[PMTilesEntry]

    /** Append one row's tile bytes (with optional in-task content dedup + RLE merge). */
    override def write(row: InternalRow): Unit = {
        if (row.isNullAt(bytesIdx)) return
        val z = row.getInt(zIdx)
        val x = row.getInt(xIdx)
        val y = row.getInt(yIdx)
        val payload = row.getBinary(bytesIdx)
        if (payload == null || payload.length == 0) return

        val tileId = PMTilesV3Encoder.hilbertId(z, x, y)
        val hash = sha256Hex(payload)
        val (offset, length) = contentToBlob.get(hash) match {
            case Some(v) => v
            case None =>
                val off = bytesWritten
                tdataStream.write(payload, 0, payload.length)
                bytesWritten += payload.length.toLong
                contentToBlob.put(hash, (off, payload.length))
                (off, payload.length)
        }

        // RLE-merge with the previous entry when content + tile_id sequence are contiguous.
        if (entries.nonEmpty) {
            val prev = entries.last
            if (prev.offset == offset && prev.length == length && prev.tileId + prev.runLength == tileId) {
                entries(entries.length - 1) = prev.copy(runLength = prev.runLength + 1)
            } else {
                entries += PMTilesEntry(tileId, offset, length, 1)
            }
        } else {
            entries += PMTilesEntry(tileId, offset, length, 1)
        }
    }

    /** Finalize tile-data scratch file, serialize entries sidecar, return a commit message. */
    override def commit(): WriterCommitMessage = {
        try tdataStream.close() catch { case _: Throwable => () }
        // Serialize the entries list as a length-prefixed binary blob: [n int][tileId long, off long, len int, rl int]*
        val out = new ByteArrayOutputStream()
        val dout = new DataOutputStream(out)
        dout.writeInt(entries.length)
        for (e <- entries) {
            dout.writeLong(e.tileId)
            dout.writeLong(e.offset)
            dout.writeInt(e.length)
            dout.writeInt(e.runLength)
        }
        dout.flush()
        val entriesStream = fs.create(entriesScratch, true)
        try entriesStream.write(out.toByteArray) finally entriesStream.close()
        PMTiles_WriterMsg(partitionId, tdataScratch.getName, entriesScratch.getName, bytesWritten)
    }

    /** Discard scratch files on abort. */
    override def abort(): Unit = {
        try tdataStream.close() catch { case _: Throwable => () }
        try fs.delete(tdataScratch, false) catch { case _: Throwable => () }
        try fs.delete(entriesScratch, false) catch { case _: Throwable => () }
    }

    /** Ensure the tdata scratch handle is closed even if the task is canceled. */
    override def close(): Unit = {
        try tdataStream.close() catch { case _: Throwable => () }
    }

    /** SHA-256 hex digest of a byte array (used for tile-content deduplication within a task). */
    private def sha256Hex(b: Array[Byte]): String = {
        val md = java.security.MessageDigest.getInstance("SHA-256")
        val digest = md.digest(b)
        val sb = new StringBuilder(digest.length * 2)
        for (by <- digest) sb.append(f"$by%02x")
        sb.toString()
    }
}
