package com.databricks.labs.gbx.pmtiles

import com.databricks.labs.gbx.util.HadoopUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.io.{ByteArrayOutputStream, DataInputStream}

/**
  * BatchWrite for the `pmtiles` DataSource.
  *
  * Per-partition (executor side, [[PMTiles_RowWriter]]):
  *   1. Each task writes its tile blobs to `{outDir}/_part_{taskAttemptId}.tdata`.
  *   2. Each task writes a sidecar `{outDir}/_part_{taskAttemptId}.entries` containing the
  *      `(tileId, offsetWithinPart, length)` tuples for entries it produced.
  *   3. Task returns a [[PMTiles_WriterMsg]] carrying its scratch-file basename and counts.
  *
  * Commit (driver side, single-task, this `commit` method):
  *   1. Sort committed messages by partitionId so the final tile-data layout is deterministic.
  *   2. Compute cumulative partition offsets (partition 0 at 0; partition 1 at sum of
  *      partition 0's tdata length; etc.).
  *   3. Load all `.entries` files, adjust offsets, sort the merged entry list by tileId,
  *      then build the root directory via [[PMTilesV3Encoder.encodeDirectory]].
  *   4. Concatenate header || rootDir || metadata || (leaf dirs empty) || tile data, where
  *      tile data is streamed from each partition's `.tdata` file.
  *   5. Write the final `path` and delete all scratch files.
  *
  * Abort: delete `_part_*` scratch files; do not delete the (already-existing) parent dir.
  */
class PMTiles_BatchWrite(
    schema: StructType,
    path: String,
    options: Map[String, String],
    hConf: SerializableConfiguration
) extends BatchWrite {

    private val metadataJson: String = options.getOrElse("metadataJson", "{}")
    private val tileCompression: Byte = options.get("tileCompression").map(_.toByte).getOrElse(PMTilesV3Encoder.COMPRESSION_NONE)
    private val tileTypeOverride: Option[Byte] = options.get("tileType").map(_.toByte)

    /** Builds the per-task data-writer factory; passes the parent directory + options + hConf. */
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
        new PMTiles_DataWriterFactory(schema, path, options, hConf)
    }

    /** Drives the merge step: read scratch, build header + root dir, write final file. */
    override def commit(messages: Array[WriterCommitMessage]): Unit = {
        val msgs = messages
            .filter(_ != null)
            .collect { case m: PMTiles_WriterMsg => m }
            .sortBy(_.partitionId)
        val outPath = new Path(HadoopUtils.cleanPath(path))
        val fs = outPath.getFileSystem(hConf.value)
        val workDir = outPath.getParent
        // Defensive: ensure the parent dir exists (mkdirs is idempotent / a no-op when present).
        if (workDir != null) fs.mkdirs(workDir)

        // 1. Compute cumulative partition offsets.
        var cumulative: Long = 0L
        val partitionStart: Array[Long] = new Array[Long](msgs.length)
        var i = 0
        while (i < msgs.length) {
            partitionStart(i) = cumulative
            cumulative += msgs(i).tileDataBytes
            i += 1
        }
        val totalTileDataBytes: Long = cumulative

        // 2. Read entries from each partition, adjusting offsets to global frame.
        val allEntries = scala.collection.mutable.ArrayBuffer.empty[PMTilesEntry]
        var idx = 0
        while (idx < msgs.length) {
            val msg = msgs(idx)
            val base = partitionStart(idx)
            val entriesPath = new Path(workDir, msg.entriesScratchName)
            val raw = readAllBytes(fs, entriesPath)
            val din = new DataInputStream(new java.io.ByteArrayInputStream(raw))
            val n = din.readInt()
            var k = 0
            while (k < n) {
                val tileId = din.readLong()
                val off = din.readLong()
                val len = din.readInt()
                val runLength = din.readInt()
                allEntries += PMTilesEntry(tileId, base + off, len, runLength)
                k += 1
            }
            idx += 1
        }

        // 3. Sort entries by tileId (the spec requires clustered layout when clustered=1, which
        //    means tile_id ascending). Multi-partition writers cannot rely on within-task
        //    ordering for the global view, so sort here.
        val sorted = allEntries.sortBy(_.tileId).toIndexedSeq

        // 4. Detect tile type from the first non-empty partition's first bytes (if any).
        val tileType: Byte = tileTypeOverride.getOrElse {
            sniffFirstTileType(fs, workDir, msgs).getOrElse(PMTilesV3Encoder.TILE_TYPE_MVT)
        }

        // 5. Encode root directory + assemble the final PMTile file.
        val rootDirBytes = PMTilesV3Encoder.encodeDirectory(sorted)
        if (rootDirBytes.length > PMTilesV3Encoder.MAX_ROOT_DIR_BYTES) {
            // Best-effort cleanup so we don't leave scratch around on an unrecoverable error.
            cleanupScratch(fs, workDir, msgs)
            throw new IllegalArgumentException(
                s"PMTiles root directory would be ${rootDirBytes.length} bytes (max " +
                s"${PMTilesV3Encoder.MAX_ROOT_DIR_BYTES} per spec § 4). v0.4.0 does not yet emit " +
                s"leaf directories; please reduce the number of tiles or split into multiple files."
            )
        }
        val metadataBytes = metadataJson.getBytes("UTF-8")
        val minZ = if (sorted.isEmpty) 0 else sorted.iterator.map(e => zOf(e.tileId)).min
        val maxZ = if (sorted.isEmpty) 0 else sorted.iterator.map(e => zOf(e.tileId)).max
        val header = PMTilesV3Encoder_BuildHeader.build(
            rootDirLength = rootDirBytes.length.toLong,
            metadataLength = metadataBytes.length.toLong,
            tileDataLength = totalTileDataBytes,
            addressedTilesCount = sorted.iterator.map(_.runLength.toLong).sum,
            tileEntriesCount = sorted.length.toLong,
            tileContentsCount = sorted.length.toLong, // upper bound; partition-local dedup only
            tileType = tileType,
            tileCompression = tileCompression,
            minZoom = minZ,
            maxZoom = maxZ
        )

        // 6. Stream the final file: header || root_dir || metadata || (no leaf) || tdata*.
        val finalOut = fs.create(outPath, true)
        try {
            finalOut.write(header)
            finalOut.write(rootDirBytes)
            finalOut.write(metadataBytes)
            // No leaf directories in v0.4.0.
            var p = 0
            while (p < msgs.length) {
                val tdataPath = new Path(workDir, msgs(p).tdataScratchName)
                val in = fs.open(tdataPath)
                try {
                    val buf = new Array[Byte](64 * 1024)
                    var r = in.read(buf)
                    while (r > 0) {
                        finalOut.write(buf, 0, r)
                        r = in.read(buf)
                    }
                } finally in.close()
                p += 1
            }
        } finally finalOut.close()

        // 7. Clean up scratch.
        cleanupScratch(fs, workDir, msgs)
    }

    /** Delete any scratch files left behind by per-partition writers. */
    override def abort(messages: Array[WriterCommitMessage]): Unit = {
        val outPath = new Path(HadoopUtils.cleanPath(path))
        val fs = outPath.getFileSystem(hConf.value)
        val workDir = outPath.getParent
        val msgs = messages
            .filter(_ != null)
            .collect { case m: PMTiles_WriterMsg => m }
        cleanupScratch(fs, workDir, msgs)
    }

    private def cleanupScratch(fs: FileSystem, workDir: Path, msgs: Seq[PMTiles_WriterMsg]): Unit = {
        for (m <- msgs) {
            try fs.delete(new Path(workDir, m.tdataScratchName), false) catch { case _: Throwable => () }
            try fs.delete(new Path(workDir, m.entriesScratchName), false) catch { case _: Throwable => () }
        }
    }

    /** Helper: read entire file contents into a byte array (entries files are small per-partition). */
    private def readAllBytes(fs: FileSystem, p: Path): Array[Byte] = {
        val in = fs.open(p)
        try {
            val out = new ByteArrayOutputStream()
            val buf = new Array[Byte](16 * 1024)
            var r = in.read(buf)
            while (r > 0) { out.write(buf, 0, r); r = in.read(buf) }
            out.toByteArray
        } finally in.close()
    }

    /** Sniff the tile type from the first 16 bytes of the first non-empty partition's tdata file. */
    private def sniffFirstTileType(fs: FileSystem, workDir: Path, msgs: Seq[PMTiles_WriterMsg]): Option[Byte] = {
        msgs.iterator
            .filter(_.tileDataBytes > 0L)
            .flatMap { m =>
                val in = fs.open(new Path(workDir, m.tdataScratchName))
                try {
                    val buf = new Array[Byte](16)
                    val r = in.read(buf)
                    if (r > 0) Some(PMTiles_Agg.detectTileType(buf.take(r))) else None
                } finally in.close()
            }
            .nextOption()
    }

    /**
      * Recover the zoom level z from a Hilbert TileID by binary search on the closed-form
      * `(4^z - 1) / 3 <= tileId < (4^(z+1) - 1) / 3` window.
      */
    private def zOf(tileId: Long): Int = {
        var z = 0
        while (z < 31) {
            val nextStart = ((1L << (2 * (z + 1))) - 1L) / 3L
            if (tileId < nextStart) return z
            z += 1
        }
        z
    }
}

/**
  * Helper: build a PMTiles v3 header for the on-disk commit path.
  *
  * Mirrors [[PMTilesV3Encoder.encode]]'s header logic but is parameterized for the streaming
  * write path where lengths/offsets are known up front. Kept colocated with the commit code so
  * the on-disk layout can evolve independently of the in-memory aggregator's header logic.
  */
private[pmtiles] object PMTilesV3Encoder_BuildHeader {

    /**
      * Build the 127-byte fixed-size PMTiles v3 header.
      *
      * Section offsets are computed from the supplied lengths assuming the canonical layout
      * `[header 127][root dir][metadata][leaf dirs (always 0 in v0.4.0)][tile data]`.
      */
    def build(
        rootDirLength: Long,
        metadataLength: Long,
        tileDataLength: Long,
        addressedTilesCount: Long,
        tileEntriesCount: Long,
        tileContentsCount: Long,
        tileType: Byte,
        tileCompression: Byte,
        minZoom: Int,
        maxZoom: Int
    ): Array[Byte] = {
        val header = java.nio.ByteBuffer.allocate(127).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        val rootDirOffset: Long = 127L
        val metadataOffset: Long = rootDirOffset + rootDirLength
        val leafDirsOffset: Long = metadataOffset + metadataLength
        val tileDataOffset: Long = leafDirsOffset // leaf len = 0

        header.put("PMTiles".getBytes("UTF-8"))
        header.put(0x03.toByte)
        header.putLong(rootDirOffset)
        header.putLong(rootDirLength)
        header.putLong(metadataOffset)
        header.putLong(metadataLength)
        header.putLong(leafDirsOffset)
        header.putLong(0L) // leaf dirs length
        header.putLong(tileDataOffset)
        header.putLong(tileDataLength)
        header.putLong(addressedTilesCount)
        header.putLong(tileEntriesCount)
        header.putLong(tileContentsCount)
        header.put(0x01.toByte) // clustered
        header.put(PMTilesV3Encoder.COMPRESSION_NONE)
        header.put(tileCompression)
        header.put(tileType)
        header.put((minZoom & 0xFF).toByte)
        header.put((maxZoom & 0xFF).toByte)
        header.putInt(scalePos(-180.0)); header.putInt(scalePos(-85.0))
        header.putInt(scalePos(180.0)); header.putInt(scalePos(85.0))
        header.put((minZoom & 0xFF).toByte)
        header.putInt(scalePos(0.0)); header.putInt(scalePos(0.0))
        require(header.position() == 127, s"PMTiles header is not 127 bytes: ${header.position()}")
        header.array()
    }

    private def scalePos(v: Double): Int = math.round(v * 1e7).toInt
}
