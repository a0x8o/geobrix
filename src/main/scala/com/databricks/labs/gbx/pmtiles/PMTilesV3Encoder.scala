package com.databricks.labs.gbx.pmtiles

import java.io.ByteArrayOutputStream
import java.nio.{ByteBuffer, ByteOrder}
import java.security.MessageDigest

/**
  * Native Scala encoder for the PMTiles v3 single-file tile archive format.
  *
  * Spec reference: https://github.com/protomaps/PMTiles/blob/main/spec/v3/spec.md
  *
  * Layout (spec § 2):
  * {{{
  *   +--------+----------------+----------+------------------+-----------+
  *   | Header | Root Directory | Metadata | Leaf Directories | Tile Data |
  *   +--------+----------------+----------+------------------+-----------+
  * }}}
  *
  *  - Header (127 bytes; spec § 3.1).
  *  - Root directory (varint entries, optionally compressed; spec § 4).
  *  - JSON metadata (UTF-8; spec § 5).
  *  - Leaf directories (empty for v0.4.0; we error out if root cannot fit in 16 KiB).
  *  - Tile data (concatenated tile blobs; spec § 2).
  *
  * For v0.4.0 we ship with `internal_compression = none (0x01)` and `tile_compression = none`
  * — callers pass through already-compressed tile bytes verbatim. Future versions may add
  * gzip/zstd for the directory.
  */
object PMTilesV3Encoder {

    /** Max compressed root-directory size per spec § 4: 16,384 - 127 = 16,257 bytes. */
    val MAX_ROOT_DIR_BYTES: Int = 16384 - 127

    /** Compression enum (spec § 3.3): 0=unknown, 1=none, 2=gzip, 3=brotli, 4=zstd. */
    val COMPRESSION_NONE: Byte = 0x01.toByte

    /** Tile type enum (spec § 3.2): 1=MVT, 2=PNG, 3=JPEG, 4=WebP. */
    val TILE_TYPE_MVT: Byte = 0x01.toByte
    val TILE_TYPE_PNG: Byte = 0x02.toByte
    val TILE_TYPE_JPEG: Byte = 0x03.toByte
    val TILE_TYPE_WEBP: Byte = 0x04.toByte

    /**
      * Encode a tile pyramid into PMTiles v3 binary format.
      *
      * Tiles can arrive in any order; the encoder sorts by Hilbert TileID, deduplicates
      * identical-content runs (RLE), and writes the canonical clustered layout.
      *
      * @param tiles           Iterator of (z, x, y, bytes) tuples — `bytes` is the tile payload,
      *                        passed through verbatim (we do not compress).
      * @param metadataJson    UTF-8 JSON metadata blob (spec § 5).
      * @param tileType        Tile content type byte (default PNG); see TILE_TYPE_* constants.
      * @param tileCompression Tile compression byte (default `none = 0x01`); tile bytes are stored
      *                        as-is — set this to match what the caller has already applied.
      * @return One PMTile binary blob.
      */
    def encode(
        tiles: Iterator[(Int, Int, Int, Array[Byte])],
        metadataJson: String,
        tileType: Byte = TILE_TYPE_PNG,
        tileCompression: Byte = COMPRESSION_NONE
    ): Array[Byte] = {
        // 1. Materialize and sort tiles by Hilbert TileID (spec § 4.1).
        val materialized = tiles.toArray
        val sorted = materialized.map { case (z, x, y, b) => (hilbertId(z, x, y), z, x, y, b) }
            .sortBy(_._1)

        // 2. Compute zoom + bounds aggregates for the header (defaults if empty).
        val minZoom: Int = if (sorted.isEmpty) 0 else sorted.map(_._2).min
        val maxZoom: Int = if (sorted.isEmpty) 0 else sorted.map(_._2).max

        // 3. Build the tile-data section + entries with RLE deduplication.
        //    Two consecutive entries with identical content & consecutive tile_ids merge into
        //    one entry with run_length > 1; consecutive entries with identical content but
        //    non-consecutive tile_ids keep distinct entries but share the same offset
        //    (length stays the same; offset references the existing blob).
        val tileDataStream = new ByteArrayOutputStream()
        // contentHash → (offset, length) for in-memory dedup.
        val seenContent = scala.collection.mutable.HashMap.empty[String, (Long, Int)]
        val entries = scala.collection.mutable.ArrayBuffer.empty[PMTilesEntry]
        var nextOffset: Long = 0L

        for ((tileId, _, _, _, payload) <- sorted) {
            require(payload != null && payload.nonEmpty, s"tile payload at tileId=$tileId is empty (spec § 4.1: length MUST be > 0)")
            val hash = sha256Hex(payload)
            val (offset, length) = seenContent.get(hash) match {
                case Some((off, len)) => (off, len)
                case None =>
                    val off = nextOffset
                    tileDataStream.write(payload, 0, payload.length)
                    nextOffset += payload.length
                    seenContent.put(hash, (off, payload.length))
                    (off, payload.length)
            }
            // RLE merge with the previous entry if both content (offset+length) AND tile_id are contiguous.
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
        val tileData = tileDataStream.toByteArray
        val tileDataLength = tileData.length.toLong
        val addressedTilesCount = sorted.length.toLong
        val tileEntriesCount = entries.length.toLong
        val tileContentsCount = seenContent.size.toLong

        // 4. Encode the root directory (spec § 4.2).
        val rootDirBytes = encodeDirectory(entries.toSeq)
        if (rootDirBytes.length > MAX_ROOT_DIR_BYTES) {
            throw new IllegalArgumentException(
                s"PMTiles root directory would be ${rootDirBytes.length} bytes (max allowed: " +
                s"$MAX_ROOT_DIR_BYTES per spec § 4); pyramid too large for the single-blob " +
                s"`gbx_pmtiles_agg` UDAF path. Use the `.write.format(\"pmtiles\")` DataSource " +
                s"writer instead — it streams to disk and splits into leaf directories."
            )
        }
        val rootDirLength = rootDirBytes.length.toLong

        // 5. Encode metadata (UTF-8 bytes; spec § 5).
        val metadataBytes = metadataJson.getBytes("UTF-8")
        val metadataLength = metadataBytes.length.toLong

        // 6. Compute section offsets.
        //    Layout: [header 127][root dir][metadata][leaf dirs (empty)][tile data].
        val rootDirOffset: Long = 127L
        val metadataOffset: Long = rootDirOffset + rootDirLength
        val leafDirsOffset: Long = metadataOffset + metadataLength
        val leafDirsLength: Long = 0L
        val tileDataOffset: Long = leafDirsOffset + leafDirsLength

        // 7. Build the header (spec § 3.1).
        val header = ByteBuffer.allocate(127).order(ByteOrder.LITTLE_ENDIAN)
        // Bytes 0-6: Magic "PMTiles".
        header.put("PMTiles".getBytes("UTF-8"))
        // Byte 7: Version (3).
        header.put(0x03.toByte)
        // Bytes 8-15: Root directory offset.
        header.putLong(rootDirOffset)
        // Bytes 16-23: Root directory length.
        header.putLong(rootDirLength)
        // Bytes 24-31: Metadata offset.
        header.putLong(metadataOffset)
        // Bytes 32-39: Metadata length.
        header.putLong(metadataLength)
        // Bytes 40-47: Leaf directories offset.
        header.putLong(leafDirsOffset)
        // Bytes 48-55: Leaf directories length.
        header.putLong(leafDirsLength)
        // Bytes 56-63: Tile data offset.
        header.putLong(tileDataOffset)
        // Bytes 64-71: Tile data length.
        header.putLong(tileDataLength)
        // Bytes 72-79: Number of addressed tiles.
        header.putLong(addressedTilesCount)
        // Bytes 80-87: Number of tile entries.
        header.putLong(tileEntriesCount)
        // Bytes 88-95: Number of tile contents.
        header.putLong(tileContentsCount)
        // Byte 96: Clustered (1 = yes; we always emit clustered output).
        header.put(0x01.toByte)
        // Byte 97: Internal compression (none for v0.4.0).
        header.put(COMPRESSION_NONE)
        // Byte 98: Tile compression.
        header.put(tileCompression)
        // Byte 99: Tile type.
        header.put(tileType)
        // Byte 100: Min zoom.
        header.put((minZoom & 0xFF).toByte)
        // Byte 101: Max zoom.
        header.put((maxZoom & 0xFF).toByte)
        // Bytes 102-109: Min position (lon, lat at scale 1e7; default to whole-world bounds).
        header.putInt(scalePos(-180.0))
        header.putInt(scalePos(-85.0))
        // Bytes 110-117: Max position.
        header.putInt(scalePos(180.0))
        header.putInt(scalePos(85.0))
        // Byte 118: Center zoom.
        header.put((minZoom & 0xFF).toByte)
        // Bytes 119-126: Center position (0,0).
        header.putInt(scalePos(0.0))
        header.putInt(scalePos(0.0))

        require(header.position() == 127, s"PMTiles header is not 127 bytes: ${header.position()}")

        // 8. Concatenate: header || root_dir || metadata || (leaf_dirs = empty) || tile_data.
        val out = new ByteArrayOutputStream()
        out.write(header.array())
        out.write(rootDirBytes)
        out.write(metadataBytes)
        // No leaf directories for v0.4.0.
        out.write(tileData)
        out.toByteArray
    }

    /**
      * Encode a sequence of directory entries per spec § 4.2.
      *
      * Layout: [n entries (varint)] [delta-encoded tileIds] [runLengths] [lengths] [offsets].
      *
      * Offsets are encoded as `offset+1` or `0` when contiguous with the previous entry
      * (spec § 4.2 Offsets). The internal-compression step is a no-op for v0.4.0 (none).
      */
    private[pmtiles] def encodeDirectory(entries: Seq[PMTilesEntry]): Array[Byte] = {
        val out = new ByteArrayOutputStream()

        // Number of entries (spec § 4.2 — varint).
        writeVarint(out, entries.length.toLong)

        // Delta-encoded TileIDs.
        var lastId: Long = 0L
        for (e <- entries) {
            writeVarint(out, e.tileId - lastId)
            lastId = e.tileId
        }

        // RunLengths.
        for (e <- entries) writeVarint(out, e.runLength.toLong)

        // Lengths.
        for (e <- entries) writeVarint(out, e.length.toLong)

        // Offsets: contiguous → 0, else offset+1.
        var nextByte: Long = 0L
        for ((e, i) <- entries.zipWithIndex) {
            if (i > 0 && e.offset == nextByte) {
                writeVarint(out, 0L)
            } else {
                writeVarint(out, e.offset + 1L)
            }
            nextByte = e.offset + e.length.toLong
        }

        out.toByteArray
    }

    /**
      * Encode an unsigned 64-bit integer as a protobuf-style varint to the given stream.
      *
      * Reference: https://protobuf.dev/programming-guides/encoding/#varints
      *
      * Note: PMTiles tile IDs and offsets can be very large; we treat the input as unsigned
      * even though Scala Long is signed (TileIDs fit in 63 bits for any practical zoom level).
      */
    private[pmtiles] def writeVarint(out: ByteArrayOutputStream, value: Long): Unit = {
        var v = value
        // While there are at least 8 more bits to encode.
        while ((v & ~0x7FL) != 0L) {
            out.write(((v & 0x7FL) | 0x80L).toInt)
            v >>>= 7
        }
        out.write((v & 0x7FL).toInt)
    }

    /**
      * Compute the PMTiles v3 cumulative Hilbert curve TileID for (z, x, y).
      *
      * The TileID is `acc(z) + d`, where:
      *  - `acc(z) = (4^z - 1) / 3` is the count of all tiles at zooms 0..z-1 (geometric series).
      *  - `d` is the standard Hilbert curve index of (x, y) in the 2^z × 2^z grid (xy2d).
      *
      * Reference Hilbert algorithm: bit-twiddling per "Programming the Hilbert Curve",
      * Lawder (2000), matching the spec's example table for z ≤ 2.
      */
    def hilbertId(z: Int, x: Int, y: Int): Long = {
        require(z >= 0 && z <= 31, s"zoom $z out of supported range [0, 31] (PMTiles spec)")
        val n = 1 << z
        require(x >= 0 && x < n, s"x=$x out of range for z=$z (must be < $n)")
        require(y >= 0 && y < n, s"y=$y out of range for z=$z (must be < $n)")

        // Accumulated tile count for all lower zooms: (4^z - 1) / 3.
        // Use the closed-form sum_{k=0}^{z-1} 4^k = (4^z - 1) / 3.
        val acc: Long = if (z == 0) 0L else ((1L << (2 * z)) - 1L) / 3L

        // Hilbert xy2d (textbook implementation; rotates quadrants as we descend).
        var rx: Int = 0
        var ry: Int = 0
        var d: Long = 0L
        var xx: Int = x
        var yy: Int = y
        var s: Int = n / 2
        while (s > 0) {
            rx = if ((xx & s) > 0) 1 else 0
            ry = if ((yy & s) > 0) 1 else 0
            d += s.toLong * s.toLong * ((3 * rx) ^ ry).toLong
            // Rotate quadrant.
            if (ry == 0) {
                if (rx == 1) {
                    xx = s - 1 - xx
                    yy = s - 1 - yy
                }
                // Swap x and y.
                val tmp = xx
                xx = yy
                yy = tmp
            }
            s /= 2
        }
        acc + d
    }

    /** SHA-256 hex digest of a byte array (used for tile-content deduplication only). */
    private def sha256Hex(b: Array[Byte]): String = {
        val md = MessageDigest.getInstance("SHA-256")
        val digest = md.digest(b)
        // Encode as hex without allocating a Java String for each byte.
        val sb = new StringBuilder(digest.length * 2)
        for (by <- digest) {
            sb.append(f"$by%02x")
        }
        sb.toString()
    }

    /** Scale a longitude / latitude to a 32-bit signed integer per spec § 3.4. */
    private def scalePos(v: Double): Int = math.round(v * 1e7).toInt
}
