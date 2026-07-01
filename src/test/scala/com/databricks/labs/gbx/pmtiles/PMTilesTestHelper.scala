package com.databricks.labs.gbx.pmtiles

import java.nio.{ByteBuffer, ByteOrder}

/**
  * Test helper: parse a PMTiles v3 binary blob and extract the raw tile bytes
  * for a given `(z, x, y)` coordinate.
  *
  * Implements the minimal PMTiles v3 binary parse needed for test assertions:
  * reads the header offsets, scans the root directory entries (delta-decoded
  * tileIds + varint fields per spec § 4.2), resolves the tile-data offset, and
  * returns the raw tile payload bytes.
  *
  * Only supports root-directory archives (no leaf directories) — which is all
  * `PMTilesV3Encoder` emits for v0.4.0.
  */
object PMTilesTestHelper {

    /**
      * Return the raw tile bytes for `(z, x, y)` from a PMTiles v3 archive blob.
      *
      * @throws AssertionError if the magic/version is wrong, the tile is not found,
      *                        or the archive uses leaf directories.
      */
    def readTile(archive: Array[Byte], z: Int, x: Int, y: Int): Array[Byte] = {
        require(archive != null && archive.length >= 127, "archive too short to contain a valid PMTiles header")

        // Verify magic and version.
        val magic = new String(archive, 0, 7, "UTF-8")
        require(magic == "PMTiles", s"bad magic: '$magic'")
        require(archive(7) == 0x03.toByte, s"expected version 3; got ${archive(7)}")

        val hdr = ByteBuffer.wrap(archive, 0, 127).order(ByteOrder.LITTLE_ENDIAN)
        hdr.position(8)
        val rootDirOff = hdr.getLong
        val rootDirLen = hdr.getLong
        // Metadata offset/length (skip).
        hdr.getLong
        hdr.getLong
        // Leaf dirs offset/length.
        hdr.getLong
        val leafDirsLen = hdr.getLong
        // Tile data offset.
        val tileDataOff = hdr.getLong

        require(leafDirsLen == 0L, "this helper does not support leaf-directory archives")

        // Decode the root directory (spec § 4.2 varint encoding).
        val dirBytes = archive.slice(rootDirOff.toInt, (rootDirOff + rootDirLen).toInt)
        val dir = ByteBuffer.wrap(dirBytes).order(ByteOrder.LITTLE_ENDIAN)

        val targetId = PMTilesV3Encoder.hilbertId(z, x, y)
        val n = readVarint(dir).toInt

        // Read delta-encoded tileIds.
        val tileIds = new Array[Long](n)
        var lastId = 0L
        for (i <- 0 until n) {
            lastId += readVarint(dir)
            tileIds(i) = lastId
        }

        // Read runLengths.
        val runLengths = new Array[Long](n)
        for (i <- 0 until n) runLengths(i) = readVarint(dir)

        // Read lengths.
        val lengths = new Array[Int](n)
        for (i <- 0 until n) lengths(i) = readVarint(dir).toInt

        // Read offsets (0 = contiguous with previous).
        val offsets = new Array[Long](n)
        var nextByte = 0L
        for (i <- 0 until n) {
            val raw = readVarint(dir)
            offsets(i) = if (raw == 0L && i > 0) nextByte else raw - 1L
            nextByte = offsets(i) + lengths(i).toLong
        }

        // Find the entry covering targetId.
        for (i <- 0 until n) {
            val tid = tileIds(i)
            val run = runLengths(i)
            if (targetId >= tid && targetId < tid + run) {
                val byteOff = (tileDataOff + offsets(i)).toInt
                val len = lengths(i)
                return archive.slice(byteOff, byteOff + len)
            }
        }

        throw new AssertionError(
            s"tile (z=$z, x=$x, y=$y, hilbertId=$targetId) not found in PMTiles archive " +
            s"(${n} directory entries)"
        )
    }

    /** Read a protobuf-style unsigned varint from `buf`. */
    private def readVarint(buf: ByteBuffer): Long = {
        var result = 0L
        var shift = 0
        var b = 0
        do {
            b = buf.get() & 0xFF
            result |= (b & 0x7FL) << shift
            shift += 7
        } while ((b & 0x80) != 0)
        result
    }
}
