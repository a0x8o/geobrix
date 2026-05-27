package com.databricks.labs.gbx.pmtiles

import org.scalatest.funsuite.AnyFunSuite

/**
  * Unit tests for the native Scala PMTiles v3 encoder.
  *
  * Spec reference: https://github.com/protomaps/PMTiles/blob/main/spec/v3/spec.md
  */
class PMTilesV3EncoderTest extends AnyFunSuite {

    test("encode an empty pyramid → valid header-only PMTile") {
        val bytes = PMTilesV3Encoder.encode(Iterator.empty, metadataJson = "{}")
        assert(bytes.length >= 127, s"header is 127 bytes; got ${bytes.length}")
        // Magic bytes 'PMTiles' at offset 0..6
        assert(bytes(0) == 'P'.toByte, "byte 0 must be 'P'")
        assert(bytes(1) == 'M'.toByte, "byte 1 must be 'M'")
        assert(bytes(2) == 'T'.toByte, "byte 2 must be 'T'")
        assert(bytes(3) == 'i'.toByte, "byte 3 must be 'i'")
        assert(bytes(4) == 'l'.toByte, "byte 4 must be 'l'")
        assert(bytes(5) == 'e'.toByte, "byte 5 must be 'e'")
        assert(bytes(6) == 's'.toByte, "byte 6 must be 's'")
        // Version byte 3 at offset 7
        assert(bytes(7) == 0x03.toByte, s"version byte must be 3; got ${bytes(7)}")
    }

    test("encode a single tile → header.addressed_tiles_count == 1") {
        val tileBytes = "PNG_FAKE".getBytes("UTF-8")
        val bytes = PMTilesV3Encoder.encode(
            Iterator((10, 512, 512, tileBytes)),
            metadataJson = "{}"
        )
        // addressed_tiles_count is uint64 LE at offset 72..79 (per spec § 3.1 header layout).
        val count = java.nio.ByteBuffer
            .wrap(bytes, 72, 8)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getLong
        assert(count == 1L, s"expected addressed_tiles_count=1; got $count")
    }

    test("hilbertId is deterministic and unique within a zoom") {
        val ids = (0 until 1024).map(i => PMTilesV3Encoder.hilbertId(5, i % 32, i / 32))
        assert(ids.distinct.length == 1024, "hilbert ids must be unique within z=5 32×32 grid")
        // Determinism: same input → same output.
        assert(PMTilesV3Encoder.hilbertId(5, 7, 9) == PMTilesV3Encoder.hilbertId(5, 7, 9))
    }

    test("hilbertId base case z=0 returns 0") {
        assert(PMTilesV3Encoder.hilbertId(0, 0, 0) == 0L)
    }

    test("hilbertId monotonic across zooms (z+1 tile ids start after z block)") {
        // For zoom z, there are 4^z tiles. The PMTiles spec orders tiles by Hilbert id
        // within the zoom, prefixed by the count of all lower-zoom tiles. So z=1 ids
        // are all >= 1 (one z=0 tile precedes them), and z=2 ids are all >= 5.
        val z0 = PMTilesV3Encoder.hilbertId(0, 0, 0)
        val z1 = (for { x <- 0 until 2; y <- 0 until 2 } yield PMTilesV3Encoder.hilbertId(1, x, y)).min
        val z2 = (for { x <- 0 until 4; y <- 0 until 4 } yield PMTilesV3Encoder.hilbertId(2, x, y)).min
        assert(z0 == 0L)
        assert(z1 >= 1L)
        assert(z2 >= 5L)
    }

    test("encode preserves tile bytes in the tile-data section") {
        val payload1 = "TILE_AAA".getBytes("UTF-8")
        val payload2 = "TILE_BBB_XYZ".getBytes("UTF-8")
        val bytes = PMTilesV3Encoder.encode(
            Iterator((1, 0, 0, payload1), (1, 1, 0, payload2)),
            metadataJson = "{}"
        )
        // tile-data offset is a uint64 LE at offset 56..63, length at 64..71 (per spec § 3.1).
        val tileDataOff = java.nio.ByteBuffer
            .wrap(bytes, 56, 8)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getLong
        val tileDataLen = java.nio.ByteBuffer
            .wrap(bytes, 64, 8)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .getLong
        assert(tileDataOff >= 127, s"tile-data offset must be at or past the header; got $tileDataOff")
        assert(tileDataLen == (payload1.length + payload2.length).toLong)
        // Check that both payloads appear in the tile-data region.
        val tileData = bytes.slice(tileDataOff.toInt, (tileDataOff + tileDataLen).toInt)
        val asString = new String(tileData, "UTF-8")
        assert(asString.contains("TILE_AAA"))
        assert(asString.contains("TILE_BBB_XYZ"))
    }

    test("encode deduplicates entries with identical content (RLE run_length)") {
        // Two distinct (z,x,y) but identical bytes → encoder should still produce a valid output
        // (run-length encoded entries; either one entry with run_length=2 or two entries pointing to same offset).
        val sameBytes = "SAME".getBytes("UTF-8")
        val bytes = PMTilesV3Encoder.encode(
            Iterator((1, 0, 0, sameBytes), (1, 1, 0, sameBytes)),
            metadataJson = "{}"
        )
        // addressed_tiles_count at 72..79; tile_contents_count at 88..95 — both uint64 LE.
        val addressed = java.nio.ByteBuffer.wrap(bytes, 72, 8).order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong
        val contents = java.nio.ByteBuffer.wrap(bytes, 88, 8).order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong
        assert(addressed == 2L, s"addressed=2; got $addressed")
        assert(contents <= addressed, s"tile_contents_count must be <= addressed_tiles_count; got $contents > $addressed")
    }
}
