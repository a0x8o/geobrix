package com.databricks.labs.gbx.pmtiles

/**
  * A single directory entry in a PMTiles archive (spec § 4.1).
  *
  * @param tileId    Hilbert-curve cumulative TileID across all zoom levels.
  * @param offset    Byte offset from the start of the tile-data section to this entry's blob.
  * @param length    Number of bytes of this tile blob (MUST be > 0; spec § 4.1 Length).
  * @param runLength Number of contiguous TileIDs this entry covers (1 = single tile; 0 = leaf
  *                  directory entry; >1 = RLE-deduplicated tile run).
  */
final case class PMTilesEntry(tileId: Long, offset: Long, length: Int, runLength: Int)
