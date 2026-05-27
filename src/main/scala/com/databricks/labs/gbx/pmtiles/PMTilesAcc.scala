package com.databricks.labs.gbx.pmtiles

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import scala.collection.mutable.ArrayBuffer

/**
  * Mutable aggregation buffer for `PMTiles_Agg`.
  *
  * Accumulates `(z, x, y, tileBytes)` tuples plus an optional JSON metadata string;
  * the buffer is the working state of the `TypedImperativeAggregate` and is shipped
  * between executors during the merge phase via `serialize` / `deserialize`.
  *
  * A safety cap (default 100 MiB per partition / merged buffer) guards against
  * runaway pipelines that try to aggregate gigabytes of tiles through the UDAF;
  * the `.write.format("pmtiles")` DataSource is the right path for those.
  */
final class PMTilesAcc(
    val tiles: ArrayBuffer[(Int, Int, Int, Array[Byte])] = ArrayBuffer.empty,
    var metadataJson: String = "{}",
    private var byteSize: Long = 0L
) extends Serializable {

    /** Append a tile and update the running byte count. */
    def add(z: Int, x: Int, y: Int, payload: Array[Byte]): PMTilesAcc = {
        if (payload != null) {
            tiles += ((z, x, y, payload))
            byteSize += payload.length.toLong
            PMTilesAcc.guardSize(byteSize)
        }
        this
    }

    /** Set the metadata JSON; called once per group from the UDAF eval phase. */
    def withMetadata(json: String): PMTilesAcc = {
        if (json != null && json.nonEmpty) metadataJson = json
        this
    }

    /** Combine two buffers (merge phase of the aggregation). */
    def merge(other: PMTilesAcc): PMTilesAcc = {
        tiles ++= other.tiles
        byteSize += other.byteSize
        PMTilesAcc.guardSize(byteSize)
        // Prefer non-default metadata from either side; later side wins on ties.
        if (other.metadataJson != null && other.metadataJson.nonEmpty && other.metadataJson != "{}") {
            metadataJson = other.metadataJson
        }
        this
    }

    /** Approximate aggregate byte size (sum of tile payload lengths only). */
    def approxByteSize: Long = byteSize

    /** Serialize the buffer for cross-executor shipping. */
    def serialize: Array[Byte] = {
        val bos = new ByteArrayOutputStream()
        val out = new DataOutputStream(bos)
        // Metadata JSON.
        val mjBytes = metadataJson.getBytes("UTF-8")
        out.writeInt(mjBytes.length)
        out.write(mjBytes)
        // Tile count.
        out.writeInt(tiles.length)
        // Tiles.
        for ((z, x, y, b) <- tiles) {
            out.writeInt(z)
            out.writeInt(x)
            out.writeInt(y)
            out.writeInt(if (b == null) 0 else b.length)
            if (b != null && b.length > 0) out.write(b)
        }
        bos.toByteArray
    }
}

object PMTilesAcc {

    /** Hard cap on the per-buffer payload byte count — guards the 2 GiB Spark cell limit. */
    val MAX_BUFFER_BYTES: Long = 100L * 1024L * 1024L // 100 MiB

    /** Sentinel empty buffer. */
    def empty: PMTilesAcc = new PMTilesAcc()

    /** Reverse of [[PMTilesAcc.serialize]]. */
    def deserialize(bytes: Array[Byte]): PMTilesAcc = {
        val in = new DataInputStream(new ByteArrayInputStream(bytes))
        val mjLen = in.readInt()
        val mjBytes = new Array[Byte](mjLen)
        in.readFully(mjBytes)
        val mj = new String(mjBytes, "UTF-8")
        val n = in.readInt()
        val tiles = ArrayBuffer.empty[(Int, Int, Int, Array[Byte])]
        var totalBytes: Long = 0L
        var i = 0
        while (i < n) {
            val z = in.readInt()
            val x = in.readInt()
            val y = in.readInt()
            val len = in.readInt()
            val payload = new Array[Byte](len)
            if (len > 0) in.readFully(payload)
            tiles += ((z, x, y, payload))
            totalBytes += len.toLong
            i += 1
        }
        new PMTilesAcc(tiles, mj, totalBytes)
    }

    /** Throws a clear error if the accumulated payload size exceeds the per-buffer cap. */
    private[pmtiles] def guardSize(currentBytes: Long): Unit = {
        if (currentBytes > MAX_BUFFER_BYTES) {
            throw new IllegalStateException(
                s"PMTiles aggregator buffer exceeded ${MAX_BUFFER_BYTES / (1024 * 1024)} MiB " +
                s"(current = ${currentBytes / (1024 * 1024)} MiB). " +
                s"Use `.write.format(\"pmtiles\").save(path)` for large pyramids — the " +
                s"`gbx_pmtiles_agg` UDAF is limited by Spark's 2 GiB cell size."
            )
        }
    }
}
