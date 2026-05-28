package com.databricks.labs.gbx.rasterx.expressions.grid

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import scala.collection.mutable.ArrayBuffer

/**
  * Mutable aggregation buffer for [[RST_GridFromPointsAgg]].
  *
  * Accumulates `(geom_wkb, value)` tuples. The buffer is the working state of
  * the `TypedImperativeAggregate` and is shipped between executors during the
  * merge phase via `serialize` / `deserialize`.
  *
  * A safety cap (default ~50 MiB of WKB across one buffer) guards against
  * runaway pipelines that try to IDW millions of points through one group;
  * IDW is O(n_points x n_cells) so the practical limit is much smaller anyway.
  */
final class GridFromPointsAcc(
    val features: ArrayBuffer[(Array[Byte], Double)] = ArrayBuffer.empty,
    private var byteSize: Long = 0L
) extends Serializable {

    def add(wkb: Array[Byte], value: Double): GridFromPointsAcc = {
        if (wkb != null && wkb.length > 0) {
            features += ((wkb, value))
            byteSize += wkb.length.toLong
            GridFromPointsAcc.guardSize(byteSize)
        }
        this
    }

    def merge(other: GridFromPointsAcc): GridFromPointsAcc = {
        features ++= other.features
        byteSize += other.byteSize
        GridFromPointsAcc.guardSize(byteSize)
        this
    }

    def approxByteSize: Long = byteSize

    def serialize: Array[Byte] = {
        val bos = new ByteArrayOutputStream()
        val out = new DataOutputStream(bos)
        out.writeInt(features.length)
        for ((wkb, v) <- features) {
            out.writeInt(wkb.length)
            out.write(wkb)
            out.writeDouble(v)
        }
        bos.toByteArray
    }
}

object GridFromPointsAcc {

    /** Hard cap on the per-buffer WKB byte count - guards memory blow-ups. */
    val MAX_BUFFER_BYTES: Long = 50L * 1024L * 1024L

    def empty: GridFromPointsAcc = new GridFromPointsAcc()

    def deserialize(bytes: Array[Byte]): GridFromPointsAcc = {
        val in = new DataInputStream(new ByteArrayInputStream(bytes))
        val n = in.readInt()
        val buf = ArrayBuffer.empty[(Array[Byte], Double)]
        var total: Long = 0L
        var i = 0
        while (i < n) {
            val len = in.readInt()
            val wkb = new Array[Byte](len)
            if (len > 0) in.readFully(wkb)
            val v = in.readDouble()
            buf += ((wkb, v))
            total += len.toLong
            i += 1
        }
        new GridFromPointsAcc(buf, total)
    }

    private[grid] def guardSize(currentBytes: Long): Unit = {
        if (currentBytes > MAX_BUFFER_BYTES) {
            throw new IllegalStateException(
                s"GridFromPoints aggregator buffer exceeded ${MAX_BUFFER_BYTES / (1024 * 1024)} MiB " +
                s"(current = ${currentBytes / (1024 * 1024)} MiB). " +
                s"IDW with millions of points is impractical; tile the workload or use a sparser " +
                s"max_points parameter."
            )
        }
    }
}
