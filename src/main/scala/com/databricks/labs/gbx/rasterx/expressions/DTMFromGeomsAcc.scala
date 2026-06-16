package com.databricks.labs.gbx.rasterx.expressions

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import scala.collection.mutable.ArrayBuffer

/** Mutable aggregation buffer for [[RST_DTMFromGeomsAgg]]: accumulates point WKB byte
 *  arrays (Z carried in the geometry). Shipped between executors via serialize/deserialize.
 */
final class DTMFromGeomsAcc(
    val points: ArrayBuffer[Array[Byte]] = ArrayBuffer.empty,
    private var byteSize: Long = 0L
) extends Serializable {

    def add(wkb: Array[Byte]): DTMFromGeomsAcc = {
        if (wkb != null && wkb.length > 0) {
            points += wkb
            byteSize += wkb.length.toLong
            DTMFromGeomsAcc.guardSize(byteSize)
        }
        this
    }

    def merge(other: DTMFromGeomsAcc): DTMFromGeomsAcc = {
        points ++= other.points
        byteSize += other.byteSize
        DTMFromGeomsAcc.guardSize(byteSize)
        this
    }

    def serialize: Array[Byte] = {
        val bos = new ByteArrayOutputStream()
        val out = new DataOutputStream(bos)
        out.writeInt(points.length)
        for (wkb <- points) { out.writeInt(wkb.length); out.write(wkb) }
        bos.toByteArray
    }
}

object DTMFromGeomsAcc {

    /** Hard cap on accumulated WKB bytes per buffer (guards memory blow-ups). */
    val MAX_BUFFER_BYTES: Long = 200L * 1024L * 1024L

    def empty: DTMFromGeomsAcc = new DTMFromGeomsAcc()

    def deserialize(bytes: Array[Byte]): DTMFromGeomsAcc = {
        val in = new DataInputStream(new ByteArrayInputStream(bytes))
        val n = in.readInt()
        val buf = ArrayBuffer.empty[Array[Byte]]
        var total = 0L
        var i = 0
        while (i < n) {
            val len = in.readInt()
            val wkb = new Array[Byte](len)
            if (len > 0) in.readFully(wkb)
            buf += wkb
            total += len.toLong
            i += 1
        }
        new DTMFromGeomsAcc(buf, total)
    }

    private[expressions] def guardSize(currentBytes: Long): Unit = {
        if (currentBytes > MAX_BUFFER_BYTES) {
            throw new IllegalStateException(
                s"rst_dtmfromgeoms_agg buffer exceeded ${MAX_BUFFER_BYTES / (1024 * 1024)} MiB " +
                s"(current = ${currentBytes / (1024 * 1024)} MiB). Tile the workload by extent.")
        }
    }
}
