package com.databricks.labs.gbx.gridx.quadbin.agg

import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer

/** Accumulator for Quadbin_CellUnionAgg. Holds streaming BIGINT cell ids. */
final case class QuadbinUnionAcc(cells: ArrayBuffer[Long]) {

    /** Append a cell id. */
    def add(cell: Long): QuadbinUnionAcc = { cells += cell; this }

    /** Merge another accumulator into this one. */
    def merge(other: QuadbinUnionAcc): QuadbinUnionAcc = { cells ++= other.cells; this }

    // serde: [count(4)][id(8)]*N
    def serialize: Array[Byte] = {
        val n = cells.size
        val bb = ByteBuffer.allocate(4 + n * 8)
        bb.putInt(n)
        cells.foreach(bb.putLong)
        bb.array()
    }

}

object QuadbinUnionAcc {

    def empty: QuadbinUnionAcc = QuadbinUnionAcc(scala.collection.mutable.ArrayBuffer.empty[Long])

    def deserialize(bytes: Array[Byte]): QuadbinUnionAcc = {
        val bb = ByteBuffer.wrap(bytes)
        val n = bb.getInt
        val buf = ArrayBuffer.empty[Long]
        var i = 0
        while (i < n) { buf += bb.getLong; i += 1 }
        QuadbinUnionAcc(buf)
    }

}
