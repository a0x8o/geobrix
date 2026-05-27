package com.databricks.labs.gbx.vectorx.expressions

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

/**
  * Aggregation buffer for `gbx_st_asmvt`. Holds a layer name and a growing list of features —
  * each feature is a tuple of `(geom_wkb, attrs_bytes)` where `attrs_bytes` is a length-prefixed
  * encoding of the per-feature attribute struct (see ST_AsMvt.encodeAttrs / decodeAttrs).
  *
  * Buffer is mutable (the `ArrayBuffer` is appended in place via `add` and merge). Custom
  * binary serialize / deserialize avoids the need for Spark to know about the inner tuples
  * and keeps the wire format compact (no Kryo / Java Serializable required).
  */
final case class MvtAcc(
    layerName: String,
    features: scala.collection.mutable.ArrayBuffer[(Array[Byte], Array[Byte])]
) {
    /** Append one feature to the buffer; null/empty WKB rows are dropped. */
    def add(geomWkb: Array[Byte], attrsBytes: Array[Byte]): MvtAcc = {
        if (geomWkb != null && geomWkb.nonEmpty) features += ((geomWkb, attrsBytes))
        this
    }

    /** Merge another partial aggregator into this one (in place). Layer name comes from `this`. */
    def merge(other: MvtAcc): MvtAcc = { features ++= other.features; this }

    /** Length-prefixed binary encoding: layerName(UTF), count(int), then for each feature
      * (geomLen(int), geom[]); (attrsLen(int) or -1 if null), attrs[]). */
    def serialize: Array[Byte] = {
        val baos = new ByteArrayOutputStream()
        val out = new DataOutputStream(baos)
        out.writeUTF(layerName)
        out.writeInt(features.length)
        features.foreach { case (g, a) =>
            out.writeInt(g.length); out.write(g)
            if (a == null) out.writeInt(-1) else { out.writeInt(a.length); out.write(a) }
        }
        out.flush(); baos.toByteArray
    }
}

object MvtAcc {
    /** Create an empty buffer bound to a layer name. */
    def empty(layerName: String): MvtAcc =
        MvtAcc(layerName, scala.collection.mutable.ArrayBuffer.empty)

    /** Inverse of `serialize`. */
    def deserialize(bytes: Array[Byte]): MvtAcc = {
        val in = new DataInputStream(new ByteArrayInputStream(bytes))
        val layerName = in.readUTF()
        val n = in.readInt()
        val features = scala.collection.mutable.ArrayBuffer.empty[(Array[Byte], Array[Byte])]
        var i = 0
        while (i < n) {
            val gLen = in.readInt(); val g = new Array[Byte](gLen); in.readFully(g)
            val aLen = in.readInt()
            val a = if (aLen < 0) null else { val buf = new Array[Byte](aLen); in.readFully(buf); buf }
            features += ((g, a)); i += 1
        }
        MvtAcc(layerName, features)
    }
}
