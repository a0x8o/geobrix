package com.databricks.labs.gbx.vectorx.expressions

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import com.databricks.labs.gbx.vectorx.mvt.MvtWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.types._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

/**
  * Aggregate expression that encodes a group of `(geom_wkb, attrs_struct)` rows into a single
  * Mapbox Vector Tile (MVT) protobuf blob via `MvtWriter` (GDAL OGR MVT driver).
  *
  * Inputs:
  *   - `geomWkb`   : per-row geometry in WKB, in tile-local coordinates
  *   - `attrs`     : per-row attribute struct (all fields stringified in v0.4.0)
  *   - `layerName` : constant string column holding the MVT layer name
  *
  * Output: `BINARY` — the MVT protobuf bytes for one layer of the tile.
  *
  * Buffer: [[MvtAcc]] — holds the layer name and a list of per-feature
  * `(wkb_bytes, attrs_encoded_bytes)` tuples until the final encode pass.
  *
  * Follows the same `TypedImperativeAggregate` pattern as
  * `com.databricks.labs.gbx.gridx.bng.agg.BNG_CellUnionAgg`. The companion object's
  * `name = "gbx_st_asmvt"` is registered with Spark via
  * `com.databricks.labs.gbx.vectorx.functions.register`.
  */
final case class ST_AsMvt(
    geomWkb: Expression,
    attrs: Expression,
    layerName: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[MvtAcc] {

    override def children: Seq[Expression] = Seq(geomWkb, attrs, layerName)
    override def nullable: Boolean = false
    override def dataType: DataType = BinaryType
    override def prettyName: String = ST_AsMvt.name
    override lazy val deterministic: Boolean = true

    override def withNewMutableAggBufferOffset(n: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = n)

    override def withNewInputAggBufferOffset(n: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = n)

    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]
    ): ST_AsMvt = copy(
      geomWkb = newChildren(0),
      attrs = newChildren(1),
      layerName = newChildren(2)
    )

    /** Resolve the constant layer-name expression once per group; throws on non-foldable/null. */
    private def evalLayerName(): String = {
        if (!layerName.foldable) {
            throw new IllegalArgumentException(
              "gbx_st_asmvt: layerName must be a constant string expression"
            )
        }
        val v = layerName.eval(InternalRow.empty)
        if (v == null) {
            throw new IllegalArgumentException("gbx_st_asmvt: layerName must not be null")
        }
        v.toString
    }

    override def createAggregationBuffer(): MvtAcc = MvtAcc.empty(evalLayerName())

    override def update(buf: MvtAcc, input: InternalRow): MvtAcc = {
        val wkb = geomWkb.eval(input).asInstanceOf[Array[Byte]]
        if (wkb != null && wkb.length > 0) {
            val attrsRow = attrs.eval(input).asInstanceOf[InternalRow]
            val encoded = encodeAttrs(attrsRow)
            buf.add(wkb, encoded)
        }
        buf
    }

    override def merge(a: MvtAcc, b: MvtAcc): MvtAcc = a.merge(b)

    override def serialize(buf: MvtAcc): Array[Byte] = buf.serialize
    override def deserialize(bytes: Array[Byte]): MvtAcc = MvtAcc.deserialize(bytes)

    // Spark's TypedImperativeAggregate calls this method to emit the final aggregated
    // result from the buffer. Walks features, decodes per-feature attribute payloads,
    // hands them to MvtWriter for protobuf encoding.
    override def eval(buf: MvtAcc): Any = {
        val featuresWithAttrs: Seq[(Array[Byte], Map[String, Any])] =
            buf.features.iterator.map { case (wkb, attrsBytes) =>
                (wkb, decodeAttrs(attrsBytes))
            }.toSeq
        MvtWriter.encode(buf.layerName, MvtWriter.DefaultExtent, featuresWithAttrs)
    }

    /**
      * Encode the attribute struct row to a length-prefixed binary payload.
      *
      * Format: `num_fields(int)` then per field `(key_len(int), key_utf8_bytes,
      * val_len(int) or -1 if null, val_utf8_bytes?)`. All values are stringified
      * (`v.toString`) per Wave 1 scope.
      */
    private def encodeAttrs(row: InternalRow): Array[Byte] = {
        if (row == null) return null
        val schema = attrs.dataType.asInstanceOf[StructType]
        val baos = new ByteArrayOutputStream()
        val out = new DataOutputStream(baos)
        out.writeInt(schema.fields.length)
        var i = 0
        while (i < schema.fields.length) {
            val name = schema.fields(i).name
            val keyBytes = name.getBytes("UTF-8")
            out.writeInt(keyBytes.length); out.write(keyBytes)
            if (row.isNullAt(i)) {
                out.writeInt(-1)
            } else {
                val raw = row.get(i, schema.fields(i).dataType)
                val s = raw.toString
                val vBytes = s.getBytes("UTF-8")
                out.writeInt(vBytes.length); out.write(vBytes)
            }
            i += 1
        }
        out.flush(); baos.toByteArray
    }

    /** Inverse of [[encodeAttrs]]. */
    private def decodeAttrs(bytes: Array[Byte]): Map[String, Any] = {
        if (bytes == null) return Map.empty[String, Any]
        val in = new DataInputStream(new ByteArrayInputStream(bytes))
        val n = in.readInt()
        val builder = Map.newBuilder[String, Any]
        var i = 0
        while (i < n) {
            val keyLen = in.readInt()
            val keyBytes = new Array[Byte](keyLen); in.readFully(keyBytes)
            val key = new String(keyBytes, "UTF-8")
            val valLen = in.readInt()
            if (valLen >= 0) {
                val vBytes = new Array[Byte](valLen); in.readFully(vBytes)
                builder += key -> new String(vBytes, "UTF-8")
            }
            // valLen < 0 → null; drop the field for the writer (it skips missing keys)
            i += 1
        }
        builder.result()
    }

}

/** Companion: SQL name `gbx_st_asmvt`, builder. */
object ST_AsMvt extends WithExpressionInfo {

    override def name: String = "gbx_st_asmvt"

    override def builder(): FunctionBuilder = {
        case Seq(g, a, l) => ST_AsMvt(g, a, l)
        case other => throw new IllegalArgumentException(
              s"gbx_st_asmvt: expected (geom_wkb, attrs_struct, layer_name) — got ${other.length} args"
            )
    }

    override def usageArgs: String = "geom_wkb, attrs_struct, layer_name"

    override def description: String =
        "Aggregator: encodes features into a Mapbox Vector Tile (MVT) protobuf blob."
}
