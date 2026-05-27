package com.databricks.labs.gbx.pmtiles

import com.databricks.labs.gbx.expressions.WithExpressionInfo
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
  * UDAF: `gbx_pmtiles_agg(bytes, z, x, y, [metadata_json])` — aggregate a set of tile rows
  * into a single in-memory PMTile v3 binary blob.
  *
  * Inputs:
  *  - `bytes` (BINARY) — the tile payload (PNG / JPEG / WebP / MVT), passed through verbatim.
  *  - `z`, `x`, `y` (INT) — tile coordinates.
  *  - `metadata_json` (STRING, optional, defaults to `{}`) — JSON metadata stored in the
  *    PMTile spec section 5 metadata section.
  *
  * Output: BINARY (the PMTile blob). Tile type byte is auto-detected from the first non-null
  * payload's magic bytes (PNG, JPEG, WEBP, otherwise MVT).
  *
  * Limited to roughly the per-Spark-cell 2 GiB ceiling; for larger pyramids, use the
  * companion DataSource: `df.write.format("pmtiles").save(path)`.
  */
final case class PMTiles_Agg(
    bytesExpr: Expression,
    zExpr: Expression,
    xExpr: Expression,
    yExpr: Expression,
    metadataJsonExpr: Expression = Literal(UTF8String.fromString("{}"), StringType),
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[PMTilesAcc] {

    override lazy val deterministic: Boolean = true
    override val nullable: Boolean = false
    override val dataType: DataType = BinaryType
    override def prettyName: String = PMTiles_Agg.name

    override def children: Seq[Expression] = Seq(bytesExpr, zExpr, xExpr, yExpr, metadataJsonExpr)

    override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): PMTiles_Agg = {
        require(newChildren.length == 5, s"PMTiles_Agg expects 5 children; got ${newChildren.length}")
        copy(
            bytesExpr = newChildren(0),
            zExpr = newChildren(1),
            xExpr = newChildren(2),
            yExpr = newChildren(3),
            metadataJsonExpr = newChildren(4)
        )
    }

    override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = newOffset)
    override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = newOffset)

    override def createAggregationBuffer(): PMTilesAcc = PMTilesAcc.empty

    override def update(buffer: PMTilesAcc, input: InternalRow): PMTilesAcc = {
        val payload = bytesExpr.eval(input).asInstanceOf[Array[Byte]]
        if (payload == null) return buffer
        val z = zExpr.eval(input).asInstanceOf[Int]
        val x = xExpr.eval(input).asInstanceOf[Int]
        val y = yExpr.eval(input).asInstanceOf[Int]
        // Metadata is a per-group constant. If still at the default sentinel, snapshot from
        // the row so it survives the executor-shipping (serialize) hop.
        if (buffer.metadataJson == "{}") {
            val mj = metadataJsonExpr.eval(input)
            if (mj != null) buffer.withMetadata(mj.toString)
        }
        buffer.add(z, x, y, payload)
    }

    override def merge(a: PMTilesAcc, b: PMTilesAcc): PMTilesAcc = a.merge(b)

    override def eval(buffer: PMTilesAcc): Any = {
        if (buffer.tiles.isEmpty) {
            // Empty group: emit a valid header-only PMTile so downstream callers always get bytes.
            PMTilesV3Encoder.encode(Iterator.empty, buffer.metadataJson)
        } else {
            val firstNonNull = buffer.tiles.iterator.map(_._4).find(b => b != null && b.nonEmpty)
            val tileType = firstNonNull.map(PMTiles_Agg.detectTileType).getOrElse(PMTilesV3Encoder.TILE_TYPE_MVT)
            PMTilesV3Encoder.encode(buffer.tiles.iterator, buffer.metadataJson, tileType)
        }
    }

    override def serialize(b: PMTilesAcc): Array[Byte] = b.serialize
    override def deserialize(bytes: Array[Byte]): PMTilesAcc = PMTilesAcc.deserialize(bytes)
}

/** Companion: SQL name `gbx_pmtiles_agg`, 4-or-5-arg builder, tile-type magic-byte sniffer. */
object PMTiles_Agg extends WithExpressionInfo {

    override def name: String = "gbx_pmtiles_agg"

    /**
      * Builder accepts 4 args (bytes, z, x, y) or 5 args (bytes, z, x, y, metadata_json).
      * The 4-arg form defaults metadata to `{}`.
      */
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => {
        require(c.length == 4 || c.length == 5,
            s"$name expects 4 (bytes, z, x, y) or 5 (bytes, z, x, y, metadata_json) arguments; got ${c.length}")
        if (c.length == 4) {
            PMTiles_Agg(c(0), c(1), c(2), c(3))
        } else {
            PMTiles_Agg(c(0), c(1), c(2), c(3), c(4))
        }
    }

    /**
      * Sniff the tile content type from the first magic bytes of a tile payload.
      *
      * Magic byte references:
      *  - PNG: `89 50 4E 47 0D 0A 1A 0A` (ISO/IEC 15948).
      *  - JPEG: `FF D8`.
      *  - WebP: `RIFF ???? WEBP` (RIFF header at 0..3, `WEBP` at 8..11).
      *
      * Defaults to MVT (0x01) for anything else — MVT is a protobuf with no fixed magic byte.
      */
    private[pmtiles] def detectTileType(bytes: Array[Byte]): Byte = {
        if (bytes == null || bytes.length < 2) return PMTilesV3Encoder.TILE_TYPE_MVT
        // PNG: 0x89 0x50 0x4E 0x47 ...
        if (bytes.length >= 4 &&
            (bytes(0) & 0xFF) == 0x89 && bytes(1) == 0x50.toByte && bytes(2) == 0x4E.toByte && bytes(3) == 0x47.toByte) {
            return PMTilesV3Encoder.TILE_TYPE_PNG
        }
        // JPEG: 0xFF 0xD8.
        if ((bytes(0) & 0xFF) == 0xFF && (bytes(1) & 0xFF) == 0xD8) {
            return PMTilesV3Encoder.TILE_TYPE_JPEG
        }
        // WebP: "RIFF" at 0..3 and "WEBP" at 8..11.
        if (bytes.length >= 12 &&
            bytes(0) == 'R'.toByte && bytes(1) == 'I'.toByte && bytes(2) == 'F'.toByte && bytes(3) == 'F'.toByte &&
            bytes(8) == 'W'.toByte && bytes(9) == 'E'.toByte && bytes(10) == 'B'.toByte && bytes(11) == 'P'.toByte) {
            return PMTilesV3Encoder.TILE_TYPE_WEBP
        }
        PMTilesV3Encoder.TILE_TYPE_MVT
    }
}
