package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.operations.BandAccessors
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.gdal.gdal.Dataset

/** Output fingerprint matching python bench/fingerprint.py for cross-API consistency. */
object BenchFingerprint {
  private val mapper = new ObjectMapper()

  /** Timing-only sentinel: an empty fingerprint. The comparator treats an empty
    * fingerprint on either side as `na` (timed, not compared), so functions whose
    * output cannot be made cross-engine-comparable (maps/structs, CRS/render-
    * dependent bytes, on-disk-vs-in-memory sizes) emit this after executing for
    * real timing. */
  val empty: String = ""

  def ofScalar(v: Any): String = {
    val n = mapper.createObjectNode()
    n.put("kind", "scalar")
    v match {
      case i: Int    => n.put("value", i)
      case l: Long   => n.put("value", l)
      case d: Double => n.put("value", d)
      case other     => n.put("value", other.toString)
    }
    mapper.writeValueAsString(n)
  }

  def ofArray(values: Array[Double]): String = {
    val n = mapper.createObjectNode()
    n.put("kind", "scalar_list")
    val arr: ArrayNode = n.putArray("values")
    values.foreach(arr.add)
    mapper.writeValueAsString(n)
  }

  /** Fingerprint a COLLECTION of output tiles (bucket C, group C4 tiling fns).
    *
    * Mirrors python bench/fingerprint.py `fingerprint_collection`: records the tile
    * COUNT plus the agg stats POOLED over every tile's valid (non-nodata) pixels
    * across all bands. Pooling is ORDER-INDEPENDENT, so heavy and light may emit
    * tiles in any order and still agree; the comparator compares `count` exactly. */
  def ofCollection(tiles: Seq[Dataset]): String = {
    val pooled = scala.collection.mutable.ArrayBuffer.empty[Double]
    tiles.foreach { ds =>
      val w = ds.GetRasterXSize()
      val h = ds.GetRasterYSize()
      for (bi <- 1 to ds.GetRasterCount()) {
        val band = ds.GetRasterBand(bi)
        val buf = Array.ofDim[Double](w * h)
        band.ReadRaster(0, 0, w, h, buf)
        val nod = BandAccessors.getNoDataValue(band)
        val valid = if (nod.isNaN) buf else buf.filterNot(_ == nod)
        pooled ++= valid
        band.delete()
      }
    }
    val n = mapper.createObjectNode()
    n.put("kind", "raster_collection")
    n.put("count", tiles.length)
    val agg: ObjectNode = n.putObject("agg")
    if (pooled.isEmpty) {
      agg.putNull("min"); agg.putNull("max"); agg.putNull("mean"); agg.putNull("std")
    } else {
      val mean = pooled.sum / pooled.length
      val variance = pooled.map(x => (x - mean) * (x - mean)).sum / pooled.length
      agg.put("min", pooled.min)
      agg.put("max", pooled.max)
      agg.put("mean", mean)
      agg.put("std", math.sqrt(variance))
    }
    mapper.writeValueAsString(n)
  }

  def ofDataset(ds: Dataset): String = {
    val w = ds.GetRasterXSize()
    val h = ds.GetRasterYSize()
    val n = mapper.createObjectNode()
    n.put("kind", "raster")
    val bands: ArrayNode = n.putArray("bands")
    for (bi <- 1 to ds.GetRasterCount()) {
      val band = ds.GetRasterBand(bi)
      val buf = Array.ofDim[Double](w * h)
      band.ReadRaster(0, 0, w, h, buf)
      val nod = BandAccessors.getNoDataValue(band)
      val valid = if (nod.isNaN) buf else buf.filterNot(_ == nod)
      val bn: ObjectNode = mapper.createObjectNode()
      val shape = bn.putArray("shape"); shape.add(h); shape.add(w)
      bn.put("dtype", BandAccessors.dataTypeHuman(band))
      bn.put("nodata_count", (buf.length - valid.length).toLong)
      if (valid.isEmpty) {
        bn.putNull("min"); bn.putNull("max"); bn.putNull("mean"); bn.putNull("std")
      } else {
        val mean = valid.sum / valid.length
        val variance = valid.map(x => (x - mean) * (x - mean)).sum / valid.length
        bn.put("min", valid.min)
        bn.put("max", valid.max)
        bn.put("mean", mean)
        bn.put("std", math.sqrt(variance))
      }
      bands.add(bn)
      band.delete()
    }
    mapper.writeValueAsString(n)
  }
}
