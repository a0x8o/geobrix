package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.operations.BandAccessors
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.gdal.gdal.Dataset

/** Output fingerprint matching python bench/fingerprint.py for cross-API consistency. */
object BenchFingerprint {
  private val mapper = new ObjectMapper()

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
