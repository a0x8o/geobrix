package com.databricks.labs.gbx.bench

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.nio.file.{Files, Paths}

case class TileEntry(path: String, cellid: Long, srid: Int, dtype: String,
                     bands: Int, tile_px: Int, nodata_frac: Double)
case class RowPool(tile_px: Int, bands: Int, dtype: String, tiles: Seq[TileEntry])
case class Corpus(seed: Long, size_sweep: Seq[TileEntry], row_pool: RowPool)

// --- geometry corpus (geometry.json) ----------------------------------------
// Geometry-input fns (rst_rasterize / rst_gridfrompoints / rst_dtmfromgeoms)
// read a deterministic, CRS-correct geometry set that BOTH engines read
// identically. WKB carries no CRS, so the srid is recorded on the set; geometry
// coords are in that CRS. WKB bytes are base64-encoded for JSON transport, so the
// heavy tier decodes the SAME bytes the pyrx tier wrote (write-once-read-both).
// Boxes / points are [base64Wkb, value] pairs; zpoints are bare base64 WKB.
case class GeometrySet(srid: Int, source_tile: String,
                       boxes: Seq[Seq[String]], points: Seq[Seq[String]],
                       zpoints: Seq[String]) {
  private def dec(b64: String): Array[Byte] = java.util.Base64.getDecoder.decode(b64)
  // (wkb, value) decoded pairs for boxes / points.
  def boxPairs: Seq[(Array[Byte], Double)] = boxes.map(p => (dec(p(0)), p(1).toDouble))
  def pointPairs: Seq[(Array[Byte], Double)] = points.map(p => (dec(p(0)), p(1).toDouble))
  def zpointWkbs: Seq[Array[Byte]] = zpoints.map(dec)
}
case class GeometryCorpus(seed: Long, srid: Int, source_tile: String,
                          sets: Map[String, GeometrySet]) {
  // The GeometrySet for a tile: by source_tile path, else by matching srid
  // (geometry is generated per CRS from a representative tile, so a tile whose own
  // path is not a geometry source still gets the in-extent set for its srid).
  def setFor(tilePath: String, tileSrid: Int): Option[GeometrySet] =
    sets.values.find(_.source_tile == tilePath)
      .orElse(sets.values.find(_.srid == tileSrid))
}

object BenchManifest {
  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def read(path: String): Corpus = {
    val bytes = Files.readAllBytes(Paths.get(path))
    mapper.readValue(bytes, classOf[Corpus])
  }

  /** Read the geometry corpus written alongside corpus.json, or None if absent
    * (older corpora have no geometry.json, so non-geometry runs still work). */
  def readGeometry(path: String): Option[GeometryCorpus] = {
    val p = Paths.get(path)
    if (!Files.exists(p)) None
    else Some(mapper.readValue(Files.readAllBytes(p), classOf[GeometryCorpus]))
  }
}
