package com.databricks.labs.gbx.bench

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.nio.file.{Files, Paths}

case class TileEntry(path: String, cellid: Long, srid: Int, dtype: String,
                     bands: Int, tile_px: Int, nodata_frac: Double)
case class RowPool(tile_px: Int, bands: Int, dtype: String, tiles: Seq[TileEntry])
case class Corpus(seed: Long, size_sweep: Seq[TileEntry], row_pool: RowPool)

object BenchManifest {
  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def read(path: String): Corpus = {
    val bytes = Files.readAllBytes(Paths.get(path))
    mapper.readValue(bytes, classOf[Corpus])
  }
}
