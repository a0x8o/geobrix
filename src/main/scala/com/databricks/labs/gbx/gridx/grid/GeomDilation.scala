package com.databricks.labs.gbx.gridx.grid

import org.locationtech.jts.geom.{Geometry, Polygon, GeometryFactory}
import scala.collection.mutable

object GeomDilation {
  val DEFAULT_MODE = "boundary-out"
  val MODES: Seq[String] = Seq(
    "boundary-out", "boundary-in", "boundary-in-ignore-holes",
    "hole-in", "hole-out", "hole-out-ignore-geom")

  final case class Classification(
      pCover: Set[Long], pCore: Set[Long],
      sCover: Set[Long], sCore: Set[Long],
      hCover: Set[Long], hCore: Set[Long]) {
    def pBorder: Set[Long] = pCover diff pCore
    def hBorder: Set[Long] = hCover diff hCore
  }

  private def solidAndHoles(geom: Geometry): (Geometry, Option[Geometry]) = {
    val gf = new GeometryFactory()
    val polys: Seq[Polygon] = (0 until geom.getNumGeometries).map(geom.getGeometryN)
      .collect { case p: Polygon => p }
    val solids: Seq[Geometry] = polys.map(p => gf.createPolygon(p.getExteriorRing.getCoordinates))
    val holes: Seq[Geometry] = polys.flatMap(p =>
      (0 until p.getNumInteriorRing).map(i => gf.createPolygon(p.getInteriorRingN(i).getCoordinates)))
    val solid: Geometry = if (solids.isEmpty) geom else solids.reduce(_ union _)
    val hole: Option[Geometry] = if (holes.isEmpty) None else Some(holes.reduce(_ union _))
    (solid, hole)
  }

  def classify(grid: GridSystem, geom: Geometry, res: Int): Classification = {
    val (solid, holeOpt) = solidAndHoles(geom)
    val cands = grid.polyfill(geom, res).toSet
    val pCover, pCore, sCover, sCore, hCover, hCore = mutable.Set.empty[Long]
    cands.foreach { c =>
      val g = grid.cellIdToGeometry(c)
      if (geom.intersects(g) && geom.intersection(g).getArea > 0) {
        pCover += c; if (geom.contains(g)) pCore += c
      }
      if (solid.intersects(g) && solid.intersection(g).getArea > 0) {
        sCover += c; if (solid.contains(g)) sCore += c
      }
      holeOpt.foreach { h =>
        if (h.intersects(g) && h.intersection(g).getArea > 0) {
          hCover += c; if (h.contains(g)) hCore += c
        }
      }
    }
    Classification(pCover.toSet, pCore.toSet, sCover.toSet, sCore.toSet, hCover.toSet, hCore.toSet)
  }

  private def setup(mode: String, cls: Classification)
      : (Set[Long], Set[Long], Long => Boolean, Set[Long]) = mode match {
    case "boundary-out"             => (cls.pBorder, cls.pCover, _ => true, cls.pCover)
    case "boundary-in"              => (cls.pBorder, cls.pBorder, cls.pCore.contains, cls.pBorder)
    case "boundary-in-ignore-holes" => (cls.pBorder, cls.pBorder, cls.sCore.contains, cls.pBorder)
    case "hole-in"                  => (cls.hBorder, cls.hBorder, cls.hCore.contains, cls.hBorder)
    case "hole-out"                 => (cls.hBorder, cls.hCover, cls.pCore.contains, cls.hBorder)
    case "hole-out-ignore-geom"     => (cls.hBorder, cls.hCover, (n: Long) => !cls.hCore.contains(n), cls.hBorder)
    case other => throw new IllegalArgumentException(s"unknown mode '$other'; expected ${MODES.mkString(",")}")
  }

  def expand(kind: String, k: Int, mode: String, grid: GridSystem, geom: Geometry, res: Int): Set[Long] = {
    val cls = classify(grid, geom, res)
    val (frontier0, visited0, admit, k0) = setup(mode, cls)
    if (k == 0) return k0
    val visited = mutable.Set.empty[Long] ++ visited0
    var frontier: Set[Long] = frontier0
    val acc = mutable.Set.empty[Long] ++ (if (kind == "ring") k0 else Set.empty[Long])
    var kk = 0
    var shellK = Set.empty[Long]
    while (frontier.nonEmpty && kk < k) {
      kk += 1
      val nxt = frontier.flatMap(c => grid.kLoop(c, 1)).filter(n => !visited.contains(n) && admit(n))
      if (nxt.isEmpty) { frontier = Set.empty }
      else {
        visited ++= nxt; frontier = nxt
        if (kind == "ring") acc ++= nxt
        if (kk == k) shellK = nxt
      }
    }
    if (kind == "ring") acc.toSet else shellK
  }
}
