package com.databricks.labs.gbx.gridx.grid

import org.locationtech.jts.geom.{Geometry, GeometryCollection, GeometryFactory, LineString, Polygon}
import org.locationtech.jts.linearref.LengthIndexedLine
import scala.collection.mutable
import scala.util.Try

object GeomDilation {
  val DEFAULT_MODE = "boundary-out"
  val MODES: Seq[String] = Seq(
    "boundary-out", "boundary-in", "boundary-in-ignore-holes",
    "hole-in", "hole-out", "hole-out-ignore-geom")

  // Coverage basis selector (LOCKED design, "COVERAGE PARAM"). Three nested predicates
  // decide which cells "belong to" a region P/S/H:
  //   coveras  -> cover    (*Cover)    : cell overlaps the region
  //   polyfill -> centroid (*Centroid) : cell CENTROID is inside the region
  //   core     -> core     (*Core)     : cell is fully contained by the region
  // Nested: cover ⊇ centroid ⊇ core. The chosen basis drives the seed (perimeter),
  // the admit bound, and boundary-ring inclusion.  Mirrors light `_dilate.COVERAGE`.
  val DEFAULT_COVERAGE = "coveras"
  val COVERAGE: Seq[String] = Seq("coveras", "polyfill", "core")

  final case class Classification(
      pCover: Set[Long], pCore: Set[Long],
      sCover: Set[Long], sCore: Set[Long],
      hCover: Set[Long], hCore: Set[Long],
      // Centroid basis (cell CENTROID inside the region) — the "polyfill" coverage.
      // Nested strictly between core and cover: core ⊆ centroid ⊆ cover.  Defaulted so
      // legacy 6-arg positional construction still compiles.
      pCentroid: Set[Long] = Set.empty, sCentroid: Set[Long] = Set.empty, hCentroid: Set[Long] = Set.empty) {
    def pBorder: Set[Long] = pCover diff pCore
    def hBorder: Set[Long] = hCover diff hCore

    /** (P_X, S_X, H_X) — belongs-to sets for the chosen coverage basis; validates `coverage`. */
    def basisSets(coverage: String): (Set[Long], Set[Long], Set[Long]) = coverage match {
      case "coveras"  => (pCover, sCover, hCover)
      case "polyfill" => (pCentroid, sCentroid, hCentroid)
      case "core"     => (pCore, sCore, hCore)
      case other =>
        throw new IllegalArgumentException(s"unknown coverage '$other'; expected ${COVERAGE.mkString(", ")}")
    }
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

  /** Topological dimension: 0=point, 1=line/ring, 2=surface/other. Mirrors `_geom_dimension`. */
  private def geomDimension(geom: Geometry): Int = geom.getGeometryType match {
    case "Point" | "MultiPoint"                       => 0
    case "LineString" | "LinearRing" | "MultiLineString" => 1
    case _                                            => 2
  }

  /** Coordinate samples along a point/line geometry, mirroring light `_sample_coords`
    * (centroid + explicit vertices + `nSamples-1` interior fractions for lines). Used only
    * as the point/line fallback when centroid-based polyfill yields no candidates. */
  private def sampleCoords(geom: Geometry, nSamples: Int = 16): Seq[(Double, Double)] = {
    val buf = mutable.ArrayBuffer.empty[(Double, Double)]
    def recurse(g: Geometry): Unit = g match {
      case gc: GeometryCollection => // Multi*/GeometryCollection (mirror hasattr(geom,'geoms'))
        (0 until gc.getNumGeometries).foreach(i => recurse(gc.getGeometryN(i)))
      case _ =>
        val c = g.getCentroid
        if (!c.isEmpty) buf += ((c.getX, c.getY))
        g.getCoordinates.foreach(coord => buf += ((coord.getX, coord.getY)))
        g match {
          case ls: LineString if nSamples > 0 =>
            val len = ls.getLength
            if (len > 0) {
              val lil = new LengthIndexedLine(ls)
              (1 until nSamples).foreach { i =>
                val p = lil.extractPoint(i.toDouble / nSamples * len)
                buf += ((p.x, p.y))
              }
            }
          case _ =>
        }
    }
    recurse(geom)
    buf.toSeq
  }

  def classify(grid: GridSystem, geom: Geometry, res: Int): Classification = {
    val (solid, holeOpt) = solidAndHoles(geom)
    val dim = geomDimension(geom)
    // polyfill the SOLID so hole-interior cells are classified (hole modes need hCore).
    val cands = mutable.Set.empty[Long] ++ grid.polyfill(solid, res)
    if (dim != 2 && cands.isEmpty) {
      // Non-polygon (point/line) fallback: BNG/custom centroid polyfill returns nothing, so
      // sample representative coordinates and map each to its containing cell (matches light).
      val seen = mutable.Set.empty[(Double, Double)]
      sampleCoords(geom).foreach { case (x, y) =>
        if (!seen.contains((x, y))) {
          seen += ((x, y))
          Try(grid.pointToCellID(x, y, res)).foreach(cands += _)
        }
      }
    }

    val pCover, pCore, sCover, sCore, hCover, hCore = mutable.Set.empty[Long]
    val pCentroid, sCentroid, hCentroid = mutable.Set.empty[Long]
    val is2d = dim == 2
    cands.foreach { c =>
      val g = grid.cellIdToGeometry(c)
      val cen = g.getCentroid // cell centroid — the "polyfill" (centroid) basis probe

      // Dimension-aware coverage: polygon uses area>0, line uses length>0, point uses intersects.
      val (pInCover, sInCover) = dim match {
        case 0 =>
          (geom.intersects(g), solid.intersects(g))
        case 1 =>
          val pl = geom.intersects(g) && geom.intersection(g).getLength > 0
          val sl = solid.intersects(g) && solid.intersection(g).getLength > 0
          (pl, sl)
        case _ =>
          val pa = geom.intersects(g) && geom.intersection(g).getArea > 0
          val sa = solid.intersects(g) && solid.intersection(g).getArea > 0
          (pa, sa)
      }
      // centroid and core bases are 2D-only: for a point/line region a cell centroid is
      // "inside" only by measure-zero coincidence, so polyfill/core coverage is empty by
      // construction (matches light — coherent and documented).
      if (pInCover) {
        pCover += c
        if (is2d && geom.contains(cen)) pCentroid += c
        if (is2d && geom.contains(g)) pCore += c
      }
      if (sInCover) {
        sCover += c
        if (is2d && solid.contains(cen)) sCentroid += c
        if (is2d && solid.contains(g)) sCore += c
      }
      holeOpt.foreach { h =>
        if (h.intersects(g) && h.intersection(g).getArea > 0) {
          hCover += c
          if (is2d && h.contains(cen)) hCentroid += c
          if (is2d && h.contains(g)) hCore += c
        }
      }
    }
    Classification(
      pCover.toSet, pCore.toSet, sCover.toSet, sCore.toSet, hCover.toSet, hCore.toSet,
      pCentroid.toSet, sCentroid.toSet, hCentroid.toSet)
  }

  /** Outer perimeter of a cell set: cells in `set` with at least one immediate neighbour
    * (kLoop distance 1) outside `set`. Alignment-robust — any non-empty set has a perimeter
    * even when the geometry aligns exactly to cell boundaries (zero straddling cells). For a
    * holed polygon, hole-interior cells are surrounded by other set cells and never appear. */
  def outerPerimeter(set: Set[Long], grid: GridSystem): Set[Long] =
    set.filter(c => grid.kLoop(c, 1).exists(n => !set.contains(n)))

  // `grid` computes perimeters; `coverage` selects the belongs-to basis X (cover/centroid/core).
  private def setup(mode: String, cls: Classification, grid: GridSystem, coverage: String)
      : (Set[Long], Set[Long], Long => Boolean, Set[Long]) = {
    if (!MODES.contains(mode))
      throw new IllegalArgumentException(s"unknown mode '$mode'; expected ${MODES.mkString(", ")}")
    val (pX, sX, hX) = cls.basisSets(coverage) // validates coverage

    if (mode.startsWith("boundary-")) {
      // boundary-* seed from the region-X outer perimeter (alignment-robust; excludes hole rim).
      val op = outerPerimeter(sX, grid)
      mode match {
        // boundary-out: OUTWARD from op; visited (pX ∪ op) blocks any inward path (the hole is
        // never reached — op is outer-only). k0 = op: the boundary covering RING is the step-0
        // anchor, symmetric with boundary-in (both seed k0=op, differ only in direction). The geom
        // INTERIOR (pX core) is still excluded (op ⊆ sBorder) — result = boundary ring + outward band.
        case "boundary-out"             => (op, pX ++ op, (_: Long) => true, op)
        // boundary-in: INWARD from op; admit pX (respect holes); k0 = op (boundary ring included).
        case "boundary-in"              => (op, op, pX.contains, op)
        // boundary-in-ignore-holes: INWARD from op; admit sX (marches across the hole interior).
        case "boundary-in-ignore-holes" => (op, op, sX.contains, op)
      }
    } else {
      // hole-* modes — alignment-robust hole-edge perimeter seeds (symmetric to outerPerimeter):
      //   voidEdge  = { c ∈ H_X : ∃ n ∉ H_X }   (hole-side cells adjacent to non-hole)
      //   solidEdge = { c ∈ P_X : ∃ n ∈ H_X }   (solid cells adjacent to the hole region)
      // Both are non-empty for a non-empty hole region regardless of grid alignment.
      val voidEdge  = hX.filter(c => grid.kLoop(c, 1).exists(n => !hX.contains(n)))
      val solidEdge = pX.filter(c => grid.kLoop(c, 1).exists(n => hX.contains(n)))
      mode match {
        // hole-in: seed void-side; expand INTO the hole interior (admit hX); visited = seed.
        case "hole-in"              => (voidEdge, voidEdge, hX.contains, voidEdge)
        // hole-out: seed solid-side; expand outward into solid (admit pX); visited = hX ∪ solidEdge
        // so the seed cannot reappear in shell 1 (disjoint-loop invariant / visited0 loop fix).
        case "hole-out"             => (solidEdge, hX ++ solidEdge, pX.contains, solidEdge)
        // hole-out-ignore-geom: solid-side seed; expand unbounded (admit not-in-hX). Same visited0 fix.
        case "hole-out-ignore-geom" => (solidEdge, hX ++ solidEdge, (n: Long) => !hX.contains(n), solidEdge)
      }
    }
  }

  def expand(kind: String, k: Int, mode: String, grid: GridSystem, geom: Geometry, res: Int,
             coverage: String = DEFAULT_COVERAGE): Set[Long] = {
    require(kind == "ring" || kind == "loop", s"kind must be 'ring' or 'loop'; got '$kind'")
    val cls = classify(grid, geom, res)
    val (frontier0, visited0, admit, k0) = setup(mode, cls, grid, coverage)
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
