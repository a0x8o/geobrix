package com.databricks.labs.gbx.rasterx.operations

/** Delaunay triangulation and Z interpolation for DTM. Used by RST_DTMFromGeoms. */
import com.databricks.labs.gbx.vectorx.jts.{JTS, JTSConformingDelaunayTriangulationBuilder}
import org.locationtech.jts.geom.util.{LinearComponentExtracter, PolygonExtracter}
import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.linearref.LengthIndexedLine

import scala.jdk.CollectionConverters._

/** Delaunay triangulation from points and breaklines; interpolates Z at grid points and builds point grids. */
object InterpolateElevation {

    /** Builds triangulation from multipoint and breaklines, then interpolates Z for each grid point. */
    def interpolate(
        multipoint: MultiPoint,
        breaklines: Seq[LineString],
        gridPoints: MultiPoint,
        mergeTolerance: Double,
        snapTolerance: Double
    ): Seq[Point] = {
        val triangles = triangulate(multipoint, breaklines, mergeTolerance, snapTolerance)

        val tree = new STRtree(4)
        triangles.foreach(p => tree.insert(p.getEnvelopeInternal, p))

        val pointsSeq = (0 until gridPoints.getNumGeometries)
            .map(i => gridPoints.getGeometryN(i))
            .collect { case p: org.locationtech.jts.geom.Point => p }
        pointsSeq
            .map(p => {
                p -> tree
                    .query(p.getEnvelopeInternal)
                    .asScala
                    .map(_.asInstanceOf[Polygon])
                    .find(_.intersects(p))
            })
            .toMap
            .collect({ case (pt, Some(ply)) => pt -> ply })
            .flatMap({ case (point: Point, poly: Polygon) =>
                val polyCoords = poly.getCoordinates
                val tri = new Triangle(polyCoords(0), polyCoords(1), polyCoords(2))
                val z = tri.interpolateZ(point.getCoordinate)
                if (z.isNaN) {
                    None // cell with degenerate triangle -> caller treats as no_data
                } else {
                    val ip = JTS.point(new Coordinate(point.getX, point.getY, z))
                    ip.setSRID(multipoint.getSRID)
                    Some(ip)
                }
            })
            .toSeq
    }

    /** Returns constrained Delaunay triangles from multiPoint and optional breaklines. */
    def triangulate(
        multiPoint: Geometry,
        breaklines: Seq[Geometry],
        mergeTolerance: Double,
        snapTolerance: Double
    ): Seq[Geometry] = {
        val multiLineString = JTS.multiLineString(breaklines)
        val triangulator = JTSConformingDelaunayTriangulationBuilder(multiPoint)
        if (breaklines.nonEmpty) triangulator.setConstraints(multiLineString)

        triangulator.setTolerance(mergeTolerance)

        val trianglesGeomCollection = triangulator.getTriangles
        val trianglePolygons = PolygonExtracter.getPolygons(trianglesGeomCollection).asScala.map(_.asInstanceOf[Polygon])

        val postProcessedTrianglePolygons = postProcessTriangulation(trianglePolygons.toSeq, multiLineString, snapTolerance)
        postProcessedTrianglePolygons
    }

    /** Snaps triangle vertices to constraint lines within tolerance and rebuilds polygons. */
    private def postProcessTriangulation(
        trianglePolygons: Seq[Polygon],
        constraintLineGeom: Geometry,
        tolerance: Double
    ): Seq[Polygon] = {
        val geomFact = constraintLineGeom.getFactory

        val constraintLines = LinearComponentExtracter
            .getLines(constraintLineGeom)
            .iterator()
            .asScala
            .toSeq
            .map(_.asInstanceOf[LineString])

        val constraintLinesTree = new STRtree(4)
        constraintLines.foreach(l => constraintLinesTree.insert(l.getEnvelopeInternal, l))

        trianglePolygons.map(t => {
            val coords = t.getCoordinates.map(c => {
                /*
                 * overwrite the z values for every coordinate lying
                 * within a fraction of the value of `tolerance`.
                 */
                val coordPoint = geomFact.createPoint(c)
                val originatingLineString = constraintLinesTree
                    .query(new Envelope(c))
                    .iterator()
                    .asScala
                    .toSeq
                    .map(_.asInstanceOf[LineString])
                    .find(l => l.intersects(coordPoint.buffer(tolerance)))
                originatingLineString match {
                    case Some(l) =>
                        val indexedLine = new LengthIndexedLine(l)
                        val index = indexedLine.indexOf(c)
                        indexedLine.extractPoint(index)
                    case None    => c
                }
            })
            geomFact.createPolygon(coords)
        })
    }

    /** Regular grid of cell-center points over a bbox.
     *  Ordering: column-major (x index varies slowest, y index varies fastest).
     *  Cell size is derived: xRes = (xmax-xmin)/widthPx, yRes = (ymax-ymin)/heightPx.
     *  Centers: x = xmin + (i + 0.5)*xRes, y = ymin + (j + 0.5)*yRes.
     */
    def pointGridBBox(
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, srid: Int
    ): MultiPoint = {
        val xRes = (xmax - xmin) / widthPx
        val yRes = (ymax - ymin) / heightPx
        val pts = for (i <- 0 until widthPx; j <- 0 until heightPx) yield {
            val x = xmin + (i + 0.5) * xRes
            val y = ymin + (j + 0.5) * yRes
            val p = JTS.point(new Coordinate(x, y))
            p.setSRID(srid)
            p
        }
        val mp = JTS.multiPoint(pts.toArray)
        mp.setSRID(srid)
        mp
    }

}
