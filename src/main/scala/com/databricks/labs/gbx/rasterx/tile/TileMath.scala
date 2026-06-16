package com.databricks.labs.gbx.rasterx.tile

/** Web-mercator (XYZ slippy-map) tile coordinate ↔ bbox math.
 *
 *  Tile (0,0) at z=0 covers the entire world in EPSG:3857. At zoom z, the world is
 *  divided into `2^z × 2^z` tiles. Y increases downward (north → south) per the
 *  standard XYZ scheme; Google / OSM / MapboxGL / Maplibre / PMTiles all follow this.
 *
 *  All math is pure and CRS-only — callers do not need GDAL to use this helper. The
 *  expressions in `rasterx/expressions/web/` use these bboxes as `-te` extents for
 *  `gdal.Warp` to render web-mercator tiles.
 */
object TileMath {

    /** Web-mercator world half-width / half-height in metres (EPSG:3857 valid extent). */
    val WEBMERC_MAX: Double = 20037508.342789244
    val WEBMERC_MIN: Double = -WEBMERC_MAX

    /** Latitude clip for the web-mercator projection (≈ ±85.0511°); values beyond go to ±∞ under
     *  the gudermannian, so any lon/lat ↔ web-mercator conversion must clamp here.
     */
    val MERC_LAT_LIMIT: Double = 85.05112878

    /** Maximum supported zoom for the pyramid generator — beyond this the tile-count explodes
     *  (`4^z` grows past 10^12 by z=21), and a single PNG render at z>20 produces ~mm-resolution
     *  output which exceeds any practical use case. Callers pass a guard here to fail fast.
     */
    val MAX_ZOOM: Int = 20

    /** Returns the bbox `(xmin, ymin, xmax, ymax)` in EPSG:3857 for the XYZ tile `(z, x, y)`.
     *
     *  Throws `IllegalArgumentException` if `z < 0` or `(x, y)` is outside `[0, 2^z)`.
     */
    def tileBboxWebMerc(z: Int, x: Int, y: Int): (Double, Double, Double, Double) = {
        require(z >= 0, s"zoom must be >= 0; got $z")
        val n = 1 << z
        require(x >= 0 && x < n && y >= 0 && y < n, s"tile ($x, $y) out of range at z=$z (n=$n)")
        val tileSize = (WEBMERC_MAX - WEBMERC_MIN) / n.toDouble
        val xmin = WEBMERC_MIN + x * tileSize
        val xmax = xmin + tileSize
        val ymax = WEBMERC_MAX - y * tileSize
        val ymin = ymax - tileSize
        (xmin, ymin, xmax, ymax)
    }

    /** Returns all XYZ tile coordinates whose web-mercator bbox intersects the input
     *  bbox `(lonMin, latMin, lonMax, latMax)` (EPSG:4326 / lon-lat degrees) at zoom `z`.
     *
     *  Latitudes are clamped to ±MERC_LAT_LIMIT before projection. Tile X/Y are clamped
     *  to `[0, 2^z)` so a fully-out-of-globe input bbox returns the closest edge tiles
     *  rather than indices that would crash downstream renderers.
     */
    def intersectingTiles(
        lonMin: Double, latMin: Double, lonMax: Double, latMax: Double, z: Int
    ): Array[(Int, Int, Int)] = {
        require(z >= 0, s"zoom must be >= 0; got $z")
        val (xMinM, yMinM) = lonLatToWebMerc(lonMin, math.max(-MERC_LAT_LIMIT, latMin))
        val (xMaxM, yMaxM) = lonLatToWebMerc(lonMax, math.min(MERC_LAT_LIMIT, latMax))
        val n = 1 << z
        val tileSize = (WEBMERC_MAX - WEBMERC_MIN) / n.toDouble
        val xFrom = math.max(0, math.floor((xMinM - WEBMERC_MIN) / tileSize).toInt)
        val xTo   = math.min(n - 1, math.floor((xMaxM - WEBMERC_MIN) / tileSize).toInt)
        // Y is north-down in XYZ — invert the meridian axis when binning.
        val yFrom = math.max(0, math.floor((WEBMERC_MAX - yMaxM) / tileSize).toInt)
        val yTo   = math.min(n - 1, math.floor((WEBMERC_MAX - yMinM) / tileSize).toInt)
        val buf = scala.collection.mutable.ArrayBuffer.empty[(Int, Int, Int)]
        var xi = xFrom
        while (xi <= xTo) {
            var yi = yFrom
            while (yi <= yTo) {
                buf += ((z, xi, yi))
                yi += 1
            }
            xi += 1
        }
        buf.toArray
    }

    /** Counts intersecting tiles without materializing the array — cheap upper-bound for
     *  guarding pyramid expansion against runaway cell counts. */
    def intersectingTileCount(
        lonMin: Double, latMin: Double, lonMax: Double, latMax: Double, z: Int
    ): Long = {
        require(z >= 0, s"zoom must be >= 0; got $z")
        val (xMinM, yMinM) = lonLatToWebMerc(lonMin, math.max(-MERC_LAT_LIMIT, latMin))
        val (xMaxM, yMaxM) = lonLatToWebMerc(lonMax, math.min(MERC_LAT_LIMIT, latMax))
        val n = 1 << z
        val tileSize = (WEBMERC_MAX - WEBMERC_MIN) / n.toDouble
        val xFrom = math.max(0, math.floor((xMinM - WEBMERC_MIN) / tileSize).toInt)
        val xTo   = math.min(n - 1, math.floor((xMaxM - WEBMERC_MIN) / tileSize).toInt)
        val yFrom = math.max(0, math.floor((WEBMERC_MAX - yMaxM) / tileSize).toInt)
        val yTo   = math.min(n - 1, math.floor((WEBMERC_MAX - yMinM) / tileSize).toInt)
        (xTo - xFrom + 1).toLong * (yTo - yFrom + 1).toLong
    }

    /** WGS84 semi-major axis in metres (used as the web-mercator sphere radius). */
    private val R: Double = 6378137.0
    private val D2R: Double = math.Pi / 180.0

    /** Forward Pseudo-Mercator transform (lon/lat → easting/northing in EPSG:3857). */
    private def lonLatToWebMerc(lon: Double, lat: Double): (Double, Double) = {
        val x = lon * D2R * R
        val y = math.log(math.tan(math.Pi / 4.0 + lat * D2R / 2.0)) * R
        (x, y)
    }
}
