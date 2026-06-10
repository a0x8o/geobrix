package com.databricks.labs.gbx.vectorx.mvt

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.gdal
import org.gdal.ogr.ogr.{CreateGeometryFromWkb, GetDriverByName}
import org.gdal.ogr.{Feature, FieldDefn}
import org.gdal.ogr.ogrConstants.{OFTString, wkbUnknown}
import org.gdal.osr.SpatialReference

import java.nio.file.{Files, Paths}
import java.util.{Vector => JVector}
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * Helper that wraps GDAL's OGR MVT driver to encode a list of `(geom_wkb, attrs_map)` tuples
  * into a single Mapbox Vector Tile (MVT) protobuf blob.
  *
  * Caller passes geometries in **tile-local coordinates** (post-clip, post-transform); the
  * writer just packages them. With `MINZOOM=0`, `MAXZOOM=0`, `EXTENT=4096`, the GDAL MVT
  * driver produces exactly one tile at `0/0/0.pbf` and we return its raw bytes. All
  * intermediate state lives in `/vsimem/<uuid>/` and is unlinked before returning.
  *
  * Attribute fields are all encoded as `OFTString` in v0.4.0 (per Wave 1 scope); native
  * int/double preservation is deferred. Field schema is derived from the first non-null
  * attrs map.
  *
  * GDAL resource management (per "GDAL resource management" in CLAUDE.md): every
  * OGR `Feature` and `Geometry` allocated inside the loop is `.delete()`'d immediately,
  * the layer/datasource are closed via `ds.delete()`, and `gdal.RmdirRecursive` cleans
  * up the `/vsimem/` directory at the end.
  */
object MvtWriter {

    /** Default extent for a tile (4096 units = MVT v2 standard). */
    val DefaultExtent: Int = 4096

    /**
      * Encode features into a single MVT protobuf blob.
      *
      * @param layerName MVT layer name (e.g. "roads")
      * @param extent    Tile extent in pixels; defaults to 4096 (MVT v2)
      * @param features  Per-feature (WKB bytes, attrs Map[fieldName -> Any (stringified)])
      * @return MVT protobuf bytes; empty Array[Byte] if no features were written
      *         (e.g. empty input or all geometries failed to parse).
      */
    def encode(
        layerName: String,
        extent: Int,
        features: Seq[(Array[Byte], Map[String, Any])]
    ): Array[Byte] = {
        ensureNativeLoaded()
        // Register OGR drivers once per JVM via the shared guard so concurrent MVT-encoding
        // tasks can't race the process-global driver registry. (init() needs an ExpressionConfig
        // not available on this static helper path; initOgr only registers OGR under the lock,
        // which is all this path needs — the native lib is already loaded above.)
        GDALManager.initOgr()
        val driver = GetDriverByName("MVT")
        if (driver == null) {
            throw new RuntimeException(
              "OGR MVT driver not found. Ensure GDAL is built with MVT driver support."
            )
        }

        val uuid = java.util.UUID.randomUUID().toString.replace("-", "_")
        val rootPath = s"/vsimem/gbx_mvt_$uuid"

        // Create options: MAXZOOM=MINZOOM=0 → single tile at z/x/y = 0/0/0.
        val createOpts = new JVector[String]()
        createOpts.addAll(Seq(
          "MAXZOOM=0",
          "MINZOOM=0",
          "COMPRESS=NO",
          s"EXTENT=$extent",
          "FORMAT=DIRECTORY"
        ).asJava)

        val ds = driver.CreateDataSource(rootPath, createOpts)
        if (ds == null) {
            throw new RuntimeException(
              s"MVT driver failed to create datasource at $rootPath: ${gdal.GetLastErrorMsg()}"
            )
        }

        val srs = new SpatialReference()
        try {
            // EPSG:3857 is the canonical MVT projection — the driver expects this for its
            // tile-bound calculations even though we feed in tile-local coordinates.
            srs.ImportFromEPSG(3857)
            val layer = ds.CreateLayer(layerName, srs, wkbUnknown)
            if (layer == null) {
                throw new RuntimeException(s"Failed to create MVT layer '$layerName'")
            }

            // Derive field schema from the first non-null attrs map. All fields are OFTString
            // in v0.4.0 (numeric/boolean preservation deferred). Use a stable key ordering.
            val schema: Seq[String] = features
                .iterator
                .map(_._2)
                .find(_ != null)
                .map(_.keys.toSeq)
                .getOrElse(Seq.empty)

            schema.foreach { fieldName =>
                val fd = new FieldDefn(fieldName, OFTString)
                layer.CreateField(fd)
                fd.delete()
            }

            // Add each feature; pair every alloc with a delete() to avoid native-side leaks.
            features.foreach { case (wkb, attrs) =>
                if (wkb != null && wkb.nonEmpty) {
                    // GDAL 3.x can throw or return null on malformed WKB depending on
                    // exception-mode config — handle both so a single bad feature can't
                    // sink the whole tile.
                    val geom = Try(CreateGeometryFromWkb(wkb)).toOption.orNull
                    if (geom != null) {
                        val feat = new Feature(layer.GetLayerDefn())
                        try {
                            feat.SetGeometry(geom)
                            if (attrs != null) {
                                schema.foreach { fieldName =>
                                    attrs.get(fieldName).foreach { v =>
                                        if (v != null) feat.SetField(fieldName, v.toString)
                                    }
                                }
                            }
                            layer.CreateFeature(feat)
                        } finally {
                            feat.delete()
                            geom.delete()
                        }
                    }
                }
            }

            // Reset any error state set by per-feature WKB-parse failures so that
            // SyncToDisk doesn't surface a stale CPL_ERROR_HANDLER message as a
            // RuntimeException when GDAL UseExceptions is enabled.
            gdal.ErrorReset()
            // SyncToDisk is best-effort: an empty or partially-failed layer can throw
            // (e.g. "OGR Error: General Error" on Sync) — we catch and let the /vsimem/
            // walk below decide whether any .pbf was actually produced.
            Try(layer.SyncToDisk())
            Try(ds.SyncToDisk())
        } finally {
            ds.delete()
            srs.delete()
        }

        // Walk /vsimem/<uuid>/ to find the .pbf file emitted by the MVT driver. With
        // MAXZOOM=MINZOOM=0 there should be exactly one — at <root>/0/0/0.pbf. If no .pbf
        // was written (empty group), return an empty Array[Byte] (caller treats as
        // "non-null, empty layer").
        val pbfPath = findPbf(rootPath)
        val bytes =
            if (pbfPath == null) Array.emptyByteArray
            else {
                val buf = gdal.GetMemFileBuffer(pbfPath)
                if (buf == null) Array.emptyByteArray else buf
            }

        // Clean up the entire /vsimem/<uuid>/ tree (metadata.json + tile dirs).
        gdal.RmdirRecursive(rootPath)

        bytes
    }

    @volatile private var nativeLoaded: Boolean = false
    private val nativeLock = new Object

    /**
      * Ensure the GDAL JNI shared library is loaded on this JVM (executor or driver).
      *
      * `ogr.RegisterAll()` and `ogr.GetDriverByName` both require `libgdalalljni.so`
      * to have been `System.load`-ed first. RasterX does this via
      * `GDALManager.loadSharedObjects` when its `register(spark)` runs, but VectorX
      * has no equivalent yet — and the call has to happen on the *executor* JVM
      * before any OGR access, not just on the driver. Idempotent guard avoids
      * reloading the library.
      */
    private def ensureNativeLoaded(): Unit = {
        if (!nativeLoaded) {
            nativeLock.synchronized {
                if (!nativeLoaded) {
                    val path = "/usr/lib/libgdalalljni.so"
                    Try {
                        if (Files.exists(Paths.get(path))) System.load(path)
                    } // any failure surfaces as the original UnsatisfiedLinkError below
                    nativeLoaded = true
                }
            }
        }
    }

    /**
      * Find the first `.pbf` file under `/vsimem/<root>/`. Uses `gdal.ReadDirRecursive`,
      * which returns relative paths. Returns the absolute path of the first `.pbf` found,
      * or `null` if none.
      */
    private def findPbf(root: String): String = {
        val entries = gdal.ReadDirRecursive(root)
        if (entries == null) return null
        val it = entries.asScala.iterator
        while (it.hasNext) {
            val rel = it.next().toString
            if (rel.endsWith(".pbf")) return s"$root/$rel"
        }
        null
    }

}
