package com.databricks.labs.gbx.vectorx.ds.geojsonl

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{BinaryType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.MapHasAsScala

/**
  * Spark Data Source V2 provider for the `geojsonl` heavyweight vector writer — the first
  * heavyweight vector writer.
  *
  * Write only:
  *   {{{
  *   df.write.format("geojsonl").mode("overwrite").save("/path/to/outdir")
  *   }}}
  *
  * It emits a DIRECTORY of newline-delimited GeoJSONL shards (OGR driver `GeoJSONSeq`, one
  * `Feature` per line) — one shard per partition, NO driver merge. An optional
  * `maxRecordsPerFile` splits a partition into several shards. This mirrors the lightweight
  * `geojsonl_gbx` writer and round-trips with the `geojson_ogr` (`multi=true`) directory reader.
  *
  * Input schema: a geometry column paired with a `<g>_srid` column (the geometry is the column
  * `X` that has a companion `X_srid`), an optional `<g>_srid_proj` PROJ4 column, and any other
  * columns as feature attributes — the same shape the `*_ogr` readers emit.
  */
//noinspection ScalaUnusedSymbol
class GeoJSONL_DataSource extends TableProvider with DataSourceRegister {

    /**
      * Overrides TableProvider.inferSchema: returns the supplied schema unchanged. Write-only
      * DataSources don't read from the path; Spark passes the producer DataFrame's schema here.
      */
    override def inferSchema(options: CaseInsensitiveStringMap): StructType =
        new StructType()

    /** Overrides TableProvider.getTable: returns a GeoJSONL_Table with the given schema + properties. */
    override def getTable(
        schema: StructType,
        partitions: Array[Transform],
        properties: java.util.Map[String, String]
    ): Table = new GeoJSONL_Table(schema, properties.asScala.toMap)

    /** Write-only DataSources should not be asked to infer the schema from the producer DataFrame. */
    override def supportsExternalMetadata(): Boolean = true

    /** Overrides DataSourceRegister.shortName: returns "geojsonl". */
    override def shortName(): String = "geojsonl"
}

object GeoJSONL_DataSource {

    /** Column roles derived from the write schema. Mirrors the light writer's `_writer_col_roles`. */
    final case class ColRoles(geomCol: String, sridCol: String, projCol: String, attrCols: Seq[String], geomIsWkb: Boolean)

    /**
      * Validate the write schema and resolve column roles.
      *
      * Convention (no overrides): the column `X` paired with `X_srid` is the geometry,
      * `X_srid_proj` is its PROJ4 fallback, and everything else is a feature attribute.
      *
      * With overrides: `geomColOpt` / `sridColOpt` name explicit geometry / SRID columns (both
      * required together when the frame does not follow the `*_srid` convention); `projColOpt`
      * names an explicit PROJ4 column (optional). An explicit override that names a column absent
      * from the schema raises a friendly error.
      *
      * The geometry column must be BINARY (WKB) or STRING (WKT). Raises a friendly error otherwise —
      * mirrors how the light `_writer_col_roles` locates the geometry.
      */
    def resolveRoles(
        schema: StructType,
        geomColOpt: Option[String] = None,
        sridColOpt: Option[String] = None,
        projColOpt: Option[String] = None
    ): ColRoles = {
        val names = schema.fieldNames.toSeq

        // geometry (required)
        val geomCol = geomColOpt match {
            case Some(g) =>
                if (!names.contains(g))
                    throw new IllegalArgumentException(
                        s"`geojsonl` writer geomCol='$g' is not a column; got ${names.mkString("[", ", ", "]")}.")
                g
            case None =>
                val sridCols = names.filter(_.endsWith("_srid"))
                if (sridCols.isEmpty)
                    throw new IllegalArgumentException(
                        "`geojsonl` writer input needs a geometry/'*_srid' column pair (from a *_ogr " +
                        s"reader) or an explicit geomCol option; got ${names.mkString("[", ", ", "]")}.")
                val g = sridCols.head.dropRight("_srid".length)
                if (!names.contains(g))
                    throw new IllegalArgumentException(
                        s"`geojsonl` writer found srid column '${sridCols.head}' but no geometry column '$g'.")
                g
        }

        // srid (required: option, else <geom>_srid)
        val sridCol = sridColOpt match {
            case Some(s) =>
                if (!names.contains(s))
                    throw new IllegalArgumentException(
                        s"`geojsonl` writer sridCol='$s' is not a column; got ${names.mkString("[", ", ", "]")}.")
                s
            case None =>
                val s = geomCol + "_srid"
                if (!names.contains(s))
                    throw new IllegalArgumentException(
                        s"`geojsonl` writer needs a SRID column: pass sridCol, or add a '$s' column " +
                        "(authority code, '0' if unknown).")
                s
        }

        // proj (optional: explicit must exist; default may be absent)
        val projCol = projColOpt match {
            case Some(p) =>
                if (!names.contains(p))
                    throw new IllegalArgumentException(
                        s"`geojsonl` writer projCol='$p' is not a column; got ${names.mkString("[", ", ", "]")}.")
                p
            case None => geomCol + "_srid_proj"
        }

        val attrCols = names.filterNot(n => n == geomCol || n == sridCol || n == projCol)
        val geomType = schema(geomCol).dataType
        val geomIsWkb = geomType match {
            case BinaryType => true
            case StringType => false
            case other =>
                throw new IllegalArgumentException(
                    s"`geojsonl` writer geometry column '$geomCol' must be BINARY (WKB) or STRING (WKT); got $other.")
        }
        ColRoles(geomCol, sridCol, projCol, attrCols, geomIsWkb)
    }
}
