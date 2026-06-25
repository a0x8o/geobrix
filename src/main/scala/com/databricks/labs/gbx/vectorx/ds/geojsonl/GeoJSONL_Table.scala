package com.databricks.labs.gbx.vectorx.ds.geojsonl

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters._

/**
  * Spark Connector Table for the `geojsonl` DataSource. Capability: `BATCH_WRITE` only.
  *
  * The write builder validates the (geometry/`*_srid` pair + attrs) schema and requires
  * `.mode("overwrite")` — append has no meaning for a directory of immutable shards in v0.4.0.
  */
class GeoJSONL_Table(schema: StructType, properties: Map[String, String])
    extends Table with SupportsWrite {

    /** Overrides Table.name: returns "geojsonl". */
    override def name(): String = "geojsonl"

    /** Overrides Table.schema: returns the producer DataFrame's schema. */
    // noinspection ScalaDeprecation
    override def schema(): StructType = schema

    /** Overrides Table.columns: one Column per schema field. */
    override def columns(): Array[Column] =
        schema.fields.map(f => Column.create(f.name, f.dataType, f.nullable))

    /** Build a write that consumes feature rows and writes a directory of GeoJSONL shards. */
    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
        // Validate the schema up front so a bad shape fails fast on the driver.
        val o = info.options()
        GeoJSONL_DataSource.resolveRoles(
            info.schema(),
            Option(o.get("geomCol")),
            Option(o.get("sridCol")),
            Option(o.get("projCol"))
        )
        new GeoJSONL_WriteBuilder(info.schema(), properties ++ info.options().asScala)
    }

    /**
      * Overrides Table.capabilities:
      *   - BATCH_WRITE so the canonical `.save(path)` path is wired up.
      *   - TRUNCATE so `.mode("overwrite")` maps to a truncate write — Spark turns an overwrite of a
      *     V2 relation into a TRUNCATE operation, which requires this capability; the WriteBuilder's
      *     `SupportsTruncate.truncate()` then records the overwrite (append, which never calls
      *     truncate, is rejected in `build()`).
      */
    override def capabilities(): java.util.Set[TableCapability] = Set(
        TableCapability.BATCH_WRITE,
        TableCapability.TRUNCATE
    ).asJava
}
