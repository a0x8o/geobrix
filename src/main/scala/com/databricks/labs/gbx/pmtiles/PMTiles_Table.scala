package com.databricks.labs.gbx.pmtiles

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/**
  * Spark Connector Table for the `pmtiles` DataSource.
  *
  * Capabilities: `BATCH_WRITE` only in v0.4.0. The trait also declares `SupportsRead` so that
  * `spark.read.format("pmtiles").schema` can be inspected (a common discovery flow), but
  * `newScanBuilder` raises a clear `UnsupportedOperationException` because the on-disk decoder
  * is not yet implemented.
  */
class PMTiles_Table(schema: StructType, properties: Map[String, String])
    extends Table with SupportsRead with SupportsWrite {

    /** Overrides Table.name: returns "pmtiles". */
    override def name(): String = "pmtiles"

    /** Overrides Table.schema: returns the canonical write schema for the DataSource. */
    // noinspection ScalaDeprecation
    override def schema(): StructType = schema

    /** Overrides Table.columns: one Column per schema field. */
    override def columns(): Array[Column] =
        schema.fields.map(f => Column.create(f.name, f.dataType, f.nullable))

    /**
      * Reads are not supported in this release — surface a clear error rather than silently
      * returning an empty DataFrame. The DataSourceRegister entry is needed for write-path
      * shortName resolution, but the read path will land here only if the user actually tries
      * `spark.read.format("pmtiles").load(...)`.
      */
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        throw new UnsupportedOperationException(
            "Reading PMTiles archives is not supported in GeoBrix 0.4.0. " +
            "The `pmtiles` DataSource is write-only — use " +
            "`df.write.format(\"pmtiles\").save(path)` to encode tile pyramids, and serve " +
            "the resulting `.pmtiles` file via MapLibre / pmtiles.io / Felt for visualization."
        )
    }

    /** Build a write that consumes (z, x, y, bytes) rows and writes a single PMTile file. */
    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
        PMTiles_DataSource.validateWriteSchema(info.schema())
        new PMTiles_WriteBuilder(info.schema(), properties ++ info.options().asScala)
    }

    /**
      * Overrides Table.capabilities:
      *   - BATCH_WRITE so the canonical `.save(path)` path is wired up.
      *   - TRUNCATE so `.save(path)` without an explicit `.mode(...)` works (PMTile container
      *     is a single binary file; "append" has no meaning).
      *   - BATCH_READ so the read code path lands in `newScanBuilder` where we can throw a
      *     descriptive "not yet supported" error rather than letting Spark surface a vague
      *     "not a valid Spark SQL Data Source" upstream.
      */
    override def capabilities(): java.util.Set[TableCapability] = Set(
        TableCapability.BATCH_READ,
        TableCapability.BATCH_WRITE,
        TableCapability.TRUNCATE
    ).asJava
}
