package com.databricks.labs.gbx.vectorx.ds.ogr

import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/**
  * Spark Connector Table for OGR: batch read only.
  *
  * Implements SupportsWrite with BATCH_WRITE capability so that Spark's write path
  * reaches newWriteBuilder rather than bypassing getTable entirely. newWriteBuilder
  * immediately throws UnsupportedOperationException with an actionable message naming
  * the per-format _gbx alternative — the write is rejected before any data moves.
  *
  * The write guard fires here (not in inferSchema) because OGR_DataSource.supportsExternalMetadata
  * returns true: on writes, Spark skips inferSchema and calls getTable(dfSchema, ...) directly,
  * then calls newWriteBuilder. On reads, inferSchema is always called (Spark passes None as schema)
  * so the read path is completely unaffected.
  */
class OGR_Table(schema: StructType, properties: Map[String, String], writeGuardMsg: String)
    extends Table with SupportsRead with SupportsWrite {

    /** Overrides Table.name: returns "ogr". */
    override def name(): String = "ogr"

    /** Overrides Table.schema: returns the inferred read schema. */
    // noinspection ScalaDeprecation
    override def schema(): StructType = schema

    /** Overrides Table.columns: one Column per schema field. */
    override def columns(): Array[Column] = schema.fields.map(f => Column.create(f.name, f.dataType, f.nullable))

    /** Overrides SupportsRead.newScanBuilder: builds scan that produces feature rows via OGR_Batch. */
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = { () =>
        new OGR_Batch(schema, properties ++ options.asScala)
    }

    /**
      * Overrides SupportsWrite.newWriteBuilder: immediately rejects the write.
      *
      * This fires only on the write path — Spark calls this after getTable when the user invokes
      * .write.format("*_ogr").save(...). Throws UnsupportedOperationException with the per-format
      * message naming the recommended _gbx alternative writer.
      */
    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
        throw new UnsupportedOperationException(writeGuardMsg)

    /**
      * Overrides Table.capabilities: BATCH_READ, BATCH_WRITE, and TRUNCATE.
      *
      * BATCH_WRITE is declared so Spark routes .save() through newWriteBuilder (where the guard
      * fires) rather than falling through to a V1 write path.
      * TRUNCATE is declared so that `.mode("overwrite")` also reaches newWriteBuilder: without it,
      * Spark raises UNSUPPORTED_FEATURE.TABLE_OPERATION ("does not support truncate") before calling
      * newWriteBuilder. With it, Spark calls newWriteBuilder and then truncate() on the result —
      * but since newWriteBuilder throws immediately, the truncate never executes.
      */
    override def capabilities(): java.util.Set[TableCapability] =
        Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.TRUNCATE).asJava

}
