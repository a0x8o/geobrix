package com.databricks.labs.gbx.pmtiles

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{BinaryType, IntegerType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.MapHasAsScala

/**
  * Spark Data Source V2 provider for the "pmtiles" format.
  *
  * Write only in v0.4.0:
  *   {{{
  *   df.write.format("pmtiles").save("/path/to/out.pmtiles")
  *   }}}
  *
  * Required write schema: `z INT, x INT, y INT, bytes BINARY` (exact match; see
  * `PMTiles_Table.newWriteBuilder` for the friendly schema-validation error).
  *
  * Read is not supported in this release — `spark.read.format("pmtiles").load(...)`
  * surfaces a clear `UnsupportedOperationException` rather than silently returning
  * an empty DataFrame.
  *
  * Use this DataSource for pyramids that exceed the in-memory ceiling of the
  * companion `gbx_pmtiles_agg` UDAF (~100 MiB of tile payload / 2 GiB Spark cell).
  */
//noinspection ScalaUnusedSymbol
class PMTiles_DataSource extends TableProvider with DataSourceRegister {

    /**
      * Overrides TableProvider.inferSchema: returns the canonical write schema. Spark calls this
      * during analysis even for write paths, so we provide the same `(z, x, y, bytes)` shape that
      * `PMTiles_Table` validates against at commit time.
      */
    override def inferSchema(options: CaseInsensitiveStringMap): StructType = PMTiles_DataSource.WRITE_SCHEMA

    /** Overrides TableProvider.getTable: returns a PMTiles_Table with the given schema and properties. */
    override def getTable(
        schema: StructType,
        partitions: Array[Transform],
        properties: java.util.Map[String, String]
    ): Table = new PMTiles_Table(schema, properties.asScala.toMap)

    /** Overrides DataSourceRegister.shortName: returns "pmtiles". */
    override def shortName(): String = "pmtiles"
}

object PMTiles_DataSource {

    /** Canonical write schema. Producer DataFrames must match this exactly. */
    val WRITE_SCHEMA: StructType = StructType(Array(
        StructField("z", IntegerType, nullable = false),
        StructField("x", IntegerType, nullable = false),
        StructField("y", IntegerType, nullable = false),
        StructField("bytes", BinaryType, nullable = true)
    ))

    /**
      * Validate that an incoming write schema matches the canonical (z, x, y, bytes) shape.
      *
      * Modelled on the `gdal_writer_schema.md` memory entry — mirrors the GDAL writer's exact-
      * schema policy so callers get the same kind of friendly error.
      */
    def validateWriteSchema(schema: StructType): Unit = {
        val required = WRITE_SCHEMA.fields.map(f => f.name -> f.dataType).toMap
        val actual = schema.fields.map(f => f.name -> f.dataType).toMap

        val missing = required.keys.filterNot(actual.contains).toSeq
        val extra = actual.keys.filterNot(required.contains).toSeq

        if (missing.nonEmpty || extra.nonEmpty) {
            throw new IllegalArgumentException(
                s"`pmtiles` DataSource requires schema exactly (z INT, x INT, y INT, bytes BINARY). " +
                s"Missing columns: ${missing.mkString("[", ", ", "]")}; " +
                s"unexpected columns: ${extra.mkString("[", ", ", "]")}. " +
                s"Got schema: ${schema.simpleString}."
            )
        }

        for ((name, expectedType) <- required) {
            val actualType = actual(name)
            if (actualType != expectedType) {
                throw new IllegalArgumentException(
                    s"`pmtiles` DataSource column `$name` must be $expectedType; got $actualType."
                )
            }
        }
    }
}
