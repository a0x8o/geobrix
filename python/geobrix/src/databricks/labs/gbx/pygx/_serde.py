from pyspark.sql.types import BinaryType, LongType, StructField, StructType

QUADBIN_CELL_SCHEMA = StructType(
    [
        StructField("cell", LongType(), False),
        StructField("geom", BinaryType(), True),
    ]
)
