from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

QUADBIN_CELL_SCHEMA = StructType(
    [
        StructField("cell", LongType(), False),
        StructField("geom", BinaryType(), True),
    ]
)

# BNG chip struct: cellid is STRING (public surface), chip is plain WKB (no SRID).
# Fields are nullable so the struct can be produced as a pandas_udf return type
# (the Arrow-inferred schema marks all fields nullable) and so a dissolved/empty
# chip can carry NULL.
BNG_CHIP_SCHEMA = StructType(
    [
        StructField("cellid", StringType(), True),
        StructField("core", BooleanType(), True),
        StructField("chip", BinaryType(), True),
    ]
)
