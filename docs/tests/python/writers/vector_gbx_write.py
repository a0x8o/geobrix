import os

from pyspark.sql import SparkSession

from databricks.labs.gbx.ds.register import register


def write_vector_gbx_example():
    spark = SparkSession.builder.getOrCreate()
    register(spark)
    root = os.environ.get("GBX_SAMPLE_DATA_ROOT", "/Volumes/main/default/test-data")
    src = f"{root}/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson"
    out = "/tmp/boroughs_out.geojson"

    df = spark.read.format("geojson_gbx").load(src)
    df.coalesce(1).write.format("geojson_gbx").mode("overwrite").save(out)

    back = spark.read.format("geojson_gbx").load(out)
    assert back.count() == df.count()
    return out
