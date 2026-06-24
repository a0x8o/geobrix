import csv
import os

import h3

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx import functions as rx

CSV = os.path.join(os.path.dirname(__file__), "data/fcc_uflw_miamidade_subset.csv")


def test_fcc_rasterize_per_speed_tier(spark):
    rows = list(csv.DictReader(open(CSV)))
    data = [
        (
            h3.str_to_int(r["h3_res8_id"]),
            int(r["max_advertised_download_speed"]),
            r["provider_id"],
        )
        for r in rows
    ]
    df = spark.createDataFrame(data, ["cellid", "speed", "provider"])
    # one raster per (provider, speed tier); res-8 cells, presence mask
    tiles = (
        df.groupBy("provider", "speed")
        .agg(rx.rst_h3_rasterize_agg("cellid").alias("tile"))
        .collect()
    )
    assert len(tiles) >= 1
    for t in tiles:
        with _serde.open_tile(bytes(t["tile"]["raster"])) as ds:
            assert (ds.read(1) == 1.0).sum() >= 1  # cells burned
