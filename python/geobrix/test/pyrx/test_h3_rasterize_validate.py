"""CI validation: round-trip and partition property tests for rst_h3_rasterize_agg.

Round-trip test:
    DEM -> gbx_rst_h3_rastertogridavg (light UDTF, LATERAL join, band 1) ->
    (cellid, measure) dict -> cells_to_raster on kring_pad=0 grid ->
    covered pixel values match per-cell measures within tolerance.

Partition test:
    Synthetic polygon -> polyfill cells -> rst_h3_rasterize_agg ->
    every burned pixel centroid re-indexes to a cell in the set and every
    NoData pixel centroid does not.

Both tests use no external mocking; real h3, rasterio, numpy assertions only.
"""

import os

import h3
import numpy as np

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx import functions as rx
from databricks.labs.gbx.pyrx.core import cellraster as cr

DEM = os.path.join(
    os.environ.get(
        "GBX_SAMPLE_DATA_ROOT",
        os.path.join(
            os.path.dirname(__file__),
            "../../../../sample-data/Volumes/main/default/geobrix_samples/geobrix-examples",
        ),
    ),
    "nyc/elevation/srtm_n40w073.tif",
)


def test_roundtrip_rastertogrid_then_rasterize(spark):
    """DEM -> rastertogridavg (band 1) -> rasterize back; covered pixels match measures."""
    res = 7
    if not os.path.exists(DEM):
        import pytest

        pytest.skip(f"sample DEM not found: {DEM}")

    with open(DEM, "rb") as fh:
        content = fh.read()

    rx.register(spark)

    # Load raster tile via SQL UDF.
    df = spark.createDataFrame([(content,)], ["raster"]).selectExpr(
        "gbx_rst_fromcontent(raster, 'GTiff') AS tile"
    )
    df.createOrReplaceTempView("_t5_dem")

    # gbx_rst_h3_rastertogridavg is a UDTF that yields flat (band, cellID, measure) rows.
    # Filter to band 1 only.
    cells_df = spark.sql(
        "SELECT t.cellID AS cellid, t.measure AS measure "
        "FROM _t5_dem, LATERAL gbx_rst_h3_rastertogridavg(tile, %d) t "
        "WHERE t.band = 1" % res
    )
    cellrows = cells_df.collect()
    assert len(cellrows) > 0, "rastertogridavg returned no cells for band 1"

    cv = {int(r["cellid"]): float(r["measure"]) for r in cellrows}

    # Rasterize back onto a tight grid (no kring padding so the grid is minimal).
    g = cr.compute_gridspec(list(cv.keys()), kring_pad=0)
    raster = cr.cells_to_raster(cv, *g, resolution=res)

    with _serde.open_tile(raster) as ds:
        arr = ds.read(1)
        covered = arr[arr != ds.nodata]
        assert covered.size > 0, "no covered pixels in round-trip raster"

        measures = np.array(sorted(cv.values()))
        # Every burned value should match the nearest cell measure within float tolerance.
        # searchsorted finds the insertion point; we clip to valid range and check closeness.
        nearest_idx = np.searchsorted(measures, covered).clip(0, len(measures) - 1)
        close_frac = np.isclose(covered, measures[nearest_idx], atol=1e-6).mean()
        assert close_frac > 0.99, (
            f"only {close_frac:.1%} of covered pixels match a cell measure "
            "(expected >99%)"
        )


def test_partition_property_via_agg(spark):
    """Every burned pixel centroid maps to a polyfill cell; NoData pixels do not."""
    res = 9
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.03), (0.03, 0.03), (0.03, 0.0)])
    cells = [h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)]
    assert len(cells) > 0, "polyfill returned no cells"

    df = spark.createDataFrame([(int(c), "TX1") for c in cells], ["cellid", "tx"])
    tile = (
        df.groupBy("tx")
        .agg(rx.rst_h3_rasterize_agg("cellid").alias("t"))
        .collect()[0]["t"]
    )
    assert tile is not None and tile["raster"] is not None

    cellset = {cr._h3_str(c) for c in cells}

    with _serde.open_tile(bytes(tile["raster"])) as ds:
        arr = ds.read(1)
        t = ds.transform
        for row in range(ds.height):
            for col in range(ds.width):
                # Pixel centroid in geographic space.
                lon, lat = t * (col + 0.5, row + 0.5)
                pixel_cell = h3.latlng_to_cell(lat, lon, res)
                burned = arr[row, col] != ds.nodata
                in_set = pixel_cell in cellset
                assert burned == in_set, (
                    f"pixel ({row},{col}) lat={lat:.6f} lon={lon:.6f}: "
                    f"burned={burned} but cell {pixel_cell} in_cellset={in_set}"
                )
