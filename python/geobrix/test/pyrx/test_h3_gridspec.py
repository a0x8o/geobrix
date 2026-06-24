import h3

from databricks.labs.gbx.pyrx import functions as rx


def test_rst_h3_gridspec_matches_core(spark):
    res = 9
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    cells = [h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)]
    df = spark.createDataFrame([(int(c), "TX1") for c in cells], ["cellid", "tx"])
    out = rx.rst_h3_gridspec(df, "cellid", "tx", pixel_size=0.005).collect()
    assert len(out) == 1
    g = out[0]["grid"]
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    exp = cr.compute_gridspec(cells, pixel_size=0.005)
    assert g["width"] == exp[5] and g["height"] == exp[6]
    assert abs(g["xmin"] - exp[0]) < 1e-9
