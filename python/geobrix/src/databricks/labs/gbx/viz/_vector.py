"""Spark DataFrame -> GeoDataFrame adapters for gbx.viz interactive maps.

Collect to the driver (single-node viz); guarded by max_rows so a large frame
does not OOM the driver. Boundaries for H3 cells use the h3 lib (portable), not
the Databricks-native h3_boundaryaswkt.
"""

import warnings


def as_gdf(df, wkt_col="wkt", *, max_rows=10_000):
    """Spark DataFrame with a WKT column -> geopandas.GeoDataFrame (EPSG:4326).

    Collects to the driver. With max_rows set (default 10_000) the frame is
    truncated to max_rows and a warning is emitted; pass max_rows=None to opt out.
    """
    from databricks.labs.gbx.viz._env import assert_viz_available

    assert_viz_available()
    import geopandas as gpd

    if wkt_col not in df.columns:
        raise ValueError(
            f"as_gdf: column {wkt_col!r} not in DataFrame columns {df.columns}"
        )
    if max_rows is None:
        pdf = df.toPandas()
    else:
        pdf = df.limit(max_rows + 1).toPandas()
        if len(pdf) > max_rows:
            pdf = pdf.iloc[:max_rows]
            warnings.warn(
                f"as_gdf: output truncated to max_rows={max_rows} for driver-side "
                "viz; pass max_rows=None to collect all rows.",
                stacklevel=2,
            )
    geometry = gpd.GeoSeries.from_wkt(pdf[wkt_col], crs=4326)
    pdf = pdf.drop(columns=[wkt_col])
    pdf["geometry"] = geometry.values
    return gpd.GeoDataFrame(pdf, geometry="geometry", crs=4326)


def cells_as_gdf(df, cell_col="cellid", extra_cols=(), *, max_rows=10_000):
    """H3 cell ids (bigint) -> boundary polygons as a GeoDataFrame (EPSG:4326).

    Boundaries come from the h3 lib (h3 v4 takes a string index, so each bigint
    cellid is converted via h3.int_to_str). extra_cols are carried through.
    """
    from databricks.labs.gbx.viz._env import assert_viz_available

    assert_viz_available()
    import h3
    from shapely.geometry import Polygon

    cols = [cell_col, *extra_cols]
    if max_rows is None:
        pdf = df.select(*cols).toPandas()
    else:
        pdf = df.select(*cols).limit(max_rows + 1).toPandas()
        if len(pdf) > max_rows:
            pdf = pdf.iloc[:max_rows]
            warnings.warn(
                f"cells_as_gdf: output truncated to max_rows={max_rows} for "
                "driver-side viz; pass max_rows=None to collect all rows.",
                stacklevel=2,
            )

    def _boundary(cell_int):
        ring = h3.cell_to_boundary(h3.int_to_str(int(cell_int)))
        # h3 v4 returns (lat, lng) pairs; shapely wants (lng, lat).
        return Polygon([(lng, lat) for lat, lng in ring])

    import geopandas as gpd

    geometry = [_boundary(c) for c in pdf[cell_col]]
    return gpd.GeoDataFrame(pdf, geometry=geometry, crs=4326)
