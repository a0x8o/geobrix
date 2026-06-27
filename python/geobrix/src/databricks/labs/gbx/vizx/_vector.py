"""Spark DataFrame -> GeoDataFrame adapters for gbx.vizx interactive maps.

Collect to the driver (single-node viz); guarded by max_rows so a large frame
does not OOM the driver. Boundaries for H3 cells use the h3 lib (portable), not
the Databricks-native h3_boundaryaswkt.
"""

import warnings

# 1.3x headroom on the sample fraction: pyspark .sample is Bernoulli
# (approximate-N), so over-sampling then .limit(max_rows) reliably fills up to
# max_rows rows while staying reproducible by seed.
_SAMPLE_HEADROOM = 1.3


def _collect_capped(df, max_rows, sample_seed, label):
    """Collect a Spark DataFrame to pandas with a max_rows cap.

    ``sample_seed=None`` -> first ``max_rows`` rows via ``.limit`` (current
    behaviour; deterministic, cheapest). ``sample_seed=<int>`` -> a reproducible
    Bernoulli sample via ``pyspark.sql.DataFrame.sample`` (seeded; one extra
    ``count()`` job) capped at ``max_rows``. Emits a truncate warning labelled
    ``label`` when the cap fires. ``max_rows=None`` collects everything.
    """
    if max_rows is None:
        return df.toPandas()

    if sample_seed is not None:
        total = df.count()
        frac = min(1.0, (max_rows * _SAMPLE_HEADROOM) / total) if total else 1.0
        sampled = df.sample(
            withReplacement=False, fraction=frac, seed=sample_seed
        ).limit(max_rows)
        pdf = sampled.toPandas()
        if total > len(pdf):
            warnings.warn(
                f"{label}: output sampled to max_rows={max_rows} (seed="
                f"{sample_seed}) for driver-side viz; pass max_rows=None to "
                "collect all rows.",
                stacklevel=3,
            )
        return pdf

    pdf = df.limit(max_rows + 1).toPandas()
    if len(pdf) > max_rows:
        pdf = pdf.iloc[:max_rows]
        warnings.warn(
            f"{label}: output truncated to max_rows={max_rows} for driver-side "
            "viz; pass max_rows=None to collect all rows.",
            stacklevel=3,
        )
    return pdf


def as_gdf(df, wkt_col="wkt", *, max_rows=10_000, sample_seed=None):
    """Spark DataFrame with a WKT column -> geopandas.GeoDataFrame (EPSG:4326).

    Collects to the driver. With max_rows set (default 10_000) the frame is
    truncated to max_rows and a warning is emitted; pass max_rows=None to opt out.

    ``sample_seed`` (Spark-only; ignored for an in-memory input): ``None``
    (default) takes the first ``max_rows`` rows via ``.limit`` (deterministic,
    partition-order arbitrary); an int draws a reproducible seeded sample via
    ``pyspark.sql.DataFrame.sample`` (same seed -> same rows) at the cost of one
    extra ``count()`` job.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()
    import geopandas as gpd

    if wkt_col not in df.columns:
        raise ValueError(
            f"as_gdf: column {wkt_col!r} not in DataFrame columns {df.columns}"
        )
    pdf = _collect_capped(df, max_rows, sample_seed, "as_gdf")
    geometry = gpd.GeoSeries.from_wkt(pdf[wkt_col], crs=4326)
    pdf = pdf.drop(columns=[wkt_col])
    pdf["geometry"] = geometry.values
    return gpd.GeoDataFrame(pdf, geometry="geometry", crs=4326)


def grid_as_gdf(grid, srid=None):
    """Grid spec (from rst_h3_gridspec) -> 1-row GeoDataFrame (EPSG:4326).

    ``grid`` is a Spark Row or dict with fields ``xmin, ymin, xmax, ymax`` and
    optionally ``srid``, ``pixel_size``, ``width``, ``height`` (the struct that
    ``rst_h3_gridspec`` returns in its ``grid`` field).

    ``srid`` overrides the grid's own ``srid`` field; if both are absent, 4326
    is assumed. When the source CRS is not 4326 the bounding box is reprojected
    via ``pyproj`` before building the GeoDataFrame.

    Optional metadata columns ``pixel_size``, ``width``, and ``height`` are
    carried through if present on the input.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()

    import geopandas as gpd
    from shapely.geometry import box

    # Resolve SRID: explicit arg > grid field > default 4326
    if srid is None:
        try:
            srid = grid["srid"]
        except (KeyError, TypeError):
            srid = 4326
    if srid is None:
        srid = 4326

    xmin = grid["xmin"]
    ymin = grid["ymin"]
    xmax = grid["xmax"]
    ymax = grid["ymax"]

    geom = box(xmin, ymin, xmax, ymax)

    if int(srid) != 4326:
        from shapely.ops import transform

        try:
            import pyproj
        except ImportError as exc:
            raise ImportError(
                "grid_as_gdf: pyproj is required for CRS reprojection. "
                "Install with: pip install pyproj"
            ) from exc
        transformer = pyproj.Transformer.from_crs(int(srid), 4326, always_xy=True)
        geom = transform(transformer.transform, geom)

    row = {"geometry": geom}
    for key in ("pixel_size", "width", "height"):
        try:
            val = grid[key]
            row[key] = val
        except Exception:  # noqa: BLE001 — KeyError/PySparkValueError/TypeError
            pass

    return gpd.GeoDataFrame([row], geometry="geometry", crs=4326)


def cells_as_gdf(
    df,
    cell_col="cellid",
    extra_cols=(),
    *,
    max_rows=10_000,
    dissolve_by=None,
    sample_seed=None,
):
    """H3 cell ids (bigint) -> boundary polygons as a GeoDataFrame (EPSG:4326).

    Boundaries come from the h3 lib (h3 v4 takes a string index, so each bigint
    cellid is converted via h3.int_to_str). extra_cols are carried through.

    ``dissolve_by`` must be one of ``extra_cols`` when set. When provided the
    returned GeoDataFrame contains one dissolved polygon per distinct value of
    that column (the union footprint) rather than one row per cell. Raises
    ``ValueError`` if ``dissolve_by`` is set but not in ``extra_cols``.

    ``sample_seed`` (Spark-only): ``None`` (default) takes the first ``max_rows``
    cells via ``.limit``; an int draws a reproducible seeded sample via
    ``pyspark.sql.DataFrame.sample`` (same seed -> same cells) at the cost of one
    extra ``count()`` job.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()

    if dissolve_by is not None and dissolve_by not in extra_cols:
        raise ValueError(
            f"cells_as_gdf: dissolve_by={dissolve_by!r} is not in "
            f"extra_cols={list(extra_cols)!r}; add it to extra_cols first."
        )

    import h3
    from shapely.geometry import Polygon

    cols = [cell_col, *extra_cols]
    pdf = _collect_capped(df.select(*cols), max_rows, sample_seed, "cells_as_gdf")

    def _boundary(cell_int):
        ring = h3.cell_to_boundary(h3.int_to_str(int(cell_int)))
        # h3 v4 returns (lat, lng) pairs; shapely wants (lng, lat).
        return Polygon([(lng, lat) for lat, lng in ring])

    import geopandas as gpd

    geometry = [_boundary(c) for c in pdf[cell_col]]
    gdf = gpd.GeoDataFrame(pdf, geometry=geometry, crs=4326)

    if dissolve_by is not None:
        gdf = gdf.dissolve(by=dissolve_by).reset_index()

    return gdf
