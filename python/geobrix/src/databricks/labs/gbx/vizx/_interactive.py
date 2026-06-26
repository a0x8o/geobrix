"""Interactive (folium) map rendering for gbx.vizx.

plot_interactive is the interactive twin of plot_static. It is scale-safe
(geopandas .explore() hangs on millions of vertices, so over a threshold it
rasterizes to a flat image overlay) and Databricks-safe (folium does not
auto-render in Databricks; it must go through displayHTML). Requires the
[vizx] extra (plus folium / rasterio for the interactive path).
"""

import shapely

_MODES = ("auto", "detailed", "fast")
_SILENCE = " (set debug_level=0 to silence)"


def _log(debug_level, level, message):
    """Emit `message` if debug_level >= level. Level-1 lines get the silence hint."""
    if debug_level >= level:
        if level == 1:
            print(message + _SILENCE)
        else:
            print(message)


def _raster_overlay(gdf, column=None, opacity=0.65, max_px=1400):
    """Rasterize polygons to a viridis PNG laid over a folium ImageOverlay.

    Numeric column -> value; categorical/object column -> integer codes; no
    column -> 1..n. NoData (gaps) is transparent. Complete (every polygon is
    burned, nothing dropped) and scales to millions of vertices, but it is a
    flat image with no per-feature hover. Returns a folium.Map.
    """
    import base64
    import io

    import folium
    import matplotlib.cm as cm
    import matplotlib.colors as mcolors
    import numpy as np
    from matplotlib.image import imsave
    from rasterio.features import rasterize
    from rasterio.transform import from_bounds

    g = gdf.to_crs(4326)
    minx, miny, maxx, maxy = map(float, g.total_bounds)
    w = max_px
    h = max(1, int(max_px * (maxy - miny) / max(maxx - minx, 1e-9)))
    transform = from_bounds(minx, miny, maxx, maxy, w, h)

    if column is not None and column in g.columns:
        col = g[column]
        if col.dtype == object or str(col.dtype).startswith("category"):
            cats = {v: i + 1 for i, v in enumerate(sorted(col.dropna().unique()))}
            vals = [cats.get(v, 0) for v in col]
        else:
            vals = col.astype(float).tolist()
    else:
        vals = list(range(1, len(g) + 1))

    order = np.argsort(vals)
    shapes = [
        (g.geometry.iloc[i], float(vals[i]))
        for i in order
        if g.geometry.iloc[i] is not None
    ]
    arr = rasterize(
        shapes,
        out_shape=(h, w),
        transform=transform,
        fill=np.nan,
        dtype="float64",
        all_touched=True,
    )
    masked = np.ma.masked_invalid(arr)
    vmin, vmax = float(np.nanmin(arr)), float(np.nanmax(arr))
    norm = mcolors.Normalize(vmin=vmin, vmax=(vmax if vmax > vmin else vmin + 1))
    rgba = cm.viridis(norm(masked))
    rgba[..., 3] = np.where(masked.mask, 0.0, opacity)

    buf = io.BytesIO()
    imsave(buf, rgba, format="png")
    buf.seek(0)
    png = "data:image/png;base64," + base64.b64encode(buf.read()).decode()

    m = folium.Map(
        location=[(miny + maxy) / 2, (minx + maxx) / 2],
        zoom_start=10,
        tiles="CartoDB positron",
    )
    folium.raster_layers.ImageOverlay(
        image=png, bounds=[[miny, minx], [maxy, maxx]], opacity=1.0
    ).add_to(m)
    m.fit_bounds([[miny, minx], [maxy, maxx]])
    return m


def _count_vertices(gdf):
    return int(shapely.get_num_coordinates(gdf.geometry.values).sum())


def _notebook_display_html():
    """Return Databricks' notebook ``displayHTML`` callable, or None.

    ``displayHTML`` is injected by Databricks into the *notebook's* user
    namespace, not into library module globals — so a bare ``displayHTML(...)``
    from inside this module raises ``NameError``. Reach it via the IPython
    user namespace instead. Returns None when unavailable (plain Jupyter, no
    IPython, etc.).
    """
    try:
        from IPython import get_ipython

        ip = get_ipython()
        return ip.user_ns.get("displayHTML") if ip is not None else None
    except Exception:  # noqa: BLE001 — IPython absent or misbehaving
        return None


def _render(m):
    """Render a folium map via the best-available display channel.

    Priority order, each wrapped so a failure falls through to the next:

    1. Databricks notebook ``displayHTML`` (fetched from the IPython user
       namespace) — side-effect render, returns None.
    2. IPython rich display (``IPython.display.display(HTML(...))``) — works
       in IPython-based notebooks (Databricks/Jupyter), returns None.
    3. Plain return of the folium map (classic auto-render) — last resort.
    """
    try:
        html = m._repr_html_()
    except Exception:  # noqa: BLE001 — fall back to the raw root render
        html = m.get_root().render()

    dh = _notebook_display_html()
    if dh is not None:
        try:
            dh(html)
            return None
        except Exception:  # noqa: BLE001 — fall through to IPython display
            pass

    try:
        from IPython.display import HTML, display

        display(HTML(html))
        return None
    except Exception:  # noqa: BLE001 — fall through to plain return
        pass

    return m


def plot_interactive(
    data,
    *,
    column=None,
    geom_col=None,
    grid_system=None,
    grid_conf=None,
    max_rows=10_000,
    sample_seed=None,
    srid=None,
    mode="auto",
    max_vertices=60_000,
    max_px=1400,
    opacity=0.65,
    debug_level=1,
    **explore_kw,
):
    """Render geometries / DGGS cells as an interactive folium map.

    ``data`` is a Spark DataFrame or a geopandas.GeoDataFrame (converted the
    same way ``plot_static`` does: ``grid_system`` set -> cell ids decoded to
    boundaries; else a geometry column via ``geom_col`` / auto-detect).

    Modes (intent-oriented):

    - ``"auto"`` (default): ``detailed`` if total vertex count
      ``<= max_vertices``, else ``fast``. Announces the chosen path at
      ``debug_level >= 1``.
    - ``"detailed"``: geopandas ``.explore()`` -- full vector with per-feature
      hover. Over ``max_vertices`` it warns (at ``debug_level >= 1``) that it
      may be slow and proceeds.
    - ``"fast"``: a rasterized image overlay (``_raster_overlay``). Complete and
      scales to millions of vertices, but a flat image (no per-feature hover).

    ``max_vertices`` (auto crossover + detailed-slow trigger) and ``max_px``
    (overlay raster resolution) are tunable; defaults are conservative
    Serverless values. ``debug_level`` is 0 (silent) / 1 (key decisions +
    warnings) / 2+ (verbose internals).

    ``sample_seed`` (Spark-only) governs how the ``max_rows`` collection cap is
    filled: ``None`` (default) takes the first ``max_rows`` rows; an int draws a
    reproducible seeded sample. When the cap fires on the ``fast`` path the map
    is a *sampled* image, not complete coverage, so (at ``debug_level >= 1``) it
    advises pre-aggregating (``st_union_agg`` / ``rst_h3_rasterize_agg``) so the
    gdf stays few-rows-many-vertices and the cap never fires.

    The map renders as the function's last statement: in Databricks via
    ``displayHTML`` (reached through the IPython user namespace; returns None),
    in other IPython notebooks via ``IPython.display`` (returns None), and as a
    last resort by returning the folium map (which auto-renders). Extra keyword
    arguments are passed to ``.explore()``. Requires the [vizx] extra plus
    folium / rasterio.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()

    if mode not in _MODES:
        raise ValueError(
            f"plot_interactive: mode={mode!r} is not one of {list(_MODES)}."
        )

    from databricks.labs.gbx.vizx._static_map import _resolve_gdf

    gdf = _resolve_gdf(
        data, geom_col, grid_system, max_rows, srid, grid_conf, sample_seed
    )

    nverts = _count_vertices(gdf)
    _log(debug_level, 2, f"plot_interactive: {nverts:,} vertices, mode={mode!r}")

    if mode == "auto":
        if nverts <= max_vertices:
            resolved = "detailed"
            _log(
                debug_level,
                1,
                f"auto -> detailed (.explore): {nverts:,} vertices "
                f"<= max_vertices={max_vertices:,}; per-feature hover available.",
            )
        else:
            resolved = "fast"
            _log(
                debug_level,
                1,
                f"auto -> fast (image overlay): {nverts:,} vertices > "
                f"max_vertices={max_vertices:,}; per-feature hover unavailable "
                "at this scale.",
            )
    else:
        resolved = mode

    if resolved == "fast":
        # len(gdf) >= max_rows is the signal the row cap likely fired during the
        # Spark->gdf collection: a "complete-coverage" raster built from a capped
        # (sampled or first-N) gdf is a contradiction, so steer the caller to
        # pre-aggregate (few rows, many vertices) and the cap never fires.
        if max_rows is not None and len(gdf) >= max_rows:
            _log(
                debug_level,
                1,
                f"fast: showing {max_rows:,} of (>= max_rows) geometries — "
                "pre-aggregate (st_union_agg / rst_h3_rasterize_agg) for "
                "complete coverage.",
            )
        _log(debug_level, 2, f"fast: rasterizing at max_px={max_px}")
        m = _raster_overlay(gdf, column, opacity, max_px)
    else:
        if nverts > max_vertices:
            _log(
                debug_level,
                1,
                f"detailed mode: {nverts:,} vertices > {max_vertices:,} "
                "— may be slow to render.",
            )
        kw = dict(explore_kw)
        if column is not None:
            kw["column"] = column
        m = gdf.explore(**kw)

    return _render(m)
