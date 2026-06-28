"""Cloud-Optimized GeoTIFF viewer for gbx.vizx.

Reads a COG decimated (an overview-equivalent read) and renders it over a
contextily basemap as a static matplotlib figure. Driver-side; requires the
[vizx] extra plus rasterio.
"""

from __future__ import annotations

import warnings


def _strip_scheme(path: str) -> str:
    for scheme in ("dbfs:", "file:"):
        if path.startswith(scheme):
            path = path[len(scheme) :]
            break
    if path.startswith("//"):
        path = "/" + path.lstrip("/")
    return path


def _render_cog(data, transform, *, crs, fig_w, fig_h, title, basemap, basemap_source):
    """Render a decimated COG array (bands, h, w) over a contextily basemap."""
    import matplotlib.pyplot as plt
    from rasterio.plot import plotting_extent, show

    from databricks.labs.gbx.vizx._raster import (
        _needs_percentile_stretch,
        _percentile_stretch,
    )

    if _needs_percentile_stretch(data):
        data = _percentile_stretch(data)
    _, ax = plt.subplots(1, figsize=(fig_w, fig_h))
    if data.shape[0] == 1:
        band = data[0]
        ax.imshow(band, extent=plotting_extent(band, transform), cmap="viridis")
    else:
        show(data, ax=ax, transform=transform)
    if basemap and crs is not None:
        try:
            import contextily as cx

            source = basemap_source or cx.providers.CartoDB.Positron
            cx.add_basemap(ax, source=source, crs=crs)
        except Exception as exc:  # noqa: BLE001 — offline/no-egress -> warn + skip
            warnings.warn(
                f"plot_cog: basemap unavailable ({type(exc).__name__}: {exc}); "
                "rendering without basemap.",
                stacklevel=2,
            )
    if title:
        ax.set_title(title)
    ax.set_axis_off()


def plot_cog(
    path,
    *,
    band=None,
    max_pixels=2000,
    fig_w=10,
    fig_h=10,
    basemap=True,
    basemap_source=None,
    title=None,
    **kw,
):
    """Render a Cloud-Optimized GeoTIFF inline over a contextily basemap.

    Reads ``path`` decimated so the longest edge is <= ``max_pixels`` (uses the
    COG's overviews when present). ``band`` (1-based) selects a single band;
    otherwise all bands render (1 -> viridis, 3+ -> RGB). Volume/DBFS scheme
    prefixes are stripped. Requires the [vizx] extra plus rasterio.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()
    import rasterio

    from databricks.labs.gbx.vizx._raster import _decimated_read

    p = _strip_scheme(str(path))
    with rasterio.open(p) as src:
        if band is not None:
            scale = max(src.width, src.height) / max_pixels
            out_h = max(1, int(src.height // scale)) if scale > 1 else src.height
            out_w = max(1, int(src.width // scale)) if scale > 1 else src.width
            data = src.read(
                indexes=[band],
                out_shape=(1, out_h, out_w),
                resampling=rasterio.enums.Resampling.bilinear,
                masked=True,
            )
            transform = src.transform * src.transform.scale(
                src.width / out_w, src.height / out_h
            )
        else:
            data, transform, _ = _decimated_read(src, max_pixels)
        crs = src.crs
    _render_cog(
        data,
        transform,
        crs=crs,
        fig_w=fig_w,
        fig_h=fig_h,
        title=title or "COG",
        basemap=basemap,
        basemap_source=basemap_source,
    )
