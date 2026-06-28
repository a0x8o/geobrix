"""Interactive (MapLibre GL) map rendering for gbx.vizx.

``plot_interactive`` is the multi-layer interactive entry point. It coerces
``layers`` via ``as_layers``, runs the budget ladder via ``prepare_layers``
(Tasks 5/6), then either builds a MapLibre GL HTML page via ``build_html`` or
delegates to ``plot_static`` when the embed budget is exceeded. No folium.

Requires the [vizx] extra.
"""

from __future__ import annotations


def _notebook_display_html():
    """Return Databricks' notebook ``displayHTML`` callable, or None.

    ``displayHTML`` is injected by Databricks into the *notebook's* user
    namespace, not into library module globals — so a bare ``displayHTML(...)``
    from inside this module raises ``NameError``. Reach it via the IPython
    user namespace instead. Returns None when unavailable (plain Python,
    no IPython, etc.).
    """
    try:
        from IPython import get_ipython

        ip = get_ipython()
        return ip.user_ns.get("displayHTML") if ip is not None else None
    except Exception:  # noqa: BLE001 — IPython absent or misbehaving
        return None


def plot_interactive(
    layers,
    *,
    basemap: str = "carto-positron",
    simplify_tiles_spec=None,
    max_embed_mb: float = 64,
    fallback: bool = True,
    center=None,
    zoom=None,
) -> "str | None":
    """Render one or more layers as an interactive MapLibre GL map.

    Args:
        layers:               A :class:`~databricks.labs.gbx.vizx._layers.Layer`,
                              a list of layers, or any input accepted by
                              :func:`~databricks.labs.gbx.vizx._layers.as_layers`
                              (GeoDataFrame, bytes pmtiles archive, etc.).
        basemap:              ``"carto-positron"`` (default) or ``"none"``.
        simplify_tiles_spec:  Optional simplification spec (Task 11, not yet
                              implemented — pass ``None``).
        max_embed_mb:         Maximum HTML embed size in mebibytes (default 64).
        fallback:             When ``True`` (default), degrade to
                              :func:`~databricks.labs.gbx.vizx._static_map.plot_static`
                              when the budget is exceeded.  When ``False``, raise.
        center:               ``[lon, lat]`` map centre override.
        zoom:                 Initial zoom level override.

    Returns:
        In a Databricks/IPython notebook: calls ``displayHTML`` and returns
        ``None``.  Outside a notebook (e.g. in tests): returns the HTML string
        so callers can assert on it.  On the static-fallback path: returns
        whatever ``plot_static`` returns.
    """
    from databricks.labs.gbx.vizx._layers import as_layers
    from databricks.labs.gbx.vizx._maplibre import build_html, prepare_layers

    lyrs = as_layers(layers)
    result = prepare_layers(
        lyrs,
        max_embed_mb=max_embed_mb,
        simplify_tiles_spec=simplify_tiles_spec,
        fallback=fallback,
    )

    if result["mode"] == "interactive":
        html = build_html(
            result["prepared"],
            basemap=basemap,
            center=center,
            zoom=zoom,
        )
        # Attempt notebook display (Databricks displayHTML via IPython user ns).
        dh = _notebook_display_html()
        if dh is not None:
            dh(html)
            return None
        # IPython.display fallback (standard Jupyter).
        try:
            from IPython.display import HTML, display

            display(HTML(html))
            return None
        except Exception:  # noqa: BLE001 — no IPython; return string for tests
            pass
        return html

    # mode == "static": delegate to plot_static.
    # result["warnings"] were already warn()'d inside prepare_layers; don't re-warn.
    from databricks.labs.gbx.vizx._static_map import plot_static

    return plot_static(result["prepared"])
