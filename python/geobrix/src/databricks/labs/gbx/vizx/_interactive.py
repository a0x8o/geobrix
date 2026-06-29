"""Interactive (MapLibre GL) map rendering for gbx.vizx.

``plot_interactive`` is the multi-layer interactive entry point. It coerces
``layers`` via ``as_layers``, runs the budget ladder via ``prepare_layers``
(Tasks 5/6), then either builds a MapLibre GL HTML page via ``build_html`` or
delegates to ``plot_static`` when the embed budget is exceeded. No folium.

Requires the [vizx] extra.
"""

from __future__ import annotations

from databricks.labs.gbx.vizx._maplibre import DEFAULT_MAX_EMBED_MB

try:
    from IPython import get_ipython
except Exception:  # noqa: BLE001 — IPython absent (plain Python)

    def get_ipython():  # type: ignore[misc]
        return None


def _notebook_display_html():
    """Return Databricks' notebook ``displayHTML`` callable, or None.

    GeoBrix vizx targets Databricks notebooks only. ``displayHTML`` renders into
    a sandboxed iframe and is NOT subject to the Jupyter/IPython cell-output
    size cap — so large base64-embedded PMTiles maps must go through it, never
    through ``IPython.display.HTML`` (which IS capped and silently drops the
    map with "Output too large").

    Databricks exposes ``displayHTML`` differently across runtimes:
      1. Classic DBR notebook kernels inject it into the IPython *user
         namespace* (``get_ipython().user_ns``).
      2. Serverless / newer kernels expose it via ``dbruntime.display`` instead,
         where it is NOT in ``user_ns`` — so the user-ns-only lookup returns
         None and callers wrongly degrade to the capped ``IPython.display.HTML``
         path. Resolve that case explicitly.

    Returns None only when no Databricks display channel is reachable (plain
    Python, no IPython, no dbruntime).
    """
    # 1. IPython user namespace (classic DBR notebooks).
    try:
        ip = get_ipython()
        if ip is not None:
            dh = ip.user_ns.get("displayHTML")
            if dh is not None:
                return dh
    except Exception:  # noqa: BLE001 — IPython misbehaving
        pass

    # 2. dbruntime.display (Serverless / newer DBR kernels).
    try:
        from dbruntime.display import displayHTML  # type: ignore[import-not-found]

        if displayHTML is not None:
            return displayHTML
    except Exception:  # noqa: BLE001 — not on a Databricks runtime
        pass

    return None


def _format_audit_line(audit: dict, max_embed_mb: float) -> str:
    """Return the one-line audit summary for printing before render."""
    layer_parts = []
    for entry in audit.get("layers", []):
        label = entry.get("label", "?")
        eb = entry.get("embed_bytes", 0)
        layer_parts.append(f"{label} {eb / 1_048_576:.1f}MB")
    summary = " + ".join(layer_parts) if layer_parts else "(no layers)"
    total = audit.get("total_embed_bytes", 0)
    fits = audit.get("fits", False)
    verdict = audit.get("verdict", "?")
    cmp = "≤" if fits else ">"
    verdict_desc = {
        "embed": "embedding inline",
        "url": "streaming from URL",
        "simplify": "simplifying",
        "static": "static fallback",
    }.get(verdict, verdict)
    return (
        f"[vizx] {summary} = {total / 1_048_576:.1f}MB "
        f"{cmp} {max_embed_mb}MB → {verdict_desc}"
    )


def plot_interactive(
    layers,
    *,
    basemap: str = "carto-positron",
    simplify_tiles_spec=None,
    max_embed_mb: float = DEFAULT_MAX_EMBED_MB,
    fallback: bool = True,
    center=None,
    zoom=None,
    dry_run: bool = False,
) -> "str | None | dict":
    """Render one or more layers as an interactive MapLibre GL map.

    Args:
        layers:               A :class:`~databricks.labs.gbx.vizx._layers.Layer`,
                              a list of layers, or any input accepted by
                              :func:`~databricks.labs.gbx.vizx._layers.as_layers`
                              (GeoDataFrame, bytes pmtiles archive, etc.).
        basemap:              ``"carto-positron"`` (default) or ``"none"``.
        simplify_tiles_spec:  Optional simplification spec (Task 11, not yet
                              implemented — pass ``None``).
        max_embed_mb:         Maximum HTML embed size in mebibytes (default
                              ``DEFAULT_MAX_EMBED_MB``). Sized for the Databricks
                              Serverless cell-output cap (10 MB default, 20 MB max);
                              measured against the base64-rendered HTML.
        fallback:             When ``True`` (default), degrade to
                              :func:`~databricks.labs.gbx.vizx._static_map.plot_static`
                              when the budget is exceeded.  When ``False``, raise.
        center:               ``[lon, lat]`` map centre override.
        zoom:                 Initial zoom level override.
        dry_run:              When ``True``, return the audit dict without rendering
                              (no ``displayHTML``, no HTML string).

    Returns:
        ``dry_run=True``: the audit dict (see :func:`audit_layers`).
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

    # Always print the one-line audit before rendering.
    audit = result.get("audit", {})
    print(_format_audit_line(audit, max_embed_mb))

    if dry_run:
        return audit

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
