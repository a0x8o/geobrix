"""Inline PMTiles viewer for gbx.vizx.

Interactive path: a self-contained MapLibre GL JS + pmtiles.js HTML page
(CDN-loaded at pinned versions, SRI-hashed) with the archive base64-embedded
as an in-browser FileSource — no tile server, no remote range requests.
Interactive by default; when the embedded archive would exceed ``max_embed_mb``
and ``fallback`` is set (the default), decode tiles on the driver and reuse
plot_raster (raster) / plot_static (vector) over a contextily basemap
(``max_embed_mb=0`` forces this static path). Requires the [vizx] extra for the
static fallback. Driver-side only.

CDN pins and SRI hashes live in ``_maplibre.py`` (the single source of truth).
"""

from __future__ import annotations

from typing import Union

from pmtiles.reader import MemorySource, all_tiles  # noqa: F401

_RASTER_TYPES = frozenset({"png", "jpeg", "webp", "avif"})


_SUPPORTED_TILE_TYPES = frozenset({"png", "jpeg", "webp", "avif", "mvt"})


def _is_raster_type(tile_type: str) -> bool:
    """True for image tile types (raster layer); False for mvt (vector)."""
    return tile_type in _RASTER_TYPES


def _strip_scheme(path: str) -> str:
    for scheme in ("dbfs:", "file:"):
        if path.startswith(scheme):
            path = path[len(scheme) :]
            break
    if path.startswith("//"):
        path = "/" + path.lstrip("/")
    return path


def _archive_bytes(path_or_bytes: Union[str, bytes, bytearray]) -> bytes:
    """Read a .pmtiles path (Volume/DBFS scheme stripped) or pass bytes through."""
    if isinstance(path_or_bytes, (bytes, bytearray)):
        return bytes(path_or_bytes)
    with open(_strip_scheme(str(path_or_bytes)), "rb") as f:
        return f.read()


def _maybe_gunzip(payload: bytes) -> bytes:
    """Inflate a gzip-compressed tile payload (PMTiles tile_compression=gzip).

    PMTiles stores tiles per the archive's tile_compression and the reader yields
    the raw stored bytes, so gzipped tiles (gzip magic 0x1f 0x8b) must be inflated
    before decoding — rasterio's MemoryFile and mapbox_vector_tile.decode both
    reject gzip-wrapped bytes. Idempotent no-op on already-raw payloads.
    """
    import gzip

    if payload and payload[:2] == b"\x1f\x8b":
        return gzip.decompress(payload)
    return payload


def _decode_mvt_to_geoms(payload: bytes, z: int, x: int, y: int):
    """Decode one MVT tile to (shapely_geom, props) pairs in WGS-84 (EPSG:4326).

    MVT features are tile-local pixel coords [0, extent] with the NW origin
    (y down), matching what pyvx writes; invert that transform back to lon/lat
    using the same tile-bounds math.
    """
    import mapbox_vector_tile as mvt
    from shapely.geometry import shape
    from shapely.ops import transform

    from databricks.labs.gbx.pyvx._mvt import _tile_bounds

    decoded = mvt.decode(_maybe_gunzip(payload))
    out = []
    for layer in decoded.values():
        extent = layer.get("extent", 4096)
        minx, miny, maxx, maxy = _tile_bounds(z, x, y)
        sx = (maxx - minx) / extent
        sy = (maxy - miny) / extent

        def _to_lonlat(px, py, zc=None, _minx=minx, _maxy=maxy, _sx=sx, _sy=sy):
            return (_minx + px * _sx, _maxy - py * _sy)

        for feat in layer.get("features", []):
            geom = shape(feat["geometry"])
            if geom.is_empty:
                continue
            out.append((transform(_to_lonlat, geom), feat.get("properties", {})))
    return out


def plot_pmtiles(
    path_or_bytes,
    *,
    max_embed_mb=None,
    set_cell_max_output=True,
    fallback=True,
    interactive_fit=None,
    style=None,
    **kw,
):
    """Render a .pmtiles archive inline in a Databricks notebook.

    Thin delegator to :func:`~databricks.labs.gbx.vizx._interactive.plot_interactive`
    with a single :func:`~databricks.labs.gbx.vizx._layers.pmtiles_layer` wrapping
    the archive. The ``style`` kwarg is forwarded to ``pmtiles_layer``; remaining
    ``**kw`` (``basemap``, ``center``, ``zoom``, etc.) are forwarded to
    ``plot_interactive``.

    Interactive path (default, when the archive fits within ``max_embed_mb``):
    a MapLibre GL JS page with the archive base64-embedded — no tile server, no
    remote range requests. Static fallback when oversized (``fallback=True``,
    the default) or ``max_embed_mb=0`` to force it.

    On Databricks Serverless a notebook cell caps output at 10 MB (20 MB max via
    ``%set_cell_max_output_size_in_mb``), and the cap counts the base64-rendered
    HTML (~4/3x the archive). An archive whose rendered size exceeds that ceiling
    cannot embed inline. ``interactive_fit`` controls how to still get an interactive
    experience — an "investment dial":

    - ``None`` (default): no reduction. Embed if it fits; otherwise fall back to
      a static render (or raise if ``fallback=False``).
    - ``"downzoom"``: invest little. Auto-fit the single archive to the budget by
      dropping the highest (densest) zoom levels (see
      :func:`~databricks.labs.gbx.vizx._pmtiles_autofit.autofit_archive`), then
      embed. One interactive map of the whole extent at reduced detail. Fast
      (no re-tiling). If even the coarsest level exceeds the budget, falls back
      to static.
    - ``"all"``: invest more for full detail — spatially shard into per-region
      sub-archives, each under budget, rendered as a multi-shard interactive
      experience. **Not yet implemented** (planned); raises
      :exc:`NotImplementedError`.

    For an archive of any size with zero embed cost, stage it at an ``https://``
    URL and pass that URL to ``pmtiles_layer`` / ``plot_pmtiles`` — it streams
    remotely and is always interactive regardless of the cell cap.
    """
    if interactive_fit not in (None, "downzoom", "all"):
        raise ValueError(
            f"plot_pmtiles: interactive_fit must be None, 'downzoom', or 'all'; "
            f"got {interactive_fit!r}"
        )
    if interactive_fit == "all":
        raise NotImplementedError(
            "plot_pmtiles: interactive_fit='all' (multi-shard full-detail interactive "
            "rendering) is not yet implemented. Use interactive_fit='downzoom' for a "
            "reduced-detail interactive map now, stage the archive at an https:// "
            "URL for full detail at zero embed cost, or pre-shard your data."
        )

    from databricks.labs.gbx.vizx._interactive import plot_interactive
    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import _resolve_embed_budget

    # Resolve the embed budget up front so the downzoom autofit + the audit all use
    # the same value (18 MB when set_cell_max_output raises the cap, else 8 MB).
    max_embed_mb = _resolve_embed_budget(max_embed_mb, set_cell_max_output)

    archive = path_or_bytes
    if interactive_fit == "downzoom" and max_embed_mb and max_embed_mb > 0:
        # Invest-little path: auto-fit by down-zooming until the archive's
        # rendered size is within budget, then embed the reduced archive.
        from databricks.labs.gbx.vizx._pmtiles_autofit import autofit_archive

        raw = _archive_bytes(path_or_bytes)
        reduced, report = autofit_archive(raw, max_embed_mb=max_embed_mb)
        if report["dropped_zooms"]:
            import warnings

            warnings.warn(
                "plot_pmtiles interactive_fit='downzoom': reduced the archive to fit the "
                f"{max_embed_mb:.1f} MB embed budget by dropping zoom level(s) "
                f"{report['dropped_zooms']} (kept z<= {report['kept_max_zoom']}; "
                f"{report['original_bytes']} -> {report['reduced_bytes']} bytes). "
                "Detail above the kept zoom is not shown; stage the archive at an "
                "https:// URL or use interactive_fit='all' (planned) for full detail.",
                stacklevel=2,
            )
        archive = reduced

    return plot_interactive(
        [pmtiles_layer(archive, style=style)],
        max_embed_mb=max_embed_mb,
        set_cell_max_output=set_cell_max_output,
        fallback=fallback,
        **kw,
    )
