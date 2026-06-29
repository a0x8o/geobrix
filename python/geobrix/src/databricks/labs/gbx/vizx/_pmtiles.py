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

from pmtiles.reader import MemorySource, all_tiles  # noqa: E402

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


def _lowest_zoom_tile(data: bytes):
    """Return (z, x, y, payload) for the lowest-zoom tile (the coarsest overview).

    ``all_tiles`` yields ``((z, x, y), payload)`` — the ZXY triple is already
    decoded by the reader's ``traverse`` helper, so no secondary
    ``tileid_to_zxy`` call is needed here.
    """
    best = None
    for (z, x, y), payload in all_tiles(MemorySource(data)):
        if best is None or z < best[0]:
            best = (z, x, y, payload)
    return best


def _static_raster_fallback(data: bytes, info: dict, **plot_kw) -> None:
    """Decode the coarsest raster tile and render it via plot_raster."""
    from databricks.labs.gbx.vizx import plot_raster

    tile = _lowest_zoom_tile(data)
    if tile is None:
        raise ValueError("plot_pmtiles: archive has no tiles to render")
    # plot_raster does not accept a basemap kwarg (raster tiles are already
    # georeferenced imagery; there is no separate tile fetch step).
    plot_kw.pop("basemap", None)
    plot_raster(tile[3], **plot_kw)


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

    decoded = mvt.decode(payload)
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


def _static_vector_fallback(data: bytes, info: dict, **plot_kw):
    """Decode MVT tiles to geometries and render via plot_static (contextily)."""
    import geopandas as gpd

    from databricks.labs.gbx.vizx import plot_static

    geoms, rows = [], []
    for (z, x, y), payload in all_tiles(MemorySource(data)):
        for geom, props in _decode_mvt_to_geoms(payload, z, x, y):
            geoms.append(geom)
            rows.append(props)
    if not geoms:
        raise ValueError("plot_pmtiles: vector archive decoded to no geometries")
    gdf = gpd.GeoDataFrame(rows, geometry=geoms, crs=4326)
    return plot_static(gdf, **plot_kw)


def plot_pmtiles(
    path_or_bytes, *, max_embed_mb=64, fallback=True, style=None, **kw
):
    """Render a .pmtiles archive inline in a Databricks/Jupyter notebook.

    Thin delegator to :func:`~databricks.labs.gbx.vizx._interactive.plot_interactive`
    with a single :func:`~databricks.labs.gbx.vizx._layers.pmtiles_layer` wrapping
    the archive. The ``style`` kwarg is forwarded to ``pmtiles_layer``; remaining
    ``**kw`` (``basemap``, ``center``, ``zoom``, etc.) are forwarded to
    ``plot_interactive``.

    Interactive path (default, when the archive fits within ``max_embed_mb``):
    a MapLibre GL JS page with the archive base64-embedded — no tile server, no
    remote range requests. Static fallback when oversized (``fallback=True``,
    the default) or ``max_embed_mb=0`` to force it.
    """
    from databricks.labs.gbx.vizx._interactive import plot_interactive
    from databricks.labs.gbx.vizx._layers import pmtiles_layer

    return plot_interactive(
        [pmtiles_layer(path_or_bytes, style=style)],
        max_embed_mb=max_embed_mb,
        fallback=fallback,
        **kw,
    )
