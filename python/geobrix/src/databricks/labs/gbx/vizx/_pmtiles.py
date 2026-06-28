"""Inline PMTiles viewer for gbx.vizx.

Interactive path: a self-contained MapLibre GL JS + pmtiles.js HTML page
(CDN-loaded at pinned versions) with the archive base64-embedded as an
in-browser FileSource — no tile server, no remote range requests. Interactive
by default; when the embedded archive would exceed ``max_embed_mb`` and
``fallback`` is set (the default), decode tiles on the driver and reuse
plot_raster (raster) / plot_static (vector) over a contextily basemap
(``max_embed_mb=0`` forces this static path). Requires the [vizx] extra for the
static fallback. Driver-side only.
"""

from __future__ import annotations

import base64
import json
from typing import Union

from pmtiles.reader import MemorySource, all_tiles  # noqa: E402

# Pinned CDN versions for reproducibility (asserted by tests).
_MAPLIBRE_JS = "https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"
_MAPLIBRE_CSS = "https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css"
_PMTILES_JS = "https://unpkg.com/pmtiles@3.2.1/dist/pmtiles.js"

_RASTER_TYPES = frozenset({"png", "jpeg", "webp", "avif"})


def _is_raster_type(tile_type: str) -> bool:
    """True for image tile types (raster layer); False for mvt/unknown (vector)."""
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


def _default_style(info: dict, source_name: str) -> dict:
    """A minimal MapLibre style: a pmtiles:// source + one raster or vector layer."""
    is_raster = _is_raster_type(info["tile_type"])
    source = {
        "type": "raster" if is_raster else "vector",
        "url": f"pmtiles://{source_name}",
    }
    if is_raster:
        source["tileSize"] = 256
        layers = [
            {
                "id": "tiles",
                "type": "raster",
                "source": source_name,
                "minzoom": info["min_zoom"],
                "maxzoom": info["max_zoom"],
            }
        ]
    else:
        # Vector: one fill + one line layer per declared source-layer (MVT layer
        # name). The pmtiles metadata's vector_layers carries those ids; fall
        # back to a single "layer0" when absent.
        vlayers = info.get("metadata", {}).get("vector_layers") or [{"id": "layer0"}]
        layers = []
        for vl in vlayers:
            sl = vl.get("id", "layer0")
            layers.append(
                {
                    "id": f"{sl}-fill",
                    "type": "fill",
                    "source": source_name,
                    "source-layer": sl,
                    "paint": {"fill-color": "#3388ff", "fill-opacity": 0.4},
                }
            )
            layers.append(
                {
                    "id": f"{sl}-line",
                    "type": "line",
                    "source": source_name,
                    "source-layer": sl,
                    "paint": {"line-color": "#1144aa", "line-width": 0.5},
                }
            )
    return {"version": 8, "sources": {source_name: source}, "layers": layers}


def _build_pmtiles_html(archive_b64: str, info: dict, *, style=None) -> str:
    """Build a self-contained MapLibre GL JS + pmtiles.js page (CDN-pinned).

    The archive bytes ride inline as ``archive_b64`` and are wrapped in an
    in-browser ``pmtiles.FileSource`` (decoded from base64) registered under the
    ``pmtiles://`` protocol, so the map streams entirely client-side — no tile
    server, no remote range requests.
    """
    source_name = "gbx"
    archive_b64 = archive_b64.replace("\n", "").replace("\r", "")
    map_style = style if style is not None else _default_style(info, source_name)
    style_json = json.dumps(map_style).replace("</", "<\\/")
    minlon, minlat, maxlon, maxlat = info["bounds"]
    clon, clat, czoom = info["center"]
    return f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8"/>
<script src="{_MAPLIBRE_JS}"></script>
<link href="{_MAPLIBRE_CSS}" rel="stylesheet"/>
<script src="{_PMTILES_JS}"></script>
<style>#gbx-map{{height:600px;width:100%;}}</style>
</head><body>
<div id="gbx-map"></div>
<script>
const b64 = "{archive_b64}";
const bin = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
const protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);
const archive = new pmtiles.PMTiles(new pmtiles.FileSource(
    new File([bin.buffer], "gbx.pmtiles")));
protocol.add(archive);
const map = new maplibregl.Map({{
    container: "gbx-map",
    style: {style_json},
    center: [{clon}, {clat}],
    zoom: {max(czoom, 0)}
}});
map.fitBounds([[{minlon}, {minlat}], [{maxlon}, {maxlat}]], {{padding: 20, duration: 0}});
</script>
</body></html>"""


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
    path_or_bytes, *, max_embed_mb=64, fallback=True, style=None, **map_kwargs
):
    """Render a .pmtiles archive inline in a Databricks/Jupyter notebook.

    Interactive path (default, when the archive fits): a MapLibre GL JS +
    pmtiles.js page (CDN-pinned) with the archive base64-embedded as an
    in-browser FileSource, rendered via displayHTML — no tile server, no remote
    range requests. Vector (MVT) -> a vector layer; raster (PNG/JPEG/WebP/AVIF)
    -> a raster layer, auto-detected from the archive header.

    Static fallback (when the base64-embedded archive would exceed
    ``max_embed_mb`` — base64 bloats ~33% — and ``fallback=True``, the default):
    decode tiles on the driver and composite. Raster -> plot_raster; vector ->
    decode MVT to geometries and plot_static over a contextily basemap.
    ``fallback=False`` raises instead of degrading; ``max_embed_mb=0``
    deliberately forces the static render (for GitHub-renderable notebooks).
    ``map_kwargs`` flow to the chosen static plotter. ``style`` overrides the
    auto MapLibre style on the interactive path. Requires the [vizx] extra for
    the static fallback.
    """
    from databricks.labs.gbx.pmtiles import pmtiles_info
    from databricks.labs.gbx.vizx._interactive import _notebook_display_html

    data = _archive_bytes(path_or_bytes)
    info = pmtiles_info(data)

    # Interactive by default. base64 inflates ~33%; compare the *embedded* size
    # against the budget and only then degrade to the static render.
    embed_mb = (len(data) * 4 / 3) / (1024 * 1024)
    if embed_mb > max_embed_mb:
        if not fallback:
            raise ValueError(
                f"plot_pmtiles: archive embeds to ~{embed_mb:.1f} MB which "
                f"exceeds max_embed_mb={max_embed_mb}; pass fallback=True for a "
                "static render or raise max_embed_mb (max_embed_mb=0 forces static)."
            )
        if _is_raster_type(info["tile_type"]):
            return _static_raster_fallback(data, info, **map_kwargs)
        return _static_vector_fallback(data, info, **map_kwargs)

    archive_b64 = base64.b64encode(data).decode("ascii")
    html = _build_pmtiles_html(archive_b64, info, style=style)
    dh = _notebook_display_html()
    if dh is not None:
        dh(html)
        return None
    try:
        from IPython.display import HTML, display

        display(HTML(html))
        return None
    except Exception:  # noqa: BLE001 — no IPython: return the HTML string
        return html
