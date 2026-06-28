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

import json
from typing import Union

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
