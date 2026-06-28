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
