"""MapLibre GL per-layer adapters and HTML builder for the VizX interactive compositor.

``layer_to_sources_layers(layer, idx)`` converts one :class:`~databricks.labs.gbx.vizx._layers.Layer`
into the MapLibre GL ``sources`` dict, ``layers`` list, and ``embed_bytes`` integer
that ``build_html`` (Task 5) stitches together into a self-contained HTML viewer.

``build_html(prepared, *, basemap, center, zoom)`` assembles N per-layer outputs into
one self-contained HTML page with SRI-pinned ``<script>`` tags, a CARTO basemap,
and pmtiles protocol registration (embed or url mode per layer).

Dispatch by ``layer.kind``:

* ``"vector"`` / ``"grid"`` — inline ``geojson`` source reprojected to EPSG:4326;
  fill/line/circle sub-layers chosen by geometry type.
* ``"raster"`` — ``image`` source with 4-corner ``coordinates`` in lon/lat;
  PNG rendered via rasterio (decimated to ≤ ``raster_max_px``).
* ``"pmtiles"`` — ``raster|vector`` source with ``pmtiles://gbx{idx}`` URL plus a
  ``_gbx_pmtiles`` sidecar dict recording embed mode or remote URL; consumed and
  popped by the HTML builder.
"""

from __future__ import annotations

import base64
import io
import json
from typing import Any


# ---------------------------------------------------------------------------
# security helper
# ---------------------------------------------------------------------------


def _json_for_script(obj) -> str:
    """json.dumps escaped for safe embedding inside an HTML <script> block.

    json.dumps alone does NOT escape ``</script>``, ``<``, ``>``, or ``&``,
    so a crafted value in user data can break out of the script context.
    The Unicode escapes produced here are valid JSON/JS string escapes —
    ``JSON.parse`` and the JS engine see the original characters at runtime.
    """
    return (
        json.dumps(obj)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
        .replace(" ", "\\u2028")
        .replace(" ", "\\u2029")
    )

# ---------------------------------------------------------------------------
# SRI-pinned CDN constants — hashes are finalised in Task 12.
# ---------------------------------------------------------------------------

_MAPLIBRE_JS = "https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"
_MAPLIBRE_JS_SRI = "sha384-REPLACE_WITH_PINNED_HASH"  # pinned in Task 12
_PMTILES_JS = "https://unpkg.com/pmtiles@3.2.0/dist/pmtiles.js"
_PMTILES_JS_SRI = "sha384-REPLACE_WITH_PINNED_HASH"  # pinned in Task 12
_CARTO_STYLE = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"

_DEFAULT_RASTER_MAX_PX = 1024


# ---------------------------------------------------------------------------
# public entry point
# ---------------------------------------------------------------------------


def layer_to_sources_layers(
    layer, idx: int, *, raster_max_px: int = _DEFAULT_RASTER_MAX_PX
) -> tuple[dict, list[dict], int]:
    """Convert *layer* to MapLibre GL ``(sources, layers, embed_bytes)``.

    Args:
        layer:        A :class:`~databricks.labs.gbx.vizx._layers.Layer`.
        idx:          Integer index; drives the source key ``f"gbx{idx}"`` and
                      layer ids ``f"gbx{idx}-{type}"``.
        raster_max_px: Maximum pixel size (longest edge) for decimated raster PNG.

    Returns:
        ``(sources, layers, embed_bytes)`` where *sources* is a dict of MapLibre
        source entries, *layers* is a list of MapLibre layer dicts, and
        *embed_bytes* reports the driver-side payload (GeoJSON bytes for vector/grid,
        PNG bytes for raster, archive bytes for pmtiles embed mode, 0 for url mode).
    """
    kind = getattr(layer, "kind", None)
    if kind in ("vector", "grid"):
        return _vector_or_grid(layer, idx)
    if kind == "raster":
        return _raster(layer, idx, raster_max_px=raster_max_px)
    if kind == "pmtiles":
        return _pmtiles(layer, idx)
    raise ValueError(f"layer_to_sources_layers: unknown layer.kind={kind!r}")


# ---------------------------------------------------------------------------
# public HTML builder
# ---------------------------------------------------------------------------


def build_html(
    prepared,
    *,
    basemap: str = "carto-positron",
    center=None,
    zoom=None,
) -> str:
    """Assemble N per-layer adapter outputs into one self-contained HTML viewer.

    Args:
        prepared: A list of ``(sources, layers, embed_bytes)`` tuples as returned
                  by :func:`layer_to_sources_layers`.  The ``sources`` dicts are
                  mutated in-place: any ``_gbx_pmtiles`` sidecar key is popped
                  before the source is serialised into the MapLibre style.
        basemap:  ``"carto-positron"`` (default) uses the CARTO Positron style as
                  the base layer.  Pass ``"none"`` to render a blank dark canvas.
        center:   ``[lon, lat]`` map centre (default San Francisco ``[-122.43, 37.77]``).
        zoom:     Initial zoom level (default ``11``).

    Returns:
        A self-contained HTML string with inline ``<script>`` tags, SRI hashes,
        and base64-embedded PMTiles archives (or URL-referenced ones).
    """
    sources: dict = {}
    layers: list = []
    pm_pairs: list[tuple[str, dict]] = []

    for s, ls, *_rest in prepared:
        for sid, sdef in s.items():
            if "_gbx_pmtiles" in sdef:
                # Read sidecar WITHOUT mutating the caller's dict — Task 6 calls
                # build_html twice (size-check then real build) and must find the
                # sidecar intact on the second call.
                pm_pairs.append((sid, sdef["_gbx_pmtiles"]))
                # Build a clean copy for the overlay (MapLibre rejects unknown keys).
                clean = {k: v for k, v in sdef.items() if k != "_gbx_pmtiles"}
                sources[sid] = clean
            else:
                sources[sid] = sdef
        layers.extend(ls)

    if basemap and basemap != "none":
        base_js = f'"{_CARTO_STYLE}"'
    else:
        base_js = "{version:8,sources:{},layers:[]}"

    overlay_json = _json_for_script({"sources": sources, "layers": layers})
    pm_js = "".join(_pmtiles_register_js(sid, info) for sid, info in pm_pairs)

    center_js = _json_for_script(center if center is not None else [-122.43, 37.77])
    zoom_js = zoom if zoom is not None else 11

    return f"""\
<div id="gbx-map" style="height:480px"></div>
<link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet"/>
<script src="{_MAPLIBRE_JS}" integrity="{_MAPLIBRE_JS_SRI}" crossorigin="anonymous"></script>
<script src="{_PMTILES_JS}" integrity="{_PMTILES_JS_SRI}" crossorigin="anonymous"></script>
<script>
  const proto = new pmtiles.Protocol();
  maplibregl.addProtocol('pmtiles', proto.tile.bind(proto));
  {pm_js}
  const map = new maplibregl.Map({{
    container: 'gbx-map',
    style: {base_js},
    center: {center_js},
    zoom: {zoom_js}
  }});
  const overlay = {overlay_json};
  map.on('load', () => {{
    for (const [sid, sdef] of Object.entries(overlay.sources)) {{
      map.addSource(sid, sdef);
    }}
    for (const ly of overlay.layers) {{
      map.addLayer(ly);
    }}
  }});
</script>"""


def _pmtiles_register_js(sid: str, info: dict) -> str:
    """Return the JS snippet that registers one PMTiles source with the protocol.

    For ``"url"`` mode the archive is fetched on demand from the remote URL.
    For ``"embed"`` mode the bytes are base64-encoded into a JS ``Uint8Array``
    wrapped in a ``File`` → ``pmtiles.FileSource``.
    """
    if info["mode"] == "url":
        return f"  proto.add(new pmtiles.PMTiles({_json_for_script(info['url'])}));\n"
    # Embed mode: base64 → Uint8Array → File → FileSource.
    # The base64 string is already ASCII-safe; json.dumps is sufficient here.
    b64 = base64.b64encode(info["bytes"]).decode("ascii")
    return (
        f"  const _b{sid} = Uint8Array.from(atob({json.dumps(b64)}), c => c.charCodeAt(0));\n"
        f"  proto.add(new pmtiles.PMTiles(new pmtiles.FileSource("
        f"new File([_b{sid}.buffer], '{sid}.pmtiles'))));\n"
    )


# ---------------------------------------------------------------------------
# vector / grid
# ---------------------------------------------------------------------------


def _gdf_for(layer) -> Any:
    """Return a GeoDataFrame for *layer* (vector or grid)."""
    from databricks.labs.gbx.vizx import _vector

    if layer.kind == "grid":
        return _vector.cells_as_gdf(
            layer.data, cell_col=layer.cellid_col or "cellid"
        )
    data = layer.data
    # Already a GeoDataFrame (has a .geometry attribute).
    if hasattr(data, "geometry"):
        return data
    # Spark DataFrame with a WKT column — collect and wrap.
    wkt_col = layer.geom_col or "wkt"
    return _vector.as_gdf(data, wkt_col=wkt_col)


def _vector_or_grid(layer, idx: int) -> tuple[dict, list[dict], int]:
    sid = f"gbx{idx}"
    gdf = _gdf_for(layer).to_crs(4326)
    gj = json.loads(gdf.to_json())

    src = {sid: {"type": "geojson", "data": gj}}

    # Collect all geometry types present in this feature collection.
    geom_types: set[str] = set()
    for feat in gj.get("features", []):
        geom = feat.get("geometry") or {}
        t = geom.get("type", "")
        if t:
            geom_types.add(t)

    layers: list[dict] = []
    color = layer.color or "#3388ff"
    opacity = layer.opacity if layer.opacity is not None else 0.5

    # Polygons → fill + outline line.
    if geom_types & {"Polygon", "MultiPolygon"}:
        if getattr(layer, "fill", True):
            layers.append(
                {
                    "id": f"{sid}-fill",
                    "type": "fill",
                    "source": sid,
                    "paint": {
                        "fill-color": color,
                        "fill-opacity": opacity,
                    },
                }
            )
        layers.append(
            {
                "id": f"{sid}-line",
                "type": "line",
                "source": sid,
                "paint": {
                    "line-color": layer.color or "#1f6fb5",
                    "line-width": layer.width or 1.0,
                },
            }
        )
    # Lines (no polygon — those already got an outline above).
    if geom_types & {"LineString", "MultiLineString"}:
        layers.append(
            {
                "id": f"{sid}-line",
                "type": "line",
                "source": sid,
                "paint": {
                    "line-color": layer.color or "#1f6fb5",
                    "line-width": layer.width or 1.0,
                },
            }
        )
    # Points.
    if geom_types & {"Point", "MultiPoint"}:
        layers.append(
            {
                "id": f"{sid}-circle",
                "type": "circle",
                "source": sid,
                "paint": {
                    "circle-color": layer.color or "#e04e2a",
                    "circle-radius": 4,
                },
            }
        )

    embed_bytes = len(json.dumps(gj).encode())
    return src, layers, embed_bytes


# ---------------------------------------------------------------------------
# raster
# ---------------------------------------------------------------------------


def _raster_to_image(
    layer, *, raster_max_px: int = _DEFAULT_RASTER_MAX_PX
) -> tuple[str, list]:
    """Render *layer.data* to a base64 PNG + 4-corner lon/lat coordinates.

    *layer.data* may be:
    - A filesystem path (str) to a GeoTIFF/COG.
    - ``bytes`` or ``bytearray`` of an in-memory GeoTIFF (e.g. a tile's
      ``raster`` field).
    - A bare ``numpy.ndarray`` (no geo metadata; unit-square [0,1] corners
      are synthesised).

    Returns ``(png_b64, corners)`` where *png_b64* is a URL-safe base64 string
    (no newlines) and *corners* is
    ``[[ulx,uly],[urx,ury],[lrx,lry],[llx,lly]]`` in lon/lat degrees.
    """
    import numpy as np

    data = layer.data

    # --- numpy ndarray path: no spatial metadata ----------------------------
    if isinstance(data, np.ndarray):
        png_b64 = _ndarray_to_png_b64(data)
        # Synthesise unit-square corners (MapLibre image source requires 4 corners).
        corners = [[0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]]
        return png_b64, corners

    # --- rasterio path (path or bytes) -------------------------------------
    import rasterio
    from rasterio.io import MemoryFile

    if isinstance(data, (bytes, bytearray)):
        # MemoryFile needs a double context-manager: outer opens the file object,
        # inner .open() returns the rasterio DatasetReader.
        with MemoryFile(bytes(data)) as mf:
            with mf.open() as src:
                png_b64, corners = _src_to_png_b64(src, raster_max_px)
        return png_b64, corners
    elif isinstance(data, str):
        # Strip dbfs:/file: scheme prefixes (mirroring _raster.py).
        path = data
        for scheme in ("dbfs:", "file:"):
            if path.startswith(scheme):
                path = path[len(scheme):]
                break
        if path.startswith("//"):
            path = "/" + path.lstrip("/")
        with rasterio.open(path) as src:
            png_b64, corners = _src_to_png_b64(src, raster_max_px)
        return png_b64, corners
    else:
        raise TypeError(
            f"_raster_to_image: unsupported data type {type(data).__name__!r}; "
            "expected str path, bytes, or numpy.ndarray"
        )


def _src_to_png_b64(src, raster_max_px: int) -> tuple[str, list]:
    """Read, decimate, render to RGBA PNG, base64-encode; extract corners."""
    import rasterio
    from rasterio.warp import transform_bounds

    # Decimate so longest edge ≤ raster_max_px.
    scale = max(src.width, src.height) / raster_max_px
    if scale > 1:
        out_h = max(1, int(src.height // scale))
        out_w = max(1, int(src.width // scale))
        data = src.read(
            out_shape=(src.count, out_h, out_w),
            resampling=rasterio.enums.Resampling.bilinear,
            masked=True,
        )
    else:
        data = src.read(masked=True)
        out_h, out_w = src.height, src.width

    # Reproject bounding box to EPSG:4326 to get lon/lat corners.
    try:
        bounds_4326 = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
    except Exception:
        # Fall back to treating the existing bounds as lon/lat.
        bounds_4326 = src.bounds

    min_lon, min_lat, max_lon, max_lat = bounds_4326
    # MapLibre image source corner order: ul, ur, lr, ll (lon, lat).
    corners = [
        [min_lon, max_lat],  # upper-left
        [max_lon, max_lat],  # upper-right
        [max_lon, min_lat],  # lower-right
        [min_lon, min_lat],  # lower-left
    ]

    png_b64 = _data_to_png_b64(data, out_h, out_w)
    return png_b64, corners


def _data_to_png_b64(data, height: int, width: int) -> str:
    """Render a (bands, H, W) masked array to a base64 RGBA PNG string."""
    import numpy as np
    from PIL import Image

    # Normalise to [0, 255] uint8 for PNG encoding.
    if isinstance(data, np.ma.MaskedArray):
        arr = data.filled(0)
    else:
        arr = np.asarray(data)

    if arr.ndim == 2:
        arr = arr[np.newaxis, ...]  # treat as single band

    n_bands = arr.shape[0]
    if n_bands == 1:
        # Greyscale → viridis-like: just map to grey for simplicity, with alpha.
        band = arr[0].astype(np.float64)
        bmin, bmax = band.min(), band.max()
        rng = max(bmax - bmin, 1e-9)
        norm = ((band - bmin) / rng * 255).astype(np.uint8)
        rgba = np.stack([norm, norm, norm, np.full_like(norm, 255)], axis=-1)
    elif n_bands >= 3:
        # Take first 3 bands as RGB.
        bands = []
        for i in range(3):
            b = arr[i].astype(np.float64)
            bmin, bmax = b.min(), b.max()
            rng = max(bmax - bmin, 1e-9)
            bands.append(((b - bmin) / rng * 255).astype(np.uint8))
        alpha = np.full((height, width), 255, dtype=np.uint8)
        rgba = np.stack(bands + [alpha], axis=-1)
    else:
        # 2 bands: treat as greyscale from first band.
        band = arr[0].astype(np.float64)
        bmin, bmax = band.min(), band.max()
        rng = max(bmax - bmin, 1e-9)
        norm = ((band - bmin) / rng * 255).astype(np.uint8)
        rgba = np.stack([norm, norm, norm, np.full_like(norm, 255)], axis=-1)

    img = Image.fromarray(rgba, mode="RGBA")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _ndarray_to_png_b64(arr) -> str:
    """Render a bare ndarray (2-D or 3-D CxHxW) to a base64 PNG string."""
    import numpy as np

    if arr.ndim == 2:
        h, w = arr.shape
    elif arr.ndim == 3:
        _, h, w = arr.shape
    else:
        # Flatten extra dims.
        arr = arr.reshape(-1, arr.shape[-2], arr.shape[-1])
        _, h, w = arr.shape
    return _data_to_png_b64(arr, h, w)


def _raster(
    layer, idx: int, *, raster_max_px: int = _DEFAULT_RASTER_MAX_PX
) -> tuple[dict, list[dict], int]:
    sid = f"gbx{idx}"
    png_b64, corners = _raster_to_image(layer, raster_max_px=raster_max_px)
    url = f"data:image/png;base64,{png_b64}"
    src = {
        sid: {
            "type": "image",
            "url": url,
            "coordinates": corners,
        }
    }
    lyr = {
        "id": f"{sid}-raster",
        "type": "raster",
        "source": sid,
        "paint": {"raster-opacity": layer.opacity if layer.opacity is not None else 1.0},
    }
    embed_bytes = len(png_b64.encode())
    return src, [lyr], embed_bytes


# ---------------------------------------------------------------------------
# pmtiles
# ---------------------------------------------------------------------------


def _extract_vector_layer_names(metadata: dict) -> list[str]:
    """Extract declared vector-layer names from TileJSON-style metadata.

    The standard TileJSON ``vector_layers`` key holds a list of objects each
    with an ``"id"`` string.  Returns that list of ids (preserving order).
    Falls back to an empty list when the key is absent or malformed.
    """
    layers = metadata.get("vector_layers", [])
    if not isinstance(layers, list):
        return []
    names = []
    for entry in layers:
        if isinstance(entry, dict) and isinstance(entry.get("id"), str):
            names.append(entry["id"])
    return names


def _resolve_pmtiles_bytes_or_url(layer) -> dict:
    """Return a sidecar info dict for the pmtiles layer.

    Returns one of:
    - ``{"mode": "url", "url": <str>, "tile_type": <str>, "vector_layer_names": <list>}``
      — when ``layer.data`` is an ``http(s)://`` URL (no local bytes needed).
      ``vector_layer_names`` is empty because we cannot inspect a remote archive
      without fetching it.
    - ``{"mode": "embed", "bytes": <bytes>, "tile_type": <str>, "vector_layer_names": <list>}``
      — when ``layer.data`` is a path or bytes archive; ``pmtiles_info`` is called
      to detect the tile type and extract declared vector layer names from metadata.
    """
    from databricks.labs.gbx.pmtiles import pmtiles_info

    data = layer.data

    # Remote URL: no need to read bytes.
    if isinstance(data, str) and (
        data.startswith("http://") or data.startswith("https://")
    ):
        # We cannot call pmtiles_info on a remote URL without fetching it.
        # Report tile_type as "unknown" for the url mode; the Task-5 HTML builder
        # can default to "vector" or the caller can supply a style.
        return {"mode": "url", "url": data, "tile_type": "unknown", "vector_layer_names": []}

    # Path on disk.
    if isinstance(data, str):
        with open(data, "rb") as f:
            raw = f.read()
        info = pmtiles_info(raw)
        return {
            "mode": "embed",
            "bytes": raw,
            "tile_type": info["tile_type"],
            "vector_layer_names": _extract_vector_layer_names(info.get("metadata", {})),
        }

    # Already bytes.
    if isinstance(data, (bytes, bytearray)):
        raw = bytes(data)
        info = pmtiles_info(raw)
        return {
            "mode": "embed",
            "bytes": raw,
            "tile_type": info["tile_type"],
            "vector_layer_names": _extract_vector_layer_names(info.get("metadata", {})),
        }

    raise TypeError(
        f"_resolve_pmtiles_bytes_or_url: unsupported data type "
        f"{type(data).__name__!r}; expected str path, http(s) URL, or bytes"
    )


def _is_raster_tile_type(tile_type: str) -> bool:
    return tile_type.lower() in ("png", "jpeg", "webp", "avif")


def _pmtiles(layer, idx: int) -> tuple[dict, list[dict], int]:
    sid = f"gbx{idx}"
    info = _resolve_pmtiles_bytes_or_url(layer)
    tile_type = info.get("tile_type", "unknown")
    is_raster = _is_raster_tile_type(tile_type)

    src: dict[str, Any] = {
        sid: {
            "type": "raster" if is_raster else "vector",
            "url": f"pmtiles://{sid}",
        }
    }
    # Sidecar consumed (and popped) by the Task-5 HTML builder.
    src[sid]["_gbx_pmtiles"] = info

    if is_raster:
        layers: list[dict] = [
            {
                "id": f"{sid}-raster",
                "type": "raster",
                "source": sid,
                "paint": {"raster-opacity": layer.opacity if layer.opacity is not None else 1.0},
            }
        ]
    else:
        # Derive the source-layer name from the archive's TileJSON metadata.
        # `vector_layer_names` lists ids in declaration order; use the first.
        # Fall back to "buildings" only when metadata carries no layer names
        # (e.g. url-mode archives that cannot be pre-inspected).
        vector_names = info.get("vector_layer_names", [])
        source_layer = vector_names[0] if vector_names else "buildings"
        layers = [
            {
                "id": f"{sid}-fill",
                "type": "fill",
                "source": sid,
                "source-layer": source_layer,
                "paint": {
                    "fill-color": layer.color or "#c33",
                    "fill-opacity": layer.opacity if layer.opacity is not None else 0.5,
                },
            }
        ]

    embed_bytes = len(info["bytes"]) if info["mode"] == "embed" else 0
    return src, layers, embed_bytes
