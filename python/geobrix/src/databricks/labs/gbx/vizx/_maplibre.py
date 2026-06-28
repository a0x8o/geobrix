"""MapLibre GL per-layer adapters for the VizX interactive compositor.

``layer_to_sources_layers(layer, idx)`` converts one :class:`~databricks.labs.gbx.vizx._layers.Layer`
into the MapLibre GL ``sources`` dict, ``layers`` list, and ``embed_bytes`` integer
that ``build_html`` (Task 5) stitches together into a self-contained HTML viewer.

Dispatch by ``layer.kind``:

* ``"vector"`` / ``"grid"`` — inline ``geojson`` source reprojected to EPSG:4326;
  fill/line/circle sub-layers chosen by geometry type.
* ``"raster"`` — ``image`` source with 4-corner ``coordinates`` in lon/lat;
  PNG rendered via rasterio (decimated to ≤ ``raster_max_px``).
* ``"pmtiles"`` — ``raster|vector`` source with ``pmtiles://gbx{idx}`` URL plus a
  ``_gbx_pmtiles`` sidecar dict recording embed mode or remote URL; consumed and
  popped by the Task-5 HTML builder.
"""

from __future__ import annotations

import base64
import io
import json
from typing import Any

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


def _resolve_pmtiles_bytes_or_url(layer) -> dict:
    """Return a sidecar info dict for the pmtiles layer.

    Returns one of:
    - ``{"mode": "url", "url": <str>, "tile_type": <str>}`` — when
      ``layer.data`` is an ``http(s)://`` URL (no local bytes needed).
    - ``{"mode": "embed", "bytes": <bytes>, "tile_type": <str>}`` — when
      ``layer.data`` is a path or bytes archive; ``pmtiles_info`` is called
      to detect the tile type.
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
        return {"mode": "url", "url": data, "tile_type": "unknown"}

    # Path on disk.
    if isinstance(data, str):
        with open(data, "rb") as f:
            raw = f.read()
        info = pmtiles_info(raw)
        return {"mode": "embed", "bytes": raw, "tile_type": info["tile_type"]}

    # Already bytes.
    if isinstance(data, (bytes, bytearray)):
        raw = bytes(data)
        info = pmtiles_info(raw)
        return {"mode": "embed", "bytes": raw, "tile_type": info["tile_type"]}

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
        layers = [
            {
                "id": f"{sid}-fill",
                "type": "fill",
                "source": sid,
                "source-layer": "buildings",
                "paint": {
                    "fill-color": layer.color or "#c33",
                    "fill-opacity": layer.opacity if layer.opacity is not None else 0.5,
                },
            }
        ]

    embed_bytes = len(info["bytes"]) if info["mode"] == "embed" else 0
    return src, layers, embed_bytes
