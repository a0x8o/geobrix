"""Phase-1.5 dynamic zoom cut-over: AnyWidget-based MapLibre viewer.

``plot_interactive_dynamic`` embeds a low-zoom (min_z..max_z) PMTiles
overview as the base MapLibre source (exactly like ``build_html`` does).
When the user pans/zooms above ``max_z``, the JS side fires a
``model.send({bbox, zoom})`` message; the Python ``on_msg`` handler calls
``on_viewport(bbox, zoom)``, base64s the returned PMTiles bytes into the
``detail`` synced trait, and the JS ``change:detail`` handler swaps the
detail source into the live map.

The comm round-trip (``model.send`` → ``on_msg`` → trait → ``change``
handler) is proven on Serverless by Spike B and is not unit-testable
headlessly.

Helper:
  ``_viewport_payload(bbox, zoom, max_z)`` → payload dict when ``zoom > max_z``,
  else ``None`` — the seam gate used by both the JS and the Python handler.
"""

from __future__ import annotations

import base64
import json
from typing import Any, Callable, Optional

import traitlets

import anywidget

from databricks.labs.gbx.vizx._maplibre import (
    _CARTO_STYLE,
    _MAPLIBRE_CSS,
    _MAPLIBRE_CSS_SRI,
    _MAPLIBRE_JS,
    _MAPLIBRE_JS_SRI,
    _PMTILES_JS,
    _PMTILES_JS_SRI,
    _json_for_script,
    layer_to_sources_layers,
)
from databricks.labs.gbx.vizx._simplify import normalize_spec, simplify_tiles_from_source


# ---------------------------------------------------------------------------
# Seam-gate helper
# ---------------------------------------------------------------------------


def _viewport_payload(
    bbox: list[float],
    zoom: float,
    max_z: int,
) -> Optional[dict]:
    """Return a payload dict when ``zoom > max_z``, else ``None``.

    This is the seam gate: at or below ``max_z`` the overview archive is
    sufficient and no viewport tiling is triggered; above ``max_z`` the
    caller should tile the current viewport.

    Args:
        bbox:  ``[west, south, east, north]`` in lon/lat degrees.
        zoom:  Current map zoom level (float).
        max_z: The overview max zoom (from the simplify_tiles_spec).

    Returns:
        ``{"bbox": bbox, "zoom": zoom}`` when ``zoom > max_z``, else ``None``.
    """
    if zoom > max_z:
        return {"bbox": bbox, "zoom": zoom}
    return None


# ---------------------------------------------------------------------------
# Default on_viewport implementation
# ---------------------------------------------------------------------------


def _default_on_viewport(
    source,
    max_z: int,
    spec: dict,
) -> Callable[[list, float], bytes]:
    """Return a closure that tiles the current viewport from *source*.

    The viewport is tiled via ``simplify_tiles_from_source`` using
    ``min_z=max_z+1`` so the detail level is always above the overview seam.

    Args:
        source: The original data source (GeoDataFrame, path, etc.) to tile from.
        max_z:  The overview max zoom — detail starts at ``max_z + 1``.
        spec:   The base simplify spec; min_z is overridden to ``max_z + 1``.

    Returns:
        A callable ``(bbox, zoom) -> bytes`` that runs tippecanoe for the
        requested viewport at ``min_z=max_z+1, max_z=zoom`` (clamped to
        ``spec["max_z"]`` if the caller's spec has a lower ceiling).
    """

    def _tile_viewport(bbox: list[float], zoom: float) -> bytes:
        # bbox is currently unused: full-source re-tile at min_z=max_z+1 (no
        # viewport clipping yet).  Kept in signature for future viewport-clipping.
        detail_min_z = max_z + 1
        detail_max_z = max(int(zoom), detail_min_z)
        viewport_spec = dict(spec)
        viewport_spec["min_z"] = detail_min_z
        viewport_spec["max_z"] = detail_max_z
        return simplify_tiles_from_source(source, spec=viewport_spec)

    return _tile_viewport


# ---------------------------------------------------------------------------
# ESM builder
# ---------------------------------------------------------------------------


def _build_esm(
    overview_sources_json: str,
    overview_layers_json: str,
    pm_register_js: str,
    max_z: int,
    center: list,
    zoom: int,
) -> str:
    """Build the AnyWidget ``_esm`` JavaScript module.

    The module:
    1. Loads MapLibre and pmtiles via CDN ``<script>`` tags injected into
       ``document.head`` (anywidget cannot use external ``<link>``/``<script>``
       in the ESM directly, but injecting into the head works on Databricks).
    2. Creates the MapLibre map with the embedded overview.
    3. Registers a ``moveend`` handler that, when ``map.getZoom() > max_z``,
       calls ``model.send({bbox, zoom})``.
    4. Listens on ``change:detail`` and adds/replaces a ``detail`` source +
       layer from the base64-encoded PMTiles archive in the trait.

    All JSON embedded in the script uses ``_json_for_script`` escaping to
    prevent breakout through ``</script>`` or ``<``/``>``/``&`` in data.

    Args:
        overview_sources_json: JSON string of the MapLibre sources dict.
        overview_layers_json:  JSON string of the MapLibre layers list.
        pm_register_js:        JS snippet(s) that register PMTiles sources
                               with the pmtiles protocol (from ``_maplibre``).
        max_z:                 The overview seam — JS fires only above this.
        center:                ``[lon, lat]`` map centre.
        zoom:                  Initial zoom level.
    """
    center_json = json.dumps(center)
    max_z_js = int(max_z)
    zoom_js = int(zoom)

    return f"""\
function _gbxLoadScript(src, integrity) {{
  return new Promise((resolve, reject) => {{
    if (document.querySelector('script[src="' + src + '"]')) {{ resolve(); return; }}
    const s = document.createElement('script');
    s.src = src;
    if (integrity) {{ s.integrity = integrity; s.crossOrigin = 'anonymous'; }}
    s.onload = resolve;
    s.onerror = reject;
    document.head.appendChild(s);
  }});
}}
function _gbxLoadCss(href, integrity) {{
  if (document.querySelector('link[href="' + href + '"]')) return;
  const l = document.createElement('link');
  l.rel = 'stylesheet'; l.href = href;
  if (integrity) {{ l.integrity = integrity; l.crossOrigin = 'anonymous'; }}
  document.head.appendChild(l);
}}

async function render({{ model, el }}) {{
  // Load MapLibre + pmtiles from SRI-pinned CDN.
  _gbxLoadCss({json.dumps(_MAPLIBRE_CSS)}, {json.dumps(_MAPLIBRE_CSS_SRI)});
  await _gbxLoadScript({json.dumps(_MAPLIBRE_JS)}, {json.dumps(_MAPLIBRE_JS_SRI)});
  await _gbxLoadScript({json.dumps(_PMTILES_JS)}, {json.dumps(_PMTILES_JS_SRI)});

  const mapDiv = document.createElement('div');
  mapDiv.style.cssText = 'height:480px;width:100%;';
  el.appendChild(mapDiv);

  // Register pmtiles protocol.
  const proto = new pmtiles.Protocol();
  maplibregl.addProtocol('pmtiles', proto.tile.bind(proto));

  // Register the embedded overview archive(s) with the protocol.
  {pm_register_js}

  // Build the map.
  const map = new maplibregl.Map({{
    container: mapDiv,
    style: {json.dumps(_CARTO_STYLE)},
    center: {center_json},
    zoom: {zoom_js},
  }});

  const overviewSources = {overview_sources_json};
  const overviewLayers = {overview_layers_json};

  map.on('load', () => {{
    for (const [sid, sdef] of Object.entries(overviewSources)) {{
      map.addSource(sid, sdef);
    }}
    for (const ly of overviewLayers) {{
      map.addLayer(ly);
    }}
  }});

  // moveend: fire model.send when zoom > max_z (above the seam).
  const MAX_Z = {max_z_js};
  map.on('moveend', () => {{
    const z = map.getZoom();
    if (z > MAX_Z) {{
      const b = map.getBounds();
      model.send({{
        bbox: [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()],
        zoom: z,
      }});
    }}
  }});

  // change:detail: add or replace the detail source from the synced trait.
  model.on('change:detail', () => {{
    const b64 = model.get('detail');
    if (!b64) return;
    try {{
      const raw = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
      const pm = new pmtiles.PMTiles(new pmtiles.FileSource(
        new File([raw.buffer], 'detail.pmtiles')
      ));
      proto.add(pm);
      if (map.getSource('gbx_detail')) {{
        map.removeLayer('gbx_detail-fill');
        map.removeSource('gbx_detail');
      }}
      map.addSource('gbx_detail', {{ type: 'vector', url: 'pmtiles://gbx_detail' }});
      pm.getHeader().then(h => {{
        const meta = h.metadata || {{}};
        const vl = (meta.vector_layers || []);
        const srcLayer = vl.length > 0 ? vl[0].id : 'buildings';
        map.addLayer({{
          id: 'gbx_detail-fill',
          type: 'fill',
          source: 'gbx_detail',
          'source-layer': srcLayer,
          paint: {{ 'fill-color': '#3388ff', 'fill-opacity': 0.5 }},
        }});
      }}).catch(() => {{
        // Raster PMTiles — try as a raster layer instead.
        if (map.getSource('gbx_detail')) {{
          map.removeSource('gbx_detail');
        }}
        map.addSource('gbx_detail', {{ type: 'raster', url: 'pmtiles://gbx_detail' }});
        map.addLayer({{
          id: 'gbx_detail-fill',
          type: 'raster',
          source: 'gbx_detail',
          paint: {{ 'raster-opacity': 0.8 }},
        }});
      }});
    }} catch(e) {{
      console.warn('[gbx dynamic] failed to load detail tiles:', e);
    }}
  }});
}}
export default {{ render }};
"""


# ---------------------------------------------------------------------------
# Widget class
# ---------------------------------------------------------------------------


def _build_dynamic_widget(
    layers,
    *,
    simplify_tiles_spec: Optional[dict],
    on_viewport: Optional[Callable],
    center: Optional[list],
    zoom: Optional[int],
) -> anywidget.AnyWidget:
    """Internal factory — builds the AnyWidget subclass and wires the comm."""
    from databricks.labs.gbx.vizx._layers import as_layers
    from databricks.labs.gbx.vizx._maplibre import _pmtiles_register_js

    lyrs = as_layers(layers)
    spec = normalize_spec(simplify_tiles_spec)
    max_z = spec["max_z"]
    _center = center if center is not None else [-122.43, 37.77]
    _zoom = zoom if zoom is not None else 11

    # Build the overview PMTiles archive from each layer via simplify_tiles_from_source,
    # then run layer_to_sources_layers on the resulting pmtiles_layer so the HTML
    # builder path (sources/layers JSON + pm sidecar) is reused verbatim.
    from databricks.labs.gbx.vizx._layers import pmtiles_layer as _pmtiles_layer

    overview_sources: dict = {}
    overview_layers: list = []
    pm_register_snippets: list[str] = []
    # Keep a reference to the first vector/raster source for the default on_viewport.
    first_source = None

    for idx, layer in enumerate(lyrs):
        kind = getattr(layer, "kind", None)
        if kind in ("vector", "grid"):
            if first_source is None:
                from databricks.labs.gbx.vizx._maplibre import _gdf_for
                first_source = _gdf_for(layer).to_crs(4326)
            overview_bytes = simplify_tiles_from_source(
                first_source if first_source is not None else layer.data,
                spec=spec,
            )
            pm_layer = _pmtiles_layer(overview_bytes, label=layer.label)
        elif kind == "pmtiles":
            # Already a PMTiles layer — embed it as-is (no re-tiling).
            pm_layer = layer
        else:
            # Raster: simplify via the raster path.
            overview_bytes = simplify_tiles_from_source(layer.data, spec=spec)
            pm_layer = _pmtiles_layer(overview_bytes, label=layer.label)

        entry = layer_to_sources_layers(pm_layer, idx)
        s, ls, _eb = entry
        for sid, sdef in s.items():
            if "_gbx_pmtiles" in sdef:
                pm_info = sdef["_gbx_pmtiles"]
                clean = {k: v for k, v in sdef.items() if k != "_gbx_pmtiles"}
                overview_sources[sid] = clean
                pm_register_snippets.append(_pmtiles_register_js(sid, pm_info))
            else:
                overview_sources[sid] = sdef
        overview_layers.extend(ls)

    # Escape overview sources/layers for safe JS embedding.
    overview_sources_json = _json_for_script(overview_sources)
    overview_layers_json = _json_for_script(overview_layers)
    pm_register_js = "".join(pm_register_snippets)

    esm = _build_esm(
        overview_sources_json,
        overview_layers_json,
        pm_register_js,
        max_z=max_z,
        center=_center,
        zoom=_zoom,
    )

    # Resolve the on_viewport callback (default: tile from first source).
    if on_viewport is not None:
        _callback = on_viewport
    else:
        _callback = _default_on_viewport(first_source, max_z, spec)

    # Build the AnyWidget subclass dynamically (traitlets require class-level
    # declarations; we create a one-shot class here to carry the ESM and traits).
    _esm_val = esm
    _max_z_val = max_z

    class _GbxDynamicWidget(anywidget.AnyWidget):
        # Plain str — anywidget sees has_trait("_esm") == False and registers it
        # as a sync=True trait automatically.  A traitlets.Unicode declaration
        # here causes anywidget to skip that step → _esm never reaches the JS
        # frontend and the widget renders blank.
        _esm = _esm_val
        detail = traitlets.Unicode("").tag(sync=True)

    widget = _GbxDynamicWidget()

    def _handle_msg(widget_instance, content, buffers=None):
        """Python-side handler for JS model.send({bbox, zoom}) messages."""
        if not isinstance(content, dict):
            return
        bbox = content.get("bbox")
        zoom = content.get("zoom")
        if bbox is None or zoom is None:
            return
        # Seam gate: only act when zoom > max_z.
        if _viewport_payload(bbox=bbox, zoom=zoom, max_z=_max_z_val) is None:
            return
        result = _callback(bbox, zoom)
        if isinstance(result, (bytes, bytearray)):
            widget_instance.detail = base64.b64encode(bytes(result)).decode("ascii")

    widget.on_msg(_handle_msg)
    # Expose the handler under a test-accessible name for direct invocation in tests.
    widget._gbx_handle_msg = lambda content: _handle_msg(widget, content)

    return widget


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def plot_interactive_dynamic(
    layers,
    *,
    simplify_tiles_spec: Optional[dict] = None,
    on_viewport: Optional[Callable] = None,
    center: Optional[list] = None,
    zoom: Optional[int] = None,
    **kw: Any,
) -> anywidget.AnyWidget:
    """Render an AnyWidget-based dynamic zoom cut-over map.

    Embeds a simplified ``min_z..max_z`` PMTiles overview as the base MapLibre
    source.  When the user pans or zooms above ``max_z``, the widget's JS side
    sends the current viewport ``{bbox, zoom}`` to the Python kernel via
    ``model.send``; the Python ``on_msg`` handler calls ``on_viewport(bbox,
    zoom)``, base64s the returned PMTiles bytes into the ``detail`` synced
    trait, and the JS ``change:detail`` handler adds/replaces a detail source
    in the live map.

    The comm round-trip is proven on Serverless by Spike B and requires a live
    kernel (notebook / Databricks interactive).  The widget does not render
    meaningfully in a static GitHub view — use ``plot_interactive`` for a
    static-fallback-compatible view.

    Args:
        layers:               A list of :class:`~databricks.labs.gbx.vizx._layers.Layer`
                              or any input accepted by
                              :func:`~databricks.labs.gbx.vizx._layers.as_layers`.
        simplify_tiles_spec:  Optional spec dict (see
                              :func:`~databricks.labs.gbx.vizx._simplify.normalize_spec`).
                              ``max_z`` controls the seam between the overview
                              and the on-demand detail tier (default ``10``).
        on_viewport:          Optional ``(bbox: list[float], zoom: float) -> bytes``
                              callback.  Receives ``[west, south, east, north]``
                              and the current zoom; must return PMTiles bytes.
                              Default: tiles the first vector/grid/raster layer
                              from source via ``simplify_tiles_from_source`` at
                              ``min_z=max_z+1``.
        center:               ``[lon, lat]`` map centre (default
                              ``[-122.43, 37.77]``).
        zoom:                 Initial zoom level (default ``11``).
        **kw:                 Reserved for future keyword arguments (ignored).

    Returns:
        An :class:`anywidget.AnyWidget` instance.  Display it in a Databricks
        notebook cell by returning it as the last expression, or pass it to
        ``IPython.display.display(w)``.
    """
    return _build_dynamic_widget(
        layers,
        simplify_tiles_spec=simplify_tiles_spec,
        on_viewport=on_viewport,
        center=center,
        zoom=zoom,
    )
