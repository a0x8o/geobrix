# VizX Multi-Layer Viewer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give VizX a unified, simple-for-users way to render any combination of vector, raster, and grid layers in one notebook map — a halo capability for "I have more data than a notebook cell can hold."

**Architecture:** A small `Layer` abstraction (vector/raster/grid/pmtiles) consumed by two renderers — `plot_static` (matplotlib, multi-layer on one `Axes`) and `plot_interactive` (one self-contained **MapLibre GL** page; folium retired). A `>64 MB` ladder (URL → embed → simplify → static) keeps it honest at scale, with `simplify_tiles_from_source` / `simplify_tiles_from_archive` (tippecanoe / GeoBrix-distributed / rasterio) producing budget-bounded overviews. A Phase-1.5 follow-on adds a dynamic zoom cut-over (embed z0–10, stream z11+ on pan/zoom via an AnyWidget JS↔kernel channel — proven on Serverless by Spike B).

**Tech Stack:** Python 3.12, `databricks.labs.gbx.vizx` (Python-only, no Scala/heavy changes), MapLibre GL JS + pmtiles.js (SRI-pinned), matplotlib + contextily (static), tippecanoe (PyPI manylinux wheel, vector simplify), rasterio (raster overviews), anywidget (Phase-1.5), pytest + Docker doc-tests.

## Global Constraints

- **One canonical name per concept** (beta = no aliases): `geom_col` (vector geometry), `cellid_col` (DGGS cell id; auto-detect via `_CELL_COL_CANDIDATES = ("cellid","cell","cell_id","h3","quadbin","bng","index")`), `column` (value to color/symbolize by). Do NOT rename `plot_static`'s existing `column`.
- **Interactive engine is MapLibre GL only.** folium is **retired** and removed from the `[vizx]` extra. `plot_interactive` no longer uses folium.
- **Python-only.** No Scala/heavy-tier or new-Spark-function changes. VizX runs driver-side.
- **Indefinite single-archive in a notebook is OUT (Phase-2/App).** Spike A: a MANAGED volume yields no presigned URL; Files-API `Range` is CORS-blocked. The `>64 MB` ladder's URL rung applies only to a user-supplied CORS-reachable `http(s)` URL (external-volume presign is parked).
- **No silent degradation.** Every reduction/simplification/fallback emits a loud, actionable warning.
- **Supply chain:** every execution-env dependency exact-version + hash-pinned (`--require-hashes`) in the `[vizx]` extra and the CI lock; the injected MapLibre/pmtiles.js use **Subresource Integrity** (`integrity="sha384-…" crossorigin="anonymous"`), not bare CDN tags.
- **Docs are executable doc-tests** (single-source rule): code lives in `docs/tests/python/`, runs in Docker, is imported by `.mdx`.
- **simplify engine policy:** tippecanoe (driver, moderate vector) / GeoBrix distributed tiling (large vector) / rasterio overviews (raster). tippecanoe is VizX viz plumbing — NOT a product tiler; GeoBrix's own tiling remains the product story.
- **Default basemap:** CARTO Positron (hosted), configurable, `none` option; must work under normal Serverless conditions. contextily retained for the static path.

---

## File Structure

**New:**
- `python/geobrix/src/databricks/labs/gbx/vizx/_layers.py` — `Layer` dataclass + `vector_layer`/`raster_layer`/`grid_layer`/`pmtiles_layer` constructors + `as_layers()` coercion (bare input → one Layer).
- `python/geobrix/src/databricks/labs/gbx/vizx/_maplibre.py` — per-layer → MapLibre sources/layers adapters; self-contained HTML builder (SRI-pinned JS, CARTO basemap); the `>64 MB` ladder.
- `python/geobrix/src/databricks/labs/gbx/vizx/_simplify.py` — `simplify_tiles_spec` schema/validation; `simplify_tiles_from_source`; `simplify_tiles_from_archive`; engine policy.
- `python/geobrix/src/databricks/labs/gbx/vizx/_dynamic.py` — Phase-1.5 AnyWidget cut-over viewer.
- Tests: `python/geobrix/test/vizx/test_layers.py`, `test_maplibre.py`, `test_ladder.py`, `test_simplify.py`, `test_dynamic.py`.
- Docs: `docs/docs/api/vizx-layers.mdx`; doc-test code `docs/tests/python/api/vizx_layers.py`; diagram `resources/images/diagrams/vizx/vizx-layers.{svg,png}` + generator `resources/images/generators/vizx-layers.py`.

**Modified:**
- `vizx/__init__.py` — export new symbols; route `plot_static`/`plot_interactive` to accept layers.
- `vizx/_static_map.py` — accept a `Layer` list; reproject + draw in order.
- `vizx/_cog.py` — add `ax=` parameter.
- `vizx/_interactive.py` — re-implemented on MapLibre (folium removed) or reduced to a thin delegator to `_maplibre`.
- `vizx/_pmtiles.py` — `plot_pmtiles` delegates to `_maplibre` (single `pmtiles_layer`).
- `python/geobrix/pyproject.toml` (or setup) `[vizx]` extra + CI lock `requirements-*.in/.txt`.
- `notebooks/examples/helios/02. Visual Basemap (XYZ).ipynb`, `03. Analytical Core (COG + STAC).ipynb` — real overlays.
- `notebooks/examples/helios/README.md`, `docs/docs/notebooks/helios.mdx` — prose fix.
- `docs/docs/api/vizx.mdx` — multi-layer + ladder narrative (or link new page).
- `notebooks/examples/{eo-series,h3-rasterize,xview}/*` — audited/migrated to the new surface.

**Prerequisite (ops, not a code task):** refresh the staged light wheel at `/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.0-py3-none-any.whl` (current one predates `pmtiles_info` in `gbx.pmtiles`) before doc-test/notebook runs on that workspace. Track via `gbx:data:push-wheel`.

---

## Task 1: `Layer` model + constructors

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/vizx/_layers.py`
- Test: `python/geobrix/test/vizx/test_layers.py`

**Interfaces:**
- Produces: `@dataclass Layer(kind: str, data, *, geom_col=None, cellid_col=None, column=None, grid_system=None, grid_conf=None, cmap="viridis", opacity=None, color=None, width=None, fill=True, label=None, style=None, simplify=None, band=None)`; `kind ∈ {"vector","raster","grid","pmtiles"}`. Constructors `vector_layer`, `raster_layer`, `grid_layer`, `pmtiles_layer` return a `Layer`. `as_layers(obj) -> list[Layer]` coerces a `Layer`, a list of `Layer`, or a bare input (DataFrame/path/bytes/array) into `list[Layer]` (bare → one vector or raster layer inferred by type; a `str`/`bytes` ending `.pmtiles` or PMTiles magic → `pmtiles`).

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/vizx/test_layers.py
import pytest
from databricks.labs.gbx.vizx._layers import (
    Layer, vector_layer, raster_layer, grid_layer, pmtiles_layer, as_layers,
)

def test_constructors_set_kind_and_params():
    v = vector_layer("df", geom_col="geom", column="pop", label="cities")
    assert v.kind == "vector" and v.geom_col == "geom" and v.column == "pop" and v.label == "cities"
    g = grid_layer("df", grid_system="h3", cellid_col="h3", column="score")
    assert g.kind == "grid" and g.grid_system == "h3" and g.cellid_col == "h3" and g.column == "score"
    r = raster_layer("/x.tif", band=1, cmap="terrain")
    assert r.kind == "raster" and r.band == 1 and r.cmap == "terrain"
    p = pmtiles_layer("/x.pmtiles")
    assert p.kind == "pmtiles"

def test_grid_layer_requires_grid_system():
    with pytest.raises(TypeError):
        grid_layer("df")  # grid_system is keyword-required

def test_as_layers_coerces_single_and_list():
    v = vector_layer("df")
    assert as_layers(v) == [v]
    assert as_layers([v, v]) == [v, v]

def test_as_layers_bare_pmtiles_path():
    [lyr] = as_layers("/data/x.pmtiles")
    assert lyr.kind == "pmtiles"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_layers.py`
Expected: FAIL (ImportError: cannot import name 'Layer').

- [ ] **Step 3: Write minimal implementation**

```python
# python/geobrix/src/databricks/labs/gbx/vizx/_layers.py
"""Layer model for the unified VizX viewers (vector / raster / grid / pmtiles)."""
from dataclasses import dataclass, field
from typing import Any, Optional

_VALID = {"vector", "raster", "grid", "pmtiles"}


@dataclass
class Layer:
    kind: str
    data: Any
    geom_col: Optional[str] = None
    cellid_col: Optional[str] = None
    column: Optional[str] = None
    grid_system: Optional[str] = None
    grid_conf: Optional[dict] = None
    cmap: str = "viridis"
    opacity: Optional[float] = None
    color: Optional[str] = None
    width: Optional[float] = None
    fill: bool = True
    band: Optional[int] = None
    style: Optional[dict] = None
    simplify: Optional[dict] = None
    label: Optional[str] = None

    def __post_init__(self):
        if self.kind not in _VALID:
            raise ValueError(f"Layer.kind must be one of {_VALID}, got {self.kind!r}")


def vector_layer(data, *, geom_col=None, column=None, cmap="viridis", fill=True,
                 color=None, width=None, opacity=0.8, simplify=None, label=None):
    return Layer("vector", data, geom_col=geom_col, column=column, cmap=cmap, fill=fill,
                 color=color, width=width, opacity=opacity, simplify=simplify, label=label)


def raster_layer(data, *, band=None, cmap="viridis", opacity=1.0, label=None):
    return Layer("raster", data, band=band, cmap=cmap, opacity=opacity, label=label)


def grid_layer(data, *, grid_system, cellid_col=None, column=None, cmap="viridis",
               opacity=0.7, grid_conf=None, label=None):
    return Layer("grid", data, grid_system=grid_system, cellid_col=cellid_col, column=column,
                 cmap=cmap, opacity=opacity, grid_conf=grid_conf, label=label)


def pmtiles_layer(data, *, style=None, simplify=None, label=None):
    return Layer("pmtiles", data, style=style, simplify=simplify, label=label)


def _looks_pmtiles(obj) -> bool:
    if isinstance(obj, (bytes, bytearray)):
        return obj[:7] == b"PMTiles"
    if isinstance(obj, str):
        return obj.endswith(".pmtiles")
    return False


def as_layers(obj) -> list:
    """Coerce a Layer / list[Layer] / bare input into list[Layer]."""
    if isinstance(obj, Layer):
        return [obj]
    if isinstance(obj, (list, tuple)) and obj and all(isinstance(x, Layer) for x in obj):
        return list(obj)
    if _looks_pmtiles(obj):
        return [pmtiles_layer(obj)]
    # bare raster: a path to a known raster ext, ndarray, or tile struct -> raster; else vector.
    if isinstance(obj, str) and obj.lower().endswith((".tif", ".tiff", ".cog")):
        return [raster_layer(obj)]
    try:
        import numpy as np
        if isinstance(obj, np.ndarray):
            return [raster_layer(obj)]
    except ImportError:
        pass
    return [vector_layer(obj)]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_layers.py`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_layers.py python/geobrix/test/vizx/test_layers.py
git commit -m "feat(vizx): Layer model + vector/raster/grid/pmtiles constructors"
```

---

## Task 2: `plot_cog` gains `ax=` (static composition)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_cog.py` (`plot_cog`, `_render_cog`)
- Test: `python/geobrix/test/vizx/test_cog_ax.py`

**Interfaces:**
- Consumes: existing `plot_cog(path, *, band=None, max_pixels=2000, fig_w=10, fig_h=10, basemap=True, basemap_source=None, title=None, **kw)`.
- Produces: `plot_cog(..., ax=None)` — when `ax` is provided, draws onto it and returns it (no new figure, no `plt.show`); when `None`, behaves as today.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/vizx/test_cog_ax.py
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from databricks.labs.gbx.vizx._cog import plot_cog

def test_plot_cog_draws_on_provided_axes(tmp_path):
    # build a tiny 1-band GeoTIFF
    import numpy as np, rasterio
    from rasterio.transform import from_origin
    p = tmp_path / "x.tif"
    data = (np.arange(64, dtype="float32").reshape(8, 8))
    with rasterio.open(p, "w", driver="GTiff", height=8, width=8, count=1, dtype="float32",
                       crs="EPSG:3857", transform=from_origin(0, 8, 1, 1)) as ds:
        ds.write(data, 1)
    fig, ax = plt.subplots()
    n_before = len(ax.images)
    out = plot_cog(str(p), basemap=False, ax=ax)
    assert out is ax
    assert len(ax.images) > n_before  # drew onto the SAME axes
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_cog_ax.py`
Expected: FAIL (`plot_cog() got an unexpected keyword argument 'ax'`).

- [ ] **Step 3: Write minimal implementation**

In `_cog.py`, thread `ax` through `plot_cog` and `_render_cog`:

```python
def plot_cog(path, *, band=None, max_pixels=2000, fig_w=10, fig_h=10,
             basemap=True, basemap_source=None, title=None, ax=None, **kw):
    data, transform, crs = _decode_cog(path, band=band, max_pixels=max_pixels)  # existing decode
    return _render_cog(data, transform, crs=crs, fig_w=fig_w, fig_h=fig_h, title=title,
                       basemap=basemap, basemap_source=basemap_source, ax=ax)

def _render_cog(data, transform, *, crs, fig_w, fig_h, title, basemap, basemap_source, ax=None):
    import matplotlib.pyplot as plt
    from rasterio.plot import plotting_extent, show
    owns_fig = ax is None
    if owns_fig:
        _, ax = plt.subplots(1, figsize=(fig_w, fig_h))
    if data.shape[0] == 1:
        ax.imshow(data[0], extent=plotting_extent(data[0], transform), cmap="viridis")
    else:
        show(data, ax=ax, transform=transform)
    if basemap and crs is not None:
        try:
            import contextily as cx
            cx.add_basemap(ax, source=basemap_source, crs=crs)
        except Exception:
            pass
    if title:
        ax.set_title(title)
    return ax
```

Keep the existing decode helper name; only the rendering path adds `ax`.

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_cog_ax.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_cog.py python/geobrix/test/vizx/test_cog_ax.py
git commit -m "feat(vizx): plot_cog accepts ax= for static multi-layer overlay"
```

---

## Task 3: `plot_static(layers)` — matplotlib multi-layer compositor

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py` (`plot_static`)
- Test: `python/geobrix/test/vizx/test_static_layers.py`

**Interfaces:**
- Consumes: `as_layers` (Task 1); `Layer`; existing single-layer `plot_static` body (vector/grid via geopandas, reproject to 3857); `plot_cog(..., ax=)` (Task 2); existing `plot_raster`.
- Produces: `plot_static(layers, *, basemap=True, basemap_source=None, title=None, fig_w=10, fig_h=10, ax=None, **single_layer_kwargs)` — accepts a `Layer`/list/bare input; draws each layer in order on one `Axes` (reprojected to EPSG:3857); returns the `Axes`. The legacy keyword call `plot_static(df, column=..., geom_col=..., grid_system=...)` still works (coerced to one layer; the per-layer kwargs override the layer fields).

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/vizx/test_static_layers.py
import matplotlib; matplotlib.use("Agg")
import geopandas as gpd
from shapely.geometry import Point, Polygon
from databricks.labs.gbx.vizx._static_map import plot_static
from databricks.labs.gbx.vizx._layers import vector_layer

def _gdf(geoms):
    return gpd.GeoDataFrame({"v": range(len(geoms))}, geometry=geoms, crs="EPSG:4326")

def test_two_vector_layers_one_axes():
    pts = _gdf([Point(-122.4, 37.7), Point(-122.41, 37.72)])
    polys = _gdf([Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)])])
    ax = plot_static([vector_layer(polys, column="v"), vector_layer(pts, color="red")],
                     basemap=False)
    # both layers drew: at least one collection from polys + one from pts
    assert len(ax.collections) >= 2

def test_legacy_single_dataframe_call_still_works():
    pts = _gdf([Point(-122.4, 37.7)])
    ax = plot_static(pts, column="v", basemap=False)
    assert ax is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_static_layers.py`
Expected: FAIL (`test_two_vector_layers_one_axes` — a list of Layers isn't handled).

- [ ] **Step 3: Write minimal implementation**

Refactor `plot_static` so its current body becomes `_draw_one_layer(layer, ax, ...)` and the public function loops:

```python
def plot_static(layers, *, basemap=True, basemap_source=None, title=None,
                fig_w=10, fig_h=10, ax=None, **legacy):
    import matplotlib.pyplot as plt
    from databricks.labs.gbx.vizx._layers import as_layers, Layer
    lyrs = as_layers(layers)
    # legacy keyword overrides apply to a single coerced layer
    if legacy and len(lyrs) == 1:
        for k, v in legacy.items():
            if hasattr(lyrs[0], k):
                setattr(lyrs[0], k, v)
    owns = ax is None
    if owns:
        _, ax = plt.subplots(figsize=(fig_w, fig_h))
    for lyr in lyrs:
        _draw_one_layer(lyr, ax)         # reprojects to 3857, draws (existing logic per kind)
    if basemap:
        try:
            import contextily as cx
            cx.add_basemap(ax, source=basemap_source, crs="EPSG:3857")
        except Exception:
            pass
    if title:
        ax.set_title(title)
    return ax
```

`_draw_one_layer` dispatches by `lyr.kind`: `vector`/`grid` reuse the existing geopandas reproject+plot path (grid via the existing `_resolve_cells` + `_GRID_DISPATCH`, using `lyr.cellid_col`/`grid_system`/`column`); `raster` calls `plot_cog(lyr.data, band=lyr.band, basemap=False, ax=ax)` (Task 2) or `plot_raster(..., ax=ax)`. Preserve the existing 3857 reprojection so layers align.

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_static_layers.py`
Expected: PASS (2 tests). Also re-run the existing `test_static_map.py` to confirm no regression:
`bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_static_map.py` → PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_static_map.py python/geobrix/test/vizx/test_static_layers.py
git commit -m "feat(vizx): plot_static accepts a Layer list (matplotlib compositor)"
```

---

## Task 4: MapLibre per-layer adapters

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/vizx/_maplibre.py`
- Test: `python/geobrix/test/vizx/test_maplibre.py`

**Interfaces:**
- Consumes: `Layer` (Task 1); `as_gdf`/`grid_as_gdf`/`cells_as_gdf` from `vizx._vector`; `pmtiles_info` from `databricks.labs.gbx.pmtiles`.
- Produces: `layer_to_sources_layers(layer, idx) -> (sources: dict, layers: list[dict], embed_bytes: int)` — converts one `Layer` to MapLibre `sources` entries + `layers` entries and reports embed cost. Vector/grid → an inline `geojson` source + fill/line/circle layers keyed `f"gbx{idx}"`; raster → an `image` source with 4-corner `coordinates`; pmtiles → a source `{"type": "<raster|vector>", "url": "pmtiles://gbx{idx}"}` plus a sidecar dict recording the archive bytes/URL for the HTML builder.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/vizx/test_maplibre.py
import geopandas as gpd
from shapely.geometry import Polygon
from databricks.labs.gbx.vizx._maplibre import layer_to_sources_layers
from databricks.labs.gbx.vizx._layers import vector_layer

def test_vector_layer_becomes_geojson_source_and_fill_layer():
    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)])],
        crs="EPSG:4326",
    )
    sources, layers, embed = layer_to_sources_layers(vector_layer(gdf, column="v"), 0)
    assert "gbx0" in sources and sources["gbx0"]["type"] == "geojson"
    assert any(l["type"] in ("fill", "line", "circle") for l in layers)
    assert embed > 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_maplibre.py`
Expected: FAIL (ImportError).

- [ ] **Step 3: Write minimal implementation**

```python
# python/geobrix/src/databricks/labs/gbx/vizx/_maplibre.py
"""MapLibre GL compositor for plot_interactive — per-layer adapters + HTML builder."""
import json


def _gdf_for(layer):
    from databricks.labs.gbx.vizx import _vector
    if layer.kind == "grid":
        return _vector.cells_as_gdf(layer.data, cell_col=layer.cellid_col or "cellid")
    return _vector.as_gdf(layer.data) if not hasattr(layer.data, "geometry") else layer.data


def layer_to_sources_layers(layer, idx):
    sid = f"gbx{idx}"
    if layer.kind in ("vector", "grid"):
        gdf = _gdf_for(layer).to_crs(4326)
        gj = json.loads(gdf.to_json())
        src = {sid: {"type": "geojson", "data": gj}}
        geomtypes = {f["geometry"]["type"] for f in gj["features"]}
        layers = []
        if geomtypes & {"Polygon", "MultiPolygon"}:
            layers.append({"id": f"{sid}-fill", "type": "fill", "source": sid,
                           "paint": {"fill-color": layer.color or "#3388ff",
                                     "fill-opacity": layer.opacity or 0.5}})
        if geomtypes & {"LineString", "MultiLineString", "Polygon", "MultiPolygon"}:
            layers.append({"id": f"{sid}-line", "type": "line", "source": sid,
                           "paint": {"line-color": layer.color or "#1f6fb5",
                                     "line-width": layer.width or 1.0}})
        if geomtypes & {"Point", "MultiPoint"}:
            layers.append({"id": f"{sid}-circle", "type": "circle", "source": sid,
                           "paint": {"circle-color": layer.color or "#e04e2a",
                                     "circle-radius": 4}})
        return src, layers, len(json.dumps(gj).encode())
    if layer.kind == "raster":
        png_b64, corners = _raster_to_image(layer)  # helper below
        src = {sid: {"type": "image", "url": f"data:image/png;base64,{png_b64}",
                     "coordinates": corners}}
        return src, [{"id": f"{sid}-raster", "type": "raster", "source": sid,
                      "paint": {"raster-opacity": layer.opacity or 1.0}}], len(png_b64)
    if layer.kind == "pmtiles":
        # the HTML builder embeds/streams; record bytes/url via a sidecar attribute
        from databricks.labs.gbx.pmtiles import pmtiles_info
        info = _resolve_pmtiles_bytes_or_url(layer)  # {"mode","bytes"|"url","tile_type"}
        is_raster = "raster" in str(info["tile_type"]).lower() or "png" in str(info["tile_type"]).lower()
        src = {sid: {"type": "raster" if is_raster else "vector", "url": f"pmtiles://{sid}"}}
        layers = ([{"id": f"{sid}-raster", "type": "raster", "source": sid}] if is_raster
                  else [{"id": f"{sid}-fill", "type": "fill", "source": sid,
                         "source-layer": "buildings",
                         "paint": {"fill-color": layer.color or "#c33", "fill-opacity": 0.5}}])
        src[sid]["_gbx_pmtiles"] = info  # sidecar consumed by build_html
        return src, layers, (len(info["bytes"]) if info["mode"] == "embed" else 0)
    raise ValueError(layer.kind)
```

Add `_raster_to_image(layer)` (decimate via rasterio to ≤ `raster_max_px`, render to RGBA PNG, base64, return `(b64, [[ulx,uly],[urx,ury],[lrx,lry],[llx,lly]])` in lon/lat) and `_resolve_pmtiles_bytes_or_url(layer)` (path/bytes → read bytes + detect tile type via `pmtiles_info`; an `http(s)` URL → `{"mode":"url","url":...}`).

- [ ] **Step 4: Run test to verify it passes**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_maplibre.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_maplibre.py python/geobrix/test/vizx/test_maplibre.py
git commit -m "feat(vizx): MapLibre per-layer adapters (geojson/image/pmtiles)"
```

---

## Task 5: MapLibre self-contained HTML builder (multi-source, SRI-pinned, CARTO basemap)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_maplibre.py` (`build_html`)
- Test: `python/geobrix/test/vizx/test_maplibre.py` (add cases)

**Interfaces:**
- Consumes: `layer_to_sources_layers` (Task 4).
- Produces: `build_html(prepared: list[tuple[sources, layers, info]], *, basemap="carto-positron", center=None, zoom=None) -> str` — one HTML page that registers the `pmtiles://` protocol once, embeds each pmtiles archive as a base64 `FileSource` (or a `FetchSource(url)` for url-mode), merges all sources/layers into one MapLibre style on top of the basemap, and loads MapLibre/pmtiles.js via **SRI-pinned** `<script>` tags.

- [ ] **Step 1: Write the failing test**

```python
def test_build_html_is_self_contained_and_sri_pinned():
    import geopandas as gpd
    from shapely.geometry import Point
    from databricks.labs.gbx.vizx._maplibre import layer_to_sources_layers, build_html
    from databricks.labs.gbx.vizx._layers import vector_layer
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    prepared = [layer_to_sources_layers(vector_layer(gdf), 0)]
    html = build_html(prepared)
    assert "maplibregl.Map" in html
    assert 'integrity="sha384-' in html and 'crossorigin="anonymous"' in html
    assert "carto" in html.lower()           # basemap wired
    assert "gbx0" in html                     # the layer's source id present
```

- [ ] **Step 2: Run test to verify it fails** → FAIL (no `build_html`).
- [ ] **Step 3: Write minimal implementation**

```python
_MAPLIBRE_JS = "https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"
_MAPLIBRE_JS_SRI = "sha384-REPLACE_WITH_PINNED_HASH"   # pin in Task 12 against the locked version
_PMTILES_JS = "https://unpkg.com/pmtiles@3.2.0/dist/pmtiles.js"
_PMTILES_JS_SRI = "sha384-REPLACE_WITH_PINNED_HASH"
_CARTO_STYLE = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"

def build_html(prepared, *, basemap="carto-positron", center=None, zoom=None):
    sources, layers, pm = {}, [], []
    for i, (s, ls, *_rest) in enumerate(prepared):
        sources.update(s); layers.extend(ls)
        for sid, sdef in s.items():
            if "_gbx_pmtiles" in sdef:
                pm.append((sid, sdef.pop("_gbx_pmtiles")))
    base = f'"{_CARTO_STYLE}"' if basemap and basemap != "none" else "{version:8,sources:{},layers:[]}"
    overlay = json.dumps({"sources": sources, "layers": layers})
    pm_js = "".join(_pmtiles_register_js(sid, info) for sid, info in pm)
    return f"""
<div id="gbx-map" style="height:480px"></div>
<link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet"/>
<script src="{_MAPLIBRE_JS}" integrity="{_MAPLIBRE_JS_SRI}" crossorigin="anonymous"></script>
<script src="{_PMTILES_JS}" integrity="{_PMTILES_JS_SRI}" crossorigin="anonymous"></script>
<script>
  const proto = new pmtiles.Protocol(); maplibregl.addProtocol('pmtiles', proto.tile);
  {pm_js}
  const map = new maplibregl.Map({{container:'gbx-map', style:{base},
      center:{json.dumps(center or [-122.43, 37.77])}, zoom:{zoom or 11}}});
  const overlay = {overlay};
  map.on('load', () => {{
    for (const [sid, sdef] of Object.entries(overlay.sources)) map.addSource(sid, sdef);
    for (const ly of overlay.layers) map.addLayer(ly);
  }});
</script>"""

def _pmtiles_register_js(sid, info):
    if info["mode"] == "url":
        return f"proto.add(new pmtiles.PMTiles({json.dumps(info['url'])}));\n"
    import base64
    b64 = base64.b64encode(info["bytes"]).decode()
    return (f"const _b{sid}=Uint8Array.from(atob({json.dumps(b64)}),c=>c.charCodeAt(0));\n"
            f"proto.add(new pmtiles.PMTiles(new pmtiles.FileSource("
            f"new File([_b{sid}.buffer],'{sid}.pmtiles'))));\n")
```

(The two SRI placeholders are computed and pinned in Task 12 against the locked unpkg versions; until then the test asserts the *attribute shape* `sha384-`, which the literal satisfies.)

- [ ] **Step 4: Run test to verify it passes** → PASS.
- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_maplibre.py python/geobrix/test/vizx/test_maplibre.py
git commit -m "feat(vizx): self-contained multi-source MapLibre HTML builder"
```

---

## Task 6: The `>64 MB` ladder

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_maplibre.py` (`prepare_layers`)
- Test: `python/geobrix/test/vizx/test_ladder.py`

**Interfaces:**
- Consumes: `layer_to_sources_layers` (Task 4); `simplify_tiles_from_*`/spec (Tasks 8-10 — import lazily; Task 6 handles only rungs 1,2,4 and calls the simplify hook if present).
- Produces: `prepare_layers(layers, *, max_embed_mb=64, simplify_tiles_spec=None, fallback=True) -> dict` returning `{"mode": "interactive"|"static", "prepared": [...], "warnings": [...]}`. Order per layer: (1) pmtiles with explicit `http(s)` URL → url-stream (0 embed); (2) prepared bytes ≤ budget → embed; (3) `simplify_tiles_spec`/`layer.simplify` present → simplify to ≤ budget, embed; (4) else → `mode="static"`. **Budget authority:** the per-layer `embed_bytes` from Task 4 is a *heuristic* used only to choose which layer to shed/simplify (note: Task-4 raster reports base64 length, vector reports raw JSON, pmtiles reports raw archive bytes — not directly comparable). The actual budget gate is the **size of the assembled HTML** (`len(build_html(prepared).encode())`) vs `max_embed_mb` — measure the real payload, don't sum mixed per-layer figures. A finished pmtiles archive is never shrunk (only url or static get it past budget). Every reduction/fallback appends a loud warning. **When the static fallback runs, a pmtiles layer is decoded to an image/geometry via the existing `_static_raster_fallback` / `_static_vector_fallback` helpers in `_pmtiles.py` and handed to `plot_static` as a `raster_layer`/`vector_layer` — pmtiles layers are never silently dropped** (`plot_static` itself only handles vector/grid/raster and must warn, not skip, on a direct pmtiles layer).

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/vizx/test_ladder.py
import geopandas as gpd
from shapely.geometry import Point
from databricks.labs.gbx.vizx._maplibre import prepare_layers
from databricks.labs.gbx.vizx._layers import vector_layer, pmtiles_layer

def _small_gdf():
    return gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")

def test_under_budget_is_interactive():
    out = prepare_layers([vector_layer(_small_gdf())], max_embed_mb=64)
    assert out["mode"] == "interactive"

def test_oversize_pmtiles_without_url_or_spec_falls_back_to_static():
    big = pmtiles_layer(b"PMTiles" + b"\x03" + b"\x00" * (5 * 1024 * 1024))
    out = prepare_layers([big], max_embed_mb=1, fallback=True)
    assert out["mode"] == "static"
    assert any("static" in w.lower() for w in out["warnings"])
```

- [ ] **Step 2: Run test to verify it fails** → FAIL (no `prepare_layers`).
- [ ] **Step 3: Write minimal implementation** — implement the four-rung loop; for rung 3 call `_simplify_layer(layer, spec)` (defined in Task 9, imported lazily inside the function with a clear error if `[vizx]` simplify deps are missing); accumulate `embed_total`; if it exceeds budget and no mitigation succeeds and `fallback`, return `mode="static"` with a warning naming the offending layer + the three remedies (stage a URL / pre-tile / reduce AOI); if `fallback=False`, raise `ValueError`.
- [ ] **Step 4: Run test to verify it passes** → PASS.
- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_maplibre.py python/geobrix/test/vizx/test_ladder.py
git commit -m "feat(vizx): >64MB ladder (url->embed->simplify->static) with warnings"
```

---

## Task 7: `plot_interactive(layers)` on MapLibre — folium retired

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_interactive.py` (rewrite `plot_interactive`), `python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py` (`plot_pmtiles` delegates)
- Test: `python/geobrix/test/vizx/test_interactive_maplibre.py`

**Interfaces:**
- Consumes: `as_layers` (Task 1), `prepare_layers` + `build_html` (Tasks 5,6).
- Produces: `plot_interactive(layers, *, basemap="carto-positron", simplify_tiles_spec=None, max_embed_mb=64, fallback=True, center=None, zoom=None) -> str|None` — coerces input, runs the ladder; `mode="interactive"` → `build_html` + `displayHTML` (return the HTML string when not in a notebook); `mode="static"` → delegates to `plot_static(layers)`. `plot_pmtiles(path_or_bytes, **kw)` becomes `plot_interactive([pmtiles_layer(path_or_bytes)], **kw)`. **No `folium` import anywhere in vizx.**

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/vizx/test_interactive_maplibre.py
import geopandas as gpd
from shapely.geometry import Point
from databricks.labs.gbx.vizx._interactive import plot_interactive
from databricks.labs.gbx.vizx._layers import vector_layer

def test_interactive_returns_maplibre_html_for_layers():
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    html = plot_interactive([vector_layer(gdf)])
    assert "maplibregl.Map" in html

def test_no_folium_import_in_vizx():
    import importlib, pkgutil, databricks.labs.gbx.vizx as v
    for m in pkgutil.iter_modules(v.__path__):
        src = importlib.import_module(f"databricks.labs.gbx.vizx.{m.name}")
        assert "folium" not in (getattr(src, "__file__", "") or "")  # sanity; real check below
    # grep-style: no module imports folium
    import subprocess, os
    root = os.path.dirname(v.__file__)
    out = subprocess.run(["grep", "-rl", "import folium", root], capture_output=True, text=True)
    assert out.stdout.strip() == "", f"folium still imported in: {out.stdout}"
```

- [ ] **Step 2: Run test to verify it fails** → FAIL (current `plot_interactive` is folium; signature mismatch).
- [ ] **Step 3: Write minimal implementation** — rewrite `plot_interactive` per the interface; delete folium code paths; update `plot_pmtiles` to delegate. When `displayHTML` is available (notebook) call it and return `None`; else return the HTML (so tests can assert).
- [ ] **Step 4: Run test to verify it passes** → PASS. Re-run existing `test_pmtiles*` and confirm `plot_pmtiles` single-archive still renders.
- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_interactive.py python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py python/geobrix/test/vizx/test_interactive_maplibre.py
git commit -m "feat(vizx): plot_interactive on MapLibre (layers); retire folium"
```

---

## Task 8: `simplify_tiles_spec` schema + validation

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/vizx/_simplify.py` (schema part)
- Test: `python/geobrix/test/vizx/test_simplify.py`

**Interfaces:**
- Produces: `normalize_spec(spec: dict|None) -> dict` applying defaults `{"budget_mb":64,"min_z":0,"max_z":10,"tolerance":"auto","drop_densest":True,"cluster_distance":None,"keep_attrs":None,"raster_max_px":1024,"effort":"fast"}` and validating types/ranges (`min_z<=max_z`, `budget_mb>0`, `effort∈{"fast","full"}`); raises `ValueError` on bad input.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/vizx/test_simplify.py
import pytest
from databricks.labs.gbx.vizx._simplify import normalize_spec

def test_defaults_applied():
    s = normalize_spec(None)
    assert s["budget_mb"] == 64 and s["min_z"] == 0 and s["max_z"] == 10 and s["effort"] == "fast"

def test_override_and_validation():
    assert normalize_spec({"max_z": 12})["max_z"] == 12
    with pytest.raises(ValueError):
        normalize_spec({"min_z": 8, "max_z": 4})
    with pytest.raises(ValueError):
        normalize_spec({"effort": "turbo"})
```

- [ ] **Step 2: Run test to verify it fails** → FAIL (ImportError).
- [ ] **Step 3: Write minimal implementation** — `normalize_spec` merges defaults, validates, returns the dict.
- [ ] **Step 4: Run test to verify it passes** → PASS.
- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_simplify.py python/geobrix/test/vizx/test_simplify.py
git commit -m "feat(vizx): simplify_tiles_spec schema + validation"
```

---

## Task 9: `simplify_tiles_from_source`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_simplify.py`
- Test: `python/geobrix/test/vizx/test_simplify.py` (add)

**Interfaces:**
- Consumes: `normalize_spec` (Task 8); tippecanoe binary (PyPI wheel); rasterio (raster); GeoBrix pyvx tiling (large-vector branch — import lazily).
- Produces: `simplify_tiles_from_source(source, *, spec=None, out_path=None) -> bytes|str` — vector source (GeoDataFrame/GeoJSON/path): write GeoJSON to a temp file, run `tippecanoe -z{max_z} -Z{min_z} --maximum-tile-bytes {budget} [--drop-densest-as-needed] [--cluster-distance N] -o out.pmtiles in.geojson`, return bytes (or write to `out_path`). Raster source: rasterio overview downsample to `raster_max_px` → raster PMTiles. The very-large-vector distributed branch is gated behind an explicit `engine="distributed"` in `spec` (default driver/tippecanoe); log which engine ran.

- [ ] **Step 1: Write the failing test** (skips cleanly if tippecanoe absent so the suite is portable):

```python
import shutil, pytest
from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

@pytest.mark.skipif(shutil.which("tippecanoe") is None, reason="tippecanoe not installed")
def test_simplify_from_geojson_under_budget(tmp_path):
    import geopandas as gpd
    from shapely.geometry import Polygon
    gdf = gpd.GeoDataFrame(
        {"v": [1, 2]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                  Polygon([(2, 2), (3, 2), (3, 3), (2, 3)])],
        crs="EPSG:4326",
    )
    out = tmp_path / "o.pmtiles"
    p = simplify_tiles_from_source(gdf, spec={"max_z": 6, "budget_mb": 8}, out_path=str(out))
    assert out.exists() and out.read_bytes()[:7] == b"PMTiles"
```

- [ ] **Step 2: Run test to verify it fails** → FAIL (ImportError) or skip if tippecanoe absent (then implement + verify in Docker where the wheel installs).
- [ ] **Step 3: Write minimal implementation** — GeoDataFrame → `to_file(tmp.geojson, driver="GeoJSON")`; build the tippecanoe argv from the normalized spec; `subprocess.run(check=True)`; read bytes; raster branch via rasterio. Raise a clear error if tippecanoe is missing and `engine != "distributed"`.
- [ ] **Step 4: Run test to verify it passes** — run in Docker (`gbx:test:python`) where `[vizx]` (incl. tippecanoe) is installed: PASS.
- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_simplify.py python/geobrix/test/vizx/test_simplify.py
git commit -m "feat(vizx): simplify_tiles_from_source (tippecanoe / rasterio, budget-bounded)"
```

---

## Task 10: `simplify_tiles_from_archive`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_simplify.py`
- Test: `python/geobrix/test/vizx/test_simplify.py` (add)

**Interfaces:**
- Consumes: `normalize_spec` (Task 8); `tile-join` (ships with tippecanoe).
- Produces: `simplify_tiles_from_archive(pmtiles_path, *, spec=None, out_path=None) -> bytes|str` — `tile-join --maximum-zoom={max_z} --maximum-tile-bytes={budget} -o out.pmtiles in.pmtiles` (down-zoom + budget-trim an existing archive without re-tiling from source). Returns bytes or writes `out_path`.

- [ ] **Step 1: Write the failing test** (skip if `tile-join` absent; in Docker it's present):

```python
import shutil, pytest
from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source, simplify_tiles_from_archive

@pytest.mark.skipif(shutil.which("tile-join") is None, reason="tile-join not installed")
def test_archive_downzoom_trims(tmp_path):
    import geopandas as gpd
    from shapely.geometry import Polygon
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Polygon([(0,0),(1,0),(1,1),(0,1)])], crs="EPSG:4326")
    src = tmp_path / "full.pmtiles"
    simplify_tiles_from_source(gdf, spec={"max_z": 8}, out_path=str(src))
    out = tmp_path / "ov.pmtiles"
    simplify_tiles_from_archive(str(src), spec={"max_z": 4, "budget_mb": 4}, out_path=str(out))
    assert out.exists() and out.read_bytes()[:7] == b"PMTiles"
```

- [ ] **Step 2: Run test to verify it fails** → FAIL/skip.
- [ ] **Step 3: Write minimal implementation** — build the `tile-join` argv from the normalized spec; `subprocess.run(check=True)`.
- [ ] **Step 4: Run test to verify it passes** (Docker) → PASS.
- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_simplify.py python/geobrix/test/vizx/test_simplify.py
git commit -m "feat(vizx): simplify_tiles_from_archive (tile-join down-zoom/trim)"
```

---

## Task 11: Wire simplify into the ladder + budget-escalate the archive path

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_maplibre.py` (`_simplify_layer` rung-3 hook), `python/geobrix/src/databricks/labs/gbx/vizx/_simplify.py` (`simplify_tiles_from_archive` budget escalation)
- Test: `python/geobrix/test/vizx/test_ladder.py` (add), `python/geobrix/test/vizx/test_simplify.py` (escalation)

**Interfaces:**
- Consumes: `simplify_tiles_from_source`/`simplify_tiles_from_archive` (Tasks 9-10), `normalize_spec` (Task 8), `_decode_mvt_to_geoms`/`all_tiles` from `_pmtiles.py`.
- Produces: `_simplify_layer(layer, spec) -> Layer` — routes by layer input (source data → `from_source`; existing archive path → `from_archive`), returns a `pmtiles_layer` of the simplified bytes; used as ladder rung 3.

**Budget escalation for `simplify_tiles_from_archive` (NEW — makes the archive budget contract real):**
`tile-join` only trims by zoom, so a zoom-trimmed archive may still have tiles over `budget_mb`. When `budget_mb` is requested and the trim is insufficient, **escalate to a source re-tile**: (1) run the cheap `tile-join` zoom-trim; (2) inspect the trimmed archive's max tile byte size; (3) if a tile still exceeds `budget_mb * 1 MiB`, **decode the archive's highest-zoom (max_z) tiles to a GeoDataFrame** via `_decode_mvt_to_geoms` over `all_tiles` (geographic geoms from z/x/y), then call `simplify_tiles_from_source(gdf, spec=...)` (tippecanoe `--drop-densest-as-needed`) which enforces the byte budget. Replace the prior `warnings.warn("budget_mb ignored")` with: within-budget-after-trim → no warning; escalated → `warnings.warn("budget enforced by re-tiling decoded features — slower; overview-grade precision", UserWarning)`; raster/undecodable archive → keep zoom-trim + the existing ignored-budget warning (no features to re-tile). Escalation is on-overflow only (the cheap trim is the common path).

- [ ] **Step 1: Failing tests** — (a) ladder: an oversize vector layer + `simplify_tiles_spec` → `prepare_layers` returns `mode=="interactive"` with a "simplified" warning (skip if tippecanoe absent). (b) escalation: build a source archive whose tiles exceed a tiny `budget_mb`, `simplify_tiles_from_archive(archive, spec={"budget_mb":<tiny>, "max_z":...})` → assert the result's max tile size is now ≤ budget (i.e. it re-tiled from source, not just zoom-trimmed) and a UserWarning about re-tiling fired.
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** — the `from_archive` escalation in `_simplify.py` (decode→`from_source` on overflow; reuse `_decode_mvt_to_geoms`/`all_tiles`), and `_simplify_layer` in `_maplibre.py` (route source→`from_source`, archive→`from_archive`; return a `pmtiles_layer` of the bytes), called from `prepare_layers` rung 3.
- [ ] **Step 4: Run → PASS** (Docker, tippecanoe present). Full vizx suite.
- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_maplibre.py python/geobrix/src/databricks/labs/gbx/vizx/_simplify.py python/geobrix/test/vizx/test_ladder.py python/geobrix/test/vizx/test_simplify.py
git commit -m "feat(vizx): wire simplify into ladder; archive budget-escalates to source re-tile"
```

---

## Task 11b: Proactive embed-size audit + report (no surprises)

The 64 MB embed budget should never be a *surprise* fallback — the per-layer + total size and the
chosen path must be auditable up front, before (and as part of) rendering.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_maplibre.py` (`prepare_layers` returns an `audit`; add `audit_layers`), `_interactive.py` (`plot_interactive` reports the audit + a `dry_run` arg)
- Test: `python/geobrix/test/vizx/test_ladder.py` (add audit tests)

**Interfaces:**
- Consumes: `layer_to_sources_layers`, `build_html`, `prepare_layers` (Tasks 4-6,11).
- Produces:
  - `audit_layers(layers, *, max_embed_mb=64, simplify_tiles_spec=None) -> dict` — a DRY pre-flight (no `displayHTML`, no render): `{"layers":[{"label","kind","embed_bytes","max_tile_bytes"(archives only, else None)}], "total_embed_bytes", "max_embed_bytes", "fits": bool, "verdict": "embed"|"simplify"|"url"|"static"}`.
  - `prepare_layers(...)` adds the same `audit` dict to its return.
  - `plot_interactive(..., dry_run=False)`: always **prints a concise audit line** before rendering (e.g. `"[vizx] buildings 12.0MB + naip 40.0MB = 52.0MB ≤ 64MB → embedding inline"` or `"... 80MB > 64MB → simplifying"` / `"→ static fallback"`); `dry_run=True` returns the audit dict WITHOUT rendering.

**Budget clarity (document + enforce):** the audited **total assembled-HTML size** is the embed-budget authority (the thing that can surprise); `simplify_tiles_spec.budget_mb` is the tippecanoe **per-tile** cap (rename its schema doc-comment from "total archive ceiling" to "per-tile byte cap"), and the ladder's post-simplify HTML-total re-check (Task 6) is what guarantees the embed fits. The audit reports BOTH the total (vs `max_embed_mb`) and per-archive max tile size so neither is a surprise.

- [ ] **Step 1: Failing tests** — `audit_layers([small vector])` returns `fits=True`, `verdict="embed"`, with a `total_embed_bytes` and a per-layer entry; `plot_interactive([...], dry_run=True)` returns the audit dict and does NOT render (no HTML string with `maplibregl.Map`). An oversize case → `fits=False`, `verdict in {"simplify","static"}`.
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** — factor the size computation in `prepare_layers` into an `audit` dict (it already measures the assembled HTML size); expose `audit_layers`; have `plot_interactive` print the one-line summary and honor `dry_run`. Fix the `budget_mb` doc-comment in `_simplify.py` `normalize_spec` (per-tile, not total).
- [ ] **Step 4: Run → PASS.** Full vizx suite.
- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_maplibre.py python/geobrix/src/databricks/labs/gbx/vizx/_interactive.py python/geobrix/src/databricks/labs/gbx/vizx/_simplify.py python/geobrix/test/vizx/test_ladder.py
git commit -m "feat(vizx): proactive embed-size audit + report (audit_layers, dry_run)"
```

---

## Task 12: Exports, extras, SRI pinning, back-compat

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/__init__.py`; `python/geobrix/pyproject.toml` (`[vizx]` extra); CI lock `requirements-pyrx-ci.in` + recompiled hashed `.txt`; `_maplibre.py` (real SRI hashes); **`_pmtiles.py` + `test_pmtiles.py` — remove the now-dead `_build_pmtiles_html` + its duplicate/divergent `_MAPLIBRE_JS`/`_PMTILES_JS` constants (pinned `pmtiles@3.2.1` vs `_maplibre.py`'s `3.2.0`) since `plot_pmtiles` now delegates; consolidate ONE CDN pin + SRI in `_maplibre.py` and drop the ~8 `test_pmtiles` tests that exercised the old standalone builder (the delegation path is covered by the new tests).**
- Test: `python/geobrix/test/vizx/test_exports.py`

**Interfaces:**
- Produces: `__init__` exports `vector_layer, raster_layer, grid_layer, pmtiles_layer, simplify_tiles_from_source, simplify_tiles_from_archive` (plus the existing `plot_*`, `as_gdf`, etc.); `[vizx]` extra **adds** `tippecanoe`, `anywidget` and **removes** `folium`; the two SRI hashes in `_maplibre.py` are the real `sha384` of the locked MapLibre/pmtiles.js versions.

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/vizx/test_exports.py
import databricks.labs.gbx.vizx as v

def test_new_public_symbols_exported():
    for name in ("vector_layer", "raster_layer", "grid_layer", "pmtiles_layer",
                 "simplify_tiles_from_source", "simplify_tiles_from_archive"):
        assert hasattr(v, name), name
    assert name in v.__all__ if hasattr(v, "__all__") else True

def test_sri_hashes_are_real():
    from databricks.labs.gbx.vizx import _maplibre as m
    assert m._MAPLIBRE_JS_SRI.startswith("sha384-") and "REPLACE" not in m._MAPLIBRE_JS_SRI
    assert m._PMTILES_JS_SRI.startswith("sha384-") and "REPLACE" not in m._PMTILES_JS_SRI
```

- [ ] **Step 2: Run test to verify it fails** → FAIL.
- [ ] **Step 3: Write minimal implementation** — add imports + `__all__` entries; edit `[vizx]` extra (add tippecanoe/anywidget, drop folium); compute SRI: `curl -s <unpkg-url> | openssl dgst -sha384 -binary | openssl base64 -A` for each pinned version, paste as `sha384-<b64>`; update the CI `.in` and recompile the hashed `.txt` per the light-CI-lock procedure.
- [ ] **Step 4: Run test to verify it passes** → PASS. Also run `gbx:test:bindings` is N/A (vizx not in registered_functions), but run `gbx:lint:python --check`.
- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/__init__.py python/geobrix/pyproject.toml python/geobrix/requirements-pyrx-ci.in python/geobrix/requirements-pyrx-ci.txt python/geobrix/src/databricks/labs/gbx/vizx/_maplibre.py python/geobrix/test/vizx/test_exports.py
git commit -m "feat(vizx): export layer/simplify API; [vizx] +tippecanoe +anywidget -folium; pin SRI"
```

---

## Task 13: Phase-1.5 dynamic zoom cut-over (AnyWidget)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/vizx/_dynamic.py`
- Test: `python/geobrix/test/vizx/test_dynamic.py`

**Interfaces:**
- Consumes: `simplify_tiles_from_source`/`_archive` (overview build), `anywidget`, `normalize_spec`.
- Produces: `plot_interactive_dynamic(layers, *, simplify_tiles_spec=None, on_viewport=None, **kw) -> anywidget.AnyWidget` — embeds the simplified `min_z..max_z` overview as the base MapLibre source; the widget's `_esm` registers a `moveend` handler that, at `zoom > max_z`, calls `model.send({bbox, zoom})`; a Python `on_msg` handler invokes `on_viewport(bbox, zoom)` (default: tile the current viewport from the source via `simplify_tiles_from_source` with `min_z=max_z+1`), base64s the result into a synced trait, and the JS adds/updates a detail source on `change`. Comm contract proven by Spike B (`model.send` → `on_msg` → trait → `change`).

- [ ] **Step 1: Write the failing test** (logic-level; the browser comm is covered by Spike B, not unit-testable headlessly):

```python
# python/geobrix/test/vizx/test_dynamic.py
import pytest
anywidget = pytest.importorskip("anywidget")
from databricks.labs.gbx.vizx._dynamic import plot_interactive_dynamic, _viewport_payload
from databricks.labs.gbx.vizx._layers import vector_layer
import geopandas as gpd
from shapely.geometry import Point

def test_builds_widget_with_overview_and_esm():
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    w = plot_interactive_dynamic([vector_layer(gdf)], simplify_tiles_spec={"max_z": 8})
    assert isinstance(w, anywidget.AnyWidget)
    assert "moveend" in w._esm and "model.send" in w._esm

def test_viewport_payload_only_fires_above_seam():
    assert _viewport_payload(bbox=[-122.5,37.7,-122.4,37.8], zoom=12, max_z=10) is not None
    assert _viewport_payload(bbox=[-122.5,37.7,-122.4,37.8], zoom=9, max_z=10) is None
```

- [ ] **Step 2: Run test to verify it fails** → FAIL.
- [ ] **Step 3: Write minimal implementation** — the AnyWidget subclass with `_esm` (overview embedded via the Task-5 builder output; `moveend` → `model.send` at `zoom>max_z`; `change:detail` → add/replace detail source) + the Python `on_msg` handler + `_viewport_payload` gate. Default `on_viewport` tiles the viewport via `simplify_tiles_from_source`.
- [ ] **Step 4: Run test to verify it passes** → PASS. (End-to-end comm verified manually per Spike B; note that in the report.)
- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_dynamic.py python/geobrix/test/vizx/test_dynamic.py
git commit -m "feat(vizx): Phase-1.5 dynamic zoom cut-over (AnyWidget overview+stream)"
```

> Task 13 lands the **reactive** loop only (moveend → prepare current viewport → refresh). Predictive prefetch is the separate Task 13b below — build it on top once the reactive comm is proven.

---

## Task 13b: Predictive tile prefetch for the dynamic viewer

A polish layer on Task 13: render the initial viewport, then a background thread pre-prepares
adjacent tiles before the user pans/zooms to them, served from cache for instant response.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_dynamic.py` (cache + prefetch thread)
- Test: `python/geobrix/test/vizx/test_dynamic.py` (add)

**Interfaces:**
- Consumes: the Task-13 AnyWidget viewport callback + the viewport tiler (`simplify_tiles_from_source`/the per-viewport prepare).
- Produces: a bounded driver-side **LRU tile cache** keyed by `(z, x, y)`; a `threading`-based prefetch worker that, after each viewport request, prepares the **ring of adjacent parent tiles** at the current zoom (and optionally `z+1`) into the cache; cache-hit serving so a pan to a prepared neighbor returns with no re-tile. The on-demand path checks the cache first (hit → instant), miss → prepare + return + trigger neighbor prefetch.

**Constraints / guardrails:**
- **Bounded cache with an explicit evict policy** (configurable max entry count / total bytes) — prefetched tiles must not grow driver memory unbounded:
  - *Baseline:* **LRU by last access** — a tile the user panned away from stops being accessed, ages to least-recently-used, and is evicted when new tiles need room (so "move away → removed" is automatic and memory is bounded).
  - *Map-aware refinement (note, build after baseline):* when full, evict the tile **farthest from the current viewport center** rather than purely oldest, keeping the active neighborhood warm.
  - *Prefetch guard:* a speculatively-prefetched tile that was **never viewed** must be evictable **before** a viewed tile (viewed > prefetched-unviewed), so eager prefetch can't push out real history.
- Prefetch runs on a **background `threading` worker** (daemon), never blocking the comm callback; heavy tiling (tippecanoe subprocess) is fine off-thread on the Serverless driver.
- **Coalesce / cancel**: a rapid sequence of viewport changes should not pile up stale prefetch work — cancel or skip prefetch for viewports the user has already left.
- Start with **neighbor-ring** prefetch; pan-velocity direction prediction is a later refinement (note, don't build).
- No `spark.conf`/`.rdd` (Serverless-safe); cache is pure driver-side Python.

- [ ] **Step 1: Failing tests** — (a) a `_TileCache` LRU: insert beyond capacity evicts the oldest; get returns a hit/miss. (b) after a viewport request, the prefetch worker populates the neighbor-ring cache entries (test with a stub tiler + a join/flush on the worker so the test is deterministic — no real sleeps). (c) a second request for a prefetched neighbor is served from cache (the tiler is NOT called again).
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** the `_TileCache` (LRU, bounded), the prefetch worker (daemon thread, coalescing), and cache-first serving in the viewport callback. Make the worker test-injectable (pass the tiler + allow a synchronous flush) so tests are deterministic.
- [ ] **Step 4: Run → PASS.** Full vizx suite. (End-to-end pan-prefetch UX is verified manually like the rest of the dynamic tier.)
- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/vizx/_dynamic.py python/geobrix/test/vizx/test_dynamic.py
git commit -m "feat(vizx): predictive neighbor-ring tile prefetch for the dynamic viewer"
```

---

## Task 14: Red-carpet docs (`vizx-layers.mdx`) + executable doc-tests + diagram

**Files:**
- Create: `docs/docs/api/vizx-layers.mdx`; `docs/tests/python/api/vizx_layers.py`; `resources/images/generators/vizx-layers.py` + `resources/images/diagrams/vizx/vizx-layers.{svg,png}`
- Modify: `docs/sidebars.js` (add the page); `docs/docs/api/vizx.mdx` (link to the new page)
- Test: doc-tests via `gbx:test:python-docs`

**Interfaces:**
- Consumes: the full public API (Tasks 1-13).
- Produces: a narrative page teaching the problem + the ladder, with multi-layer static + interactive examples, the ephemeral-vs-durable story, honest scale guidance (link Helios NB04 sharding + note the App is the indefinite-single-archive path), and a decision-tree diagram. Doc code is real + asserted in `vizx_layers.py` and imported by the `.mdx` via raw-loader.

- [ ] **Step 1: Write the failing doc-test**

```python
# docs/tests/python/api/vizx_layers.py
def multilayer_static_example():
    import geopandas as gpd
    from shapely.geometry import Point, Polygon
    from databricks.labs.gbx.vizx import vector_layer, grid_layer, plot_static
    pts = gpd.GeoDataFrame({"v":[1]}, geometry=[Point(-122.4,37.7)], crs="EPSG:4326")
    ax = plot_static([vector_layer(pts, color="red")], basemap=False)
    assert ax is not None
    return ax

def simplify_durable_example(tmp_path):
    import shutil
    if shutil.which("tippecanoe") is None:
        return None
    import geopandas as gpd
    from shapely.geometry import Polygon
    from databricks.labs.gbx.vizx import simplify_tiles_from_source
    gdf = gpd.GeoDataFrame({"v":[1]}, geometry=[Polygon([(0,0),(1,0),(1,1),(0,1)])], crs="EPSG:4326")
    out = f"{tmp_path}/overview.pmtiles"
    simplify_tiles_from_source(gdf, spec={"max_z":6}, out_path=out)
    return out
```

- [ ] **Step 2: Run to verify it fails** → FAIL (until imports exist; here they do after Tasks 1-9, so this gates the docs land after code).
- [ ] **Step 3: Write the `.mdx`** importing those functions via `!!raw-loader!`, add the page to `sidebars.js`, link from `vizx.mdx`; generate the diagram (extend the diagram-generator pattern; render via the documented Chrome+PIL recipe into `resources/images/diagrams/vizx/`).
- [ ] **Step 4: Run doc-tests in Docker**

Run (via Task subagent): `bash scripts/commands/gbx-test-python-docs.sh --path docs/tests/python/api/vizx_layers.py --log vizx-layers-docs.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add docs/docs/api/vizx-layers.mdx docs/tests/python/api/vizx_layers.py docs/sidebars.js docs/docs/api/vizx.mdx resources/images/generators/vizx-layers.py resources/images/diagrams/vizx/
git commit -m "docs(vizx): red-carpet multi-layer + ladder page with executable examples"
```

---

## Task 14b: Capture real interactive screenshots + embed in docs/README/notebooks

Real screenshots of the interactive MapLibre viewer in action — so `vizx-layers.mdx`, the
`README.md`, and the notebooks (which render static by default) all *preview* the interactive
experience. **Runs AFTER Task 14 (owns `vizx-layers.mdx`) and Task 16 (finalizes the Helios
notebook cells)** so screenshots embed into finished files, not race them.

**Files:**
- Create: `resources/images/diagrams/vizx/screenshots/*.png` (the captures) + a small capture script (host-only, git-ignored or under `resources/images/generators/`).
- Modify: `docs/docs/api/vizx-layers.mdx`, `notebooks/examples/helios/README.md` (+ `docs/docs/notebooks/helios.mdx`), and the Helios notebooks NB01–04 (add a ~50%-width screenshot in a markdown cell directly ABOVE each conditional `plot_interactive`/`show_pmtiles` cell).

**Interfaces:** `plot_interactive([...])` returns the self-contained HTML string (Task 7); the chrome-devtools MCP renders + screenshots.

**Capture pipeline:**
- [ ] **Step 1 (de-risk PROTOTYPE):** build ONE small multi-layer interactive HTML via `plot_interactive([vector + raster + grid], …)` (return the HTML string; write to a temp `.html` under the repo so it's host-reachable). Via the chrome-devtools MCP: `new_page` → `navigate_page` to the `file://` URL → **`wait_for` the MapLibre canvas to actually paint** (wait on the rendered `.maplibregl-canvas` / a network-idle, NOT a fixed sleep) → `take_screenshot`. Read the PNG back and CONFIRM it shows a rendered map (not blank/loading). If headless can't paint the WebGL map even after waiting: fall back to (a) a real-browser capture, or (b) screenshot the static composite (`plot_static`) and label it as the static preview — and report the limitation. **Do not proceed to embedding until a non-blank capture is confirmed.**
- [ ] **Step 2:** produce the final captures — a full-size view for `vizx-layers.mdx`/README (the multi-layer overlay) and the per-notebook captures (one per notebook's interactive cell, content matching that notebook: NB01 buildings, NB02 buildings+NAIP, NB03 hillshade+buildings(+solar grid), NB04 a shard). Save under `resources/images/diagrams/vizx/screenshots/`.
- [ ] **Step 3:** embed — `vizx-layers.mdx` (a hero screenshot near the interactive section) + `README.md` + each Helios notebook (a markdown cell with the ~50%-width image immediately above the conditional interactive cell, captioned "Interactive view (INTERACTIVE_PLOTS=True)"). Use the repo-relative image path convention the other docs use.
- [ ] **Step 4:** verify the notebooks still parse (nbformat) + the cell-by-cell harness reaches its config ceiling unchanged; the mdx/README render references resolve. `grep` for internal vocab in any touched user-facing doc → none.
- [ ] **Step 5: Commit** the screenshots + the doc/notebook edits: `docs(vizx): real interactive viewer screenshots in docs, README, notebooks` (≤72; trailer `Co-authored-by: Isaac`).

**Note:** the capture script + headless render are HOST-only (Chrome). Screenshots need CDN + CARTO-basemap internet to paint; the host has it.

---

## Task 15: Audit, migrate, AND showcase the new viewer in the example notebooks

This task does two things per notebook: (a) migrate any usage the folium-retirement or
`plot_interactive` signature change would break, and (b) **actively showcase** the new multi-layer
interactive viewer on each notebook's *final* artifact — not just keep it from breaking. The
showcase is the point: these notebooks are where users learn the capability exists.

**Files:**
- Modify: `notebooks/examples/eo-series/03. Gridded EO Data.ipynb`, `04. Band Stacking + Clipping.ipynb`, `notebooks/examples/xview/Clipping - xView.ipynb`, `notebooks/examples/h3-rasterize/*.ipynb`, and their READMEs / `docs/docs/notebooks/*.mdx` where they call `plot_*`/`show_*`/folium.
- Test: per-notebook cell-by-cell harness up to the config ceiling.

**Interfaces:**
- Consumes: `vector_layer`/`raster_layer`/`grid_layer`/`pmtiles_layer` + `plot_interactive([...])` (Tasks 1,7); `cells_as_gdf` (existing). Produces: every VizX usage reads consistently against the new surface; no folium references remain; each example ends with a real multi-layer interactive showcase honoring `INTERACTIVE_PLOTS`.

- [ ] **Step 1: Audit (work-list).** Run `grep -rn -E "plot_interactive|plot_static|plot_pmtiles|plot_cog|plot_raster|show_(pmtiles|cog|raster)|folium" notebooks/ docs/docs` and record every hit with its call shape. No code change yet.
- [ ] **Step 2: Migrate** each affected call to the new signatures (a multi-arg `plot_interactive(df, column=...)` still works via coercion; remove folium-specific kwargs; fix prose referencing folium).
- [ ] **Step 3: Showcase — eo-series.** The series currently renders its final raster with a single-layer `plot_raster`. Add an interactive multi-layer cell on the merged/stacked result:
  - **NB03 "Gridded EO Data"** — after the `rst_merge_agg` cell that produces `kring_df` (the merged raster per H3 kring; the existing `plot_raster(kring_df.select("tile.raster").first()[0])`), add:
    ```python
    # Showcase: the merged raster + its H3 tessellation in one interactive map.
    from databricks.labs.gbx.vizx import raster_layer, grid_layer, plot_interactive
    plot_interactive([
        raster_layer(kring_df.select("tile.raster").first()[0]),
        grid_layer(kring_df, grid_system="h3", cellid_col="kring", opacity=0.25),
    ])
    ```
  - **NB04 "Band Stacking + Clipping"** — after `stacked_df` (the band-stacked rasters) and the clip cell (`to_plot[0]["clip_tile"]["raster"]`), add a showcase overlaying the stacked RGB tile with the clip cutline / H3 cells via `plot_interactive([raster_layer(<stacked tile>), grid_layer(stacked_df, grid_system="h3", cellid_col="cellid", opacity=0.25)])`.
  Both go through `show_*`/the `INTERACTIVE_PLOTS` toggle so the committed `.ipynb` stays static for GitHub.
- [ ] **Step 4: Showcase — xview.** After the clip result (`clip_raster = clip_row['tile_clip']['raster']`, currently `plot_raster`/`plot_file`), add the headline "clip to vector" view interactively — the clipped aerial tile with the labeled detected-object boundaries on top:
    ```python
    from databricks.labs.gbx.vizx import raster_layer, vector_layer, plot_interactive
    plot_interactive([
        raster_layer(clip_raster),
        vector_layer(objects_gdf, color="#ff3", width=2),   # the xView object boundaries (from the objects table)
    ])
    ```
  (`objects_gdf` = the detected-objects geometries built in section [3]; convert via `as_gdf` if it's a Spark DataFrame.)
- [ ] **Step 5: Showcase — h3-rasterize (opportunistic).** If it produces a raster + an H3 grid, add `plot_interactive([raster_layer(<raster>), grid_layer(<cells_df>, grid_system="h3")])` on the final result; skip if no natural multi-layer pairing.
- [ ] **Step 6: Verify** each touched notebook with `bash scripts/commands/gbx-test-notebooks.sh --path "<notebook>"` to its config ceiling (no NEW early-cell break); `grep -rn folium notebooks/ docs/docs` prints nothing.
- [ ] **Step 7: Commit**

```bash
git add notebooks/ docs/docs
git commit -m "docs: migrate + showcase the multi-layer viewer in eo-series/xview/h3-rasterize"
```

---

## Task 16: Helios NB02/NB03 real overlays + prose fix

**Files:**
- Modify: `notebooks/examples/helios/02. Visual Basemap (XYZ).ipynb`, `03. Analytical Core (COG + STAC).ipynb`; `notebooks/examples/helios/README.md`; `docs/docs/notebooks/helios.mdx`
- Test: cell-by-cell harness to config ceiling

**Interfaces:**
- Consumes: `plot_interactive([...])` multi-layer.
- Produces: NB02 actually overlays the buildings (NB01) over the NAIP basemap in one `plot_interactive([pmtiles_layer(naip), pmtiles_layer(buildings)])` call; NB03 overlays hillshade + buildings (+ the solar-score grid where natural). Prose in README/helios.mdx no longer implies an overlay the old single-archive calls didn't do.

- [ ] **Step 1: Edit NB02** to add a real multi-layer overlay cell using `plot_interactive([...])` (after the single-archive cells, with an honest comment).
- [ ] **Step 2: Edit NB03** similarly (hillshade + buildings + optional `grid_layer` of `solar_score`).
- [ ] **Step 3: Fix prose** in README + helios.mdx (the "overlays it with the buildings layer" lines now describe a real overlay; keep the static `INTERACTIVE_PLOTS` default honest).
- [ ] **Step 4: Verify** with `gbx:test:notebooks --path` for NB02/NB03 to the config ceiling; `grep -rn -iE "wave [0-9]" docs/docs/notebooks/helios.mdx` prints nothing (QC voice).
- [ ] **Step 5: Commit**

```bash
git add "notebooks/examples/helios/02. Visual Basemap (XYZ).ipynb" "notebooks/examples/helios/03. Analytical Core (COG + STAC).ipynb" notebooks/examples/helios/README.md docs/docs/notebooks/helios.mdx
git commit -m "docs(helios): real multi-layer overlays in NB02/NB03 + honest prose"
```

---

## Self-Review

**Spec coverage:** Layer model (T1); plot_static multi-layer + plot_cog ax= (T2,T3); MapLibre adapters/builder/folium-retire (T4,T5,T7); >64MB ladder (T6,T11); simplify two-flavor + spec + tippecanoe/tile-join/rasterio engine policy (T8,T9,T10); exports/extras/SRI/supply-chain (T12); Phase-1.5 dynamic cut-over (T13); red-carpet docs + diagram + doc-tests (T14); notebook/doc audit + migrate + **showcase** the new viewer in eo-series/xview/h3-rasterize (T15); Helios rewiring + prose (T16). Indefinite single-archive correctly OUT (App). contextily retained on the static path (T2/T3). Covered.

**Placeholder scan:** The only intentional deferred literal is the SRI hash, explicitly computed and asserted real in Task 12 (test guards against `REPLACE`). No "TBD/handle edge cases" steps.

**Type consistency:** `Layer` fields and constructor params (`geom_col`/`cellid_col`/`column`/`grid_system`/`simplify`) are used identically in T3/T4/T11; `prepare_layers`/`build_html`/`layer_to_sources_layers`/`normalize_spec`/`simplify_tiles_from_source`/`simplify_tiles_from_archive` names match across tasks; `plot_interactive`/`plot_static` signatures consistent T3/T7/T11.

---

## Notes for the executor

- **Docker-only tests:** Tasks 9-11, 14 need tippecanoe/`tile-join` + sample data → run via `gbx:test:python` / `gbx:test:python-docs` in the dev container. Pure-logic tests (T1,T2,T3,T4,T5,T6,T8,T13) run host or Docker.
- **Prerequisite:** refresh the staged light wheel on `geospatial_docs` before the Helios/doc-test runs (current one predates `pmtiles_info` in `gbx.pmtiles`).
- **Lint before push:** `gbx:lint:python --check` (host black may differ from Docker — verify in-container).
