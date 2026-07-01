# Pattern: In-notebook PMTiles rendering via base64-embedded in-browser FileSource

**Status:** Correctness and portability confirmed (offline-safe, no-server, GitHub-renderable
static fallback); no distributed compute path — driver-side render only. No measured compute
speedup to report.

---

## Problem

Rendering a PMTiles archive inline in a Databricks/Jupyter notebook has two naive approaches:

1. **External tile server** — spin up a local or remote HTTP server, serve range requests, point
   a map SDK at it. Requires a running process, a reachable port, and active HTTP connections for
   every tile fetch. Breaks in air-gapped or offline environments; does not render in static
   notebook exports (GitHub, nbviewer).
2. **Remote-URL PMTiles** — pass an `https://` archive URL directly to MapLibre + pmtiles.js.
   Requires the archive to be publicly accessible, triggers real network traffic for every
   range-request batch, and does not work for private or locally-generated archives on a UC Volume.

Both patterns fail in offline/CI environments and produce blank cells in static notebook renders.

---

## Symptom / signature

- A notebook cell shows a blank map or raises a network error in a restricted environment.
- The map is empty when the notebook is viewed statically (GitHub preview, nbconvert HTML).
- A tile-server subprocess is required before the map cell will render.
- A PMTiles archive lives on a UC Volume or local path, but the map SDK needs an HTTP URL.

---

## The pattern

**Base64-embedded in-browser FileSource + size-guarded static fallback.**

Three complementary moves:

### 1. Embed the archive inline as a base64 string; wrap it in a browser-side FileSource

```javascript
const b64 = "<base64-encoded archive bytes>";
const bin  = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
const protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);
const archive = new pmtiles.PMTiles(new pmtiles.FileSource(
    new File([bin.buffer], "gbx.pmtiles")));
protocol.add(archive);
// MapLibre now resolves pmtiles://gbx/z/x/y range-requests against
// the in-browser File object — zero HTTP calls.
```

The entire archive rides in the HTML blob. `pmtiles.FileSource` handles the spec-compliant
range-request logic in the browser JS engine. No tile server, no remote HTTP connection,
no CORS issue, no port binding.

### 2. Pin CDN script versions for reproducibility

```python
_MAPLIBRE_JS  = "https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"
_MAPLIBRE_CSS = "https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css"
_PMTILES_JS   = "https://unpkg.com/pmtiles@3.2.1/dist/pmtiles.js"
```

Unpinned CDN references can pull a breaking major when the notebook re-renders months later.
Pin at the minor-version level; update deliberately.

### 3. Size-guard + static fallback for large archives and GitHub-renderable output

Base64 encoding inflates the original archive bytes by approximately 33%
(`embed_size = len(data) * 4 / 3`). For a 64 MB archive the HTML blob is ~85 MB, which
overloads `displayHTML`. A size guard caps the interactive path and degrades gracefully:

```python
embed_mb = (len(data) * 4 / 3) / (1024 * 1024)
if embed_mb > max_embed_mb:          # default 64 MB
    if _is_raster_type(info["tile_type"]):
        return _static_raster_fallback(data, info, **kw)   # plot_raster
    return _static_vector_fallback(data, info, **kw)       # plot_static / contextily
```

`max_embed_mb=0` deliberately forces the static path — useful for notebook authors who want
the cell to produce a PNG/matplotlib figure that GitHub can render. `fallback=False` raises
instead of degrading, making the oversized case an explicit error for callers that want to
enforce the interactive path.

### 4. Type detection from the PMTiles header

```python
_RASTER_TYPES = frozenset({"png", "jpeg", "webp", "avif"})

def _is_raster_type(tile_type: str) -> bool:
    return tile_type in _RASTER_TYPES
```

`pmtiles_info` reads the archive header's `tile_type` field. The viewer auto-selects
a MapLibre raster or vector layer, and the static fallback chooses between `plot_raster`
(raster tiles decoded via rasterio MemoryFile) and `plot_static` (MVT tiles decoded to
GeoDataFrame via mapbox_vector_tile + shapely, then laid over a contextily basemap).
No user-visible type flag is required.

---

## Applicability matrix

### (a) Other light-tier functions this applies to

| Function / module | Applies? | Notes |
|---|---|---|
| Future `gbx.vizx` viewers (additional tile formats) | Yes — same pattern | Any new inline viewer should embed the data bytes directly and size-guard before interactive. |
| `gbx.vizx.plot_cog` | Partial | COG is rendered static-only (decimated rasterio read + contextily basemap); no in-browser FileSource needed because COG tiles are not bundled as a PMTiles archive. Decimated read avoids a full-resolution driver read — that is a correctness/resource pattern, not a compute gain. |
| `gbx.pmtiles.pmtiles_info` | Supporting role | Provides the header metadata (type, bounds, zoom range) that drives both the interactive MapLibre style auto-selection and the static fallback branch. Keeping the inspector as a standalone public function means future viewers do not need to duplicate header-parsing logic. |
| Overture / STAC download notebooks | No | Those are distributed Spark paths; the rendering pattern is orthogonal. |

### (b) Heavy-tier functions (same + similar)

| Function / Scala class | Applies? | Notes |
|---|---|---|
| Any Scala expression | N/A | These viewers are driver-side Python functions, not Spark expressions. There is no JVM equivalent of displayHTML / MapLibre HTML rendering. The heavy tier does not have an in-notebook map viewer path. |
| `gbx_pmtiles_agg` (heavy Scala aggregator) | N/A — produces the archive, does not render it | The heavy aggregator writes the PMTiles bytes to a path or returns them; `plot_pmtiles` is then called driver-side to view the result. No architectural change to the aggregator is needed. |

**Verdict:** Driver-side, light-tier-only pattern. The heavy tier writes PMTiles; the light tier
renders them. The FileSource embedding technique is specific to the browser JS sandbox and has
no heavy-tier counterpart.

---

## Evidence

**Correctness / portability:**

- `test_build_html_embeds_base64_and_registers_protocol` asserts the HTML blob contains the
  base64 string, `new pmtiles.Protocol`, `addProtocol`, `pmtiles.FileSource`, and `pmtiles://`.
  This proves no HTTP tile-server URL is emitted — every tile fetch resolves in-browser against
  the embedded File object.
- `test_plot_pmtiles_interactive_routes_through_displayhtml` runs the full code path with a
  real PMTiles archive (offline, no network): `displayHTML` is called, `maplibre-gl@4.7.1` and
  `pmtiles@3.2.1` appear in the output. No HTTP connection is opened.
- `test_plot_pmtiles_size_guard_uses_raster_fallback` and `test_plot_pmtiles_size_guard_uses_vector_fallback`
  confirm the `max_embed_mb` guard triggers and routes to the correct static plotter.
- `test_plot_pmtiles_oversized_without_fallback_raises` confirms `fallback=False` raises with
  a clear message rather than silently truncating.

**Base64 overhead math:**
Base64 encodes 3 bytes as 4 ASCII characters → overhead is exactly 4/3 ≈ 33.3%.
A 48 MB archive embeds as ~64 MB of ASCII; a 96 MB archive would embed as ~128 MB.
The default `max_embed_mb=64` guards against HTML blobs that overload `displayHTML` in
Databricks notebooks (observed limit ~100-150 MB in practice; 64 MB gives headroom).

**No measured distributed compute speedup:** This is a rendering/portability pattern.
There is no distributed Spark path involved; the rendering is entirely driver-side.
Timing comparisons with a tile server are deliberately omitted — the gain is availability
(works offline, works in static export) not wall-clock speedup.

---

## Canonical code references

- `python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py`
  - `_build_pmtiles_html` — constructs the self-contained HTML with base64 + FileSource
  - `plot_pmtiles` — main entry; size-guard + interactive vs static routing
  - `_static_raster_fallback`, `_static_vector_fallback` — tile decode + plot for static path
  - `_is_raster_type` — header-driven type detection
- `python/geobrix/src/databricks/labs/gbx/vizx/_cog.py`
  - `plot_cog` — decimated COG read + contextily basemap (static-only; see applicability note)
- `python/geobrix/src/databricks/labs/gbx/pmtiles/_inspect.py`
  - `pmtiles_info` — header reader supplying tile_type, bounds, zoom range to the viewer
- Tests: `python/geobrix/test/vizx/test_pmtiles.py` (offline; all assertions listed above)
- Tests: `python/geobrix/test/pmtiles_light/test_inspect.py` (inspector offline)
