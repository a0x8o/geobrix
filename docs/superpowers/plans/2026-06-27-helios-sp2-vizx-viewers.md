# VizX Viewers (SP2) Implementation Plan

**REQUIRED SUB-SKILL:** Use the `superpowers:test-driven-development` skill for every task — write the failing test first, watch it fail for the right reason, then write the minimal implementation to make it pass. Use `superpowers:verification-before-completion` before claiming any task done.

**Goal:** Ship net-new public `gbx.vizx` viewers — `plot_pmtiles` (interactive MapLibre GL JS + pmtiles.js, base64-embedded archive, with a Python-side static fallback) and `plot_cog` (rasterio overview read over a contextily basemap) — plus a reusable driver-side PMTiles inspector `gbx.pmtiles.pmtiles_info`. These let the Helios notebook series *show* its PMTiles/COG output inline in a Databricks notebook.

**Architecture:**
- **Inspector (`pmtiles/_inspect.py`):** `pmtiles_info(path) -> dict` reads a `.pmtiles` header via the existing `pmtiles` PyPI dep (`Reader.header()` / `Reader.metadata()`), normalizing `tile_type` (enum → string), min/max zoom, bounds (e7 → degrees), tile count, and tilejson-ish metadata. Consumed by both viewers and the static fallback.
- **Interactive path (`vizx/_pmtiles.py`):** build a self-contained HTML page that CDN-loads pinned `maplibre-gl` + `pmtiles` JS, registers the `pmtiles://` protocol, and feeds the archive bytes as a base64 in-browser `pmtiles.FileSource` — no HTTP server, no remote range requests. Vector (`tile_type == "mvt"`) → a MapLibre vector layer; raster (png/jpeg/webp) → a raster layer. Rendered through the existing `_interactive._notebook_display_html()` displayHTML channel with its IPython fallback chain.
- **Static fallback (`vizx/_pmtiles.py`):** the interactive map is the default; when the base64-embedded archive would exceed `max_embed_mb` (base64 bloats ~33%) and `fallback=True` (default), degrade to a static render — decode tiles with the Python `pmtiles` reader and composite: raster → reuse `vizx.plot_raster`; vector → decode MVT (`mapbox_vector_tile.decode`, already a pyvx dep) to shapely geometries → reuse `vizx.plot_static` over a contextily basemap. `fallback=False` raises instead of degrading; `max_embed_mb=0` deliberately forces the static render (GitHub-renderable committed notebooks). No new dep.
- **COG (`vizx/_cog.py`):** `plot_cog(path, *, band=None, **kw)` → `rasterio` overview/decimated read → `plot_raster` over a contextily basemap; the interactive-map raster-source injection is **optional** (decided in Task 7 — default static-only).

**Tech Stack:** Python 3.12, PySpark/Spark Connect (driver-side only here — no executors), `pmtiles` (present), `rasterio` (present), `mapbox_vector_tile` (present, a pyvx dep), `matplotlib`/`geopandas`/`contextily` ([vizx] extra, present). MapLibre GL JS + pmtiles.js are CDN-loaded at **pinned versions** (no Python dep).

---

## Global Constraints

- **Python 3.12+.** Pure driver-side code; no Spark executors, no Serverless config knobs.
- **`[vizx]` extra, import-guarded.** Every public function calls `assert_viz_available()` (from `vizx/_env.py`) before importing matplotlib/geopandas/contextily. The inspector lives in `gbx.pmtiles` and uses only the `pmtiles` dep (already present, light tier) — it does NOT require `[vizx]`.
- **NO new Python deps.** The interactive path is CDN JS. The static fallback reuses `pmtiles` + `mapbox_vector_tile` + `rasterio` + the `[vizx]` stack, all present. `plot_cog` uses `rasterio` only. (If, and only if, `plot_cog` later adopts `rio-tiler` for nicer overview selection, run the full light-CI-lock checklist: add to `requirements-pyrx-ci.in` AND `requirements-dev-container.in`, recompile the hashed `.txt`, then re-pin. This plan does NOT adopt rio-tiler — `band=`/decimated read is sufficient.)
- **Pin CDN JS versions** for reproducibility: `maplibre-gl@4.7.1` (`https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js` + `maplibre-gl.css`) and `pmtiles@3.2.1` (`https://unpkg.com/pmtiles@3.2.1/dist/pmtiles.js`). Defined as module constants so tests assert the exact pinned URLs.
- **TDD** — failing test first, minimal impl, per-task green run + commit.
- **Commit hygiene** — subject ≤72 chars; a WHY body for non-trivial commits. End commit messages with the `Co-authored-by: Isaac` trailer.
- **Docs voice** — no internal planning vocabulary (no wave numbers) in any docstring or docs page.
- **Docker:** none of these unit tests need Docker — they are offline/driver-only. They DO need the `[vizx]` extra deps (`matplotlib`, `geopandas`, `contextily`) plus `pmtiles`, `mapbox_vector_tile`, `rasterio` present in the env. The dev container has all of these; `gbx:test:python --path python/geobrix/test/vizx/...` runs them. If a bare host venv lacks them, `pip install 'geobrix[vizx]'` then ensure `pmtiles`/`mapbox-vector-tile` are installed (they ship with `[light]`).

---

### Task 1: PMTiles inspector — `pmtiles_info`

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/pmtiles/_inspect.py`
- Test: `python/geobrix/test/pmtiles_light/test_inspect.py`

**Interfaces:**
- Produces: `pmtiles_info(path: str | bytes | bytearray) -> dict` with keys `tile_type` (str: `"mvt"|"png"|"jpeg"|"webp"|"avif"|"unknown"`), `min_zoom` (int), `max_zoom` (int), `bounds` (tuple `(min_lon, min_lat, max_lon, max_lat)` in degrees), `center` (tuple `(lon, lat, zoom)`), `tile_count` (int), `metadata` (dict), `tile_compression` (str).
- Consumes: the `pmtiles` PyPI dep (`pmtiles.reader.Reader`, `MemorySource`, `MmapSource`, `all_tiles`; `pmtiles.tile.TileType`, `Compression`).

**Steps:**

- [ ] **Step 1** — Write the failing test. The fixture builds a tiny in-memory PMTiles archive (raster PNG + vector MVT) with the SAME `Writer` path the light agg uses, so the test is self-contained and needs no committed binary.

```python
# python/geobrix/test/pmtiles_light/test_inspect.py
"""Offline tests for the driver-side PMTiles inspector (pmtiles_info)."""

import io

import pytest
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer

_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16  # sniffs as PNG
_MVT = b"mvt-payload\x00\x01\x02"  # non-magic bytes => MVT


def _build_archive(tiles, tile_type, *, name="demo"):
    """tiles: list of (z, x, y, payload). Returns PMTiles v3 bytes over SF."""
    buf = io.BytesIO()
    w = Writer(buf)
    zs = [z for z, _, _, _ in tiles]
    header = {
        "tile_type": tile_type,
        "tile_compression": Compression.NONE,
        "internal_compression": Compression.GZIP,
        "min_zoom": min(zs),
        "max_zoom": max(zs),
        "min_lon_e7": int(-122.52 * 1e7),
        "min_lat_e7": int(37.70 * 1e7),
        "max_lon_e7": int(-122.35 * 1e7),
        "max_lat_e7": int(37.83 * 1e7),
        "center_zoom": min(zs),
        "center_lon_e7": int(-122.44 * 1e7),
        "center_lat_e7": int(37.76 * 1e7),
    }
    for z, x, y, payload in sorted(tiles, key=lambda t: zxy_to_tileid(t[0], t[1], t[2])):
        w.write_tile(zxy_to_tileid(z, x, y), payload)
    w.finalize(header, {"name": name, "vector_layers": [{"id": "demo"}]})
    return buf.getvalue()


@pytest.fixture
def raster_pmtiles():
    return _build_archive([(0, 0, 0, _PNG), (1, 0, 0, _PNG)], TileType.PNG)


@pytest.fixture
def vector_pmtiles():
    return _build_archive([(10, 163, 395, _MVT)], TileType.MVT, name="bldgs")


def test_info_from_bytes_raster(raster_pmtiles):
    from databricks.labs.gbx.pmtiles import pmtiles_info

    info = pmtiles_info(raster_pmtiles)
    assert info["tile_type"] == "png"
    assert info["min_zoom"] == 0
    assert info["max_zoom"] == 1
    assert info["tile_count"] == 2
    minlon, minlat, maxlon, maxlat = info["bounds"]
    assert -122.6 < minlon < maxlon < -122.3
    assert 37.6 < minlat < maxlat < 37.9
    assert info["metadata"].get("name") == "demo"
    assert info["tile_compression"] == "none"


def test_info_from_bytes_vector(vector_pmtiles):
    from databricks.labs.gbx.pmtiles import pmtiles_info

    info = pmtiles_info(vector_pmtiles)
    assert info["tile_type"] == "mvt"
    assert info["min_zoom"] == info["max_zoom"] == 10
    assert info["tile_count"] == 1
    assert info["metadata"].get("name") == "bldgs"


def test_info_from_path(raster_pmtiles, tmp_path):
    from databricks.labs.gbx.pmtiles import pmtiles_info

    p = tmp_path / "r.pmtiles"
    p.write_bytes(raster_pmtiles)
    info = pmtiles_info(str(p))
    assert info["tile_type"] == "png"
    assert info["tile_count"] == 2


def test_info_strips_dbfs_scheme(raster_pmtiles, tmp_path):
    # Databricks Volume paths often arrive scheme-qualified; the bare FUSE path
    # is what the reader opens. Strip dbfs:/file: like plot_file does.
    from databricks.labs.gbx.pmtiles import pmtiles_info

    p = tmp_path / "r.pmtiles"
    p.write_bytes(raster_pmtiles)
    info = pmtiles_info("dbfs:" + str(p))
    assert info["tile_count"] == 2


def test_center_tuple(vector_pmtiles):
    from databricks.labs.gbx.pmtiles import pmtiles_info

    lon, lat, zoom = pmtiles_info(vector_pmtiles)["center"]
    assert -122.6 < lon < -122.3 and 37.6 < lat < 37.9 and zoom == 10
```

- [ ] **Step 2** — Run, expect failure (import error / no `pmtiles_info`):

```
gbx:test:python --path python/geobrix/test/pmtiles_light/test_inspect.py
```
Expected: `ImportError: cannot import name 'pmtiles_info'` (collection error) on all 5 tests.

- [ ] **Step 3** — Minimal implementation:

```python
# python/geobrix/src/databricks/labs/gbx/pmtiles/_inspect.py
"""Driver-side PMTiles inspector. Spark-side PMTiles read is unsupported, so a
local-driver header reader is broadly useful and is consumed by the gbx.vizx
viewers (type detection + static fallback). Uses the existing `pmtiles` dep."""

from __future__ import annotations

from typing import Union

from pmtiles.reader import MemorySource, Reader, all_tiles
from pmtiles.tile import Compression, TileType

# TileType / Compression enum -> the lowercase string keys the viewers branch on.
_TILE_TYPE_NAME = {
    TileType.MVT: "mvt",
    TileType.PNG: "png",
    TileType.JPEG: "jpeg",
    TileType.WEBP: "webp",
    TileType.AVIF: "avif",
    TileType.UNKNOWN: "unknown",
}
_COMPRESSION_NAME = {
    Compression.UNKNOWN: "unknown",
    Compression.NONE: "none",
    Compression.GZIP: "gzip",
    Compression.BROTLI: "brotli",
    Compression.ZSTD: "zstd",
}


def _strip_scheme(path: str) -> str:
    for scheme in ("dbfs:", "file:"):
        if path.startswith(scheme):
            path = path[len(scheme) :]
            break
    if path.startswith("//"):
        path = "/" + path.lstrip("/")
    return path


def _read_bytes(path_or_bytes: Union[str, bytes, bytearray]) -> bytes:
    if isinstance(path_or_bytes, (bytes, bytearray)):
        return bytes(path_or_bytes)
    with open(_strip_scheme(str(path_or_bytes)), "rb") as f:
        return f.read()


def pmtiles_info(path: Union[str, bytes, bytearray]) -> dict:
    """Parse a .pmtiles archive header into a plain dict.

    ``path`` is a filesystem path (Volume/DBFS scheme prefixes are stripped) or
    the archive bytes. Returns ``tile_type`` (lowercase string), ``min_zoom`` /
    ``max_zoom`` (int), ``bounds`` (min_lon, min_lat, max_lon, max_lat degrees),
    ``center`` (lon, lat, zoom), ``tile_count`` (int), ``metadata`` (dict), and
    ``tile_compression`` (lowercase string). Driver-side only.
    """
    data = _read_bytes(path_or_bytes)
    source = MemorySource(data)
    reader = Reader(source)
    h = reader.header()
    metadata = reader.metadata()
    tile_count = sum(1 for _ in all_tiles(MemorySource(data)))
    return {
        "tile_type": _TILE_TYPE_NAME.get(h["tile_type"], "unknown"),
        "tile_compression": _COMPRESSION_NAME.get(h["tile_compression"], "unknown"),
        "min_zoom": int(h["min_zoom"]),
        "max_zoom": int(h["max_zoom"]),
        "bounds": (
            h["min_lon_e7"] / 1e7,
            h["min_lat_e7"] / 1e7,
            h["max_lon_e7"] / 1e7,
            h["max_lat_e7"] / 1e7,
        ),
        "center": (
            h["center_lon_e7"] / 1e7,
            h["center_lat_e7"] / 1e7,
            int(h["center_zoom"]),
        ),
        "tile_count": int(tile_count),
        "metadata": dict(metadata) if metadata else {},
    }
```

Wire the lazy re-export in `pmtiles/__init__.py` — extend `__all__` and `__getattr__` so heavy-tier imports still don't pull `pandas`/`_agg_light`, but `pmtiles_info` (which only needs `pmtiles`, present in both tiers) imports cleanly:

```python
# edit python/geobrix/src/databricks/labs/gbx/pmtiles/__init__.py
__all__ = ["register_pmtiles_agg", "pmtiles_info"]


def __getattr__(name):
    if name == "register_pmtiles_agg":
        from databricks.labs.gbx.pmtiles._agg_light import register_pmtiles_agg

        return register_pmtiles_agg
    if name == "pmtiles_info":
        from databricks.labs.gbx.pmtiles._inspect import pmtiles_info

        return pmtiles_info
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
```

- [ ] **Step 4** — Run, expect green:

```
gbx:test:python --path python/geobrix/test/pmtiles_light/test_inspect.py
```
Expected: `5 passed`.

- [ ] **Step 5** — Commit:

```
git add python/geobrix/src/databricks/labs/gbx/pmtiles/_inspect.py \
        python/geobrix/src/databricks/labs/gbx/pmtiles/__init__.py \
        python/geobrix/test/pmtiles_light/test_inspect.py
git commit -m "feat(pmtiles): driver-side pmtiles_info header inspector

Spark-side PMTiles read is unsupported, so a local-driver header reader
is needed by the VizX viewers for vector/raster type detection and the
static fallback. Lazy re-export keeps heavy-tier imports pandas-free.

Co-authored-by: Isaac"
```

---

### Task 2: Vector/raster type classification helper

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py` (helpers only this task)
- Test: `python/geobrix/test/vizx/test_pmtiles.py` (helper tests this task)

**Interfaces:**
- Produces: `_is_raster_type(tile_type: str) -> bool` and `_archive_bytes(path_or_bytes) -> bytes` (mirrors the inspector's path/scheme handling) in `vizx/_pmtiles.py`.
- Consumes: `pmtiles_info` from Task 1.

**Steps:**

- [ ] **Step 1** — Write the failing test:

```python
# python/geobrix/test/vizx/test_pmtiles.py
"""Offline tests for plot_pmtiles (interactive HTML + static fallback)."""

import io

import pytest
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer

_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16


def _build_archive(tiles, tile_type, *, name="demo"):
    buf = io.BytesIO()
    w = Writer(buf)
    zs = [z for z, _, _, _ in tiles]
    header = {
        "tile_type": tile_type,
        "tile_compression": Compression.NONE,
        "internal_compression": Compression.GZIP,
        "min_zoom": min(zs),
        "max_zoom": max(zs),
        "min_lon_e7": int(-122.52 * 1e7),
        "min_lat_e7": int(37.70 * 1e7),
        "max_lon_e7": int(-122.35 * 1e7),
        "max_lat_e7": int(37.83 * 1e7),
        "center_zoom": min(zs),
        "center_lon_e7": int(-122.44 * 1e7),
        "center_lat_e7": int(37.76 * 1e7),
    }
    for z, x, y, payload in sorted(tiles, key=lambda t: zxy_to_tileid(t[0], t[1], t[2])):
        w.write_tile(zxy_to_tileid(z, x, y), payload)
    w.finalize(header, {"name": name, "vector_layers": [{"id": "demo"}]})
    return buf.getvalue()


def test_is_raster_type():
    from databricks.labs.gbx.vizx import _pmtiles as p

    assert p._is_raster_type("png") is True
    assert p._is_raster_type("jpeg") is True
    assert p._is_raster_type("webp") is True
    assert p._is_raster_type("avif") is True
    assert p._is_raster_type("mvt") is False
    assert p._is_raster_type("unknown") is False


def test_archive_bytes_passthrough_and_path(tmp_path):
    from databricks.labs.gbx.vizx import _pmtiles as p

    raw = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)
    assert p._archive_bytes(raw) == raw
    f = tmp_path / "a.pmtiles"
    f.write_bytes(raw)
    assert p._archive_bytes(str(f)) == raw
    assert p._archive_bytes("dbfs:" + str(f)) == raw
```

- [ ] **Step 2** — Run, expect failure:

```
gbx:test:python --path python/geobrix/test/vizx/test_pmtiles.py
```
Expected: `ModuleNotFoundError` / `ImportError: cannot import name '_pmtiles'` on collection.

- [ ] **Step 3** — Minimal implementation (create `vizx/_pmtiles.py` with helpers + the pinned CDN constants used in later tasks):

```python
# python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py
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
```

- [ ] **Step 4** — Run, expect green:

```
gbx:test:python --path python/geobrix/test/vizx/test_pmtiles.py
```
Expected: `2 passed`.

- [ ] **Step 5** — Commit:

```
git add python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py \
        python/geobrix/test/vizx/test_pmtiles.py
git commit -m "feat(vizx): pmtiles type-detect + archive-bytes helpers

Co-authored-by: Isaac"
```

---

### Task 3: Interactive HTML builder (`_build_pmtiles_html`)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py`
- Test: `python/geobrix/test/vizx/test_pmtiles.py`

**Interfaces:**
- Produces: `_build_pmtiles_html(archive_b64: str, info: dict, *, style=None) -> str` in `vizx/_pmtiles.py`.
- Consumes: the pinned CDN constants; `info` dict shape from `pmtiles_info`.

**Steps:**

- [ ] **Step 1** — Write the failing test (assert HTML structure: pinned CDN script tags, base64 source embed, protocol registration, vector-vs-raster layer):

```python
# append to python/geobrix/test/vizx/test_pmtiles.py
def _info(tile_type, *, min_zoom=0, max_zoom=2):
    return {
        "tile_type": tile_type,
        "tile_compression": "none",
        "min_zoom": min_zoom,
        "max_zoom": max_zoom,
        "bounds": (-122.52, 37.70, -122.35, 37.83),
        "center": (-122.44, 37.76, min_zoom),
        "tile_count": 3,
        "metadata": {"vector_layers": [{"id": "demo"}]},
    }


def test_build_html_pins_cdn_versions():
    from databricks.labs.gbx.vizx import _pmtiles as p

    html = p._build_pmtiles_html("QUJD", _info("png"))
    assert "maplibre-gl@4.7.1/dist/maplibre-gl.js" in html
    assert "maplibre-gl@4.7.1/dist/maplibre-gl.css" in html
    assert "pmtiles@3.2.1/dist/pmtiles.js" in html


def test_build_html_embeds_base64_and_registers_protocol():
    from databricks.labs.gbx.vizx import _pmtiles as p

    html = p._build_pmtiles_html("QUJDREVG", _info("png"))
    assert "QUJDREVG" in html  # the base64 archive is embedded inline
    assert "new pmtiles.Protocol" in html
    assert "addProtocol" in html
    assert "pmtiles.FileSource" in html or "FileSource" in html
    assert "pmtiles://" in html


def test_build_html_raster_layer_for_png():
    from databricks.labs.gbx.vizx import _pmtiles as p

    html = p._build_pmtiles_html("QUJD", _info("png"))
    assert '"type": "raster"' in html or "'type': 'raster'" in html or "type: \"raster\"" in html


def test_build_html_vector_layer_for_mvt():
    from databricks.labs.gbx.vizx import _pmtiles as p

    html = p._build_pmtiles_html("QUJD", _info("mvt"))
    assert '"type": "vector"' in html or "type: \"vector\"" in html
    # the source-layer id from the metadata vector_layers drives the fill layer
    assert "demo" in html


def test_build_html_honors_custom_style():
    from databricks.labs.gbx.vizx import _pmtiles as p

    html = p._build_pmtiles_html("QUJD", _info("mvt"), style={"version": 8, "layers": []})
    assert '"version": 8' in html
```

- [ ] **Step 2** — Run, expect failure:

```
gbx:test:python --path python/geobrix/test/vizx/test_pmtiles.py
```
Expected: 5 new tests fail with `AttributeError: module ... has no attribute '_build_pmtiles_html'`.

- [ ] **Step 3** — Minimal implementation (append to `_pmtiles.py`):

```python
# append to python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py
import json


def _default_style(info: dict, source_name: str) -> dict:
    """A minimal MapLibre style: a pmtiles:// source + one raster or vector layer."""
    is_raster = _is_raster_type(info["tile_type"])
    source = {
        "type": "raster" if is_raster else "vector",
        "url": f"pmtiles://{source_name}",
    }
    if is_raster:
        source["tileSize"] = 256
        layers = [{"id": "tiles", "type": "raster", "source": source_name}]
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
    map_style = style if style is not None else _default_style(info, source_name)
    style_json = json.dumps(map_style)
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
```

- [ ] **Step 4** — Run, expect green:

```
gbx:test:python --path python/geobrix/test/vizx/test_pmtiles.py
```
Expected: all tests pass (7 total in the file so far).

- [ ] **Step 5** — Commit:

```
git add python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py \
        python/geobrix/test/vizx/test_pmtiles.py
git commit -m "feat(vizx): MapLibre+pmtiles.js HTML builder (base64 FileSource)

CDN versions pinned (maplibre-gl 4.7.1, pmtiles 3.2.1); archive rides
inline as a base64 in-browser FileSource so the map streams client-side
with no tile server. Vector vs raster layer chosen from the header type.

Co-authored-by: Isaac"
```

---

### Task 4: Raster static fallback (`_static_raster_fallback`)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py`
- Test: `python/geobrix/test/vizx/test_pmtiles.py`

**Interfaces:**
- Produces: `_static_raster_fallback(data: bytes, info: dict, **plot_kw) -> None` in `vizx/_pmtiles.py` — picks the lowest-zoom tile, decodes the image payload, hands it to `vizx.plot_raster`.
- Consumes: `pmtiles.reader.all_tiles`; `vizx.plot_raster`.

**Steps:**

- [ ] **Step 1** — Write the failing test. To make a *real* decodable raster tile, build a tiny PNG with rasterio/matplotlib in the fixture so `plot_raster` actually decodes it:

```python
# append to python/geobrix/test/vizx/test_pmtiles.py
def _real_png_tile():
    # A real 8x8 RGB PNG so plot_raster's rasterio MemoryFile can decode it.
    import io as _io

    import matplotlib
    matplotlib.use("Agg")
    import numpy as np
    from matplotlib.image import imsave

    buf = _io.BytesIO()
    imsave(buf, (np.random.rand(8, 8, 3)), format="png")
    return buf.getvalue()


def test_static_raster_fallback_calls_plot_raster(monkeypatch):
    from databricks.labs.gbx.vizx import _pmtiles as p

    png = _real_png_tile()
    archive = _build_archive([(0, 0, 0, png)], TileType.PNG)
    captured = {}
    monkeypatch.setattr(
        "databricks.labs.gbx.vizx.plot_raster",
        lambda raster_bytes, **kw: captured.update(n=len(raster_bytes)),
    )
    info = p_info = __import__(
        "databricks.labs.gbx.pmtiles", fromlist=["pmtiles_info"]
    ).pmtiles_info(archive)
    p._static_raster_fallback(archive, info)
    assert captured["n"] == len(png)  # the decoded lowest-zoom tile bytes
```

- [ ] **Step 2** — Run, expect failure:

```
gbx:test:python --path python/geobrix/test/vizx/test_pmtiles.py
```
Expected: `AttributeError: ... '_static_raster_fallback'`.

- [ ] **Step 3** — Minimal implementation (append to `_pmtiles.py`):

```python
# append to python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py
from pmtiles.reader import MemorySource, all_tiles  # noqa: E402
from pmtiles.tile import tileid_to_zxy  # noqa: E402


def _lowest_zoom_tile(data: bytes):
    """Return (z, x, y, payload) for the lowest-zoom tile (the coarsest overview)."""
    best = None
    for tileid, payload in all_tiles(MemorySource(data)):
        z, x, y = tileid_to_zxy(tileid)
        if best is None or z < best[0]:
            best = (z, x, y, payload)
    return best


def _static_raster_fallback(data: bytes, info: dict, **plot_kw) -> None:
    """Decode the coarsest raster tile and render it via plot_raster."""
    from databricks.labs.gbx.vizx import plot_raster

    tile = _lowest_zoom_tile(data)
    if tile is None:
        raise ValueError("plot_pmtiles: archive has no tiles to render")
    plot_raster(tile[3], **plot_kw)
```

- [ ] **Step 4** — Run, expect green:

```
gbx:test:python --path python/geobrix/test/vizx/test_pmtiles.py
```
Expected: all pass.

- [ ] **Step 5** — Commit:

```
git add python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py \
        python/geobrix/test/vizx/test_pmtiles.py
git commit -m "feat(vizx): raster pmtiles static fallback via plot_raster

Co-authored-by: Isaac"
```

---

### Task 5: Vector static fallback (`_static_vector_fallback`)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py`
- Test: `python/geobrix/test/vizx/test_pmtiles.py`

**Interfaces:**
- Produces: `_static_vector_fallback(data: bytes, info: dict, **plot_kw) -> object` — decode each MVT tile to geometries (tile-local → WGS84 via `pyvx._mvt._tile_bounds`), build a GeoDataFrame, render via `vizx.plot_static` over a contextily basemap.
- Consumes: `mapbox_vector_tile.decode`; `pyvx._mvt` (`_tile_bounds`); `geopandas`; `vizx.plot_static`.

**Steps:**

- [ ] **Step 1** — Write the failing test. Build a vector PMTiles whose MVT tile holds a real geometry, assert the fallback yields a non-empty GeoDataFrame in EPSG:4326 and calls plot_static:

```python
# append to python/geobrix/test/vizx/test_pmtiles.py
def _real_mvt_tile(z, x, y):
    # Encode a polygon in tile-local pixel space for tile (z,x,y) (origin NW),
    # the same convention pyvx writes, so the fallback reprojects it back to 4326.
    import mapbox_vector_tile as mvt
    from shapely.geometry import box

    return mvt.encode(
        {"name": "demo", "features": [
            {"geometry": box(1000, 1000, 3000, 3000), "properties": {"v": 1}}]},
        default_options={"extents": 4096, "y_coord_down": True},
    )


def test_static_vector_fallback_builds_gdf_and_plots(monkeypatch):
    import geopandas as gpd

    from databricks.labs.gbx.vizx import _pmtiles as p

    z, x, y = 10, 163, 395  # an SF-area tile
    blob = _real_mvt_tile(z, x, y)
    archive = _build_archive([(z, x, y, blob)], TileType.MVT)
    info = __import__(
        "databricks.labs.gbx.pmtiles", fromlist=["pmtiles_info"]
    ).pmtiles_info(archive)

    captured = {}

    def _fake_plot_static(gdf, **kw):
        captured["gdf"] = gdf
        captured["kw"] = kw
        return "AX"

    monkeypatch.setattr(
        "databricks.labs.gbx.vizx.plot_static", _fake_plot_static
    )
    out = p._static_vector_fallback(archive, info, basemap=False)
    assert out == "AX"
    gdf = captured["gdf"]
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) >= 1
    assert gdf.crs.to_epsg() == 4326
    # geometry reprojected into the SF tile's lon/lat extent
    minx, miny, maxx, maxy = gdf.total_bounds
    assert -123 < minx < maxx < -121 and 37 < miny < maxy < 39
```

- [ ] **Step 2** — Run, expect failure:

```
gbx:test:python --path python/geobrix/test/vizx/test_pmtiles.py
```
Expected: `AttributeError: ... '_static_vector_fallback'`.

- [ ] **Step 3** — Minimal implementation (append to `_pmtiles.py`):

```python
# append to python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py
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
    for tileid, payload in all_tiles(MemorySource(data)):
        z, x, y = tileid_to_zxy(tileid)
        for geom, props in _decode_mvt_to_geoms(payload, z, x, y):
            geoms.append(geom)
            rows.append(props)
    if not geoms:
        raise ValueError("plot_pmtiles: vector archive decoded to no geometries")
    gdf = gpd.GeoDataFrame(rows, geometry=geoms, crs=4326)
    return plot_static(gdf, **plot_kw)
```

- [ ] **Step 4** — Run, expect green:

```
gbx:test:python --path python/geobrix/test/vizx/test_pmtiles.py
```
Expected: all pass.

- [ ] **Step 5** — Commit:

```
git add python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py \
        python/geobrix/test/vizx/test_pmtiles.py
git commit -m "feat(vizx): vector pmtiles static fallback (MVT decode -> gdf)

Decodes tile-local MVT features back to WGS-84 with the same tile-bounds
math pyvx writes, then renders via plot_static over a contextily basemap.

Co-authored-by: Isaac"
```

---

### Task 6: `plot_pmtiles` dispatch + size guard

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py`
- Test: `python/geobrix/test/vizx/test_pmtiles.py`

**Interfaces:**
- Produces: `plot_pmtiles(path_or_bytes, *, max_embed_mb=64, fallback=True, style=None, **map_kwargs)` — **PINNED public signature**.
- Consumes: `_archive_bytes`, `pmtiles_info`, `_build_pmtiles_html`, `_is_raster_type`, `_static_raster_fallback`, `_static_vector_fallback`, `_interactive._notebook_display_html`.

**Steps:**

- [ ] **Step 1** — Write the failing test. Cover: interactive HTML routed through displayHTML; size-guard → raster fallback; explicit `fallback=True` → vector fallback; bad type. Patch `_notebook_display_html` to capture the HTML and assert structure:

```python
# append to python/geobrix/test/vizx/test_pmtiles.py
def test_plot_pmtiles_interactive_routes_through_displayhtml(monkeypatch):
    from databricks.labs.gbx.vizx import _pmtiles as p

    archive = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)
    captured = {}
    monkeypatch.setattr(
        "databricks.labs.gbx.vizx._interactive._notebook_display_html",
        lambda: (lambda html: captured.update(html=html)),
    )
    out = p.plot_pmtiles(archive)  # small -> interactive
    assert out is None  # displayHTML render returns None
    html = captured["html"]
    assert "maplibre-gl@4.7.1" in html and "pmtiles@3.2.1" in html
    assert "pmtiles://" in html


def test_plot_pmtiles_size_guard_uses_raster_fallback(monkeypatch):
    from databricks.labs.gbx.vizx import _pmtiles as p

    png = _real_png_tile()
    archive = _build_archive([(0, 0, 0, png)], TileType.PNG)
    called = {}
    monkeypatch.setattr(p, "_static_raster_fallback",
                        lambda data, info, **kw: called.update(raster=True))
    # max_embed_mb tiny -> archive exceeds it -> static path
    p.plot_pmtiles(archive, max_embed_mb=1e-9)
    assert called.get("raster") is True


def test_plot_pmtiles_size_guard_uses_vector_fallback(monkeypatch):
    from databricks.labs.gbx.vizx import _pmtiles as p

    blob = _real_mvt_tile(10, 163, 395)
    archive = _build_archive([(10, 163, 395, blob)], TileType.MVT)
    called = {}
    monkeypatch.setattr(p, "_static_vector_fallback",
                        lambda data, info, **kw: called.update(vector=True) or "AX")
    # tiny budget -> archive exceeds it -> static vector path (fallback default True)
    p.plot_pmtiles(archive, max_embed_mb=1e-9)
    assert called.get("vector") is True


def test_plot_pmtiles_oversized_without_fallback_raises():
    from databricks.labs.gbx.vizx import _pmtiles as p

    archive = _build_archive([(0, 0, 0, _PNG)], TileType.PNG)
    with pytest.raises(ValueError, match="exceeds max_embed_mb"):
        p.plot_pmtiles(archive, max_embed_mb=1e-9, fallback=False)
```

- [ ] **Step 2** — Run, expect failure:

```
gbx:test:python --path python/geobrix/test/vizx/test_pmtiles.py
```
Expected: `AttributeError: ... 'plot_pmtiles'`.

- [ ] **Step 3** — Minimal implementation (append to `_pmtiles.py`):

```python
# append to python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py
def plot_pmtiles(path_or_bytes, *, max_embed_mb=64, fallback=True, style=None,
                 **map_kwargs):
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
```

> **Note on `fallback` semantics:** per the spec, the **interactive** MapLibre map is the default. `fallback` governs what happens when the base64-embedded archive would exceed `max_embed_mb`: `fallback=True` (default) degrades gracefully to the static render; `fallback=False` raises so the caller knows the archive is too large to embed. For a deliberately static, GitHub-renderable render (committed notebooks where an interactive map wouldn't persist), pass `max_embed_mb=0`. Tests above lock this contract (`test_plot_pmtiles_interactive_routes_through_displayhtml` proves interactive-by-default; the size-guard tests prove auto-degrade; `test_plot_pmtiles_oversized_without_fallback_raises` proves the raise).

- [ ] **Step 4** — Run, expect green:

```
gbx:test:python --path python/geobrix/test/vizx/test_pmtiles.py
```
Expected: all pass.

- [ ] **Step 5** — Commit:

```
git add python/geobrix/src/databricks/labs/gbx/vizx/_pmtiles.py \
        python/geobrix/test/vizx/test_pmtiles.py
git commit -m "feat(vizx): plot_pmtiles dispatch + base64 size guard

Interactive MapLibre map by default; when the ~33%-inflated base64 embed
would exceed max_embed_mb, fallback=True (default) degrades to the static
render and fallback=False raises. max_embed_mb=0 forces static.

Co-authored-by: Isaac"
```

---

### Task 7: `plot_cog`

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/vizx/_cog.py`
- Test: `python/geobrix/test/vizx/test_cog.py`

**Interfaces:**
- Produces: `plot_cog(path, *, band=None, **kw)` — **PINNED public signature**.
- Consumes: `rasterio`; `vizx._raster.plot_file` / `plot_raster` machinery; contextily basemap (via the decimated read + a static render).

**Decision (spec open item):** `plot_cog` is **static-only** (rasterio overview/decimated read over a contextily basemap). The interactive raster-source injection is left out — a COG is not a PMTiles archive, so embedding it as an in-browser FileSource is not applicable, and a remote raster source would need range requests against storage (the very thing the PMTiles base64 embed avoids). If a future need arises, convert the COG to raster PMTiles (`gbx_rst_cog_convert` → pyramid → `gbx_pmtiles_agg`) and use `plot_pmtiles`. This keeps `plot_cog` a single, predictable static path.

**Steps:**

- [ ] **Step 1** — Write the failing test. Build a real single-band + multi-band COG-ish GeoTIFF in memory with rasterio, assert `plot_cog` produces a figure and honors `band=`:

```python
# python/geobrix/test/vizx/test_cog.py
"""Offline tests for plot_cog (rasterio overview read over a contextily basemap)."""

import matplotlib
import pytest

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402


def _write_tif(tmp_path, bands=3, size=32, crs="EPSG:3857"):
    import rasterio
    from rasterio.transform import from_bounds

    path = tmp_path / "cog.tif"
    data = (np.random.rand(bands, size, size) * 1000).astype("uint16")
    transform = from_bounds(-1.36e7, 4.5e6, -1.35e7, 4.51e6, size, size)
    with rasterio.open(
        path, "w", driver="GTiff", height=size, width=size, count=bands,
        dtype="uint16", crs=crs, transform=transform,
    ) as dst:
        dst.write(data)
    return str(path)


def test_plot_cog_renders_figure(tmp_path):
    from databricks.labs.gbx.vizx import plot_cog

    plt.close("all")
    path = _write_tif(tmp_path, bands=3)
    plot_cog(path)
    assert len(plt.get_fignums()) >= 1
    plt.close("all")


def test_plot_cog_band_select(tmp_path, monkeypatch):
    from databricks.labs.gbx.vizx import _cog

    path = _write_tif(tmp_path, bands=3)
    captured = {}
    # capture the array handed to the renderer to confirm a single band was read
    monkeypatch.setattr(
        _cog, "_render_cog",
        lambda data, transform, **kw: captured.update(shape=data.shape),
    )
    _cog.plot_cog(path, band=2)
    assert captured["shape"][0] == 1  # one band selected


def test_plot_cog_strips_dbfs_scheme(tmp_path):
    from databricks.labs.gbx.vizx import plot_cog

    plt.close("all")
    path = _write_tif(tmp_path, bands=1)
    plot_cog("dbfs:" + path)  # must not raise on the scheme prefix
    assert len(plt.get_fignums()) >= 1
    plt.close("all")
```

- [ ] **Step 2** — Run, expect failure:

```
gbx:test:python --path python/geobrix/test/vizx/test_cog.py
```
Expected: `ImportError: cannot import name 'plot_cog'` / `_cog` missing.

- [ ] **Step 3** — Minimal implementation:

```python
# python/geobrix/src/databricks/labs/gbx/vizx/_cog.py
"""Cloud-Optimized GeoTIFF viewer for gbx.vizx.

Reads a COG decimated (an overview-equivalent read) and renders it over a
contextily basemap as a static matplotlib figure. Driver-side; requires the
[vizx] extra plus rasterio.
"""

from __future__ import annotations

import warnings


def _strip_scheme(path: str) -> str:
    for scheme in ("dbfs:", "file:"):
        if path.startswith(scheme):
            path = path[len(scheme) :]
            break
    if path.startswith("//"):
        path = "/" + path.lstrip("/")
    return path


def _render_cog(data, transform, *, crs, fig_w, fig_h, title, basemap, basemap_source):
    """Render a decimated COG array (bands, h, w) over a contextily basemap."""
    import matplotlib.pyplot as plt
    import numpy as np
    from rasterio.plot import plotting_extent, show

    from databricks.labs.gbx.vizx._raster import (
        _needs_percentile_stretch,
        _percentile_stretch,
    )

    if _needs_percentile_stretch(data):
        data = _percentile_stretch(data)
    _, ax = plt.subplots(1, figsize=(fig_w, fig_h))
    if data.shape[0] == 1:
        band = data[0]
        ax.imshow(band, extent=plotting_extent(band, transform), cmap="viridis")
    else:
        show(data, ax=ax, transform=transform)
    if basemap and crs is not None:
        try:
            import contextily as cx

            source = basemap_source or cx.providers.CartoDB.Positron
            cx.add_basemap(ax, source=source, crs=crs)
        except Exception as exc:  # noqa: BLE001 — offline/no-egress -> warn + skip
            warnings.warn(
                f"plot_cog: basemap unavailable ({type(exc).__name__}: {exc}); "
                "rendering without basemap.",
                stacklevel=2,
            )
    if title:
        ax.set_title(title)
    ax.set_axis_off()


def plot_cog(path, *, band=None, max_pixels=2000, fig_w=10, fig_h=10,
             basemap=True, basemap_source=None, title=None, **kw):
    """Render a Cloud-Optimized GeoTIFF inline over a contextily basemap.

    Reads ``path`` decimated so the longest edge is <= ``max_pixels`` (uses the
    COG's overviews when present). ``band`` (1-based) selects a single band;
    otherwise all bands render (1 -> viridis, 3+ -> RGB). Volume/DBFS scheme
    prefixes are stripped. Requires the [vizx] extra plus rasterio.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()
    import rasterio

    from databricks.labs.gbx.vizx._raster import _decimated_read

    p = _strip_scheme(str(path))
    with rasterio.open(p) as src:
        if band is not None:
            scale = max(src.width, src.height) / max_pixels
            out_h = max(1, int(src.height // scale)) if scale > 1 else src.height
            out_w = max(1, int(src.width // scale)) if scale > 1 else src.width
            data = src.read(
                indexes=[band],
                out_shape=(1, out_h, out_w),
                resampling=rasterio.enums.Resampling.bilinear,
                masked=True,
            )
            transform = src.transform * src.transform.scale(
                src.width / out_w, src.height / out_h
            )
        else:
            data, transform, _ = _decimated_read(src, max_pixels)
        crs = src.crs
    _render_cog(
        data, transform, crs=crs, fig_w=fig_w, fig_h=fig_h,
        title=title or "COG", basemap=basemap, basemap_source=basemap_source,
    )
```

- [ ] **Step 4** — Run, expect green:

```
gbx:test:python --path python/geobrix/test/vizx/test_cog.py
```
Expected: all pass. (The basemap fetch fails offline → warns + renders without it; the test only asserts a figure is produced.)

- [ ] **Step 5** — Commit:

```
git add python/geobrix/src/databricks/labs/gbx/vizx/_cog.py \
        python/geobrix/test/vizx/test_cog.py
git commit -m "feat(vizx): plot_cog static COG viewer over contextily basemap

Decimated/overview rasterio read; band= selects one band. Static-only
(interactive raster-source injection deferred — COGs aren't PMTiles;
convert to raster PMTiles + plot_pmtiles for an interactive map).

Co-authored-by: Isaac"
```

---

### Task 8: Public exports

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/__init__.py`
- Test: `python/geobrix/test/vizx/test_pmtiles.py` (export assertion)

**Interfaces:**
- Produces: `from databricks.labs.gbx.vizx import plot_pmtiles, plot_cog`; both in `__all__`.

**Steps:**

- [ ] **Step 1** — Write the failing test:

```python
# append to python/geobrix/test/vizx/test_pmtiles.py
def test_public_exports():
    import databricks.labs.gbx.vizx as vizx

    assert hasattr(vizx, "plot_pmtiles")
    assert hasattr(vizx, "plot_cog")
    assert "plot_pmtiles" in vizx.__all__
    assert "plot_cog" in vizx.__all__
```

- [ ] **Step 2** — Run, expect failure:

```
gbx:test:python --path python/geobrix/test/vizx/test_pmtiles.py::test_public_exports
```
Expected: `AttributeError: module 'databricks.labs.gbx.vizx' has no attribute 'plot_pmtiles'`.

- [ ] **Step 3** — Minimal implementation (edit `vizx/__init__.py`):

```python
from databricks.labs.gbx.vizx._cog import plot_cog
from databricks.labs.gbx.vizx._interactive import plot_interactive
from databricks.labs.gbx.vizx._pmtiles import plot_pmtiles
from databricks.labs.gbx.vizx._raster import plot_file, plot_mask_layers, plot_raster
from databricks.labs.gbx.vizx._static_map import plot_static
from databricks.labs.gbx.vizx._vector import as_gdf, cells_as_gdf, grid_as_gdf

__all__ = [
    "plot_raster",
    "plot_file",
    "plot_mask_layers",
    "plot_static",
    "plot_interactive",
    "plot_pmtiles",
    "plot_cog",
    "as_gdf",
    "cells_as_gdf",
    "grid_as_gdf",
]
```

> Verify `_pmtiles`/`_cog` are import-safe at module level: `_pmtiles.py` top-level imports `base64`, `json`, and `pmtiles.*` (present in both light tiers); `_cog.py` top-level imports only `warnings`. Heavy viz deps stay lazy inside functions. So importing `vizx` does not pull matplotlib/geopandas — consistent with the rest of the package.

- [ ] **Step 4** — Run the full vizx + inspector suites:

```
gbx:test:python --path python/geobrix/test/vizx/
gbx:test:python --path python/geobrix/test/pmtiles_light/test_inspect.py
```
Expected: all green.

- [ ] **Step 5** — Commit:

```
git add python/geobrix/src/databricks/labs/gbx/vizx/__init__.py \
        python/geobrix/test/vizx/test_pmtiles.py
git commit -m "feat(vizx): export plot_pmtiles + plot_cog

Co-authored-by: Isaac"
```

---

### Task 9: Docs note + doc-test wiring + final green run

**Files:**
- Modify: `docs/docs/api/` viz/vizx page (the existing VizX reference page) — add `plot_pmtiles` / `plot_cog` / `pmtiles_info` with a short usage snippet sourced from a doc-test.
- Create (if a vizx doc-test module exists, extend it; else add): `docs/tests/python/.../vizx_*` doc-test functions exercising `pmtiles_info` on a fixture archive and the static fallback (offline, real assertions).

**Steps:**

- [ ] **Step 1** — Locate the VizX docs page and any existing vizx doc-test module:

```
grep -rln "plot_static\|plot_raster\|gbx.vizx" docs/docs/ docs/tests/python/
```

- [ ] **Step 2** — Add a doc-test that builds a tiny fixture PMTiles archive (the same `_build_archive` helper as the unit tests — copy it inline; doc-tests must be self-contained and execute real assertions), calls `pmtiles_info`, asserts `tile_type`/`tile_count`, and runs `plot_pmtiles(..., max_embed_mb=0, basemap=False)` (forces the static path) to confirm it produces output without network. Import the snippet into the MDX via raw-loader per repo convention. Keep the docs voice clean (no internal vocabulary).

- [ ] **Step 3** — Run the vizx doc tests in Docker (doc tests only run in Docker). Dispatch a Task subagent for the long-running container run:

```
gbx:test:python-docs --log helios-sp2-vizx-docs.log
```
Expected: the new vizx doc-test nodes pass. Narrow to the failing node IDs and rerun only those until green; do not retest passing packages.

- [ ] **Step 4** — Run binding-parity-adjacent sanity (these are pure-Python public API additions, not registered SQL functions, so `registered_functions.txt` is unchanged — confirm no parity check expects them):

```
grep -rn "plot_pmtiles\|plot_cog\|pmtiles_info" python/geobrix/src/databricks/labs/gbx/bench/registered_functions.txt docs/tests-function-info/registered_functions.txt
```
Expected: no matches (these are module functions, not SQL UDFs — nothing to register).

- [ ] **Step 5** — Run python lint (CI gate) before committing docs:

```
gbx:lint:python --check
```
Fix isort/black/flake8 findings in-container if the host black differs.

- [ ] **Step 6** — Commit:

```
git add docs/
git commit -m "docs(vizx): document plot_pmtiles, plot_cog, pmtiles_info

Co-authored-by: Isaac"
```

---

### Task 10: Capture validated performance gains

**Files:**
- Create (only if a gain is validated): `docs/superpowers/performance/<slug>.md` + `docs/superpowers/performance/README.md` (index, if first) + a thin pointer memory entry.

**Steps:**

- [ ] **Step 1** — Assess whether SP2 surfaced any reusable rendering/decoding gain worth capturing. Candidate: the **base64-embed-vs-tile-server PMTiles rendering** pattern (no HTTP server / no remote range requests; entire archive streams in-browser) and the **driver-side overview/decimated read** for `plot_cog` (avoids full-resolution reads). These are correctness/UX patterns more than a measured speedup; record only what is genuinely a *gain over an alternative* with evidence.

- [ ] **Step 2** — If a gain qualifies, write one corpus file: problem → symptom/signature → the fix → applicability matrix (light-similar: other VizX inline renderers; heavy-same+similar: N/A — these are driver-side light-tier-only viewers, so record "heavy not applicable" and why) → evidence (the offline test asserting no network calls + the embed-size math) → canonical code refs (`vizx/_pmtiles.py`, `vizx/_cog.py`). Create `docs/superpowers/performance/README.md` as the index if this is the first corpus entry.

- [ ] **Step 3** — Add the paired thin pointer memory (slug + one-line) that `[[links]]` to the corpus file. Keep it one line under ~200 chars (MEMORY.md is already near its size limit).

- [ ] **Step 4** — If no gain qualifies, record the assessment verdict ("no measurable rendering gain; the base64-embed pattern is a correctness/portability choice, not a speedup") in the SP2 plan-completion note and skip corpus/memory creation. Either way, the assessment is performed, not assumed.

- [ ] **Step 5** — Final full green run across both touched suites + commit any corpus/memory:

```
gbx:test:python --path python/geobrix/test/vizx/
gbx:test:python --path python/geobrix/test/pmtiles_light/
```
Expected: all green. Commit:

```
git add docs/superpowers/performance/ 2>/dev/null; \
git commit -m "docs(perf): capture PMTiles base64-embed rendering pattern

Co-authored-by: Isaac" || echo "no perf corpus entry (assessed: not a measurable gain)"
```

---

## Self-review against the spec (SP2 + cross-cutting)

- **Coverage of SP2 surface:** `plot_pmtiles` (Tasks 2-6, pinned signature), `plot_cog` (Task 7, pinned signature), `pmtiles_info` (Task 1, pinned `-> dict`) — all present. Files match the spec exactly: `vizx/_pmtiles.py`, `vizx/_cog.py`, `pmtiles/_inspect.py`; tests `test/vizx/test_pmtiles.py`, `test/vizx/test_cog.py`, plus the inspector test placed in the already-registered `test/pmtiles_light/` dir (matching the existing `pmtiles_light`/`pmtiles_bindings` layout) rather than a brand-new `test/pmtiles/` dir — avoids a CI-lock dir registration the spec's "vizx test dir already registered" note implies is the goal.
- **Interactive path** uses `_notebook_display_html()` + the IPython fallback chain (Task 6), pinned CDN versions (Task 3 constants), base64 in-browser `FileSource` + `pmtiles://` protocol registration (Task 3, asserted by tests).
- **Type detection** from header `tile_type` (Task 1 inspector → Task 2 `_is_raster_type`).
- **Size guard** compares the ~33%-inflated base64 size against `max_embed_mb` (Task 6).
- **Static fallbacks** reuse `plot_raster` (Task 4) and `plot_static` over contextily (Task 5); no new dep — `mapbox_vector_tile` is an existing pyvx dep, `contextily` an existing `[vizx]` dep.
- **No new deps** (Global Constraints); rio-tiler explicitly NOT adopted — the CI-lock checklist is documented as conditional only.
- **Placeholder scan:** no TODOs; no dead branches. `fallback` semantics corrected to interactive-by-default with auto-degrade (the size-guard tests + the no-fallback-raise test lock the contract).
- **Type consistency:** these are Python module functions, not registered SQL UDFs, so cross-language naming / `registered_functions.txt` parity does not apply (Task 9 Step 4 confirms no parity hook expects them).
- **TDD + per-task commits + commit hygiene** (≤72-char subjects, WHY bodies, `Co-authored-by: Isaac`) throughout.
- **Performance capture** step present (Task 10), assessment-not-assumed.
- **Docs voice** clean (Task 9).
