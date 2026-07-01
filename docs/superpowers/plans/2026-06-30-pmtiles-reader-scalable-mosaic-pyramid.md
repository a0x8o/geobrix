# `pmtiles_gbx` Reader — Scalable Per-Tile Mosaic Pyramid + Archive Read — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `reader()` to the `pmtiles_gbx` DataSource that produces a `(z,x,y,bytes)` tile stream — `source="raster"` builds an XYZ pyramid from source COGs via per-tile mosaic reads (scalable, seamless, memory-bounded), `source="archive"` reads an existing `.pmtiles` back to tiles.

**Architecture:** A pure, Spark-free core (`ds/_xyz_mosaic.py`) does tile enumeration (morecantile) + per-tile mosaic compositing (rio-tiler `mosaic_reader` over in-memory rasterio datasets). Two thin `DataSourceReader`s (raster, archive) wrap it; fan-out comes from `InputPartition`s (scan partitions, not AQE-coalesced). `PMTilesGbxDataSource.reader()` dispatches on the `source` option.

**Tech Stack:** Python 3.12, rio-tiler, morecantile, rasterio, the `pmtiles` lib, PySpark DataSource V2, pytest. Tests run via the project venv on the host.

## Global Constraints

- **Serverless-safe:** call `databricks.labs.gbx.pyrx._env.configure_gdal_env()` at the start of every executor `read()`; no `spark.conf.set`/`_jvm`/`sparkContext`/`.rdd`.
- **FUSE-safe:** read source/archive bytes **sequentially** (`open(path,"rb").read()`) into an in-memory dataset (`rasterio.io.MemoryFile`) / `pmtiles.reader.MemorySource`; never random-seek a Volume path.
- **Thread-safe:** `render_tile` calls `mosaic_reader(..., threads=0)` (serial) — rasterio datasets aren't safe for concurrent reads; the reader pre-loads source datasets single-threaded before rendering.
- **Output schema is exactly `(z int, x int, y int, bytes binary)`** — identical to the `pmtiles_gbx` writer's required input (`assert_input_schema`, `ds/pmtiles.py:55`), so reader → writer needs no glue.
- **No new SQL function**; do NOT touch `function-info.json` or `docs/tests-function-info/registered_functions.txt`.
- **Tests on the host venv:** `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate && python -m pytest <path> -v`.
- v1 raster mode targets uint8 imagery; `pixelSelection` v1 supports only `"first"`. OUT OF SCOPE: `source="vector"`, rescale-for-EO, infer-source-from-path, overview simplification, heavy-tier parity.

---

### Task 1: Pure per-tile mosaic core (`ds/_xyz_mosaic.py`)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/_xyz_mosaic.py`
- Test: `python/geobrix/test/ds/test_xyz_mosaic.py`

**Interfaces:**
- Produces:
  - `enumerate_tiles(bbox, min_z, max_z) -> list[tuple[int,int,int]]` — bbox is `(minx,miny,maxx,maxy)` EPSG:4326; returns `(z,x,y)` for every WebMercatorQuad tile intersecting bbox across `min_z..max_z`.
  - `source_bounds_union(paths) -> tuple[float,float,float,float]` — EPSG:4326 union of the rasters' bounds.
  - `render_tile(z, x, y, datasets, tile_format="PNG") -> bytes | None` — composite the open rasterio `datasets` for tile `(z,x,y)` via `mosaic_reader` (serial); PNG bytes, or `None` if no dataset covers the tile.

- [ ] **Step 1: Write the failing tests**

```python
# python/geobrix/test/ds/test_xyz_mosaic.py
import io
import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds
from PIL import Image

from databricks.labs.gbx.ds._xyz_mosaic import (
    enumerate_tiles, source_bounds_union, render_tile,
)


def _cog_bytes(w, s, e, n, px=128, val=200):
    """A uint8 RGB EPSG:4326 raster filling [w,s,e,n] with a constant value."""
    data = np.full((3, px, px), val, dtype="uint8")
    profile = dict(driver="GTiff", width=px, height=px, count=3, dtype="uint8",
                   crs="EPSG:4326", transform=from_bounds(w, s, e, n, px, px))
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data)
        return mf.read()


def _open(b):
    return MemoryFile(b).open()


def test_enumerate_tiles_covers_bbox():
    tiles = enumerate_tiles((-122.52, 37.70, -122.35, 37.83), 12, 13)
    zs = {z for z, x, y in tiles}
    assert zs == {12, 13}
    assert all(isinstance(v, int) for t in tiles for v in t)
    assert len(tiles) >= 4  # multiple tiles across the AOI


def test_source_bounds_union():
    a = _open(_cog_bytes(10.0, 50.0, 11.0, 51.0))
    b = _open(_cog_bytes(11.0, 50.0, 12.0, 51.0))
    try:
        u = source_bounds_union([a, b]) if False else None
    finally:
        a.close(); b.close()
    # union takes PATHS in production; here assert via the path-based helper below
    import tempfile, os
    paths = []
    for bb in (_cog_bytes(10.0, 50.0, 11.0, 51.0), _cog_bytes(11.0, 49.0, 12.0, 51.0)):
        fd, p = tempfile.mkstemp(suffix=".tif"); os.write(fd, bb); os.close(fd); paths.append(p)
    u = source_bounds_union(paths)
    assert u[0] == 10.0 and u[1] == 49.0 and u[2] == 12.0 and u[3] == 51.0


def test_render_tile_composites_all_covering_sources():
    # Two adjacent quads; a tile spanning the seam must composite BOTH (the cluster bug).
    left = _open(_cog_bytes(-122.50, 37.74, -122.45, 37.79, val=120))
    right = _open(_cog_bytes(-122.45, 37.74, -122.40, 37.79, val=220))
    try:
        import morecantile
        tms = morecantile.tms.get("WebMercatorQuad")
        # a high zoom tile near the seam lon=-122.45, lat~37.765
        t = next(iter(tms.tiles(-122.46, 37.76, -122.44, 37.77, [16])))
        png = render_tile(t.z, t.x, t.y, [left, right])
        assert png is not None
        arr = np.asarray(Image.open(io.BytesIO(png)).convert("RGBA"))
        assert float(np.mean(arr[:, :, 3] == 255)) > 0.99   # fully covered, no seam gap
        # both source values appear (left ~120, right ~220) -> composited from both
        lo = float(np.mean((arr[:, :, 0] > 90) & (arr[:, :, 0] < 150)))
        hi = float(np.mean(arr[:, :, 0] > 190))
        assert lo > 0 and hi > 0
    finally:
        left.close(); right.close()


def test_render_tile_none_when_no_source_covers():
    only = _open(_cog_bytes(-122.50, 37.74, -122.45, 37.79))
    try:
        import morecantile
        tms = morecantile.tms.get("WebMercatorQuad")
        far = next(iter(tms.tiles(10.0, 50.0, 10.1, 50.1, [16])))  # far away
        assert render_tile(far.z, far.x, far.y, [only]) is None
    finally:
        only.close()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate && python -m pytest python/geobrix/test/ds/test_xyz_mosaic.py -v`
Expected: FAIL — `ModuleNotFoundError: ... ds._xyz_mosaic`.

- [ ] **Step 3: Implement the core**

```python
# python/geobrix/src/databricks/labs/gbx/ds/_xyz_mosaic.py
"""Pure (Spark-free) per-tile XYZ mosaic core for the pmtiles_gbx raster reader.

Enumerate slippy-map tiles for an AOI (morecantile WebMercatorQuad) and render each
tile by compositing the covering source rasters with rio-tiler's mosaic_reader. No
full mosaic is built: each tile reads only its 256x256 window, so memory is bounded
per tile and the work distributes one (z,x,y) at a time.
"""

from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

BBox = Tuple[float, float, float, float]


def _tms():
    import morecantile

    return morecantile.tms.get("WebMercatorQuad")


def enumerate_tiles(bbox: BBox, min_z: int, max_z: int) -> List[Tuple[int, int, int]]:
    """Every (z, x, y) WebMercatorQuad tile intersecting bbox (EPSG:4326) across z."""
    tms = _tms()
    w, s, e, n = bbox
    out: List[Tuple[int, int, int]] = []
    for z in range(int(min_z), int(max_z) + 1):
        for t in tms.tiles(w, s, e, n, [z]):
            out.append((int(t.z), int(t.x), int(t.y)))
    return out


def source_bounds_union(paths: Sequence[str]) -> BBox:
    """EPSG:4326 union of the source rasters' bounds."""
    import rasterio
    from rasterio.warp import transform_bounds

    ws = ss = es = ns = None
    for p in paths:
        with rasterio.open(p) as ds:
            w, s, e, n = transform_bounds(ds.crs, "EPSG:4326", *ds.bounds)
        ws = w if ws is None else min(ws, w)
        ss = s if ss is None else min(ss, s)
        es = e if es is None else max(es, e)
        ns = n if ns is None else max(ns, n)
    if ws is None:
        raise ValueError("source_bounds_union: no source rasters")
    return (ws, ss, es, ns)


def render_tile(z, x, y, datasets, tile_format: str = "PNG") -> Optional[bytes]:
    """Composite the open rasterio `datasets` for tile (z,x,y); PNG bytes or None.

    Uses rio-tiler mosaic_reader serially (threads=0) — rasterio datasets are not safe
    for concurrent reads, and the inputs are already in memory so serial is cheap.
    Returns None when no dataset covers the tile (caller skips it).
    """
    from rio_tiler.errors import EmptyMosaicError
    from rio_tiler.io import Reader
    from rio_tiler.mosaic import mosaic_reader

    def _read(ds, tx, ty, tz):
        with Reader(None, dataset=ds) as cog:
            return cog.tile(tx, ty, tz)

    try:
        img, _ = mosaic_reader(list(datasets), _read, int(x), int(y), int(z), threads=0)
    except EmptyMosaicError:
        return None
    return bytes(img.render(img_format=tile_format))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate && python -m pytest python/geobrix/test/ds/test_xyz_mosaic.py -v`
Expected: PASS (4 tests). The composite test is the regression for the cluster western-quad bug.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects 2>/dev/null || true
git add python/geobrix/src/databricks/labs/gbx/ds/_xyz_mosaic.py python/geobrix/test/ds/test_xyz_mosaic.py
git commit -m "feat(ds): _xyz_mosaic core — per-tile mosaic compositing + tile enumeration

Spark-free core for the pmtiles_gbx raster reader: morecantile tile enumeration,
source-bounds union, and per-tile mosaic_reader compositing (serial, None on no
coverage). Unit-tested incl. the boundary-composite regression.

Co-authored-by: Isaac"
```

---

### Task 2: `source="raster"` reader (per-tile mosaic pyramid)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/_pmtiles_read.py` (the reader classes; keeps `ds/pmtiles.py` focused)
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/pmtiles.py` (add `reader()` dispatch to `PMTilesGbxDataSource`)
- Test: `python/geobrix/test/ds/test_pmtiles_reader.py`

**Interfaces:**
- Consumes: `_xyz_mosaic.enumerate_tiles`, `source_bounds_union`, `render_tile` (Task 1); `_listing.list_files` + `_listing.to_spark_uri` (`ds/_listing.py`, as used by `RasterGbxReader`); `databricks.labs.gbx.pyrx._env.configure_gdal_env`.
- Produces: `PMtilesRasterReader(DataSourceReader)`; `PMTilesGbxDataSource.reader(schema)` returns it when `options.get("source","raster")=="raster"`. Output rows `(z:int,x:int,y:int,bytes:binary)`.

- [ ] **Step 1: Write the failing integration tests**

```python
# python/geobrix/test/ds/test_pmtiles_reader.py
import io
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from PIL import Image

from databricks.labs.gbx.ds.pmtiles import PMTilesGbxDataSource


def _write_cog(path, w, s, e, n, px=256, val=200):
    data = np.full((3, px, px), val, dtype="uint8")
    with rasterio.open(path, "w", driver="GTiff", width=px, height=px, count=3,
                       dtype="uint8", crs="EPSG:4326",
                       transform=from_bounds(w, s, e, n, px, px)) as ds:
        ds.write(data)


def test_raster_reader_schema_and_fanout(spark, tmp_path):
    # two adjacent quads over a small AOI
    _write_cog(str(tmp_path / "a.tif"), -122.50, 37.74, -122.45, 37.79, val=120)
    _write_cog(str(tmp_path / "b.tif"), -122.45, 37.74, -122.40, 37.79, val=220)
    spark.dataSource.register(PMTilesGbxDataSource)
    df = (spark.read.format("pmtiles_gbx")
          .option("source", "raster").option("path", str(tmp_path))
          .option("bbox", "-122.50,37.74,-122.40,37.79")
          .option("minZoom", "14").option("maxZoom", "16")
          .option("tilesPerPartition", "20")
          .load())
    assert [f.name for f in df.schema.fields] == ["z", "x", "y", "bytes"]
    rows = df.collect()
    assert len(rows) > 0
    assert df.rdd.getNumPartitions() >= 2            # fans out via InputPartitions
    # no (z,x,y) duplicates (each tile produced once)
    keys = [(r["z"], r["x"], r["y"]) for r in rows]
    assert len(keys) == len(set(keys))


def test_raster_reader_composites_seam_tile(spark, tmp_path):
    _write_cog(str(tmp_path / "a.tif"), -122.50, 37.74, -122.45, 37.79, val=120)
    _write_cog(str(tmp_path / "b.tif"), -122.45, 37.74, -122.40, 37.79, val=220)
    spark.dataSource.register(PMTilesGbxDataSource)
    rows = (spark.read.format("pmtiles_gbx")
            .option("source", "raster").option("path", str(tmp_path))
            .option("bbox", "-122.50,37.74,-122.40,37.79")
            .option("minZoom", "16").option("maxZoom", "16")
            .load().collect())
    # find a fully-covered tile; assert it carries BOTH source values (composited)
    both = 0
    for r in rows:
        arr = np.asarray(Image.open(io.BytesIO(bytes(r["bytes"]))).convert("RGBA"))
        if float(np.mean(arr[:, :, 3] == 255)) > 0.99:
            lo = (arr[:, :, 0] > 90) & (arr[:, :, 0] < 150)
            hi = arr[:, :, 0] > 190
            if lo.any() and hi.any():
                both += 1
    assert both >= 1, "no tile composited both quads -> the western-quad bug"


def test_raster_reader_bbox_defaults_to_source_union(spark, tmp_path):
    _write_cog(str(tmp_path / "a.tif"), -122.50, 37.74, -122.45, 37.79)
    spark.dataSource.register(PMTilesGbxDataSource)
    df = (spark.read.format("pmtiles_gbx")
          .option("source", "raster").option("path", str(tmp_path))
          .option("minZoom", "15").option("maxZoom", "15").load())  # no bbox
    assert len(df.collect()) > 0
```

(The `spark` fixture comes from `python/geobrix/test/ds/conftest.py`.)

- [ ] **Step 2: Run to verify they fail**

Run: `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate && python -m pytest python/geobrix/test/ds/test_pmtiles_reader.py -v`
Expected: FAIL — `PMTilesGbxDataSource` has no `reader()` (Spark raises "does not support read" / `NotImplementedError`).

- [ ] **Step 3: Implement the raster reader**

Create `python/geobrix/src/databricks/labs/gbx/ds/_pmtiles_read.py`:

```python
"""DataSourceReaders for pmtiles_gbx: raster (per-tile mosaic pyramid) + archive."""

from __future__ import annotations

import re
from typing import Dict, Iterator, List, Sequence, Tuple

from pyspark.sql.datasource import DataSourceReader, InputPartition

from databricks.labs.gbx.ds import _listing, _xyz_mosaic


class _TilesPartition(InputPartition):
    def __init__(self, tiles: List[Tuple[int, int, int]], sources: List[str]):
        self.tiles = tiles
        self.sources = sources


def _chunk(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


class PMtilesRasterReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("pmtiles_gbx raster reader requires a 'path' (dir of COGs).")
        self.filter_regex = options.get("filterRegex", r".*\.tif$")
        self.min_z = int(options.get("minZoom", "0"))
        self.max_z = int(options.get("maxZoom", "0"))
        self.tiles_per_partition = int(options.get("tilesPerPartition", "64"))
        self.tile_format = options.get("tileFormat", "png").upper()
        ps = options.get("pixelSelection", "first").lower()
        if ps != "first":
            raise ValueError(f"pmtiles_gbx v1 supports pixelSelection='first' only; got {ps!r}")
        bbox_opt = options.get("bbox")
        self.bbox = (
            tuple(float(v) for v in bbox_opt.split(",")) if bbox_opt else None
        )
        if self.bbox is not None and len(self.bbox) != 4:
            raise ValueError("pmtiles_gbx bbox must be 'minx,miny,maxx,maxy'")

    def partitions(self) -> Sequence[InputPartition]:
        import rasterio
        from rasterio.warp import transform_bounds

        sources = _listing.list_files(self.path, self.filter_regex)
        if not sources:
            raise ValueError(f"pmtiles_gbx raster reader: no rasters under {self.path}")
        bbox = self.bbox or _xyz_mosaic.source_bounds_union(sources)
        # per-source WGS84 bounds, to attach only intersecting sources to each chunk
        src_bounds = []
        for p in sources:
            with rasterio.open(p) as ds:
                src_bounds.append((p, transform_bounds(ds.crs, "EPSG:4326", *ds.bounds)))
        tiles = _xyz_mosaic.enumerate_tiles(bbox, self.min_z, self.max_z)
        # spatial grouping: sort by (z, y, x) so chunks are contiguous
        tiles.sort()
        parts: List[InputPartition] = []
        tms = _xyz_mosaic._tms()
        for chunk in _chunk(tiles, self.tiles_per_partition):
            # combined WGS84 bbox of the chunk's tiles
            cb = [tms.bounds(__import__("morecantile").Tile(x, y, z)) for z, x, y in chunk]
            cw = min(b.left for b in cb); cs = min(b.bottom for b in cb)
            ce = max(b.right for b in cb); cn = max(b.top for b in cb)
            needed = [p for p, (w, s, e, n) in src_bounds
                      if not (e < cw or w > ce or n < cs or s > cn)]
            parts.append(_TilesPartition(chunk, needed or sources))
        return parts

    def read(self, partition: "_TilesPartition") -> Iterator[Tuple]:
        import rasterio
        from rasterio.io import MemoryFile

        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # FUSE-safe: sequential byte read -> in-memory dataset, pre-loaded single-threaded
        mfs, datasets = [], []
        try:
            for p in partition.sources:
                with open(p, "rb") as fh:
                    mf = MemoryFile(fh.read())
                mfs.append(mf)
                datasets.append(mf.open())
            for (z, x, y) in partition.tiles:
                png = _xyz_mosaic.render_tile(z, x, y, datasets, tile_format=self.tile_format)
                if png is not None:
                    yield (z, x, y, png)
        finally:
            for ds in datasets:
                ds.close()
            for mf in mfs:
                mf.close()
```

In `python/geobrix/src/databricks/labs/gbx/ds/pmtiles.py`, add a `reader()` to `PMTilesGbxDataSource` (the class at line 70; it already has `name()`, `schema()`, `writer()`):

```python
    def reader(self, schema: StructType) -> "DataSourceReader":  # noqa: F821
        # Per-branch (lazy) imports so the raster reader works before the archive
        # reader exists (Task 2 lands before Task 3).
        source = self.options.get("source", "raster").lower()
        if source == "raster":
            from databricks.labs.gbx.ds._pmtiles_read import PMtilesRasterReader
            return PMtilesRasterReader(self.options)
        if source == "archive":
            from databricks.labs.gbx.ds._pmtiles_read import PMtilesArchiveReader
            return PMtilesArchiveReader(self.options)
        raise ValueError(f"pmtiles_gbx: unknown source={source!r} (use 'raster' or 'archive')")
```

(Add `from pyspark.sql.datasource import DataSourceReader` to the imports if not present. `schema()` already returns `(z,x,y,bytes)` — the shared read/write schema; confirm and reuse it.)

- [ ] **Step 4: Run the raster tests to verify they pass**

Run: `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate && python -m pytest python/geobrix/test/ds/test_pmtiles_reader.py -v -k raster`
Expected: PASS. (Task 3 adds the archive test to the same file.)

- [ ] **Step 5: Run the existing pmtiles writer tests for regression**

Run: `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate && python -m pytest python/geobrix/test/ds/test_pmtiles_parity.py python/geobrix/test/ds/test_pmtiles.py -v`
Expected: PASS (the writer + schema are unchanged; `reader()` is additive).

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects 2>/dev/null || true
git add python/geobrix/src/databricks/labs/gbx/ds/_pmtiles_read.py python/geobrix/src/databricks/labs/gbx/ds/pmtiles.py python/geobrix/test/ds/test_pmtiles_reader.py
git commit -m "feat(ds): pmtiles_gbx source=raster reader (scalable per-tile mosaic pyramid)

reader() dispatches on source; the raster reader enumerates tiles, groups them into
InputPartitions (fan-out, AQE-coalesce-proof), and renders each via the _xyz_mosaic
core with configure_gdal_env + FUSE-safe pre-loaded in-memory datasets. Output schema
matches the writer so reader->writer needs no glue.

Co-authored-by: Isaac"
```

---

### Task 3: `source="archive"` reader (read an existing `.pmtiles`)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/_pmtiles_read.py` (add `PMtilesArchiveReader`)
- Test: `python/geobrix/test/ds/test_pmtiles_reader.py` (add round-trip test)

**Interfaces:**
- Consumes: the `pmtiles` lib (`pmtiles.reader.MemorySource`, `all_tiles`); the `pmtiles_gbx` writer (to build a fixture archive for the round-trip).
- Produces: `PMtilesArchiveReader(DataSourceReader)` — reads a `.pmtiles` file → `(z,x,y,bytes)`.

- [ ] **Step 1: Write the failing round-trip test**

```python
# add to python/geobrix/test/ds/test_pmtiles_reader.py
def test_archive_reader_roundtrip(spark, tmp_path):
    from pyspark.sql import Row
    spark.dataSource.register(PMTilesGbxDataSource)
    # build a small archive via the writer
    tiles = [Row(z=14, x=2615 + i, y=6330, bytes=_png_bytes(i)) for i in range(3)]
    out = str(tmp_path / "rt.pmtiles")
    spark.createDataFrame(tiles).write.format("pmtiles_gbx").option("shardZoom", "0").mode("overwrite").save(out)
    # read it back
    back = (spark.read.format("pmtiles_gbx").option("source", "archive").option("path", out).load().collect())
    got = {(r["z"], r["x"], r["y"]): bytes(r["bytes"]) for r in back}
    assert set(got) == {(14, 2615, 6330), (14, 2616, 6330), (14, 2617, 6330)}


def _png_bytes(i):
    import io
    import numpy as np
    from PIL import Image
    a = np.full((256, 256, 3), 40 + i * 20, dtype="uint8")
    buf = io.BytesIO(); Image.fromarray(a).save(buf, format="PNG"); return buf.getvalue()
```

- [ ] **Step 2: Run to verify it fails**

Run: `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate && python -m pytest python/geobrix/test/ds/test_pmtiles_reader.py::test_archive_reader_roundtrip -v`
Expected: FAIL — `ImportError: cannot import name 'PMtilesArchiveReader'` (referenced by `reader()` but not yet defined).

- [ ] **Step 3: Implement the archive reader**

Append to `ds/_pmtiles_read.py`:

```python
class PMtilesArchiveReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("pmtiles_gbx archive reader requires a 'path' (.pmtiles file).")
        self.tiles_per_partition = int(options.get("tilesPerPartition", "2048"))

    def _entries(self) -> List[Tuple[int, int, int]]:
        from pmtiles.reader import MemorySource, all_tiles

        with open(self.path, "rb") as fh:
            raw = fh.read()
        return [(z, x, y) for (z, x, y), _ in all_tiles(MemorySource(raw))]

    def partitions(self) -> Sequence[InputPartition]:
        entries = self._entries()
        return [_TilesPartition(list(c), [self.path]) for c in _chunk(entries, self.tiles_per_partition)]

    def read(self, partition: "_TilesPartition") -> Iterator[Tuple]:
        from pmtiles.reader import MemorySource, Reader

        # FUSE-safe: read archive bytes sequentially, then serve tiles in memory
        with open(partition.sources[0], "rb") as fh:
            raw = fh.read()
        reader = Reader(MemorySource(raw))
        for (z, x, y) in partition.tiles:
            data = reader.get(z, x, y)
            if data is not None:
                yield (z, x, y, bytes(data))
```

(Confirm the `pmtiles` lib's tile-fetch API — `Reader(MemorySource(raw)).get(z, x, y)`; if the installed version exposes it differently, mirror the call used elsewhere in `ds/pmtiles.py`/tests, e.g. iterate `all_tiles` and select. Keep the FUSE-safe sequential read either way.)

- [ ] **Step 4: Run the round-trip test to verify it passes**

Run: `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate && python -m pytest python/geobrix/test/ds/test_pmtiles_reader.py -v`
Expected: PASS (all raster + archive tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects 2>/dev/null || true
git add python/geobrix/src/databricks/labs/gbx/ds/_pmtiles_read.py python/geobrix/test/ds/test_pmtiles_reader.py
git commit -m "feat(ds): pmtiles_gbx source=archive reader (read existing .pmtiles -> tiles)

Round-trips with the writer; FUSE-safe sequential archive read via the pmtiles lib
MemorySource. Completes the read/write symmetry of the pmtiles_gbx tile family.

Co-authored-by: Isaac"
```

---

## Self-Review

**1. Spec coverage:**
- Pure core (enumerate/union/render) + boundary-composite regression → Task 1. ✓
- `source="raster"` reader (options, partitions fan-out, configure_gdal_env, FUSE-safe pre-loaded cache, skip-empty, schema==writer) → Task 2. ✓
- `source="archive"` reader (read existing .pmtiles, round-trip) → Task 3. ✓
- `reader()` dispatch on `source`; shared `schema()`; no register change → Task 2. ✓
- Serverless-safe / FUSE-safe / thread-safe (threads=0) / no SQL function → Global Constraints + baked into each task. ✓
- Out of scope (vector, rescale, infer-path, overview simplification, heavy parity) → Global Constraints. ✓

**2. Placeholder scan:** No TBD/TODO. The two "confirm the exact API" notes (schema() reuse; pmtiles `.get` vs `all_tiles`) name the precise file/call to check and a concrete fallback — not open-ended placeholders.

**3. Type consistency:** `enumerate_tiles`/`source_bounds_union`/`render_tile` signatures match between Task 1 (definition) and Task 2 (use). `_TilesPartition(tiles, sources)` is defined in Task 2 and reused in Task 3. Output rows `(z,x,y,bytes)` consistent across both readers and equal to the writer's `assert_input_schema`. `reader()` (Task 2) references `PMtilesArchiveReader` delivered in Task 3 — Task 2's raster tests run with `-k raster` so the missing import doesn't block; Task 3 completes it (noted in Task 2 Step 4).
