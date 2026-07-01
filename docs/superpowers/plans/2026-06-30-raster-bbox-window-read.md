# Raster `bbox` Window-on-Read Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a correct-by-construction `bbox` window-on-read option to the light raster readers (`raster_gbx`/`gtiff_gbx`) and a `StacClient.download(bbox=)` companion, so customers stop hand-rolling rasterio windowed reads (and the clip-vs-`window_transform` georef footgun).

**Architecture:** One shared geometry primitive `ds/_window.py: window_for_bbox(src, bbox, bbox_crs)` returns a **clipped, integer, in-bounds `rasterio.windows.Window`** (or `None` on no-overlap). Because the window is guaranteed in-bounds, any caller doing `ds.read(win)` + `ds.window_transform(win)` gets agreeing pixels and georeference — the footgun cannot occur. The reader hands that window to the existing `_encode.encode_tile`; `StacClient`'s fetch opens the (signed) href with rasterio (`/vsicurl` for https → only AOI byte ranges), windows it, and writes a windowed GeoTIFF.

**Tech Stack:** Python 3.12, rasterio, PySpark DataSource V2, pytest. Tests run via `gbx:test:python --path python/geobrix/test/ds/` and `.../test/stac/`.

> **Refinement vs the committed spec:** the spec sketched the primitive returning `(data, transform, profile)`; this plan returns the clipped **`Window`** instead, so the reader reuses `_encode.encode_tile` (no duplicate encode logic) and the StacClient does its own read+write. The correctness principle is identical — the clip is centralized in the primitive, and an in-bounds window makes read/transform agree everywhere.

## Global Constraints

- **Serverless-safe (light tier):** no `spark.conf.set`, no `_jvm`/`sparkContext`/`.rdd`; GDAL only via `rasterio` (never raw `osgeo.gdal`); call `databricks.labs.gbx.pyrx._env.configure_gdal_env()` before executor reads (the reader already does).
- **No new SQL function:** reader/client option surface only — do NOT touch `function-info.json` or `docs/tests-function-info/registered_functions.txt`.
- **`bbox` string format:** `"minx,miny,maxx,maxy"` (same as the vector reader, `ds/vector.py:540`). A non-4 value raises `ValueError`.
- **CRS convention:** plain `bbox` is in the source CRS; `bboxCrs` (e.g. `"EPSG:4326"`) declares the bbox CRS and the primitive reprojects the bbox to the source CRS (mirrors `rst_clip`'s SRID rule).
- **Georef correctness is the headline:** the window is clipped to the dataset BEFORE the transform is derived; an overhang regression test is required.
- **Volume FUSE can't seek:** windowed reads in the reader must stage the source to worker-local disk with a sequential copy first (the reader's phase-2 already does this — mirror it for the bbox path).
- **Non-overlap:** reader yields no tile for a non-overlapping source file (skip); `StacClient.download` with a non-overlapping bbox raises a clear error.
- **Tier scope:** light only. Heavy `gdal`/`gtiff_gdal` parity, vector-reader `bboxCrs` backport, on-read decimation (`rst_resample_to_res` exists), and the Helios notebook rework are OUT OF SCOPE.

## File Structure

- `python/geobrix/src/databricks/labs/gbx/ds/_window.py` — **new**; the `window_for_bbox` primitive. One responsibility: bbox → clipped in-bounds Window.
- `python/geobrix/test/ds/test_window.py` — **new**; primitive unit tests (overhang regression, CRS transform, no-overlap).
- `python/geobrix/src/databricks/labs/gbx/ds/raster.py` — modify `RasterGbxReader.__init__` (parse `bbox`/`bboxCrs`) and `RasterGbxReader.read` (bbox branch: stage-to-local + window + encode). `GTiffGbxReader` inherits both (ds/gtiff.py:16), so `gtiff_gbx` gets the option for free.
- `python/geobrix/test/ds/test_raster_bbox.py` — **new**; Spark integration tests for `raster_gbx` + `gtiff_gbx`.
- `python/geobrix/src/databricks/labs/gbx/stac/_download.py` — modify `fetch_validate_publish` (add `bbox`/`bbox_crs`; windowed-fetch branch).
- `python/geobrix/src/databricks/labs/gbx/stac/client.py` — modify `StacClient.download` (add `bbox`/`bbox_crs`, thread into the `_fetch` UDF).
- `python/geobrix/test/stac/test_download_bbox.py` — **new**; windowed fetch tests (local file as href).

---

### Task 1: `window_for_bbox` primitive

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/ds/_window.py`
- Test: `python/geobrix/test/ds/test_window.py`

**Interfaces:**
- Produces: `window_for_bbox(src, bbox: tuple[float,float,float,float], bbox_crs: str | None = None) -> rasterio.windows.Window | None`. `src` is an open `rasterio.DatasetReader`. Returns a clipped, integer, in-bounds Window, or `None` if the bbox does not overlap `src`.

- [ ] **Step 1: Write the failing tests**

```python
# python/geobrix/test/ds/test_window.py
import rasterio
from rasterio.io import MemoryFile
from rasterio.warp import transform_bounds

from databricks.labs.gbx.ds._window import window_for_bbox
from databricks.labs.gbx.test.ds.conftest import make_geotiff_bytes  # if importable; else use the local helper below


def _open(width=4, height=3, epsg=4326):
    # Fixture extent: origin (10.0, 50.0), 0.5 px -> x[10,12], y[48.5,50] in EPSG:4326.
    mf = MemoryFile(make_geotiff_bytes(width=width, height=height, epsg=epsg))
    return mf, mf.open()


def test_fully_inside_window_matches_bbox():
    mf, ds = _open()
    try:
        win = window_for_bbox(ds, (10.5, 49.0, 11.5, 50.0))  # inside x[10,12], y[48.5,50]
        assert win is not None
        b = rasterio.windows.bounds(win, ds.transform)  # (left, bottom, right, top)
        assert b == (10.5, 49.0, 11.5, 50.0)
    finally:
        ds.close(); mf.close()


def test_north_overhang_is_clipped_not_shifted():
    # Regression for the NB-02 georef bug: a bbox whose top (51.0) is north of the
    # dataset top (50.0) must clip to the dataset top, NOT report row 0 at 51.0.
    mf, ds = _open()
    try:
        win = window_for_bbox(ds, (10.5, 49.0, 11.5, 51.0))
        assert win is not None
        top = rasterio.windows.bounds(win, ds.transform)[3]
        assert top == 50.0, f"top should clip to dataset top 50.0, got {top}"
        assert win.row_off == 0
    finally:
        ds.close(); mf.close()


def test_no_overlap_returns_none():
    mf, ds = _open()
    try:
        assert window_for_bbox(ds, (20.0, 20.0, 21.0, 21.0)) is None
    finally:
        ds.close(); mf.close()


def test_bbox_crs_is_reprojected():
    # Source in EPSG:3857 over a known SF extent; a WGS84 bbox inside it must be
    # transformed to 3857 before windowing (proves bbox_crs is applied).
    w, s, e, n = transform_bounds("EPSG:4326", "EPSG:3857", -122.5, 37.7, -122.4, 37.8)
    from rasterio.transform import from_bounds as _affine_from_bounds
    profile = dict(driver="GTiff", width=100, height=100, count=1, dtype="uint8",
                   crs="EPSG:3857", transform=_affine_from_bounds(w, s, e, n, 100, 100))
    with MemoryFile() as src_mf:
        with src_mf.open(**profile) as out:
            import numpy as np
            out.write(np.zeros((1, 100, 100), dtype="uint8"))
        data = src_mf.read()
    mf = MemoryFile(data); ds = mf.open()
    try:
        win = window_for_bbox(ds, (-122.47, 37.72, -122.43, 37.78), bbox_crs="EPSG:4326")
        assert win is not None
        b = rasterio.windows.bounds(win, ds.transform)  # in source CRS (3857)
        exp = transform_bounds("EPSG:4326", "EPSG:3857", -122.47, 37.72, -122.43, 37.78)
        # within one source pixel (rounding to whole pixels)
        px = abs(ds.transform.a)
        assert all(abs(a - c) <= px for a, c in zip(b, exp))
    finally:
        ds.close(); mf.close()
```

(If `make_geotiff_bytes` is not importable as a module path, copy the 12-line builder from `python/geobrix/test/ds/conftest.py:58` into this test file.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_window.py`
Expected: FAIL with `ModuleNotFoundError: ... ds._window` / `ImportError: cannot import name 'window_for_bbox'`.

- [ ] **Step 3: Implement the primitive**

```python
# python/geobrix/src/databricks/labs/gbx/ds/_window.py
"""Clip-safe AOI windowing for raster reads.

window_for_bbox computes the pixel Window of a dataset covering a bbox and CLIPS it
to the dataset BEFORE returning it. Callers do ds.read(win) + ds.window_transform(win)
on an in-bounds window, so pixels and georeference always agree -- the clip-vs-
window_transform footgun (read clips to the dataset, but window_transform used the
unclipped window's origin -> raster shifted by the overhang) cannot occur.
"""

from __future__ import annotations

from typing import Optional, Tuple

from rasterio.windows import Window, from_bounds as _from_bounds


def window_for_bbox(
    src,
    bbox: Tuple[float, float, float, float],
    bbox_crs: Optional[str] = None,
) -> Optional[Window]:
    """Clipped, integer, in-bounds Window of ``src`` covering ``bbox``.

    bbox is (minx, miny, maxx, maxy). bbox_crs (e.g. "EPSG:4326") declares the bbox
    CRS; None means the bbox is already in src.crs. Returns None if the bbox does not
    overlap the dataset.
    """
    minx, miny, maxx, maxy = bbox
    if bbox_crs is not None and str(bbox_crs) != str(src.crs):
        from rasterio.warp import transform_bounds as _transform_bounds

        minx, miny, maxx, maxy = _transform_bounds(
            bbox_crs, src.crs, minx, miny, maxx, maxy
        )
    win = _from_bounds(minx, miny, maxx, maxy, transform=src.transform)
    # Whole-pixel coverage of the bbox, then clip to the dataset extent.
    win = win.round_offsets(op="floor").round_lengths(op="ceil")
    try:
        win = win.intersection(Window(0, 0, src.width, src.height))
    except Exception:  # rasterio.errors.WindowError when disjoint
        return None
    if win.width < 1 or win.height < 1:
        return None
    return Window(int(win.col_off), int(win.row_off), int(win.width), int(win.height))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_window.py`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects 2>/dev/null || true
git add python/geobrix/src/databricks/labs/gbx/ds/_window.py python/geobrix/test/ds/test_window.py
git commit -m "feat(ds): window_for_bbox clip-safe AOI windowing primitive

Returns a clipped, in-bounds rasterio Window so read + window_transform agree
(fixes the clip-vs-window_transform georef footgun class). Source-CRS bbox with
an optional bbox_crs reprojection.

Co-authored-by: Isaac"
```

---

### Task 2: `bbox`/`bboxCrs` options on the raster readers

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py` (`RasterGbxReader.__init__` ~line 76-81; `RasterGbxReader.read` ~line 87)
- Test: `python/geobrix/test/ds/test_raster_bbox.py`

**Interfaces:**
- Consumes: `window_for_bbox` (Task 1); existing `_encode.encode_tile(ds, window=(col,row,w,h), source_path, all_parents)` and `_listing.to_spark_uri`.
- Produces: reader options `.option("bbox", "minx,miny,maxx,maxy")` and `.option("bboxCrs", "EPSG:4326")` on `raster_gbx` and (inherited) `gtiff_gbx`.

- [ ] **Step 1: Write the failing integration tests**

```python
# python/geobrix/test/ds/test_raster_bbox.py
import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.ds.gtiff import GTiffGbxDataSource


def _write_sample(path, width=4, height=3, epsg=4326):
    # extent: origin (10.0, 50.0), 0.5 px -> x[10,12], y[48.5,50]
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with rasterio.open(path, "w", driver="GTiff", width=width, height=height, count=1,
                       dtype="float32", crs=f"EPSG:{epsg}",
                       transform=from_origin(10.0, 50.0, 0.5, 0.5), nodata=-9999.0) as ds:
        ds.write(data, 1)


def _tile_bounds(row):
    with MemoryFile(bytes(row["tile"]["raster"])) as mf, mf.open() as out:
        b = out.bounds
        return (b.left, b.bottom, b.right, b.top), (out.width, out.height)


def test_bbox_windows_to_aoi(spark, tmp_path):
    f = tmp_path / "s.tif"; _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").option("bbox", "10.5,49.0,11.5,50.0").load(str(f))
    rows = df.collect()
    assert len(rows) == 1
    bounds, (w, h) = _tile_bounds(rows[0])
    assert bounds == (10.5, 49.0, 11.5, 50.0)
    assert (w, h) == (2, 2)  # 1.0 deg / 0.5 px


def test_bbox_north_overhang_clips(spark, tmp_path):
    f = tmp_path / "s.tif"; _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").option("bbox", "10.5,49.0,11.5,51.0").load(str(f))
    bounds, _ = _tile_bounds(df.collect()[0])
    assert bounds[3] == 50.0  # top clipped to dataset top, not 51.0


def test_non_overlapping_file_is_skipped(spark, tmp_path):
    f = tmp_path / "s.tif"; _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").option("bbox", "20,20,21,21").load(str(f))
    assert df.collect() == []


def test_gtiff_gbx_parity(spark, tmp_path):
    f = tmp_path / "s.tif"; _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    opt = ("bbox", "10.5,49.0,11.5,50.0")
    r1 = spark.read.format("raster_gbx").option(*opt).load(str(f)).collect()[0]
    r2 = spark.read.format("gtiff_gbx").option(*opt).load(str(f)).collect()[0]
    assert _tile_bounds(r1) == _tile_bounds(r2)


def test_malformed_bbox_raises(spark, tmp_path):
    f = tmp_path / "s.tif"; _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    import pytest
    with pytest.raises(Exception):
        spark.read.format("raster_gbx").option("bbox", "1,2,3").load(str(f)).collect()
```

(The `spark` fixture is provided by `python/geobrix/test/ds/conftest.py`.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_raster_bbox.py`
Expected: FAIL — `bbox` is ignored, so `test_bbox_windows_to_aoi` returns the whole 4x3 raster (bounds `(10,48.5,12,50)`, not `(10.5,49,11.5,50)`), and `test_non_overlapping_file_is_skipped` returns 1 row.

- [ ] **Step 3: Parse the options in `RasterGbxReader.__init__`**

In `python/geobrix/src/databricks/labs/gbx/ds/raster.py`, extend `__init__` (after the `filter_regex` line, ~line 81):

```python
        self.filter_regex = options.get("filterRegex", ".*")
        # Optional AOI window-on-read. `bbox` is "minx,miny,maxx,maxy" in the source
        # CRS by default; `bboxCrs` (e.g. "EPSG:4326") declares the bbox CRS and the
        # window primitive reprojects it. None = read the whole raster (prior behavior).
        bbox_opt = options.get("bbox")
        if bbox_opt:
            parts = [float(v) for v in str(bbox_opt).split(",")]
            if len(parts) != 4:
                raise ValueError(
                    "raster bbox option must be 'minx,miny,maxx,maxy'; got "
                    f"'{bbox_opt}'"
                )
            self.bbox = tuple(parts)
        else:
            self.bbox = None
        self.bbox_crs = options.get("bboxCrs")
```

- [ ] **Step 4: Add the bbox branch to `RasterGbxReader.read`**

In `read`, right after `source = _listing.to_spark_uri(partition.file_path)` (~line 102) and before the `size_bytes = ...` line, insert the short-circuit branch:

```python
        # AOI window-on-read: stage to worker-local disk (FUSE-safe sequential copy --
        # Volume FUSE cannot serve the per-window seeks), then window from local disk.
        # bbox disables the whole-image fast path and the multi-tile split.
        if self.bbox is not None:
            from databricks.labs.gbx.ds._window import window_for_bbox

            staged_dir = tempfile.mkdtemp(prefix="gbx_raster_")
            try:
                local_path = os.path.join(
                    staged_dir, os.path.basename(partition.file_path) or "raster.tif"
                )
                with (
                    open(partition.file_path, "rb") as _src,
                    open(local_path, "wb") as _dst,
                ):
                    shutil.copyfileobj(_src, _dst, length=8 * 1024 * 1024)
                with rasterio.open(local_path) as ds:
                    win = window_for_bbox(ds, self.bbox, self.bbox_crs)
                    if win is None:
                        return  # source does not overlap the AOI -> emit nothing
                    cellid, raster_bytes, meta = _encode.encode_tile(
                        ds,
                        window=(
                            int(win.col_off),
                            int(win.row_off),
                            int(win.width),
                            int(win.height),
                        ),
                        source_path=partition.file_path,
                        all_parents="",
                    )
                    yield (source, (cellid, raster_bytes, meta))
            finally:
                shutil.rmtree(staged_dir, ignore_errors=True)
            return
```

(`os`, `shutil`, `tempfile`, and `rasterio` are already imported at the top of `read`.)

- [ ] **Step 5: Run the tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_raster_bbox.py`
Expected: PASS (5 tests).

- [ ] **Step 6: Run the existing raster reader tests to confirm no regression**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_raster_datasource.py`
Expected: PASS (the no-bbox path is unchanged — `self.bbox is None` short-circuits nothing).

- [ ] **Step 7: Commit**

```bash
chmod -R u+rwX .git/objects 2>/dev/null || true
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_bbox.py
git commit -m "feat(ds): bbox/bboxCrs window-on-read on raster_gbx/gtiff_gbx

A bbox option windows each source to the AOI via window_for_bbox (clip-safe),
staging to local disk first (FUSE-safe). Non-overlapping sources are skipped.
gtiff_gbx inherits the option. Mirrors the vector reader's bbox; source-CRS
default with a bboxCrs override.

Co-authored-by: Isaac"
```

---

### Task 3: `StacClient.download(bbox=, bbox_crs=)` windowed fetch

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/stac/_download.py` (`fetch_validate_publish`)
- Modify: `python/geobrix/src/databricks/labs/gbx/stac/client.py` (`StacClient.download`, ~line 209)
- Test: `python/geobrix/test/stac/test_download_bbox.py`

**Interfaces:**
- Consumes: `window_for_bbox` (Task 1).
- Produces: `fetch_validate_publish(..., bbox=None, bbox_crs=None)` and `StacClient.download(..., bbox=None, bbox_crs=None)`. When `bbox` is set, the fetch opens the (signed) href with rasterio (`/vsicurl` for https → AOI byte ranges only), windows it, and writes a windowed GeoTIFF; a non-overlapping bbox raises `ValueError`.

- [ ] **Step 1: Write the failing tests**

```python
# python/geobrix/test/stac/test_download_bbox.py
import os

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.stac._download import fetch_validate_publish


def _write_gtiff(path, width=8, height=8):
    # extent: origin (0, 8), 1.0 px -> x[0,8], y[0,8]
    with rasterio.open(path, "w", driver="GTiff", height=height, width=width, count=1,
                       dtype="uint8", crs="EPSG:4326", transform=from_origin(0, 8, 1, 1)) as dst:
        dst.write(np.arange(width * height, dtype="uint8").reshape(1, height, width))


def test_windowed_fetch_clips_to_bbox(tmp_path):
    src = tmp_path / "src.tif"; _write_gtiff(str(src))
    out_dir = tmp_path / "out"
    res = fetch_validate_publish(
        lambda: str(src), str(out_dir), "win.tif", bbox=(2, 2, 5, 6)
    )
    assert res == os.path.join(str(out_dir), "win.tif")
    with rasterio.open(res) as ds:
        b = ds.bounds
        assert (b.left, b.bottom, b.right, b.top) == (2, 2, 5, 6)
        assert (ds.width, ds.height) == (3, 4)


def test_windowed_fetch_north_overhang_clips(tmp_path):
    src = tmp_path / "src.tif"; _write_gtiff(str(src))
    out_dir = tmp_path / "out"
    res = fetch_validate_publish(
        lambda: str(src), str(out_dir), "win.tif", bbox=(2, 2, 10, 12)  # E+N overhang
    )
    with rasterio.open(res) as ds:
        assert ds.bounds.top == 8  # clipped to dataset top, not 12
        assert ds.bounds.right == 8


def test_windowed_fetch_no_overlap_raises(tmp_path):
    src = tmp_path / "src.tif"; _write_gtiff(str(src))
    out_dir = tmp_path / "out"
    res = fetch_validate_publish(
        lambda: str(src), str(out_dir), "win.tif", bbox=(20, 20, 21, 21), max_tries=1
    )
    assert res is None  # no overlap -> all attempts fail -> None (no file published)
    assert not os.path.exists(os.path.join(str(out_dir), "win.tif"))


def test_no_bbox_path_unchanged(tmp_path):
    # bbox=None must keep the byte-faithful download path.
    src = tmp_path / "src.tif"; _write_gtiff(str(src))
    out_dir = tmp_path / "out"

    def get(href, timeout=None, stream=None):
        class R:
            def raise_for_status(self): pass
            def iter_content(self, n): yield open(str(src), "rb").read()
        return R()

    res = fetch_validate_publish(lambda: "http://x/ok.tif", str(out_dir), "ok.tif", get=get)
    assert res == os.path.join(str(out_dir), "ok.tif")
    assert os.path.getsize(res) == os.path.getsize(str(src))  # byte-identical (no window)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/stac/test_download_bbox.py`
Expected: FAIL — `fetch_validate_publish` has no `bbox` parameter (`TypeError: unexpected keyword argument 'bbox'`).

- [ ] **Step 3: Add the windowed-fetch branch to `fetch_validate_publish`**

In `python/geobrix/src/databricks/labs/gbx/stac/_download.py`, add a helper and extend the signature + the per-attempt fetch:

```python
def windowed_download(href: str, outpath: str, bbox, bbox_crs=None) -> str:
    """Open href (rasterio /vsicurl for https; any path locally), window to bbox, and
    write a windowed GeoTIFF. The window is clipped to the dataset, so the output is
    correctly georeferenced. Raises ValueError if the bbox does not overlap the asset."""
    import rasterio

    from databricks.labs.gbx.ds._window import window_for_bbox

    with rasterio.open(href) as src:
        win = window_for_bbox(src, bbox, bbox_crs)
        if win is None:
            raise ValueError(f"bbox {bbox} does not overlap the asset {href!r}")
        data = src.read(window=win)
        profile = src.profile.copy()
        profile.update(
            driver="GTiff",
            width=int(win.width),
            height=int(win.height),
            transform=src.window_transform(win),
        )
        with rasterio.open(outpath, "w", **profile) as dst:
            dst.write(data)
    return outpath
```

Update `fetch_validate_publish`'s signature to add `bbox=None, bbox_crs=None` (after `validate: bool = True`), and replace the per-attempt download line:

```python
            local = os.path.join(tmpd, safe_filename)
            if bbox is not None:
                # Windowed read decodes-on-read; a successful write IS the validation,
                # so publish directly (skip the separate read_validate window-decode).
                windowed_download(href_fn(), local, bbox, bbox_crs)
                shutil.copyfile(local, outpath)
                return outpath
            download_href(href_fn(), local, get=get)
            if validate:
                if read_validate(local):
                    shutil.copyfile(local, outpath)  # publish only validated files
                    return outpath
            else:
                shutil.copyfile(local, outpath)
                return outpath
```

(The existing idempotency short-circuit at the top still applies; a windowed re-run that finds a valid `outpath` returns it.)

- [ ] **Step 4: Run the `_download` tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/stac/test_download_bbox.py`
Expected: PASS (4 tests).

- [ ] **Step 5: Thread `bbox`/`bbox_crs` through `StacClient.download`**

In `python/geobrix/src/databricks/labs/gbx/stac/client.py`, add `bbox: Optional[Sequence[float]] = None, bbox_crs: Optional[str] = None` to `download`'s signature (after `partitions`), capture them into locals next to `sign`/`_validate`, and pass them into the `fetch_validate_publish` call inside the `_fetch` UDF:

```python
        sign = self.sign
        _validate = validate
        _bbox = tuple(bbox) if bbox is not None else None
        _bbox_crs = bbox_crs
        _injected_get = _get_fn
        ...
            return fetch_validate_publish(
                href_fn,
                out_dir,
                filename,
                max_tries=max_tries,
                validate=_validate,
                bbox=_bbox,
                bbox_crs=_bbox_crs,
                **kwargs,
            )
```

Add to `download`'s docstring: "bbox=(minx,miny,maxx,maxy) windows each asset to the AOI on read (source CRS by default; bbox_crs declares the bbox CRS)."

- [ ] **Step 6: Add a client-level test**

Append to `python/geobrix/test/stac/test_download_bbox.py`:

```python
def test_client_download_threads_bbox(spark, tmp_path, monkeypatch):
    # End-to-end via StacClient.download with a local file as the (unsigned) href.
    from databricks.labs.gbx.stac.client import StacClient
    src = tmp_path / "src.tif"; _write_gtiff(str(src))
    out_dir = tmp_path / "out"
    df = spark.createDataFrame(
        [("item1", "image", str(src))], ["item_id", "asset_name", "href"]
    )
    client = StacClient.__new__(StacClient)  # bypass __init__ (no network/catalog)
    client.sign = "none"  # resolve_signer("none") must be identity; assert below
    res = client.download(df, str(out_dir), bbox=(2, 2, 5, 6)).collect()
    assert len(res) == 1 and res[0]["is_out_file_valid"]
    with rasterio.open(res[0]["out_file_path"]) as ds:
        assert (ds.width, ds.height) == (3, 4)
```

If `resolve_signer("none")` is not already an identity signer, the test should use whatever sign value `StacClient` treats as "no signing" (check `python/geobrix/src/databricks/labs/gbx/stac/_sign.py`); adjust `client.sign` accordingly. The `spark` fixture for the stac tests comes from `python/geobrix/test/stac/` conftest (or create a local `local[2]` session fixture mirroring `test/ds/conftest.py:spark` if none exists).

- [ ] **Step 7: Run the full stac download + client tests**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/stac/test_download_bbox.py` then `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/stac/test_download.py`
Expected: PASS (windowed tests + the pre-existing `_download` tests unchanged).

- [ ] **Step 8: Commit**

```bash
chmod -R u+rwX .git/objects 2>/dev/null || true
git add python/geobrix/src/databricks/labs/gbx/stac/_download.py python/geobrix/src/databricks/labs/gbx/stac/client.py python/geobrix/test/stac/test_download_bbox.py
git commit -m "feat(stac): StacClient.download(bbox=) windowed AOI fetch

Windowed-read variant of fetch_validate_publish opens the signed href with
rasterio (/vsicurl range reads for https) and writes a windowed GeoTIFF via the
clip-safe window_for_bbox, inside the existing re-sign/retry loop. Removes the
need for hand-rolled rasterio windowed staging. Source-CRS bbox with a bbox_crs
override; non-overlapping bbox raises.

Co-authored-by: Isaac"
```

---

## Self-Review

**1. Spec coverage:**
- Shared windowing primitive + footgun fix → Task 1. ✓
- Reader `bbox`/`bboxCrs`, fast-path disable, non-overlap skip, gtiff_gbx parity → Task 2. ✓
- `StacClient.download(bbox=)` windowed `/vsicurl` read → Task 3. ✓
- CRS convention (source default + bboxCrs) → Task 1 primitive + Task 2/3 threading. ✓
- Decimation out of scope, heavy parity out of scope, notebook rework out of scope → Global Constraints. ✓
- Overhang regression test → Task 1 Step 1 (`test_north_overhang_is_clipped_not_shifted`) + Task 2 (`test_bbox_north_overhang_clips`) + Task 3 (`test_windowed_fetch_north_overhang_clips`). ✓

**2. Placeholder scan:** No TBD/TODO; every code step has complete code. The one conditional ("if `resolve_signer('none')` is not identity…") names the exact file to check and the concrete adjustment — not a placeholder.

**3. Type consistency:** `window_for_bbox(src, bbox, bbox_crs) -> Window | None` is consumed identically in Tasks 2 and 3 (`win.col_off/row_off/width/height`, `None` → skip/raise). `bbox` is a 4-float tuple throughout. `encode_tile(ds, window=(col,row,w,h), source_path, all_parents)` matches its definition at `ds/_encode.py:19`.
