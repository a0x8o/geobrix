# DemDownloader (3DEP DEM Downloader) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a product `DemDownloader` (+ one-shot `download_dem_aoi`) to `gbx.sample` that stages USGS 3DEP elevation for an AOI via Planetary Computer STAC, mirroring `NaipDownloader` with resolution (gsd) selection instead of year (vintage) selection.

**Architecture:** New module `sample/dem.py` mirroring `sample/naip.py` — a driver-side `discover` (metadata only) + a distributed `download` that wraps `StacClient.download`'s windowed, fanned-out fetch, + a `read` (raster_gbx). Selection axis is `gsd`: `download(resolution="finest")` picks the minimum gsd (10 m over 30 m); an int picks that exact gsd; graceful no-op when a source lacks `gsd`.

**Tech Stack:** Python 3.12, PySpark, `StacClient` (existing), pytest. Tests on the host venv.

## Global Constraints

- **Mirror `NaipDownloader`** (`python/geobrix/src/databricks/labs/gbx/sample/naip.py`) closely — same method shapes, docstrings-style, injection seam.
- **Serverless-safe:** no `spark.conf.set` / `_jvm` / `sparkContext` / `.rdd` / `.cache()` / `.persist()`. Parallelism only from `StacClient.download`'s `spark.range` fan-out.
- **Online-only:** requires `pystac-client` + `planetary-computer`; no offline/synthetic fallback. `_stac_client` param is the offline-test injection seam.
- **No new SQL function**; do NOT touch `function-info.json` or `docs/tests-function-info/registered_functions.txt`.
- 3DEP specifics: collection `"3dep-seamless"`, asset `"data"` (both `__init__` params, overridable); gsd from `item_properties["gsd"]`.
- **Refinement vs spec:** `discover(resolution=None)` = show all gsd tiers (int filters) — mirrors `NaipDownloader.discover(year=None)`; `download(resolution="finest")` = pick min gsd. (The spec wrote `discover(resolution="finest")`; `None` is used for the "show all" default to avoid "finest" meaning two things. Same intent.)
- **Tests on the host venv:** `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate && python -m pytest python/geobrix/test/sample/test_dem.py -v`.
- Local commits only (pushes held this session). Commit messages end with `Co-authored-by: Isaac`.

**Follow-ups (NOT in this plan):** wiring NB-03 cell-5 to `download_dem_aoi`; the docs "3DEP Downloader (DEM)" page; the live Serverless smoke test. The deliverable here is `sample/dem.py` + its offline mock test suite.

---

### Task 1: `DemDownloader` module — discover / read / export

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/sample/dem.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/sample/__init__.py`
- Test: `python/geobrix/test/sample/test_dem.py`

**Interfaces:**
- Produces:
  - `_bbox_to_geojson_polygon(bbox) -> str` — closed GeoJSON Polygon ring from `(minx,miny,maxx,maxy)`.
  - `class DemDownloader(catalog=PLANETARY_COMPUTER, sign="planetary_computer", collection="3dep-seamless", asset="data", _stac_client=None)`
  - `DemDownloader.discover(bbox, resolution=None, spark=None) -> DataFrame` — columns `item_id, gsd, item_bbox, href`; `resolution=None` returns all tiers, `resolution=<int>` filters `gsd==int`.
  - `DemDownloader.read(out_dir, spark=None) -> DataFrame` — `raster_gbx` reader → `tile` column.
  - `download_dem_aoi(spark, bbox, out_dir, resolution="finest", max_mpp=None, **kw)` — one-shot (delegates to `download`, delivered in Task 2).
- Consumes: `databricks.labs.gbx.stac.StacClient`; the `raster_gbx` DataSource (registered by `ds.register`).

- [ ] **Step 1: Write the failing tests**

Create `python/geobrix/test/sample/test_dem.py`:

```python
"""Tests for DemDownloader — no network; StacClient is fully stubbed.

Mirrors test_naip.py's driver-path stubbing. Selection axis is gsd (resolution),
not year: 3dep-seamless offers the same area at 10 m and 30 m.

Coverage:
  D1  — discover() returns [item_id, gsd, item_bbox, href], only the "data" asset
  D2  — discover(resolution=10) filters to that gsd
  D3  — discover() deduplicates overlapping items
  DL1 — download(resolution="finest") picks the minimum gsd (10 m)
  DL2 — download(resolution=30) selects that exact gsd
  DL3 — download() passes bbox / bbox_crs / max_mpp / partitions to StacClient.download
  DL4 — download() returned DataFrame carries StacClient.download schema
  DL5 — download() empty result carries the canonical schema (no href, Boolean valid)
  DL6 — download(resolution="finest") with no gsd property keeps all items (graceful)
  E1  — export: DemDownloader + download_dem_aoi importable from sample
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType, BooleanType, DoubleType, LongType, MapType, StringType,
    StructField, StructType,
)

from databricks.labs.gbx.sample.dem import DemDownloader, _bbox_to_geojson_polygon  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("dem-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


# Two gsd tiers (10 m + 30 m), two AOI items each; asset "data" + a non-data asset.
_FAKE_ITEMS = [
    {"id": "dep_10_a", "bbox": [-122.45, 37.74, -122.40, 37.78],
     "properties": {"datetime": "2021-01-01T00:00:00Z", "gsd": "10"},
     "assets": {"data": {"href": "file:///fake/dep_10_a.tif"},
                "rendered_preview": {"href": "file:///fake/prev_a.png"}}},
    {"id": "dep_10_b", "bbox": [-122.50, 37.70, -122.42, 37.76],
     "properties": {"datetime": "2021-01-01T00:00:00Z", "gsd": "10"},
     "assets": {"data": {"href": "file:///fake/dep_10_b.tif"}}},
    {"id": "dep_30_a", "bbox": [-122.45, 37.74, -122.40, 37.78],
     "properties": {"datetime": "2019-01-01T00:00:00Z", "gsd": "30"},
     "assets": {"data": {"href": "file:///fake/dep_30_a.tif"}}},
    {"id": "dep_30_b", "bbox": [-122.50, 37.70, -122.42, 37.76],
     "properties": {"datetime": "2019-01-01T00:00:00Z", "gsd": "30"},
     "assets": {"data": {"href": "file:///fake/dep_30_b.tif"}}},
]

_FAKE_ITEMS_NO_GSD = [
    {"id": "dep_nogsd", "bbox": [-122.45, 37.74, -122.40, 37.78],
     "properties": {"datetime": "2020-01-01T00:00:00Z"},
     "assets": {"data": {"href": "file:///fake/dep_nogsd.tif"}}},
]


class _MockStacClient:
    """Captures search + download calls; returns controlled DataFrames."""

    def __init__(self, search_df, download_df=None):
        self._search_df = search_df
        self._download_df = download_df
        self.download_calls = []

    def search(self, df, geojson_col, collections, datetime, partitions=512):
        return self._search_df

    def download(self, df, out_dir, **kwargs):
        rows = df.select("item_id", "asset_name", "href").collect()
        self.download_calls.append(
            {"item_ids": sorted(r["item_id"] for r in rows), "out_dir": out_dir, **kwargs}
        )
        spark = SparkSession.getActiveSession()
        schema = StructType([
            StructField("item_id", StringType()),
            StructField("asset_name", StringType()),
            StructField("out_file_path", StringType()),
            StructField("out_file_sz", LongType()),
            StructField("is_out_file_valid", BooleanType()),
        ])
        if self._download_df is not None:
            return self._download_df
        _rows = [(iid, "data", f"/fake/out/{iid}.tif", 1000, True)
                 for iid in self.download_calls[-1]["item_ids"]]
        return spark.createDataFrame(_rows, schema).withColumn("last_update", F.current_timestamp())


def _make_search_df(spark, items):
    rows = []
    for d in items:
        props = {k: str(v) for k, v in d["properties"].items()}
        bbox = list(d["bbox"])
        date = d["properties"].get("datetime", "")[:10]
        for asset_name, asset in d["assets"].items():
            rows.append((d["id"], date, bbox, props, asset_name, asset["href"]))
    schema = StructType([
        StructField("item_id", StringType()),
        StructField("date", StringType()),
        StructField("item_bbox", ArrayType(DoubleType())),
        StructField("item_properties", MapType(StringType(), StringType())),
        StructField("asset_name", StringType()),
        StructField("href", StringType()),
    ])
    return spark.createDataFrame(rows if rows else [], schema)


def _make_download_df(spark, item_ids):
    rows = [(iid, "data", f"/fake/out/{iid}.tif", 12345, True) for iid in item_ids]
    schema = StructType([
        StructField("item_id", StringType()),
        StructField("asset_name", StringType()),
        StructField("out_file_path", StringType()),
        StructField("out_file_sz", LongType()),
        StructField("is_out_file_valid", BooleanType()),
    ])
    return spark.createDataFrame(rows, schema).withColumn("last_update", F.current_timestamp())


# --- D1 ---
def test_discover_returns_expected_columns_and_rows(spark):
    mock = _MockStacClient(_make_search_df(spark, _FAKE_ITEMS))
    dd = DemDownloader(_stac_client=mock)
    df = dd.discover((-122.52, 37.70, -122.36, 37.83), spark=spark)
    assert set(df.columns) == {"item_id", "gsd", "item_bbox", "href"}, df.columns
    rows = df.collect()
    assert {r["item_id"] for r in rows} == {"dep_10_a", "dep_10_b", "dep_30_a", "dep_30_b"}
    # only "data" asset hrefs (rendered_preview filtered out)
    assert all("prev" not in r["href"] for r in rows)


# --- D2 ---
def test_discover_resolution_filter(spark):
    mock = _MockStacClient(_make_search_df(spark, _FAKE_ITEMS))
    dd = DemDownloader(_stac_client=mock)
    rows = dd.discover((-122.52, 37.70, -122.36, 37.83), resolution=10, spark=spark).collect()
    assert len(rows) == 2 and all(r["gsd"] == 10 for r in rows)
    assert {r["item_id"] for r in rows} == {"dep_10_a", "dep_10_b"}


# --- D3 ---
def test_discover_deduplicates_items(spark):
    mock = _MockStacClient(_make_search_df(spark, _FAKE_ITEMS + _FAKE_ITEMS))
    dd = DemDownloader(_stac_client=mock)
    ids = [r["item_id"] for r in dd.discover((-122.52, 37.70, -122.36, 37.83), spark=spark).collect()]
    assert len(ids) == len(set(ids)), ids


# --- E1 ---
def test_export_dem_downloader_from_sample_init():
    from databricks.labs.gbx.sample import DemDownloader as DD
    from databricks.labs.gbx.sample import download_dem_aoi as dda
    assert DD is DemDownloader
    assert callable(dda)


def test_bbox_to_geojson_polygon_shape():
    import json
    d = json.loads(_bbox_to_geojson_polygon((-1.0, 2.0, 3.0, 4.0)))
    assert d["type"] == "Polygon"
    ring = d["coordinates"][0]
    assert len(ring) == 5 and ring[0] == ring[-1]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate && python -m pytest python/geobrix/test/sample/test_dem.py -v`
Expected: FAIL — `ModuleNotFoundError: ... sample.dem` (and `download_dem_aoi` import error in E1; that test also fails until Task 2, run it with `-k "discover or export or bbox"` for now — the download tests come in Task 2).

- [ ] **Step 3: Create the module (discover / read / scaffold)**

Create `python/geobrix/src/databricks/labs/gbx/sample/dem.py`:

```python
"""DemDownloader — AOI-driven USGS 3DEP elevation staging via Planetary Computer STAC.

Mirrors NaipDownloader's shape: a driver-side discovery step (metadata-only), then
DISTRIBUTED asset I/O via StacClient.download(). The selection axis is resolution (gsd):
``download(resolution="finest")`` picks the minimum gsd (10 m over 30 m); an int picks
that exact gsd. Signing is handled by StacClient (``planetary_computer`` modifier).

ONLINE-ONLY — no offline fallback. Requires pystac-client and planetary-computer.

Injection seam (offline tests): pass ``_stac_client`` (a pre-built or mock StacClient)
to bypass catalog network access.

Serverless-safe: no spark.conf.set, _jvm, .rdd, cache, or persist. Parallelism via
StacClient.download()'s spark.range fan-out.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence, Union

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

PLANETARY_COMPUTER = "https://planetarycomputer.microsoft.com/api/stac/v1"
DEM_COLLECTION = "3dep-seamless"
# 3DEP-seamless exposes its DEM raster under the "data" asset (not "image").
_DEM_ASSET = "data"
# 3dep-seamless is a mosaic; a wide datetime bracket avoids guessing vintages.
_DEM_DATETIME = "2000-01-01/2030-01-01"


def _bbox_to_geojson_polygon(bbox: Sequence[float]) -> str:
    """Convert (minx, miny, maxx, maxy) to a GeoJSON Polygon string."""
    import json

    minx, miny, maxx, maxy = bbox
    coords = [
        [minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny],
    ]
    return json.dumps({"type": "Polygon", "coordinates": [coords]})


class DemDownloader:
    """Distributed, AOI-driven 3DEP DEM downloader via Planetary Computer STAC.

    Discovery (``discover``) is driver-side, metadata-only. Download (``download``)
    fans out via StacClient.download() — Serverless-safe. Selection is by resolution
    (gsd): ``"finest"`` picks the minimum gsd; an int picks that exact gsd.

    Parameters
    ----------
    catalog:      STAC API root URL (default: Planetary Computer).
    sign:         Signing modifier for StacClient (``"planetary_computer"``).
    collection:   STAC collection ID (default ``"3dep-seamless"``).
    asset:        Asset name to download (default ``"data"``).
    _stac_client: Injectable StacClient (or mock) for offline unit tests.
    """

    def __init__(
        self,
        catalog: str = PLANETARY_COMPUTER,
        sign: str = "planetary_computer",
        collection: str = DEM_COLLECTION,
        asset: str = _DEM_ASSET,
        _stac_client=None,
    ):
        self.catalog = catalog
        self.sign = sign
        self.collection = collection
        self.asset = asset
        self._stac_client = _stac_client

    def _get_stac_client(self):
        if self._stac_client is not None:
            return self._stac_client
        from databricks.labs.gbx.stac import StacClient

        return StacClient(catalog=self.catalog, sign=self.sign)

    def _aoi_dataframe(self, bbox: Sequence[float], spark=None) -> "DataFrame":
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        return spark.createDataFrame(
            [(_bbox_to_geojson_polygon(bbox),)], ["geojson"]
        )

    def _gsd_col(self):
        """Column expr: item_properties['gsd'] as an int (nullable)."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import IntegerType

        return F.col("item_properties")["gsd"].cast(IntegerType())

    def discover(
        self, bbox: Sequence[float], resolution: Optional[int] = None, spark=None
    ) -> "DataFrame":
        """Search Planetary Computer for 3DEP items intersecting bbox.

        Returns one row per distinct DEM ``data`` asset: item_id (str), gsd (int),
        item_bbox (array<double>), href (str). ``resolution=None`` returns all gsd
        tiers; an int keeps only items whose gsd equals it.
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = spark or SparkSession.getActiveSession()
        client = self._get_stac_client()
        aoi_df = self._aoi_dataframe(bbox, spark)

        raw = client.search(
            aoi_df, geojson_col="geojson",
            collections=[self.collection], datetime=_DEM_DATETIME,
        )
        img = raw.filter(F.col("asset_name") == self.asset)
        out = (
            img.withColumn("gsd", self._gsd_col())
            .select("item_id", "gsd", "item_bbox", "href")
            .distinct()
        )
        if resolution is not None:
            out = out.filter(F.col("gsd") == int(resolution))
        return out

    def read(self, out_dir: str, spark=None) -> "DataFrame":
        """Load downloaded DEM GeoTIFFs from out_dir into a raster tile DataFrame.

        Mirrors NaipDownloader.read(): the ``raster_gbx`` reader, filtered to ``*.tif``,
        repartitioned by source path (Serverless-safe, column-hash repartition).
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = spark or SparkSession.getActiveSession()
        return (
            spark.read.format("raster_gbx")
            .option("filterRegex", r".*\.tif$")
            .load(out_dir)
            .repartition(64, F.col("source"))
            .select("tile")
        )
```

Then add to `python/geobrix/src/databricks/labs/gbx/sample/__init__.py` (next to the NAIP import, and in `__all__`):

```python
from databricks.labs.gbx.sample.dem import DemDownloader, download_dem_aoi
```

(Add `"DemDownloader"` and `"download_dem_aoi"` to the `__all__` list.)

Note: `download_dem_aoi` is delivered in Task 2 but the import + `__all__` entry are added now; the export test (E1) is in Task 2's run. For Task 1, run the discover/bbox tests only.

- [ ] **Step 4: Run the Task-1 tests to verify they pass**

Run: `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate && python -m pytest python/geobrix/test/sample/test_dem.py -v -k "discover or bbox"`
Expected: PASS (D1, D2, D3, helper). (The `__init__` import line references `download_dem_aoi`, which Task 2 adds — so add a temporary stub OR sequence Task 2 immediately; see Step 5.)

- [ ] **Step 5: Add a minimal `download_dem_aoi` stub + `download` so the module imports, then commit**

To keep the module importable at the Task-1 boundary, add the `download` method and `download_dem_aoi` now (they are Task 2's focus, but a module that imports a missing name breaks Task 1's own tests). Add the full `download` + `download_dem_aoi` from Task 2 Step 3. Then:

```bash
chmod -R u+rwX .git/objects 2>/dev/null || true
git add python/geobrix/src/databricks/labs/gbx/sample/dem.py python/geobrix/src/databricks/labs/gbx/sample/__init__.py python/geobrix/test/sample/test_dem.py
git commit -m "feat(sample): DemDownloader — 3DEP DEM discover/read + module scaffold

AOI-driven 3DEP elevation downloader mirroring NaipDownloader; discover() returns
gsd tiers, read() via raster_gbx. Selection axis is resolution (gsd), not year.

Co-authored-by: Isaac"
```

(Task 1 and Task 2 both touch `dem.py`; because a Python module can't import a name that doesn't exist, `download`/`download_dem_aoi` land in the same commit as the scaffold. The reviewer treats Task 2's `download` logic + its tests as the second gate.)

---

### Task 2: `download` (gsd selection) + `download_dem_aoi` one-shot

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/sample/dem.py` (add `download` + `download_dem_aoi`)
- Test: `python/geobrix/test/sample/test_dem.py` (add DL1–DL6 + E1)

**Interfaces:**
- Consumes: `DemDownloader` (Task 1); `StacClient.download(df, out_dir, bbox=, bbox_crs=, max_mpp=, partitions=)`.
- Produces:
  - `DemDownloader.download(bbox, out_dir, resolution="finest", bbox_crs="EPSG:4326", max_mpp=None, partitions=None, spark=None) -> DataFrame` — returns `StacClient.download`'s result.
  - `download_dem_aoi(spark, bbox, out_dir, resolution="finest", max_mpp=None, **kw) -> DataFrame`.

- [ ] **Step 1: Write the failing download tests**

Append to `python/geobrix/test/sample/test_dem.py`:

```python
# --- DL1: finest picks the minimum gsd (10 m) ---
def test_download_picks_finest_gsd(spark, tmp_path):
    mock = _MockStacClient(_make_search_df(spark, _FAKE_ITEMS),
                           _make_download_df(spark, ["dep_10_a", "dep_10_b"]))
    dd = DemDownloader(_stac_client=mock)
    result = dd.download((-122.52, 37.70, -122.36, 37.83), str(tmp_path / "o"),
                         resolution="finest", spark=spark)
    assert len(mock.download_calls) == 1
    assert set(mock.download_calls[0]["item_ids"]) == {"dep_10_a", "dep_10_b"}
    assert {"item_id", "asset_name", "out_file_path", "last_update"} <= {f.name for f in result.schema}


# --- DL2: resolution=int selects that gsd ---
def test_download_resolution_int_selects_tier(spark, tmp_path):
    mock = _MockStacClient(_make_search_df(spark, _FAKE_ITEMS),
                           _make_download_df(spark, ["dep_30_a", "dep_30_b"]))
    dd = DemDownloader(_stac_client=mock)
    dd.download((-122.52, 37.70, -122.36, 37.83), str(tmp_path / "o"), resolution=30, spark=spark)
    assert set(mock.download_calls[0]["item_ids"]) == {"dep_30_a", "dep_30_b"}


# --- DL3: kwargs forwarded ---
def test_download_passes_kwargs(spark, tmp_path):
    mock = _MockStacClient(_make_search_df(spark, _FAKE_ITEMS),
                           _make_download_df(spark, ["dep_10_a"]))
    bbox = (-122.52, 37.70, -122.36, 37.83)
    dd = DemDownloader(_stac_client=mock)
    dd.download(bbox, str(tmp_path / "o"), resolution="finest",
                bbox_crs="EPSG:4326", max_mpp=5.0, partitions=8, spark=spark)
    call = mock.download_calls[0]
    assert call["bbox"] == list(bbox)
    assert call["bbox_crs"] == "EPSG:4326"
    assert call["max_mpp"] == 5.0
    assert call["partitions"] == 8


# --- DL4: result carries StacClient.download schema ---
def test_download_returns_stac_schema(spark, tmp_path):
    mock = _MockStacClient(_make_search_df(spark, _FAKE_ITEMS),
                           _make_download_df(spark, ["dep_10_a"]))
    dd = DemDownloader(_stac_client=mock)
    result = dd.download((-122.52, 37.70, -122.36, 37.83), str(tmp_path / "o"), spark=spark)
    required = {"item_id", "asset_name", "out_file_path", "out_file_sz", "is_out_file_valid", "last_update"}
    assert required <= {f.name for f in result.schema}


# --- DL5: empty result carries the canonical schema ---
def test_download_empty_returns_canonical_schema(spark, tmp_path):
    mock = _MockStacClient(_make_search_df(spark, []))
    dd = DemDownloader(_stac_client=mock)
    result = dd.download((-122.52, 37.70, -122.36, 37.83), str(tmp_path / "o"), spark=spark)
    assert result.count() == 0
    schema_map = {f.name: f.dataType for f in result.schema}
    assert {"item_id", "asset_name", "out_file_path", "out_file_sz", "is_out_file_valid", "last_update"} <= set(schema_map)
    assert "href" not in schema_map
    assert isinstance(schema_map["is_out_file_valid"], BooleanType)
    assert isinstance(schema_map["out_file_sz"], LongType)


# --- DL6: finest with no gsd property keeps all items (graceful) ---
def test_download_finest_no_gsd_keeps_all(spark, tmp_path):
    mock = _MockStacClient(_make_search_df(spark, _FAKE_ITEMS_NO_GSD),
                           _make_download_df(spark, ["dep_nogsd"]))
    dd = DemDownloader(_stac_client=mock)
    dd.download((-122.52, 37.70, -122.36, 37.83), str(tmp_path / "o"),
                resolution="finest", spark=spark)
    assert mock.download_calls[0]["item_ids"] == ["dep_nogsd"]
```

(E1 export + the helper test are already in the file from Task 1 and will now pass once `download_dem_aoi` exists.)

- [ ] **Step 2: Run to verify the download tests fail**

Run: `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate && python -m pytest python/geobrix/test/sample/test_dem.py -v -k "download or export"`
Expected: FAIL — `AttributeError: 'DemDownloader' object has no attribute 'download'` / `ImportError: download_dem_aoi` (if not already added in Task 1 Step 5).

- [ ] **Step 3: Implement `download` + `download_dem_aoi`**

Add to `DemDownloader` in `sample/dem.py` (after `read`), and the module-level function at the end:

```python
    def download(
        self,
        bbox: Sequence[float],
        out_dir: str,
        resolution: Union[int, str] = "finest",
        bbox_crs: str = "EPSG:4326",
        max_mpp: Optional[float] = None,
        partitions: Optional[int] = None,
        spark=None,
    ) -> "DataFrame":
        """Search, select a gsd tier, and download 3DEP tiles to out_dir.

        resolution="finest" (default) picks the minimum gsd (e.g. 10 m over 30 m);
        an int picks that exact gsd. When a source has no gsd property, "finest"
        keeps all matching items (graceful no-op). Returns StacClient.download's
        result: item_id, asset_name, out_file_path, out_file_sz, is_out_file_valid,
        last_update.
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = spark or SparkSession.getActiveSession()
        client = self._get_stac_client()
        aoi_df = self._aoi_dataframe(bbox, spark)

        raw = client.search(
            aoi_df, geojson_col="geojson",
            collections=[self.collection], datetime=_DEM_DATETIME,
        )
        img = raw.filter(F.col("asset_name") == self.asset).withColumn(
            "_gsd", self._gsd_col()
        )

        if resolution == "finest":
            min_row = img.agg(F.min("_gsd").alias("m")).first()
            selected = min_row["m"] if min_row is not None else None
            # A gsd tier exists -> keep the finest; else (no gsd property, or no
            # items at all) keep the matching set as-is (empty stays empty).
            vintage = img.filter(F.col("_gsd") == selected) if selected is not None else img
        else:
            vintage = img.filter(F.col("_gsd") == int(resolution))

        vintage = vintage.select("item_id", "asset_name", "href")
        return client.download(
            vintage, out_dir,
            bbox=list(bbox), bbox_crs=bbox_crs, max_mpp=max_mpp, partitions=partitions,
        )
```

Module-level one-shot (end of file):

```python
def download_dem_aoi(
    spark,
    bbox: Sequence[float],
    out_dir: str,
    resolution: Union[int, str] = "finest",
    max_mpp: Optional[float] = None,
    **kw,
) -> "DataFrame":
    """One-shot: construct a default DemDownloader and download a DEM for an AOI.

    Convenience wrapper — Planetary Computer catalog, planetary_computer signing,
    3dep-seamless collection, "data" asset. Forwards **kw (e.g. partitions, bbox_crs).
    """
    downloader = DemDownloader()
    return downloader.download(
        bbox, out_dir, resolution=resolution, max_mpp=max_mpp, spark=spark, **kw
    )
```

- [ ] **Step 4: Run the full test file to verify all pass**

Run: `source /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/activate && python -m pytest python/geobrix/test/sample/test_dem.py -v`
Expected: PASS (D1–D3, DL1–DL6, E1, helper).

- [ ] **Step 5: Serverless-safe source scan**

Run: `grep -nE "spark\.conf\.set|_jvm|sparkContext|\.rdd|\.cache\(|\.persist\(" python/geobrix/src/databricks/labs/gbx/sample/dem.py`
Expected: no matches (empty output). If any match, remove it — the module must be Serverless-safe.

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects 2>/dev/null || true
git add python/geobrix/src/databricks/labs/gbx/sample/dem.py python/geobrix/test/sample/test_dem.py
git commit -m "feat(sample): DemDownloader.download gsd selection + download_dem_aoi

resolution='finest' picks the minimum gsd (10 m over 30 m); int picks that exact gsd;
graceful no-op when a source lacks gsd. One-shot download_dem_aoi mirrors download_naip_aoi.

Co-authored-by: Isaac"
```

---

## Self-Review

**1. Spec coverage:**
- `DemDownloader` in `sample/dem.py`, exported → Task 1. ✓
- discover/download/read + one-shot, mirroring NaipDownloader → Tasks 1+2. ✓
- Selection axis = gsd: finest→min, int→exact, no-op if no gsd → Task 2 `download` + DL1/DL2/DL6. ✓
- collection/asset `__init__` params → Task 1 `__init__`. ✓
- Serverless-safe (no spark.conf/_jvm/.rdd) → Task 2 Step 5 scan. ✓
- Online-only + `_stac_client` injection seam → Task 1 `_get_stac_client`. ✓
- No new SQL function / no function-info change → nothing in the plan touches those. ✓
- Empty-result canonical schema → DL5. ✓
- Follow-ups (NB-03 wiring, docs page, live Serverless smoke) explicitly out of scope → header. ✓

**2. Placeholder scan:** No TBD/TODO. The only cross-task nuance (`download`/`download_dem_aoi` land in the Task-1 commit so the module imports) is stated explicitly with the reason, not left vague.

**3. Type consistency:** `discover` → `(item_id, gsd, item_bbox, href)`; `download`/`download_dem_aoi` signatures match between the interface blocks, the impl, and the tests. `resolution` is `Optional[int]` for `discover` (None=all) and `Union[int,str]` for `download` (default `"finest"`). `_gsd_col()` used identically in `discover` and `download`. `download` returns `StacClient.download`'s schema (item_id, asset_name, out_file_path, out_file_sz, is_out_file_valid, last_update), asserted by DL4/DL5.
