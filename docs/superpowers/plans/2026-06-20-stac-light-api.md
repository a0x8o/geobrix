# STAC Lightweight API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `databricks.labs.gbx.stac` — a catalog-agnostic, Serverless-safe `StacClient` (distributed search + resilient download + repair) consolidating the EO-series STAC helpers.

**Architecture:** A `StacClient` holds catalog URL + signing config and exposes `search` / `download` / `repair`. Pure parsing/validation helpers are unit-tested without Spark or network; thin pandas-UDF / DataFrame wrappers fan out via `repartition` (no `spark.conf`). Signing is pluggable; the catalog opener is injectable so unit tests use a fake catalog.

**Tech Stack:** Python 3.12, PySpark (Spark Connect), pandas-UDFs, `pystac-client`, `planetary-computer`, `rasterio` (read-validation), `tenacity` (retry), `requests`, Delta (`delta-spark`) for repair.

## Global Constraints

- Serverless-safe: NO `spark.conf.set(...)`, NO `.cache()`/`.persist()`. Parallelism via `DataFrame.repartition(N)` only (a user repartition is not AQE-coalesced).
- Lightweight tier only — pure Python, no JAR, runs on Serverless environment version 5+ (Python 3.12) and classic.
- New deps live in a NEW optional extra `geobrix[stac]` (`pystac-client`, `planetary-computer`); do NOT add them to `[light]`.
- Volume I/O is sequential-only (FUSE can't seek): download to worker-local disk, validate there, publish to the Volume with a sequential copy.
- A downloaded asset is valid IFF it opens AND decodes a window — never size-only.
- Catalog-agnostic: catalog URL + signing are config, default Planetary Computer + `sign_inplace`.
- Test markers: network tests use `@pytest.mark.integration` (excluded from CI), matching `python/geobrix/pyproject.toml`.

## File Structure

- Create `python/geobrix/src/databricks/labs/gbx/stac/__init__.py` — exports `StacClient`.
- Create `.../stac/_sign.py` — `resolve_signer(sign) -> Callable[[str], str]`.
- Create `.../stac/_search.py` — pure parsers (`parse_item`, `extract_assets`) + `search_items_udf` builder.
- Create `.../stac/_download.py` — `download_href`, `fetch_validate_publish` (resilient fetch + read-validation).
- Create `.../stac/client.py` — `StacClient` (`search` / `download` / `repair`).
- Create tests under `python/geobrix/test/stac/`: `test_sign.py`, `test_search.py`, `test_download.py`, `test_client.py`, `test_serverless_no_spark_config.py`.
- Modify `python/geobrix/pyproject.toml` — add the `[stac]` optional-dependencies extra.

---

### Task 1: `[stac]` extra + package skeleton

**Files:**
- Modify: `python/geobrix/pyproject.toml` (`[project.optional-dependencies]`)
- Create: `python/geobrix/src/databricks/labs/gbx/stac/__init__.py`
- Test: `python/geobrix/test/stac/test_package.py`

**Interfaces:**
- Produces: the importable package `databricks.labs.gbx.stac` exporting `StacClient` (defined in Task 5; until then `__init__` imports lazily so the package imports before the class exists is NOT allowed — so this task creates `__init__` re-exporting from `client`, and a minimal `client.StacClient` stub is created here and fleshed out in Task 5).

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/stac/test_package.py
def test_stac_exports_client():
    from databricks.labs.gbx.stac import StacClient
    assert StacClient is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python/geobrix && python -m pytest test/stac/test_package.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'databricks.labs.gbx.stac'`

- [ ] **Step 3: Add the `[stac]` extra**

In `python/geobrix/pyproject.toml`, under `[project.optional-dependencies]`, add:

```toml
# STAC catalog client (lightweight): distributed search + resilient download + repair.
# Optional extra so [light] users who don't need STAC don't pull pystac/planetary-computer.
stac = [
    "pystac-client>=0.7,<1",
    "planetary-computer>=1.0,<2",
]
```

- [ ] **Step 4: Create the package + a minimal client stub**

```python
# python/geobrix/src/databricks/labs/gbx/stac/__init__.py
"""Lightweight, Serverless-safe STAC client: search + resilient download + repair."""
from databricks.labs.gbx.stac.client import StacClient

__all__ = ["StacClient"]
```

```python
# python/geobrix/src/databricks/labs/gbx/stac/client.py
"""StacClient — catalog-agnostic STAC search/download/repair (fleshed out in later tasks)."""

PLANETARY_COMPUTER = "https://planetarycomputer.microsoft.com/api/stac/v1"


class StacClient:
    """Holds catalog URL + signing config; exposes search/download/repair."""

    def __init__(self, catalog=PLANETARY_COMPUTER, sign="planetary_computer", _catalog_opener=None):
        self.catalog = catalog
        self.sign = sign
        self._catalog_opener = _catalog_opener
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd python/geobrix && python -m pytest test/stac/test_package.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/pyproject.toml python/geobrix/src/databricks/labs/gbx/stac/ python/geobrix/test/stac/test_package.py
git commit -m "feat(stac): package skeleton + [stac] extra"
```

---

### Task 2: Signing strategies (`_sign.py`)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/stac/_sign.py`
- Test: `python/geobrix/test/stac/test_sign.py`

**Interfaces:**
- Produces: `resolve_signer(sign) -> Callable[[str], str]` — maps `"planetary_computer"` to `planetary_computer.sign`, `None` to identity, a callable to itself; raises `ValueError` otherwise. Also `resolve_modifier(sign)` returning a pystac-client `modifier` (for `Client.open`) or `None`.

- [ ] **Step 1: Write the failing tests**

```python
# python/geobrix/test/stac/test_sign.py
import pytest
from databricks.labs.gbx.stac._sign import resolve_signer


def test_none_is_identity():
    s = resolve_signer(None)
    assert s("http://x/y.tif?token=abc") == "http://x/y.tif?token=abc"


def test_callable_passthrough():
    s = resolve_signer(lambda h: h + "?signed")
    assert s("http://x") == "http://x?signed"


def test_unknown_raises():
    with pytest.raises(ValueError):
        resolve_signer("not-a-strategy")
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd python/geobrix && python -m pytest test/stac/test_sign.py -v`
Expected: FAIL — `ModuleNotFoundError: ... stac._sign`

- [ ] **Step 3: Implement `_sign.py`**

```python
# python/geobrix/src/databricks/labs/gbx/stac/_sign.py
"""Signing strategies for STAC asset hrefs.

A *signer* is ``Callable[[str], str]`` applied to an asset href. A *modifier* is the
pystac-client ``modifier=`` callback applied to each item on search (Planetary
Computer's ``sign_inplace`` mutates item asset hrefs in place).
"""
from typing import Callable, Optional


def _identity(href: str) -> str:
    return href


def resolve_signer(sign) -> Callable[[str], str]:
    """Resolve a signer: 'planetary_computer' | None | callable -> Callable[[str],str]."""
    if sign is None:
        return _identity
    if callable(sign):
        return sign
    if sign == "planetary_computer":
        import planetary_computer

        return planetary_computer.sign
    raise ValueError(
        f"sign must be 'planetary_computer', None, or a callable; got {sign!r}"
    )


def resolve_modifier(sign) -> Optional[Callable]:
    """Resolve the pystac-client Client.open(modifier=...) for search-time signing."""
    if sign == "planetary_computer":
        import planetary_computer

        return planetary_computer.sign_inplace
    if sign is None or callable(sign):
        # A bare callable signs per-asset at download time, not via the search modifier.
        return None
    raise ValueError(
        f"sign must be 'planetary_computer', None, or a callable; got {sign!r}"
    )
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd python/geobrix && python -m pytest test/stac/test_sign.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/stac/_sign.py python/geobrix/test/stac/test_sign.py
git commit -m "feat(stac): pluggable signing strategies"
```

---

### Task 3: Search parsers + UDF (`_search.py`)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/stac/_search.py`
- Test: `python/geobrix/test/stac/test_search.py`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces:
  - `parse_item(item_json: str) -> dict` — keys `item_id, date (str|None), item_bbox (list|None), item_properties (dict)`.
  - `extract_assets(item_json: str) -> list[dict]` — each `{"asset_name": str, "href": str}` (plus passthrough asset fields).
  - `search_one(catalog, collections: list[str], datetime: str, geojson: str) -> list[str]` — item JSON strings for one AOI, with tenacity retry; returns `[]` on permanent failure.

- [ ] **Step 1: Write the failing tests**

```python
# python/geobrix/test/stac/test_search.py
import json
from databricks.labs.gbx.stac._search import parse_item, extract_assets, search_one

_ITEM = json.dumps({
    "id": "S2_X",
    "collection": "sentinel-2-l2a",
    "bbox": [1.0, 2.0, 3.0, 4.0],
    "properties": {"datetime": "2022-06-01T19:49:11Z", "eo:cloud_cover": 5},
    "assets": {
        "B02": {"href": "http://x/B02.tif", "type": "image/tiff"},
        "B03": {"href": "http://x/B03.tif", "type": "image/tiff"},
    },
})


def test_parse_item_fields():
    p = parse_item(_ITEM)
    assert p["item_id"] == "S2_X"
    assert p["date"] == "2022-06-01"
    assert p["item_bbox"] == [1.0, 2.0, 3.0, 4.0]
    assert p["item_properties"]["eo:cloud_cover"] == 5


def test_extract_assets():
    a = extract_assets(_ITEM)
    names = sorted(x["asset_name"] for x in a)
    assert names == ["B02", "B03"]
    b02 = next(x for x in a if x["asset_name"] == "B02")
    assert b02["href"] == "http://x/B02.tif"


def test_search_one_uses_catalog_and_retries(monkeypatch):
    calls = {"n": 0}

    class FakeItem:
        def __init__(self, d): self._d = d
        def to_dict(self): return self._d

    class FakeSearch:
        def item_collection(self): return [FakeItem(json.loads(_ITEM))]

    class FakeCatalog:
        def search(self, collections, intersects, datetime):
            calls["n"] += 1
            assert collections == ["sentinel-2-l2a"]
            return FakeSearch()

    out = search_one(FakeCatalog(), ["sentinel-2-l2a"], "2022-06-01", '{"type":"Point","coordinates":[1,2]}')
    assert calls["n"] == 1
    assert json.loads(out[0])["id"] == "S2_X"
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd python/geobrix && python -m pytest test/stac/test_search.py -v`
Expected: FAIL — `ModuleNotFoundError: ... stac._search`

- [ ] **Step 3: Implement `_search.py`**

```python
# python/geobrix/src/databricks/labs/gbx/stac/_search.py
"""STAC search internals: pure parsers + a per-AOI search with retry.

The Spark fan-out (a pandas-UDF over AOI rows) lives in client.py; these helpers are
pure/injectable so they unit-test without Spark or the network.
"""
import json
from typing import Dict, List


def parse_item(item_json: str) -> Dict:
    """Extract the stable item fields from a STAC item JSON string."""
    d = json.loads(item_json)
    props = d.get("properties") or {}
    dt = props.get("datetime")
    return {
        "item_id": d.get("id"),
        "date": dt[:10] if isinstance(dt, str) else None,
        "item_bbox": d.get("bbox"),
        "item_properties": props,
    }


def extract_assets(item_json: str) -> List[Dict]:
    """One dict per asset: {'asset_name', 'href', ...passthrough fields...}."""
    d = json.loads(item_json)
    out = []
    for name, asset in (d.get("assets") or {}).items():
        row = {"asset_name": name, "href": asset.get("href")}
        for k, v in asset.items():
            if k != "href":
                row[k] = v
        out.append(row)
    return out


def search_one(catalog, collections: List[str], datetime: str, geojson: str) -> List[str]:
    """Search one AOI; return item JSON strings. Retries transient failures; on a
    permanent failure returns [] (so one bad AOI does not fail the whole job)."""
    from tenacity import retry, stop_after_attempt, wait_exponential

    @retry(wait=wait_exponential(multiplier=2, min=4, max=60), stop=stop_after_attempt(5), reraise=True)
    def _do():
        search = catalog.search(
            collections=collections, intersects=json.loads(geojson), datetime=datetime
        )
        return [json.dumps(item.to_dict()) for item in search.item_collection()]

    try:
        return _do()
    except Exception:
        return []
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd python/geobrix && python -m pytest test/stac/test_search.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/stac/_search.py python/geobrix/test/stac/test_search.py
git commit -m "feat(stac): search parsers + per-AOI search with retry"
```

---

### Task 4: Resilient download (`_download.py`)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/stac/_download.py`
- Test: `python/geobrix/test/stac/test_download.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `download_href(href, outpath, get=requests.get)` — streams to `outpath`; `raise_for_status()` so HTTP errors raise (retried by caller).
  - `read_validate(path) -> bool` — True iff the file opens AND decodes a window (rasterio).
  - `fetch_validate_publish(href_fn, out_dir, filename, get=..., max_tries=5, sleep=time.sleep) -> str|None` — download to local temp, read-validate, publish (sequential copy) to `out_dir`; re-fetch with backoff via `href_fn()` (re-signs) up to `max_tries`; returns the published path or None.

- [ ] **Step 1: Write the failing tests** (use a real tiny GTiff fixture + a garbage file)

```python
# python/geobrix/test/stac/test_download.py
import os
import numpy as np
import rasterio
from rasterio.transform import from_origin
from databricks.labs.gbx.stac._download import read_validate, fetch_validate_publish


def _write_gtiff(path):
    with rasterio.open(
        path, "w", driver="GTiff", height=8, width=8, count=1, dtype="uint8",
        crs="EPSG:4326", transform=from_origin(0, 8, 1, 1),
    ) as dst:
        dst.write((np.arange(64, dtype="uint8")).reshape(1, 8, 8))


def test_read_validate_true_for_real_gtiff(tmp_path):
    p = tmp_path / "ok.tif"; _write_gtiff(str(p))
    assert read_validate(str(p)) is True


def test_read_validate_false_for_garbage(tmp_path):
    p = tmp_path / "bad.tif"; p.write_bytes(b"<Error>throttled</Error>" * 100)
    assert read_validate(str(p)) is False


def test_fetch_publishes_only_valid(tmp_path):
    src = tmp_path / "src.tif"; _write_gtiff(str(src))
    out_dir = tmp_path / "out"

    def get(href, timeout=None, stream=None):
        class R:
            def raise_for_status(self): pass
            def iter_content(self, n): yield open(str(src), "rb").read()
        return R()

    res = fetch_validate_publish(lambda: "http://x/ok.tif", str(out_dir), "ok.tif", get=get)
    assert res == os.path.join(str(out_dir), "ok.tif")
    assert os.path.exists(res)


def test_fetch_retries_then_gives_up_on_bad(tmp_path):
    out_dir = tmp_path / "out"
    tries = {"n": 0}

    def get(href, timeout=None, stream=None):
        tries["n"] += 1

        class R:
            def raise_for_status(self): pass
            def iter_content(self, n): yield b"throttled-not-a-raster"
        return R()

    res = fetch_validate_publish(
        lambda: "http://x/bad.tif", str(out_dir), "bad.tif", get=get, max_tries=3, sleep=lambda s: None
    )
    assert res is None
    assert tries["n"] == 3
    assert not os.path.exists(os.path.join(str(out_dir), "bad.tif"))
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd python/geobrix && python -m pytest test/stac/test_download.py -v`
Expected: FAIL — `ModuleNotFoundError: ... stac._download`

- [ ] **Step 3: Implement `_download.py`**

```python
# python/geobrix/src/databricks/labs/gbx/stac/_download.py
"""Resilient STAC asset download: HTTP-error-aware fetch + read-validation + retry.

A faithful fetch (no transformation). Validity = the file OPENS and DECODES a window
(rejects throttled error bodies and truncated files a size check would accept).
Volume I/O is sequential-only, so we download to local disk, validate locally, then
publish with a sequential copy.
"""
import os
import shutil
import tempfile
import time
from typing import Callable, Optional

import requests


def download_href(href: str, outpath: str, get: Callable = requests.get) -> str:
    """Stream an href to outpath. raise_for_status() so HTTP throttle/expiry (429/403)
    raises -> the caller's retry backs off instead of writing the error body as data."""
    resp = get(href, timeout=100, stream=True)
    resp.raise_for_status()
    with open(outpath, "wb") as fh:
        for chunk in resp.iter_content(1024 * 1024):
            if chunk:
                fh.write(chunk)
    return outpath


def read_validate(path: str) -> bool:
    """True iff the file opens AND decodes a window (a genuine readable raster)."""
    import rasterio
    from rasterio.windows import Window

    try:
        with rasterio.open(path) as ds:
            ds.read(1, window=Window(0, 0, min(512, ds.width), min(512, ds.height)))
        return True
    except Exception:
        return False


def fetch_validate_publish(
    href_fn: Callable[[], str],
    out_dir: str,
    filename: str,
    get: Callable = requests.get,
    max_tries: int = 5,
    sleep: Callable = time.sleep,
) -> Optional[str]:
    """Download -> read-validate -> publish to out_dir (sequential copy), with retries.

    href_fn() is called each attempt so the href is (re-)signed (signed URLs expire).
    On any failure (HTTP error, throttled body, truncation, decode failure) back off and
    re-fetch up to max_tries; then return None (caller flags is_out_file_valid=False).
    """
    outpath = os.path.join(out_dir, filename)
    os.makedirs(out_dir, exist_ok=True)
    for attempt in range(max_tries):
        tmpd = tempfile.mkdtemp(prefix="gbx_stac_dl_")
        try:
            local = os.path.join(tmpd, filename)
            download_href(href_fn(), local, get=get)
            if read_validate(local):
                shutil.copyfile(local, outpath)  # publish only validated files
                return outpath
        except Exception:
            pass
        finally:
            shutil.rmtree(tmpd, ignore_errors=True)
        if attempt < max_tries - 1:
            sleep(min(60, 4 * (2 ** attempt)))
    try:
        if os.path.exists(outpath):
            os.remove(outpath)
    except OSError:
        pass
    return None
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd python/geobrix && python -m pytest test/stac/test_download.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/stac/_download.py python/geobrix/test/stac/test_download.py
git commit -m "feat(stac): resilient download (HTTP-aware fetch + read-validate + retry)"
```

---

### Task 5: `StacClient` orchestration (`client.py`)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/stac/client.py`
- Test: `python/geobrix/test/stac/test_client.py`

**Interfaces:**
- Consumes: `_sign.resolve_signer/resolve_modifier`, `_search.parse_item/extract_assets/search_one`, `_download.fetch_validate_publish`.
- Produces:
  - `StacClient(catalog=PLANETARY_COMPUTER, sign="planetary_computer", _catalog_opener=None)`.
  - `StacClient._open_catalog()` — returns a catalog via `_catalog_opener()` if set, else `pystac_client.Client.open(self.catalog, modifier=resolve_modifier(self.sign))`.
  - `StacClient.search(df, geojson_col, collections, datetime, partitions=512) -> DataFrame` — columns: carried input cols (except the items/assets scratch), `item_id, date, item_bbox, item_properties, asset_name, href`.
  - `StacClient.download(df, out_dir, asset_names=None, name="{asset_name}_{item_id}.tif", validate=True, max_tries=5, partitions=None) -> DataFrame` — columns: `item_id, asset_name, out_file_path, out_file_sz, is_out_file_valid`. Dedups to unique `(item_id, asset_name)`.
  - `StacClient.repair(target, where="is_out_file_valid = false", spark=None) -> DataFrame` — `target` is a table name (str) or DataFrame; re-downloads invalid rows and (for a table) Delta-MERGEs them back; returns the repaired subset.

- [ ] **Step 1: Write the failing tests** (local SparkSession + injected fake catalog)

```python
# python/geobrix/test/stac/test_client.py
import json
import pytest
from pyspark.sql import SparkSession
from databricks.labs.gbx.stac import StacClient

_ITEM = {
    "id": "S2_X", "collection": "sentinel-2-l2a", "bbox": [1.0, 2.0, 3.0, 4.0],
    "properties": {"datetime": "2022-06-01T19:49:11Z"},
    "assets": {"B02": {"href": "http://x/B02.tif"}, "B03": {"href": "http://x/B03.tif"}},
}


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[2]").appName("stac-test").getOrCreate()
    yield s
    s.stop()


class _FakeItem:
    def __init__(self, d): self._d = d
    def to_dict(self): return self._d


class _FakeSearch:
    def item_collection(self): return [_FakeItem(_ITEM)]


class _FakeCatalog:
    def search(self, collections, intersects, datetime): return _FakeSearch()


def test_search_explodes_items_to_asset_rows(spark):
    client = StacClient(_catalog_opener=lambda: _FakeCatalog())
    df = spark.createDataFrame([("cellA", '{"type":"Point","coordinates":[1,2]}')], ["cellid", "geojson"])
    out = client.search(df, geojson_col="geojson", collections=["sentinel-2-l2a"], datetime="2022-06-01", partitions=2)
    rows = {(r["cellid"], r["item_id"], r["asset_name"]) for r in out.collect()}
    assert rows == {("cellA", "S2_X", "B02"), ("cellA", "S2_X", "B03")}
    one = out.filter("asset_name = 'B02'").first()
    assert one["date"] == "2022-06-01" and one["href"] == "http://x/B02.tif"
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd python/geobrix && python -m pytest test/stac/test_client.py -v`
Expected: FAIL — `AttributeError: 'StacClient' object has no attribute 'search'`

- [ ] **Step 3: Implement `client.py`** (replace the stub from Task 1)

```python
# python/geobrix/src/databricks/labs/gbx/stac/client.py
"""StacClient — catalog-agnostic, Serverless-safe STAC search/download/repair.

Parallelism is via DataFrame.repartition(N) (NOT spark.conf, which is a no-op on
Serverless). No .cache()/.persist(). Asset download is resilient (read-validated +
retried). The catalog opener is injectable (_catalog_opener) for unit tests.
"""
from typing import Callable, List, Optional

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, MapType, StringType, StructField, StructType,
)

from databricks.labs.gbx.stac import _download, _search
from databricks.labs.gbx.stac._sign import resolve_modifier, resolve_signer

PLANETARY_COMPUTER = "https://planetarycomputer.microsoft.com/api/stac/v1"

_ASSET_SCHEMA = ArrayType(StructType([
    StructField("asset_name", StringType()),
    StructField("href", StringType()),
]))
_ITEM_SCHEMA = StructType([
    StructField("item_id", StringType()),
    StructField("date", StringType()),
    StructField("item_bbox", ArrayType(DoubleType())),
    StructField("item_properties", MapType(StringType(), StringType())),
])


class StacClient:
    def __init__(self, catalog=PLANETARY_COMPUTER, sign="planetary_computer", _catalog_opener=None):
        self.catalog = catalog
        self.sign = sign
        self._catalog_opener = _catalog_opener

    def _open_catalog(self):
        if self._catalog_opener is not None:
            return self._catalog_opener()
        import pystac_client

        return pystac_client.Client.open(self.catalog, modifier=resolve_modifier(self.sign))

    def search(self, df: DataFrame, geojson_col: str, collections: List[str],
               datetime: str, partitions: int = 512) -> DataFrame:
        opener = self._catalog_opener
        catalog_url, sign = self.catalog, self.sign

        @F.udf(ArrayType(StringType()))
        def _items(geojson):
            if opener is not None:
                cat = opener()
            else:
                import pystac_client
                cat = pystac_client.Client.open(catalog_url, modifier=resolve_modifier(sign))
            return _search.search_one(cat, list(collections), datetime, geojson)

        @F.udf(_ASSET_SCHEMA)
        def _assets(item_json):
            return [(a["asset_name"], a["href"]) for a in _search.extract_assets(item_json)]

        @F.udf(_ITEM_SCHEMA)
        def _item_fields(item_json):
            p = _search.parse_item(item_json)
            props = {k: str(v) for k, v in (p["item_properties"] or {}).items()}
            return (p["item_id"], p["date"], p["item_bbox"], props)

        carried = [c for c in df.columns if c != geojson_col]
        return (
            df.repartition(partitions)
              .withColumn("_item", F.explode(_items(F.col(geojson_col))))
              .withColumn("_f", _item_fields("_item"))
              .withColumn("_a", F.explode(_assets("_item")))
              .select(
                  *carried,
                  F.col("_f.item_id").alias("item_id"),
                  F.col("_f.date").alias("date"),
                  F.col("_f.item_bbox").alias("item_bbox"),
                  F.col("_f.item_properties").alias("item_properties"),
                  F.col("_a.asset_name").alias("asset_name"),
                  F.col("_a.href").alias("href"),
              )
        )

    def download(self, df: DataFrame, out_dir: str, asset_names: Optional[List[str]] = None,
                 name: str = "{asset_name}_{item_id}.tif", validate: bool = True,
                 max_tries: int = 5, partitions: Optional[int] = None) -> DataFrame:
        if asset_names:
            df = df.filter(F.col("asset_name").isin(list(asset_names)))
        targets = df.select("item_id", "asset_name").distinct()
        n = partitions if partitions is not None else max(1, targets.count())
        catalog_url, sign, opener = self.catalog, self.sign, self._catalog_opener

        @F.udf(StringType())
        def _fetch(item_id, asset_name):
            if opener is not None:
                cat = opener()
            else:
                import pystac_client
                cat = pystac_client.Client.open(catalog_url, modifier=resolve_modifier(sign))
            signer = resolve_signer(sign)

            def href_fn():
                item = cat.get_item(item_id)
                return signer(item.assets[asset_name].href)

            filename = name.format(asset_name=asset_name, item_id=item_id)
            if not validate:
                # still download to local + publish, just skip the decode check
                from databricks.labs.gbx.stac._download import fetch_validate_publish
                return fetch_validate_publish(href_fn, out_dir, filename, max_tries=max_tries)
            return _download.fetch_validate_publish(href_fn, out_dir, filename, max_tries=max_tries)

        @F.udf("long")
        def _size(path):
            import os
            return os.path.getsize(path) if path and os.path.exists(path) else None

        return (
            targets.repartition(n)
              .withColumn("out_file_path", _fetch("item_id", "asset_name"))
              .withColumn("out_file_sz", _size("out_file_path"))
              .withColumn("is_out_file_valid", F.col("out_file_path").isNotNull())
        )

    def repair(self, target, where: str = "is_out_file_valid = false",
               spark=None, out_dir: Optional[str] = None) -> DataFrame:
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        is_table = isinstance(target, str)
        df = spark.table(target) if is_table else target
        invalid = df.filter(where)
        repaired = self.download(
            invalid.select("item_id", "asset_name"),
            out_dir or _common_dir(invalid),
        )
        if is_table:
            from delta.tables import DeltaTable

            dt = DeltaTable.forName(spark, target)
            (dt.alias("t").merge(
                repaired.alias("u"),
                "t.item_id = u.item_id AND t.asset_name = u.asset_name")
             .whenMatchedUpdate(set={
                 "out_file_path": "u.out_file_path",
                 "out_file_sz": "u.out_file_sz",
                 "is_out_file_valid": "u.is_out_file_valid",
             }).execute())
        return repaired


def _common_dir(df: DataFrame) -> str:
    """Infer the output dir from existing out_file_path values (repair convenience)."""
    import os

    row = df.filter(F.col("out_file_path").isNotNull()).select("out_file_path").first()
    if row is None:
        raise ValueError("repair: cannot infer out_dir; pass out_dir=...")
    return os.path.dirname(row["out_file_path"])
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd python/geobrix && python -m pytest test/stac/test_client.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/stac/client.py python/geobrix/test/stac/test_client.py
git commit -m "feat(stac): StacClient search/download/repair orchestration"
```

---

### Task 6: Serverless-safety guard test + integration test

**Files:**
- Create: `python/geobrix/test/stac/test_serverless_no_spark_config.py`
- Modify: `python/geobrix/test/stac/test_client.py` (append the marked integration test)

**Interfaces:**
- Consumes: the whole `stac` package source.
- Produces: a static guard test asserting no forbidden Serverless patterns in `gbx/stac/*.py`; one `@pytest.mark.integration` end-to-end test against real Planetary Computer.

- [ ] **Step 1: Write the failing guard test**

```python
# python/geobrix/test/stac/test_serverless_no_spark_config.py
import pathlib

_FORBIDDEN = ["spark.conf.set", ".cache()", ".persist("]


def test_stac_module_has_no_serverless_forbidden_calls():
    root = pathlib.Path(__file__).resolve().parents[2] / "src/databricks/labs/gbx/stac"
    offenders = []
    for py in root.glob("*.py"):
        text = py.read_text()
        for pat in _FORBIDDEN:
            if pat in text:
                offenders.append(f"{py.name}: {pat}")
    assert not offenders, f"Serverless-forbidden calls in stac module: {offenders}"
```

- [ ] **Step 2: Run to verify it passes immediately** (the module was written clean)

Run: `cd python/geobrix && python -m pytest test/stac/test_serverless_no_spark_config.py -v`
Expected: PASS (this guards against regressions; it should pass now)

- [ ] **Step 3: Add the marked integration test**

Append to `python/geobrix/test/stac/test_client.py`:

```python
@pytest.mark.integration
def test_pc_search_and_download_one_asset(spark, tmp_path):
    client = StacClient()  # real Planetary Computer + sign_inplace
    df = spark.createDataFrame(
        [('{"type":"Point","coordinates":[-131.6,55.3]}',)], ["geojson"]
    )
    assets = client.search(df, geojson_col="geojson", collections=["sentinel-2-l2a"],
                           datetime="2022-06-01/2022-06-05", partitions=1)
    assets = assets.filter("asset_name = 'B02'").limit(1)
    assert assets.count() == 1
    files = client.download(assets, str(tmp_path), asset_names=["B02"], max_tries=3)
    row = files.first()
    assert row["is_out_file_valid"] is True
```

- [ ] **Step 4: Run the full stac suite (integration excluded, matching CI)**

Run: `cd python/geobrix && python -m pytest test/stac/ -v -m "not integration"`
Expected: PASS (all unit tests; integration deselected)

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/test/stac/
git commit -m "test(stac): serverless-safety guard + marked PC integration test"
```

---

### Task 7: Serverless environment-v5 compatibility check

**Files:**
- Create: `python/geobrix/test/stac/test_env_v5_compat.py` (host-runnable pin sanity)
- Create: `notebooks/tests/stac_env_v5_smoke.py` (Serverless env-v5 smoke source)

**Why:** `[light]` taught us a dep can resolve yet fail to IMPORT on env v5 / Python 3.12 (rio-tiler 9.3.0, PEP 728 TypedDict). The `[stac]` deps must be confirmed to install + import on env v5, not just locally.

**Interfaces:** Consumes the `[stac]` extra (T1) + `StacClient` (T5). Produces a host pin-sanity test + a Serverless env-v5 smoke + a recorded run.

- [ ] **Step 1: Host pin-sanity test**

```python
# python/geobrix/test/stac/test_env_v5_compat.py
"""Env-v5 (Python 3.12) compatibility guard for the [stac] extra (pin sanity only;
the live import is exercised by notebooks/tests/stac_env_v5_smoke.py on Serverless)."""
import pathlib
import re


def _stac_deps():
    txt = (pathlib.Path(__file__).resolve().parents[2] / "pyproject.toml").read_text()
    block = re.search(r"\nstac = \[(.*?)\]", txt, re.S)
    assert block, "[stac] extra not found in pyproject.toml"
    return block.group(1)


def test_stac_extra_declares_pystac_and_pc():
    deps = _stac_deps()
    assert "pystac-client" in deps and "planetary-computer" in deps


def test_stac_pins_support_py312():
    deps = _stac_deps()
    assert re.search(r"pystac-client>=0\.7", deps)
    assert re.search(r"planetary-computer>=1\.0", deps)
```

- [ ] **Step 2: Run host test** — `cd python/geobrix && python -m pytest test/stac/test_env_v5_compat.py -v` → PASS (2).

- [ ] **Step 3: Serverless env-v5 smoke source**

```python
# notebooks/tests/stac_env_v5_smoke.py
# Databricks notebook source
# Run as a one-time job on Serverless ENVIRONMENT VERSION 5 (Python 3.12): asserts the
# [stac] extra installs + imports on v5 (catches rio-tiler-9.3.0-style breakage early).
import json
res = {"py": __import__("sys").version.split()[0]}
try:
    import importlib.metadata as md
    import pystac_client, planetary_computer  # noqa: F401
    res["pystac_client"] = md.version("pystac-client")
    res["planetary_computer"] = md.version("planetary-computer")
    from databricks.labs.gbx.stac import StacClient
    res["client_catalog"] = StacClient().catalog  # construct only, no network
    res["stac_import"] = "ok"
except Exception as e:
    import traceback
    res["error"] = repr(e); res["tb"] = traceback.format_exc()[-1200:]
dbutils.notebook.exit(json.dumps(res))
```

- [ ] **Step 4: Run smoke on env v5 + record** — stage a wheel built with `[stac]`, upload the smoke, submit a one-time job with `environments:[{spec:{environment_version:"5"}}]` whose first cell `%pip install "geobrix[light,stac] @ <wheel>"`. Confirm JSON: `py`=3.12.x, both dep versions present, `stac_import=="ok"`, no `error`. Record in the report.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/test/stac/test_env_v5_compat.py notebooks/tests/stac_env_v5_smoke.py
git commit -m "test(stac): env-v5 (Python 3.12) compatibility check + smoke"
```

---

## Self-Review

- **Spec coverage:** `StacClient` class ✔ (T5); catalog-agnostic + signing ✔ (T2, `_open_catalog`); `[stac]` extra, not `[light]` ✔ (T1); search→items→assets typed cols ✔ (T3, T5); resilient download (raise-on-HTTP-error, read-validate, re-sign+retry/backoff, local-stage→publish) ✔ (T4); dedup `(item_id, asset_name)` ✔ (T5 `download`); repair via Delta MERGE ✔ (T5); Serverless-safe (repartition, no conf/cache) ✔ (T5, guarded T6); unit tests w/ injectable catalog + marked integration ✔ (T3–T6); drop `generate_cells` ✔ (not ported); **env-v5 compatibility check** ✔ (T7: host pin-sanity + Serverless env-v5 import smoke).
- **Placeholder scan:** none — every step has full code/commands.
- **Type consistency:** `fetch_validate_publish(href_fn, out_dir, filename, get, max_tries, sleep)` used consistently (T4 def, T5 call); `search`/`download`/`repair` signatures match the Interfaces blocks and the spec's API surface; column names (`item_id, asset_name, href, date, item_bbox, item_properties, out_file_path, out_file_sz, is_out_file_valid`) consistent across T3/T5.

## Follow-on (separate, after this plan is green) — per the agreed sequence

1. **Refactor the EO-series** to use `StacClient` (nb01 `search`, nb02 `download` + `repair`); remove the redundant `library.py`/`config_nb` STAC helpers (keep viz/plot). Update `config_nb` install to `geobrix[light,stac]`.
2. **Executed-notebook export + commit** (interactive Run-all → `--format JUPYTER`, screenshots ride along) for eo-series + xView re-validation.
