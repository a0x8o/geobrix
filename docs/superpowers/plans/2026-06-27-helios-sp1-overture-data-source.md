# Overture Data Source (SP1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to execute this plan — dispatch each task to a fresh subagent with the full task text, gate at the review checkpoint between tasks, and never carry implementation context forward beyond what a task's **Interfaces** block declares.

**Goal:** Build `gbx.sample.overture` — an API-level, distributed, AOI-driven Overture Maps GeoParquet data source (all themes/types), mirroring `gbx.stac.StacClient`'s shape and test-injection seams. The performant default is a distributed Spark read of Overture GeoParquet over the cloud path with `bbox`-struct predicate pushdown, written distributed to a UC Volume plus an optional metadata Delta table; whole-file STAC HTTP-href download is the fallback. Discovery traverses Overture's static STAC `catalog.json`, filters items client-side by bbox, and uses the `overturemaps` CLI as a fast-path when present.

**Architecture:** Two modules sit beside `sample/_bundle.py` in the WHL:
- `sample/_overture_discover.py` — pure, driver-side, network-free-when-injected helpers: bbox-intersection math, STAC catalog traversal (via injectable opener), release resolution, and an optional `overturemaps` CLI fast-path. Unit-testable in isolation with no Spark and no network.
- `sample/overture.py` — public `OvertureClient` (`discover` / `download` / `read`) + the `download_overture_aoi` one-shot convenience. Holds the Spark distribution logic: Serverless-safe `repartition(N, col)` distributed read + AOI rewrite (default), asset-level HTTP-href download (fallback), parquet-open validation, idempotent skip, and the metadata Delta `MERGE`.

`OvertureClient` carries two injection seams identical to `StacClient`: `_catalog_opener` (returns a traversable catalog object; when set, discovery runs on the driver with no network) and `_get_fn` (an HTTP fetcher passed through to the fallback downloader). Both default to `None` (production paths construct the real opener/fetcher).

**Tech Stack:** Python 3.12+, PySpark 4.0.0 (local mode for unit tests), `pystac` (static catalog traversal — distinct from `pystac-client`), `geopandas`/`pyarrow` (parquet read, already present), Delta Lake (`delta.tables.DeltaTable` for the MERGE). `overturemaps` CLI optional (fast-path only). Tests run offline via injected opener + injected fetcher + a local `SparkSession`.

## Global Constraints

These project-wide constraints (from the spec's cross-cutting sections and `CLAUDE.md`) apply to every task below. Copying them verbatim so a subagent executing one task in isolation does not violate them:

- **Version floor:** Python 3.12+. New deps must be pinned in lockstep with DBR 17.3 LTS where a DBR-installed version exists; otherwise pinned independently.
- **Serverless is the first-class target (hard constraints):**
  - Parallelism is **only** via `DataFrame.repartition(N, column)` — hash by a column. A number-only `repartition(N)` is AQE-coalesced back toward 1 partition (serial) on Serverless and is forbidden.
  - **No** `spark.conf.set` (no-op on Serverless), **no** `.cache()` / `.persist()` / `.checkpoint()`, **no** `.rdd`, **no** `sparkContext` / `_jvm`. Only `udf.register` + Column expressions and DataFrame ops.
  - When iterating a plan locally, **verify partitions are not coalesced** with `df.rdd.getNumPartitions()` — note: `.rdd` is allowed *only* in local-test assertions, never in product code paths. (The local SparkSession in tests is Classic, so `.rdd` works there for the assertion; production code never touches it.)
  - `CREATE TEMP TABLE` materialization (to pin a distributed result) is Serverless / DBR 18.1+ only — do not rely on it for the default path's correctness.
- **Light-CI-lock checklist (do BOTH halves, see Task 11):**
  - (a) Add new light runtime deps to `python/geobrix/requirements-pyrx-ci.in` **and** `python/geobrix/requirements-dev-container.in`, then recompile the hash-pinned `.txt` locks (`uv pip compile --generate-hashes --python-version 3.12 ...`).
  - (b) Register the new `test/sample/` directory in **both** `test/conftest.py`'s `_LIGHT_TEST_DIRS` (so the heavyweight CI phase skips it) **and** the explicit pytest dir list in `.github/actions/pyrx_build/action.yml` (so the light phase RUNS it).
- **Unity Catalog Volumes rules:** `/Volumes/...` is FUSE-mounted — use `pathlib`/`os`, not the Files SDK. The Volume root must pre-exist; only paths under it can be created (`os.makedirs(volume_root, exist_ok=True)` is a no-op). Avoid `seek`; sequential I/O only. For writes prefer temp-file-then-`shutil.copy`. Sanitize env-derived strings before building volume paths.
- **TDD by default:** every task writes a failing test first, runs it to confirm the failure, then writes minimal code to pass, then re-runs green, then commits. The test is the definition of done.
- **Commit hygiene:** subject ≤72 chars; a WHY body for any non-trivial/multi-purpose commit. End commit messages with the `Co-authored-by: Isaac` trailer.
- **No aliases / canonical names:** one canonical name per public symbol; signatures below are pinned because SP2/SP3 depend on them.
- **Docs voice:** no internal planning vocabulary (no wave numbers / dispatch references) in any user-facing text.

---

### Task 1: bbox-intersect util + discovery parsing (`_overture_discover.py`)

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/sample/_overture_discover.py`
- Test: `python/geobrix/test/sample/test_overture_discover.py`
- Create: `python/geobrix/test/sample/__init__.py` (empty package marker, mirrors `test/stac/__init__.py` so cloudpickle resolves fakes by module name)

**Interfaces:**
- Produces:
  - `bbox_intersects(a, b) -> bool` where `a`, `b` are `(minx, miny, maxx, maxy)` tuples; axis-aligned overlap test (touching edges count as intersecting).
  - `normalize_bbox(bbox) -> tuple[float, float, float, float]` — accepts a 4-tuple/list, validates `minx<=maxx` and `miny<=maxy`, returns a float tuple; raises `ValueError` on malformed input.
  - `OVERTURE_THEMES: dict[str, list[str]]` — the canonical theme→types map: `addresses:["address"]`, `base:["infrastructure","land","land_cover","land_use","water","bathymetry"]`, `buildings:["building","building_part"]`, `divisions:["division","division_area","division_boundary"]`, `places:["place"]`, `transportation:["connector","segment"]`.
  - `expand_themes(themes) -> list[tuple[str, str]]` — `None` → every `(theme, type)` pair from `OVERTURE_THEMES`; a list of theme names → that subset's pairs; raises `ValueError` on an unknown theme.

- [ ] **Step 1: Write the failing test for `bbox_intersects` + `normalize_bbox`.**
```python
# python/geobrix/test/sample/test_overture_discover.py
import pytest

from databricks.labs.gbx.sample._overture_discover import (
    bbox_intersects,
    normalize_bbox,
)


def test_bbox_intersects_overlap():
    assert bbox_intersects((0, 0, 10, 10), (5, 5, 15, 15)) is True


def test_bbox_intersects_touching_edge():
    # touching edges count as intersecting (inclusive)
    assert bbox_intersects((0, 0, 10, 10), (10, 0, 20, 10)) is True


def test_bbox_intersects_disjoint():
    assert bbox_intersects((0, 0, 1, 1), (5, 5, 6, 6)) is False


def test_normalize_bbox_returns_floats():
    assert normalize_bbox([1, 2, 3, 4]) == (1.0, 2.0, 3.0, 4.0)


def test_normalize_bbox_rejects_inverted():
    with pytest.raises(ValueError):
        normalize_bbox((10, 0, 0, 10))
```

- [ ] **Step 2: Run it; confirm it fails on the missing module.**
  `gbx:test:python --path test/sample/test_overture_discover.py`
  Expected: `ModuleNotFoundError: No module named 'databricks.labs.gbx.sample._overture_discover'` (collection error).

- [ ] **Step 3: Write minimal `bbox_intersects` + `normalize_bbox` (+ module docstring) in `_overture_discover.py`.**
```python
"""Overture static-STAC discovery helpers (driver-side, network-free when injected).

Kept separate from overture.py so the catalog traversal / bbox-intersect / CLI
fast-path logic is unit-testable in isolation, with no Spark and no network. The
catalog opener is injected by OvertureClient (_catalog_opener) for offline tests,
exactly like StacClient's seam.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

Bbox = Tuple[float, float, float, float]


def normalize_bbox(bbox) -> Bbox:
    """Validate and float-cast a (minx, miny, maxx, maxy) bbox."""
    if bbox is None or len(bbox) != 4:
        raise ValueError(f"bbox must be (minx, miny, maxx, maxy); got {bbox!r}")
    minx, miny, maxx, maxy = (float(v) for v in bbox)
    if minx > maxx or miny > maxy:
        raise ValueError(f"bbox is inverted (min > max): {bbox!r}")
    return (minx, miny, maxx, maxy)


def bbox_intersects(a, b) -> bool:
    """Axis-aligned overlap test; touching edges count as intersecting."""
    ax0, ay0, ax1, ay1 = a
    bx0, by0, bx1, by1 = b
    return ax0 <= bx1 and bx0 <= ax1 and ay0 <= by1 and by0 <= ay1
```

- [ ] **Step 4: Re-run; confirm the bbox tests pass.**
  `gbx:test:python --path test/sample/test_overture_discover.py`
  Expected: 5 passed.

- [ ] **Step 5: Add the failing test for `OVERTURE_THEMES` + `expand_themes`.**
```python
# append to test_overture_discover.py
from databricks.labs.gbx.sample._overture_discover import (
    OVERTURE_THEMES,
    expand_themes,
)


def test_overture_themes_complete():
    assert set(OVERTURE_THEMES) == {
        "addresses",
        "base",
        "buildings",
        "divisions",
        "places",
        "transportation",
    }
    assert OVERTURE_THEMES["buildings"] == ["building", "building_part"]


def test_expand_themes_none_is_all_pairs():
    pairs = expand_themes(None)
    assert ("buildings", "building") in pairs
    assert ("transportation", "segment") in pairs
    # one pair per (theme, type)
    assert len(pairs) == sum(len(v) for v in OVERTURE_THEMES.values())


def test_expand_themes_subset():
    assert expand_themes(["places"]) == [("places", "place")]


def test_expand_themes_unknown_raises():
    with pytest.raises(ValueError):
        expand_themes(["weather"])
```

- [ ] **Step 6: Run it; confirm ImportError on the new symbols.**
  `gbx:test:python --path test/sample/test_overture_discover.py`
  Expected: `ImportError: cannot import name 'OVERTURE_THEMES'`.

- [ ] **Step 7: Add `OVERTURE_THEMES` + `expand_themes`.**
```python
OVERTURE_THEMES = {
    "addresses": ["address"],
    "base": ["infrastructure", "land", "land_cover", "land_use", "water", "bathymetry"],
    "buildings": ["building", "building_part"],
    "divisions": ["division", "division_area", "division_boundary"],
    "places": ["place"],
    "transportation": ["connector", "segment"],
}


def expand_themes(themes: Optional[List[str]]) -> List[Tuple[str, str]]:
    """themes=None -> every (theme, type) pair; a list -> that subset's pairs."""
    names = list(OVERTURE_THEMES) if themes is None else list(themes)
    pairs: List[Tuple[str, str]] = []
    for name in names:
        if name not in OVERTURE_THEMES:
            raise ValueError(
                f"unknown Overture theme {name!r}; valid: {sorted(OVERTURE_THEMES)}"
            )
        pairs.extend((name, t) for t in OVERTURE_THEMES[name])
    return pairs
```

- [ ] **Step 8: Re-run; confirm all pass.**
  `gbx:test:python --path test/sample/test_overture_discover.py`
  Expected: 9 passed.

- [ ] **Step 9: Commit.**
  `git add python/geobrix/src/databricks/labs/gbx/sample/_overture_discover.py python/geobrix/test/sample/__init__.py python/geobrix/test/sample/test_overture_discover.py`
  `git commit -m "feat(sample): overture bbox-intersect + theme expansion helpers" -m "Foundation for the Overture discovery module: axis-aligned bbox intersection, bbox normalization, and the canonical theme->type map + expansion (None => all themes). Pure driver-side helpers, no Spark/network, unit-tested in isolation." -m "Co-authored-by: Isaac"`

---

### Task 2: static catalog traversal with an injected opener

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/sample/_overture_discover.py`
- Test: `python/geobrix/test/sample/test_overture_discover.py`
- Create: `python/geobrix/test/sample/_fake_overture_catalog.py` (top-level importable fakes, mirrors `test/stac/_fake_catalog.py`)

**Interfaces:**
- Consumes: `bbox_intersects`, `normalize_bbox`, `expand_themes` (Task 1).
- Produces:
  - `traverse_catalog(opener, bbox, theme_pairs) -> list[dict]` — walks a static STAC catalog (root `catalog.json` → child collections → items), filters items whose `bbox` intersects the AOI, restricts to the requested `(theme, type)` pairs, and returns one dict per intersecting GeoParquet asset with keys: `theme`, `type`, `href`, `asset_bbox` (a 4-float list). `opener()` returns a catalog object exposing `.get_children()` (collections) and `.get_items()` (items); each item exposes `.bbox`, `.properties` (carrying `theme`/`type`), and `.assets` (mapping name → object with `.href`). This is the `pystac.Catalog` shape, faked in tests.

- [ ] **Step 1: Write the importable fake catalog.**
```python
# python/geobrix/test/sample/_fake_overture_catalog.py
"""Top-level importable fake Overture static STAC catalog for offline tests.

Mirrors the pystac.Catalog surface that traverse_catalog walks: a root with
get_children() -> collections, each with get_items() -> items, each item with
.bbox, .properties (theme/type), and .assets (name -> obj with .href).
Importable (not a closure) so it can be injected as _catalog_opener and, if ever
used on a worker, resolved by cloudpickle via the module name.
"""

from __future__ import annotations


class _Asset:
    def __init__(self, href):
        self.href = href


class _Item:
    def __init__(self, bbox, theme, type_, href):
        self.bbox = bbox
        self.properties = {"theme": theme, "type": type_}
        self.assets = {"data": _Asset(href)}


class _Collection:
    def __init__(self, items):
        self._items = items

    def get_items(self):
        return list(self._items)


class FakeOvertureCatalog:
    """Two collections: SF buildings (intersects) + a faraway places item (disjoint)."""

    def get_children(self):
        sf = _Collection(
            [
                _Item(
                    [-122.52, 37.70, -122.36, 37.83],
                    "buildings",
                    "building",
                    "s3://overturemaps-us-west-2/release/buildings/building/sf.parquet",
                )
            ]
        )
        faraway = _Collection(
            [
                _Item(
                    [10.0, 50.0, 11.0, 51.0],
                    "places",
                    "place",
                    "s3://overturemaps-us-west-2/release/places/place/eu.parquet",
                )
            ]
        )
        return [sf, faraway]


def open_fake_overture():
    return FakeOvertureCatalog()
```

- [ ] **Step 2: Write the failing test for `traverse_catalog`.**
```python
# append to test_overture_discover.py
from databricks.labs.gbx.sample._overture_discover import traverse_catalog
from test.sample._fake_overture_catalog import open_fake_overture


def test_traverse_catalog_bbox_filters_disjoint():
    sf_bbox = (-122.45, 37.74, -122.40, 37.78)
    rows = traverse_catalog(open_fake_overture, sf_bbox, [("buildings", "building")])
    assert len(rows) == 1
    r = rows[0]
    assert r["theme"] == "buildings"
    assert r["type"] == "building"
    assert r["href"].endswith("sf.parquet")
    assert r["asset_bbox"] == [-122.52, 37.70, -122.36, 37.83]


def test_traverse_catalog_skips_unrequested_pairs():
    # AOI covers the whole world, but we only ask for places -> the SF building drops out
    rows = traverse_catalog(open_fake_overture, (-180, -90, 180, 90), [("places", "place")])
    assert [r["type"] for r in rows] == ["place"]
```

- [ ] **Step 3: Run it; confirm ImportError / failure.**
  `gbx:test:python --path test/sample/test_overture_discover.py`
  Expected: `ImportError: cannot import name 'traverse_catalog'`.

- [ ] **Step 4: Implement `traverse_catalog`.**
```python
def traverse_catalog(opener, bbox, theme_pairs):
    """Walk a static STAC catalog and return one dict per intersecting GeoParquet asset.

    opener() returns a pystac.Catalog-shaped object. Items are filtered by AOI
    bbox intersection and restricted to the requested (theme, type) pairs.
    """
    aoi = normalize_bbox(bbox)
    wanted = set(theme_pairs)
    rows = []
    catalog = opener()
    for collection in catalog.get_children():
        for item in collection.get_items():
            props = item.properties or {}
            pair = (props.get("theme"), props.get("type"))
            if pair not in wanted:
                continue
            item_bbox = list(item.bbox)
            if not bbox_intersects(aoi, tuple(item_bbox)):
                continue
            for asset in item.assets.values():
                rows.append(
                    {
                        "theme": pair[0],
                        "type": pair[1],
                        "href": asset.href,
                        "asset_bbox": [float(v) for v in item_bbox],
                    }
                )
    return rows
```

- [ ] **Step 5: Re-run; confirm green.**
  `gbx:test:python --path test/sample/test_overture_discover.py`
  Expected: 11 passed.

- [ ] **Step 6: Commit.**
  `git add python/geobrix/src/databricks/labs/gbx/sample/_overture_discover.py python/geobrix/test/sample/_fake_overture_catalog.py python/geobrix/test/sample/test_overture_discover.py`
  `git commit -m "feat(sample): traverse Overture static STAC with injected opener" -m "traverse_catalog walks a pystac.Catalog-shaped root (children -> items), filters items by AOI bbox intersection, restricts to requested (theme,type) pairs, and emits one row per GeoParquet asset (theme/type/href/asset_bbox). Tested offline with an importable fake catalog (no network)." -m "Co-authored-by: Isaac"`

---

### Task 3: release resolution + optional `overturemaps` CLI fast-path

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/sample/_overture_discover.py`
- Test: `python/geobrix/test/sample/test_overture_discover.py`

**Interfaces:**
- Consumes: `traverse_catalog`, `expand_themes` (Tasks 1–2).
- Produces:
  - `resolve_release(opener, release=None) -> str` — `release=None` → the latest release id from the catalog (a catalog exposing `.extra_fields["overture:releases"]` as a sorted list, or `.id`); an explicit string returns unchanged. Raises `ValueError` if `release=None` and no release metadata is discoverable.
  - `cli_discover(bbox, theme_pairs, release, runner=subprocess.run) -> Optional[list[dict]]` — when the `overturemaps` CLI is importable/on PATH, shell out per `(theme, type)` to list intersecting parquet paths and return rows in the same shape as `traverse_catalog`; returns `None` when the CLI is unavailable (signals the caller to use the traversal fallback). `runner` is injectable for tests.

- [ ] **Step 1: Failing test for `resolve_release`.**
```python
# append to test_overture_discover.py
from databricks.labs.gbx.sample._overture_discover import resolve_release


class _RelCatalog:
    extra_fields = {"overture:releases": ["2024-01-01", "2024-07-01"]}


class _NoRelCatalog:
    extra_fields = {}
    id = None


def test_resolve_release_explicit_passthrough():
    assert resolve_release(lambda: _RelCatalog(), "2023-12-12") == "2023-12-12"


def test_resolve_release_latest():
    assert resolve_release(lambda: _RelCatalog(), None) == "2024-07-01"


def test_resolve_release_missing_raises():
    with pytest.raises(ValueError):
        resolve_release(lambda: _NoRelCatalog(), None)
```

- [ ] **Step 2: Run; confirm ImportError.**
  `gbx:test:python --path test/sample/test_overture_discover.py`
  Expected: `ImportError: cannot import name 'resolve_release'`.

- [ ] **Step 3: Implement `resolve_release` + import `subprocess`.**
```python
import subprocess  # add to the module imports


def resolve_release(opener, release: Optional[str] = None) -> str:
    """release=None -> latest release id from the catalog; an explicit string passes through."""
    if release is not None:
        return release
    catalog = opener()
    releases = getattr(catalog, "extra_fields", {}).get("overture:releases")
    if releases:
        return sorted(releases)[-1]
    cat_id = getattr(catalog, "id", None)
    if cat_id:
        return cat_id
    raise ValueError(
        "could not resolve latest Overture release from the catalog; pass release=... explicitly"
    )
```

- [ ] **Step 4: Re-run; confirm green.**
  `gbx:test:python --path test/sample/test_overture_discover.py`
  Expected: 14 passed.

- [ ] **Step 5: Failing test for `cli_discover` (injected runner; absent-CLI path).**
```python
# append to test_overture_discover.py
from databricks.labs.gbx.sample._overture_discover import cli_discover


def test_cli_discover_absent_returns_none(monkeypatch):
    # No overturemaps on PATH -> None so the caller falls back to traversal.
    monkeypatch.setattr(
        "databricks.labs.gbx.sample._overture_discover.shutil.which",
        lambda name: None,
    )
    assert cli_discover((-122.5, 37.7, -122.3, 37.8), [("buildings", "building")], "2024-07-01") is None


def test_cli_discover_present_parses_runner(monkeypatch):
    monkeypatch.setattr(
        "databricks.labs.gbx.sample._overture_discover.shutil.which",
        lambda name: "/usr/bin/overturemaps",
    )

    class _Completed:
        returncode = 0
        stdout = "s3://overturemaps-us-west-2/2024-07-01/buildings/building/part-0.parquet\n"

    rows = cli_discover(
        (-122.5, 37.7, -122.3, 37.8),
        [("buildings", "building")],
        "2024-07-01",
        runner=lambda *a, **k: _Completed(),
    )
    assert len(rows) == 1
    assert rows[0]["theme"] == "buildings"
    assert rows[0]["type"] == "building"
    assert rows[0]["href"].endswith("part-0.parquet")
    assert rows[0]["asset_bbox"] == [-122.5, 37.7, -122.3, 37.8]
```

- [ ] **Step 6: Run; confirm ImportError.**
  `gbx:test:python --path test/sample/test_overture_discover.py`
  Expected: `ImportError: cannot import name 'cli_discover'`.

- [ ] **Step 7: Implement `cli_discover` + import `shutil`.**
```python
import shutil  # add to the module imports


def cli_discover(bbox, theme_pairs, release, runner=subprocess.run):
    """Fast-path via the `overturemaps` CLI when present; None otherwise.

    Returns rows shaped like traverse_catalog (theme/type/href/asset_bbox). The
    asset_bbox is the AOI bbox (the CLI lists paths intersecting the bbox, not
    per-file extents), which is sufficient for downstream pushdown bookkeeping.
    """
    if shutil.which("overturemaps") is None:
        return None
    aoi = normalize_bbox(bbox)
    bbox_arg = ",".join(str(v) for v in aoi)
    rows = []
    for theme, type_ in theme_pairs:
        completed = runner(
            [
                "overturemaps",
                "download",
                "--bbox",
                bbox_arg,
                "--release",
                release,
                "--type",
                type_,
                "--list-paths",
            ],
            capture_output=True,
            text=True,
        )
        if getattr(completed, "returncode", 1) != 0:
            continue
        for line in (completed.stdout or "").splitlines():
            href = line.strip()
            if href:
                rows.append(
                    {
                        "theme": theme,
                        "type": type_,
                        "href": href,
                        "asset_bbox": list(aoi),
                    }
                )
    return rows
```

- [ ] **Step 8: Re-run; confirm green.**
  `gbx:test:python --path test/sample/test_overture_discover.py`
  Expected: 16 passed.

- [ ] **Step 9: Commit.**
  `git add python/geobrix/src/databricks/labs/gbx/sample/_overture_discover.py python/geobrix/test/sample/test_overture_discover.py`
  `git commit -m "feat(sample): Overture release resolution + CLI fast-path" -m "resolve_release picks the latest release from catalog metadata (or passes an explicit pin through); cli_discover shells out to the optional overturemaps CLI per (theme,type) and returns None when absent so callers fall back to static traversal. Runner injectable; tested offline." -m "Co-authored-by: Isaac"`

---

### Task 4: `OvertureClient.discover` → DataFrame

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/sample/overture.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/sample/__init__.py`
- Test: `python/geobrix/test/sample/test_overture.py`

**Interfaces:**
- Consumes: `_overture_discover.expand_themes`, `traverse_catalog`, `cli_discover`, `resolve_release`.
- Produces:
  - `class OvertureClient(catalog="https://stac.overturemaps.org/catalog.json", release=None, _catalog_opener=None, _get_fn=None)`.
  - `OvertureClient.discover(bbox, themes=None, release=None) -> DataFrame` — columns (in order): `theme: string`, `type: string`, `href: string`, `asset_bbox: array<double>`, `release: string`. `themes=None` → all themes/types. CLI fast-path is tried first (skipped when `_catalog_opener` is injected — offline tests force traversal); otherwise static traversal via the opener.

- [ ] **Step 1: Failing test for `discover`.**
```python
# python/geobrix/test/sample/test_overture.py
import pytest

pyspark = pytest.importorskip("pyspark")

from databricks.labs.gbx.sample.overture import OvertureClient
from test.sample._fake_overture_catalog import open_fake_overture


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("overture-sp1-test")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_discover_columns_and_filter(spark):
    client = OvertureClient(
        release="2024-07-01", _catalog_opener=open_fake_overture
    )
    df = client.discover((-122.45, 37.74, -122.40, 37.78), themes=["buildings"])
    assert df.columns == ["theme", "type", "href", "asset_bbox", "release"]
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["theme"] == "buildings"
    assert rows[0]["release"] == "2024-07-01"
    assert rows[0]["asset_bbox"] == [-122.52, 37.70, -122.36, 37.83]


def test_discover_all_themes_when_none(spark):
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)
    df = client.discover((-180, -90, 180, 90), themes=None)
    # fake catalog has a building + a place; both fall inside the world bbox
    assert {r["type"] for r in df.collect()} == {"building", "place"}
```

- [ ] **Step 2: Run; confirm collection/import failure.**
  `gbx:test:python --path test/sample/test_overture.py`
  Expected: `ModuleNotFoundError: No module named 'databricks.labs.gbx.sample.overture'`.

- [ ] **Step 3: Implement `OvertureClient.__init__` + `_open_catalog` + `discover`.**
```python
"""OvertureClient — distributed, AOI-driven Overture Maps GeoParquet data source.

Mirrors gbx.stac.StacClient: a static-catalog discovery step (driver-side,
metadata-only), then DISTRIBUTED asset I/O. Default I/O path is a distributed
Spark read of Overture GeoParquet over the cloud path with bbox-struct predicate
pushdown (AOI rows only), written to a UC Volume + an optional metadata Delta
table; an HTTP-href whole-file download is the fallback. Serverless-safe:
parallelism only via repartition(N, col); no spark.conf/cache/persist/.rdd.

Injection seams (offline tests): _catalog_opener (returns a pystac.Catalog-shaped
object) and _get_fn (an HTTP fetcher passed to the fallback downloader).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

OVERTURE_CATALOG = "https://stac.overturemaps.org/catalog.json"

# Spark schema for discover() output — pinned; SP2/SP3 depend on it.
_DISCOVER_COLS = ["theme", "type", "href", "asset_bbox", "release"]


def _discover_schema():
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )


class OvertureClient:
    def __init__(
        self,
        catalog: str = OVERTURE_CATALOG,
        release: Optional[str] = None,
        _catalog_opener=None,
        _get_fn=None,
    ):
        self.catalog = catalog
        self.release = release
        self._catalog_opener = _catalog_opener
        self._get_fn = _get_fn

    def _open_catalog(self):
        if self._catalog_opener is not None:
            return self._catalog_opener()
        import pystac

        return pystac.Catalog.from_file(self.catalog)

    def _opener(self):
        return self._catalog_opener if self._catalog_opener is not None else self._open_catalog

    def discover(self, bbox, themes=None, release=None) -> "DataFrame":
        """One row per intersecting GeoParquet asset for the AOI.

        Columns: theme, type, href, asset_bbox, release. themes=None => ALL.
        Driver-side + metadata-only (lightweight); asset I/O happens in download().
        """
        from pyspark.sql import SparkSession

        from databricks.labs.gbx.sample._overture_discover import (
            cli_discover,
            expand_themes,
            resolve_release,
            traverse_catalog,
        )

        opener = self._opener()
        rel = resolve_release(opener, release or self.release)
        pairs = expand_themes(themes)

        rows = None
        # CLI fast-path only in production (no injected opener); offline tests force traversal.
        if self._catalog_opener is None:
            rows = cli_discover(bbox, pairs, rel)
        if rows is None:
            rows = traverse_catalog(opener, bbox, pairs)

        for r in rows:
            r["release"] = rel

        spark = SparkSession.getActiveSession()
        schema = _discover_schema()
        if not rows:
            return spark.createDataFrame([], schema)
        ordered = [tuple(r[c] for c in _DISCOVER_COLS) for r in rows]
        return spark.createDataFrame(ordered, schema)
```

- [ ] **Step 4: Re-export `OvertureClient` from `sample/__init__.py`.**
  Add `OvertureClient` (and, after Task 10, `download_overture_aoi`) to the import block and `__all__`:
```python
from databricks.labs.gbx.sample.overture import OvertureClient

# ... extend __all__ with "OvertureClient"
```

- [ ] **Step 5: Re-run; confirm green.**
  `gbx:test:python --path test/sample/test_overture.py`
  Expected: 2 passed.

- [ ] **Step 6: Commit.**
  `git add python/geobrix/src/databricks/labs/gbx/sample/overture.py python/geobrix/src/databricks/labs/gbx/sample/__init__.py python/geobrix/test/sample/test_overture.py`
  `git commit -m "feat(sample): OvertureClient.discover over static STAC" -m "OvertureClient mirrors StacClient's shape and injection seams. discover() resolves the release, expands themes (None=>all), tries the CLI fast-path (production only) then static traversal, and returns a typed DataFrame (theme/type/href/asset_bbox/release). Re-exported from sample/__init__." -m "Co-authored-by: Isaac"`

---

### Task 5: distributed read + AOI rewrite (the performant default download path)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/sample/overture.py`
- Test: `python/geobrix/test/sample/test_overture.py`

**Interfaces:**
- Consumes: `OvertureClient.discover` output DataFrame.
- Produces:
  - private `OvertureClient._download_distributed(assets_df, out_dir, *, bbox, validate, partitions) -> DataFrame` — for each discovered asset, distributed-read the GeoParquet over its cloud `href` with a `bbox`-struct predicate pushdown (`F.col("bbox.xmin") <= maxx`, etc., when a `bbox` struct column is present; else read whole then no-op filter), repartition by `(theme, type, href)` (column hash, never number-only), write the AOI subset to `out_dir/<theme>/<type>/` as parquet on the Volume, and emit the metadata rows (cols per the `download` contract). Must not coalesce: tested via `getNumPartitions`.

- [ ] **Step 1: Failing test asserting distributed plan is not coalesced + AOI subset written.**
```python
# append to test_overture.py
import os


def _write_fake_overture_parquet(spark, path, bbox_struct=True):
    """Write a tiny GeoParquet-ish parquet with a bbox struct so pushdown can fire."""
    from pyspark.sql import Row

    if bbox_struct:
        rows = [
            Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76)),
            Row(id=2, bbox=Row(xmin=10.0, ymin=50.0, xmax=10.1, ymax=50.1)),  # outside SF
        ]
    else:
        rows = [Row(id=1), Row(id=2)]
    spark.createDataFrame(rows).write.mode("overwrite").parquet(path)


def test_download_distributed_writes_aoi_subset(spark, tmp_path):
    src = str(tmp_path / "src.parquet")
    _write_fake_overture_parquet(spark, src)
    out_dir = str(tmp_path / "out")

    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    assets = spark.createDataFrame(
        [("buildings", "building", src, [-122.52, 37.70, -122.36, 37.83], "2024-07-01")],
        schema,
    )
    meta = client._download_distributed(
        assets, out_dir, bbox=(-122.45, 37.74, -122.40, 37.78), validate=True, partitions=4
    )
    # Serverless-safety: hash-by-column repartition is NOT AQE-coalesced to 1.
    assert meta.rdd.getNumPartitions() > 1
    mrows = meta.collect()
    assert len(mrows) == 1
    written = mrows[0]["source"]
    assert written == mrows[0]["path"]  # source aliased as path
    assert os.path.isdir(written) or os.path.exists(written)
    # only the in-AOI row survived the bbox-struct pushdown
    subset = spark.read.parquet(written)
    assert subset.count() == 1
    assert subset.collect()[0]["id"] == 1
```

- [ ] **Step 2: Run; confirm AttributeError on the missing method.**
  `gbx:test:python --path test/sample/test_overture.py`
  Expected: `AttributeError: 'OvertureClient' object has no attribute '_download_distributed'`.

- [ ] **Step 3: Implement `_download_distributed`.**
```python
    def _download_distributed(
        self, assets_df, out_dir, *, bbox, validate, partitions
    ) -> "DataFrame":
        """Performant default: distributed read of each asset's GeoParquet with a
        bbox-struct predicate pushdown, AOI subset written to the Volume per
        (theme, type). Returns the metadata rows. Serverless-safe: repartition by
        column only; no spark.conf/cache/persist."""
        import os

        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        from databricks.labs.gbx.sample._overture_discover import normalize_bbox

        spark = SparkSession.getActiveSession()
        minx, miny, maxx, maxy = normalize_bbox(bbox)
        assets = assets_df.select(*_DISCOVER_COLS).collect()

        meta_rows = []
        for a in assets:
            df = spark.read.parquet(a["href"])
            # bbox-struct predicate pushdown when the Overture `bbox` struct is present.
            if "bbox" in df.columns:
                df = df.filter(
                    (F.col("bbox.xmin") <= F.lit(maxx))
                    & (F.col("bbox.xmax") >= F.lit(minx))
                    & (F.col("bbox.ymin") <= F.lit(maxy))
                    & (F.col("bbox.ymax") >= F.lit(miny))
                )
            target = os.path.join(out_dir, a["theme"], a["type"])
            # Hash-by-column repartition (NOT number-only): on Serverless a
            # round-robin repartition(N) is AQE-coalesced to 1 (serial). Hash by a
            # real source column so the per-asset row groups spread across cores.
            # Prefer the Overture `id` column; else hash by the first column.
            key = "id" if "id" in df.columns else df.columns[0]
            (
                df.repartition(partitions, F.col(key))
                .write.mode("overwrite")
                .parquet(target)
            )
            valid = True
            if validate:
                try:
                    spark.read.parquet(target).limit(1).count()
                except Exception:
                    valid = False
            try:
                sz = sum(
                    os.path.getsize(os.path.join(target, f))
                    for f in os.listdir(target)
                    if f.endswith(".parquet")
                )
            except OSError:
                sz = None
            meta_rows.append(
                {
                    "theme": a["theme"],
                    "type": a["type"],
                    "source": target,
                    "out_file_sz": sz,
                    "is_out_file_valid": valid,
                    "asset_bbox": a["asset_bbox"],
                    "release": a["release"],
                    "href": a["href"],
                }
            )

        return _meta_dataframe(spark, meta_rows, partitions)
```
  Add a module-level helper that builds the metadata DataFrame (reused by the fallback path in Task 6), repartitions by a column so the result stays distributed and is not AQE-coalesced, and aliases `source` as `path`:
```python
_META_COLS = [
    "theme",
    "type",
    "source",
    "path",
    "out_file_sz",
    "is_out_file_valid",
    "last_update",
    "asset_bbox",
    "release",
    "href",
]


def _meta_schema():
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("source", StringType()),
            StructField("out_file_sz", LongType()),
            StructField("is_out_file_valid", BooleanType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
            StructField("href", StringType()),
        ]
    )


def _meta_dataframe(spark, meta_rows, partitions):
    from pyspark.sql import functions as F

    cols = ["theme", "type", "source", "out_file_sz", "is_out_file_valid", "asset_bbox", "release", "href"]
    if not meta_rows:
        df = spark.createDataFrame([], _meta_schema())
    else:
        df = spark.createDataFrame(
            [tuple(r[c] for c in cols) for r in meta_rows], _meta_schema()
        )
    n = max(1, partitions or 1)
    return (
        # repartition by (theme, type, source) keeps the result distributed
        # (column-hash, not AQE-coalesced) per the Serverless rule.
        df.repartition(n, F.col("theme"), F.col("type"), F.col("source"))
        .withColumn("path", F.col("source"))
        .withColumn("last_update", F.current_timestamp())
        .select(
            "theme",
            "type",
            "source",
            "path",
            "out_file_sz",
            "is_out_file_valid",
            "last_update",
            "asset_bbox",
            "release",
            "href",
        )
    )
```
  Implementer note: the repartition hashes by a real source column (`id` when present, else the first column) — never a number-only `repartition(partitions)`, which AQE-coalesces to serial on Serverless. The Step 1 test asserts `getNumPartitions() > 1` on the resulting metadata DataFrame.

- [ ] **Step 4: Re-run; confirm green and the not-coalesced assertion holds.**
  `gbx:test:python --path test/sample/test_overture.py`
  Expected: 3 passed (the new test included).

- [ ] **Step 5: Commit.**
  `git add python/geobrix/src/databricks/labs/gbx/sample/overture.py python/geobrix/test/sample/test_overture.py`
  `git commit -m "feat(sample): distributed Overture read + AOI rewrite default" -m "Performant default download path: distributed-read each asset's GeoParquet with a bbox-struct predicate pushdown (AOI rows only), write the subset per (theme,type) to the Volume, emit metadata. Repartition by column (never number-only) so the plan stays distributed on Serverless; verified via getNumPartitions > 1." -m "Co-authored-by: Isaac"`

---

### Task 6: whole-file HTTP-href download fallback (injected `_get_fn`)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/sample/overture.py`
- Test: `python/geobrix/test/sample/test_overture.py`

**Interfaces:**
- Consumes: `OvertureClient.discover` output; the `_get_fn` seam.
- Produces:
  - private `OvertureClient._download_fallback(assets_df, out_dir, *, validate, max_tries, partitions) -> DataFrame` — fan whole-file downloads out with `repartition(N, F.col("href"))`, stream each `href` to `out_dir/<theme>/<type>/<basename>` (temp-file then sequential copy, Volume-safe), validate by opening the parquet, emit the same metadata schema as the distributed path. Uses `self._get_fn` when injected (offline tests), `requests.get` otherwise.

- [ ] **Step 1: Failing test with an injected fetcher (no network).**
```python
# append to test_overture.py
def test_download_fallback_injected_fetcher(spark, tmp_path):
    # build a real source parquet, then serve its bytes through a fake _get_fn
    src = str(tmp_path / "asset.parquet")
    from pyspark.sql import Row

    spark.createDataFrame([Row(id=1), Row(id=2)]).coalesce(1).write.mode(
        "overwrite"
    ).parquet(src)
    # the "asset" is a single part file inside src
    part = [f for f in os.listdir(src) if f.endswith(".parquet")][0]
    src_file = os.path.join(src, part)

    def fake_get(href, timeout=None, stream=False):
        class _Resp:
            status_code = 200

            def raise_for_status(self):
                pass

            def iter_content(self, n):
                with open(src_file, "rb") as fh:
                    while True:
                        chunk = fh.read(n)
                        if not chunk:
                            break
                        yield chunk

        return _Resp()

    client = OvertureClient(
        release="2024-07-01", _catalog_opener=open_fake_overture, _get_fn=fake_get
    )
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    out_dir = str(tmp_path / "out_fb")
    assets = spark.createDataFrame(
        [("places", "place", "http://fake/place.parquet", [0.0, 0.0, 1.0, 1.0], "2024-07-01")],
        schema,
    )
    meta = client._download_fallback(
        assets, out_dir, validate=True, max_tries=2, partitions=4
    )
    assert meta.rdd.getNumPartitions() > 1
    row = meta.collect()[0]
    assert row["is_out_file_valid"] is True
    assert os.path.exists(row["source"])
    assert row["source"] == row["path"]
    assert spark.read.parquet(row["source"]).count() == 2
```

- [ ] **Step 2: Run; confirm AttributeError.**
  `gbx:test:python --path test/sample/test_overture.py`
  Expected: `AttributeError: ... '_download_fallback'`.

- [ ] **Step 3: Implement `_download_fallback`.**
```python
    def _download_fallback(
        self, assets_df, out_dir, *, validate, max_tries, partitions
    ) -> "DataFrame":
        """Fallback: whole-file HTTP download fanned out by href (column-hash
        repartition, Serverless-safe). Temp-file then sequential copy (Volume-safe).
        Returns the same metadata schema as the distributed path."""
        import os
        import shutil
        import tempfile

        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            ArrayType,
            BooleanType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        spark = SparkSession.getActiveSession()
        get_fn = self._get_fn  # None in production; injectable for tests
        _validate = validate
        _max_tries = max_tries

        row_schema = StructType(
            [
                StructField("theme", StringType()),
                StructField("type", StringType()),
                StructField("source", StringType()),
                StructField("out_file_sz", LongType()),
                StructField("is_out_file_valid", BooleanType()),
                StructField("asset_bbox", ArrayType(DoubleType())),
                StructField("release", StringType()),
                StructField("href", StringType()),
            ]
        )

        @F.udf(row_schema)
        def _fetch(theme, type_, href, asset_bbox, release):
            getter = get_fn
            if getter is None:
                import requests

                getter = requests.get
            target_dir = os.path.join(out_dir, theme, type_)
            os.makedirs(target_dir, exist_ok=True)
            basename = os.path.basename(href.split("?")[0]) or "asset.parquet"
            outpath = os.path.join(target_dir, basename)
            # idempotent skip: a present, openable file is left as-is
            if os.path.exists(outpath) and _is_valid_parquet(outpath):
                sz = os.path.getsize(outpath)
                return (theme, type_, outpath, sz, True, asset_bbox, release, href)
            last_exc = None
            for _ in range(max(1, _max_tries)):
                tmpd = tempfile.mkdtemp(prefix="gbx_overture_")
                try:
                    local = os.path.join(tmpd, basename)
                    resp = getter(href, timeout=100, stream=True)
                    resp.raise_for_status()
                    with open(local, "wb") as fh:
                        for chunk in resp.iter_content(1024 * 1024):
                            if chunk:
                                fh.write(chunk)
                    ok = (not _validate) or _is_valid_parquet(local)
                    if ok:
                        shutil.copyfile(local, outpath)
                        sz = os.path.getsize(outpath)
                        return (theme, type_, outpath, sz, True, asset_bbox, release, href)
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                finally:
                    shutil.rmtree(tmpd, ignore_errors=True)
            return (theme, type_, None, None, False, asset_bbox, release, href)

        n = max(1, partitions or 1)
        fetched = (
            assets_df.select(*_DISCOVER_COLS)
            # column-hash repartition by href (NOT number-only) for Serverless.
            .repartition(n, F.col("href"))
            .withColumn(
                "_m",
                _fetch("theme", "type", "href", "asset_bbox", "release"),
            )
            .select("_m.*")
            .withColumn("path", F.col("source"))
            .withColumn("last_update", F.current_timestamp())
            .select(
                "theme",
                "type",
                "source",
                "path",
                "out_file_sz",
                "is_out_file_valid",
                "last_update",
                "asset_bbox",
                "release",
                "href",
            )
        )
        return fetched
```
  Add the module-level parquet validity helper (importable inside the UDF):
```python
def _is_valid_parquet(path: str) -> bool:
    """True iff the parquet opens (pyarrow). Validity = opens, not raster-decodable."""
    try:
        import pyarrow.parquet as pq

        pq.ParquetFile(path).metadata  # touch metadata to force a read
        return True
    except Exception:  # noqa: BLE001
        return False
```

- [ ] **Step 4: Re-run; confirm green.**
  `gbx:test:python --path test/sample/test_overture.py`
  Expected: 4 passed.

- [ ] **Step 5: Commit.**
  `git add python/geobrix/src/databricks/labs/gbx/sample/overture.py python/geobrix/test/sample/test_overture.py`
  `git commit -m "feat(sample): whole-file Overture download fallback path" -m "Asset-level HTTP-href download for when no cloud read is available: fan out by href (column-hash repartition, Serverless-safe), temp-file then sequential copy (Volume-safe), retry + parquet-open validate + idempotent skip. _get_fn injectable; tested offline with a fake fetcher serving real parquet bytes." -m "Co-authored-by: Isaac"`

---

### Task 7: public `download` — path selection, validation, idempotency

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/sample/overture.py`
- Test: `python/geobrix/test/sample/test_overture.py`

**Interfaces:**
- Consumes: `_download_distributed`, `_download_fallback`.
- Produces:
  - `OvertureClient.download(assets_df, out_dir, *, table=None, validate=True, max_tries=5, partitions=None) -> DataFrame` — columns (order pinned): `theme, type, source, path, out_file_sz, is_out_file_valid, last_update, asset_bbox, release, href`. Chooses the distributed read path when an asset `href` is a cloud path (`s3://`/`abfs://`/`abfss://`/`gs://`/`wasbs://`); otherwise the HTTP-href fallback. (The `table=` MERGE is added in Task 8.) `partitions=None` defaults to `max(1, asset_count)`.

- [ ] **Step 1: Failing test for path selection + idempotent skip.**
```python
# append to test_overture.py
def test_download_routes_cloud_to_distributed(spark, tmp_path):
    src = str(tmp_path / "cloudish.parquet")
    from pyspark.sql import Row

    # simulate a cloud asset with a local path that LOOKS like a cloud read target
    spark.createDataFrame([Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76))]).write.mode(
        "overwrite"
    ).parquet(src)
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    # local file path -> not a cloud scheme, so it routes to distributed-read anyway
    # only when the caller forces it; here use a real local path and assert columns + idempotency
    assets = spark.createDataFrame(
        [("buildings", "building", src, [-122.52, 37.70, -122.36, 37.83], "2024-07-01")],
        schema,
    )
    out_dir = str(tmp_path / "dl")
    meta = client.download(
        assets, out_dir, bbox=(-122.45, 37.74, -122.40, 37.78), partitions=4
    )
    assert meta.columns == [
        "theme",
        "type",
        "source",
        "path",
        "out_file_sz",
        "is_out_file_valid",
        "last_update",
        "asset_bbox",
        "release",
        "href",
    ]
    first = meta.collect()
    assert len(first) == 1 and first[0]["is_out_file_valid"] is True
    # idempotent re-run: same target, still valid, no error
    meta2 = client.download(
        assets, out_dir, bbox=(-122.45, 37.74, -122.40, 37.78), partitions=4
    )
    assert meta2.collect()[0]["is_out_file_valid"] is True
```

- [ ] **Step 2: Run; confirm TypeError (download takes no `bbox`) / AttributeError.**
  `gbx:test:python --path test/sample/test_overture.py`
  Expected: `TypeError: download() got an unexpected keyword argument 'bbox'` (or the method is absent).

- [ ] **Step 3: Implement `download` with the routing.** Add `bbox` to the public signature (needed for the distributed pushdown; default `None` → read whole asset). Pin the kwargs from the spec contract plus `bbox`:
```python
    _CLOUD_SCHEMES = ("s3://", "s3a://", "abfs://", "abfss://", "gs://", "wasbs://")

    def download(
        self,
        assets_df,
        out_dir,
        *,
        bbox=None,
        table=None,
        validate=True,
        max_tries=5,
        partitions=None,
    ) -> "DataFrame":
        """Distributed download of discovered assets to out_dir (a Volume).

        Default path: distributed read + AOI rewrite with bbox-struct pushdown
        (when assets are cloud-readable). Fallback: whole-file HTTP-href download.
        table=<name> UPSERTs the metadata to a Delta table keyed by
        (theme, type, source). Serverless-safe; idempotent skip on valid targets.
        """
        from pyspark.sql import functions as F

        assets = assets_df.select(*_DISCOVER_COLS)
        n = partitions if partitions is not None else max(1, assets.count())

        hrefs = [r["href"] for r in assets.select("href").distinct().collect()]
        is_cloud = bool(hrefs) and all(
            any(h.startswith(s) for s in self._CLOUD_SCHEMES)
            or h.startswith("/")  # local Volume / FUSE path is Spark-readable
            for h in hrefs
        )

        if is_cloud:
            meta = self._download_distributed(
                assets, out_dir, bbox=bbox, validate=validate, partitions=n
            )
        else:
            meta = self._download_fallback(
                assets, out_dir, validate=validate, max_tries=max_tries, partitions=n
            )

        if table is not None:
            meta = self._merge_metadata(meta, table)  # added in Task 8
        return meta
```
  Note for the implementer: until Task 8 lands `_merge_metadata`, keep the `table` branch out (the spec contract has `table=None` default, so this test passes without it). Add the `_merge_metadata` call in Task 8.

- [ ] **Step 4: Re-run; confirm green.**
  `gbx:test:python --path test/sample/test_overture.py`
  Expected: 5 passed.

- [ ] **Step 5: Commit.**
  `git add python/geobrix/src/databricks/labs/gbx/sample/overture.py python/geobrix/test/sample/test_overture.py`
  `git commit -m "feat(sample): OvertureClient.download path selection" -m "Public download() routes cloud-readable assets to the distributed read+AOI-rewrite path and http hrefs to the whole-file fallback, returns the pinned metadata schema (source aliased as path), and is idempotent on re-run. Partitions default to asset count." -m "Co-authored-by: Isaac"`

---

### Task 8: metadata Delta MERGE table output

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/sample/overture.py`
- Test: `python/geobrix/test/sample/test_overture.py`

**Interfaces:**
- Consumes: `download` metadata DataFrame.
- Produces:
  - private `OvertureClient._merge_metadata(meta_df, table) -> DataFrame` — first run creates `table` (Delta) from `meta_df`; subsequent runs UPSERT via `DeltaTable.merge` keyed by `(theme, type, source)` (whenMatchedUpdate the volatile cols `path, out_file_sz, is_out_file_valid, last_update, asset_bbox, release, href`; whenNotMatchedInsertAll). Returns `meta_df` unchanged. Mirrors `StacClient.repair`'s MERGE.

- [ ] **Step 1: Failing test for create-then-upsert idempotency.** Requires Delta; gate with `importorskip`.
```python
# append to test_overture.py
def test_download_table_merge_idempotent(spark, tmp_path):
    pytest.importorskip("delta")
    # a SparkSession with Delta configured is required; skip if not available
    try:
        spark.sql("SELECT 1")  # sanity
        spark.range(1).write.format("delta").mode("overwrite").save(
            str(tmp_path / "_delta_probe")
        )
    except Exception:
        pytest.skip("Delta not enabled on this local SparkSession")

    src = str(tmp_path / "m.parquet")
    from pyspark.sql import Row

    spark.createDataFrame([Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76))]).write.mode(
        "overwrite"
    ).parquet(src)
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )
    assets = spark.createDataFrame(
        [("buildings", "building", src, [-122.52, 37.70, -122.36, 37.83], "2024-07-01")],
        schema,
    )
    table = "overture_meta_test"
    out_dir = str(tmp_path / "dl2")
    client.download(assets, out_dir, bbox=(-122.45, 37.74, -122.40, 37.78), table=table, partitions=2)
    client.download(assets, out_dir, bbox=(-122.45, 37.74, -122.40, 37.78), table=table, partitions=2)
    # MERGE keyed by (theme, type, source) -> still exactly one row, not two
    assert spark.table(table).count() == 1
    spark.sql(f"DROP TABLE IF EXISTS {table}")
```
  Note: if the module-scoped `spark` fixture lacks Delta, add the Delta extension/catalog config to the fixture builder (`spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`, `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`) guarded by an `importorskip("delta")`, OR keep this test skipped locally and exercise the MERGE in the optional Volume/cluster smoke (note below). Prefer configuring the fixture so the test runs in Docker where `delta-spark` is present.

- [ ] **Step 2: Run; confirm failure (no `_merge_metadata` wired / table not created).**
  `gbx:test:python --path test/sample/test_overture.py::test_download_table_merge_idempotent`
  Expected: failure or skip if Delta unavailable; failure (`AnalysisException`/`AttributeError`) when Delta is present.

- [ ] **Step 3: Implement `_merge_metadata` and wire it into `download`.**
```python
    def _merge_metadata(self, meta_df, table):
        """Create or UPSERT the metadata Delta table keyed by (theme, type, source)."""
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if not spark.catalog.tableExists(table):
            meta_df.write.format("delta").mode("overwrite").saveAsTable(table)
            return meta_df

        from delta.tables import DeltaTable

        dt = DeltaTable.forName(spark, table)
        (
            dt.alias("t")
            .merge(
                meta_df.alias("u"),
                "t.theme = u.theme AND t.type = u.type AND t.source = u.source",
            )
            .whenMatchedUpdate(
                set={
                    "path": "u.path",
                    "out_file_sz": "u.out_file_sz",
                    "is_out_file_valid": "u.is_out_file_valid",
                    "last_update": "u.last_update",
                    "asset_bbox": "u.asset_bbox",
                    "release": "u.release",
                    "href": "u.href",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        return meta_df
```
  In `download`, keep the `if table is not None: meta = self._merge_metadata(meta, table)` branch (now real).

- [ ] **Step 4: Re-run; confirm green (or skipped on a Delta-less local session, green in Docker).**
  `gbx:test:python --path test/sample/test_overture.py`
  Expected: all pass in Docker; locally the Delta test may skip.

- [ ] **Step 5: Commit.**
  `git add python/geobrix/src/databricks/labs/gbx/sample/overture.py python/geobrix/test/sample/test_overture.py`
  `git commit -m "feat(sample): metadata Delta MERGE for Overture downloads" -m "table=<name> persists/UPSERTs the per-asset metadata to a Delta table, idempotent MERGE keyed by (theme,type,source) (mirrors StacClient.repair). First run creates the table; re-runs update volatile cols so the catalog stays queryable and re-runnable." -m "Co-authored-by: Isaac"`

---

### Task 9: `read` — from a Volume dir and from a metadata table/DataFrame

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/sample/overture.py`
- Test: `python/geobrix/test/sample/test_overture.py`

**Interfaces:**
- Consumes: downloaded parquet on a Volume; the metadata Delta table/DataFrame (`source`/`path` column).
- Produces:
  - `OvertureClient.read(source, theme=None, type=None, bbox=None) -> DataFrame` — `source` may be (a) a Volume directory (read parquet recursively, filter by `theme`/`type` sub-path or columns), (b) a metadata Delta **table name** (string resolving to a table with a `source`/`path` column → union-read each per-asset path), or (c) a metadata **DataFrame** (same). `bbox` applies the `bbox`-struct AOI filter when a `bbox` struct column is present.

- [ ] **Step 1: Failing test for both read modes.**
```python
# append to test_overture.py
def test_read_from_volume_dir(spark, tmp_path):
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)
    out_dir = str(tmp_path / "rd")
    target = os.path.join(out_dir, "buildings", "building")
    from pyspark.sql import Row

    spark.createDataFrame(
        [Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76))]
    ).write.mode("overwrite").parquet(target)
    df = client.read(out_dir)
    assert df.count() == 1
    # bbox filter retains the in-AOI row
    df2 = client.read(out_dir, bbox=(-122.45, 37.74, -122.40, 37.78))
    assert df2.count() == 1
    df3 = client.read(out_dir, bbox=(0, 0, 1, 1))  # disjoint
    assert df3.count() == 0


def test_read_from_metadata_dataframe(spark, tmp_path):
    client = OvertureClient(release="2024-07-01", _catalog_opener=open_fake_overture)
    target = str(tmp_path / "assetdir")
    from pyspark.sql import Row

    spark.createDataFrame([Row(id=7)]).write.mode("overwrite").parquet(target)
    from pyspark.sql import functions as F

    meta = spark.createDataFrame([(target,)], ["source"]).withColumn(
        "path", F.col("source")
    )
    df = client.read(meta)
    assert df.collect()[0]["id"] == 7
```

- [ ] **Step 2: Run; confirm AttributeError.**
  `gbx:test:python --path test/sample/test_overture.py`
  Expected: `AttributeError: ... 'read'`.

- [ ] **Step 3: Implement `read`.**
```python
    def read(self, source, theme=None, type=None, bbox=None) -> "DataFrame":
        """Load downloaded GeoParquet back into Spark with an optional bbox AOI filter.

        source may be a Volume directory, a metadata Delta table NAME, or a
        metadata DataFrame carrying a source/path column pointing at per-asset paths.
        """
        from pyspark.sql import DataFrame as _DF
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = SparkSession.getActiveSession()

        def _read_paths(paths):
            # union-read each per-asset path (recursive parquet under each)
            dfs = [spark.read.parquet(p) for p in paths]
            out = dfs[0]
            for d in dfs[1:]:
                out = out.unionByName(d, allowMissingColumns=True)
            return out

        if isinstance(source, _DF):
            col = "source" if "source" in source.columns else "path"
            paths = [r[col] for r in source.select(col).distinct().collect()]
            df = _read_paths(paths)
        elif isinstance(source, str) and spark.catalog.tableExists(source):
            meta = spark.table(source)
            col = "source" if "source" in meta.columns else "path"
            paths = [r[col] for r in meta.select(col).distinct().collect()]
            df = _read_paths(paths)
        else:
            # a Volume directory: read parquet recursively (per theme/type subdirs)
            base = source
            if theme is not None and type is not None:
                import os

                base = os.path.join(source, theme, type)
            df = spark.read.option("recursiveFileLookup", "true").parquet(base)

        if bbox is not None and "bbox" in df.columns:
            from databricks.labs.gbx.sample._overture_discover import normalize_bbox

            minx, miny, maxx, maxy = normalize_bbox(bbox)
            df = df.filter(
                (F.col("bbox.xmin") <= F.lit(maxx))
                & (F.col("bbox.xmax") >= F.lit(minx))
                & (F.col("bbox.ymin") <= F.lit(maxy))
                & (F.col("bbox.ymax") >= F.lit(miny))
            )
        return df
```

- [ ] **Step 4: Re-run; confirm green.**
  `gbx:test:python --path test/sample/test_overture.py`
  Expected: 7 (+ Delta-gated) passed.

- [ ] **Step 5: Commit.**
  `git add python/geobrix/src/databricks/labs/gbx/sample/overture.py python/geobrix/test/sample/test_overture.py`
  `git commit -m "feat(sample): OvertureClient.read from dir or metadata table" -m "read() loads downloaded GeoParquet back into Spark from a Volume directory, a metadata Delta table name, or a metadata DataFrame (source/path column), with an optional bbox-struct AOI filter. The metadata table can directly drive distributed reads." -m "Co-authored-by: Isaac"`

---

### Task 10: `download_overture_aoi` convenience one-shot

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/sample/overture.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/sample/__init__.py`
- Test: `python/geobrix/test/sample/test_overture.py`

**Interfaces:**
- Consumes: `OvertureClient.discover` + `download`.
- Produces:
  - module fn `download_overture_aoi(bbox, out_dir, themes=None, release=None, table=None) -> DataFrame` — constructs a default `OvertureClient`, `discover`s the AOI, then `download`s (passing `bbox` for the AOI pushdown and `table`). Returns the metadata DataFrame.

- [ ] **Step 1: Failing test (offline via injected opener through a default-client monkeypatch).**
```python
# append to test_overture.py
def test_download_overture_aoi_one_shot(spark, tmp_path, monkeypatch):
    # Force the convenience fn's default client to use the fake opener (offline).
    import databricks.labs.gbx.sample.overture as ov

    src = str(tmp_path / "aoi.parquet")
    from pyspark.sql import Row

    spark.createDataFrame(
        [Row(id=1, bbox=Row(xmin=-122.42, ymin=37.75, xmax=-122.41, ymax=37.76))]
    ).write.mode("overwrite").parquet(src)

    orig_init = ov.OvertureClient.__init__

    def patched_init(self, *a, **k):
        k["_catalog_opener"] = open_fake_overture
        orig_init(self, *a, **k)

    monkeypatch.setattr(ov.OvertureClient, "__init__", patched_init)

    # The fake catalog's SF building href points at s3://...; rewrite discover to our local src
    orig_discover = ov.OvertureClient.discover

    def patched_discover(self, bbox, themes=None, release=None):
        df = orig_discover(self, bbox, themes=themes, release=release)
        from pyspark.sql import functions as F

        return df.withColumn("href", F.lit(src))

    monkeypatch.setattr(ov.OvertureClient, "discover", patched_discover)

    out_dir = str(tmp_path / "oneshot")
    meta = ov.download_overture_aoi(
        (-122.45, 37.74, -122.40, 37.78), out_dir, themes=["buildings"], release="2024-07-01"
    )
    rows = meta.collect()
    assert len(rows) == 1
    assert rows[0]["theme"] == "buildings"
    assert rows[0]["is_out_file_valid"] is True
```

- [ ] **Step 2: Run; confirm AttributeError (no `download_overture_aoi`).**
  `gbx:test:python --path test/sample/test_overture.py`
  Expected: `AttributeError: module ... has no attribute 'download_overture_aoi'`.

- [ ] **Step 3: Implement `download_overture_aoi` and re-export it.**
```python
def download_overture_aoi(bbox, out_dir, themes=None, release=None, table=None) -> "DataFrame":
    """One-shot: discover the AOI's Overture assets and download them to out_dir.

    Constructs a default OvertureClient, discovers (themes=None => all), then
    downloads with the AOI bbox pushdown and an optional metadata Delta table.
    """
    client = OvertureClient(release=release)
    assets = client.discover(bbox, themes=themes, release=release)
    return client.download(assets, out_dir, bbox=bbox, table=table)
```
  In `sample/__init__.py` add `download_overture_aoi` to the `overture` import and to `__all__`.

- [ ] **Step 4: Re-run; confirm green.**
  `gbx:test:python --path test/sample/test_overture.py`
  Expected: all (non-Delta-gated) pass.

- [ ] **Step 5: Commit.**
  `git add python/geobrix/src/databricks/labs/gbx/sample/overture.py python/geobrix/src/databricks/labs/gbx/sample/__init__.py python/geobrix/test/sample/test_overture.py`
  `git commit -m "feat(sample): download_overture_aoi one-shot convenience" -m "Module-level convenience that discovers an AOI's Overture assets and downloads them in one call (themes=None => all), passing the AOI bbox pushdown and optional metadata Delta table through. Re-exported from sample/__init__." -m "Co-authored-by: Isaac"`

---

### Task 11: light-CI-lock wiring (deps + test dir registration) + final green run

**Files:**
- Modify: `python/geobrix/requirements-pyrx-ci.in` (+ regenerate `requirements-pyrx-ci.txt`)
- Modify: `python/geobrix/requirements-dev-container.in` (+ regenerate `requirements-dev-container.txt`)
- Modify: `python/geobrix/pyproject.toml` (add `pystac` to a new/extended `overture` extra or the `stac` extra)
- Modify: `python/geobrix/test/conftest.py` (`_LIGHT_TEST_DIRS`)
- Modify: `.github/actions/pyrx_build/action.yml` (pytest dir list)
- Test: full light-suite run

**Interfaces:**
- Consumes: the `pystac` runtime dependency introduced by `_overture_discover._open_catalog`.
- Produces: a CI environment that installs `pystac` and a test phase that RUNS `test/sample/`.

- [ ] **Step 1: Add `pystac` to both `.in` files.** In `requirements-pyrx-ci.in`, add under a new comment block (mirroring the STAC block):
```
# --- Overture static-STAC catalog traversal (gbx.sample.overture). pystac is the
# static-catalog reader (distinct from pystac-client, which is the search-API client
# the stac/ suite stubs). geopandas/pyarrow for parquet read are already pinned above. ---
pystac==1.11.0
```
  Add the same `pystac==1.11.0` pin to `requirements-dev-container.in` under the "geospatial dev stack" block. (Confirm `1.11.0` resolves on the corp PyPI proxy; bump to the latest <2 that does.)

- [ ] **Step 2: Add the `overture` extra to `pyproject.toml`.** Mirror the `stac` extra:
```toml
# Overture Maps data source (gbx.sample.overture): static-STAC catalog traversal.
# pystac is the static-catalog reader; geopandas/pyarrow (in [light]) do parquet read.
overture = [
    "pystac>=1.9,<2",
]
```

- [ ] **Step 3: Register `test/sample` in `conftest.py`.** Add `"sample"` to `_LIGHT_TEST_DIRS` and extend the "Light test dirs so far:" docstring line to include `sample`.

- [ ] **Step 4: Register `test/sample` in the light CI phase.** In `.github/actions/pyrx_build/action.yml`, add `test/sample` to the pytest dir list and extend the inline comment listing the light dirs:
```
pytest test/pyrx test/ds test/pyvx test/pygx test/pmtiles_light test/stac test/vizx test/sample -m "not integration" -v
```

- [ ] **Step 5: Regenerate the hash-pinned locks (in Docker, per the .in header instructions).** Dispatch via a Task subagent (long-running, touches the container):
  - pyrx-ci: `uv pip compile --generate-hashes --python-version 3.12 --output-file requirements-pyrx-ci.txt requirements-pyrx-ci.in` (cwd `python/geobrix`).
  - dev-container: `docker exec geobrix-dev bash -lc 'cd /root/geobrix/python/geobrix && uv pip compile --generate-hashes --python-version 3.12 --output-file requirements-dev-container.txt requirements-dev-container.in'`.
  Expected: both `.txt` files gain hashed `pystac==...` (and any new transitive pins).

- [ ] **Step 6: Run the full sample suite green (in Docker, all tests incl. Delta).** Dispatch via a Task subagent:
  `gbx:test:python --path test/sample/ --log overture-sp1.log`
  Expected: all of `test_overture_discover.py` + `test_overture.py` pass, including the Delta MERGE test (Docker has `delta-spark`).

- [ ] **Step 7: Lint before commit.**
  `gbx:lint:python --check` (verify isort/black/flake8 against the Docker formatter; reformat in-container if the host black differs).
  Expected: clean.

- [ ] **Step 8: Commit.**
  `git add python/geobrix/requirements-pyrx-ci.in python/geobrix/requirements-pyrx-ci.txt python/geobrix/requirements-dev-container.in python/geobrix/requirements-dev-container.txt python/geobrix/pyproject.toml python/geobrix/test/conftest.py .github/actions/pyrx_build/action.yml`
  `git commit -m "build(sample): light-CI-lock wiring for gbx.sample.overture" -m "Add pystac (static-STAC traversal) to both CI .in locks + regenerate hashed .txt; add the [overture] extra; register test/sample in both the heavy-skip _LIGHT_TEST_DIRS and the light-phase pytest dir list so the suite is collected in Docker/light CI and skipped in the heavy env. Full sample suite green." -m "Co-authored-by: Isaac"`

---

### Task 12: capture validated performance gains (standing practice)

**Files:**
- Create (if a gain is validated): `docs/superpowers/performance/README.md` (index, first time) + `docs/superpowers/performance/<slug>.md` (the pattern)
- Add: a thin pointer memory under the user's geobrix memory dir (slug + one-line `[[link]]`)

**Interfaces:**
- Consumes: any measured distribution/throughput gain surfaced while building SP1 (e.g. bbox-struct pushdown reducing rows read; column-hash repartition keeping the AOI rewrite distributed vs a number-only repartition coalescing to serial).
- Produces: a recorded pattern with an applicability matrix, per the spec's performance methodology.

- [ ] **Step 1: Decide whether SP1 produced a validated gain.** The likely candidate is "column-hash repartition keeps the Overture AOI rewrite distributed where number-only repartition coalesces to serial on Serverless" — already a known rule, so assess whether SP1 adds a *new* validated data point (e.g. measured row-reduction from bbox-struct pushdown on real Overture parquet during the optional Volume/cluster smoke). If no NEW gain is measured offline, record the assessment verdict ("no new gain beyond the existing repartition-by-column rule; pushdown row-reduction to be measured in the optional cluster smoke") and skip the corpus file.

- [ ] **Step 2: If a gain is validated, write the corpus pattern file.** Create `docs/superpowers/performance/README.md` (if absent) as a one-line index, then `docs/superpowers/performance/overture-bbox-pushdown.md` with sections: problem → symptom/signature → the fix → applicability matrix (light-similar: other `sample`/`ds` distributed readers; heavy-same+similar: N/A — no heavy Overture path) → evidence/bench numbers → canonical code refs (`overture.py::_download_distributed`).

- [ ] **Step 3: Add the paired thin pointer memory.** One line: slug + one-line summary that `[[links]]` to the corpus file (do not bloat MEMORY.md; keep under ~200 chars).

- [ ] **Step 4: Commit (only the docs/memory, if created).**
  `git add docs/superpowers/performance/`
  `git commit -m "docs(perf): capture Overture distribution gain assessment" -m "Per the tiling performance methodology: record the SP1 distribution finding (bbox-struct pushdown + column-hash repartition keeping the AOI rewrite distributed) with its applicability matrix (light-similar readers; no heavy Overture path), or the not-applicable verdict when no new gain beyond the existing repartition-by-column rule was measured." -m "Co-authored-by: Isaac"`

---

## Optional Volume / cluster smoke (not required for SP1 unit tests)

All SP1 unit tests above run **offline** (injected `_catalog_opener` + injected `_get_fn` + a local `SparkSession`); Docker is needed only for the Delta MERGE test (Task 8) and the lock regeneration (Task 11). The following are **optional** validations, deferred but noted:

- A real-network discover against `https://stac.overturemaps.org/catalog.json` (confirms the live catalog shape matches `traverse_catalog`'s assumptions; spec open item).
- A cluster/Serverless smoke confirming Spark can directly read Overture's public cloud paths (`s3://overturemaps-us-west-2` / `abfs://...`) for the distributed default — the spec flags this as a "validate during SP1" open item. If direct cloud read is unavailable/requester-pays-blocked, the HTTP-href fallback becomes the primary path (the code already supports both); record the verdict in the perf corpus (Task 12).
