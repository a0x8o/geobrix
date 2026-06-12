# Light Vector Readers (`*_gbx`, pyogrio) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build five pyogrio-backed PySpark DataSource V2 vector readers (`ogr_gbx` + `shapefile_gbx`/`geojson_gbx`/`gpkg_gbx`/`file_gdb_gbx`) that emit the exact same schema as the heavy Scala OGR readers, so the light tier reaches reader parity for vector data.

**Architecture:** One module `databricks.labs.gbx.ds.vector` holds the generic `OgrGbxReader`/`OgrGbxDataSource` (schema from `pyogrio.read_info`, partitions = `chunkSize`-feature slices via `read_arrow(skip_features, max_features)`, rows = arrow→WKB tuples) plus four ~3-line named presets (subclass + `driverName`). Pure-Python (pyogrio + pyproj + shapely + pyarrow), Serverless-safe, registered via `gbx.ds.register`.

**Tech Stack:** PySpark DataSource V2, `pyogrio` 0.12.x (`read_info`/`read_arrow`), `pyproj` (CRS→srid/proj4), `shapely` (WKT branch), `pyarrow`.

**Reference spec:** `docs/superpowers/specs/2026-06-12-light-vector-readers-design.md`

---

## Conventions / key facts (read before starting)

- **Run tests** with the repo venv: `/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pytest <path> -v -p no:cacheprovider`. Light/unit tests need no Docker; light-vs-heavy parity is Docker/integration (skip-if-heavy).
- **Serverless-safe:** product code in `gbx/ds/` must NOT use `._jvm`/`.sparkContext`/`.rdd`/`.conf.set`/`SparkConf`. Import `pyogrio`/`pyproj`/`shapely` **inside `read()`** (lazy), like `raster.py` imports rasterio lazily — keeps partitions picklable + Serverless-safe.
- **Commit hygiene:** before each `git commit` run `chmod -R u+rwX .git/objects`; trailer EXACTLY `Co-authored-by: Isaac` (repo convention; a security linter may warn — ignore; never a human name); subjects ≤72 chars; NO push.
- **Heavy schema to match (parity bar)** — from `OGR_SchemaInference.scala`:
  - Field order: **attributes first, then geometry columns.**
  - Attribute column = OGR field name (or `field_<j>` if empty), Spark type per `getType`:
    `Boolean→BooleanType, Integer→IntegerType, Integer64→LongType, Real→DoubleType, String/WideString→StringType, Date→DateType, Time→TimestampType, DateTime→TimestampType, Binary→BinaryType, IntegerList→ArrayType(IntegerType), RealList→ArrayType(DoubleType), StringList/WideStringList→ArrayType(StringType), else→StringType.`
  - Geometry (single field `j=0` in v1): `geom_0` (`BinaryType` WKB if `asWKB=true` default, else `StringType` WKT) using the OGR geom field name if present else `geom_0`; then `geom_0_srid` (`StringType`, authority code e.g. `"4326"`, fallback `"0"`); then `geom_0_srid_proj` (`StringType`, PROJ4, fallback `""`). All nullable.
  - Heavy options (exact names): `driverName` (default `""`=auto), `asWKB` (default `"true"`), `chunkSize` (default `"10000"`), `layerNumber` (default `"0"`), `layerName` (default `""`).
- **pyogrio facts (verified, 0.12.1):**
  - `read_info(path, layer=…)` → dict with `crs` (e.g. `"EPSG:4326"`), `fields` (list of names), `ogr_types` (list like `"OFTString"`/`"OFTInteger"`/`"OFTReal"`/`"OFTInteger64"`/`"OFTDate"`/`"OFTDateTime"`/`"OFTTime"`/`"OFTBinary"`/list variants), `ogr_subtypes` (e.g. `"OFSTBoolean"`/`"OFSTNone"`), `geometry_name` (often `""`), `features` (count), `layer_name`.
  - `read_arrow(path, layer=…, skip_features=, max_features=, read_geometry=True, datetime_as_string=False)` → `(meta, pyarrow.Table)`. The geometry column in the table is named `meta["geometry_name"] or "wkb_geometry"`, type `binary` (WKB). Attribute columns precede it.
  - CRS→srid/proj4: `from pyproj import CRS; c = CRS.from_user_input(info["crs"]); auth = c.to_authority()  # ('EPSG','4326') or None; proj4 = c.to_proj4()`.
- **Corpus paths** (doc-tests import `SAMPLE_DATA_BASE` from `docs/tests/python/readers/path_config.py`):
  - GeoJSON: `{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojson`
  - GeoJSONSeq: `{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojsonl`
  - Shapefile (zip): `{SAMPLE_DATA_BASE}/nyc/subway/nyc_subway.shp.zip`
  - GeoPackage: `{SAMPLE_DATA_BASE}/nyc/geopackage/nyc_complete.gpkg`
  - FileGDB (zip): `{SAMPLE_DATA_BASE}/nyc/filegdb/NYC_Sample.gdb.zip`
  - For local unit tests (no Volumes), tasks below generate tiny in-memory/temp vector files instead.

## File map

- Create: `python/geobrix/src/databricks/labs/gbx/ds/vector.py` — all five readers + `_vector_schema` + `_ogr_to_spark` + `_crs_to_srid_proj` + `_zip_vsi`.
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/register.py` — add five sources to `_SOURCES`.
- Modify: `python/geobrix/pyproject.toml` (`[light]`), `requirements-pyrx-ci.in/.txt`, `requirements-dev-container.in/.txt` — add `pyogrio` + `pyproj`.
- Modify: `python/geobrix/test/pyrx/test_serverless_no_spark_config.py` — add `vector.py` to the coverage list.
- Create tests: `python/geobrix/test/ds/test_vector_schema.py`, `test_vector_reader.py`, `test_vector_named.py`, `test_vector_parity.py`.
- Modify: `bench/readers.py` (+ `run_format_read` already exists), `bench/cluster.py` (+ `_CELL_VECTOR`), launcher (`--benchmark-vector`); `docs/docs/api/benchmarking.mdx`.
- Modify docs: `docs/docs/readers/{ogr,shapefile,geojson,geopackage,filegdb}.mdx` (add lightweight tab, drop note) + new `docs/tests/python/readers/*_gbx_examples.py`.

---

### Task 1: Add `pyogrio` + `pyproj` to the light deps

**Files:** `python/geobrix/pyproject.toml`, `requirements-pyrx-ci.in`, `requirements-dev-container.in` (+ regenerate the `.txt` locks)

- [ ] **Step 1: Add to the `[light]` extra** in `python/geobrix/pyproject.toml`, after the `quadbin`/`pmtiles` lines:

```toml
    "quadbin>=0.2,<0.3",
    "pmtiles>=3.4,<4",
    # OGR-free vector reading for the light *_gbx vector DataSources. pyogrio
    # bundles its own libgdal; pyproj (pulled by pyogrio) maps CRS->srid/proj4.
    "pyogrio>=0.8,<1",
    "pyproj>=3.6",
```

- [ ] **Step 2: Add to both lock sources.** In `python/geobrix/requirements-pyrx-ci.in` and `python/geobrix/requirements-dev-container.in`, add (pin a current version, e.g. `0.12.1` / `3.7.2` — use whatever resolves) near the geospatial stack:

```
pyogrio==0.12.1
pyproj==3.7.2
```

- [ ] **Step 3: Regenerate the hash-pinned locks** in the dev container (PyPI is firewalled; use the Databricks proxy). Run via the dev container (`bash scripts/commands/gbx-docker-start.sh` if not running):

```bash
bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix/python/geobrix && \
  uv pip compile --generate-hashes --python-version 3.12 \
  --index-url https://pypi-proxy.dev.databricks.com/simple \
  --output-file requirements-pyrx-ci.txt requirements-pyrx-ci.in && \
  uv pip compile --generate-hashes --python-version 3.12 \
  --index-url https://pypi-proxy.dev.databricks.com/simple \
  --output-file requirements-dev-container.txt requirements-dev-container.in"
```

- [ ] **Step 4: Install into the repo venv + verify import**

```bash
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -m pip install --require-hashes -r python/geobrix/requirements-pyrx-ci.txt >/dev/null 2>&1 || \
  /Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/pip install "pyogrio>=0.8,<1" "pyproj>=3.6"
/Users/mjohns/IdeaProjects/geobrix/.venv-pyrx/bin/python -c "import pyogrio, pyproj; print('pyogrio', pyogrio.__version__, 'pyproj', pyproj.__version__)"
```
Expected: prints versions.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/pyproject.toml python/geobrix/requirements-pyrx-ci.in python/geobrix/requirements-pyrx-ci.txt python/geobrix/requirements-dev-container.in python/geobrix/requirements-dev-container.txt
git commit -m "build(light): add pyogrio + pyproj for vector readers

Co-authored-by: Isaac"
```

---

### Task 2: Schema builder + type map (pure, unit-tested)

**Files:** Create `python/geobrix/src/databricks/labs/gbx/ds/vector.py` (schema parts only); Test `python/geobrix/test/ds/test_vector_schema.py`

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/ds/test_vector_schema.py
from pyspark.sql.types import (
    BinaryType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructType,
)
from databricks.labs.gbx.ds.vector import _ogr_to_spark, _vector_schema, _crs_to_srid_proj


def test_ogr_to_spark_map():
    assert isinstance(_ogr_to_spark("OFTString", "OFSTNone"), StringType)
    assert isinstance(_ogr_to_spark("OFTInteger", "OFSTNone"), IntegerType)
    assert isinstance(_ogr_to_spark("OFTInteger", "OFSTBoolean"), BooleanType)
    assert isinstance(_ogr_to_spark("OFTInteger64", "OFSTNone"), LongType)
    assert isinstance(_ogr_to_spark("OFTReal", "OFSTNone"), DoubleType)
    assert isinstance(_ogr_to_spark("OFTUnknownFuture", "OFSTNone"), StringType)  # default


def test_vector_schema_matches_heavy_layout():
    info = {
        "fields": ["name", "pop", "area"],
        "ogr_types": ["OFTString", "OFTInteger", "OFTReal"],
        "ogr_subtypes": ["OFSTNone", "OFSTNone", "OFSTNone"],
        "geometry_name": "",
    }
    schema = _vector_schema(info, as_wkb=True)
    names = [f.name for f in schema.fields]
    # attributes first, then geom_0 + srid + proj
    assert names == ["name", "pop", "area", "geom_0", "geom_0_srid", "geom_0_srid_proj"]
    by = {f.name: f for f in schema.fields}
    assert isinstance(by["pop"].dataType, IntegerType)
    assert isinstance(by["area"].dataType, DoubleType)
    assert isinstance(by["geom_0"].dataType, BinaryType)
    assert isinstance(by["geom_0_srid"].dataType, StringType)
    assert all(f.nullable for f in schema.fields)


def test_vector_schema_wkt_is_string():
    info = {"fields": [], "ogr_types": [], "ogr_subtypes": [], "geometry_name": ""}
    schema = _vector_schema(info, as_wkb=False)
    assert isinstance({f.name: f for f in schema.fields}["geom_0"].dataType, StringType)


def test_crs_to_srid_proj():
    srid, proj4 = _crs_to_srid_proj("EPSG:4326")
    assert srid == "4326"
    assert "+proj=longlat" in proj4
    assert _crs_to_srid_proj(None) == ("0", "")
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/test_vector_schema.py -v -p no:cacheprovider`
Expected: FAIL (ModuleNotFoundError `...ds.vector`).

- [ ] **Step 3: Create `vector.py` with the schema parts**

```python
# python/geobrix/src/databricks/labs/gbx/ds/vector.py
"""Light vector readers (*_gbx) — pyogrio-backed PySpark DataSource V2, emitting
the same schema as the heavyweight Scala OGR readers (geom_j WKB + srid + proj4 +
typed attributes). Pure-Python / Serverless-safe (no JVM)."""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# OGR field type (+ subtype) -> Spark type, matching heavy OGR_SchemaInference.getType.
_OGR_TO_SPARK = {
    "OFTInteger": IntegerType,
    "OFTInteger64": LongType,
    "OFTReal": DoubleType,
    "OFTString": StringType,
    "OFTWideString": StringType,
    "OFTDate": DateType,
    "OFTTime": TimestampType,
    "OFTDateTime": TimestampType,
    "OFTBinary": BinaryType,
}
_OGR_LIST_TO_SPARK = {
    "OFTIntegerList": IntegerType,
    "OFTRealList": DoubleType,
    "OFTStringList": StringType,
    "OFTWideStringList": StringType,
}


def _ogr_to_spark(ogr_type: str, subtype: str):
    if subtype == "OFSTBoolean":
        return BooleanType()
    if ogr_type in _OGR_LIST_TO_SPARK:
        return ArrayType(_OGR_LIST_TO_SPARK[ogr_type]())
    return _OGR_TO_SPARK.get(ogr_type, StringType)()


def _geom_name(info: Dict) -> str:
    # Heavy uses the OGR geom field name if present, else geom_0 (single-geom v1).
    return info.get("geometry_name") or "geom_0"


def _vector_schema(info: Dict, as_wkb: bool) -> StructType:
    fields: List[StructField] = []
    names = list(info.get("fields", []))
    ogr_types = list(info.get("ogr_types", []))
    subtypes = list(info.get("ogr_subtypes", []))
    for j, name in enumerate(names):
        col = name if name else f"field_{j}"
        ot = ogr_types[j] if j < len(ogr_types) else "OFTString"
        st = subtypes[j] if j < len(subtypes) else "OFSTNone"
        fields.append(StructField(col, _ogr_to_spark(ot, st), True))
    gname = _geom_name(info)
    geom_type = BinaryType() if as_wkb else StringType()
    fields.append(StructField(gname, geom_type, True))
    fields.append(StructField(gname + "_srid", StringType(), True))
    fields.append(StructField(gname + "_srid_proj", StringType(), True))
    return StructType(fields)


def _crs_to_srid_proj(crs) -> Tuple[str, str]:
    """(authority code string e.g. '4326' or '0', PROJ4 string or '')."""
    if not crs:
        return "0", ""
    try:
        from pyproj import CRS

        c = CRS.from_user_input(crs)
        auth = c.to_authority()
        srid = auth[1] if auth else "0"
        try:
            proj4 = c.to_proj4() or ""
        except Exception:
            proj4 = ""
        return srid, proj4
    except Exception:
        return "0", ""
```

- [ ] **Step 4: Run to verify pass**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/test_vector_schema.py -v -p no:cacheprovider`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_vector_schema.py
git commit -m "feat(ds): vector schema builder + OGR->Spark type map (heavy parity)

Co-authored-by: Isaac"
```

---

### Task 3: Generic `ogr_gbx` reader + DataSource (+ register)

**Files:** Modify `vector.py` (add reader/datasource + zip helper), `register.py`; Test `python/geobrix/test/ds/test_vector_reader.py`

- [ ] **Step 1: Write the failing test** (generates a tiny GeoJSON locally so it needs no Volumes)

```python
# python/geobrix/test/ds/test_vector_reader.py
import json
import os
import tempfile

from shapely import from_wkb
from databricks.labs.gbx.ds.register import register

_GJ = {
    "type": "FeatureCollection",
    "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},
    "features": [
        {"type": "Feature", "properties": {"name": "a", "pop": 10},
         "geometry": {"type": "Point", "coordinates": [-73.9, 40.7]}},
        {"type": "Feature", "properties": {"name": "b", "pop": 20},
         "geometry": {"type": "Point", "coordinates": [-0.1, 51.5]}},
    ],
}


def _gj_path(tmp):
    p = os.path.join(tmp, "pts.geojson")
    with open(p, "w") as f:
        json.dump(_GJ, f)
    return p


def test_ogr_gbx_reads_wkb_schema(spark, tmp_path):
    register(spark)
    p = _gj_path(str(tmp_path))
    df = spark.read.format("ogr_gbx").load(p)
    assert df.columns == ["name", "pop", "geom_0", "geom_0_srid", "geom_0_srid_proj"]
    rows = df.orderBy("name").collect()
    assert rows[0]["name"] == "a" and rows[0]["pop"] == 10
    assert rows[0]["geom_0_srid"] == "4326"
    # geom_0 is valid WKB
    assert from_wkb(bytes(rows[0]["geom_0"])).geom_type == "Point"
    assert df.count() == 2


def test_ogr_gbx_wkt_option(spark, tmp_path):
    register(spark)
    p = _gj_path(str(tmp_path))
    df = spark.read.format("ogr_gbx").option("asWKB", "false").load(p)
    g = df.orderBy("name").collect()[0]["geom_0"]
    assert isinstance(g, str) and g.upper().startswith("POINT")


def test_ogr_gbx_chunksize_partitions(spark, tmp_path):
    register(spark)
    p = _gj_path(str(tmp_path))
    df = spark.read.format("ogr_gbx").option("chunkSize", "1").load(p)
    assert df.rdd.getNumPartitions() >= 2  # 2 features / chunk 1
    assert df.count() == 2
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/test_vector_reader.py -v -p no:cacheprovider`
Expected: FAIL (`ogr_gbx` not registered).

- [ ] **Step 3: Add the reader + DataSource + zip helper to `vector.py`** (append):

```python
def _zip_vsi(path: str) -> str:
    """Map a zipped vector source to a GDAL /vsizip/ path (so OGR reads it in place)."""
    low = path.lower()
    if low.endswith(".zip"):
        return "/vsizip/" + path
    return path


class _ChunkPartition(InputPartition):
    """One contiguous feature slice of one layer (picklable)."""

    def __init__(self, path, driver, layer, as_wkb, skip, count, field_names):
        self.path = path
        self.driver = driver
        self.layer = layer
        self.as_wkb = as_wkb
        self.skip = skip
        self.count = count
        self.field_names = field_names


class OgrGbxReader(DataSourceReader):
    _DRIVER = ""  # named subclasses override

    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("ogr_gbx requires a 'path' (e.g. .load(path)).")
        self.driver = options.get("driverName", "") or self._DRIVER
        self.as_wkb = options.get("asWKB", "true").lower() != "false"
        self.chunk_size = int(options.get("chunkSize", "10000"))
        self.layer_number = int(options.get("layerNumber", "0"))
        self.layer_name = options.get("layerName", "")

    def _layer(self):
        return self.layer_name if self.layer_name else self.layer_number

    def _info(self):
        import pyogrio

        kw = {"layer": self._layer()}
        if self.driver:
            kw["driver"] = self.driver
        return pyogrio.read_info(_zip_vsi(self.path), **kw)

    def schema(self) -> StructType:
        return _vector_schema(self._info(), self.as_wkb)

    def partitions(self) -> Sequence[InputPartition]:
        info = self._info()
        n = int(info.get("features", 0) or 0)
        names = list(info.get("fields", []))
        names = [nm if nm else f"field_{j}" for j, nm in enumerate(names)]
        chunk = max(1, self.chunk_size)
        parts = []
        skip = 0
        # at least one partition even for empty/unknown-count sources
        while skip < n or (n == 0 and skip == 0):
            parts.append(
                _ChunkPartition(self.path, self.driver, self._layer(),
                                self.as_wkb, skip, chunk, names)
            )
            skip += chunk
            if n == 0:
                break
        return parts

    def read(self, partition: "_ChunkPartition"):
        import pyogrio

        kw = {
            "layer": partition.layer,
            "skip_features": partition.skip,
            "max_features": partition.count,
            "read_geometry": True,
            "datetime_as_string": False,
        }
        if partition.driver:
            kw["driver"] = partition.driver
        meta, tbl = pyogrio.read_arrow(_zip_vsi(partition.path), **kw)
        gcol = meta.get("geometry_name") or "wkb_geometry"
        srid, proj4 = _crs_to_srid_proj(meta.get("crs"))
        attr_cols = [c for c in tbl.column_names if c != gcol]
        # column-wise to python, then row tuples (attrs..., geom, srid, proj)
        cols = {c: tbl.column(c).to_pylist() for c in tbl.column_names}
        geom = cols.get(gcol, [None] * tbl.num_rows)
        for i in range(tbl.num_rows):
            g = geom[i]
            if g is not None and not partition.as_wkb:
                from shapely import from_wkb

                g = from_wkb(bytes(g)).wkt
            elif g is not None:
                g = bytes(g)
            row = tuple(cols[c][i] for c in attr_cols) + (g, srid, proj4)
            yield row


class OgrGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "ogr_gbx"

    _READER = OgrGbxReader

    def schema(self) -> StructType:
        return self._READER(self.options).schema()

    def reader(self, schema: StructType) -> DataSourceReader:
        return self._READER(self.options)
```

NOTE on attribute order: `read_arrow`'s table lists attribute columns in the same order as `read_info["fields"]`, then the geometry column — matching `_vector_schema` (attributes then geom). `attr_cols` (table order minus geom) preserves it. If an attribute name was empty and `_vector_schema` renamed it to `field_<j>`, pyogrio still returns it under its arrow name; for the corpus all attribute fields are named, so this matches — the parity test (Task 6) is the gate for any rename edge.

- [ ] **Step 4: Register `ogr_gbx`** — edit `register.py`:

```python
from databricks.labs.gbx.ds.gtiff import GTiffGbxDataSource
from databricks.labs.gbx.ds.pmtiles import PMTilesGbxDataSource
from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.ds.vector import OgrGbxDataSource

_SOURCES = (
    RasterGbxDataSource,
    GTiffGbxDataSource,
    PMTilesGbxDataSource,
    OgrGbxDataSource,
)
```

- [ ] **Step 5: Run to verify pass**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/test_vector_reader.py -v -p no:cacheprovider`
Expected: 3 passed. (If `chunkSize` partition count assertion is environment-flaky, confirm `df.count()==2` and ≥1 partition; tune the partition test to assert the union, not an exact count, if Spark coalesces.)

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/src/databricks/labs/gbx/ds/register.py python/geobrix/test/ds/test_vector_reader.py
git commit -m "feat(ds): generic ogr_gbx vector reader (pyogrio, chunked, WKB/WKT)

Co-authored-by: Isaac"
```

---

### Task 4: Named presets (`shapefile_gbx`/`geojson_gbx`/`gpkg_gbx`/`file_gdb_gbx`)

**Files:** Modify `vector.py` (add 4 subclasses), `register.py`; Test `python/geobrix/test/ds/test_vector_named.py`

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/ds/test_vector_named.py
import json
import os

from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.ds import vector as V


def test_named_drivers_preset():
    assert V.ShapefileGbxDataSource.name() == "shapefile_gbx"
    assert V.GeoJSONGbxDataSource.name() == "geojson_gbx"
    assert V.GpkgGbxDataSource.name() == "gpkg_gbx"
    assert V.FileGdbGbxDataSource.name() == "file_gdb_gbx"
    assert V.ShapefileGbxDataSource._READER._DRIVER == "ESRI Shapefile"
    assert V.GeoJSONGbxDataSource._READER._DRIVER == "GeoJSON"
    assert V.GpkgGbxDataSource._READER._DRIVER == "GPKG"
    assert V.FileGdbGbxDataSource._READER._DRIVER == "OpenFileGDB"


def test_geojson_gbx_reads(spark, tmp_path):
    register(spark)
    p = os.path.join(str(tmp_path), "pts.geojson")
    with open(p, "w") as f:
        json.dump({"type": "FeatureCollection",
                   "features": [{"type": "Feature", "properties": {"k": 1},
                                 "geometry": {"type": "Point", "coordinates": [0, 0]}}]}, f)
    df = spark.read.format("geojson_gbx").load(p)
    assert "geom_0" in df.columns and df.count() == 1
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/test_vector_named.py -v -p no:cacheprovider`
Expected: FAIL (`ShapefileGbxDataSource` undefined / `geojson_gbx` not registered).

- [ ] **Step 3: Add the four presets to `vector.py`** (append):

```python
class _ShapefileReader(OgrGbxReader):
    _DRIVER = "ESRI Shapefile"


class _GeoJSONReader(OgrGbxReader):
    _DRIVER = "GeoJSON"


class _GpkgReader(OgrGbxReader):
    _DRIVER = "GPKG"


class _FileGdbReader(OgrGbxReader):
    _DRIVER = "OpenFileGDB"


class ShapefileGbxDataSource(OgrGbxDataSource):
    _READER = _ShapefileReader

    @classmethod
    def name(cls) -> str:
        return "shapefile_gbx"


class GeoJSONGbxDataSource(OgrGbxDataSource):
    _READER = _GeoJSONReader

    @classmethod
    def name(cls) -> str:
        return "geojson_gbx"


class GpkgGbxDataSource(OgrGbxDataSource):
    _READER = _GpkgReader

    @classmethod
    def name(cls) -> str:
        return "gpkg_gbx"


class FileGdbGbxDataSource(OgrGbxDataSource):
    _READER = _FileGdbReader

    @classmethod
    def name(cls) -> str:
        return "file_gdb_gbx"
```

- [ ] **Step 4: Register the four** — extend `_SOURCES` in `register.py`:

```python
from databricks.labs.gbx.ds.vector import (
    FileGdbGbxDataSource,
    GeoJSONGbxDataSource,
    GpkgGbxDataSource,
    OgrGbxDataSource,
    ShapefileGbxDataSource,
)

_SOURCES = (
    RasterGbxDataSource,
    GTiffGbxDataSource,
    PMTilesGbxDataSource,
    OgrGbxDataSource,
    ShapefileGbxDataSource,
    GeoJSONGbxDataSource,
    GpkgGbxDataSource,
    FileGdbGbxDataSource,
)
```

- [ ] **Step 5: Run to verify pass**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/test_vector_named.py -v -p no:cacheprovider`
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/src/databricks/labs/gbx/ds/register.py python/geobrix/test/ds/test_vector_named.py
git commit -m "feat(ds): named vector readers (shapefile/geojson/gpkg/file_gdb _gbx)

Co-authored-by: Isaac"
```

---

### Task 5: Serverless guard coverage

**Files:** Modify `python/geobrix/test/pyrx/test_serverless_no_spark_config.py`

- [ ] **Step 1: Add `vector.py` to the coverage assertion** — in `test_serverless_scan_includes_ds_modules`, append `"vector.py"` to the required tuple (after `"shard.py"`).

- [ ] **Step 2: Run the guard + the full vector + ds suite**

```bash
.venv-pyrx/bin/python -m pytest python/geobrix/test/pyrx/test_serverless_no_spark_config.py python/geobrix/test/ds -q -p no:cacheprovider
```
Expected: PASS (the guard's forbidden-pattern scan over `vector.py` must pass — confirms no `_jvm`/`.conf.set`/`.rdd`; pyogrio/pyproj/shapely imports are inside `read()`/`_info()`).

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/pyrx/test_serverless_no_spark_config.py
git commit -m "test(ds): cover vector.py in the Serverless guard scan

Co-authored-by: Isaac"
```

---

### Task 6: Light-vs-heavy parity test (Docker / integration, skip-if-heavy)

**Files:** Create `python/geobrix/test/ds/test_vector_parity.py`

- [ ] **Step 1: Write the parity test** (mirrors `test/ds/test_writer_parity.py`'s `spark_with_jar` skip pattern; reuses the corpus)

```python
# python/geobrix/test/ds/test_vector_parity.py
"""Light vs heavy vector reader parity (Docker / integration).

Same source -> light *_gbx vs heavy *_ogr produce the same schema (columns +
types), row count, and decoded geometries. Skips unless the geobrix JAR is staged
+ sample data is mounted."""

import logging
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
_JARS = sorted((_HERE.parents[3] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))
SAMPLE = os.environ.get(
    "GBX_SAMPLE_DATA_ROOT",
    "/Volumes/main/default/test-data",
).rstrip("/") + "/geobrix-examples"

_CASES = [
    ("geojson_gbx", "geojson_ogr", f"{SAMPLE}/nyc/boroughs/nyc_boroughs.geojson"),
    ("shapefile_gbx", "shapefile_ogr", f"{SAMPLE}/nyc/subway/nyc_subway.shp.zip"),
    ("gpkg_gbx", "gpkg_ogr", f"{SAMPLE}/nyc/geopackage/nyc_complete.gpkg"),
    ("file_gdb_gbx", "file_gdb_ogr", f"{SAMPLE}/nyc/filegdb/NYC_Sample.gdb.zip"),
]


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip("no geobrix JAR staged")
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    s = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-ds-vector-parity")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    from databricks.labs.gbx.ds.register import register

    register(s)
    yield s


@pytest.mark.parametrize("light_fmt,heavy_fmt,path", _CASES)
def test_vector_reader_parity(spark_with_jar, light_fmt, heavy_fmt, path):
    if not os.path.exists(path):
        pytest.skip(f"sample not mounted: {path}")
    spark = spark_with_jar
    light = spark.read.format(light_fmt).load(path)
    heavy = spark.read.format(heavy_fmt).load(path)
    # same schema (names + types)
    assert [(f.name, f.dataType.simpleString()) for f in light.schema.fields] == [
        (f.name, f.dataType.simpleString()) for f in heavy.schema.fields
    ]
    assert light.count() == heavy.count()
    # decoded geometry sets match (compare WKB geom_0 bytes)
    lg = {bytes(r["geom_0"]) for r in light.select("geom_0").collect()}
    hg = {bytes(r["geom_0"]) for r in heavy.select("geom_0").collect()}
    assert lg == hg
```

- [ ] **Step 2: Run locally (skips without JAR)**

Run: `.venv-pyrx/bin/python -m pytest python/geobrix/test/ds/test_vector_parity.py -v -p no:cacheprovider --no-header -rs`
Expected: skipped (no JAR locally) — NOT errored. Runs/asserts in Docker/cluster.

- [ ] **Step 3: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/ds/test_vector_parity.py
git commit -m "test(ds): light-vs-heavy vector reader parity (skip-if-heavy)

Co-authored-by: Isaac"
```

---

### Task 7: Bench — `--benchmark-vector` (light `*_gbx` vs heavy `*_ogr` + parity)

> **Phasing note:** this is the **initial** vector benchmark — limited to the
> sample-data formats already on the Volume. The **scaled/final** benchmark (varied
> formats + larger feature counts) comes in a later phase once the **light vector
> writers** exist to *generate* that corpus. Wire the harness + numbers now; the
> bigger run is a follow-on.

**Files:** Modify `bench/cluster.py` (PREAMBLE flags + `_CELL_VECTOR` + `build_bench_notebook`), `notebooks/tests/push_and_run_bench_on_cluster.py`, `scripts/commands/gbx-bench-cluster.sh`; Test `python/geobrix/test/bench/test_cluster_vector.py`

This mirrors the existing `--benchmark-readers` / `--benchmark-pmtiles` wiring exactly (read those first).

- [ ] **Step 1: Add PREAMBLE flags** in `cluster.py` next to `BENCHMARK_PMTILES`/`PMTILES_ONLY`:

```
BENCHMARK_VECTOR = {benchmark_vector!r}
VECTOR_ONLY = {vector_only!r}
```

- [ ] **Step 2: Add `_CELL_VECTOR`** in `cluster.py`, modeled on `_CELL_READERS`. For each light/heavy vector pair, time `run_format_read` and assert row-count + geom parity:

```python
_CELL_VECTOR = """# Vector reader benchmark: light *_gbx vs heavy *_ogr (+ parity)
from databricks.labs.gbx.bench import readers as _rd
_vbase = f"{CORPUS}/vector"  # operator stages the vector corpus here
_vcases = [
    ("geojson_gbx", "geojson_ogr", _vbase + "/nyc_boroughs.geojson"),
    ("shapefile_gbx", "shapefile_ogr", _vbase + "/nyc_subway.shp.zip"),
    ("gpkg_gbx", "gpkg_ogr", _vbase + "/nyc_complete.gpkg"),
    ("file_gdb_gbx", "file_gdb_ogr", _vbase + "/NYC_Sample.gdb.zip"),
]
_vrows = []
for _lfmt, _hfmt, _vp in _vcases:
    if LIGHTWEIGHT:
        _r = _rd.run_format_read(spark, _vp, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                 api="lightweight", fmt=_lfmt, where="cluster")
        _sink([_r]); lw.append(_r); _vrows.append(_r)
    if HEAVYWEIGHT:
        _r = _rd.run_format_read(spark, _vp, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                 api="heavyweight", fmt=_hfmt, where="cluster")
        _sink([_r]); hw.append(_r); _vrows.append(_r)
    if LIGHTWEIGHT and HEAVYWEIGHT:
        _lc = spark.read.format(_lfmt).load(_vp).count()
        _hc = spark.read.format(_hfmt).load(_vp).count()
        print(f"VECTOR PARITY {_lfmt}: light={_lc} heavy={_hc} {'PASS' if _lc==_hc else 'FAIL'}")
        assert _lc == _hc, f"row-count parity FAIL for {_lfmt}: {_lc} != {_hc}"
if _vrows:
    _md = results.summarize(_vrows)
    _show_md(f"vector reader benchmark -- {RUN_ID}", _md)
"""
```

- [ ] **Step 3: Wire into `build_bench_notebook`** — add `benchmark_vector=bool(cfg.get("benchmark_vector"))` and `vector_only=bool(cfg.get("vector_only"))` to the `_PREAMBLE.format(...)` call; add the locals; gate the fn cells with `and not vector_only`; append `if benchmark_vector or vector_only: cells.append(_cell(_CELL_VECTOR))` (after the pmtiles cell).

- [ ] **Step 4: Wire the launcher** (`push_and_run_bench_on_cluster.py`) mirroring `benchmark_pmtiles`/`pmtiles_only`: argv parse `--benchmark-vector`/`--vector-only`, add to cfg, pass to `_expected_rows`, and `if vector_only: cfg["modes"]="spark-path"`.

- [ ] **Step 5: Document the flags** in `scripts/commands/gbx-bench-cluster.sh` help (it forwards unknown args).

- [ ] **Step 6: Smoke test** (mirror `test/bench/test_cluster_pmtiles.py`): assert `build_bench_notebook` with `benchmark_vector=True` includes a cell containing `run_format_read` + `VECTOR PARITY`, and `vector_only=True` omits the per-function cells. Run:

```bash
.venv-pyrx/bin/python -m pytest python/geobrix/test/bench/test_cluster_vector.py -q -p no:cacheprovider
```
Expected: pass.

- [ ] **Step 7: Add a benchmarking.mdx section** — `docs/docs/api/benchmarking.mdx`, a "Results — vector readers" subsection (placeholder table to be filled from a cluster run, mirroring the PMTiles section's prose: light `*_gbx` vs heavy `*_ogr`, row-count parity, run with `gbx:bench:cluster --benchmark-vector`).

- [ ] **Step 8: Lint + commit**

```bash
bash scripts/commands/gbx-lint-python.sh --check   # fix any findings in changed files
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/bench/cluster.py notebooks/tests/push_and_run_bench_on_cluster.py scripts/commands/gbx-bench-cluster.sh python/geobrix/test/bench/test_cluster_vector.py docs/docs/api/benchmarking.mdx
git commit -m "feat(bench): --benchmark-vector (light *_gbx vs heavy *_ogr + parity)

Co-authored-by: Isaac"
```

---

### Task 8: Docs — add Lightweight tab to the five vector reader pages

**Files:** Modify `docs/docs/readers/{ogr,shapefile,geojson,geopackage,filegdb}.mdx`; Create `docs/tests/python/readers/{ogr,shapefile,geojson,geopackage,filegdb}_gbx_examples.py` (+ matching `test_*` runners).

For EACH of the five pages (they currently have the `:::note No lightweight equivalent yet` admonition + heavy body):

- [ ] **Step 1: Create the lightweight doc-test example.** e.g. `docs/tests/python/readers/geojson_gbx_examples.py`:

```python
"""Executable doc example for the lightweight geojson_gbx reader (Docker)."""

import path_config  # noqa: F401  (sets SAMPLE_DATA_BASE)
from path_config import SAMPLE_DATA_BASE

READ_GEOJSON_GBX = """# Lightweight GeoJSON reader (pyogrio; no JAR)
from databricks.labs.gbx.ds.register import register
register(spark)
df = spark.read.format("geojson_gbx").load(SAMPLE)   # same (geom_0, *_srid, attrs) schema as geojson_ogr
df.show()"""

SAMPLE = f"{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojson"


def read_geojson_gbx(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)
    df = spark.read.format("geojson_gbx").load(SAMPLE)
    assert "geom_0" in df.columns and "geom_0_srid" in df.columns
    assert df.count() > 0
```
(Analogous files for ogr/shapefile/geopackage/filegdb with their corpus paths + format names `ogr_gbx`/`shapefile_gbx`/`gpkg_gbx`/`file_gdb_gbx`; the shapefile/filegdb ones use the `.shp.zip`/`.gdb.zip` paths.) Add a `test_<fmt>_gbx_examples.py` runner per file that calls the verification function (mirror an existing `docs/tests/python/readers/test_*_examples.py`).

- [ ] **Step 2: Restructure each reader page to a tabbed page** — convert the heavy-only page to the same `<Tabs groupId="gbx-tier" queryString="tier">` form used by `readers/raster.mdx`: a **Lightweight tab first** (the `*_gbx` reader, importing the new example via raw-loader + `<CodeFromTest>`), then a **Heavyweight tab** holding the page's current body. **Remove** the `:::note No lightweight equivalent yet` admonition. Keep `sidebar_label` as the format name. Example head for `readers/geojson.mdx`:

```mdx
---
sidebar_position: 5
sidebar_label: GeoJSON
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeFromTest from '@site/src/components/CodeFromTest';
import geojsonExamples from '!!raw-loader!../../tests/python/readers/geojson_examples.py';
import geojsonScala from '!!raw-loader!../../tests/scala/readers/GeoJSONExamples.scala';
import geojsonGbx from '!!raw-loader!../../tests/python/readers/geojson_gbx_examples.py';

# GeoJSON Reader

Read GeoJSON into the shared OGR schema (`geom_0` WKB + `geom_0_srid` +
`geom_0_srid_proj` + attributes). The **lightweight** `geojson_gbx` reader
(pyogrio, JAR-free) and the **heavyweight** `geojson_ogr` reader are
interchangeable — see [Choosing an Execution Tier](../api/execution-tiers#the-one-line-swap).

<Tabs groupId="gbx-tier" queryString="tier">
<TabItem value="lightweight" label="Lightweight · geojson_gbx">

<CodeFromTest code={geojsonGbx} language="python" functionName="READ_GEOJSON_GBX"
  source="docs/tests/python/readers/geojson_gbx_examples.py"
  testFile="docs/tests/python/readers/test_geojson_gbx_examples.py" />

</TabItem>
<TabItem value="heavyweight" label="Heavyweight · geojson_ogr">

<!-- the page's CURRENT body, moved verbatim (drop the no-equivalent note + the old H1) -->

</TabItem>
</Tabs>
```
(Analogous for ogr/shapefile/geopackage/filegdb — tab labels `Lightweight · ogr_gbx`/`shapefile_gbx`/`gpkg_gbx`/`file_gdb_gbx` and `Heavyweight · ogr`/`shapefile_ogr`/`gpkg_ogr`/`file_gdb_ogr`.)

- [ ] **Step 3: Run the reader doc-tests (Docker) + internals-leak**

```bash
gbx:test:python-docs --path readers/ --log vector-gbx-docs.log
grep -rn -iE "wave [0-9]+" docs/docs/ ; echo "exit:$?"
```
Expected: the five new `*_gbx` reader doc-tests pass; internals-leak clean (`exit:1`).

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/docs/readers/ogr.mdx docs/docs/readers/shapefile.mdx docs/docs/readers/geojson.mdx docs/docs/readers/geopackage.mdx docs/docs/readers/filegdb.mdx docs/tests/python/readers/
git commit -m "docs(readers): lightweight tab for the five vector readers

Co-authored-by: Isaac"
```

---

## Final verification (after all tasks)

- [ ] Full light suite: `.venv-pyrx/bin/python -m pytest python/geobrix/test/ds python/geobrix/test/bench/test_cluster_vector.py python/geobrix/test/pyrx/test_serverless_no_spark_config.py -v` — green (vector parity SKIPs locally).
- [ ] Python lint (CI gate): `gbx:lint:python --check` (Docker) — clean.
- [ ] Docs build: Docusaurus build succeeds (no broken links / sidebar ids); the five vector pages render the lightweight tab + dropped the note.
- [ ] Binding parity unaffected: the `*_gbx` vector readers are DataSource formats, not registered functions — `gbx:test:bindings` unchanged.
- [ ] On-cluster (operator): `gbx:bench:cluster --benchmark-vector` runs the 5 light-vs-heavy reader pairs + row-count parity; fill the benchmarking.mdx vector table; the Docker `test_vector_parity.py` confirms schema+geometry parity.

## Self-Review notes (plan vs spec)

- **Spec coverage:** 5 readers → Tasks 3 (generic) + 4 (named); schema parity + type map → Task 2 (gated by Task 6 parity); options (driverName/asWKB/chunkSize/layerNumber/layerName) → Task 3; chunked partitioning + `/vsizip/` → Task 3; pyogrio/pyproj dep → Task 1; Serverless guard → Task 5; docs tabs + drop note → Task 8; per-reader bench + parity → Task 7; light-vs-heavy parity test → Task 6.
- **Naming consistency:** `OgrGbxReader`/`OgrGbxDataSource` + `_DRIVER` preset + `ShapefileGbxDataSource`/`GeoJSONGbxDataSource`/`GpkgGbxDataSource`/`FileGdbGbxDataSource`; format names `ogr_gbx`/`shapefile_gbx`/`geojson_gbx`/`gpkg_gbx`/`file_gdb_gbx`; heavy option names `driverName`/`asWKB`/`chunkSize`/`layerNumber`/`layerName`; `_vector_schema`/`_ogr_to_spark`/`_crs_to_srid_proj`/`_zip_vsi`/`_ChunkPartition` — used identically across tasks.
- **Known edges (gated by the parity test, Task 6), documented:** OGR→Spark type fidelity (int width, date/timestamp, bool subtype) — the map in Task 2 mirrors heavy's `getType`; if a corpus format reveals a mismatch, fix `_OGR_TO_SPARK`. Empty geometry / null geom rows → emit `None`/empty per heavy ("0"/""). Multi-geometry-field is out of scope (single `geom_0`, per spec).
- **Heavy option-name note:** the heavy reader parses `layerNumber` (per `OGR_Batch.scala`); the `ogr.mdx` options table historically said `layerN`. The light reader uses the heavy code's actual names (`layerNumber`/`layerName`); if the docs table is stale it can be corrected in Task 8.
- **Placeholder scan:** the only `<!-- … -->` markers are explicit "move the current page body verbatim" instructions (Task 8) with the exact source named — not placeholders. The benchmarking.mdx vector table is intentionally filled from an operator cluster run (Task 7 step 7 / final verification), as the PMTiles section was.
