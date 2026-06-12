# Light Vector Writers (`*_gbx`, pyogrio) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add net-new pure-Python vector writers (`ogr_gbx` + `shapefile_gbx`, `geojson_gbx`, `gpkg_gbx`, `file_gdb_gbx`) as PySpark DataSource V2 writers backed by pyogrio, so `read(ogr_gbx) → write(<fmt>_gbx)` round-trips and the writers double as the Phase-3 benchmark-corpus generator.

**Architecture:** Two-phase merge mirroring the PMTiles writer (`ds/pmtiles.py`): each executor `write()` serializes its partition's Arrow table (attributes + WKB geometry + srid/proj metadata columns) to one lossless Arrow-IPC fragment under a shared-FS scratch dir; the driver `commit()` reads every fragment, infers `geometry_type` + `crs` once over the combined data, then writes them into a single output file via `pyogrio.write_arrow` (first table plain, the rest with `append=True`). `abort()` cleans scratch + partial output. All writers live in the existing `ds/vector.py`; the named writers reuse the reader subclasses' `_DRIVER`. Pure-Python / Serverless-safe (pyogrio/pyarrow/shapely/pyproj lazy-imported inside methods).

**Tech Stack:** Python 3.12, PySpark DataSource V2 (`pyspark.sql.datasource`), pyogrio 0.12.1 (`write_arrow`/`read_arrow`/`read_info`), pyarrow (`feather`), shapely (WKB↔WKT + geom_type), pyproj (CRS), pytest + local Spark (`local[2]`).

---

## File Structure

- **`python/geobrix/src/databricks/labs/gbx/ds/vector.py`** (MODIFY) — append writer classes/helpers below the existing readers. One file: readers + writers for the vector tier change together and share `_zip_vsi`, the OGR/CRS maps, and the geom-column conventions.
  - `_VectorCommitMessage` (dataclass) — carries one fragment's Arrow-IPC path.
  - `_geometry_type_of(wkb: bytes) -> str` — OGR geometry-type name from a WKB blob (shapely `geom_type`).
  - `_srid_to_crs(srid: str, proj4: str) -> Optional[str]` — inverse of the reader's `_crs_to_spark`; `"4326"`→`"EPSG:4326"`, else proj4, else `None`.
  - `_writer_col_roles(schema) -> (geom_col, srid_col, proj_col, attr_cols)` — derive column roles from the schema (the column `X` paired with `X_srid`).
  - `OgrGbxWriter(DataSourceWriter)` — `write()`/`commit()`/`abort()`.
  - `OgrGbxDataSource.writer(self, schema, overwrite)` — validates schema, builds the writer with the driver from `self._READER._DRIVER` (or `driverName` option). The four named `*GbxDataSource` subclasses inherit `.writer()` unchanged.
- **`python/geobrix/test/ds/test_vector_writer.py`** (CREATE) — unit + local-Spark round-trip / merge / CRS / mode tests.
- **`python/geobrix/test/ds/test_vector_writer_parity.py`** (CREATE) — Docker integration round-trip against the real corpus (`@pytest.mark.integration`).
- **`python/geobrix/test/pyrx/test_serverless_no_spark_config.py`** (MODIFY) — `vector.py` is already in the scanned list (readers); confirm it still passes after the writer additions.
- **`python/geobrix/src/databricks/labs/gbx/bench/readers.py`** (MODIFY) — add `run_vector_write(spark, light_fmt, path, out_dir)` returning `(seconds, roundtrip_ok)`.
- **`python/geobrix/src/databricks/labs/gbx/bench/cluster.py`** (MODIFY) — extend `_CELL_VECTOR` to call the writer timing + round-trip per format.
- **`docs/docs/writers/vector.mdx`** (CREATE) — lightweight-only vector writer page (a `:::note` that heavy has no vector writer).
- **`docs/docs/writers/overview.mdx`** (MODIFY) — add the vector writer to the lightweight Available-Writers table.
- **`docs/tests/python/api/vectorx_functions_*.py`** area / **`docs/tests/python/writers/`** (CREATE doc-test) — a real write→read round-trip exercised by `gbx:test:python-docs`.
- **`scripts/commands/gbx-data-generate-vector-corpus.{md,sh}`** (CREATE) — corpus-generator command wrapping the writers (Phase-3 enabler).

---

## Task 1: Writer helpers (pure functions, no Spark)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/vector.py` (append after line 98, near `_zip_vsi`)
- Test: `python/geobrix/test/ds/test_vector_writer.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/ds/test_vector_writer.py
from shapely import Point, LineString, to_wkb

from databricks.labs.gbx.ds.vector import (
    _geometry_type_of,
    _srid_to_crs,
    _writer_col_roles,
)
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def test_geometry_type_of_point_and_line():
    assert _geometry_type_of(to_wkb(Point(1, 2))) == "Point"
    assert _geometry_type_of(to_wkb(LineString([(0, 0), (1, 1)]))) == "LineString"


def test_srid_to_crs():
    assert _srid_to_crs("4326", "") == "EPSG:4326"
    assert _srid_to_crs("0", "+proj=longlat +datum=WGS84 +no_defs") == (
        "+proj=longlat +datum=WGS84 +no_defs"
    )
    assert _srid_to_crs("0", "") is None
    assert _srid_to_crs("", "") is None


def test_writer_col_roles_named_geom():
    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("pop", IntegerType()),
            StructField("SHAPE", BinaryType()),
            StructField("SHAPE_srid", StringType()),
            StructField("SHAPE_srid_proj", StringType()),
        ]
    )
    geom, srid, proj, attrs = _writer_col_roles(schema)
    assert (geom, srid, proj) == ("SHAPE", "SHAPE_srid", "SHAPE_srid_proj")
    assert attrs == ["name", "pop"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_writer.py -v`
Expected: FAIL — `ImportError: cannot import name '_geometry_type_of'`.

- [ ] **Step 3: Write minimal implementation**

Append to `python/geobrix/src/databricks/labs/gbx/ds/vector.py` (after `_zip_vsi`, before `class _ChunkPartition`):

```python
def _geometry_type_of(wkb: bytes) -> str:
    """OGR geometry-type name (e.g. 'Point', 'MultiPolygon') from a WKB blob."""
    from shapely import from_wkb

    return from_wkb(bytes(wkb)).geom_type


def _srid_to_crs(srid: str, proj4: str):
    """Inverse of the reader's CRS encoding: authority code -> 'EPSG:<code>',
    else the PROJ4 string, else None (CRS-less)."""
    if srid and srid != "0":
        return f"EPSG:{srid}"
    if proj4:
        return proj4
    return None


def _writer_col_roles(schema):
    """(geom_col, srid_col, proj_col, attr_cols) derived from the reader schema:
    the column X paired with X_srid is the geometry; X_srid_proj is its proj4;
    everything else is an attribute. Mirrors how the parity test finds geom."""
    names = [f.name for f in schema.fields]
    srid_cols = [n for n in names if n.endswith("_srid")]
    if not srid_cols:
        raise ValueError(
            "vector writer input needs a geometry/'*_srid' column pair "
            f"(from a *_gbx reader); got columns {names}"
        )
    srid_col = srid_cols[0]
    geom_col = srid_col[: -len("_srid")]
    proj_col = geom_col + "_srid_proj"
    if geom_col not in names:
        raise ValueError(f"no geometry column '{geom_col}' for srid '{srid_col}'")
    attr_cols = [n for n in names if n not in (geom_col, srid_col, proj_col)]
    return geom_col, srid_col, proj_col, attr_cols
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_writer.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_vector_writer.py
git commit -m "feat(ds): vector writer helpers (geom-type, crs, col roles)

Pure-function building blocks for the light vector writers: OGR geometry-type
name from WKB, srid/proj4 -> pyogrio crs, and schema column-role derivation
(the column paired with *_srid is the geometry).

Co-authored-by: Isaac"
```

---

## Task 2: `OgrGbxWriter` + `OgrGbxDataSource.writer()` — generic GeoJSON round-trip (single partition)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/vector.py` (append writer class + `.writer()` on `OgrGbxDataSource`)
- Test: `python/geobrix/test/ds/test_vector_writer.py`

- [ ] **Step 1: Write the failing test**

Append to `test/ds/test_vector_writer.py`:

```python
from shapely import from_wkb as _from_wkb

from databricks.labs.gbx.ds.register import register


def _wkb_df(spark):
    rows = [
        ("a", 10, bytearray(to_wkb(Point(-73.9, 40.7))), "4326", ""),
        ("b", 20, bytearray(to_wkb(Point(-0.1, 51.5))), "4326", ""),
    ]
    return spark.createDataFrame(
        rows, schema="name string, pop int, geom_0 binary, "
        "geom_0_srid string, geom_0_srid_proj string"
    )


def test_geojson_roundtrip_single_partition(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "out.geojson")
    _wkb_df(spark).coalesce(1).write.format("ogr_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)

    back = spark.read.format("ogr_gbx").load(out)
    assert back.count() == 2
    got = {r["name"]: r["pop"] for r in back.collect()}
    assert got == {"a": 10, "b": 20}
    # geometry survives (derive geom col from schema, like parity tests)
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    geoms = {_from_wkb(bytes(r[gcol])).geom_type for r in back.select(gcol).collect()}
    assert geoms == {"Point"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_writer.py::test_geojson_roundtrip_single_partition -v`
Expected: FAIL — `pyspark...` error that `ogr_gbx` has no writer (`writer() not implemented` / unsupported save).

- [ ] **Step 3: Write minimal implementation**

Add imports at the top of `vector.py` (extend the existing import lines):

```python
import os
import shutil
import uuid
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceWriter,
    InputPartition,
    WriterCommitMessage,
)
```

Append to `vector.py` (after the named reader subclasses):

```python
@dataclass
class _VectorCommitMessage(WriterCommitMessage):
    frag_path: str


class OgrGbxWriter(DataSourceWriter):
    """Two-phase vector writer: each partition -> one Arrow-IPC fragment in a
    shared-FS scratch dir; the driver merges fragments into one output file via
    pyogrio.write_arrow (first plain, rest append=True). Mirrors the PMTiles
    writer's executor-scratch / driver-merge shape."""

    def __init__(self, path, schema, driver, options, overwrite):
        opts = {k.lower(): v for k, v in options.items()}
        self.path = path
        self.driver = options.get("driverName", "") or driver
        if not self.driver:
            raise ValueError(
                "ogr_gbx writer requires a 'driverName' option (e.g. 'GeoJSON')."
            )
        self.overwrite = overwrite
        self.geometry_type_override = opts.get("geometrytype")
        self.layer_name = opts.get("layername")
        self.geom_col, self.srid_col, self.proj_col, self.attr_cols = (
            _writer_col_roles(schema)
        )
        self._col_order = [f.name for f in schema.fields]
        self._geom_is_wkb = any(
            f.name == self.geom_col and isinstance(f.dataType, BinaryType)
            for f in schema.fields
        )
        parent = os.path.dirname(self.path) or "."
        self.scratch_dir = os.path.join(parent, "_vec_scratch")
        if not self.overwrite and self._target_exists():
            raise ValueError(
                "ogr_gbx does not support append; use .mode('overwrite')."
            )

    def _target_exists(self) -> bool:
        return os.path.exists(self.path) and (
            os.path.isfile(self.path) or bool(os.listdir(self.path))
        )

    # ---- executor: partition rows -> one Arrow-IPC fragment ----
    def write(self, iterator: Iterator) -> WriterCommitMessage:
        import pyarrow as pa
        import pyarrow.feather as feather
        from shapely import from_wkt, to_wkb

        idx = {n: i for i, n in enumerate(self._col_order)}
        cols: Dict[str, list] = {n: [] for n in self._col_order}
        for row in iterator:
            for n in self._col_order:
                v = row[idx[n]]
                if n == self.geom_col and v is not None and not self._geom_is_wkb:
                    v = to_wkb(from_wkt(v))  # WKT input -> WKB
                elif n == self.geom_col and v is not None:
                    v = bytes(v)
                cols[n].append(v)
        if not cols[self.geom_col]:
            return _VectorCommitMessage(frag_path="")  # empty partition
        os.makedirs(self.scratch_dir, exist_ok=True)
        tbl = pa.table({n: cols[n] for n in self._col_order})
        frag = os.path.join(self.scratch_dir, f"frag-{uuid.uuid4().hex}.arrow")
        feather.write_feather(tbl, frag)
        return _VectorCommitMessage(frag_path=frag)

    # ---- driver: merge fragments into one output file ----
    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        import pyarrow.feather as feather
        import pyogrio

        frags = [
            m.frag_path
            for m in messages
            if isinstance(m, _VectorCommitMessage) and m.frag_path
        ]
        try:
            if not frags:
                return
            self._prepare_target()
            tables = [feather.read_table(f) for f in frags]
            geom_type, crs = self._infer_geom_crs(tables)
            kw = dict(
                driver=self.driver,
                geometry_name=self.geom_col,
                geometry_type=geom_type,
                crs=crs,
            )
            if self.layer_name:
                kw["layer"] = self.layer_name
            for n, tbl in enumerate(tables):
                out_tbl = tbl.drop_columns(
                    [c for c in (self.srid_col, self.proj_col) if c in tbl.column_names]
                )
                pyogrio.write_arrow(out_tbl, self.path, append=(n > 0), **kw)
        finally:
            shutil.rmtree(self.scratch_dir, ignore_errors=True)

    def _infer_geom_crs(self, tables) -> Tuple[str, Optional[str]]:
        geom_type, crs = self.geometry_type_override, None
        for tbl in tables:
            g = tbl.column(self.geom_col).to_pylist()
            s = tbl.column(self.srid_col).to_pylist() if self.srid_col in tbl.column_names else []
            p = tbl.column(self.proj_col).to_pylist() if self.proj_col in tbl.column_names else []
            for i, gv in enumerate(g):
                if gv is None:
                    continue
                if geom_type is None:
                    geom_type = _geometry_type_of(gv)
                if crs is None:
                    crs = _srid_to_crs(
                        s[i] if i < len(s) else "", p[i] if i < len(p) else ""
                    )
                break
            if geom_type is not None and crs is not None:
                break
        return geom_type or "Unknown", crs

    def _prepare_target(self) -> None:
        # PySpark may pre-create self.path as a directory; vector output is a
        # single file (or driver-managed dir). Clear it and write directly —
        # no os.rename (FUSE-unsafe on DBFS/Volumes); write_arrow writes
        # sequentially so a direct write to a FUSE path is safe.
        parent = os.path.dirname(self.path) or "."
        os.makedirs(parent, exist_ok=True)
        if os.path.isdir(self.path):
            shutil.rmtree(self.path)
        elif os.path.isfile(self.path):
            os.remove(self.path)

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        shutil.rmtree(self.scratch_dir, ignore_errors=True)
        if os.path.isfile(self.path):
            os.remove(self.path)
        elif os.path.isdir(self.path):
            shutil.rmtree(self.path, ignore_errors=True)
```

Add `.writer()` to `OgrGbxDataSource` (insert after its `reader()` method):

```python
    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        path = self.options.get("path")
        if not path:
            raise ValueError("ogr_gbx writer requires an output path (.save(path)).")
        return OgrGbxWriter(
            path, schema, self._READER._DRIVER, dict(self.options), overwrite
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_writer.py::test_geojson_roundtrip_single_partition -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_vector_writer.py
git commit -m "feat(ds): OgrGbxWriter two-phase vector writer (GeoJSON round-trip)

Executor writes each partition to an Arrow-IPC scratch fragment; driver merges
into one file via pyogrio.write_arrow (append for fragments 2..n), inferring
geometry_type + crs from the data and dropping the srid/proj metadata columns.

Co-authored-by: Isaac"
```

---

## Task 3: Multi-partition merge (no lost/duplicated rows)

**Files:**
- Test: `python/geobrix/test/ds/test_vector_writer.py`

- [ ] **Step 1: Write the failing test**

Append to `test/ds/test_vector_writer.py`:

```python
def test_multi_partition_merge(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "multi.geojson")
    rows = [
        (str(i), i, bytearray(to_wkb(Point(float(i) / 10.0, 40.0))), "4326", "")
        for i in range(50)
    ]
    df = spark.createDataFrame(
        rows,
        schema="name string, pop int, geom_0 binary, "
        "geom_0_srid string, geom_0_srid_proj string",
    ).repartition(4)
    assert df.rdd.getNumPartitions() == 4
    df.write.format("ogr_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)
    back = spark.read.format("ogr_gbx").load(out)
    assert back.count() == 50
    assert {r["name"] for r in back.collect()} == {str(i) for i in range(50)}
```

- [ ] **Step 2: Run test to verify it fails or passes**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_writer.py::test_multi_partition_merge -v`
Expected: PASS if the Task-2 merge is correct. If it FAILS (e.g. only the last partition's rows present), the append loop in `commit()` is the bug — fix so every fragment is appended.

- [ ] **Step 3: Fix if needed**

If the test fails, confirm `commit()` iterates ALL fragments with `append=(n > 0)` and that `write()` returns a fragment per non-empty partition. No new code expected if Task 2 is correct.

- [ ] **Step 4: Re-run to confirm PASS**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_writer.py -v`
Expected: PASS (all writer tests).

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/ds/test_vector_writer.py
git commit -m "test(ds): vector writer multi-partition merge keeps all rows

Co-authored-by: Isaac"
```

---

## Task 4: CRS + geometry_type inference + override, and `append` rejection

**Files:**
- Test: `python/geobrix/test/ds/test_vector_writer.py`

- [ ] **Step 1: Write the failing test**

Append to `test/ds/test_vector_writer.py`:

```python
import pytest
from shapely import Polygon


def test_crs_roundtrips(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "crs.geojson")
    _wkb_df(spark).coalesce(1).write.format("ogr_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)
    back = spark.read.format("ogr_gbx").load(out)
    scol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0]
    assert {r[scol] for r in back.select(scol).collect()} == {"4326"}


def test_geometry_type_override(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "poly.geojson")
    poly = to_wkb(Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]))
    df = spark.createDataFrame(
        [("p", bytearray(poly), "4326", "")],
        schema="name string, geom_0 binary, geom_0_srid string, "
        "geom_0_srid_proj string",
    )
    df.coalesce(1).write.format("ogr_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).option("geometryType", "Polygon").save(out)
    back = spark.read.format("ogr_gbx").load(out)
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    assert _from_wkb(bytes(back.collect()[0][gcol])).geom_type == "Polygon"


def test_append_mode_rejected(spark, tmp_path):
    register(spark)
    out = str(tmp_path / "exists.geojson")
    _wkb_df(spark).coalesce(1).write.format("ogr_gbx").mode("overwrite").option(
        "driverName", "GeoJSON"
    ).save(out)
    with pytest.raises(Exception) as ei:
        _wkb_df(spark).write.format("ogr_gbx").mode("append").option(
            "driverName", "GeoJSON"
        ).save(out)
    assert "append" in str(ei.value).lower()
```

- [ ] **Step 2: Run test to verify it fails or passes**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_writer.py -k "crs or geometry_type or append_mode" -v`
Expected: `crs` and `geometry_type_override` PASS from Task 2; `append_mode_rejected` PASS because `OgrGbxWriter.__init__` raises when `not overwrite and target exists`.

- [ ] **Step 3: Fix if needed**

If `append` is not rejected (Spark may pass `overwrite=False` without an existing target on first save), confirm the guard triggers only when the target exists; the test pre-creates it, so the guard must fire. No new code expected.

- [ ] **Step 4: Re-run to confirm**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_writer.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/ds/test_vector_writer.py
git commit -m "test(ds): vector writer CRS/geometry-type/override + append-rejection

Co-authored-by: Isaac"
```

---

## Task 5: Named writers (`shapefile_gbx`, `geojson_gbx`, `gpkg_gbx`) round-trip

**Files:**
- Test: `python/geobrix/test/ds/test_vector_writer.py`
- (No source change expected — named `*GbxDataSource` inherit `.writer()`; `self._READER._DRIVER` supplies the driver.)

- [ ] **Step 1: Write the failing test**

Append to `test/ds/test_vector_writer.py`:

```python
@pytest.mark.parametrize(
    "fmt,target",
    [
        ("geojson_gbx", "named.geojson"),
        ("gpkg_gbx", "named.gpkg"),
        ("shapefile_gbx", "named.shp"),
    ],
)
def test_named_writer_roundtrip(spark, tmp_path, fmt, target):
    register(spark)
    out = str(tmp_path / target)
    _wkb_df(spark).coalesce(1).write.format(fmt).mode("overwrite").save(out)
    back = spark.read.format(fmt).load(out)
    assert back.count() == 2
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    assert {
        _from_wkb(bytes(r[gcol])).geom_type for r in back.select(gcol).collect()
    } == {"Point"}
```

- [ ] **Step 2: Run test to verify it passes (or surfaces a driver gap)**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_writer.py::test_named_writer_roundtrip -v`
Expected: `geojson_gbx` + `gpkg_gbx` PASS. `shapefile_gbx` writes sidecar files next to `named.shp`; the `shapefile_gbx` reader reads the `.shp` directly (its `_zip_vsi` only rewrites `.zip` paths, so a plain `.shp` passes through). If the named writer can't find a driver, the bug is `OgrGbxDataSource.writer()` not reading `self._READER._DRIVER` — fix it.

- [ ] **Step 3: Fix if needed**

If `shapefile_gbx` fails because Shapefile attribute-name truncation breaks the round-trip count/geometry, keep attributes short in the test (already `name`, `pop`) — no source change. If a named driver is missing, ensure `OgrGbxDataSource.writer()` passes `self._READER._DRIVER`.

- [ ] **Step 4: Re-run to confirm**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_writer.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/ds/test_vector_writer.py
git commit -m "test(ds): named vector writers (geojson/gpkg/shapefile) round-trip

Named *GbxDataSource subclasses inherit .writer(); the driver comes from the
paired reader's _DRIVER, so no per-writer subclass is needed.

Co-authored-by: Isaac"
```

---

## Task 6: Serverless guard still green

**Files:**
- Modify (verify): `python/geobrix/test/pyrx/test_serverless_no_spark_config.py`

- [ ] **Step 1: Confirm `vector.py` is in the scanned modules**

Run: `cd python/geobrix && grep -n "vector" test/pyrx/test_serverless_no_spark_config.py`
Expected: `vector.py` already listed (added when the readers landed). If absent, add it to the module list.

- [ ] **Step 2: Run the guard**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/pyrx/test_serverless_no_spark_config.py -v`
Expected: PASS — the writer additions use only `pyogrio`/`pyarrow`/`shapely`/`os`/`shutil`/`uuid`, no `_jvm`/`.conf.set`/`.rdd`/`SparkConf`.

- [ ] **Step 3: Fix if needed**

If the guard flags a banned pattern, remove it (the writer must not touch Spark internals). No expected violations.

- [ ] **Step 4: Commit (only if the test file changed)**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/pyrx/test_serverless_no_spark_config.py
git commit -m "test(pyrx): keep vector.py under the Serverless no-Spark-config guard

Co-authored-by: Isaac"
```

---

## Task 7: FileGDB writer (best-effort, version-gated)

**Files:**
- Test: `python/geobrix/test/ds/test_vector_writer.py`

- [ ] **Step 1: Write the failing/skipping test**

Append to `test/ds/test_vector_writer.py`:

```python
def _ogr_can_create(driver: str) -> bool:
    try:
        import tempfile

        from shapely import Point, to_wkb
        import pyarrow as pa
        import pyogrio

        d = tempfile.mkdtemp()
        path = d + ("/t.gdb" if driver == "OpenFileGDB" else "/t.out")
        tbl = pa.table({"g": [to_wkb(Point(0, 0))]})
        pyogrio.write_arrow(
            tbl, path, driver=driver, geometry_name="g",
            geometry_type="Point", crs="EPSG:4326",
        )
        return True
    except Exception:
        return False


def test_file_gdb_writer_roundtrip(spark, tmp_path):
    register(spark)
    if not _ogr_can_create("OpenFileGDB"):
        pytest.skip("installed GDAL OpenFileGDB driver cannot create datasets")
    out = str(tmp_path / "out.gdb")
    _wkb_df(spark).coalesce(1).write.format("file_gdb_gbx").mode("overwrite").save(out)
    back = spark.read.format("file_gdb_gbx").load(out)
    assert back.count() == 2
```

- [ ] **Step 2: Run test**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_writer.py::test_file_gdb_writer_roundtrip -v`
Expected: PASS or SKIP (skip if local GDAL OpenFileGDB is read-only). The Docker container's GDAL (DBR-aligned) is the authoritative check — see Task 11.

- [ ] **Step 3: Fix if needed**

No source change expected; `file_gdb_gbx` inherits `.writer()` and `_FileGdbReader._DRIVER = "OpenFileGDB"`. If create fails everywhere, leave the skip (FileGDB write is documented best-effort).

- [ ] **Step 4: Re-run to confirm**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_writer.py -v`
Expected: PASS/SKIP, no errors.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/ds/test_vector_writer.py
git commit -m "test(ds): file_gdb_gbx writer round-trip (version-gated skip)

Co-authored-by: Isaac"
```

---

## Task 8: Docker integration round-trip parity against the real corpus

**Files:**
- Create: `python/geobrix/test/ds/test_vector_writer_parity.py`

- [ ] **Step 1: Write the test**

```python
"""Light vector writer round-trip (Docker / integration).

read(<fmt>_gbx) -> write(<fmt>_gbx) -> read(<fmt>_gbx) is feature-count and
geometry stable against the real corpus. Writer is light-only (heavy has no
vector writer); this is the writer's correctness gate. Skips unless sample
data is mounted."""

import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

SAMPLE = (
    os.environ.get("GBX_SAMPLE_DATA_ROOT", "/Volumes/main/default/test-data").rstrip(
        "/"
    )
    + "/geobrix-examples"
)

_CASES = [
    ("geojson_gbx", f"{SAMPLE}/nyc/boroughs/nyc_boroughs.geojson", "rt.geojson"),
    ("gpkg_gbx", f"{SAMPLE}/nyc/geopackage/nyc_complete.gpkg", "rt.gpkg"),
    ("shapefile_gbx", f"{SAMPLE}/nyc/subway/nyc_subway.shp.zip", "rt.shp"),
]


@pytest.fixture(scope="module")
def spark():
    import logging

    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    s = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-ds-vector-writer-parity")
        .getOrCreate()
    )
    from databricks.labs.gbx.ds.register import register

    register(s)
    yield s


@pytest.mark.parametrize("fmt,src,target", _CASES)
def test_vector_writer_roundtrip(spark, tmp_path, fmt, src, target):
    if not os.path.exists(src):
        pytest.skip(f"sample not mounted: {src}")
    src_df = spark.read.format(fmt).load(src)
    n = src_df.count()
    out = str(tmp_path / target)
    src_df.coalesce(1).write.format(fmt).mode("overwrite").save(out)
    back = spark.read.format(fmt).load(out)
    assert back.count() == n
    gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
    assert back.where(f"{gcol} is not null").count() == n
```

- [ ] **Step 2: Run in Docker (dispatch a Task subagent — long-running)**

Start the volumes container and run the integration test (sample data + `GBX_SAMPLE_DATA_ROOT` full bundle). Per `docker-volumes-for-integration-tests` memory:

```bash
./scripts/docker/start_docker_with_volumes.sh
docker exec -e GBX_SAMPLE_DATA_ROOT=/Volumes/main/default/geobrix_samples geobrix-dev \
  bash -lc "cd /root/geobrix/python/geobrix && python -m pytest test/ds/test_vector_writer_parity.py -v"
```

Expected: 3 round-trips PASS (or SKIP if a corpus file is absent in the mounted bundle).

- [ ] **Step 3: Fix if needed**

If shapefile round-trip drops features due to field-name truncation, that is OGR Shapefile behavior, not a writer bug — assert on geometry count only (already done). If a format fails to write, fix the writer.

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX .git/objects
git add python/geobrix/test/ds/test_vector_writer_parity.py
git commit -m "test(ds): Docker round-trip parity for light vector writers

Co-authored-by: Isaac"
```

---

## Task 9: Bench — vector writer timing + round-trip gate

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/readers.py` (add `run_vector_write`)
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/cluster.py` (`_CELL_VECTOR` calls the writer path)

- [ ] **Step 1: Add `run_vector_write` to `python/geobrix/src/databricks/labs/gbx/bench/readers.py`**

Read `python/geobrix/src/databricks/labs/gbx/bench/readers.py` first to match the existing `run_format_read`/`run_pmtiles_write` style (timing helper, return shape). Then add:

```python
def run_vector_write(spark, fmt, src_path, out_path, warmups=1, measured=1):
    """Time read(fmt)->write(fmt) and assert read-back parity. Light-only
    (no heavy vector writer). Returns (median_seconds, roundtrip_ok)."""
    import time

    src = spark.read.format(fmt).load(src_path)
    n = src.count()

    def _once(target):
        t0 = time.perf_counter()
        src.coalesce(1).write.format(fmt).mode("overwrite").save(target)
        return time.perf_counter() - t0

    for w in range(warmups):
        _once(out_path + f".warm{w}")
    times = []
    ok = True
    for m in range(measured):
        target = out_path + f".m{m}"
        times.append(_once(target))
        back = spark.read.format(fmt).load(target)
        gcol = [f.name for f in back.schema.fields if f.name.endswith("_srid")][0][:-5]
        ok = ok and back.where(f"{gcol} is not null").count() == n
    times.sort()
    return times[len(times) // 2], ok
```

- [ ] **Step 2: Wire into `_CELL_VECTOR` in `python/geobrix/src/databricks/labs/gbx/bench/cluster.py`**

Read `python/geobrix/src/databricks/labs/gbx/bench/cluster.py`'s `_CELL_VECTOR` block. After the existing reader parity per format, add a writer leg guarded by the existing `BENCHMARK_VECTOR` flag — for each corpus format, call `run_vector_write` and record `light_write_s` + `roundtrip_ok` into the same results row the readers use. Keep the heavy columns null for the writer leg (no heavy vector writer).

- [ ] **Step 3: Smoke-test the bench cell locally (no cluster)**

Run a 1-tile/1-format local smoke through the existing bench smoke entrypoint (mirror `test-logs/bench-readers-smoke.log`'s invocation) to confirm `run_vector_write` imports and returns a tuple. Expected: a printed `(seconds, True)`.

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX .git/objects
git add bench/readers.py bench/cluster.py
git commit -m "feat(bench): vector writer timing + round-trip gate in --benchmark-vector

Light-only write timing per format with a read-back parity assertion (the
writer's correctness gate; no heavy vector writer to compare).

Co-authored-by: Isaac"
```

---

## Task 10: Docs — lightweight-only vector writer page + concurrency framing

**Files:**
- Create: `docs/docs/writers/vector.mdx`
- Modify: `docs/docs/writers/overview.mdx` (add the row + a prominent concurrency/perf section)
- Modify: `docs/docs/readers/overview.mdx` (add the same concurrency/perf section)
- Create: a doc-test under `docs/tests/python/writers/` exercised by `gbx:test:python-docs`

**User directive — make the distributed advantage a "big deal" (esp. on the overview pages).**
The light readers/writers are NOT single-node rasterio/pyogrio wrappers — they are Spark
DataSource V2 connectors that parallelize across the cluster. State this prominently and
concretely (factual, not marketing) in BOTH overview pages' lightweight tab, e.g. an
admonition / "Why this scales" subsection covering:
- **Readers — partitioned parallel reads:** vector readers slice features by `chunkSize`
  into partitions read concurrently across executors; raster readers split large files by
  `sizeInMB`. A single-node `pyogrio.read_*` / `rasterio.open` call reads one file
  sequentially on one machine; these readers fan the work across the cluster and yield a
  distributed DataFrame ready for joins/aggregations with no driver-side `collect`. This
  scales past a single machine's memory.
- **Writers — per-partition parallel writes + driver merge:** each executor writes its
  partition concurrently to a scratch fragment; the driver merges into the final output
  (two-phase). A single-node `pyogrio.write_*` serializes one file on one machine.
- **PMTiles writer — distributed spatial sharding:** partitions tiles into bounded
  per-shard archives written in parallel, then catalogs them — horizontal scaling vs a
  single memory-bound archive on one node. The merge is FUSE/object-store-safe (sequential,
  no `os.rename`) so it works on UC Volumes / DBFS.
Keep it concrete and tie each claim to the actual mechanism/option. No internal vocab
(no "wave"); no empty superlatives — describe the mechanism, let it speak.

- [ ] **Step 1: Write the doc-test (the doc's source of truth)**

Create `docs/tests/python/writers/vector_gbx_write.py` with a real write→read round-trip against the corpus (follow the existing reader doc-test pattern in `docs/tests/python/readers/`; read `GBX_SAMPLE_DATA_ROOT`). It must execute and assert (feature count stable), not just compile.

```python
# docs/tests/python/writers/vector_gbx_write.py
import os

from pyspark.sql import SparkSession

from databricks.labs.gbx.ds.register import register


def write_vector_gbx_example():
    spark = SparkSession.builder.getOrCreate()
    register(spark)
    root = os.environ.get("GBX_SAMPLE_DATA_ROOT", "/Volumes/main/default/test-data")
    src = f"{root}/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson"
    out = "/tmp/boroughs_out.geojson"

    df = spark.read.format("geojson_gbx").load(src)
    df.coalesce(1).write.format("geojson_gbx").mode("overwrite").save(out)

    back = spark.read.format("geojson_gbx").load(out)
    assert back.count() == df.count()
    return out
```

- [ ] **Step 2: Create `docs/docs/writers/vector.mdx`**

Lightweight-only page (no heavyweight tab; a `:::note` says heavy has no vector writer), importing the doc-test via raw-loader. Match the structure of `docs/docs/writers/pmtiles.mdx`:

```mdx
---
sidebar_position: 5
---

import CodeBlock from '@theme/CodeBlock';
import VectorWrite from '!!raw-loader!../../tests/python/writers/vector_gbx_write.py';

# Vector Writer

`geojson_gbx` / `shapefile_gbx` / `gpkg_gbx` / `file_gdb_gbx` / `ogr_gbx` — pure-Python
DataSource V2 writers (pyogrio). They take the light vector reader's schema
(`…attributes, geom_0` WKB, `geom_0_srid`, `geom_0_srid_proj`), so
`read → write` round-trips.

:::note No heavyweight equivalent
The heavyweight tier has no vector writer; vector output flows through Spark's
built-in writers. These `*_gbx` writers are lightweight-only.
:::

:::note Register first
Call `register(spark)` once before using any `*_gbx` format (see the
[Writers Overview](./overview)).
:::

## Options

| Option | Default | Behavior |
|---|---|---|
| `driverName` | required for `ogr_gbx`; preset by named writers | OGR driver. |
| `mode` | `overwrite` | `overwrite` only; `append` is rejected. |
| `geometryType` | inferred from the data | Override the OGR geometry type. |
| `layerName` | driver default | Output layer name where supported. |

## Example

<CodeBlock language="python">{VectorWrite}</CodeBlock>
```

- [ ] **Step 3: Add the row to `docs/docs/writers/overview.mdx`**

In the lightweight `### Available Writers` table (after the PMTiles row at line 34), add:

```
| [Vector Writer](./vector) | `geojson_gbx` / `shapefile_gbx` / `gpkg_gbx` / `file_gdb_gbx` / `ogr_gbx` | Pure-Python vector writers (pyogrio); round-trip with the `*_gbx` readers. |
```

- [ ] **Step 4: Run the doc-test in Docker + build docs (dispatch a Task subagent)**

Run: `gbx:test:python-docs --path docs/tests/python/writers/` and `gbx:docs:start` build check.
Expected: doc-test PASSES; docs build clean; `grep -rn -iE "wave [0-9]+" docs/docs/writers/vector.mdx` prints nothing.

- [ ] **Step 5: Commit**

```bash
chmod -R u+rwX .git/objects
git add docs/docs/writers/vector.mdx docs/docs/writers/overview.mdx docs/tests/python/writers/vector_gbx_write.py
git commit -m "docs(writers): lightweight-only Vector writer page + overview row

Co-authored-by: Isaac"
```

---

## Task 11: Corpus-generator command (Phase-3 enabler)

**Files:**
- Create: `scripts/commands/gbx-data-generate-vector-corpus.md`
- Create: `scripts/commands/gbx-data-generate-vector-corpus.sh`

- [ ] **Step 1: Write the `.md` registration**

Create `scripts/commands/gbx-data-generate-vector-corpus.md` (follow `scripts/commands/gbx-data-generate-minimal-bundle.md`): title, 1-2 sentence description, usage `bash scripts/commands/gbx-data-generate-vector-corpus.sh [OPTIONS]`, options (`--format`, `--features`, `--geometry`, `--out`, `--log`, `--help`), 2 examples.

- [ ] **Step 2: Write the `.sh` implementation**

Create `scripts/commands/gbx-data-generate-vector-corpus.sh` sourcing `common.sh` (for `check_docker`, `resolve_log_path`, `setup_log_file`, `show_banner`, `SCRIPT_DIR`/`PROJECT_ROOT` per the CLAUDE.md procedure). It runs inside the dev container and invokes a small Python that uses the `*_gbx` writer to emit N synthetic features of a chosen geometry type/format to `--out`. Real behavior, no placeholders; non-zero exit on failure. The generator Python:

```python
# emitted/run inside the container by the .sh
import sys

from pyspark.sql import SparkSession
from shapely import Point, to_wkb

from databricks.labs.gbx.ds.register import register

fmt, n, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]
spark = SparkSession.builder.getOrCreate()
register(spark)
rows = [
    (str(i), i, bytearray(to_wkb(Point(float(i % 360) - 180.0, float(i % 170) - 85.0))),
     "4326", "")
    for i in range(n)
]
df = spark.createDataFrame(
    rows,
    schema="name string, val int, geom_0 binary, geom_0_srid string, "
    "geom_0_srid_proj string",
)
df.write.format(fmt).mode("overwrite").save(out)
print(f"wrote {n} features to {out} as {fmt}")
```

- [ ] **Step 3: Make executable + smoke-test**

```bash
chmod +x scripts/commands/gbx-data-generate-vector-corpus.sh
bash scripts/commands/gbx-data-generate-vector-corpus.sh --help
```
Expected: prints usage, exit 0. Then a tiny in-container run (`--format geojson_gbx --features 100 --out /tmp/corpus_test.geojson`) prints the wrote-line and exits 0.

- [ ] **Step 4: Commit**

```bash
chmod -R u+rwX .git/objects
git add scripts/commands/gbx-data-generate-vector-corpus.md scripts/commands/gbx-data-generate-vector-corpus.sh
git commit -m "feat(data): gbx:data:generate-vector-corpus command (writer-backed)

Generates synthetic vector data via the *_gbx writers for Phase-3 scaled
benchmarking; runs in the dev container.

Co-authored-by: Isaac"
```

---

## Task 12: Lint + full vector test sweep before handoff

**Files:** none (verification)

- [ ] **Step 1: Python lint (CI gate)**

Run (per `run-python-lint-before-push` + `host-vs-docker-black-mismatch` memories — verify with the Docker check, not just host `--fix`):
`gbx:lint:python --check`
Expected: isort/black/flake8 clean for `vector.py`, the new tests, and `bench/*`.

- [ ] **Step 2: Full local vector DataSource sweep**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_writer.py test/ds/test_vector_reader.py test/ds/test_vector_named.py test/ds/test_vector_schema.py test/ds/test_register.py -v`
Expected: all PASS (FileGDB write may SKIP locally).

- [ ] **Step 3: Serverless guard final**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/pyrx/test_serverless_no_spark_config.py -v`
Expected: PASS.

- [ ] **Step 4: Commit any lint fixes**

```bash
chmod -R u+rwX .git/objects
git add -A
git commit -m "chore(ds): lint fixes for light vector writers

Co-authored-by: Isaac"
```

---

## Self-Review

**1. Spec coverage** (spec: `docs/superpowers/specs/2026-06-12-light-vector-writers-design.md`):
- Two-phase merge to one file → Tasks 2, 3 (executor fragment + driver merge; shared-FS scratch; FUSE-safe direct write; abort cleanup).
- Geometry + CRS handling (WKB geom col, WKT→WKB, inferred geometry_type + override, crs from srid/proj, srid/proj consumed not written) → Tasks 2, 4.
- Options table (`driverName`, `mode` overwrite-only/append-rejected, `geometryType`, `layerName`) → Tasks 2, 4.
- Architecture/files (`OgrGbxWriter`, `.writer()`, helpers, Serverless-safe lazy imports) → Tasks 1, 2, 6.
- Five writers (`ogr_gbx` + 4 named) → Tasks 2, 5, 7.
- Docs (lightweight-only page + note + doc-test round-trip) → Task 10.
- Benchmark (light-only writer timing + round-trip gate, in `--benchmark-vector`; benchmarking.mdx) → Task 9 (benchmarking.mdx vector-writer subsection is appended when the Task-9 numbers land — tracked, not a placeholder, since timings don't exist until the bench runs).
- Corpus generator (Phase-3 enabler) → Task 11.
- Testing (round-trip per format, multi-partition, CRS/geom-type, mode, Serverless, Docker) → Tasks 2–8.
- Out of scope (multi-geom field, heavy vector writer) → honored (single `geom_0`/derived geom col only).

**2. Placeholder scan:** No "TBD/TODO". The only deferred item is the benchmarking.mdx writer numbers, which legitimately do not exist until Task 9's bench runs; the doc edit is part of the bench-run follow-up, consistent with how reader numbers were handled.

**3. Type consistency:** `_VectorCommitMessage.frag_path` (str) used in `write`/`commit`; `_writer_col_roles` returns `(geom_col, srid_col, proj_col, attr_cols)` consistently; `OgrGbxWriter.__init__(path, schema, driver, options, overwrite)` matches the `OgrGbxDataSource.writer()` call; `run_vector_write(...) -> (seconds, ok)` consistent between `readers.py` and `cluster.py`. Driver source-of-truth is `self._READER._DRIVER` in both reader and writer paths.

---

## Execution Handoff

Plan complete. Recommended: subagent-driven-development (fresh subagent per task, two-stage review). Tasks 8, 10 dispatch Docker subagents (long-running); Task 9's cluster bench number-fill follows the next `--benchmark-vector` run.
