# Scaled Vector Benchmark Corpus + Bench Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate a realistic at-scale vector benchmark corpus (1M-polygon seed per format → GeoJSON/Shapefile/GeoPackage/FileGDB → replicate ×100), wire it into the cluster vector bench, run it, and fill the per-page + central benchmark numbers with real light-vs-heavy figures.

**Architecture:** A pure-Python corpus generator (`bench/corpus_vector.py`) mints a 1M-row polygon DataFrame in the writer schema, transcodes it to each format via the existing `*_gbx` writers (FileGDB via the native-osgeo hybrid — cluster only), and replicates each seed into a per-format directory on the bench Volume. A `gbx:bench:generate-vector-corpus` command runs the pipeline locally (small-scale validation) and on the cluster (full scale). The bench's `_CELL_VECTOR` gains a scaled mode that reads the 1M seed + writes 1M rows per format, light vs heavy, recording the real numbers.

**Tech Stack:** PySpark, the `*_gbx` vector readers/writers, pyogrio, shapely, the cluster bench harness (`databricks.labs.gbx.bench`).

---

## Reframe (2026-06-12): pipeline-shaped bench + format-capacity sizing

After Tasks 1–6 landed, the scenario and sizing were refined (supersedes the relevant parts of Tasks 6–8 below):

**Two-leg pipeline (not a `read.format(x)→write.format(x)` round-trip):**
- **Reader = ingest.** Read *enough files with enough rows* and **write a Delta table** (the common "load vector data into Delta" pipeline). Timed = the full read→Delta materialization (forces a real read, not a lazy `count()` that games the parser). Light `*_gbx` vs heavy `*_ogr`. Source = a directory of N vector files (the replicated copies; shapefile/FileGDB copies are zipped `.shp.zip`/`.gdb.zip` so both tiers dir-read them).
- **Writer = export.** Start from a **Delta table** of vector data and **write a single file** in format x. Timed = table→single-file write (the writer's driver-side merge). Light only (heavy has no named-vector writers — documented gap). Multi-file output ("subdivide a table into a handful of files") is the **future API capability**, noted not benchmarked.

**Capacity-driven sizing — push toward each format's ceiling; the limit must read as the *format's*, not GeoBrix's (never let a round number look like our cap):**

| Format | Hard ceiling | Cause | ≈ box-polygon features at ceiling |
|---|---|---|---|
| Shapefile | **2 GB** per `.shp` (and per `.dbf`) | 32-bit offsets in 16-bit words (OGR caps 4 GB; 2 GB compat norm) | **~15.8 M** |
| GeoPackage | **~17.6 TB** (4 KB pages; ~281 TB max) | SQLite `page_size × max_page_count` | billions (no practical cap) |
| FileGDB | **2.1 B rows** / 1 TB per FC (→256 TB) | OBJECTID = signed int32 | ~2.1 billion |
| GeoJSON | **none** (RFC 7946) | text; bounded only by disk/parse memory | unbounded |

- **Writer-export (capacity demo):** source = a single ~14 M-polygon Delta table (generated directly via `generate_polygon_seed`, not from the vector corpus) → one file per format. ~14 M ≈ 1.9 GB shapefile — near (safely under) its 2 GB ceiling, **no deliberate break**; the others carry it and are *documented* (cited) to go to billions/TB. Single-file is driver-bound → measure on the small validation; if too heavy (esp. GPKG/FileGDB), cap the writer leg at the largest size that completes cleanly and document the figure there.
- **Reader-ingest (scale demo):** read a directory of N copies (1 M each) → Delta; N chosen for "enough files with enough rows" (distributed, scales fine). Final N picked from the small-validation throughput (light GPKG read uses the read-only-Volume in-memory fallback and is the slow path — size so it completes).

**Harness shape:** `run_format_read` → read dir + `write.format("delta").saveAsTable` (timed); `run_vector_write` → read a source Delta table + write one file (timed). `_CELL_VECTOR` materializes the ~14 M writer-source table once (untimed), runs the reader leg (copies dir → Delta, both tiers) and the writer leg (source table → single file, light).

**Benchmarking.mdx (Task 8):** fill the two legs' numbers AND state each format's true ceiling (cited) so the capacity story is explicit.

---

## File structure

- **Create `python/geobrix/src/databricks/labs/gbx/bench/corpus_vector.py`** — the generator: `generate_polygon_seed`, `transcode_vector_seed`, `replicate_vector_seed`, `build_vector_corpus` (orchestrator). Pure functions over a SparkSession; no bench-harness coupling.
- **Create `python/geobrix/test/bench/test_corpus_vector.py`** — local small-scale tests (1000 rows, ×3 copies; FileGDB skips without osgeo).
- **Create `scripts/commands/gbx-bench-generate-vector-corpus.{md,sh}`** — CLI wrapper (params `--rows`, `--copies`, `--formats`, `--out`, `--log`), runs in the dev container / on the cluster.
- **Modify `python/geobrix/src/databricks/labs/gbx/ds/vector.py`** — `OgrGbxReader` gains directory enumeration (one partition per vector file in a dir) so the bench can read the ×100 corpus and users can read a folder of files.
- **Modify `python/geobrix/src/databricks/labs/gbx/bench/cluster.py`** (`_CELL_VECTOR`) + **`bench/readers.py`** — a scaled-corpus path: read the 1M seed + the ×N directory, write 1M rows, per format, light vs heavy.
- **Modify `docs/docs/api/benchmarking.mdx`** + the 15 reader/writer page `Benchmark & tradeoff` callouts — fill the real numbers.

The corpus lives at `{CORPUS}/vector-scale/<fmt>/seed.<ext>` (the 1M seed) and `{CORPUS}/vector-scale/<fmt>/copies/copy_<i>.<ext>` (the ×N replicas). `<ext>`: `.geojson`, `.shp`, `.gpkg`, `.gdb`.

---

## Task 1: Polygon seed generator

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/bench/corpus_vector.py`
- Test: `python/geobrix/test/bench/test_corpus_vector.py`

- [ ] **Step 1: Write the failing test**

```python
# python/geobrix/test/bench/test_corpus_vector.py
import logging

import pytest
from shapely import from_wkb


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-corpus-vector")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s


def test_generate_polygon_seed(spark):
    from databricks.labs.gbx.bench.corpus_vector import generate_polygon_seed

    df = generate_polygon_seed(spark, 200, srid="4326")
    assert [f.name for f in df.schema.fields] == [
        "geom_0",
        "geom_0_srid",
        "geom_0_srid_proj",
        "id",
        "name",
    ]
    assert df.count() == 200
    row = df.orderBy("id").first()
    assert row["geom_0_srid"] == "4326"
    g = from_wkb(bytes(row["geom_0"]))
    assert g.geom_type == "Polygon"
    assert -180.0 <= g.bounds[0] <= 180.0 and -90.0 <= g.bounds[1] <= 90.0
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/bench/test_corpus_vector.py::test_generate_polygon_seed -v`
Expected: FAIL — `ModuleNotFoundError`/`ImportError` (corpus_vector / generate_polygon_seed not defined).

- [ ] **Step 3: Implement `generate_polygon_seed`**

```python
# python/geobrix/src/databricks/labs/gbx/bench/corpus_vector.py
"""Scaled vector benchmark corpus generator. Mints a 1M-polygon seed in the light
vector-writer schema, transcodes it to each format via the *_gbx writers, and
replicates each seed into a per-format directory on the bench Volume. Runs locally
(small scale) and on the bench cluster (full scale). FileGDB writing needs the
heavyweight GDAL natives (native osgeo) -- cluster only."""

from __future__ import annotations

import os
import shutil
from typing import List


def generate_polygon_seed(spark, n_rows: int, srid: str = "4326"):
    """A DataFrame of ``n_rows`` synthetic polygons in the light vector-writer schema
    (geom_0 WKB, geom_0_srid, geom_0_srid_proj, id, name). Polygons are small axis-
    aligned boxes at deterministic pseudo-random lon/lat from the row id."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import BinaryType

    @F.udf(BinaryType())
    def _poly(i):
        from shapely import box, to_wkb

        lon = (int(i) * 73 % 35900) / 100.0 - 179.0
        lat = (int(i) * 37 % 17800) / 100.0 - 89.0
        d = 0.01
        return bytes(to_wkb(box(lon, lat, lon + d, lat + d)))

    return spark.range(n_rows).select(
        _poly(F.col("id")).alias("geom_0"),
        F.lit(srid).alias("geom_0_srid"),
        F.lit("").alias("geom_0_srid_proj"),
        F.col("id").cast("int").alias("id"),
        F.concat(F.lit("feat_"), F.col("id").cast("string")).alias("name"),
    )
```

- [ ] **Step 4: Run it to verify it passes**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/bench/test_corpus_vector.py::test_generate_polygon_seed -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/mjohns/IdeaProjects/geobrix
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/bench/corpus_vector.py python/geobrix/test/bench/test_corpus_vector.py
git commit -m "feat(bench): polygon seed generator for the scaled vector corpus

Co-authored-by: Isaac"
```

---

## Task 2: Transcode the seed to each vector format

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/corpus_vector.py`
- Test: `python/geobrix/test/bench/test_corpus_vector.py`

- [ ] **Step 1: Write the failing test**

```python
def test_transcode_vector_seed(spark, tmp_path):
    from databricks.labs.gbx.bench.corpus_vector import (
        generate_polygon_seed,
        transcode_vector_seed,
    )
    from databricks.labs.gbx.ds.register import register

    register(spark)
    seed = generate_polygon_seed(spark, 100)
    # file_gdb needs native osgeo (heavy natives) -> exclude locally
    fmts = ["geojson_gbx", "shapefile_gbx", "gpkg_gbx"]
    out = transcode_vector_seed(spark, seed, fmts, str(tmp_path / "vec"))
    for fmt in fmts:
        assert fmt in out
        back = spark.read.format(fmt).load(out[fmt])
        assert back.count() == 100
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/bench/test_corpus_vector.py::test_transcode_vector_seed -v`
Expected: FAIL — `transcode_vector_seed` not defined.

- [ ] **Step 3: Implement `transcode_vector_seed`**

Append to `corpus_vector.py`:

```python
_EXT = {
    "geojson_gbx": "geojson",
    "shapefile_gbx": "shp",
    "gpkg_gbx": "gpkg",
    "file_gdb_gbx": "gdb",
    "vector_gbx": "geojson",
}


def transcode_vector_seed(spark, seed_df, formats: List[str], out_base: str) -> dict:
    """Write the seed DataFrame to each format's seed file via the *_gbx writers.
    Returns {fmt: seed_path}. The seed is cached so each write reuses it. FileGDB
    requires the native osgeo (heavyweight GDAL natives)."""
    seed_df = seed_df.cache()
    seed_df.count()  # materialize the cache
    out: dict = {}
    for fmt in formats:
        ext = _EXT.get(fmt, "out")
        path = f"{out_base}/{fmt}/seed.{ext}"
        writer = seed_df.coalesce(1).write.format(fmt).mode("overwrite")
        if fmt in ("vector_gbx", "ogr_gbx"):
            writer = writer.option("driverName", "GeoJSON")
        writer.save(path)
        out[fmt] = path
    return out
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/bench/test_corpus_vector.py::test_transcode_vector_seed -v`
Expected: PASS (geojson/shapefile/gpkg round-trip).

- [ ] **Step 5: Commit**

```bash
cd /Users/mjohns/IdeaProjects/geobrix
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/bench/corpus_vector.py python/geobrix/test/bench/test_corpus_vector.py
git commit -m "feat(bench): transcode the vector seed to each *_gbx format

Co-authored-by: Isaac"
```

---

## Task 3: Replicate each seed ×N

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/corpus_vector.py`
- Test: `python/geobrix/test/bench/test_corpus_vector.py`

- [ ] **Step 1: Write the failing test**

```python
def test_replicate_vector_seed(spark, tmp_path):
    import os

    from databricks.labs.gbx.bench.corpus_vector import replicate_vector_seed

    # a fake single-file seed
    seed = str(tmp_path / "seed.geojson")
    with open(seed, "w") as fh:
        fh.write('{"type":"FeatureCollection","features":[]}')
    copies_dir = str(tmp_path / "copies")
    paths = replicate_vector_seed(seed, 5, copies_dir)
    assert len(paths) == 5
    assert all(os.path.exists(p) for p in paths)
    assert sorted(os.listdir(copies_dir)) == [f"copy_{i}.geojson" for i in range(5)]
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/bench/test_corpus_vector.py::test_replicate_vector_seed -v`
Expected: FAIL — `replicate_vector_seed` not defined.

- [ ] **Step 3: Implement `replicate_vector_seed`**

Append to `corpus_vector.py`:

```python
def replicate_vector_seed(seed_path: str, n_copies: int, copies_dir: str) -> List[str]:
    """Copy a per-format seed (a file, a `.shp` + sidecars, or a `.gdb` dir) ``n_copies``
    times into ``copies_dir`` as ``copy_<i>.<ext>``. Sequential copies (FUSE-safe).
    Returns the copy paths."""
    os.makedirs(copies_dir, exist_ok=True)
    base = os.path.basename(seed_path.rstrip("/"))
    stem, _, ext = base.partition(".")
    paths: List[str] = []
    for i in range(n_copies):
        dst = os.path.join(copies_dir, f"copy_{i}.{ext}" if ext else f"copy_{i}")
        if os.path.isdir(seed_path):  # FileGDB .gdb directory
            shutil.copytree(seed_path, dst, dirs_exist_ok=True)
        else:
            shutil.copy(seed_path, dst)
            # Shapefile sidecars (.shx/.dbf/.prj) share the stem -- copy them too.
            src_dir = os.path.dirname(seed_path) or "."
            src_stem = base.split(".")[0]
            for sib in os.listdir(src_dir):
                if sib.startswith(src_stem + ".") and sib != base:
                    sib_ext = sib[len(src_stem) + 1 :]
                    shutil.copy(
                        os.path.join(src_dir, sib),
                        os.path.join(copies_dir, f"copy_{i}.{sib_ext}"),
                    )
        paths.append(dst)
    return paths
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/bench/test_corpus_vector.py::test_replicate_vector_seed -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/mjohns/IdeaProjects/geobrix
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/bench/corpus_vector.py python/geobrix/test/bench/test_corpus_vector.py
git commit -m "feat(bench): replicate per-format vector seeds (incl. sidecars/.gdb dir)

Co-authored-by: Isaac"
```

---

## Task 4: Orchestrator `build_vector_corpus` + CLI command

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/corpus_vector.py`
- Create: `scripts/commands/gbx-bench-generate-vector-corpus.md`, `scripts/commands/gbx-bench-generate-vector-corpus.sh`
- Test: `python/geobrix/test/bench/test_corpus_vector.py`

- [ ] **Step 1: Write the failing test**

```python
def test_build_vector_corpus(spark, tmp_path):
    import os

    from databricks.labs.gbx.bench.corpus_vector import build_vector_corpus
    from databricks.labs.gbx.ds.register import register

    register(spark)
    out = build_vector_corpus(
        spark, rows=50, copies=3,
        formats=["geojson_gbx", "gpkg_gbx"], out_base=str(tmp_path / "vc"),
    )
    for fmt in ("geojson_gbx", "gpkg_gbx"):
        assert os.path.exists(out[fmt]["seed"])
        assert len(out[fmt]["copies"]) == 3
        assert spark.read.format(fmt).load(out[fmt]["seed"]).count() == 50
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/bench/test_corpus_vector.py::test_build_vector_corpus -v`
Expected: FAIL — `build_vector_corpus` not defined.

- [ ] **Step 3: Implement `build_vector_corpus`**

Append to `corpus_vector.py`:

```python
def build_vector_corpus(
    spark, rows: int, copies: int, formats: List[str], out_base: str, srid: str = "4326"
) -> dict:
    """Full pipeline: generate the polygon seed -> transcode to each format ->
    replicate ×copies. Returns {fmt: {"seed": path, "copies": [paths]}}."""
    from databricks.labs.gbx.ds.register import register

    register(spark)
    seed_df = generate_polygon_seed(spark, rows, srid=srid)
    seeds = transcode_vector_seed(spark, seed_df, formats, out_base)
    result: dict = {}
    for fmt, seed_path in seeds.items():
        copies_dir = f"{out_base}/{fmt}/copies"
        result[fmt] = {
            "seed": seed_path,
            "copies": replicate_vector_seed(seed_path, copies, copies_dir),
        }
    return result
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/bench/test_corpus_vector.py -v`
Expected: all PASS.

- [ ] **Step 5: Create the CLI command**

`scripts/commands/gbx-bench-generate-vector-corpus.md`: title, description, usage `bash scripts/commands/gbx-bench-generate-vector-corpus.sh [OPTIONS]`, options (`--rows` default `1000000`, `--copies` default `100`, `--formats` default `geojson_gbx,shapefile_gbx,gpkg_gbx,file_gdb_gbx`, `--out` default `/Volumes/.../bench-corpus/vector-scale`, `--log`, `--help`), 2 examples (small local, full cluster). Mirror `scripts/commands/gbx-data-generate-vector-corpus.md`.

`scripts/commands/gbx-bench-generate-vector-corpus.sh`: source `common.sh`; parse the options; run in the dev container (or note cluster execution); invoke a small inline Python that calls `build_vector_corpus` with a `SparkSession.builder.getOrCreate()`. Mirror the structure of `scripts/commands/gbx-data-generate-vector-corpus.sh` (which already runs a writer-backed generator in the container). `chmod +x` it.

- [ ] **Step 6: Smoke-test**

```bash
bash scripts/commands/gbx-bench-generate-vector-corpus.sh --help        # prints usage, exit 0
# small local run (geojson/gpkg only — no osgeo locally):
bash scripts/commands/gbx-bench-generate-vector-corpus.sh --rows 500 --copies 2 --formats geojson_gbx,gpkg_gbx --out /tmp/vc_smoke
```
Expected: the `--help` exits 0; the small run reports the seeds + copies created.

- [ ] **Step 7: Commit**

```bash
cd /Users/mjohns/IdeaProjects/geobrix
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/bench/corpus_vector.py python/geobrix/test/bench/test_corpus_vector.py scripts/commands/gbx-bench-generate-vector-corpus.md scripts/commands/gbx-bench-generate-vector-corpus.sh
git commit -m "feat(bench): gbx:bench:generate-vector-corpus (seed->transcode->replicate)

Co-authored-by: Isaac"
```

---

## Task 5: Light vector reader — directory enumeration

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/vector.py` (`OgrGbxReader.partitions`/`schema`)
- Test: `python/geobrix/test/ds/test_vector_reader.py`

So the bench (and users) can read the ×N corpus folder: when `path` is a directory, enumerate the vector files in it (one `_ChunkPartition` per file × its feature chunks), and infer the schema from the first file.

- [ ] **Step 1: Write the failing test**

```python
def test_ogr_gbx_reads_directory(spark, tmp_path):
    register(spark)
    import os
    d = os.path.join(str(tmp_path), "many")
    os.makedirs(d)
    for k in range(3):
        with open(os.path.join(d, f"p{k}.geojson"), "w") as f:
            json.dump(_GJ, f)  # _GJ is the 2-feature FeatureCollection in this file
    df = spark.read.format("geojson_gbx").load(d)
    assert df.count() == 6  # 3 files x 2 features
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_reader.py::test_ogr_gbx_reads_directory -v`
Expected: FAIL — the reader treats the dir as one path and errors or returns 0.

- [ ] **Step 3: Implement directory enumeration**

In `OgrGbxReader`, add a helper that lists the member files when `self.path` is a directory (matching the format's extensions; for shapefile use `.shp`/`.shz`/`.zip`, geojson `.geojson`/`.json`/`.geojsonl`, gpkg `.gpkg`, filegdb `.gdb` dirs), and make `schema()` read the first member and `partitions()` emit chunk partitions per member. Single-file paths keep current behavior. Show the full method bodies:

```python
    _EXT_FOR_DRIVER = {
        "GeoJSON": (".geojson", ".json"),
        "GeoJSONSeq": (".geojsonl", ".geojsons"),
        "ESRI Shapefile": (".shp", ".shz", ".zip"),
        "GPKG": (".gpkg",),
        "OpenFileGDB": (".gdb",),
    }

    def _members(self) -> List[str]:
        """If self.path is a directory of vector files, the member paths; else [self.path]."""
        if not os.path.isdir(self.path) or self.path.lower().endswith(".gdb"):
            return [self.path]
        exts = self._EXT_FOR_DRIVER.get(self.driver) or ()
        names = sorted(os.listdir(self.path))
        members = [
            os.path.join(self.path, n)
            for n in names
            if (exts and n.lower().endswith(exts)) or n.lower().endswith(".gdb")
        ]
        return members or [self.path]

    def schema(self) -> StructType:
        first = self._members()[0]
        return _vector_schema(self._info_for(first), self.as_wkb)
```

Refactor `_info()` into `_info_for(path)` (the current `_info` body parameterized by path, keeping the read-only in-memory fallback), and make `partitions()` loop over `self._members()`, emitting `_ChunkPartition(member, ...)` chunks for each. (`read()` already takes the partition's `path`, so it is unchanged.)

- [ ] **Step 4: Run to verify it passes**

Run: `cd python/geobrix && ../../.venv-pyrx/bin/python -m pytest test/ds/test_vector_reader.py -v`
Expected: all PASS (existing single-file tests + the new directory test).

- [ ] **Step 5: Commit**

```bash
cd /Users/mjohns/IdeaProjects/geobrix
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/ds/vector.py python/geobrix/test/ds/test_vector_reader.py
git commit -m "feat(ds): light vector reader enumerates a directory of files

Co-authored-by: Isaac"
```

---

## Task 6: Scaled-corpus mode in the vector bench

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/cluster.py` (`_CELL_VECTOR` + a `VECTOR_SCALE` preamble flag), `notebooks/tests/push_and_run_bench_on_cluster.py` (parse `--vector-scale`)
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/readers.py` (no signature change expected; `run_format_read`/`run_vector_write` already take a path)

- [ ] **Step 1: Add the `--vector-scale` flag (launcher)**

In `push_and_run_bench_on_cluster.py`, parse `vector_scale = "--vector-scale" in sys.argv` near the other flags, pass it through `build_bench_notebook(cfg)` (add `vector_scale` to `cfg`), and into the PREAMBLE as `VECTOR_SCALE = {vector_scale!r}`.

- [ ] **Step 2: Branch `_CELL_VECTOR` on `VECTOR_SCALE`**

When `VECTOR_SCALE` is true, point the cases at the scaled corpus and read the **copies directory** (the reader now enumerates it) for the reader leg, and write the **seed read-back DataFrame** for the writer leg. Concretely, the scaled cases use `f"{CORPUS}/vector-scale/{fmt}/copies"` for the read path and `f"{CORPUS}/vector-scale/{fmt}/seed.<ext>"` for the writer's source. Keep the existing tiny-corpus cases for the default (non-scale) run. Show the scaled `_vcases` block and that the reader path is the `copies` dir while the writer reads the seed and writes it back. Heavy geojson keeps `multi=false`.

- [ ] **Step 3: Smoke-test the notebook builder locally**

```bash
cd /Users/mjohns/IdeaProjects/geobrix && .venv-pyrx/bin/python -c "
import sys; sys.path.insert(0,'python/geobrix/src')
from databricks.labs.gbx.bench.cluster import build_bench_notebook
nb=build_bench_notebook({'corpus':'/Volumes/x','out_dir':'/Volumes/x/o','table':'t','run_id':'r','functions':'','set':'core','modes':'spark-path','row_counts':'1000','warmup':1,'measured':1,'spark_warmup':1,'spark_measured':1,'partition_size':0,'truncate':False,'truncate_all':False,'resume':False,'fix_errors':True,'redo_functions':'','lightweight':True,'heavyweight':True,'explain_only':False,'benchmark_readers':False,'readers_only':False,'benchmark_pmtiles':False,'pmtiles_only':False,'benchmark_vector':True,'vector_only':True,'vector_scale':True,'wheel':'/Volumes/x/w.whl'})
print('cells:', len(nb['cells']))
print('VECTOR_SCALE' in str(nb))
"
```
Expected: prints a cell count and `True` (the flag threads through). Adjust the cfg keys to match the real `build_bench_notebook` signature if it differs (read it first).

- [ ] **Step 4: Commit**

```bash
cd /Users/mjohns/IdeaProjects/geobrix
chmod -R u+rwX .git/objects
git add python/geobrix/src/databricks/labs/gbx/bench/cluster.py notebooks/tests/push_and_run_bench_on_cluster.py
git commit -m "feat(bench): --vector-scale mode reads the 1M-seed corpus + copies dir

Co-authored-by: Isaac"
```

---

## Task 7: Generate the corpus + run the scaled bench (cluster, operational)

**Files:** none (operational). Requires the heavy GDAL natives on the cluster (for the FileGDB seed) and a rebuilt+staged wheel.

- [ ] **Step 1: Rebuild + stage the wheel** (it carries `corpus_vector.py` + the reader/bench changes):
`GBX_BUNDLE_SKIP_JAR_UPLOAD=1 bash scripts/commands/gbx-data-push-wheel.sh` → "Done: …/geobrix-0.4.0-py3-none-any.whl".

- [ ] **Step 2: Generate the corpus on the cluster** (1M × 100, polygons, all 4 formats) via a one-off notebook/job that calls `build_vector_corpus(spark, rows=1_000_000, copies=100, formats=[…4…], out_base=f"{CORPUS}/vector-scale")`. (FileGDB seed needs the natives — confirm `osgeo` imports on the cluster, as proven earlier.) Verify the seeds + 100 copies exist per format on the Volume.

- [ ] **Step 3: Run the scaled vector bench**:
`export GBX_BUNDLE_WHEEL_VOLUME_PATH=…; bash scripts/commands/gbx-bench-cluster.sh --vector-only --vector-scale --row-counts 1000 --log vector-scale.log`
Expected: `run_id=cluster-vector` rows for each format's reader (light vs heavy, ~1M-scale) + writer (light, 1M rows), all `status=ok`.

- [ ] **Step 4: Capture the numbers** — query `geospatial_docs.geobrix.bench_results` for `run_id='cluster-vector'`, record per-format light/heavy `iter_median_s` + `throughput_rows_s`.

---

## Task 8: Fill the benchmark numbers — CENTRAL Benchmarking page only

**Decision (user, 2026-06-12):** benchmarks are **consolidated to the single
[Benchmarking](../api/benchmarking) page**, NOT duplicated per page. The per-format reader/
writer pages keep ONLY their prominent `Benchmark & tradeoff` note + link (already in place
— do NOT add per-page numbers). So this task touches `benchmarking.mdx` alone.

**Files:**
- Modify: `docs/docs/api/benchmarking.mdx` (vector reader + writer results tables — replace the `—` placeholders with the Task-7 numbers).

- [ ] **Step 1** — put the Task-7 per-format numbers into `benchmarking.mdx`'s vector reader + writer tables: light vs heavy median + throughput where both tiers have an implementation (readers: all 4 formats; writers: light `*_gbx` for all, heavy only for PMTiles/raster — vector heavy has no writer; FileGDB writer is the native-osgeo hybrid). Method line: 1M-polygon seed, ×100 copies, cluster, median of measured iters.
- [ ] **Step 2** — `cd docs && npm run build` → SUCCESS; `grep -rn -iE "wave [0-9]+" docs/docs/` empty. Confirm the 15 per-page `Benchmark & tradeoff` callouts are UNCHANGED (note + link only).
- [ ] **Step 3: Commit**

```bash
cd /Users/mjohns/IdeaProjects/geobrix
chmod -R u+rwX .git/objects
git add docs/docs/api/benchmarking.mdx
git commit -m "docs(bench): fill scaled vector reader/writer numbers on the Benchmarking page

Co-authored-by: Isaac"
```

---

## Self-Review

**Spec coverage:** seed generation (T1), transcode to geojson/shapefile/gpkg/filegdb via writers (T2), replicate ×100 (T3), orchestrator + CLI runnable locally & on cluster (T4), reader directory support to read the copies (T5), bench wiring scaled mode (T6), cluster generate+run at 1M×100 (T7), fill per-page + benchmarking.mdx numbers (T8). FileGDB-needs-natives is called out in T2/T7. Local small-scale validation in T1–T4 (FileGDB excluded locally — no osgeo). ✓

**Placeholder scan:** no TBD/TODO. T6/T8 reference reading the real `build_bench_notebook` signature / inserting real numbers from T7 (numbers don't exist until the run) — consistent with how the reader numbers were handled; the code steps (T1–T5) carry full implementations.

**Type consistency:** `generate_polygon_seed`→`transcode_vector_seed`→`replicate_vector_seed`→`build_vector_corpus` signatures align; seed schema `(geom_0, geom_0_srid, geom_0_srid_proj, id, name)` matches the writer's `_writer_col_roles` contract (geom + `*_srid`); `_EXT` map consistent across transcode/replicate; the reader directory enumeration reuses the existing `_ChunkPartition`/`read()`.

---

## Execution Handoff

Recommended: subagent-driven-development (fresh subagent per task, two-stage review). Tasks 1–6 are local/code (TDD, FileGDB-write steps skip locally without osgeo); Task 7 is the cluster generate+run; Task 8 fills the numbers from Task 7.
