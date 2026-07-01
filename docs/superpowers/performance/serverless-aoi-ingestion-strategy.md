# Pattern: Serverless-first AOI ingestion strategy

**Status:** Pattern/correctness confirmed; cluster-scale speedup NOT yet measured (deferred to
optional cluster smoke; see Evidence section).

---

## Problem

A downloader/reader that ingests geographic data for an area of interest (AOI) has two naive
choices:

1. **Whole-file download on the driver** — pull entire continental/regional files, then filter
   client-side. Bottleneck: all bytes move through the driver; no parallelism; blocks on
   network I/O; scales linearly with source file size, not with AOI size.
2. **Number-only repartition fan-out** — `df.repartition(N)` per asset, then write. On
   Serverless, AQE coalesces a round-robin repartition toward 1 (serial), defeating the intent.

Both patterns fail at scale on Databricks Serverless: the first is serial by design; the second
silently degrades to serial at runtime.

---

## Symptom / signature

- A handful of large cloud parquet files cover the AOI; only a small fraction of rows are
  actually within the bbox.
- A whole-file download of those files is gigabytes; the AOI subset is megabytes.
- `repartition(N)` (number-only) appears in the plan but write-task count stays at 1 in the
  Spark UI.
- Worker network activity is absent (all I/O on the driver) or uniformly flat (serial tasks).

---

## The fix / pattern

**Serverless-first AOI ingestion** — three complementary moves:

### 1. Distributed read-in-place with bbox-struct predicate pushdown

When cloud parquet paths are available (object-store schemes or FUSE-mounted Volumes):

```python
df = spark.read.parquet(cloud_href)
if "bbox" in df.columns:           # Overture schema: nested bbox struct
    df = df.filter(
        (F.col("bbox.xmin") <= F.lit(maxx))
        & (F.col("bbox.xmax") >= F.lit(minx))
        & (F.col("bbox.ymin") <= F.lit(maxy))
        & (F.col("bbox.ymax") >= F.lit(miny))
    )
# AOI rows only move to executors; full files never land on the driver.
```

The Spark parquet reader + predicate pushdown means only row-groups whose bbox overlaps the
AOI are read. Bytes move on workers in parallel; the driver handles only metadata.

### 2. Column-hash repartition (NOT number-only)

```python
key = "id" if "id" in df.columns else df.columns[0]
df.repartition(partitions, F.col(key)).write.mode("overwrite").parquet(target)
```

On Serverless, `repartition(N)` (round-robin) is AQE-coalesced toward 1. Hashing by a
real column forces the shuffle to respect N partitions; AQE cannot coalesce a hash
repartition. See `[[serverless-fanout-repartition-by-column]]` for the full rule.

### 3. Cloud-scheme routing with HTTP-href fallback

Route to the distributed path when hrefs start with a cloud object-store scheme
(`s3://`, `abfs://`, `gs://`, etc.) or `/` (FUSE Volume). Fall back to whole-file HTTP
download (fanned out per-href with column-hash repartition) when only `https://` is
available:

```python
is_cloud = all(
    h.startswith(cloud_scheme) or h.startswith("/") for h in hrefs
)
if is_cloud:
    _download_distributed(assets_df, ...)   # read-in-place
else:
    _download_fallback(assets_df, ...)      # whole-file HTTP, still column-hash fanned
```

The fallback still uses `repartition(N, F.col("href"))` — serial driver download is NOT
the fallback; parallel per-href download is.

---

## Applicability matrix

### (a) Other light-tier functions this applies to

| Function / module | Applies? | Notes |
|---|---|---|
| `gbx.stac.StacClient.download` | Yes — same pattern | Uses per-href fan-out with column-hash repartition; add distributed read-in-place for cloud hrefs in a future pass |
| `gbx.sample._bundle` / `GbxBundle` | Partial | Per-file download; add column-hash repartition if not already present |
| Future `sample/` sources (NAIP, 3DEP, etc.) | Yes | Adopt the same cloud-scheme router: distributed-read when cloud path available, HTTP fallback otherwise |
| `pyrx` DataSource V2 raster readers (`raster_gbx`, `gtiff_gbx`) | No — different shape | Reads are already distributed per-tile by the DataSource V2 scan; AOI is applied via partition pruning, not a bbox struct filter |
| Light vector readers (`ogr_pyvx`, etc.) | Partial | pyogrio reads are per-file on executors; add bbox pushdown where the format supports spatial filter (e.g. GeoPackage rtree) |

### (b) Heavy-tier functions (same + similar)

| Function / Scala class | Applies? | Notes |
|---|---|---|
| Overture path (heavy) | N/A — no heavy Overture path | `OvertureClient` is light-only; no Scala expression reads Overture GeoParquet |
| Heavy OGR/GDAL readers | N/A — different arch | Heavy reads go through JVM OGR/GDAL DataSourceV2; spatial filtering is OGR `SetSpatialFilter` at the driver or executor level, not Spark predicate pushdown |
| Heavy `rst_fromfile` / raster readers | N/A | GDAL reads are per-tile on the JVM; predicate pushdown is not the bottleneck |

**Verdict:** This is a light-tier downloader pattern. The heavy tier reads via OGR/GDAL or
Spark DataSource V2 with JVM-level spatial filtering; the Spark bbox-struct predicate pushdown
technique is specific to GeoParquet sources with a nested `bbox` struct column (Overture schema).

---

## Evidence

**Pattern/correctness:** The distributed-read path and bbox-struct filter are structurally
correct — the Spark predicate pushdown on a nested struct column (`bbox.xmin`, etc.) is
standard Parquet predicate pushdown, confirmed in the Spark 4.0 Parquet reader. Column-hash
repartition correctness was empirically confirmed 2026-06-22 (see
`[[serverless-fanout-repartition-by-column]]`).

**Cluster-scale speedup: NOT YET MEASURED.** SP1 has only offline unit tests (injected catalog
+ fetcher; no real network). The distributed-read advantage (AOI bytes vs full-file bytes) is
a cluster-scale property. The open item from the design is: confirm Serverless can read
`s3://overturemaps-us-west-2/...` / `abfs://...` directly (requester-pays / credential config);
if blocked, the HTTP-href fallback becomes the primary path. A cluster smoke is deferred but
will produce concrete row-count and timing numbers for this entry.

**When the cluster smoke runs**, add numbers here:
- Full-file size (per theme/type asset) vs AOI-subset bytes written
- Wall time: distributed-read path vs hypothetical whole-file driver download
- Task count in Spark UI: confirm N > 1 with column-hash repartition

---

## Canonical code references

- `python/geobrix/src/databricks/labs/gbx/sample/overture.py`
  - `OvertureClient._download_distributed` — distributed read + bbox filter + column-hash write
  - `OvertureClient._download_fallback` — HTTP fan-out with column-hash repartition by `href`
  - `OvertureClient.download` — cloud-scheme router (distributed vs fallback)
  - `OvertureClient.read` — re-reads downloaded parquet with optional bbox re-filter
- `python/geobrix/src/databricks/labs/gbx/sample/_overture_discover.py`
  - `normalize_bbox`, `bbox_intersects` — shared bbox helpers used in pushdown
- Tests: `python/geobrix/test/sample/test_overture.py` (offline, injected seams)
