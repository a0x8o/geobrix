<img src="resources/images/GeoBriX.png" width="50%" />

[![build](https://github.com/databrickslabs/geobrix/actions/workflows/build_main.yml/badge.svg)](https://github.com/databrickslabs/geobrix/actions/workflows/build_main.yml)
[![codecov](https://codecov.io/gh/databrickslabs/geobrix/branch/main/graph/badge.svg)](https://codecov.io/gh/databrickslabs/geobrix)
[![documentation](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://databrickslabs.github.io/geobrix/)
[![scala](https://img.shields.io/badge/scala-2.13-red.svg)](https://www.scala-lang.org/)
[![python](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)
[![license](https://img.shields.io/badge/license-Databricks-blue.svg)](LICENSE)

**GeoBrix** is a high-performance spatial library for Databricks that fills the gaps around the platform's **native** spatial — raster, discrete global grids, and vector format I/O — and is built to drive you *deeper* into Databricks-native [`GEOMETRY`/`GEOGRAPHY` and ST/H3 functions](https://databrickslabs.github.io/geobrix/docs/databricks-spatial), not replace them. It is the modern successor to [DBLabs Mosaic](https://databrickslabs.github.io/mosaic/) (now in maintenance).

> **Full docs:** **https://databrickslabs.github.io/geobrix/** — this README is the 2-minute tour.

## Highlights

- **Lightweight tier (`pyrx`)** — pure Python on [rasterio](https://rasterio.readthedocs.io/)/[pyogrio](https://pyogrio.readthedocs.io/), **no JAR, no init script, no native GDAL bundle**. Runs on **Serverless**, standard (shared), Lakeflow pipelines, and **ARM** — where the heavyweight tier can't.
- **Heavyweight tier (`rasterx`)** — Scala + native GDAL for distributed processing on classic (x86) clusters. **Same function names across tiers** — switching is a one-line import change.
- **RasterX** — 100+ raster functions (the platform has no built-in raster): I/O & tiling, terrain, band math, focal, viewshed, vector↔raster, and raster→grid aggregation.
- **GridX** — discrete global grids beyond H3: **British National Grid**, CARTO **Quadbin**, and **custom** user-defined grids (indexing, neighbors, tessellation, polyfill, set ops).
- **VectorX** — augments native ST: Mapbox Vector Tile (**MVT**) encoding, **TIN** surface modeling, and legacy-Mosaic geometry migration.
- **Readers _and_ writers** — vector (GeoJSON/Shapefile/GeoPackage/FileGDB), raster (GDAL/GeoTIFF), and **PMTiles** — in both tiers (see the tables below).

See [benchmarks](https://databrickslabs.github.io/geobrix/docs/api/benchmarking) for light-vs-heavy timings.

## Quick start (lightweight)

Stage the wheel (a [Releases](https://github.com/databrickslabs/geobrix/releases) artifact, not on PyPI) in a Unity Catalog Volume, then install the `light` extra:

```python
%pip install '/Volumes/<catalog>/<schema>/<volume>/geobrix-<version>-py3-none-any.whl[light]'
```

```python
from databricks.labs.gbx.ds.register import register   # *_gbx readers/writers
from databricks.labs.gbx.pyrx import functions as rx    # gbx_rst_* functions

register(spark)
rx.register(spark)   # optional — only to call the gbx_rst_* SQL functions

# Read a GeoTIFF and compute with RasterX
rasters = spark.read.format("gtiff_gbx").load("/Volumes/<catalog>/<schema>/<volume>/*.tif")
rasters.select(rx.rst_width("tile"), rx.rst_srid("tile")).show()

# Vector read -> write (round-trips with the matching reader)
boroughs = spark.read.format("geojson_gbx").load("/Volumes/.../boroughs.geojson")
boroughs.write.format("geojson_gbx").mode("overwrite").save("/Volumes/.../out.geojson")
```

**Heavyweight** is the same code with `from databricks.labs.gbx.rasterx import functions as rx`, plus the JAR and a GDAL init script — see [Installing & Choosing a Tier](https://databrickslabs.github.io/geobrix/docs/api/execution-tiers).

## Readers & writers

Lightweight formats use the `*_gbx` suffix; heavyweight use `*_ogr` (vector) / `gdal` (raster). Light and heavy emit the **same schema**, so they are drop-in swaps. Full options and examples: [Readers](https://databrickslabs.github.io/geobrix/docs/readers/overview) · [Writers](https://databrickslabs.github.io/geobrix/docs/writers/overview).

**Raster & tiles**

| Format | Read (light / heavy) | Write (light / heavy) |
|---|---|---|
| Raster (any GDAL driver) | `raster_gbx` / `gdal` | `raster_gbx` / `gdal` |
| GeoTIFF | `gtiff_gbx` / `gtiff_gdal` | `gtiff_gbx` / `gtiff_gdal` |
| PMTiles | — | `pmtiles_gbx` / `pmtiles` |

**Vector** (heavyweight vector is read-only; use the light `*_gbx` writers for vector output)

| Format | Read (light / heavy) | Write (light) |
|---|---|---|
| Vector (any OGR driver) | `vector_gbx` / `ogr` | `vector_gbx` |
| Shapefile | `shapefile_gbx` / `shapefile_ogr` | `shapefile_gbx` |
| GeoJSON | `geojson_gbx` / `geojson_ogr` | `geojson_gbx` |
| GeoPackage | `gpkg_gbx` / `gpkg_ogr` | `gpkg_gbx` |
| File Geodatabase | `file_gdb_gbx` / `file_gdb_ogr` | `file_gdb_gbx` ¹ |

¹ FileGDB write requires a runtime whose GDAL build includes OpenFileGDB write support.

Light vector readers/writers exchange geometry as **WKB/WKT** with companion `*_srid` columns — convert to/from Databricks `GEOMETRY` with `st_geomfromwkb` / `st_aswkb` (see [Databricks Spatial](https://databrickslabs.github.io/geobrix/docs/databricks-spatial)).

## Packages

<img src="resources/images/RasterX.png" width="18%" /> <img src="resources/images/GridX.png" width="18%" /> <img src="resources/images/VectorX.png" width="18%" />

- **[RasterX](https://databrickslabs.github.io/geobrix/docs/api/raster-functions)** — raster I/O and analytics (gap-filling; the platform has no built-in raster).
- **[GridX](https://databrickslabs.github.io/geobrix/docs/api/gridx-functions)** — BNG, Quadbin, and custom grids (pairs with native H3 for global hex).
- **[VectorX](https://databrickslabs.github.io/geobrix/docs/api/vectorx-functions)** — MVT tiles, TIN surfaces, and legacy-geometry migration on top of native ST.

All SQL functions register with a `gbx_` prefix (e.g. `gbx_rst_clip`, `gbx_bng_cellarea`, `gbx_st_asmvt`) so usage is clearly attributable to GeoBrix on classic compute. Python/Scala bindings mirror the names — see [docs](https://databrickslabs.github.io/geobrix/).

<img src="resources/images/geobrix_vision.png" width="70%" />

## Background

Now that the platform's built-in [Spatial SQL functions](https://databrickslabs.github.io/geobrix/docs/databricks-spatial) (~100 ST + 35+ H3) reached public preview in DBR 17.1, GeoBrix delivers the next generation of *product-augmenting* capabilities — modernized from the popular [DBLabs Mosaic](https://databrickslabs.github.io/mosaic/) project to work with the [Data Intelligence Platform](https://www.databricks.com/product/data-intelligence-platform). Mosaic is in maintenance (targets DBR 13.3, retired at [EoS Aug 2026](https://docs.databricks.com/aws/en/release-notes/runtime/#supported-databricks-runtime-lts-releases)); GeoBrix is the path forward on modern runtimes and native spatial. See [Background](https://databrickslabs.github.io/geobrix/) for the full story.

## Known limitations

- Native Databricks `GEOMETRY`/`GEOGRAPHY` are not produced directly yet — geometries are exchanged as **WKB/WKT** (+ `*_srid`); convert with the native ST functions ([Databricks Spatial](https://databrickslabs.github.io/geobrix/docs/databricks-spatial)).
- Spatial KNN is not yet ported; nor is H3 for geometry-based k-ring / k-loop.

## Building, deploying, releasing

See the [`scripts`](./scripts) folder and the [docs](https://databrickslabs.github.io/geobrix/).

## Support

Databricks Labs projects are provided **AS-IS**, for exploration only, and are **not** covered by Databricks SLAs. Please file issues as [GitHub Issues](https://github.com/databrickslabs/geobrix/issues); they are reviewed as time permits. Do not file Databricks support tickets for these projects.
