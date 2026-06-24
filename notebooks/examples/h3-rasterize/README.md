# H3 Cell Rasterize + Band Stacking Demo

This example notebook (`h3_rasterize_demo.ipynb`) demonstrates the complete
H3-cell rasterization pipeline on real elevation data, starting from a DEM and
producing a multi-band GeoTIFF stack.

## What the notebook demonstrates

1. **DEM isobands** — the SRTM tile `srtm_n40w073.tif` (New York area,
   EPSG:4326) is read with rasterio and quantized into elevation isobands
   (every 100 m).  Each isoband produces a filled polygon — analogous to a
   signal-strength contour in a wireless-coverage pipeline.

2. **H3 polyfill** — each band polygon is converted to H3 hexagonal cells at
   resolution 8 via `h3.polygon_to_cells`.  Cell IDs are stored as 64-bit
   integers for PySpark compatibility.

3. **Shared canvas** — `rx.rst_h3_gridspec` computes a single pixel-snapped
   bounding box that covers all H3 cells across all band levels.  Using a
   shared canvas ensures every per-band tile is pixel-aligned before stacking.

4. **Per-band rasterize** — `rx.rst_h3_rasterize_agg` burns each band's cells
   onto the shared canvas, producing one presence-mask tile per elevation
   threshold.

5. **Stack** — `rx.rst_frombands_agg` assembles the per-band tiles into a
   single multi-band GeoTIFF ordered by band level (lowest threshold = band 1).

6. **Visualization** — `plot_raster` renders the stacked raster as a
   false-colour composite, confirming the pipeline reconstructs the terrain.

## Telco / coverage-analysis analogy

| DEM demo | Coverage-analysis equivalent |
|---|---|
| Elevation isoband polygon | Signal contour polygon (e.g. −80 dBm zone) |
| `band_level` (integer) | Threshold index (tier 1 / tier 2 / …) |
| H3 polyfill at res 8 | H3 coverage cells per threshold |
| Multi-band stacked tile | Multi-threshold stacked coverage raster |

The stacked tile is ready for downstream analysis — for example, joining against
subscriber locations or exporting as PMTiles for web visualization.

## Requirements

- **Databricks cluster or Serverless compute** — the notebook targets the
  GeoBrix lightweight tier (`geobrix[light,viz]`) and runs on Serverless.
  No JAR or GDAL init script is required.

- **GeoBrix sample-data Volume** — the DEM is read from
  `/Volumes/main/geobrix_samples/geobrix-examples/nyc/elevation/srtm_n40w073.tif`.
  Run `gbx:data:download --bundle essential` to populate the Volume if it is
  empty, or adjust `DEM_PATH` in the notebook to point at your own SRTM tile.

- **Wheel** — update the `%pip install` cell to point at your staged
  `geobrix-0.4.0-py3-none-any.whl` if the path differs from the default
  `/Volumes/main/geobrix_samples/sample-data/`.

## Running the notebook

1. Open the notebook in Databricks.
2. Attach to a Serverless or single-node cluster (no GPU, no GDAL init script).
3. Run all cells in order.  The `%pip install` cell restarts the Python kernel;
   subsequent cells import from the freshly installed wheel.
4. Outputs are empty until executed on a cluster — cell outputs in the committed
   file are intentionally blank.

## Related resources

- [GeoBrix RasterX API](https://databrickslabs.github.io/geobrix/docs/api/rasterx)
- [EO-Series notebooks](../eo-series/) — STAC download, band stacking, clipping
