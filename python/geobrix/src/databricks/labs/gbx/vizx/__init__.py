"""gbx.vizx — tier-agnostic visualization helpers (requires the [vizx] extra).

Raster rendering (plot_raster / plot_file), static and interactive map
rendering (plot_static / plot_interactive), and Spark DataFrame ->
GeoDataFrame adapters (as_gdf / cells_as_gdf). Install with
``pip install 'geobrix[vizx]'``.
"""

from databricks.labs.gbx.vizx._cog import plot_cog
from databricks.labs.gbx.vizx._interactive import plot_interactive
from databricks.labs.gbx.vizx._pmtiles import plot_pmtiles
from databricks.labs.gbx.vizx._raster import plot_file, plot_mask_layers, plot_raster
from databricks.labs.gbx.vizx._static_map import plot_static
from databricks.labs.gbx.vizx._vector import as_gdf, cells_as_gdf, grid_as_gdf

__all__ = [
    "plot_raster",
    "plot_file",
    "plot_mask_layers",
    "plot_static",
    "plot_interactive",
    "plot_pmtiles",
    "plot_cog",
    "as_gdf",
    "cells_as_gdf",
    "grid_as_gdf",
]
