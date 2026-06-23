"""gbx.viz — tier-agnostic visualization helpers (requires the [viz] extra).

Raster rendering (plot_raster / plot_file) and Spark DataFrame -> GeoDataFrame
adapters (as_gdf / cells_as_gdf) for interactive maps. Install with
``pip install 'geobrix[viz]'``.
"""

from databricks.labs.gbx.viz._raster import plot_file, plot_raster
from databricks.labs.gbx.viz._vector import as_gdf, cells_as_gdf

__all__ = ["plot_raster", "plot_file", "as_gdf", "cells_as_gdf"]
