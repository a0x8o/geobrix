"""pyrx — lightweight, JAR-free raster API (PySpark + rasterio).

Pure-Python/PySpark sibling of the heavyweight ``rasterx`` package. Function
names and signatures mirror ``rasterx`` exactly so swapping the import is a
one-line upgrade/downgrade:

    from databricks.labs.gbx.rasterx import functions as rx   # heavyweight (JAR)
    from databricks.labs.gbx.pyrx import functions as prx     # lightweight (no JAR)
"""

from databricks.labs.gbx.pyrx._env import assert_rasterio_available, configure_gdal_env

# Configure the bundled GDAL/PROJ env on import (driver side). Worker processes
# call configure_gdal_env() again inside each UDF body.
configure_gdal_env()

__all__ = ["assert_rasterio_available", "configure_gdal_env"]
