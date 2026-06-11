"""pyrx.ds — pure-Python/PySpark DataSource V2 raster readers + writer.

Light-tier swap-out for the GDAL-backed Scala readers. See
docs/superpowers/specs/2026-06-11-light-readers-raster-design.md.
"""
from databricks.labs.gbx.pyrx.ds import register  # noqa: E402,F401
from databricks.labs.gbx.pyrx.ds.register import _try_register_on_import

_try_register_on_import()
