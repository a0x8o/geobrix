"""Shared helpers for heavy-tier rasterx Python tests.

``rst_fromfile`` is **lightweight-only** (issue #34): ``rasterx.functions.rst_fromfile``
(and SQL ``gbx_rst_fromfile``) delegate to the ``pyrx`` Python ``pandas_udf``, which imports
pandas/rasterio. The heavyweight CI env has no pandas/rasterio, so heavy-tier tests must not
load tiles via ``rst_fromfile``.

These helpers load a **local** test raster by reading its bytes and decoding via the
heavy-native Scala/GDAL ``gbx_rst_fromcontent`` -- no pandas/rasterio.
"""


def read_bytes(path):
    """Read the raw bytes of a local file (for building a content column)."""
    with open(path, "rb") as fh:
        return fh.read()


def tile_from_path(rx, f, path, driver="GTiff"):
    """Heavy-native drop-in for ``rx.rst_fromfile(f.lit(path), f.lit(driver))``.

    ``rst_fromfile`` is lightweight-only (issue #34, pyrx UDF); the heavy tier loads a
    LOCAL test raster by reading its bytes and decoding via Scala/GDAL
    ``gbx_rst_fromcontent`` -- no pandas/rasterio. Returns a tile Column.
    """
    return rx.rst_fromcontent(f.lit(read_bytes(path)), f.lit(driver))
