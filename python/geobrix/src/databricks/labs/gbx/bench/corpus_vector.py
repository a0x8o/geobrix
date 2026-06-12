"""Scaled vector benchmark corpus generator. Mints a 1M-polygon seed in the light
vector-writer schema, transcodes it to each format via the *_gbx writers, and
replicates each seed into a per-format directory on the bench Volume. Runs locally
(small scale) and on the bench cluster (full scale). FileGDB writing needs the
heavyweight GDAL natives (native osgeo) -- cluster only.

Shapefile and FileGDB seeds+copies are stored as self-contained zip archives
(.shp.zip / .gdb.zip) so both the light (*_gbx) and heavy (*_ogr) readers can read
a directory of copies -- the heavy OGR dir-read requires each entry to be a single
self-contained file."""

from __future__ import annotations

import os
import shutil
import tempfile
import zipfile
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


_EXT = {
    "geojson_gbx": "geojson",
    # shapefile_gbx and file_gdb_gbx produce .shp.zip / .gdb.zip after transcode_vector_seed
    # zips the raw writer output; the _EXT values below are the intermediate extensions
    # produced by the *_gbx writers before zipping.
    "shapefile_gbx": "shp",
    "gpkg_gbx": "gpkg",
    "file_gdb_gbx": "gdb",
    "vector_gbx": "geojson",
}


def _zip_shapefile(seed_dir: str, stem: str) -> str:
    """Zip the shapefile component files (seed.*) from seed_dir into seed_dir/stem.shp.zip.
    The archive is flat: each component sits at the zip root (no subdirectory), matching
    what /vsizip/…/seed.shp.zip expects for ESRI Shapefile.

    The zip is built on driver-local disk then sequential-copied to the target: UC Volumes
    are object storage, and ``zipfile.close()`` seeks back to write the central directory,
    which fails on a FUSE mount (``OSError: [Errno 5]``).  Removes the loose component files
    after.  Returns the zip path."""
    zip_path = os.path.join(seed_dir, f"{stem}.shp.zip")
    components = [
        n
        for n in os.listdir(seed_dir)
        if n.startswith(stem + ".") and not n.endswith(".zip")
    ]
    local_dir = tempfile.mkdtemp(prefix="gbx_zipshp_")
    try:
        local_zip = os.path.join(local_dir, f"{stem}.shp.zip")
        with zipfile.ZipFile(local_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for name in components:
                zf.write(os.path.join(seed_dir, name), arcname=name)
        shutil.copy(local_zip, zip_path)  # sequential -> FUSE-safe
    finally:
        shutil.rmtree(local_dir, ignore_errors=True)
    for name in components:
        os.remove(os.path.join(seed_dir, name))
    return zip_path


def _zip_gdb(gdb_path: str) -> str:
    """Zip seed.gdb/ into seed.gdb.zip such that the archive contains the seed.gdb/
    directory at its root (arcname = seed.gdb/<relpath>).  /vsizip/…/seed.gdb.zip then
    exposes the .gdb for OpenFileGDB.

    Built on driver-local disk then sequential-copied to the target (FUSE-safe -- see
    _zip_shapefile).  Removes the original .gdb directory after.  Returns the zip path."""
    gdb_name = os.path.basename(gdb_path.rstrip("/"))  # e.g. "seed.gdb"
    zip_path = gdb_path + ".zip"
    local_dir = tempfile.mkdtemp(prefix="gbx_zipgdb_")
    try:
        local_zip = os.path.join(local_dir, gdb_name + ".zip")
        with zipfile.ZipFile(local_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for dirpath, _dirnames, filenames in os.walk(gdb_path):
                for fname in filenames:
                    full = os.path.join(dirpath, fname)
                    zf.write(
                        full, arcname=os.path.join(gdb_name, os.path.relpath(full, gdb_path))
                    )
        shutil.copy(local_zip, zip_path)  # sequential -> FUSE-safe
    finally:
        shutil.rmtree(local_dir, ignore_errors=True)
    shutil.rmtree(gdb_path)
    return zip_path


def transcode_vector_seed(spark, seed_df, formats: List[str], out_base: str) -> dict:
    """Write the seed DataFrame to each format's seed file via the *_gbx writers.
    Returns {fmt: seed_path}. The seed is cached so each write reuses it. FileGDB
    requires the native osgeo (heavyweight GDAL natives).

    Shapefile and FileGDB outputs are zipped into self-contained archives (.shp.zip
    and .gdb.zip respectively) so both the light and heavy readers can dir-read a
    directory of copies -- the heavy OGR dir-read needs each entry to be one file."""
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
        if fmt == "shapefile_gbx":
            seed_dir = os.path.dirname(path)
            path = _zip_shapefile(seed_dir, "seed")
        elif fmt == "file_gdb_gbx" and os.path.isdir(path):
            path = _zip_gdb(path)
        out[fmt] = path
    return out


def replicate_vector_seed(seed_path: str, n_copies: int, copies_dir: str) -> List[str]:
    """Copy the per-format seed n_copies times into copies_dir as copy_<i>.<ext>.
    Sequential copies (FUSE-safe). Returns the copy paths.

    Seeds for shapefile_gbx / file_gdb_gbx are single .shp.zip / .gdb.zip archives
    produced by transcode_vector_seed, so every format is a single file copy.  A bare
    .gdb directory (non-zipped) is accepted as a fallback and tree-copied."""
    os.makedirs(copies_dir, exist_ok=True)
    base = os.path.basename(seed_path.rstrip("/"))
    # Preserve the full extension after the first dot (e.g. "shp.zip", "gdb.zip",
    # "geojson", "gpkg") so copy_0.shp.zip / copy_0.gdb.zip are named correctly.
    dot = base.find(".")
    ext = base[dot + 1:] if dot != -1 else ""
    paths: List[str] = []
    for i in range(n_copies):
        dst = os.path.join(copies_dir, f"copy_{i}.{ext}" if ext else f"copy_{i}")
        if os.path.isdir(seed_path):  # fallback: bare .gdb directory (non-zipped)
            shutil.copytree(seed_path, dst, dirs_exist_ok=True)
        else:
            shutil.copy(seed_path, dst)
        paths.append(dst)
    return paths


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
