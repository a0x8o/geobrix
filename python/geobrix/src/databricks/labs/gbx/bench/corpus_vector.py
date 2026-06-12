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
