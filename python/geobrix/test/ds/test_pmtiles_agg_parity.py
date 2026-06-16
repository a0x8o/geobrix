"""Cross-tier decoded parity for gbx_pmtiles_agg (light vs heavy). JAR-gated."""

import contextlib
from pathlib import Path

import pytest
from pmtiles import tile as _pmtile
from pmtiles.reader import MmapSource, Reader
from pmtiles.tile import Compression

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

# Two MVT-ish payloads + one POLYGON-ish payload across two zooms.
_TILES = [
    ("g", b"tile-point-0\x00", 3, 2, 4),
    ("g", b"tile-point-1\x00", 3, 5, 6),
    ("g", b"tile-polygon-\x07\x08\x09", 2, 1, 1),
]


@contextlib.contextmanager
def _internal_compression_aware(reader):
    """Honor the archive's ``internal_compression`` when reading directories.

    The stock ``pmtiles`` ``deserialize_directory`` unconditionally
    ``gzip.decompress``-es the directory bytes. The heavy (Scala) writer emits
    ``internal_compression=NONE`` directories (uncompressed varint stream), which
    that hardcoded path can't read. The light writer forces GZIP. To compare both
    tiers we make the decoder respect the header: pass directory bytes through
    untouched when the header says NONE, and gzip-decompress otherwise.
    """
    if reader.header()["internal_compression"] == Compression.NONE:
        orig = _pmtile.gzip.decompress
        _pmtile.gzip.decompress = lambda b: b
        try:
            yield
        finally:
            _pmtile.gzip.decompress = orig
    else:
        yield


def _decode(path):
    out = {}
    with open(path, "rb") as f:
        r = Reader(MmapSource(f))
        with _internal_compression_aware(r):
            for z in range(0, 8):
                n = 2**z
                for x in range(n):
                    for y in range(n):
                        t = r.get(z, x, y)
                        if t is not None:
                            out[(z, x, y)] = t
    return out


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip("no geobrix JAR staged")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("pmtiles-agg-parity")
        .config("spark.jars", str(_JARS[0]))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


def _archive(spark, register_fn, tmp_path, name):
    register_fn(spark)
    from databricks.labs.gbx.pmtiles import functions as pt

    df = spark.createDataFrame(_TILES, ["g", "tile", "z", "x", "y"])
    blob = (
        df.groupBy("g")
        .agg(pt.pmtiles_agg("tile", "z", "x", "y").alias("arc"))
        .collect()[0]["arc"]
    )
    p = tmp_path / f"{name}.pmtiles"
    p.write_bytes(blob)
    return _decode(p)


def test_decoded_tile_parity(spark_with_jar, tmp_path):
    from databricks.labs.gbx.pmtiles import functions as heavy_pt
    from databricks.labs.gbx.pmtiles._agg_light import register_pmtiles_agg

    light_tiles = _archive(spark_with_jar, register_pmtiles_agg, tmp_path, "light")
    heavy_tiles = _archive(spark_with_jar, heavy_pt.register, tmp_path, "heavy")
    assert light_tiles == heavy_tiles
