"""End-to-end tests for the PMTiles Python bindings.

Covers:
  - Registration via ``register_ds``.
  - UDAF path: ``pmtiles_agg`` returns a valid PMTile v3 binary blob.
  - DataSource path: ``df.write.format("pmtiles").mode("overwrite").save(path)``
    produces a single ``.pmtiles`` file with the correct header.
"""

import logging
import struct
import tempfile
import uuid
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()
JAR_URI = JAR.as_uri()


@pytest.fixture(scope="session")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark = (
        SparkSession.builder.config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=INFO,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    return spark


@pytest.fixture(scope="session")
def pmtiles_registered(spark):
    """Register PMTiles functions once for all tests."""
    from databricks.labs.gbx.pmtiles import functions as px

    px.register(spark)
    return px


def _read_addressed_tiles(pmt_bytes: bytes) -> int:
    """Decode the uint64 LE at offset 72 = number of addressed tiles (spec § 3.1)."""
    return struct.unpack_from("<Q", pmt_bytes, 72)[0]


def _read_tile_type(pmt_bytes: bytes) -> int:
    """Decode the byte at offset 99 = tile_type enum (1=MVT, 2=PNG, 3=JPEG, 4=WEBP)."""
    return pmt_bytes[99]


def test_pmtiles_agg_blob_metadata_and_png_detect(spark, pmtiles_registered):
    """UDAF round-trip: valid blob + addressed count + metadata round-trip + PNG auto-detect."""
    # 4-tile pyramid with no metadata.
    tiles = [
        (1, 0, 0, b"tile_00"),
        (1, 0, 1, b"tile_01"),
        (1, 1, 0, b"tile_10"),
        (1, 1, 1, b"tile_11"),
    ]
    df = spark.createDataFrame(tiles, schema=["z", "x", "y", "bytes"])
    pmt = df.agg(
        pmtiles_registered.pmtiles_agg(
            f.col("bytes"), f.col("z"), f.col("x"), f.col("y")
        ).alias("pmt")
    ).collect()[0]["pmt"]
    assert pmt is not None and pmt[:7] == b"PMTiles" and pmt[7] == 3
    assert _read_addressed_tiles(pmt) == 4

    # Single tile with metadata JSON — verify round-trip.
    df_meta = spark.createDataFrame([(1, 0, 0, b"X")], schema=["z", "x", "y", "bytes"])
    pmt_meta = df_meta.agg(
        pmtiles_registered.pmtiles_agg(
            f.col("bytes"),
            f.col("z"),
            f.col("x"),
            f.col("y"),
            f.lit('{"name":"pytest"}'),
        ).alias("pmt")
    ).collect()[0]["pmt"]
    meta_off = struct.unpack_from("<Q", pmt_meta, 24)[0]
    meta_len = struct.unpack_from("<Q", pmt_meta, 32)[0]
    assert (
        pmt_meta[meta_off : meta_off + meta_len].decode("utf-8") == '{"name":"pytest"}'
    )

    # PNG magic auto-detect (tile_type byte at offset 99 = 2).
    png_magic = bytes([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00])
    df_png = spark.createDataFrame(
        [(1, 0, 0, png_magic)], schema=["z", "x", "y", "bytes"]
    )
    pmt_png = df_png.agg(
        pmtiles_registered.pmtiles_agg(
            f.col("bytes"), f.col("z"), f.col("x"), f.col("y")
        ).alias("pmt")
    ).collect()[0]["pmt"]
    assert _read_tile_type(pmt_png) == 2  # PNG


def test_pmtiles_datasource_write(spark, pmtiles_registered):
    """The .write.format("pmtiles") DataSource should produce a single file."""
    tiles = [
        (2, x, y, f"tile_{x}_{y}".encode("utf-8")) for x in range(3) for y in range(3)
    ]
    df = spark.createDataFrame(tiles, schema=["z", "x", "y", "bytes"]).repartition(
        2, f.col("x"), f.col("y")
    )

    with tempfile.TemporaryDirectory(prefix="pmtiles-py-test-") as tmp:
        out_path = f"{tmp}/out-{uuid.uuid4()}.pmtiles"
        df.write.format("pmtiles").mode("overwrite").save(out_path)
        # Read back as bytes and verify header.
        with open(out_path, "rb") as fh:
            blob = fh.read()
        assert blob[:7] == b"PMTiles"
        assert blob[7] == 3
        assert _read_addressed_tiles(blob) == 9
        # No leftover scratch files.
        leftovers = [p.name for p in Path(tmp).iterdir() if p.name.startswith("_part_")]
        assert not leftovers, f"scratch files left behind: {leftovers}"


def test_pmtiles_read_not_supported(spark, pmtiles_registered):
    """Reading a PMTile should surface a clear 'not supported' message, not class-not-found."""
    with pytest.raises(Exception) as exc_info:
        spark.read.format("pmtiles").load("/tmp/does-not-matter").collect()
    msg = str(exc_info.value)
    assert "Reading PMTiles archives is not supported" in msg, f"got: {msg}"
