"""Light-vs-heavy raster reader parity (Docker / integration).

Reads the SAME sample raster through the heavy Scala ``gdal`` reader and the
light pure-Python ``raster_gbx`` reader and asserts the swap-out contract:

- equal tile/row count,
- ``cellid == -1`` on every row (matches GDAL_Reader.scala),
- equal ``tile.metadata`` KEY-SET (values may legitimately differ),
- decoded pixel arrays equal within tolerance (NOT byte-for-byte — both tiers
  independently re-encode to GTiff via different GDAL builds).

Runs in Docker only: needs the geobrix JAR on the classpath (for ``gdal``) and
the sample data FUSE-mounted at ``/Volumes``. Marked ``integration`` so it is
excluded from the default (CI) run; execute with::

    bash scripts/commands/gbx-test-python.sh \
        --path python/geobrix/test/pyrx/ds/test_reader_parity.py \
        --with-integration --log reader-parity.log
"""

import logging
import os
from pathlib import Path

import numpy as np
import pytest
from rasterio.io import MemoryFile

pytestmark = pytest.mark.integration

# Real SRTM sample present under the FUSE-mounted Volume inside the dev
# container (16 KB → one tile, so row count is 1 for both tiers). Override with
# GBX_PARITY_SAMPLE to point at another raster.
SAMPLE = os.environ.get(
    "GBX_PARITY_SAMPLE",
    "/Volumes/main/geobrix_samples/geobrix-examples/london/elevation/srtm_n51w001.tif",
)

# Tolerances mirror bench/compare.py (light vs heavy never byte-equal).
REL_TOL = 1e-3
ABS_TOL = 1e-3

# JAR discovery mirrors python/geobrix/test/rasterx/test_pixel_ops.py.
_HERE = Path(__file__).resolve()
_LIBDIR = (_HERE.parents[3] / "lib").resolve()
_JAR_CANDIDATES = sorted(_LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))


@pytest.fixture(scope="module")
def spark_with_jar():
    """A SparkSession with the geobrix JAR on the classpath.

    The JAR makes the heavy ``gdal`` DataSource resolvable (auto-discovered via
    META-INF/services); the light ``raster_gbx`` DataSource is pure Python and
    is registered explicitly below.
    """
    if not _JAR_CANDIDATES:
        pytest.skip(f"no geobrix JAR in {_LIBDIR} (build/stage it first)")
    if not os.path.exists(SAMPLE):
        pytest.skip(f"sample raster not mounted at {SAMPLE}")

    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    jar = str(_JAR_CANDIDATES[-1])
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pyrx-ds-parity")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=ERROR,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", jar)
        .getOrCreate()
    )
    from databricks.labs.gbx.pyrx.ds.register import register

    register(session)
    yield session


def _decode(raster_bytes):
    with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as ds:
        return ds.read()  # (bands, h, w)


def test_raster_gbx_matches_gdal(spark_with_jar):
    heavy = spark_with_jar.read.format("gdal").load(SAMPLE).orderBy("source").collect()
    light = (
        spark_with_jar.read.format("raster_gbx")
        .load(SAMPLE)
        .orderBy("source")
        .collect()
    )

    assert len(light) == len(
        heavy
    ), f"tile/row count differs: light={len(light)} heavy={len(heavy)}"
    assert len(light) >= 1

    for h, l in zip(heavy, light):
        assert l["tile"]["cellid"] == -1
        assert h["tile"]["cellid"] == l["tile"]["cellid"]
        assert set(l["tile"]["metadata"].keys()) == set(h["tile"]["metadata"].keys())
        ha, la = _decode(h["tile"]["raster"]), _decode(l["tile"]["raster"])
        assert (
            ha.shape == la.shape
        ), f"tile pixel dims differ: light={la.shape} heavy={ha.shape}"
        np.testing.assert_allclose(la, ha, rtol=REL_TOL, atol=ABS_TOL)
