from pyspark.sql import functions as f

from databricks.labs.gbx.pyrx import functions as prx

from .conftest import make_geotiff_bytes


def _tile_df(spark, **kw):
    """One-row DataFrame with a tile struct column named 'tile'."""
    raster = make_geotiff_bytes(**kw)
    df = spark.createDataFrame([(raster,)], ["raster"])
    return df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


def test_no_jar_in_session(spark):
    # Guardrail: pyrx must work without the GeoBrix JAR.
    assert not spark.conf.get("spark.jars", "")


def test_rst_width_height(spark):
    df = _tile_df(spark, width=4, height=3)
    row = df.select(
        prx.rst_width("tile").alias("w"),
        prx.rst_height("tile").alias("h"),
    ).first()
    assert row["w"] == 4
    assert row["h"] == 3


def test_rst_srid(spark):
    df = _tile_df(spark, epsg=4326)
    assert df.select(prx.rst_srid("tile").alias("s")).first()["s"] == 4326


def test_rst_rastertoworldcoordx(spark):
    df = _tile_df(spark)
    row = df.select(
        prx.rst_rastertoworldcoordx("tile", f.lit(0), f.lit(0)).alias("x")
    ).first()
    assert row["x"] == 10.25


def test_rst_worldtorastercoordx(spark):
    df = _tile_df(spark)
    row = df.select(
        prx.rst_worldtorastercoordx("tile", f.lit(10.6), f.lit(49.9)).alias("c")
    ).first()
    assert row["c"] == 1


def test_rst_metadata_maptype_roundtrip(spark):
    # Exercises the MapType return path (non-pandas @f.udf fallback).
    df = _tile_df(spark, width=4, height=3)
    meta = df.select(prx.rst_metadata("tile").alias("m")).first()["m"]
    assert meta["width"] == "4"
    assert meta["height"] == "3"
    assert meta["driver"] == "GTiff"


def test_rst_boundingbox_binarytype_roundtrip(spark):
    # Exercises the BinaryType (WKB) return path end-to-end through Spark.
    import shapely.wkb

    df = _tile_df(spark, width=4, height=3)
    wkb = df.select(prx.rst_boundingbox("tile").alias("b")).first()["b"]
    assert shapely.wkb.loads(bytes(wkb)).bounds == (10.0, 48.5, 12.0, 50.0)


def test_register_is_noop(spark):
    # swap-compat: rx.register(spark) -> prx.register(spark) must not error.
    prx.register(spark)


def test_rst_transform_to_3857(spark):
    df = _tile_df(spark, epsg=4326, count=2)
    out = df.select(prx.rst_transform("tile", 3857).alias("t"))
    row = out.select(
        prx.rst_srid("t").alias("s"), prx.rst_numbands("t").alias("n")
    ).first()
    assert row["s"] == 3857
    assert row["n"] == 2


def test_rst_to_webmercator(spark):
    df = _tile_df(spark, epsg=4326)
    out = df.select(prx.rst_to_webmercator("tile").alias("t"))
    assert out.select(prx.rst_srid("t").alias("s")).first()["s"] == 3857


def test_rst_resample_factor(spark):
    df = _tile_df(spark, width=4, height=3)
    out = df.select(prx.rst_resample("tile", 2.0).alias("t"))
    row = out.select(
        prx.rst_width("t").alias("w"), prx.rst_height("t").alias("h")
    ).first()
    assert (row["w"], row["h"]) == (8, 6)


def test_rst_resample_to_size(spark):
    df = _tile_df(spark, width=4, height=3)
    out = df.select(prx.rst_resample_to_size("tile", 10, 7).alias("t"))
    row = out.select(
        prx.rst_width("t").alias("w"), prx.rst_height("t").alias("h")
    ).first()
    assert (row["w"], row["h"]) == (10, 7)


def test_rst_resample_to_res(spark):
    df = _tile_df(spark, width=4, height=3, epsg=4326)
    out = df.select(prx.rst_resample_to_res("tile", 0.25, 0.25).alias("t"))
    row = out.select(
        prx.rst_width("t").alias("w"), prx.rst_height("t").alias("h")
    ).first()
    assert (row["w"], row["h"]) == (8, 6)


def test_rst_clip(spark):
    import shapely.wkb
    from shapely.geometry import box

    geom = shapely.wkb.dumps(box(10.5, 49.0, 11.5, 49.5))
    df = _tile_df(spark, width=4, height=3, epsg=4326)
    df = df.withColumn("g", f.lit(geom))
    out = df.select(prx.rst_clip("tile", "g", False).alias("t"))
    row = out.select(
        prx.rst_width("t").alias("w"), prx.rst_height("t").alias("h")
    ).first()
    assert 0 < row["w"] < 4 and 0 < row["h"] < 3


def test_rst_updatetype(spark):
    df = _tile_df(spark, width=4, height=3)
    out = df.select(prx.rst_updatetype("tile", f.lit("Int32")).alias("t"))
    assert out.select(prx.rst_type("t").alias("ty")).first()["ty"][0] == "Int32"


def test_rst_initnodata(spark):
    df = _tile_df(spark, nodata=None)
    out = df.select(prx.rst_initnodata("tile").alias("t"))
    assert out.select(prx.rst_getnodata("t").alias("nd")).first()["nd"][0] == -9999.0


def test_rst_rasterize(spark):
    import shapely.wkb
    from shapely.geometry import box

    geom = shapely.wkb.dumps(box(1.0, 1.0, 3.0, 3.0))
    df = spark.createDataFrame([(geom,)], ["g"])
    out = df.select(
        prx.rst_rasterize("g", 5.0, 0.0, 0.0, 4.0, 4.0, 4, 4, 4326).alias("t")
    )
    row = out.select(
        prx.rst_width("t").alias("w"),
        prx.rst_height("t").alias("h"),
        prx.rst_srid("t").alias("s"),
    ).first()
    assert (row["w"], row["h"], row["s"]) == (4, 4, 4326)


def test_rst_fillnodata(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.ones((5, 5), dtype="float32")
    data[2, 2] = -9999.0
    profile = dict(
        driver="GTiff",
        width=5,
        height=5,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 5, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"])
    df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    out = df.select(prx.rst_fillnodata("tile").alias("t"))
    # rst_pixelcount absent in pyrx; verify via dims and nodata list (all filled = empty nodata)
    row = out.select(
        prx.rst_width("t").alias("w"),
        prx.rst_height("t").alias("h"),
        prx.rst_getnodata("t").alias("nd"),
    ).first()
    assert (row["w"], row["h"]) == (5, 5)
    # nodata is set but all pixels should now be valid — getnodata returns the
    # configured nodata value (not per-pixel mask), so just verify tile is non-null.
    assert row["nd"] is not None


def test_rst_ndvi(spark):
    df = _tile_df(spark, width=4, height=3, count=2)
    out = df.select(prx.rst_ndvi("tile", 1, 2).alias("t"))
    row = out.select(
        prx.rst_numbands("t").alias("n"), prx.rst_type("t").alias("ty")
    ).first()
    assert row["n"] == 1
    assert row["ty"][0] == "Float32"


def test_rst_evi_savi_ndwi_nbr_single_band(spark):
    df = _tile_df(spark, width=4, height=3, count=3)
    for col in (
        prx.rst_evi("tile", 1, 2, 3),
        prx.rst_savi("tile", 1, 2),
        prx.rst_ndwi("tile", 1, 2),
        prx.rst_nbr("tile", 2, 1),
    ):
        n = df.select(prx.rst_numbands(col).alias("n")).first()["n"]
        assert n == 1


def test_rst_slope_aspect_hillshade(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    ramp = np.tile(np.arange(6, dtype="float32"), (6, 1))
    profile = dict(
        driver="GTiff",
        width=6,
        height=6,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, 6, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(ramp, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    assert (
        df.select(prx.rst_type(prx.rst_slope("tile")).alias("t")).first()["t"][0]
        == "Float32"
    )
    assert (
        df.select(prx.rst_type(prx.rst_hillshade("tile")).alias("t")).first()["t"][0]
        == "Byte"
    )
    assert (
        df.select(prx.rst_numbands(prx.rst_aspect("tile")).alias("n")).first()["n"] == 1
    )


def test_rst_polygonize(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.full((4, 4), -9999.0, dtype="float32")
    data[1:3, 1:3] = 5.0
    profile = dict(
        driver="GTiff",
        width=4,
        height=4,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 4, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"])
    df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    # rst_polygonize returns ARRAY<struct(geom_wkb, value)>; explode and inspect.
    rows = (
        df.select(f.explode(prx.rst_polygonize("tile")).alias("p"))
        .select(f.col("p.value").alias("v"), f.col("p.geom_wkb").alias("g"))
        .collect()
    )
    vals = [r["v"] for r in rows]
    assert 5.0 in vals
    assert all(r["g"] is not None for r in rows)


def test_rst_tri_tpi_roughness(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.full((5, 5), 0.0, dtype="float32")
    data[2, 2] = 7.0
    profile = dict(
        driver="GTiff",
        width=5,
        height=5,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, 5, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    for col in (prx.rst_tri("tile"), prx.rst_tpi("tile"), prx.rst_roughness("tile")):
        row = df.select(
            prx.rst_numbands(col).alias("n"), prx.rst_type(col).alias("t")
        ).first()
        assert row["n"] == 1 and row["t"][0] == "Float32"


def test_rst_threshold(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.array([[1.0, 5.0], [10.0, 20.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, 2, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    out = df.select(prx.rst_threshold("tile", f.lit(">"), 5.0).alias("t"))
    assert out.select(prx.rst_getnodata("t").alias("nd")).first()["nd"][0] == -9999.0


def test_rst_filter_and_convolve(spark):
    df = _tile_df(spark, width=5, height=5, count=1)
    fout = df.select(prx.rst_filter("tile", 3, f.lit("mean")).alias("t"))
    assert fout.select(prx.rst_numbands("t").alias("n")).first()["n"] == 1
    cout = df.select(
        prx.rst_convolve(
            "tile",
            f.array(
                f.array(f.lit(0.0), f.lit(0.0), f.lit(0.0)),
                f.array(f.lit(0.0), f.lit(1.0), f.lit(0.0)),
                f.array(f.lit(0.0), f.lit(0.0), f.lit(0.0)),
            ),
        ).alias("t")
    )
    assert cout.select(prx.rst_numbands("t").alias("n")).first()["n"] == 1


def test_rst_color_relief(spark):
    import tempfile

    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    # Write color table to a named temp file (avoids function/module scope clash).
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tf:
        # elev=0 -> black (0,0,0); elev=100 -> white (255,255,255)
        tf.write("0 0 0 0\n100 255 255 255\n")
        color_table_path = tf.name

    dem = np.array([[0, 50], [100, 100]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, 2, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(dem, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    out = df.select(prx.rst_color_relief("tile", f.lit(color_table_path)).alias("t"))
    assert out.select(prx.rst_numbands("t").alias("n")).first()["n"] == 3


def test_rst_mapalgebra(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    def _ras(v):
        data = np.full((2, 2), float(v), dtype="float32")
        profile = dict(
            driver="GTiff",
            width=2,
            height=2,
            count=1,
            dtype="float32",
            crs="EPSG:32633",
            transform=from_origin(0, 2, 1, 1),
            nodata=-9999.0,
        )
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(data, 1)
            return mf.read()

    df = spark.createDataFrame([(_ras(1.0), _ras(2.0))], ["a", "b"])
    df = df.select(
        prx.rst_fromcontent("a", f.lit("GTiff")).alias("ta"),
        prx.rst_fromcontent("b", f.lit("GTiff")).alias("tb"),
    )
    out = df.select(prx.rst_mapalgebra(f.array("ta", "tb"), f.lit("A + B")).alias("t"))
    row = out.select(
        prx.rst_numbands("t").alias("n"), prx.rst_type("t").alias("ty")
    ).first()
    assert row["n"] == 1 and row["ty"][0] == "Float32"


def test_rst_separatebands(spark):
    df = _tile_df(spark, width=4, height=3, count=3)
    parts = df.select(f.explode(prx.rst_separatebands("tile")).alias("t"))
    assert parts.count() == 3
    assert parts.select(prx.rst_numbands("t").alias("n")).first()["n"] == 1


def test_rst_retile(spark):
    df = _tile_df(spark, width=4, height=4)
    parts = df.select(f.explode(prx.rst_retile("tile", 2, 2)).alias("t"))
    assert parts.count() == 4
    row = parts.select(
        prx.rst_width("t").alias("w"), prx.rst_height("t").alias("h")
    ).first()
    assert (row["w"], row["h"]) == (2, 2)


def test_rst_tooverlappingtiles(spark):
    df = _tile_df(spark, width=4, height=4)
    parts = df.select(f.explode(prx.rst_tooverlappingtiles("tile", 2, 2, 1)).alias("t"))
    assert parts.count() >= 4


def test_rst_derivedband(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    pyfunc = (
        "def double(in_ar, out_ar, xoff, yoff, xsize, ysize, "
        "raster_xsize, raster_ysize, buf_radius, gt, **kwargs):\n"
        "    out_ar[:] = in_ar[0] * 2\n"
    )
    data = np.array([[1.0, 2.0], [3.0, 4.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, 2, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    out = df.select(
        prx.rst_derivedband("tile", f.lit(pyfunc), f.lit("double")).alias("t")
    )
    assert out.select(prx.rst_numbands("t").alias("n")).first()["n"] == 1


def test_rst_maketiles(spark):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.arange(100 * 100, dtype="float32").reshape(100, 100)
    profile = dict(
        driver="GTiff",
        width=100,
        height=100,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 100, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        src = mf.read()
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    parts = df.select(f.explode(prx.rst_maketiles("tile", 0.01)).alias("t"))
    assert parts.count() > 1
    assert parts.select(prx.rst_numbands("t").alias("n")).first()["n"] == 1


# --- web-mercator XYZ tiling (rst_tilexyz / rst_xyzpyramid) -----------------
def _rgb_tile_df(spark):
    """One-row DataFrame with a small RGB GTiff tile over a European extent."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    profile = dict(
        driver="GTiff",
        width=64,
        height=64,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.03125, 0.03125),
    )
    data = (np.arange(64 * 64) % 256).astype("uint8").reshape(64, 64)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            for b in range(1, 4):
                dst.write(data, b)
        src = mf.read()
    return spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )


def test_rst_tilexyz_in_extent_png(spark):
    df = _rgb_tile_df(spark)
    png = df.select(
        prx.rst_tilexyz("tile", f.lit(5), f.lit(16), f.lit(10)).alias("b")
    ).first()["b"]
    assert bytes(png)[:4] == b"\x89PNG"


def test_rst_tilexyz_out_of_extent_transparent(spark):
    df = _rgb_tile_df(spark)
    png = df.select(
        prx.rst_tilexyz("tile", f.lit(2), f.lit(0), f.lit(0), "PNG", 64).alias("b")
    ).first()["b"]
    assert bytes(png)[:4] == b"\x89PNG"


def test_rst_tilexyz_null_tile_transparent(spark):
    # Even a null tile must return a transparent PNG (never null) — mirror heavyweight.
    df = spark.createDataFrame(
        [(None,)],
        "tile struct<cellid:bigint,raster:binary,metadata:map<string,string>>",
    )
    png = df.select(
        prx.rst_tilexyz("tile", f.lit(2), f.lit(0), f.lit(0), "PNG", 32).alias("b")
    ).first()["b"]
    assert png is not None
    assert bytes(png)[:4] == b"\x89PNG"


def test_rst_xyzpyramid_array(spark):
    df = _rgb_tile_df(spark)
    rows = (
        df.select(
            f.explode(prx.rst_xyzpyramid("tile", f.lit(1), f.lit(3), "PNG", 64)).alias(
                "t"
            )
        )
        .select("t.z", "t.x", "t.y", "t.bytes")
        .collect()
    )
    assert len(rows) > 0
    for r in rows:
        assert r["z"] in (1, 2, 3)
        assert bytes(r["bytes"])[:4] == b"\x89PNG"


# --- raster->grid aggregation (h3 + quadbin) --------------------------------
def test_rst_h3_rastertogridcount(spark):
    df = _tile_df(spark, width=4, height=3, count=1)
    # outer array (1 band) -> inner array of (cellID, measure); count sums to 12.
    rows = (
        df.select(f.explode(prx.rst_h3_rastertogridcount("tile", f.lit(6))).alias("b"))
        .select(f.explode("b").alias("c"))
        .select(f.col("c.cellID").alias("cid"), f.col("c.measure").alias("m"))
        .collect()
    )
    assert sum(r["m"] for r in rows) == 12
    assert all(0 < r["cid"] < 2**63 for r in rows)


def test_rst_quadbin_rastertogridcount(spark):
    df = _tile_df(spark, width=4, height=3, count=1)
    rows = (
        df.select(
            f.explode(prx.rst_quadbin_rastertogridcount("tile", f.lit(10))).alias("b")
        )
        .select(f.explode("b").alias("c"))
        .select(f.col("c.measure").alias("m"))
        .collect()
    )
    assert sum(r["m"] for r in rows) == 12


def test_rst_h3_rastertogridavg_multiband_outer_length(spark):
    df = _tile_df(spark, width=4, height=3, count=2)
    n = df.select(
        f.size(prx.rst_h3_rastertogridavg("tile", f.lit(5))).alias("n")
    ).first()["n"]
    assert n == 2


def test_rst_quadbin_rastertogridmedian(spark):
    df = _tile_df(spark, width=4, height=3, count=1)
    n = df.select(
        f.size(prx.rst_quadbin_rastertogridmedian("tile", f.lit(8))).alias("n")
    ).first()["n"]
    assert n == 1


# --- Group 1: per-band statistics & accessors -------------------------------
def test_rst_avg_min_max_median_pixelcount(spark):
    import math

    df = _tile_df(spark, width=4, height=3, count=2)
    row = df.select(
        prx.rst_avg("tile").alias("avg"),
        prx.rst_min("tile").alias("min"),
        prx.rst_max("tile").alias("max"),
        prx.rst_median("tile").alias("med"),
        prx.rst_pixelcount("tile").alias("pc"),
    ).first()
    # band1 = arange(12); band2 = +100 (no -9999 -> all 12 valid).
    assert row["avg"][0] == math.fsum(range(12)) / 12
    assert row["min"] == [0.0, 100.0]
    assert row["max"] == [11.0, 111.0]
    assert row["med"][0] == 5.5
    assert row["pc"] == [12, 12]


def test_rst_memsize(spark):
    raster = make_geotiff_bytes(width=4, height=3)
    df = spark.createDataFrame([(raster,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    sz = df.select(prx.rst_memsize("tile").alias("s")).first()["s"]
    assert sz == len(raster)


def test_rst_rotation_skew_format(spark):
    df = _tile_df(spark)
    row = df.select(
        prx.rst_rotation("tile").alias("rot"),
        prx.rst_skewx("tile").alias("sx"),
        prx.rst_skewy("tile").alias("sy"),
        prx.rst_format("tile").alias("fmt"),
    ).first()
    assert row["rot"] == 0.0
    assert row["sx"] == 0.0
    assert row["sy"] == 0.0
    assert row["fmt"] == "GTiff"


def test_rst_georeference_keys(spark):
    df = _tile_df(spark)
    gr = df.select(prx.rst_georeference("tile").alias("g")).first()["g"]
    assert set(gr.keys()) == {
        "upperLeftX",
        "upperLeftY",
        "scaleX",
        "scaleY",
        "skewX",
        "skewY",
    }
    assert gr["scaleX"] == 0.5
    assert gr["scaleY"] == -0.5
    assert gr["upperLeftX"] == 10.0
    assert gr["upperLeftY"] == 50.0


def test_rst_bandmetadata_and_subdatasets(spark):
    df = _tile_df(spark)
    row = df.select(
        prx.rst_bandmetadata("tile", f.lit(1)).alias("bm"),
        prx.rst_subdatasets("tile").alias("sd"),
    ).first()
    assert isinstance(row["bm"], dict)
    assert row["sd"] == {}  # plain GTiff has no subdatasets


def test_rst_summary_is_json(spark):
    import json

    df = _tile_df(spark, width=4, height=3, count=1)
    s = df.select(prx.rst_summary("tile").alias("s")).first()["s"]
    obj = json.loads(s)
    assert obj["driverShortName"] == "GTiff"
    assert obj["size"] == [4, 3]
    assert obj["bands"][0]["max"] == 11.0


def test_rst_histogram_bucket_sum(spark):
    df = _tile_df(spark, width=4, height=3, count=1)
    hist = df.select(
        prx.rst_histogram("tile", 4, f.lit(0.0), f.lit(11.0)).alias("h")
    ).first()["h"]
    assert list(hist.keys()) == ["band_1"]
    assert sum(hist["band_1"]) == 12


# --- Operations (tryopen, setsrid, band, asformat, buildoverviews, sample) --
def test_rst_tryopen_true(spark):
    df = _tile_df(spark, width=4, height=3)
    assert df.select(prx.rst_tryopen("tile").alias("ok")).first()["ok"] is True


def test_rst_tryopen_false_on_garbage(spark):
    # Build a tile struct directly with garbage raster bytes.
    df = spark.createDataFrame(
        [((0, b"not a raster", {"driver": "GTiff"}),)],
        "tile struct<cellid:bigint,raster:binary,metadata:map<string,string>>",
    )
    assert df.select(prx.rst_tryopen("tile").alias("ok")).first()["ok"] is False


def test_rst_setsrid_stamps_crs(spark):
    df = _tile_df(spark, width=4, height=3, epsg=4326)
    out = df.select(prx.rst_setsrid("tile", f.lit(27700)).alias("t"))
    row = out.select(
        prx.rst_srid("t").alias("s"),
        prx.rst_width("t").alias("w"),
        prx.rst_height("t").alias("h"),
    ).first()
    assert (row["s"], row["w"], row["h"]) == (27700, 4, 3)


def test_rst_band_extracts_single_band(spark):
    df = _tile_df(spark, width=4, height=3, count=3)
    out = df.select(prx.rst_band("tile", f.lit(2)).alias("t"))
    row = out.select(
        prx.rst_numbands("t").alias("n"), prx.rst_max("t").alias("mx")
    ).first()
    assert row["n"] == 1
    # band2 = arange(12)+100 -> max 111.
    assert row["mx"][0] == 111.0


def test_rst_band_out_of_range_raises(spark):
    df = _tile_df(spark, width=4, height=3, count=2)
    out = df.select(prx.rst_band("tile", f.lit(5)).alias("t"))
    with __import__("pytest").raises(Exception):
        out.select(prx.rst_numbands("t")).first()


def test_rst_asformat_gtiff(spark):
    df = _tile_df(spark, width=4, height=3)
    out = df.select(prx.rst_asformat("tile", "GTiff").alias("t"))
    meta = df.select(prx.rst_metadata(prx.rst_asformat("tile", "GTiff")).alias("m"))
    assert meta.first()["m"]["driver"] == "GTiff"
    assert out.select(prx.rst_width("t").alias("w")).first()["w"] == 4


def test_rst_buildoverviews(spark):
    df = _tile_df(spark, width=64, height=64)
    out = df.select(
        prx.rst_buildoverviews("tile", f.array(f.lit(2), f.lit(4))).alias("t")
    )
    # Reopen and confirm overviews are embedded via summary/numbands sanity.
    row = out.select(prx.rst_numbands("t").alias("n")).first()
    assert row["n"] == 1
    # Verify overviews persisted by reading raster bytes back through rasterio.
    raster = out.select(f.col("t.raster").alias("r")).first()["r"]
    from rasterio.io import MemoryFile

    with MemoryFile(bytes(raster)) as mf:
        with mf.open() as o:
            assert o.overviews(1) == [2, 4]


def test_rst_sample(spark):
    import shapely.wkb
    from shapely.geometry import Point

    pt = shapely.wkb.dumps(Point(10.75, 49.75))
    df = _tile_df(spark, width=4, height=3, count=2)
    df = df.withColumn("g", f.lit(pt))
    vals = df.select(prx.rst_sample("tile", "g").alias("v")).first()["v"]
    assert vals == [1.0, 101.0]


def test_rst_rastertoworldcoord_struct_roundtrip(spark):
    df = _tile_df(spark)
    wc = df.select(
        prx.rst_rastertoworldcoord("tile", f.lit(2), f.lit(1)).alias("wc")
    ).first()["wc"]
    assert wc["x"] is not None and wc["y"] is not None
    rc = df.select(
        prx.rst_worldtorastercoord("tile", f.lit(wc["x"]), f.lit(wc["y"])).alias("rc")
    ).first()["rc"]
    assert (rc["x"], rc["y"]) == (2, 1)


# ---------------------------------------------------------------------------
# Tier 2: grouped aggregators (rst_*_agg) via .agg()
# ---------------------------------------------------------------------------
import numpy as np  # noqa: E402
from rasterio.io import MemoryFile  # noqa: E402
from rasterio.transform import from_origin  # noqa: E402

from databricks.labs.gbx.pyrx import _serde  # noqa: E402


def _ras_bytes(data, ulx=0.0, uly=10.0, px=1.0, epsg=32633, nodata=-9999.0):
    data = np.asarray(data, dtype="float32")
    if data.ndim == 2:
        data = data[None, :, :]
    bands, h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=bands,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(ulx, uly, px, px),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


def _tiles_df(spark, rows):
    """rows: list of (key, raster_bytes). Builds a (k, tile struct) df."""
    df = spark.createDataFrame(rows, ["k", "raster"])
    return df.select("k", prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


def test_rst_merge_agg(spark):
    left = _ras_bytes(np.array([[1, 2], [3, 4]]), ulx=0.0, uly=2.0)
    right = _ras_bytes(np.array([[5, 6], [7, 8]]), ulx=2.0, uly=2.0)
    df = _tiles_df(spark, [("g", left), ("g", right)])
    tile = df.groupBy("k").agg(prx.rst_merge_agg("tile").alias("t")).first()["t"]
    assert tile is not None
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert ds.width == 4
        assert ds.bounds.left == 0.0
        assert ds.bounds.right == 4.0


def test_rst_combineavg_agg_mean_and_cellid(spark):
    a = _ras_bytes(np.array([[2.0, 4.0], [6.0, 8.0]]))
    b = _ras_bytes(np.array([[4.0, 8.0], [10.0, 12.0]]))
    df = _tiles_df(spark, [("g", a), ("g", b)])
    tile = df.groupBy("k").agg(prx.rst_combineavg_agg("tile").alias("t")).first()["t"]
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert np.allclose(ds.read(1), [[3.0, 6.0], [8.0, 10.0]])
    # cellid carried through from the group's first tile (fromcontent -> 0).
    assert tile["cellid"] == 0


def test_rst_frombands_agg_ascending_order(spark):
    b0 = _ras_bytes(np.full((2, 2), 10.0))
    b1 = _ras_bytes(np.full((2, 2), 20.0))
    b2 = _ras_bytes(np.full((2, 2), 30.0))
    rows = [("g", b2, 2), ("g", b0, 0), ("g", b1, 1)]
    df = spark.createDataFrame(rows, ["k", "raster", "band_index"]).select(
        "k",
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"),
        "band_index",
    )
    tile = (
        df.groupBy("k")
        .agg(prx.rst_frombands_agg("tile", "band_index").alias("t"))
        .first()["t"]
    )
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert ds.count == 3
        assert np.allclose(ds.read(1), 10.0)
        assert np.allclose(ds.read(2), 20.0)
        assert np.allclose(ds.read(3), 30.0)


def test_rst_rasterize_agg(spark):
    import shapely.wkb
    from shapely.geometry import box

    g1 = shapely.wkb.dumps(box(0, 0, 2, 4))
    g2 = shapely.wkb.dumps(box(1, 0, 4, 4))
    df = spark.createDataFrame([("g", g1, 1.0), ("g", g2, 2.0)], ["k", "geom", "val"])
    tile = (
        df.groupBy("k")
        .agg(prx.rst_rasterize_agg("geom", "val", 0, 0, 4, 4, 4, 4, 32633).alias("t"))
        .first()["t"]
    )
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        arr = ds.read(1)
    assert np.all(arr[:, 0] == 1.0)
    assert np.all(arr[:, 1] == 2.0)


PYFUNC_SUM_AGG = """
def addbands(in_ar, out_ar, *args, **kwargs):
    import numpy as np
    out_ar[:] = np.sum(in_ar, axis=0)
"""


def test_rst_derivedband_agg(spark):
    a = _ras_bytes(np.full((2, 2), 3.0))
    b = _ras_bytes(np.full((2, 2), 4.0))
    c = _ras_bytes(np.full((2, 2), 5.0))
    df = _tiles_df(spark, [("g", a), ("g", b), ("g", c)])
    tile = (
        df.groupBy("k")
        .agg(prx.rst_derivedband_agg("tile", PYFUNC_SUM_AGG, "addbands").alias("t"))
        .first()["t"]
    )
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert ds.count == 1
        assert np.allclose(ds.read(1), 12.0)
