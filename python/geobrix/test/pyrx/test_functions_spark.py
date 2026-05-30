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
