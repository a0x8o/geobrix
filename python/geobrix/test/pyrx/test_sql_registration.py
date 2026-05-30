"""Tests for explicit SQL registration of gbx_rst_* via prx.register(spark).

register(spark) should install the same SQL names the heavyweight rasterx
package uses, but bound to the pyspark/rasterio UDFs — so
``spark.sql("SELECT gbx_rst_width(tile) ...")`` works identically whether
the user registered the heavyweight (Scala) or lightweight (pyrx) package.
"""

from pyspark.sql import functions as f

from databricks.labs.gbx.pyrx import functions as prx

from .conftest import make_geotiff_bytes


def _tile_view(spark, name="t", **kw):
    raster = make_geotiff_bytes(**kw)
    df = spark.createDataFrame([(raster,)], ["raster"])
    df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    df.createOrReplaceTempView(name)


def test_register_enables_scalar_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=4, height=3, epsg=4326)
    row = spark.sql(
        "SELECT gbx_rst_width(tile) AS w, gbx_rst_height(tile) AS h, gbx_rst_srid(tile) AS s FROM t"
    ).first()
    assert (row["w"], row["h"], row["s"]) == (4, 3, 4326)


def test_register_enables_coord_sql(spark):
    prx.register(spark)
    _tile_view(spark, epsg=4326)
    row = spark.sql(
        "SELECT gbx_rst_worldtorastercoordx(tile, 10.6, 49.9) AS c FROM t"
    ).first()
    assert row["c"] == 1


def test_register_enables_tile_returning_sql_composition(spark):
    prx.register(spark)
    _tile_view(spark, epsg=4326)
    # reproject in SQL, then read its SRID in SQL — composes like heavyweight.
    row = spark.sql(
        "SELECT gbx_rst_srid(gbx_rst_transform(tile, 3857)) AS s FROM t"
    ).first()
    assert row["s"] == 3857


def test_register_enables_polygonize_sql(spark):
    prx.register(spark)
    _tile_view(spark, epsg=4326)
    # polygonize SQL UDF requires explicit band and connectedness args (no SQL defaults).
    n = spark.sql("SELECT size(gbx_rst_polygonize(tile, 1, 4)) AS n FROM t").first()[
        "n"
    ]
    assert n >= 1


def _rgb_tile_view(spark, name="t"):
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
    df = spark.createDataFrame([(src,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView(name)


def test_register_enables_tilexyz_sql(spark):
    prx.register(spark)
    _rgb_tile_view(spark)
    # SQL callers must pass all args (no Python defaults in SQL UDFs).
    png = spark.sql(
        "SELECT gbx_rst_tilexyz(tile, 5, 16, 10, 'PNG', 256, 'bilinear') AS b FROM t"
    ).first()["b"]
    assert bytes(png)[:4] == b"\x89PNG"


def test_register_enables_xyzpyramid_sql(spark):
    prx.register(spark)
    _rgb_tile_view(spark)
    n = spark.sql(
        "SELECT size(gbx_rst_xyzpyramid(tile, 1, 2, 'PNG', 64, 'bilinear')) AS n FROM t"
    ).first()["n"]
    assert n >= 1


def test_register_enables_h3_rastertogrid_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=4, height=3, epsg=4326)
    # nested ARRAY<ARRAY<struct>>: outer size == band count (1).
    n = spark.sql(
        "SELECT size(gbx_rst_h3_rastertogridcount(tile, 6)) AS n FROM t"
    ).first()["n"]
    assert n == 1


def test_register_enables_quadbin_rastertogrid_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=4, height=3, epsg=4326)
    # explode both levels and confirm counts sum to the 12 valid pixels.
    total = spark.sql(
        "SELECT sum(c.measure) AS total FROM t "
        "LATERAL VIEW explode(gbx_rst_quadbin_rastertogridcount(tile, 10)) AS b "
        "LATERAL VIEW explode(b) AS c"
    ).first()["total"]
    assert total == 12


# --- Operations via SQL -----------------------------------------------------
def test_register_enables_tryopen_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=4, height=3)
    ok = spark.sql("SELECT gbx_rst_tryopen(tile) AS ok FROM t").first()["ok"]
    assert ok is True


def test_register_enables_setsrid_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=4, height=3, epsg=4326)
    s = spark.sql(
        "SELECT gbx_rst_srid(gbx_rst_setsrid(tile, 27700)) AS s FROM t"
    ).first()["s"]
    assert s == 27700


def test_register_enables_band_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=4, height=3, count=3)
    n = spark.sql("SELECT gbx_rst_numbands(gbx_rst_band(tile, 2)) AS n FROM t").first()[
        "n"
    ]
    assert n == 1


def test_register_enables_asformat_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=4, height=3)
    drv = spark.sql(
        "SELECT gbx_rst_metadata(gbx_rst_asformat(tile, 'GTiff'))['driver'] AS d FROM t"
    ).first()["d"]
    assert drv == "GTiff"


def test_register_enables_buildoverviews_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=64, height=64)
    # SQL callers pass all args (no Python defaults in SQL UDFs).
    n = spark.sql(
        "SELECT gbx_rst_numbands(gbx_rst_buildoverviews(tile, array(2, 4), 'average')) "
        "AS n FROM t"
    ).first()["n"]
    assert n == 1


def test_register_enables_sample_sql(spark):
    import shapely.wkb
    from shapely.geometry import Point

    prx.register(spark)
    raster = make_geotiff_bytes(width=4, height=3, count=2)
    pt = shapely.wkb.dumps(Point(10.75, 49.75))
    df = spark.createDataFrame([(raster, pt)], ["raster", "g"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"), f.col("g")
    )
    df.createOrReplaceTempView("t")
    vals = spark.sql("SELECT gbx_rst_sample(tile, g) AS v FROM t").first()["v"]
    assert vals == [1.0, 101.0]


# --- Group 1: statistics & accessors via SQL --------------------------------
def test_register_enables_stats_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=4, height=3, count=1)
    row = spark.sql(
        "SELECT gbx_rst_avg(tile) AS avg, gbx_rst_min(tile) AS mn, "
        "gbx_rst_max(tile) AS mx, gbx_rst_pixelcount(tile) AS pc FROM t"
    ).first()
    assert row["mn"] == [0.0]
    assert row["mx"] == [11.0]
    assert row["pc"] == [12]


def test_register_enables_memsize_format_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=4, height=3)
    row = spark.sql(
        "SELECT gbx_rst_memsize(tile) AS sz, gbx_rst_format(tile) AS fmt FROM t"
    ).first()
    assert row["sz"] > 0
    assert row["fmt"] == "GTiff"


def test_register_enables_georeference_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=4, height=3)
    gr = spark.sql("SELECT gbx_rst_georeference(tile) AS g FROM t").first()["g"]
    assert gr["scaleX"] == 0.5
    assert gr["scaleY"] == -0.5


def test_register_enables_histogram_sql(spark):
    prx.register(spark)
    _tile_view(spark, width=4, height=3, count=1)
    # SQL callers pass all args (no Python defaults in SQL UDFs).
    hist = spark.sql(
        "SELECT gbx_rst_histogram(tile, 4, 0.0, 11.0, false) AS h FROM t"
    ).first()["h"]
    assert sum(hist["band_1"]) == 12


def test_register_enables_coord_structs_sql(spark):
    prx.register(spark)
    _tile_view(spark, epsg=4326)
    # round-trip pixel (2,1) through both struct coord UDFs.
    out = spark.sql(
        "SELECT gbx_rst_rastertoworldcoord(tile, 2, 1) AS wc FROM t"
    ).first()["wc"]
    assert out["x"] is not None and out["y"] is not None
    rc = spark.sql(
        f"SELECT gbx_rst_worldtorastercoord(tile, {out['x']}, {out['y']}) AS rc FROM t"
    ).first()["rc"]
    assert (rc["x"], rc["y"]) == (2, 1)


# --- Tier 2: grouped aggregators via SQL GROUP BY ---------------------------
def _agg_ras_bytes(data, ulx=0.0, uly=10.0, px=1.0, epsg=32633, nodata=-9999.0):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

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


def test_register_enables_merge_agg_sql(spark):
    import numpy as np

    prx.register(spark)
    left = _agg_ras_bytes(np.array([[1, 2], [3, 4]]), ulx=0.0, uly=2.0)
    right = _agg_ras_bytes(np.array([[5, 6], [7, 8]]), ulx=2.0, uly=2.0)
    df = spark.createDataFrame([("g", left), ("g", right)], ["k", "raster"]).select(
        "k", prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("v")
    # Grouped-agg returns BINARY in SQL; accepts the tile STRUCT column directly.
    raster = spark.sql("SELECT gbx_rst_merge_agg(tile) AS b FROM v GROUP BY k").first()[
        "b"
    ]
    assert raster is not None and len(bytes(raster)) > 0
    # Wrap with gbx_rst_fromcontent to recover a tile struct.
    w = spark.sql(
        "SELECT gbx_rst_width(gbx_rst_fromcontent(gbx_rst_merge_agg(tile), 'GTiff')) "
        "AS w FROM v GROUP BY k"
    ).first()["w"]
    assert w == 4


def test_register_enables_combineavg_agg_sql(spark):
    import numpy as np

    prx.register(spark)
    a = _agg_ras_bytes(np.array([[2.0, 4.0], [6.0, 8.0]]))
    b = _agg_ras_bytes(np.array([[4.0, 8.0], [10.0, 12.0]]))
    df = spark.createDataFrame([("g", a), ("g", b)], ["k", "raster"]).select(
        "k", prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("v")
    avg = spark.sql(
        "SELECT gbx_rst_avg(gbx_rst_fromcontent(gbx_rst_combineavg_agg(tile), 'GTiff')) "
        "AS a FROM v GROUP BY k"
    ).first()["a"]
    # mean of all pixels [[3,6],[8,10]] = 6.75
    assert avg[0] == 6.75


def test_register_enables_frombands_agg_sql(spark):
    import numpy as np

    prx.register(spark)
    b0 = _agg_ras_bytes(np.full((2, 2), 10.0))
    b1 = _agg_ras_bytes(np.full((2, 2), 20.0))
    df = spark.createDataFrame(
        [("g", b1, 1), ("g", b0, 0)], ["k", "raster", "band_index"]
    ).select(
        "k",
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"),
        "band_index",
    )
    df.createOrReplaceTempView("v")
    n = spark.sql(
        "SELECT gbx_rst_numbands("
        "gbx_rst_fromcontent(gbx_rst_frombands_agg(tile, band_index), 'GTiff')) "
        "AS n FROM v GROUP BY k"
    ).first()["n"]
    assert n == 2


def test_register_enables_rasterize_agg_sql(spark):
    import shapely.wkb
    from shapely.geometry import box

    prx.register(spark)
    g1 = shapely.wkb.dumps(box(0, 0, 2, 4))
    g2 = shapely.wkb.dumps(box(1, 0, 4, 4))
    spark.createDataFrame(
        [("g", g1, 1.0), ("g", g2, 2.0)], ["k", "geom", "val"]
    ).createOrReplaceTempView("v")
    w = spark.sql(
        "SELECT gbx_rst_width(gbx_rst_fromcontent("
        "gbx_rst_rasterize_agg(geom, val, 0, 0, 4, 4, 4, 4, 32633), 'GTiff')) "
        "AS w FROM v GROUP BY k"
    ).first()["w"]
    assert w == 4


def test_register_enables_derivedband_agg_sql(spark):
    import numpy as np

    prx.register(spark)
    pyfunc = (
        "def addbands(in_ar, out_ar, *args, **kwargs):\n"
        "    import numpy as np\n"
        "    out_ar[:] = np.sum(in_ar, axis=0)\n"
    )
    a = _agg_ras_bytes(np.full((2, 2), 3.0))
    b = _agg_ras_bytes(np.full((2, 2), 4.0))
    df = spark.createDataFrame([("g", a), ("g", b)], ["k", "raster"]).select(
        "k", prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("v")
    mx = spark.sql(
        "SELECT gbx_rst_max(gbx_rst_fromcontent("
        f"gbx_rst_derivedband_agg(tile, '{pyfunc}', 'addbands'), 'GTiff')) "
        "AS m FROM v GROUP BY k"
    ).first()["m"]
    assert mx[0] == 7.0
