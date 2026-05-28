"""
SQL examples for VectorX functions documentation.

Used by the function-info generator and by docs via CodeFromTest.
"""


def st_legacyaswkb_sql_example():
    """Convert legacy Mosaic geometry to WKB (SQL). Requires table with geom_legacy column."""
    return """
SELECT gbx_st_legacyaswkb(geom_legacy) AS wkb FROM legacy_table;
"""


def st_asmvt_sql_example():
    """Aggregate features into a Mapbox Vector Tile (MVT) protobuf blob (SQL).

    The view `features` here is a 2-row sample with WKB geometries (`POINT(0.1, 0.1)`
    and `POINT(0.5, 0.5)`) and a `(name, id)` attribute struct. Real pipelines would
    `GROUP BY z, x, y` after composing tile-local coordinates upstream.
    """
    return """
WITH features AS (
    SELECT unhex('01010000009A9999999999B93F9A9999999999B93F') AS geom_wkb,
           named_struct('name', 'a', 'id', 1L) AS attrs
    UNION ALL SELECT unhex('0101000000000000000000E03F000000000000E03F'),
           named_struct('name', 'b', 'id', 2L)
)
SELECT length(gbx_st_asmvt(geom_wkb, attrs, 'layer1')) AS mvt_bytes_len FROM features;
"""


def st_asmvt_pyramid_sql_example():
    """Explode one feature into one row per intersecting (z, x, y) tile, encoded as MVT (SQL).

    The view `features` here is a single polygon (WKB for a rectangle spanning lon -30..+30,
    lat 10..20). At z=2 the polygon straddles the prime meridian (tiles x=1 and x=2 in the
    y=1 row), so the generator emits 2 rows. Output struct column `t.tile` carries
    `(z, x, y, mvt_bytes)`; pipe the bytes into `gbx_pmtiles_agg` for vector publishing.
    """
    return """
WITH features AS (
    SELECT unhex('010300000001000000050000000000000000003EC000000000000024400000000000003E4000000000000024400000000000003E4000000000000034400000000000003EC000000000000034400000000000003EC00000000000002440') AS geom_wkb,
           named_struct('name', 'region-a', 'id', 1L) AS attrs
)
SELECT t.tile.z AS z, length(t.tile.mvt_bytes) AS mvt_bytes_len
FROM features
LATERAL VIEW gbx_st_asmvt_pyramid(geom_wkb, attrs, 2, 2, 'regions') t AS tile;
"""
