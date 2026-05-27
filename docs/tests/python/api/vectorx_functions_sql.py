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
