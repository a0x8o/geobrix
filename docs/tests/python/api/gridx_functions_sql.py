"""
SQL examples for GridX (BNG) functions documentation.

All SQL examples are executable and tested. These are imported into the
documentation via CodeFromTest components to ensure single-copy pattern.
"""

# ============================================================================
# Conversion Functions - Convert BNG cells to standard formats
# ============================================================================

def bng_aswkb_sql_example():
    """Convert BNG cell to WKB format"""
    return """
SELECT gbx_bng_aswkb('TQ3080') as wkb_geom;
"""


def bng_aswkt_sql_example():
    """Convert BNG cell to WKT format"""
    return """
SELECT gbx_bng_aswkt('TQ3080') as wkt_geom;
"""


# ============================================================================
# Core Functions - Basic cell operations
# ============================================================================

def bng_cellarea_sql_example():
    """Get area of BNG grid cell (returns square kilometres)."""
    return """
SELECT 
    'TQ3080' as cell,
    gbx_bng_cellarea('TQ3080') as area_km2
FROM locations;
"""


def bng_centroid_sql_example():
    """Get centroid of BNG grid cell"""
    return """
SELECT gbx_bng_centroid('TQ3080') as centroid;
"""


def bng_distance_sql_example():
    """Distance between two BNG cells (grid steps)"""
    return """
SELECT gbx_bng_distance('TQ3080', 'TQ3081') as distance_m;
"""


def bng_euclideandistance_sql_example():
    """Euclidean distance between two BNG cells in metres"""
    return """
SELECT gbx_bng_euclideandistance('TQ3080', 'TQ3081') as euclidean_distance_m;
"""


# ============================================================================
# Coordinate Conversion - Convert coordinates/points to BNG
# ============================================================================

def bng_eastnorthasbng_sql_example():
    """Convert OS Grid Reference (easting, northing) to BNG cell. Resolution: BNG index or string (e.g. '1km')."""
    return """
-- Convert OS Grid Reference coordinates (easting, northing); resolution '1km' or integer 3
SELECT gbx_bng_eastnorthasbng(530000, 180000, '1km') as bng_cell;
"""


def bng_pointascell_sql_example():
    """Convert point geometry to BNG cell. Point must be WKT in BNG coords (eastings, northings) or WKB; resolution '1km' or 3."""
    return """
-- Point in BNG coordinates (eastings, northings); resolution '1km' for 1 km cell
SELECT gbx_bng_pointascell('POINT(530000 180000)', '1km') as london_cell;
"""


# ============================================================================
# K-Ring Functions - Generate neighboring cells
# ============================================================================

def bng_kring_sql_example():
    """Get k-ring of cells around a center"""
    return """
-- Get all cells within 2 rings of center
SELECT 
    cell_id,
    gbx_bng_kring(cell_id, 2) as nearby_cells
FROM locations;
"""


def bng_kloop_sql_example():
    """Get k-loop (hollow ring) around a center cell"""
    return """
SELECT 
    cell_id,
    gbx_bng_kloop(cell_id, 2) as kloop_cells
FROM locations;
"""


def bng_kringexplode_sql_example():
    """Explode k-ring into one row per cell"""
    return """
SELECT 
    center_cell,
    gbx_bng_kringexplode(center_cell, 2) as nearby_cell
FROM (SELECT 'TQ3080' as center_cell);
"""


def bng_kloopexplode_sql_example():
    """Explode k-loop into one row per cell"""
    return """
SELECT 
    center_cell,
    gbx_bng_kloopexplode(center_cell, 2) as ring_cell
FROM (SELECT 'TQ3080' as center_cell);
"""


def bng_geomkring_sql_example():
    """K-ring of cells for a geometry at given resolution"""
    return """
SELECT gbx_bng_geomkring(
    st_geomfromtext('POLYGON((-0.1 51.5, -0.1 51.6, 0.0 51.6, 0.0 51.5, -0.1 51.5))'),
    3, 1
) as kring_cells;
"""


def bng_geomkloop_sql_example():
    """K-loop for a geometry at given resolution"""
    return """
SELECT gbx_bng_geomkloop(
    st_geomfromtext('POLYGON((-0.1 51.5, -0.1 51.6, 0.0 51.6, 0.0 51.5, -0.1 51.5))'),
    3, 1
) as kloop_cells;
"""


def bng_geomkringexplode_sql_example():
    """Explode geometry k-ring into one row per cell"""
    return """
SELECT gbx_bng_geomkringexplode(
    st_geomfromtext('POINT(-0.1278 51.5074)'), 3, 1
) as cell;
"""


def bng_geomkloopexplode_sql_example():
    """Explode geometry k-loop into one row per cell"""
    return """
SELECT gbx_bng_geomkloopexplode(
    st_geomfromtext('POINT(-0.1278 51.5074)'), 3, 1
) as cell;
"""


# ============================================================================
# Tessellation Functions - Fill geometries with cells
# ============================================================================

def bng_polyfill_sql_example():
    """Fill polygon with BNG cells"""
    return """
-- Fill a polygon with 1km cells
SELECT 
    region_name,
    gbx_bng_polyfill(boundary, 3) as cells
FROM regions;
"""


def bng_tessellate_sql_example():
    """Tessellate geometry into BNG cells with geometries"""
    return """
SELECT gbx_bng_tessellate(
    st_geomfromtext('POLYGON((-0.1 51.5, -0.1 51.6, 0.0 51.6, 0.0 51.5, -0.1 51.5))'),
    3
) as tessellation;
"""


def bng_tessellateexplode_sql_example():
    """Explode tessellation into one row per cell"""
    return """
SELECT gbx_bng_tessellateexplode(
    st_geomfromtext('POLYGON((-0.1 51.5, -0.1 51.6, 0.0 51.6, 0.0 51.5, -0.1 51.5))'),
    3
) as cell_info;
"""


# ============================================================================
# Aggregator Functions - Aggregate multiple cells
# ============================================================================

def bng_cellintersection_agg_sql_example():
    """Aggregate intersection of multiple cells"""
    return """
-- Find common cell across groups
SELECT 
    group_id,
    gbx_bng_cellintersection_agg(cell_id) as common_cell
FROM observations
GROUP BY group_id;
"""


def bng_cellunion_agg_sql_example():
    """Aggregate union of multiple cells (performance example)"""
    return """
SELECT 
    region,
    gbx_bng_cellunion_agg(cell_id) as bounding_cell
FROM observations
GROUP BY region;
"""


# =============================================================================
# EXAMPLE OUTPUT (show-type result for docs, same style as quick-start)
# =============================================================================

bng_aswkb_sql_example_output = """
+--------------------+
|wkb_geom            |
+--------------------+
|[BINARY]            |
+--------------------+
"""

bng_aswkt_sql_example_output = """
+------------------------------------------+
|wkt_geom                                  |
+------------------------------------------+
|POLYGON ((...))                           |
+------------------------------------------+
"""

bng_cellarea_sql_example_output = """
+------+----------+
|cell  |area_km2  |
+------+----------+
|TQ3080|1.0       |
+------+----------+
"""

bng_centroid_sql_example_output = """
+--------------------+
|centroid            |
+--------------------+
|POINT (...)         |
+--------------------+
"""

bng_eastnorthasbng_sql_example_output = """
+----------+
|bng_cell  |
+----------+
|TQ3080    |
+----------+
"""

bng_pointascell_sql_example_output = """
+------------+
|london_cell |
+------------+
|TQ3080      |
+------------+
"""

bng_kring_sql_example_output = """
+------+--------------------------------+
|cell_id|nearby_cells                   |
+------+--------------------------------+
|TQ3080|[TQ3079, TQ3081, TQ2979, ...]   |
+------+--------------------------------+
"""

bng_polyfill_sql_example_output = """
+------------+-------------------+
|region_name |cells              |
+------------+-------------------+
|London      |[TQ3079, TQ3080,..]|
+------------+-------------------+
"""

bng_cellintersection_agg_sql_example_output = """
+--------+------------+
|group_id|common_cell |
+--------+------------+
|1       |TQ3080      |
+--------+------------+
"""

bng_cellunion_agg_sql_example_output = """
+------+--------------+
|region|bounding_cell |
+------+--------------+
|South |TQ3080        |
+------+--------------+
"""


# ============================================================================
# Quadbin (CARTO v0) — 9 grid-math functions
# ============================================================================

def quadbin_pointascell_sql_example():
    """Convert lon/lat (EPSG:4326) to a quadbin cell at a given zoom (0..26)."""
    return """
SELECT gbx_quadbin_pointascell(-122.4194, 37.7749, 10) as sf_cell;
"""


def quadbin_aswkb_sql_example():
    """Return the quadbin cell footprint as EWKB (SRID=4326)."""
    return """
SELECT gbx_quadbin_aswkb(gbx_quadbin_pointascell(0.0, 0.0, 8)) as wkb;
"""


def quadbin_centroid_sql_example():
    """Return the quadbin cell centroid as EWKB POINT (SRID=4326)."""
    return """
SELECT gbx_quadbin_centroid(gbx_quadbin_pointascell(0.0, 0.0, 8)) as centroid;
"""


def quadbin_resolution_sql_example():
    """Return the resolution (zoom 0..26) of a quadbin cell."""
    return """
SELECT gbx_quadbin_resolution(gbx_quadbin_pointascell(0.0, 0.0, 12)) as z;
"""


def quadbin_polyfill_sql_example():
    """Polyfill a geometry's bbox with quadbin cells at a given zoom (0..20)."""
    return """
SELECT gbx_quadbin_polyfill(
    st_geomfromtext('POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))'), 5
) as cells;
"""


def quadbin_kring_sql_example():
    """Return all cells within Chebyshev distance k of a quadbin cell (inclusive)."""
    return """
SELECT gbx_quadbin_kring(gbx_quadbin_pointascell(0.0, 0.0, 10), 1) as ring;
"""


def quadbin_tessellate_sql_example():
    """Tessellate a geometry into quadbin cells; returns array of struct(cell, geom)."""
    return """
SELECT gbx_quadbin_tessellate(
    st_geomfromtext('POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))'), 5
) as chips;
"""


def quadbin_cellunion_sql_example():
    """Union an ARRAY<BIGINT> of quadbin cells to a single MultiPolygon EWKB."""
    return """
SELECT gbx_quadbin_cellunion(
    gbx_quadbin_kring(gbx_quadbin_pointascell(0.0, 0.0, 8), 1)
) as union_geom;
"""


def quadbin_cellunion_agg_sql_example():
    """Aggregator: union quadbin cells per group into a single MultiPolygon EWKB."""
    return """
SELECT region, gbx_quadbin_cellunion_agg(cell) AS coverage
FROM grid_cells
GROUP BY region;
"""


quadbin_cellunion_agg_sql_example_output = """
+------+--------+
|region|coverage|
+------+--------+
|...   |[BINARY]|
+------+--------+
"""


def quadbin_distance_sql_example():
    """Chebyshev distance between two quadbin cells at the same resolution."""
    return """
SELECT gbx_quadbin_distance(
    gbx_quadbin_pointascell(0.0, 0.0, 10),
    gbx_quadbin_pointascell(0.0001, 0.0, 10)
) as d;
"""


quadbin_pointascell_sql_example_output = """
+-------------------+
|sf_cell            |
+-------------------+
|5233961839712272383|
+-------------------+
"""

quadbin_kring_sql_example_output = """
+--------------------------------------------+
|ring                                        |
+--------------------------------------------+
|[5227553336189779967, ..., (9 cells)]       |
+--------------------------------------------+
"""

quadbin_polyfill_sql_example_output = """
+--------------------------------------------+
|cells                                       |
+--------------------------------------------+
|[5215660717881425919, ...]                  |
+--------------------------------------------+
"""


# ============================================================================
# Custom Grid — user-defined regular grid functions
# ============================================================================

def custom_grid_sql_example():
    """Define a user-specified regular grid from origin, extent, resolution, and SRID."""
    return """
SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid;
"""


def custom_pointascell_sql_example():
    """Index points into a user-defined regular grid."""
    return """
SELECT gbx_custom_pointascell(geom, gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700), 5) AS cell FROM points;
"""


def custom_cellaswkb_sql_example():
    """Return the WKB footprint of a custom grid cell."""
    return """
SELECT gbx_custom_cellaswkb(cell, gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)) AS geom FROM cells;
"""


def custom_cellaswkt_sql_example():
    """Return the WKT footprint of a custom grid cell."""
    return """
SELECT gbx_custom_cellaswkt(cell, gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)) AS wkt FROM cells;
"""


def custom_centroid_sql_example():
    """Return the centroid of a custom grid cell."""
    return """
SELECT gbx_custom_centroid(cell, gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)) AS centroid FROM cells;
"""


def custom_polyfill_sql_example():
    """Fill a geometry with custom grid cells at the given resolution."""
    return """
SELECT region_id, gbx_custom_polyfill(geom, gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700), 5) AS cells FROM regions;
"""


def custom_kring_sql_example():
    """Return all custom grid cells within k steps of a center cell."""
    return """
SELECT gbx_custom_kring(cell, gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700), 1) AS ring FROM cells;
"""


custom_grid_sql_example_output = """
+--------------------+
|grid                |
+--------------------+
|{...}               |
+--------------------+
"""

custom_pointascell_sql_example_output = """
+----+
|cell|
+----+
|... |
+----+
"""

custom_cellaswkb_sql_example_output = """
+--------------------+
|geom                |
+--------------------+
|[BINARY]            |
+--------------------+
"""

custom_cellaswkt_sql_example_output = """
+------------------------------------------+
|wkt                                       |
+------------------------------------------+
|POLYGON ((...))                           |
+------------------------------------------+
"""

custom_centroid_sql_example_output = """
+--------------------+
|centroid            |
+--------------------+
|POINT (...)         |
+--------------------+
"""

custom_polyfill_sql_example_output = """
+---------+--------------+
|region_id|cells         |
+---------+--------------+
|1        |[...]         |
+---------+--------------+
"""

custom_kring_sql_example_output = """
+----+------+
|cell|ring  |
+----+------+
|... |[...]  |
+----+------+
"""
