"""Spark-free H3, quadbin, and BNG raster tessellation.

Mirrors heavyweight ``RST_H3_Tessellate`` / ``RST_Quadbin_Tessellate`` /
``RST_BNG_Tessellate`` (``RasterTessellate``).

The single generic entry point is :func:`iter_tessellate`, which dispatches on
``grid`` for the ~40% divergent cell math (CRS setup, polyfill, cell geometry,
id encoding, validity filter) while sharing the ~60% common body (mode
validation, centroid dispatch, positive-area keep-test, clip, yield).

The three per-grid wrappers (:func:`iter_tessellate_h3`,
:func:`iter_tessellate_quadbin`, :func:`iter_tessellate_bng`) are kept as thin
delegates for backward compatibility with bench callers and existing tests.
"""

from collections import defaultdict
from contextlib import contextmanager, nullcontext

import h3
import numpy as np
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.warp import transform_bounds, transform_geom
from shapely.geometry import Polygon, box, mapping, shape

from databricks.labs.gbx.pygx import _bng, _quadbin
from databricks.labs.gbx.pyrx.core import edit, warp

H3_MAX_RES = 15
QUADBIN_MAX_RES = _quadbin._MAX_POLYFILL_RES

_WGS84 = "EPSG:4326"
_BNG_EPSG = 27700
_VALID_MODES = {"covering", "centroid"}
_VALID_GRIDS = frozenset({"h3", "quadbin", "bng"})


def _has_positive_area_overlap(cell_poly, bbox_poly) -> bool:
    """Covering keep-test: cell is emitted iff it has POSITIVE-AREA overlap with the raster bbox.

    Mere boundary touch (a shared edge line or corner point) is NOT enough. On a grid-aligned
    tile (raster edges land exactly on cell boundaries) a fringe cell just outside the data
    shares only a 1-D boundary with the raster: ``cell_poly.intersects(bbox) is True`` but
    ``cell_poly.intersection(bbox).area == 0.0`` and it holds ZERO source pixels, so the clip
    would (via rasterio "shapes do not overlap") drop it anyway. This guard makes that intent
    EXPLICIT and consistent with the heavyweight tier's positive-area keep-test.

    KEEPS cells with real areal overlap even if they clip to all-NoData (cloud hole, the raster's
    own NoData) — those have ``area > 0`` and must still be emitted (covering mode fills their
    position; the clip yields a real chip with NoData pixels, not a gap in the mosaic).

    ``cell_poly`` and ``bbox_poly`` MUST be in the same CRS.
    """
    if not cell_poly.intersects(bbox_poly):
        return False
    return cell_poly.intersection(bbox_poly).area > 0.0


_DEFAULT_NODATA = -9999.0


def _cell_polygon_lonlat(cell: str) -> Polygon:
    """H3 cell hexagon as a shapely Polygon in (lon, lat) order.

    ``h3.cell_to_boundary`` returns (lat, lng) tuples; shapely expects
    (lon, lat), so the coordinates are flipped.
    """
    boundary = h3.cell_to_boundary(cell)  # list of (lat, lng)
    return Polygon([(lng, lat) for lat, lng in boundary])


def _h3_str_to_signed_int64(cell: str) -> int:
    """Convert an H3 cell string id to a signed int64 (matching Spark LongType)."""
    cellid = h3.str_to_int(cell)
    if cellid >= 2**63:
        cellid -= 2**64
    return cellid


def _quadbin_uint64_to_signed_int64(cell: int) -> int:
    """Convert an unsigned quadbin cell id to a signed int64 (Spark LongType)."""
    if cell >= 2**63:
        return cell - 2**64
    return int(cell)


# ---------------------------------------------------------------------------
# BNG context manager (shared by covering and centroid)
# ---------------------------------------------------------------------------


@contextmanager
def _as_bng_dataset(ds):
    """Yield ``ds`` (or a 27700-warped copy) as an open EPSG:27700 dataset.

    BNG has no lon/lat input path — the raster must be in EPSG:27700 before cell
    geometry (``pygx._bng.cell_id_to_geometry``) and the geometric keep-test can
    be applied.  When ``ds`` is already 27700 it is yielded unchanged; otherwise
    it is reprojected (nearest) via :func:`warp.reproject_to_srid` and the warped
    dataset is yielded and cleaned up.  Mirrors heavy ``warpToBng``.
    """
    src_epsg = ds.crs.to_epsg() if ds.crs else None
    if src_epsg == _BNG_EPSG:
        yield ds
        return
    warped_bytes = warp.reproject_to_srid(ds, _BNG_EPSG, resampling="nearest")
    with MemoryFile(warped_bytes) as mf:
        with mf.open() as work_ds:
            yield work_ds


# ---------------------------------------------------------------------------
# Per-grid oracle helpers (the divergent ~40%)
# ---------------------------------------------------------------------------


def _resolve_resolution(resolution, grid: str):
    """Validate and normalise resolution for the given grid.

    H3 and quadbin accept integer indices; BNG additionally accepts string keys
    (e.g. ``"1km"``, ``"100m"``).
    """
    if grid == "h3":
        resolution = int(resolution)
        if resolution < 0 or resolution > H3_MAX_RES:
            raise ValueError(
                f"rst_h3_tessellate: resolution must be in [0, {H3_MAX_RES}]; "
                f"got {resolution}"
            )
    elif grid == "quadbin":
        resolution = int(resolution)
        if resolution < 0 or resolution > QUADBIN_MAX_RES:
            raise ValueError(
                f"rst_quadbin_tessellate: resolution must be in [0, {QUADBIN_MAX_RES}]; "
                f"got {resolution}"
            )
    else:  # bng
        resolution = _bng.get_resolution(resolution)
    return resolution


def _polyfill_cells(bbox_poly, resolution, grid: str):
    """Return the iterable of raw cell ids covering *bbox_poly* for *grid*.

    ``bbox_poly`` must be a shapely geometry in the work CRS for the grid:
    WGS84 for h3/quadbin, EPSG:27700 for bng.  BNG applies the buffer-before-
    polyfill fix internally.
    """
    if grid == "h3":
        west, south, east, north = bbox_poly.bounds
        h3_bbox = h3.LatLngPoly(
            [(south, west), (north, west), (north, east), (south, east)]
        )
        return h3.polygon_to_cells_experimental(h3_bbox, resolution, contain="overlap")
    elif grid == "quadbin":
        return _quadbin.polyfill(bbox_poly, resolution)
    else:  # bng — buffer so centroid-BFS doesn't miss boundary cells
        buf_radius = _bng.get_buffer_radius(resolution)
        return _bng.polyfill(bbox_poly.buffer(buf_radius), resolution)


def _cell_geom(cell, grid: str):
    """Return the cell polygon in the work CRS (WGS84 for h3/quadbin; 27700 for bng)."""
    if grid == "h3":
        return _cell_polygon_lonlat(cell)
    elif grid == "quadbin":
        # as_wkb returns EWKB with SRID=4326; shapely.wkb.loads handles EWKB
        # transparently (reads the geometry; SRID is not used here because
        # reprojection is handled separately by the need_reproject branch).
        return shapely.wkb.loads(_quadbin.as_wkb(cell))
    else:  # bng
        return _bng.cell_id_to_geometry(cell)  # already EPSG:27700


def _cell_is_valid(cell, grid: str) -> bool:
    """Return ``True`` unless the cell is structurally invalid (BNG only)."""
    if grid == "bng":
        return bool(_bng.is_valid(cell))
    return True


def _encode_cellid(cell, grid: str):
    """Encode the raw cell id to the Python type yielded to callers.

    H3 and quadbin yield signed int64; BNG yields a String id (e.g. ``"TQ38"``).
    """
    if grid == "h3":
        return _h3_str_to_signed_int64(cell)
    elif grid == "quadbin":
        return _quadbin_uint64_to_signed_int64(cell)
    else:  # bng
        return _bng.format(cell)


# ---------------------------------------------------------------------------
# Consolidated centroid implementation
# ---------------------------------------------------------------------------


def _centroid_chips_inner(work_ds, resolution, grid: str):
    """Inner centroid-partition loop for an already-prepared dataset.

    For h3/quadbin ``work_ds`` is the original dataset (pixel coords are
    reprojected to WGS84 if needed).  For BNG ``work_ds`` is already in
    EPSG:27700 and pixel coords are used directly as eastings/northings.

    Yields ``(cellid, gtiff_bytes)`` pairs.
    """
    rows, cols = np.mgrid[0 : work_ds.height, 0 : work_ds.width]
    xs, ys = work_ds.xy(rows.ravel(), cols.ravel())
    xs = np.asarray(xs, dtype="float64")
    ys = np.asarray(ys, dtype="float64")

    if grid in ("h3", "quadbin"):
        dst_epsg = work_ds.crs.to_epsg() if work_ds.crs else None
        if dst_epsg != 4326:
            from rasterio.warp import transform as warp_transform

            coord_x, coord_y = warp_transform(
                work_ds.crs, _WGS84, xs.tolist(), ys.tolist()
            )
            coord_x = np.asarray(coord_x, dtype="float64")
            coord_y = np.asarray(coord_y, dtype="float64")
        else:
            coord_x, coord_y = xs, ys  # already WGS84; x=lon, y=lat
    else:  # bng: work_ds already EPSG:27700; xs=eastings, ys=northings
        coord_x, coord_y = xs, ys

    data = work_ds.read()  # shape (bands, height, width)
    nodata = work_ds.nodata

    if nodata is not None:
        valid_flat = ~np.all(data.reshape(work_ds.count, -1).T == nodata, axis=1)
    else:
        valid_flat = np.ones(work_ds.height * work_ds.width, dtype=bool)

    cell_pixels: dict = defaultdict(list)
    for flat_idx in np.where(valid_flat)[0]:
        cx = float(coord_x[flat_idx])
        cy = float(coord_y[flat_idx])
        if grid == "h3":
            cell = h3.latlng_to_cell(cy, cx, resolution)  # latlng_to_cell(lat, lon)
        elif grid == "quadbin":
            cell = _quadbin.point_as_cell(cx, cy, resolution)  # (lon, lat)
        else:  # bng
            cell = _bng.point_to_cell_id(cx, cy, resolution)  # (easting, northing)
            if not _bng.is_valid(cell):
                continue
        cell_pixels[cell].append(int(flat_idx))

    profile = work_ds.profile.copy()
    profile.update(driver="GTiff")
    nd = nodata if nodata is not None else _DEFAULT_NODATA
    if nodata is None:
        profile["nodata"] = nd

    for cell, flat_indices in cell_pixels.items():
        chip = np.full_like(data, nd)
        row_idx, col_idx = np.unravel_index(
            flat_indices, (work_ds.height, work_ds.width)
        )
        chip[:, row_idx, col_idx] = data[:, row_idx, col_idx]

        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(chip)
            raster_bytes = mf.read()

        yield (_encode_cellid(cell, grid), raster_bytes)


def _centroid_chips_generic(ds, resolution, grid: str):
    """Consolidated centroid-partition dispatcher for all grids.

    BNG warps the dataset to EPSG:27700 first (via :func:`_as_bng_dataset`).
    H3 and quadbin use the original dataset, reprojecting pixel coords to WGS84
    inside :func:`_centroid_chips_inner` if needed.

    Yields ``(cellid, gtiff_bytes)`` pairs.
    """
    if grid == "bng":
        with _as_bng_dataset(ds) as work_ds:
            yield from _centroid_chips_inner(work_ds, resolution, grid)
    else:
        yield from _centroid_chips_inner(ds, resolution, grid)


# ---------------------------------------------------------------------------
# Generic tessellate (the consolidated public entry point)
# ---------------------------------------------------------------------------


def iter_tessellate(ds, resolution, grid: str, mode: str = "covering"):
    """Generic streaming tessellate: yield ``(cellid, gtiff_bytes)`` per overlapping cell.

    Dispatches on *grid* for the ~40% divergent cell math (CRS setup, polyfill,
    cell geometry, id encoding, validity filter) while sharing the ~60% common
    body (mode validation, centroid dispatch, positive-area keep-test, clip,
    yield).

    Args:
        ds:         Open rasterio ``DatasetReader``.
        resolution: Grid-appropriate resolution:

                    - H3: int in ``[0, 15]``
                    - Quadbin: int in ``[0, 26]`` (polyfill limited to
                      ``[0, 20]``; see :data:`QUADBIN_MAX_RES`)
                    - BNG: int index ``±1..±6`` or string key (e.g.
                      ``"1km"``, ``"100m"``); resolved via
                      ``pygx._bng.get_resolution``

        grid:       One of ``"h3"``, ``"quadbin"``, ``"bng"``.
        mode:       ``"covering"`` (default) — clip each overlapping cell
                    boundary; ``"centroid"`` — strict pixel partition: each
                    valid pixel assigned to exactly one cell by its centroid.

    Yields:
        ``(cellid, raster_bytes)`` tuples, one per overlapping cell.
        ``cellid`` is a signed int64 for h3 and quadbin, a BNG String id
        (e.g. ``"TQ38"``) for bng.
    """
    if grid not in _VALID_GRIDS:
        raise ValueError(
            f"iter_tessellate: grid must be one of {sorted(_VALID_GRIDS)}; "
            f"got {grid!r}"
        )
    if mode not in _VALID_MODES:
        raise ValueError(
            f"rst_{grid}_tessellate: mode must be one of covering, centroid; "
            f"got '{mode}'"
        )

    resolution = _resolve_resolution(resolution, grid)

    if mode == "centroid":
        yield from _centroid_chips_generic(ds, resolution, grid)
        return

    # ---- covering mode -------------------------------------------------------
    # BNG: warp the dataset to EPSG:27700 and operate in that CRS throughout.
    # H3 / quadbin: use the original dataset; bbox/polyfill/keep-test live in
    # WGS84; cell polys are reprojected to ds.crs only for the rasterio clip.
    _ctx = _as_bng_dataset(ds) if grid == "bng" else nullcontext(ds)

    with _ctx as work_ds:
        if grid == "bng":
            west, south, east, north = work_ds.bounds
        else:
            west, south, east, north = transform_bounds(ds.crs, _WGS84, *ds.bounds)
        bbox_poly = box(west, south, east, north)

        covered = _polyfill_cells(bbox_poly, resolution, grid)

        dst_epsg = work_ds.crs.to_epsg() if work_ds.crs else None
        need_reproject = grid != "bng" and dst_epsg != 4326

        for cell in covered:
            if not _cell_is_valid(cell, grid):
                continue
            cell_poly = _cell_geom(cell, grid)
            # Positive-area covering keep-test: drop edge-only-touching cells
            # (zero pixel overlap on grid-aligned tiles); keep real areal overlap
            # including all-NoData-but-overlapping cells.  The clip below remains
            # the pixel-level safety net.
            if not _has_positive_area_overlap(cell_poly, bbox_poly):
                continue
            if need_reproject:
                geom = transform_geom(_WGS84, work_ds.crs, mapping(cell_poly))
                cell_poly = shape(geom)
            try:
                # all_touched=True: boundary pixels touched by the cell edge are
                # included in the chip, consistent with the covering selection.
                clipped = edit.clip_to_geom(work_ds, cell_poly, all_touched=True)
            except ValueError:
                # rasterio.mask raises ValueError when the shape does not overlap.
                continue
            # clip_to_geom returns None only on true geometric non-overlap (the
            # rasterio "Input shapes do not overlap raster" case).  A cell that
            # overlaps the bbox but clips to entirely NoData is still emitted.
            if clipped is None:
                continue
            yield (_encode_cellid(cell, grid), clipped)


# ---------------------------------------------------------------------------
# Per-grid thin delegates (kept for backward compat with bench + existing tests)
# ---------------------------------------------------------------------------


def iter_tessellate_h3(ds, resolution: int, mode: str = "covering"):
    """Streaming H3 tessellate — delegates to :func:`iter_tessellate`.

    Kept for backward compatibility with bench callers and existing tests.

    Yields ``(cellid_int, gtiff_bytes)`` one cell at a time — never buffers the
    full cell list (large-fan-out OOM guard).

    Args:
        ds:         Open rasterio ``DatasetReader``.
        resolution: H3 resolution in ``[0, 15]``.
        mode:       ``"covering"`` (default) or ``"centroid"``.

    Yields:
        ``(cellid, raster_bytes)`` tuples.  ``cellid`` is the signed int64 H3
        cell id.
    """
    yield from iter_tessellate(ds, resolution, "h3", mode)


def tessellate_h3(ds, resolution: int) -> list:
    """Tessellate a raster into H3 cells; return ``[(cellid_int, gtiff_bytes)]``.

    List-materializing wrapper around :func:`iter_tessellate_h3` (kept for the
    Spark-free core API and bench/parity callers).
    """
    return list(iter_tessellate_h3(ds, resolution))


def iter_tessellate_quadbin(ds, resolution: int, mode: str = "covering"):
    """Streaming quadbin tessellate — delegates to :func:`iter_tessellate`.

    Kept for backward compatibility with bench callers and existing tests.

    Yields ``(cellid_int, gtiff_bytes)`` one cell at a time.

    Args:
        ds:         Open rasterio ``DatasetReader``.
        resolution: Quadbin resolution in ``[0, 26]`` (polyfill limited to
                    ``[0, 20]``; see :data:`QUADBIN_MAX_RES`).
        mode:       ``"covering"`` (default) or ``"centroid"``.

    Yields:
        ``(cellid, raster_bytes)`` tuples.  ``cellid`` is a signed int64
        quadbin cell id.
    """
    yield from iter_tessellate(ds, resolution, "quadbin", mode)


def iter_tessellate_bng(ds, resolution, mode: str = "covering"):
    """Streaming BNG tessellate — delegates to :func:`iter_tessellate`.

    Kept for backward compatibility with bench callers and existing tests.

    The raster is reprojected to EPSG:27700 first (skipped if already 27700).
    Out-of-GB cells are dropped.  Enumeration is BOUNDARY-COMPLETE (bbox is
    buffered before polyfill so boundary cells are not dropped by the centroid
    flood-fill).

    Args:
        ds:         Open rasterio ``DatasetReader`` (any CRS; warped to 27700).
        resolution: BNG resolution — an Int index (±1..±6) or a resolutionMap
                    string key (e.g. ``"1km"``, ``"100m"``).
        mode:       ``"covering"`` (default) or ``"centroid"``.

    Yields:
        ``(cellid, raster_bytes)`` tuples.  ``cellid`` is a BNG String id
        (e.g. ``"TQ38"``).
    """
    yield from iter_tessellate(ds, resolution, "bng", mode)
