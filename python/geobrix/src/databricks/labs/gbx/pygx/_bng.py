"""Pure-Python British National Grid (BNG) core for the pygx light tier.

A faithful port of the heavy ``com.databricks.labs.gbx.gridx.grid.BNG`` Scala
object (gridx/grid/BNG.scala). No PyPI BNG library exists; this module reproduces
the codec, cell geometry, neighborhood walks, polyfill, and tessellation EXACTLY
so light and heavy share bit-identical cell ids and cell sets.

Coordinates are EPSG:27700 eastings/northings. Cell ids are STRING in the public
surface (``format``/``parse`` round-trip a Long digit-id internally). Geometry is
emitted as plain WKB (NO SRID) and WKT, matching heavy ``JTS.toWKB``/``toWKT``
(BNG does NOT stamp an SRID, unlike quadbin which is EWKB SRID 4326).

Mosaic-lineage bug status (validated against this port):
  * mosaic#434 (fixed upstream in mosaic#580): 100km NE/NW/SE/SW cells are res 1,
    not quadrant resolutions. Carried here by the digit-length + trailing-quadrant
    logic in ``get_resolution_from_digits``/``format`` (a 100km id is 6 digits,
    trailing quadrant-digit 0).
  * mosaic#423: grid-aligned polygons must not emit POINT/LINESTRING chips. Carried
    here by the input-geometry-type filter on border chips in ``tessellate``.
"""

import math
from typing import Any

from shapely import to_wkb as _to_wkb
from shapely.geometry import box as _box

CRS_ID = 27700
NAME = "BNG"

QUADRANTS = ["", "SW", "NW", "NE", "SE"]

RESOLUTION_MAP = {
    "500km": -1,
    "100km": 1,
    "50km": -2,
    "10km": 2,
    "5km": -3,
    "1km": 3,
    "500m": -4,
    "100m": 4,
    "50m": -5,
    "10m": 5,
    "5m": -6,
    "1m": 6,
}
SIZE_MAP = {
    "500km": 500000,
    "100km": 100000,
    "50km": 50000,
    "10km": 10000,
    "5km": 5000,
    "1km": 1000,
    "500m": 500,
    "100m": 100,
    "50m": 50,
    "10m": 10,
    "5m": 5,
    "1m": 1,
}
RESOLUTIONS = {1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6}

LETTER_MAP = [
    ["SV", "SW", "SX", "SY", "SZ", "TV", "TW", "TX"],
    ["SQ", "SR", "SS", "ST", "SU", "TQ", "TR", "TS"],
    ["SL", "SM", "SN", "SO", "SP", "TL", "TM", "TN"],
    ["SF", "SG", "SH", "SJ", "SK", "TF", "TG", "TH"],
    ["SA", "SB", "SC", "SD", "SE", "TA", "TB", "TC"],
    ["NV", "NW", "NX", "NY", "NZ", "OV", "OW", "OX"],
    ["NQ", "NR", "NS", "NT", "NU", "OQ", "OR", "OS"],
    ["NL", "NM", "NN", "NO", "NP", "OL", "OM", "ON"],
    ["NF", "NG", "NH", "NJ", "NK", "OF", "OG", "OH"],
    ["NA", "NB", "NC", "ND", "NE", "OA", "OB", "OC"],
    ["HV", "HW", "HX", "HY", "HZ", "JV", "JW", "JX"],
    ["HQ", "HR", "HS", "HT", "HU", "JQ", "JR", "JS"],
    ["HL", "HM", "HN", "HO", "HP", "JL", "JM", "JN"],
    ["HF", "HG", "HH", "HJ", "HK", "JF", "JG", "JH"],
]


def get_resolution(res: Any) -> int:
    """Resolution Int. Overloads BNG.getResolution(Any) and BNG.getResolution(Seq[Int]).

    - A list/tuple of digits dispatches to ``get_resolution_from_digits`` (the
      Scala ``getResolution(Seq[Int])`` overload: resolution implied by digit
      length + trailing quadrant marker).
    - An Int index passes through if in RESOLUTIONS; a resolutionMap string key
      maps to its index. NEVER accepts metres-as-Int (e.g. 1000).
    """
    if isinstance(res, (list, tuple)):
        return get_resolution_from_digits(res)
    if isinstance(res, bool):
        raise ValueError(f"BNG resolution not supported; found {res!r}")
    if isinstance(res, int):
        if res in RESOLUTIONS:
            return res
        raise ValueError(
            f"BNG resolution index must be one of {sorted(RESOLUTIONS)} "
            f"(1=100km..6=1m, negatives=quadrants); got {res}. "
            "Metres-as-Int (e.g. 1000) is NOT a resolution."
        )
    if isinstance(res, str) and res in RESOLUTION_MAP:
        return RESOLUTION_MAP[res]
    raise ValueError(f"BNG resolution not supported; found {res!r}")


def get_edge_size(resolution: int) -> int:
    """Edge size (metres) for an Int resolution (BNG.getEdgeSize(Int))."""
    res_str = get_resolution_str(resolution)
    return SIZE_MAP[res_str]


def get_resolution_str(resolution: int) -> str:
    for k, v in RESOLUTION_MAP.items():
        if v == resolution:
            return k
    return ""


def cell_digits(cell_id: int) -> list:
    """Cell id Long -> list of decimal digits (BNG.cellDigits)."""
    return [int(c) for c in str(cell_id)]


def _safe_digit_index(digit_slice, max_idx: int) -> int:
    s = "".join(str(d) for d in digit_slice)
    n = 0 if s == "" else int(s)
    return max(0, min(max_idx, n))


def get_resolution_from_digits(digits) -> int:
    """BNG.getResolution(Seq[Int]) — resolution implied by digit length + quadrant."""
    if len(digits) < 6:
        return -1  # 500km
    quadrant = digits[-1]
    k = (len(digits) - 6) // 2
    return -(k + 2) if quadrant > 0 else k + 1


def get_x(digits, edge_size: int) -> int:
    n = len(digits)
    k = (n - 6) // 2
    x_digits = digits[1:3] + digits[5 : 5 + k]
    quadrant = digits[-1]
    edge_adj = 2 * edge_size if quadrant > 0 else edge_size
    x_offset = edge_size if quadrant in (3, 4) else 0
    return int("".join(str(d) for d in x_digits)) * edge_adj + x_offset


def get_y(digits, edge_size: int) -> int:
    n = len(digits)
    k = (n - 6) // 2
    y_digits = digits[3:5] + digits[5 + k : 5 + 2 * k]
    quadrant = digits[-1]
    edge_adj = 2 * edge_size if quadrant > 0 else edge_size
    y_offset = edge_size if quadrant in (2, 3) else 0
    return int("".join(str(d) for d in y_digits)) * edge_adj + y_offset


def get_quadrant(
    resolution: int, eastings: float, northings: float, divisor: float
) -> int:
    if resolution < -1:
        e_q = eastings / divisor
        n_q = northings / divisor
        e_dec = e_q - math.floor(e_q)
        n_dec = n_q - math.floor(n_q)
        if e_dec < 0.5 and n_dec < 0.5:
            return 1  # SW
        if e_dec < 0.5:
            return 2  # NW
        if n_dec < 0.5:
            return 4  # SE
        return 3  # NE
    return 0


def encode(e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution) -> int:
    id_placeholder = 10 ** (5 + 2 * n_positions - 2)
    e_letter_shift = 10 ** (3 + 2 * n_positions - 2)
    n_letter_shift = 10 ** (1 + 2 * n_positions - 2)
    e_shift = 10**n_positions
    n_shift = 10
    if resolution == -1:
        val = (id_placeholder + e_letter * e_letter_shift) / 100 + quadrant
    else:
        val = (
            id_placeholder
            + e_letter * e_letter_shift
            + n_letter * n_letter_shift
            + e_bin * e_shift
            + n_bin * n_shift
            + quadrant
        )
    return int(val)


def point_to_cell_id(eastings: float, northings: float, resolution: int) -> int:
    if math.isnan(eastings) or math.isnan(northings):
        raise ValueError("NaN coordinates are not supported.")
    e_int = int(eastings)
    n_int = int(northings)
    # Scala uses integer division (eastingsInt / 100000); int(...) truncates toward
    # zero, matching JVM Int division for the BNG-positive coordinate domain.
    e_letter = int(e_int / 100000)
    n_letter = int(n_int / 100000)
    if resolution < 0:
        divisor = 10 ** (6 - abs(resolution) + 1)
    else:
        divisor = 10 ** (6 - resolution)
    quadrant = get_quadrant(resolution, e_int, n_int, divisor)
    n_positions = abs(resolution) if resolution >= -1 else abs(resolution) - 1
    e_bin = math.floor((e_int % 100000) / divisor)
    n_bin = math.floor((n_int % 100000) / divisor)
    return encode(e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution)


def format(cell_id: int) -> str:
    digits = cell_digits(cell_id)
    if len(digits) < 6:
        x_idx = _safe_digit_index(digits[3:5], len(LETTER_MAP) - 1)
        y_idx = _safe_digit_index(digits[1:3], len(LETTER_MAP[0]) - 1)
        return LETTER_MAP[x_idx][y_idx][0]
    q_idx = max(0, min(len(QUADRANTS) - 1, digits[-1]))
    x_idx = _safe_digit_index(digits[3:5], len(LETTER_MAP) - 1)
    y_idx = _safe_digit_index(digits[1:3], len(LETTER_MAP[0]) - 1)
    prefix = LETTER_MAP[x_idx][y_idx]
    coords = digits[5:-1]
    k = len(coords) // 2
    if not coords:
        x_str = y_str = ""
    else:
        x_part = coords[:k]
        y_part = coords[k : 2 * k]
        # padTo(k, 0): right-pad to length k with zeros.
        x_part = x_part + [0] * (k - len(x_part))
        y_part = y_part + [0] * (k - len(y_part))
        x_str = "".join(str(d) for d in x_part)
        y_str = "".join(str(d) for d in y_part)
    return f"{prefix}{x_str}{y_str}{QUADRANTS[q_idx]}"


def parse(cell_id: str) -> int:
    prefix = cell_id[:2] if len(cell_id) >= 2 else f"{cell_id}V"
    letter_row = next(row for row in LETTER_MAP if prefix in row)
    e_letter = letter_row.index(prefix)
    n_letter = LETTER_MAP.index(letter_row)
    if len(cell_id) == 1:
        return encode(e_letter, 0, 0, 0, 0, 1, -1)
    suffix = cell_id[-2:]
    quadrant = (
        QUADRANTS.index(suffix) if (suffix in QUADRANTS and len(cell_id) > 2) else 0
    )
    bin_digits = cell_id[2:-2] if quadrant > 0 else cell_id[2:]
    if not bin_digits:
        return encode(e_letter, n_letter, 0, 0, quadrant, 1, -2)
    half = len(bin_digits) // 2
    # Scala: dropRight(len/2) keeps the LEFT half, drop(len/2) keeps the RIGHT half.
    e_bin = int(bin_digits[: len(bin_digits) - half] or "0")
    n_bin = int(bin_digits[half:] or "0")
    n_positions = half + 1
    resolution = (n_positions + 1) if quadrant == 0 else -n_positions
    return encode(e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution)


def area(cell_id: int) -> float:
    """Cell area in square KILOMETRES (BNG.area): (edgeSize/1000)^2."""
    resolution = get_resolution_from_digits(cell_digits(cell_id))
    edge = float(get_edge_size(resolution))
    return (edge / 1000.0) ** 2


def distance(cell_id: int, cell_id2: int) -> int:
    """Manhattan grid distance in edge-size units (BNG.distance)."""
    d1, d2 = cell_digits(cell_id), cell_digits(cell_id2)
    edge = get_edge_size(
        min(get_resolution_from_digits(d1), get_resolution_from_digits(d2))
    )
    x1, x2 = get_x(d1, edge), get_x(d2, edge)
    y1, y2 = get_y(d1, edge), get_y(d2, edge)
    return abs(x1 - x2) // edge + abs(y1 - y2) // edge


def euclidean_distance(cell_id: int, cell_id2: int) -> int:
    """Chebyshev (max of dx, dy) grid distance in edge-size units.

    Mirrors BNG.euclideanDistance: along a diagonal the distance is 1 where
    Manhattan would be 2.
    """
    d1, d2 = cell_digits(cell_id), cell_digits(cell_id2)
    edge = get_edge_size(
        min(get_resolution_from_digits(d1), get_resolution_from_digits(d2))
    )
    x1, x2 = get_x(d1, edge), get_x(d2, edge)
    y1, y2 = get_y(d1, edge), get_y(d2, edge)
    return max(abs(x1 - x2), abs(y1 - y2)) // edge


def point_as_cell(eastings: float, northings: float, resolution) -> str:
    """EPSG:27700 (eastings, northings) -> STRING cellid (BNG_EastNorthAsBNG core)."""
    res = get_resolution(resolution)
    return format(point_to_cell_id(float(eastings), float(northings), res))


# east_north_as_bng is an alias of point_as_cell at the coordinate level; the SQL
# split (pointascell takes a POINT geom, eastnorthasbng takes scalar e/n) happens
# in functions.py. Both call this.
east_north_as_bng = point_as_cell


# ---------------------------------------------------------------------------
# cell -> geometry (BNG.cellIdToGeometry / aswkb / aswkt / centroid)
#
# Heavy emits PLAIN WKB (no SRID) via JTS.toWKB and plain WKT via JTS.toWKT.
# BNG does NOT stamp an SRID (unlike quadbin, which is EWKB SRID 4326), so the
# cell polygon is built with shapely ``box`` and serialized WITHOUT include_srid.
# ---------------------------------------------------------------------------


def cell_id_to_geometry(cell_id: int):
    """Closed BNG cell polygon (shapely), NO SRID (BNG.cellIdToGeometry).

    The ring is (x,y),(x+e,y),(x+e,y+e),(x,y+e),(x,y) from getX/getY/getEdgeSize
    in EPSG:27700 eastings/northings.
    """
    digits = cell_digits(cell_id)
    resolution = get_resolution_from_digits(digits)
    edge = get_edge_size(resolution)
    x = get_x(digits, edge)
    y = get_y(digits, edge)
    return _box(x, y, x + edge, y + edge)


def cell_aswkb(cell_id: int) -> bytes:
    """Cell polygon as plain WKB (no SRID; heavy uses toWKB, not toEWKB)."""
    return _to_wkb(cell_id_to_geometry(cell_id))  # include_srid defaults False


def cell_aswkt(cell_id: int) -> str:
    """Cell polygon as WKT text (BNG.asWKT)."""
    return cell_id_to_geometry(cell_id).wkt


def cell_centroid(cell_id: int) -> bytes:
    """Cell centre as plain WKB POINT (no SRID)."""
    return _to_wkb(cell_id_to_geometry(cell_id).centroid)


def k_loop(cell_id: int, k: int) -> list:
    """Hollow square ring of Long cell ids at radius k (BNG.kLoop).

    Walks the four corners plus the interior edge runs of the square at
    distance ``k * edgeSize`` around the cell centre, mapping each point back
    to a cell id. Mirrors the Scala ``until ... by edgeSize`` (exclusive upper
    bound) edge runs via Python ``range`` (also exclusive).
    """
    digits = cell_digits(cell_id)
    resolution = get_resolution_from_digits(digits)
    edge = get_edge_size(resolution)
    x = get_x(digits, edge)
    y = get_y(digits, edge)
    xmin, xmax = x - k * edge, x + k * edge
    ymin, ymax = y - k * edge, y + k * edge
    pts = [(xmin, ymin), (xmin, ymax), (xmax, ymax), (xmax, ymin)]
    pts += [(xmin, yy) for yy in range(ymin + edge, ymax, edge)]  # left
    pts += [(xmax, yy) for yy in range(ymin + edge, ymax, edge)]  # right
    pts += [(xx, ymax) for xx in range(xmin + edge, xmax, edge)]  # up
    pts += [(xx, ymin) for xx in range(xmin + edge, xmax, edge)]  # down
    return [point_to_cell_id(px, py, resolution) for (px, py) in pts]


def k_ring(cell_id: int, n: int) -> list:
    """Center + all k-loops 1..n of Long cell ids (BNG.kRing)."""
    if n == 1:
        return [cell_id] + k_loop(cell_id, 1)
    out = [cell_id]
    for k in range(1, n + 1):
        out += k_loop(cell_id, k)
    return out


def k_ring_str(cell_id_str: str, n: int) -> list:
    """String-id wrapper over :func:`k_ring` (parse -> walk -> format)."""
    return [format(c) for c in k_ring(parse(cell_id_str), int(n))]


def k_loop_str(cell_id_str: str, k: int) -> list:
    """String-id wrapper over :func:`k_loop` (parse -> walk -> format)."""
    return [format(c) for c in k_loop(parse(cell_id_str), int(k))]
