"""DEM terrain derivatives: slope, aspect, hillshade (Horn's 3x3 method).

Implements the same algorithms as ``gdaldem`` but in pure NumPy — no scipy.
Exact bit-parity with GDAL is NOT guaranteed; the outputs match to within
a rounding tolerance on well-behaved inputs.

Horn's method:
    For each pixel, take the 3x3 neighbourhood (edge-replicated):
        a=NW  b=N   c=NE
        d=W   e=ctr f=E
        g=SW  h=S   i=SE

    dzdx = ((c + 2f + i) - (a + 2d + g)) / (8 * xres)
    dzdy = ((g + 2h + i) - (a + 2b + c)) / (8 * yres)

Single-band output in every case:
    slope     -> Float32 (degrees or percent)   nodata -9999
    aspect    -> Float32 (compass or trig deg)  nodata -9999
    hillshade -> uint8   valid [1..255], 0=nodata  nodata 0
"""

import math

import numpy as np
import pyproj
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core._nodata import emit, propagate_invalid, read_masked

_NODATA = -9999.0


def _horn_gradients(ds) -> tuple:
    """Return (dzdx, dzdy, valid) for the first band using Horn's 3x3 kernel.

    Edge pixels are computed using edge-replication (``np.pad(mode='edge')``),
    so the output arrays are the same shape as the source band.  Pixel
    ground sizes come from the absolute values of the affine transform's
    scale components.  ``valid`` is the input validity mask (False where the
    source band is NoData) for downstream NoData propagation.
    """
    xres = abs(ds.transform.a)
    yres = abs(ds.transform.e)

    band, valid = read_masked(ds)
    # Edge-replicate one pixel on each side.
    p = np.pad(band, 1, mode="edge")

    a = p[0:-2, 0:-2]  # NW
    b = p[0:-2, 1:-1]  # N
    c = p[0:-2, 2:]  # NE
    d = p[1:-1, 0:-2]  # W
    f = p[1:-1, 2:]  # E
    g = p[2:, 0:-2]  # SW
    h = p[2:, 1:-1]  # S
    i = p[2:, 2:]  # SE

    dzdx = ((c + 2.0 * f + i) - (a + 2.0 * d + g)) / (8.0 * xres)
    dzdy = ((g + 2.0 * h + i) - (a + 2.0 * b + c)) / (8.0 * yres)

    return dzdx, dzdy, valid


def _gdaldem_scale(ds) -> tuple:
    """Replicate GDAL 3.11 gdaldem auto-scale (xscale, yscale) from the CRS.

    Mirrors apps/gdaldem_lib.cpp GDALDEMProcessing defaulting (triggered when no
    explicit scale): geographic CRS -> anisotropic latitude-based degree->metre;
    projected CRS -> linear units; unknown/no CRS -> (1.0, 1.0).
    """
    crs = ds.crs
    if crs is None:
        return 1.0, 1.0
    try:
        pcrs = pyproj.CRS.from_user_input(crs)
    except Exception:
        return 1.0, 1.0
    zunit = 1.0  # vertical unit; GDAL assumes metre when band UnitType is unset
    units = getattr(ds, "units", None)
    if units and units[0]:
        u = units[0].lower()
        if u in ("ft", "foot", "feet", "foot (international)"):
            zunit = 0.3048
        elif u in ("us-ft", "foot_us", "us survey foot"):
            zunit = 1200.0 / 3937.0
    if pcrs.is_geographic:
        ang = math.pi / 180.0  # GetAngularUnits for degree-based geographic CRS
        a = pcrs.ellipsoid.semi_major_metre
        yscale = ang * a / zunit
        mean_lat = (ds.transform.f + ds.height * ds.transform.e / 2.0) * ang
        xscale = yscale * math.cos(mean_lat)
        return xscale, yscale
    if pcrs.is_projected:
        lin = pcrs.axis_info[0].unit_conversion_factor  # metres per linear unit
        s = lin / zunit
        return s, s
    return 1.0, 1.0


def _resolve_scale(ds, xscale, yscale) -> tuple:
    """Explicit (xscale, yscale) when BOTH given, else GDAL-normal auto-scale."""
    if xscale is not None and yscale is not None:
        return float(xscale), float(yscale)
    return _gdaldem_scale(ds)


def slope(ds, unit: str = "degrees", scale: float = 1.0) -> bytes:
    """Compute terrain slope (Horn's method).

    Args:
        ds:    Open rasterio DatasetReader.  Band 1 is used as the DEM.
        unit:  ``"degrees"`` (default) or ``"percent"``.
        scale: Ratio of vertical units to horizontal units (default 1.0).
               For geographic-degree grids use ~111120 to match gdaldem.

    Returns:
        Single-band Float32 GTiff bytes; nodata = -9999.
    """
    dzdx, dzdy, valid = _horn_gradients(ds)

    # Apply scale to the combined gradient magnitude.
    magnitude = (1.0 / float(scale)) * np.sqrt(dzdx**2 + dzdy**2)
    slope_rad = np.arctan(magnitude)

    if unit == "percent":
        result = np.tan(slope_rad) * 100.0  # == magnitude * 100
    else:
        result = np.degrees(slope_rad)

    return emit(ds, result, _NODATA, propagate_invalid(valid), "float32")


def aspect(ds, trigonometric: bool = False, zero_for_flat: bool = False) -> bytes:
    """Compute terrain aspect (Horn's method).

    Default output is compass degrees: 0 = North, increasing clockwise.
    Flat cells (dzdx == dzdy == 0) are set to -9999 (or 0 if zero_for_flat).

    Args:
        ds:              Open rasterio DatasetReader.  Band 1 used as DEM.
        trigonometric:   If True, return math-convention degrees (CCW from
                         east) instead of compass degrees (CW from north).
        zero_for_flat:   If True, flat cells get 0 instead of -9999.

    Returns:
        Single-band Float32 GTiff bytes; nodata = -9999.
    """
    dzdx, dzdy, valid = _horn_gradients(ds)

    # Math convention: arctan2(dzdy, -dzdx) — angle CCW from east in [-180,180].
    aspect_rad = np.arctan2(dzdy, -dzdx)
    aspect_deg = np.degrees(aspect_rad)

    if trigonometric:
        result = aspect_deg
    else:
        # Convert to compass bearing: 0 = N, clockwise.
        # compass = (90 - math_degrees) % 360
        result = (90.0 - aspect_deg) % 360.0

    # Mark flat cells.
    flat_mask = (dzdx == 0.0) & (dzdy == 0.0)
    flat_value = 0.0 if zero_for_flat else _NODATA
    result = np.where(flat_mask, flat_value, result)

    return emit(ds, result, _NODATA, propagate_invalid(valid), "float32")


def _neighbors(band: np.ndarray) -> tuple:
    """Return (center, [n0..n7]) for a 2-D array using edge-replication.

    Neighbor order (matches Wilson 2007 / gdaldem convention):
        n0=NW  n1=N   n2=NE
        n3=W          n4=E
        n5=SW  n6=S   n7=SE
    """
    p = np.pad(band, 1, mode="edge")
    center = p[1:-1, 1:-1]
    neighbors = [
        p[0:-2, 0:-2],  # NW
        p[0:-2, 1:-1],  # N
        p[0:-2, 2:],  # NE
        p[1:-1, 0:-2],  # W
        p[1:-1, 2:],  # E
        p[2:, 0:-2],  # SW
        p[2:, 1:-1],  # S
        p[2:, 2:],  # SE
    ]
    return center, neighbors


def tri(ds) -> bytes:
    """Compute Terrain Ruggedness Index (Riley 1999, ``gdaldem`` default).

    TRI = sqrt of the sum of squared differences between the center cell and
    each of its 8 neighbours (3x3 edge-replicated window).  This matches the
    Riley algorithm that ``gdaldem TRI`` has used by default since GDAL 3.3.
    Flat terrain yields 0.

    Args:
        ds: Open rasterio DatasetReader.  Band 1 is used as the DEM.

    Returns:
        Single-band Float32 GTiff bytes; nodata = -9999.
    """
    band, valid = read_masked(ds)
    center, nbrs = _neighbors(band)
    result = np.sqrt(
        np.sum(np.stack([(center - n) ** 2 for n in nbrs], axis=0), axis=0)
    )
    return emit(ds, result, _NODATA, propagate_invalid(valid), "float32")


def tpi(ds) -> bytes:
    """Compute Topographic Position Index.

    TPI = center - mean(8 neighbours).  Positive values are local highs;
    negative values are local lows; flat terrain yields 0.

    Args:
        ds: Open rasterio DatasetReader.  Band 1 is used as the DEM.

    Returns:
        Single-band Float32 GTiff bytes; nodata = -9999.
    """
    band, valid = read_masked(ds)
    center, nbrs = _neighbors(band)
    result = center - np.mean(np.stack(nbrs, axis=0), axis=0)
    return emit(ds, result, _NODATA, propagate_invalid(valid), "float32")


def roughness(ds) -> bytes:
    """Compute terrain roughness (max - min over the 3x3 window).

    Roughness = max(3x3 window including centre) - min(3x3 window including
    centre).  Flat terrain yields 0.

    Args:
        ds: Open rasterio DatasetReader.  Band 1 is used as the DEM.

    Returns:
        Single-band Float32 GTiff bytes; nodata = -9999.
    """
    band, valid = read_masked(ds)
    center, nbrs = _neighbors(band)
    window = np.stack([center] + nbrs, axis=0)  # shape (9, H, W)
    result = np.max(window, axis=0) - np.min(window, axis=0)
    return emit(ds, result, _NODATA, propagate_invalid(valid), "float32")


def _parse_color_table(path: str, band_min: float, band_max: float) -> tuple:
    """Parse a gdaldem-style color table file.

    Returns:
        (stops, nv_color, has_alpha) where:
          - stops: sorted list of (elevation, (r, g, b[, a])) tuples
          - nv_color: tuple (r, g, b[, a]) or None
          - has_alpha: True when any entry includes an alpha channel
    """
    stops = []
    nv_color = None
    has_alpha = False

    with open(path) as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            # Allow both whitespace and comma separators.
            import re

            parts = re.split(r"[\s,]+", line)
            if len(parts) < 4:
                continue

            key_tok = parts[0].lower()
            try:
                r, g, b = int(parts[1]), int(parts[2]), int(parts[3])
            except (ValueError, IndexError):
                continue

            a = None
            if len(parts) >= 5:
                try:
                    a = int(parts[4])
                    has_alpha = True
                except ValueError:
                    pass
            # Note: exactly 4 tokens = (elev, R, G, B) — no alpha channel.

            color = (r, g, b, a) if a is not None else (r, g, b)

            if key_tok in ("nv", "nodata"):
                nv_color = color
                continue
            if key_tok == "default":
                # Treat 'default' as a fallback; skip for now (np.interp clamps).
                continue
            if key_tok.endswith("%"):
                try:
                    pct = float(key_tok[:-1]) / 100.0
                    elev = band_min + pct * (band_max - band_min)
                except ValueError:
                    continue
            else:
                try:
                    elev = float(key_tok)
                except ValueError:
                    continue

            stops.append((elev, color))

    stops.sort(key=lambda x: x[0])
    return stops, nv_color, has_alpha


def color_relief(ds, color_table_path: str) -> bytes:
    """Map a single-band DEM through a gdaldem-style color table to RGB(A) Byte output.

    Parses the color table (elevation, R, G, B[, A] per line; ``nv`` for NoData;
    ``<n>%`` for percentage of the band range; ``#`` comments and blank lines
    ignored; whitespace or comma separators).  Applies linear interpolation per
    channel via ``np.interp`` (out-of-range values clamp to first/last stop).
    Outputs a 3-band (RGB) or 4-band (RGBA) Byte GTiff.

    Args:
        ds:               Open rasterio DatasetReader.  Band 1 is used as the DEM.
        color_table_path: Path to a gdaldem color file.

    Returns:
        3- or 4-band Byte GTiff bytes; nodata not set on output.
    """
    dem = ds.read(1).astype("float64")

    # Compute min/max excluding NoData for % stop resolution.
    nodata = ds.nodata
    if nodata is not None:
        valid_mask = dem != nodata
        valid = dem[valid_mask]
    else:
        valid_mask = np.ones(dem.shape, dtype=bool)
        valid = dem.ravel()

    if valid.size == 0:
        band_min, band_max = 0.0, 1.0
    else:
        band_min = float(valid.min())
        band_max = float(valid.max())
    if band_min == band_max:
        band_max = band_min + 1.0

    stops, nv_color, has_alpha = _parse_color_table(
        color_table_path, band_min, band_max
    )

    if not stops:
        raise ValueError(f"No valid color stops found in {color_table_path!r}")

    nbands = 4 if has_alpha else 3
    elevs = np.array([s[0] for s in stops], dtype="float64")

    # Build one channel array per band (fill missing alpha with 255).
    channels = []
    for ch in range(nbands):
        default_val = 255.0 if ch == 3 else 0.0
        vals = np.array(
            [float(s[1][ch]) if ch < len(s[1]) else default_val for s in stops],
            dtype="float64",
        )
        channels.append(np.interp(dem, elevs, vals))

    # Apply nv color to NoData pixels.
    if nodata is not None and nv_color is not None:
        nd_mask = ~valid_mask
        if nd_mask.any():
            for ch in range(nbands):
                default_val = 255.0 if ch == 3 else 0.0
                fill = float(nv_color[ch]) if ch < len(nv_color) else default_val
                channels[ch][nd_mask] = fill

    # Stack to (nbands, H, W) and cast to uint8.
    out_arr = np.clip(np.round(np.stack(channels, axis=0)), 0, 255).astype("uint8")

    profile = ds.profile.copy()
    profile.update(driver="GTiff", count=nbands, dtype="uint8", nodata=None)

    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(out_arr)
        return mf.read()


def hillshade(
    ds,
    azimuth: float = 315.0,
    altitude: float = 45.0,
    z_factor: float = 1.0,
) -> bytes:
    """Compute hillshade matching ``gdaldem hillshade`` (Horn).

    Uses GDAL's exact gradient-magnitude rational form (the cosine-of-the-
    incidence-angle ``cang`` expression from ``GDALHillshadeAlg``), expressed
    in terms of pyrx's Horn gradients (``dzdx``, ``dzdy``).  Azimuth and
    altitude follow GDAL's convention: azimuth is degrees clockwise from north
    (default 315 = NW); altitude is the sun's angle above the horizon in
    degrees (default 45).  ``z_factor`` is vertical exaggeration.

    Args:
        ds:        Open rasterio DatasetReader.  Band 1 used as DEM.
        azimuth:   Sun azimuth in degrees clockwise from north (default 315).
        altitude:  Sun altitude above horizon in degrees (default 45).
        z_factor:  Vertical exaggeration (default 1.0).

    Returns:
        Single-band Byte (uint8) GTiff bytes; valid values [1..255],
        0 reserved for NoData (gdaldem convention).
    """
    dzdx, dzdy, valid = _horn_gradients(ds)

    z = float(z_factor)
    alt_rad = float(altitude) * np.pi / 180.0
    az_rad = float(azimuth) * np.pi / 180.0
    sin_alt = np.sin(alt_rad)
    cos_alt = np.cos(alt_rad)
    sin_az = np.sin(az_rad)
    cos_az = np.cos(az_rad)

    # GDAL gdaldem hillshade (Horn) rational form, in pyrx gradient terms.
    cang = (sin_alt + cos_alt * z * (dzdy * cos_az - dzdx * sin_az)) / np.sqrt(
        1.0 + z * z * (dzdx * dzdx + dzdy * dzdy)
    )
    # gdaldem scaling: valid pixels map to [1, 255]; cang<=0 floors to 1.
    # 0 is reserved exclusively for NoData.
    hs = np.where(cang <= 0.0, 1.0, 1.0 + 254.0 * cang)

    return emit(ds, hs, 0, propagate_invalid(valid), "uint8")
