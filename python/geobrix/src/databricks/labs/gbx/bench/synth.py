"""Deterministic multi-tile input synthesis for the `tile_array` bench adapter.

The C3 multi-tile functions (rst_frombands / rst_combineavg / rst_merge) each
consume an ARRAY of tiles, but a corpus row carries ONE tile. This module derives
the multi-tile input from that single source and writes it to disk ONCE, so BOTH
benchmark engines (pyrx + heavy) read byte-identical input files.

Write-once-read-both is the safest cross-engine input-identity guarantee: rather
than each engine re-synthesizing (and risking subtly different GTiff encodings),
the pyrx runner synthesizes the files, and the heavy runner reads the SAME paths.
Both engines locate them via the deterministic ``synth_dir`` (computed identically
from corpus root + tile path + fn), so no coordination channel is needed beyond
agreeing on the path math.

Synthesis recipes (deterministic, idempotent):
  * ``frombands``  -> split the N-band source into N single-band GTiffs (N paths,
                      in band order; element i carries source band i).
  * ``combineavg`` -> 2 ALIGNED copies of the source (identical
                      transform/shape/CRS) so the per-pixel mean is well-defined.
  * ``merge``      -> 2 copies with OFFSET geotransform origins (origin shifted by
                      half the tile width/height) so their extents tile into a
                      union for the mosaic.

Idempotent: a given (source, fn) always writes to the same paths with the same
bytes; if the outputs already exist they are reused (re-synthesis is skipped).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import List

import rasterio
from rasterio.transform import Affine

# Recipes and their output filenames. The element ORDER is the band/merge order
# the consuming function relies on (frombands element 0 -> band 1, etc.).
_RECIPES = ("frombands", "combineavg", "merge")


def synth_dir(corpus_root, tile_rel_path, fn: str) -> str:
    """Deterministic output dir for a (corpus tile, fn) synthesis.

    Both engines compute this identically from the corpus root, the tile's
    corpus-relative path, and the function name, so the heavy runner reads the
    exact files the pyrx runner wrote. Lives UNDER the corpus root so it travels
    with the corpus and is cleaned up with it.
    """
    # A short stable hash of the tile path keeps the dir name filesystem-safe and
    # collision-free across nested corpus layouts, while staying deterministic.
    stem = hashlib.sha1(str(tile_rel_path).encode("utf-8")).hexdigest()[:12]
    return str(Path(corpus_root) / "_synth" / fn / stem)


def synthesize(src_path: str, fn: str, out_dir: str) -> List[str]:
    """Synthesize the multi-tile input for ``fn`` from ``src_path``.

    Returns the list of output GTiff paths in consumption order. Writes the files
    once; if they already exist (idempotent re-run) they are returned as-is.
    """
    if fn not in _RECIPES:
        raise ValueError(f"unknown synth recipe: {fn}")
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    if fn == "frombands":
        return _synth_frombands(src_path, out)
    if fn == "combineavg":
        return _synth_combineavg(src_path, out)
    return _synth_merge(src_path, out)


def _write(profile: dict, bands: list, path: Path) -> None:
    """Write ``bands`` (list of 2-D arrays) to ``path`` as a GTiff."""
    with rasterio.open(path, "w", **profile) as dst:
        for i, arr in enumerate(bands, start=1):
            dst.write(arr, i)


def _synth_frombands(src_path: str, out: Path) -> List[str]:
    """Split the N-band source into N single-band GTiffs (band order preserved)."""
    paths: List[str] = []
    with rasterio.open(src_path) as ds:
        n = ds.count
        for b in range(1, n + 1):
            p = out / f"band_{b:02d}.tif"
            paths.append(str(p))
            if p.exists():
                continue
            profile = ds.profile.copy()
            profile.update(driver="GTiff", count=1)
            _write(profile, [ds.read(b)], p)
    return paths


def _synth_combineavg(src_path: str, out: Path) -> List[str]:
    """Two ALIGNED copies of the source (identical grid) for a per-pixel mean."""
    paths: List[str] = []
    with rasterio.open(src_path) as ds:
        data = ds.read()
        profile = ds.profile.copy()
        profile.update(driver="GTiff")
        for i in range(2):
            p = out / f"copy_{i}.tif"
            paths.append(str(p))
            if p.exists():
                continue
            _write(profile, [data[b] for b in range(data.shape[0])], p)
    return paths


def _synth_merge(src_path: str, out: Path) -> List[str]:
    """Two copies with OFFSET origins so their extents tile into a union mosaic.

    The first copy keeps the source geotransform; the second shifts its origin by
    half the tile width (east) and half the tile height (south) so the two
    extents are distinct and abut/overlap into a larger union. The pixel size and
    CRS are unchanged so both copies snap onto one common grid for the mosaic.
    """
    paths: List[str] = []
    with rasterio.open(src_path) as ds:
        data = ds.read()
        base = ds.transform
        w, h = ds.width, ds.height
        # half-extent offsets in CRS units (a >= 0 east shift, a downward south
        # shift since pixel height base.e is negative for north-up rasters).
        dx = base.a * (w / 2.0)
        dy = base.e * (h / 2.0)
        offset = Affine(base.a, base.b, base.c + dx, base.d, base.e, base.f + dy)
        for i, tr in enumerate((base, offset)):
            p = out / f"part_{i}.tif"
            paths.append(str(p))
            if p.exists():
                continue
            profile = ds.profile.copy()
            profile.update(driver="GTiff", transform=tr)
            _write(profile, [data[b] for b in range(data.shape[0])], p)
    return paths
