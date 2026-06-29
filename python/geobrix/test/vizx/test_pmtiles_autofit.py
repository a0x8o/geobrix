"""Tests for vizx PMTiles auto-fit reduction (auto_shard='sample').

The reducer down-zooms an oversized archive (drops the highest zoom levels)
until the base64-rendered embed size fits the interactive budget, so a single
large archive can still display interactively at reduced detail. Tier-agnostic:
works for both raster and vector tiles by rebuilding a smaller archive from the
tiles already present (no re-tiling, no tippecanoe).
"""

import io

import pytest
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer

from databricks.labs.gbx.vizx._pmtiles_autofit import autofit_archive


def _build_archive(tiles, tile_type=TileType.MVT, *, name="demo"):
    """Build a PMTiles archive from (z, x, y, payload) tuples."""
    buf = io.BytesIO()
    w = Writer(buf)
    zs = [z for z, _, _, _ in tiles]
    header = {
        "tile_type": tile_type,
        "tile_compression": Compression.NONE,
        "internal_compression": Compression.GZIP,
        "min_zoom": min(zs),
        "max_zoom": max(zs),
        "min_lon_e7": int(-122.52 * 1e7),
        "min_lat_e7": int(37.70 * 1e7),
        "max_lon_e7": int(-122.35 * 1e7),
        "max_lat_e7": int(37.83 * 1e7),
        "center_zoom": min(zs),
        "center_lon_e7": int(-122.44 * 1e7),
        "center_lat_e7": int(37.76 * 1e7),
    }
    for z, x, y, payload in sorted(
        tiles, key=lambda t: zxy_to_tileid(t[0], t[1], t[2])
    ):
        w.write_tile(zxy_to_tileid(z, x, y), payload)
    w.finalize(header, {"name": name, "vector_layers": [{"id": "demo"}]})
    return buf.getvalue()


def _multi_zoom_archive(tile_type=TileType.MVT, payload_size=4096):
    """An archive whose size is dominated by the highest zoom level.

    z0: 1 tile, z1: ~4 tiles, z2: ~16 tiles. Each tile gets DISTINCT, effectively
    incompressible bytes (os.urandom) so PMTiles internal compression + tile
    dedup can't collapse them -- dropping z2 then z1 monotonically shrinks the
    archive (the property the reducer relies on).
    """
    import os

    tiles = []
    for z in range(3):
        n = 2**z
        for x in range(n):
            for y in range(n):
                tiles.append((z, x, y, os.urandom(payload_size)))
    return _build_archive(tiles, tile_type)


def _tile_zooms(archive_bytes):
    from pmtiles.reader import MemorySource, all_tiles

    return sorted({z for (z, _, _), _ in all_tiles(MemorySource(archive_bytes))})


def test_autofit_drops_high_zoom_until_under_budget():
    """A multi-zoom archive over budget is reduced by dropping top zoom levels."""
    archive = _multi_zoom_archive()
    full_zooms = _tile_zooms(archive)
    assert full_zooms == [0, 1, 2]

    # Budget small enough to require dropping the densest (z2) level, but big
    # enough to keep z0/z1. Budget is in MB; archive here is tens of KB, so use
    # a fractional MB that lands between the z<=1 and z<=2 sizes.
    full_size = len(archive)
    # Target ~ between the z<=1 subset and the full archive.
    target_mb = (full_size * 0.6) / 1_048_576

    reduced, report = autofit_archive(archive, max_embed_mb=target_mb)

    # Reduced archive fits the (base64-inflated) budget.
    assert len(reduced) * (4.0 / 3.0) <= target_mb * 1_048_576
    # It dropped the top zoom(s) but kept the coarse ones.
    reduced_zooms = _tile_zooms(reduced)
    assert reduced_zooms, "reduced archive must keep at least one zoom level"
    assert max(reduced_zooms) < max(full_zooms), "top zoom should have been dropped"
    assert 0 in reduced_zooms, "coarsest zoom must be preserved"
    # Report is informative.
    assert report["dropped_zooms"]
    assert report["kept_max_zoom"] == max(reduced_zooms)
    assert report["fits"] is True


def test_autofit_noop_when_already_fits():
    """When the archive already fits, return it unchanged with fits=True."""
    archive = _multi_zoom_archive()
    big_budget_mb = (len(archive) * 10) / 1_048_576
    reduced, report = autofit_archive(archive, max_embed_mb=big_budget_mb)
    assert reduced == archive
    assert report["fits"] is True
    assert report["dropped_zooms"] == []


def test_autofit_preserves_tile_type_vector():
    """Reduced vector archive is still readable and reports MVT tile type."""
    from databricks.labs.gbx.pmtiles import pmtiles_info

    archive = _multi_zoom_archive(TileType.MVT)
    target_mb = (len(archive) * 0.5) / 1_048_576
    reduced, _ = autofit_archive(archive, max_embed_mb=target_mb)
    info = pmtiles_info(reduced)
    assert info["tile_type"] == "mvt"


def test_autofit_keeps_at_least_coarsest_even_if_over_budget():
    """If even the coarsest level exceeds budget, keep it (degenerate) and flag
    fits=False so the caller can route to static rather than emit nothing."""
    archive = _multi_zoom_archive(payload_size=4096)
    # Absurdly tiny budget: even z0 alone won't fit.
    reduced, report = autofit_archive(archive, max_embed_mb=0.0000001)
    reduced_zooms = _tile_zooms(reduced)
    assert reduced_zooms == [0], "must retain only the coarsest level as last resort"
    assert report["fits"] is False


def test_autofit_rejects_bad_budget():
    archive = _multi_zoom_archive()
    with pytest.raises(ValueError):
        autofit_archive(archive, max_embed_mb=0)
    with pytest.raises(ValueError):
        autofit_archive(archive, max_embed_mb=-1)
