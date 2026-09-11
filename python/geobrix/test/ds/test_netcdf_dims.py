"""TDD tests for netcdf_gbx dimIndex and fanout options.

Fixture: (time=3, level=2, lat=3, lon=4) variable 'temp'.
Slice (t, l) has all values == t * 10 + l, so exact slice identity
is detectable without ambiguity.

Also covers: default backward-compat, overlap ValueError, var-without-fanout-dim,
unknown/typo'd dim validation, duplicate dimIndex key warning, coord values in metadata.
"""

from __future__ import annotations

import logging
from typing import List, Tuple

import numpy as np
import pytest
from netCDF4 import Dataset
from rasterio.io import MemoryFile

from databricks.labs.gbx.ds import _netcdf
from databricks.labs.gbx.ds.netcdf import NetcdfRasterReader
from databricks.labs.gbx.ds.raster import _FilePartition

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _write_4d_grid(path: str, ntime: int = 3, nlevel: int = 2) -> None:
    """Write a (time, level, lat, lon) NetCDF with known per-slice values.

    Value at (t, l, :, :) == float(t * 10 + l).
    Coordinates: time=[0.0,1.0,2.0], level=[1000.0,500.0].
    """
    with Dataset(path, "w") as ds:
        ds.createDimension("time", ntime)
        ds.createDimension("level", nlevel)
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)

        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]

        t_var = ds.createVariable("time", "f8", ("time",))
        t_var[:] = [float(i) for i in range(ntime)]

        l_var = ds.createVariable("level", "f8", ("level",))
        l_var[:] = [1000.0, 500.0][:nlevel]

        v = ds.createVariable(
            "temp", "f4", ("time", "level", "lat", "lon"), fill_value=-9999.0
        )
        for t in range(ntime):
            for lev in range(nlevel):
                v[t, lev, :, :] = float(t * 10 + lev)


def _write_2d_grid(path: str) -> None:
    """Write a (lat, lon) grid — no leading dims."""
    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]
        v = ds.createVariable("sst", "f4", ("lat", "lon"), fill_value=-9999.0)
        v[:] = np.arange(12, dtype="float32").reshape(3, 4)


def _write_mixed_grid(path: str) -> None:
    """Write a file with two variables in the same grid extent.

    - 'temp' (time=3, lat=3, lon=4): value at time t == float(t).
    - 'sst'  (lat=3, lon=4): no leading dims.
    Coordinates: time=[0.0, 1.0, 2.0].
    Used to test per-variable fanout: 'temp' expands, 'sst' stays single-row.
    """
    with Dataset(path, "w") as ds:
        ds.createDimension("time", 3)
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)

        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]

        t_var = ds.createVariable("time", "f8", ("time",))
        t_var[:] = [0.0, 1.0, 2.0]

        temp = ds.createVariable(
            "temp", "f4", ("time", "lat", "lon"), fill_value=-9999.0
        )
        for t in range(3):
            temp[t, :, :] = float(t)

        sst = ds.createVariable("sst", "f4", ("lat", "lon"), fill_value=-9999.0)
        sst[:] = np.arange(12, dtype="float32").reshape(3, 4)


def _read_reader(path: str, options: dict) -> List[Tuple]:
    """Run NetcdfRasterReader.read() directly and collect all yielded tuples."""
    opts = {"path": path, **options}
    reader = NetcdfRasterReader(opts)
    partition = _FilePartition(path, -1)
    return list(reader.read(partition))


def _tile_mean(row: Tuple) -> float:
    """Decode the (source, tile) row and return the mean pixel value."""
    _, tile_tuple = row
    _, raster_bytes, _ = tile_tuple
    with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as ds:
        return float(ds.read(1).mean())


# ---------------------------------------------------------------------------
# Helper tests: leading_dims
# ---------------------------------------------------------------------------


def test_leading_dims_returns_non_spatial(tmp_path):
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        ld = _netcdf.leading_dims(ds, "temp")
    assert ld == [("time", 3), ("level", 2)]


def test_leading_dims_empty_for_2d_var(tmp_path):
    p = str(tmp_path / "g.nc")
    _write_2d_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        ld = _netcdf.leading_dims(ds, "sst")
    assert ld == []


# ---------------------------------------------------------------------------
# Helper tests: array_2d with sel
# ---------------------------------------------------------------------------


def test_array_2d_sel_reads_named_slice(tmp_path):
    """array_2d(sel={"time": 2, "level": 1}) reads slice (t=2, l=1) → all 21."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        arr = _netcdf.array_2d(ds, "temp", sel={"time": 2, "level": 1})
    np.testing.assert_allclose(arr, np.full((3, 4), 21.0, dtype="float32"), rtol=1e-6)


def test_array_2d_sel_index_zero_no_warn(tmp_path, caplog):
    """Explicitly selecting index 0 via sel must NOT warn (it's intentional)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.ds._netcdf"):
            _netcdf.array_2d(ds, "temp", sel={"time": 0, "level": 0})
    warn_msgs = [
        r.getMessage() for r in caplog.records if "leading dimension" in r.getMessage()
    ]
    assert warn_msgs == [], f"unexpected warn for explicit sel=0: {warn_msgs}"


def test_array_2d_sel_out_of_range_raises(tmp_path):
    """Out-of-range index → ValueError."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        with pytest.raises(ValueError, match="out of range"):
            _netcdf.array_2d(ds, "temp", sel={"time": 99})


# ---------------------------------------------------------------------------
# (a) dimIndex option
# ---------------------------------------------------------------------------


def test_dimindex_reads_correct_slice(tmp_path):
    """dimIndex='time=2,level=1' reads the (t=2, l=1) slice → mean=21.0."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"dimIndex": "time=2,level=1"})
    assert len(rows) == 1
    assert pytest.approx(_tile_mean(rows[0]), abs=1e-4) == 21.0


def test_dimindex_source_contains_suffix(tmp_path):
    """dimIndex source must be NETCDF:'path':var[level=1,time=2] (sorted suffix)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"dimIndex": "time=2,level=1"})
    source, _ = rows[0]
    assert source.endswith(":temp[level=1,time=2]"), f"unexpected source: {source}"


def test_dimindex_slice_in_metadata(tmp_path):
    """dimIndex: tile metadata must contain sliceDims key."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"dimIndex": "time=2,level=1"})
    _, (_, _, meta) = rows[0]
    assert "sliceDims" in meta
    assert "level=1" in meta["sliceDims"]
    assert "time=2" in meta["sliceDims"]


# ---------------------------------------------------------------------------
# (b) fanout="time,level" — Cartesian product
# ---------------------------------------------------------------------------


def test_fanout_both_dims_emits_cartesian(tmp_path):
    """fanout='time,level' → 3×2 = 6 rows, each the correct slice."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time,level"})
    assert len(rows) == 6


def test_fanout_both_dims_correct_values(tmp_path):
    """Each fanout row carries the correct mean value."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time,level"})
    means = sorted(_tile_mean(r) for r in rows)
    expected = sorted(float(t * 10 + lev) for t in range(3) for lev in range(2))
    np.testing.assert_allclose(means, expected, atol=1e-4)


def test_fanout_both_dims_unique_sources(tmp_path):
    """All 6 fanout rows must have unique source strings."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time,level"})
    sources = [r[0] for r in rows]
    assert len(set(sources)) == 6, f"non-unique sources: {sources}"


def test_fanout_both_dims_sources_have_suffix(tmp_path):
    """Every fanout source must contain the bracketed slice suffix."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time,level"})
    for source, _ in rows:
        assert "[" in source and "]" in source, f"missing suffix in source: {source}"


def test_fanout_slice_info_in_metadata(tmp_path):
    """Each fanout row has sliceDims in its tile metadata."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time,level"})
    for _, (_, _, meta) in rows:
        assert "sliceDims" in meta


# ---------------------------------------------------------------------------
# (c) fanout="time" only — level pinned to 0 with warn
# ---------------------------------------------------------------------------


def test_fanout_time_only_emits_3_rows(tmp_path):
    """fanout='time' with no level dimIndex → 3 rows (level pinned at 0)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time"})
    assert len(rows) == 3


def test_fanout_time_only_values_correct(tmp_path):
    """With fanout='time', each row is the t=0/1/2 slice at level=0 → 0,10,20."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time"})
    means = sorted(_tile_mean(r) for r in rows)
    np.testing.assert_allclose(means, [0.0, 10.0, 20.0], atol=1e-4)


def test_fanout_time_only_warns_on_level(tmp_path, caplog):
    """fanout='time' must warn that 'level' is pinned to 0 (size>1, not selected)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.ds._netcdf"):
        _read_reader(p, {"fanout": "time"})
    warn_msgs = [
        r.getMessage() for r in caplog.records if "leading dimension" in r.getMessage()
    ]
    assert any(
        "level" in m for m in warn_msgs
    ), f"expected a warning for unpinned 'level' dim; got: {warn_msgs}"


# ---------------------------------------------------------------------------
# (d) Default behavior (no options) — backward compat
# ---------------------------------------------------------------------------


def test_default_one_row_per_variable(tmp_path):
    """No options → one row per variable (unchanged)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {})
    assert len(rows) == 1


def test_default_source_format_unchanged(tmp_path):
    """No options → source is NETCDF:'path':var with no suffix (byte-compat)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {})
    source, _ = rows[0]
    assert source == f'NETCDF:"{p}":temp'


def test_default_warns_on_multi_time(tmp_path, caplog):
    """No options on a time>1 variable → still warns (unchanged)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.ds._netcdf"):
        _read_reader(p, {})
    msgs = [r.getMessage() for r in caplog.records]
    assert any("time" in m and "leading dimension" in m for m in msgs)


def test_default_reads_index_zero_slice(tmp_path):
    """No options → reads slice (t=0, l=0) → mean=0."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {})
    assert pytest.approx(_tile_mean(rows[0]), abs=1e-4) == 0.0


def test_default_no_slice_dims_in_metadata(tmp_path):
    """No options → sliceDims must NOT appear in tile metadata (strict compat)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {})
    _, (_, _, meta) = rows[0]
    assert "sliceDims" not in meta


# ---------------------------------------------------------------------------
# (e) Overlap guard
# ---------------------------------------------------------------------------


def test_overlap_dimindex_and_fanout_raises(tmp_path):
    """A dim in both dimIndex and fanout must raise ValueError."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with pytest.raises(ValueError, match="dimIndex.*fanout|fanout.*dimIndex|both"):
        _read_reader(p, {"dimIndex": "time=1", "fanout": "time"})


# ---------------------------------------------------------------------------
# (f) Variable lacking the fanout dim → single row, no expansion
#     (uses a mixed file so the dim IS known to at least one var)
# ---------------------------------------------------------------------------


def test_fanout_var_lacking_dim_emits_one_row(tmp_path):
    """In a mixed file, the var without the fanout dim emits exactly one row."""
    p = str(tmp_path / "m.nc")
    _write_mixed_grid(p)
    rows = _read_reader(p, {"fanout": "time"})
    # temp → 3 rows (time=0,1,2); sst → 1 row (no time dim)
    sst_rows = [r for r in rows if ":sst" in r[0]]
    assert len(sst_rows) == 1


def test_fanout_var_lacking_dim_has_no_suffix(tmp_path):
    """In a mixed file, the var without the fanout dim has a bare source."""
    p = str(tmp_path / "m.nc")
    _write_mixed_grid(p)
    rows = _read_reader(p, {"fanout": "time"})
    sst_sources = [r[0] for r in rows if ":sst" in r[0]]
    assert len(sst_sources) == 1
    assert "[" not in sst_sources[0], f"unexpected suffix: {sst_sources[0]}"


def test_fanout_total_rows_mixed_file(tmp_path):
    """In a mixed file, fanout='time' → 3 (temp) + 1 (sst) = 4 total rows."""
    p = str(tmp_path / "m.nc")
    _write_mixed_grid(p)
    rows = _read_reader(p, {"fanout": "time"})
    assert len(rows) == 4


# ---------------------------------------------------------------------------
# (g) Unknown/typo'd dim → ValueError
# ---------------------------------------------------------------------------


def test_dimindex_unknown_dim_raises(tmp_path):
    """A typo'd dim in dimIndex (unknown to all selected vars) → ValueError."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with pytest.raises(ValueError, match="tyme|unknown"):
        _read_reader(p, {"dimIndex": "tyme=2"})


def test_fanout_unknown_dim_raises(tmp_path):
    """A typo'd dim in fanout (unknown to all selected vars) → ValueError."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with pytest.raises(ValueError, match="tyme|unknown"):
        _read_reader(p, {"fanout": "tyme"})


def test_dimindex_valid_error_lists_known_dims(tmp_path):
    """The ValueError message lists the valid leading dims to aid diagnosis."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with pytest.raises(ValueError) as exc_info:
        _read_reader(p, {"dimIndex": "tyme=2"})
    msg = str(exc_info.value)
    assert "time" in msg and "level" in msg, f"error should name valid dims; got: {msg}"


# ---------------------------------------------------------------------------
# (h) Duplicate key in dimIndex → UserWarning, last value wins
# ---------------------------------------------------------------------------


def test_dimindex_duplicate_key_warns(tmp_path):
    """dimIndex with a repeated dim emits UserWarning; last value is used."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with pytest.warns(UserWarning, match="duplicate"):
        rows = _read_reader(p, {"dimIndex": "time=1,time=2"})
    # last value wins: time=2, level falls to 0 → value = 2*10+0 = 20.0
    assert pytest.approx(_tile_mean(rows[0]), abs=1e-4) == 20.0


# ---------------------------------------------------------------------------
# (i) Coordinate values in tile metadata
# ---------------------------------------------------------------------------


def test_dimindex_coord_values_in_metadata(tmp_path):
    """dimIndex: sliceCoord_time and sliceCoord_level carry the actual coord values."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"dimIndex": "time=2,level=1"})
    _, (_, _, meta) = rows[0]
    # Fixture: time=[0.0,1.0,2.0] → time[2]=2.0; level=[1000.0,500.0] → level[1]=500.0
    assert "sliceCoord_time" in meta, "sliceCoord_time missing from metadata"
    assert "sliceCoord_level" in meta, "sliceCoord_level missing from metadata"
    assert float(meta["sliceCoord_time"]) == pytest.approx(2.0)
    assert float(meta["sliceCoord_level"]) == pytest.approx(500.0)


def test_fanout_coord_values_in_metadata(tmp_path):
    """fanout: each row's sliceCoord_* carries the correct coordinate value."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time,level"})
    # Every row must carry both coord keys.
    for _, (_, _, meta) in rows:
        assert "sliceCoord_time" in meta
        assert "sliceCoord_level" in meta
    # Build the set of (time_coord, level_coord) pairs across all rows.
    coord_pairs = frozenset(
        (float(meta["sliceCoord_time"]), float(meta["sliceCoord_level"]))
        for _, (_, _, meta) in rows
    )
    # Fixture: time=[0,1,2], level=[1000.0(idx0),500.0(idx1)]
    expected = frozenset(
        (float(t), 1000.0 if lev == 0 else 500.0) for t in range(3) for lev in range(2)
    )
    assert coord_pairs == expected, f"coord pairs mismatch: got {coord_pairs}"
