"""Unit tests for VectorGbxReader._members — recursive directory listing contract.

These are pure-unit tests (no Spark) that verify _members() enumerates matching
vector files. Tests cover:
  - recursive shapefile enumeration (Task B1 — the core change)
  - flat-directory regression (existing flat behaviour unchanged)
  - bare-file path passthrough (unchanged)
  - .gdb directory passthrough (unchanged)
"""

import os

import pytest

from databricks.labs.gbx.ds.vector import VectorGbxReader


def _make_reader(path: str, driver: str = "ESRI Shapefile") -> VectorGbxReader:
    """Build a VectorGbxReader pointing at *path* with *driver*, bypassing Spark."""
    return VectorGbxReader({"path": path, "driverName": driver})


def _touch_shp_bundle(directory: str, stem: str) -> None:
    """Create a minimal shapefile bundle (.shp + .dbf + .shx sidecars)."""
    for ext in (".shp", ".dbf", ".shx"):
        open(os.path.join(directory, stem + ext), "wb").close()


# ---------------------------------------------------------------------------
# Task B1 — recursive enumeration
# ---------------------------------------------------------------------------


def test_members_recursive_shapefiles(tmp_path):
    """_members() must recurse into subdirectories and enumerate ALL .shp files."""
    sub1 = tmp_path / "sub1"
    sub2 = tmp_path / "sub2"
    sub1.mkdir()
    sub2.mkdir()

    _touch_shp_bundle(str(sub1), "a")
    _touch_shp_bundle(str(sub2), "b")

    reader = _make_reader(str(tmp_path))
    members = reader._members()

    basenames = {os.path.basename(m) for m in members}
    assert basenames == {"a.shp", "b.shp"}, (
        f"Expected {{a.shp, b.shp}} but got {basenames}; full members={members}"
    )


def test_members_recursive_deeply_nested(tmp_path):
    """Recursion must work for arbitrarily deep nesting."""
    deep = tmp_path / "a" / "b" / "c"
    deep.mkdir(parents=True)
    _touch_shp_bundle(str(deep), "deep")

    reader = _make_reader(str(tmp_path))
    members = reader._members()

    assert any(m.endswith("deep.shp") for m in members), (
        f"Expected deep.shp in members but got {members}"
    )


# ---------------------------------------------------------------------------
# Regression — flat directory (unchanged behaviour)
# ---------------------------------------------------------------------------


def test_members_flat_directory_shapefiles(tmp_path):
    """Flat directory (no sub-dirs) still enumerates .shp files."""
    _touch_shp_bundle(str(tmp_path), "x")
    _touch_shp_bundle(str(tmp_path), "y")

    reader = _make_reader(str(tmp_path))
    members = reader._members()

    basenames = {os.path.basename(m) for m in members}
    assert basenames == {"x.shp", "y.shp"}


def test_members_sidecars_excluded(tmp_path):
    """Only .shp files (not .dbf/.shx sidecars) are returned for ESRI Shapefile driver."""
    _touch_shp_bundle(str(tmp_path), "only")

    reader = _make_reader(str(tmp_path))
    members = reader._members()

    # Only the .shp file should be in members; sidecars are filtered out
    assert all(m.endswith(".shp") or m.endswith(".shz") or m.endswith(".zip") for m in members), (
        f"Unexpected sidecar in members: {members}"
    )
    assert any(m.endswith("only.shp") for m in members)


# ---------------------------------------------------------------------------
# Regression — bare file path (unchanged behaviour)
# ---------------------------------------------------------------------------


def test_members_bare_file_returns_self(tmp_path):
    """A path pointing to a plain file returns [self.path] unchanged."""
    shp = tmp_path / "single.shp"
    shp.write_bytes(b"")

    reader = _make_reader(str(shp))
    members = reader._members()

    assert members == [str(shp)]


# ---------------------------------------------------------------------------
# Regression — .gdb directory passthrough (unchanged behaviour)
# ---------------------------------------------------------------------------


def test_members_gdb_dir_returns_self(tmp_path):
    """A .gdb directory is a single FileGDB dataset and must be returned as-is."""
    gdb = tmp_path / "dataset.gdb"
    gdb.mkdir()
    # Add a file inside so it's a real dir with contents
    (gdb / "a00000001.gdbtable").write_bytes(b"")

    reader = _make_reader(str(gdb), driver="OpenFileGDB")
    members = reader._members()

    assert members == [str(gdb)], (
        f"Expected [{gdb}] but got {members}"
    )


# ---------------------------------------------------------------------------
# Regression — empty directory falls back to [self.path] (unchanged behaviour)
# ---------------------------------------------------------------------------


def test_members_empty_dir_falls_back_to_path(tmp_path):
    """An empty directory with no matching files returns [self.path] as fallback."""
    empty = tmp_path / "empty"
    empty.mkdir()

    reader = _make_reader(str(empty))
    members = reader._members()

    assert members == [str(empty)]
