"""Unit tests for _canonical_ext, _complete_ext, and _resolve_single_file_output.

These are pure-Python / pure-unit tests (no Spark, no Docker needed).
"""

import os

import pytest

from databricks.labs.gbx.ds.vector import (
    _canonical_ext,
    _complete_ext,
    _resolve_single_file_output,
)


def test_canonical_ext():
    assert _canonical_ext("GPKG", False) == ".gpkg"
    assert _canonical_ext("GeoJSON", False) == ".geojson"
    assert _canonical_ext("ESRI Shapefile", True) == ".shp.zip"
    assert _canonical_ext("OpenFileGDB", False) == ".gdb"
    assert _canonical_ext("OpenFileGDB", True) == ".gdb.zip"


def test_complete_ext_appends_when_missing():
    assert _complete_ext("roads", ".shp.zip") == "roads.shp.zip"
    assert _complete_ext("roads.shp", ".shp.zip") == "roads.shp.zip"  # partial -> complete
    assert _complete_ext("roads.shp.zip", ".shp.zip") == "roads.shp.zip"  # already complete
    assert _complete_ext("city", ".gpkg") == "city.gpkg"
    assert _complete_ext("city.gpkg", ".gpkg") == "city.gpkg"


def test_complete_ext_rejects_wrong_geo_ext():
    with pytest.raises(ValueError, match="expected .shp.zip"):
        _complete_ext("roads.gpkg", ".shp.zip")


# ---- Task A2: _resolve_single_file_output ----


def test_case1_filename_given(tmp_path):
    out = _resolve_single_file_output(str(tmp_path / "newdir"), "roads", ".shp.zip")
    assert out == str(tmp_path / "newdir" / "roads.shp.zip")
    assert os.path.isdir(tmp_path / "newdir")  # parent created


def test_case2_existing_dir_no_filename(tmp_path):
    d = tmp_path / "roads_dir"
    d.mkdir()
    out = _resolve_single_file_output(str(d), None, ".shp.zip")
    assert out == str(d / "roads_dir.shp.zip")  # named after the dir, under it


def test_case3_stem_path_no_filename(tmp_path):
    out = _resolve_single_file_output(str(tmp_path / "sub" / "roads"), None, ".gpkg")
    assert out == str(tmp_path / "sub" / "roads.gpkg")  # complete ext on the stem
    assert os.path.isdir(tmp_path / "sub")  # parent created


def test_filename_extension_completed(tmp_path):
    out = _resolve_single_file_output(str(tmp_path), "roads.shp", ".shp.zip")
    assert out == str(tmp_path / "roads.shp.zip")
