"""Unit tests for recursive path listing with regex filter."""

import os

import pytest

from databricks.labs.gbx.pyrx.ds import _listing


@pytest.fixture
def tree(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "one.tif").write_bytes(b"x")
    (tmp_path / "a" / "two.tif").write_bytes(b"x")
    (tmp_path / "a" / "skip.txt").write_bytes(b"x")
    (tmp_path / "b").mkdir()
    (tmp_path / "b" / "three.tif").write_bytes(b"x")
    return tmp_path


def test_lists_all_files_recursively_default_regex(tree):
    files = _listing.list_files(str(tree), filter_regex=".*")
    assert len(files) == 4
    assert all(os.path.isabs(f) for f in files)


def test_regex_filters_by_full_path(tree):
    files = _listing.list_files(str(tree), filter_regex=r".*\.tif$")
    assert len(files) == 3
    assert all(f.endswith(".tif") for f in files)


def test_single_file_path_returns_that_file(tree):
    target = str(tree / "a" / "one.tif")
    files = _listing.list_files(target, filter_regex=".*")
    assert files == [target]


def test_no_match_raises(tree):
    with pytest.raises(FileNotFoundError):
        _listing.list_files(str(tree), filter_regex=r".*\.nope$")
