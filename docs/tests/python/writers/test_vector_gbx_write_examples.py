"""Executes the vector writer doc examples against real sample data (Docker).

Marked integration: requires the corpus at GBX_SAMPLE_DATA_ROOT and a Spark
session with the lightweight data sources registered. Run via gbx:test:python-docs.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import vector_gbx_write_examples as ex  # noqa: E402


@pytest.mark.integration
def test_write_vector_gbx(spark):
    ex.write_vector_gbx(spark)


@pytest.mark.integration
def test_write_shapefile_gbx(spark):
    ex.write_shapefile_gbx(spark)


@pytest.mark.integration
def test_write_geojson_gbx(spark):
    ex.write_geojson_gbx(spark)


@pytest.mark.integration
def test_write_gpkg_gbx(spark):
    ex.write_gpkg_gbx(spark)


@pytest.mark.integration
def test_write_file_gdb_gbx(spark):
    ex.write_file_gdb_gbx(spark)
