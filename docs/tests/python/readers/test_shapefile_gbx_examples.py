"""Executes the shapefile_gbx reader doc examples against real sample data (Docker)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import shapefile_gbx_examples as ex  # noqa: E402


def test_read_shapefile_gbx(spark):
    ex.read_shapefile_gbx(spark)
