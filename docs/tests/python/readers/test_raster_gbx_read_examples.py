"""Executes the raster_gbx reader doc examples against real sample data (Docker)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import raster_gbx_read_examples as ex  # noqa: E402


def test_read_raster_gbx(spark):
    ex.read_raster_gbx(spark)


def test_read_gtiff_gbx(spark):
    ex.read_gtiff_gbx(spark)
