"""Executes the ogr_gbx reader doc examples against real sample data (Docker)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import ogr_gbx_examples as ex  # noqa: E402


def test_read_ogr_gbx(spark):
    ex.read_ogr_gbx(spark)
