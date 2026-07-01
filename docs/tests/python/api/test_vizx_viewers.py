"""Execute the VizX viewers doc examples (Docker)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import vizx_viewers as ex  # noqa: E402


def test_pmtiles_info_example():
    ex.pmtiles_info_example()


def test_plot_pmtiles_static_example():
    ex.plot_pmtiles_static_example()


def test_plot_cog_example():
    ex.plot_cog_example()
