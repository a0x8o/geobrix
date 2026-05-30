"""Pure-function tests for the generic named-index dispatcher (rst_index)."""

import numpy as np
import pytest

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import indices

from .conftest import make_geotiff_bytes


def test_index_ndvi_values():
    # band1 (red) pixels 0..11, band2 (nir) 100..111. NDVI=(nir-red)/(nir+red).
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, count=2)) as ds:
        out = indices.index(ds, "ndvi", {"red": 1, "nir": 2})
        red = ds.read(1).astype("float64")
        nir = ds.read(2).astype("float64")
    expected = (nir - red) / (nir + red)
    with _serde.open_tile(out) as o:
        assert o.count == 1
        assert o.dtypes[0] == "float32"
        arr = o.read(1)
        assert np.allclose(arr, expected.astype("float32"), atol=1e-5)


def test_index_case_insensitive_name_and_keys():
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, count=2)) as ds:
        lower = indices.index(ds, "ndvi", {"red": 1, "nir": 2})
        upper = indices.index(ds, "NDVI", {"RED": 1, "NIR": 2})
    with _serde.open_tile(lower) as a, _serde.open_tile(upper) as b:
        assert np.allclose(a.read(1), b.read(1), atol=1e-6)


def test_index_unknown_formula_raises():
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, count=2)) as ds:
        with pytest.raises(ValueError) as exc:
            indices.index(ds, "bogus", {"red": 1, "nir": 2})
    # error must list the known formula names
    assert "ndvi" in str(exc.value)


def test_index_missing_required_band_raises():
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, count=2)) as ds:
        with pytest.raises(ValueError) as exc:
            indices.index(ds, "ndvi", {"red": 1})  # missing nir
    assert "nir" in str(exc.value)


def test_index_single_band_float32_output():
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, count=2)) as ds:
        out = indices.index(ds, "gndvi", {"green": 1, "nir": 2})
    with _serde.open_tile(out) as o:
        assert o.count == 1 and o.dtypes[0] == "float32"


def test_index_msavi_runs_sqrt_path():
    # msavi uses sqrt and **; ensure it evaluates to finite values on sane input.
    with _serde.open_tile(make_geotiff_bytes(width=4, height=3, count=2)) as ds:
        out = indices.index(ds, "msavi", {"red": 1, "nir": 2})
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        assert o.count == 1 and o.dtypes[0] == "float32"
        # at least some finite (non-nodata) pixels
        assert np.isfinite(arr[arr != -9999.0]).any()
