"""Cheap, deterministic output fingerprints for heavy-vs-light consistency checks.

A function's output is summarized so two API implementations can be compared for
numeric agreement (not byte-equality). Computed OUTSIDE the timed loop.
"""

from __future__ import annotations

import json
from typing import Any

import numpy as np

from databricks.labs.gbx.pyrx import _serde


def _py(x):
    """Coerce numpy scalars to native Python types so json.dumps can serialize them."""
    if isinstance(x, np.floating):
        return float(x)
    if isinstance(x, np.integer):
        return int(x)
    if isinstance(x, np.bool_):
        return bool(x)
    return x


def _stat(arr_valid: np.ndarray) -> dict:
    if arr_valid.size == 0:
        return {"min": None, "max": None, "mean": None, "std": None}
    return {
        "min": float(np.min(arr_valid)),
        "max": float(np.max(arr_valid)),
        "mean": float(np.mean(arr_valid)),
        "std": float(np.std(arr_valid)),
    }


def _pool_valid_pixels(tile_bytes) -> np.ndarray:
    """Read every band of a raster tile, returning its valid (non-nodata) pixels.

    Used by the collection fingerprint to pool pixels across many output tiles.
    """
    parts = []
    with _serde.open_tile(bytes(tile_bytes)) as ds:
        nod = ds.nodata
        for bi in range(1, ds.count + 1):
            a = ds.read(bi)
            valid = a[a != nod] if nod is not None else a.ravel()
            parts.append(np.asarray(valid, dtype="float64").ravel())
    return np.concatenate(parts) if parts else np.empty(0, dtype="float64")


def fingerprint_collection(tiles) -> str:
    """Fingerprint a COLLECTION of output tiles (bucket C, group C4 tiling fns).

    The output is a LIST of tile byte strings. The fingerprint records the tile
    COUNT plus the agg stats pooled over ALL tiles' valid pixels across every
    band. Pooling is ORDER-INDEPENDENT, so heavy and light may emit tiles in any
    order and still agree, while the count is compared exactly.
    """
    tiles = list(tiles)
    pools = [_pool_valid_pixels(t) for t in tiles]
    pooled = np.concatenate(pools) if pools else np.empty(0, dtype="float64")
    return json.dumps(
        {"kind": "raster_collection", "count": len(tiles), "agg": _stat(pooled)},
        sort_keys=True,
    )


def fingerprint_output(out: Any) -> str:
    """Return a JSON string summarizing a function output for consistency comparison."""
    # Raster output: GTiff bytes -> per-band stats over valid (non-nodata) pixels.
    if isinstance(out, (bytes, bytearray)):
        bands = []
        with _serde.open_tile(bytes(out)) as ds:
            nod = ds.nodata
            for bi in range(1, ds.count + 1):
                a = ds.read(bi)
                valid = a[a != nod] if nod is not None else a.ravel()
                stat = _stat(np.asarray(valid, dtype="float64"))
                bands.append(
                    {
                        "shape": [int(a.shape[0]), int(a.shape[1])],
                        "dtype": str(a.dtype),
                        "nodata_count": int(a.size - valid.size),
                        **stat,
                    }
                )
        return json.dumps({"kind": "raster", "bands": bands}, sort_keys=True)
    if isinstance(out, (list, tuple)):
        # A LIST of raster tile bytes -> a raster_collection fingerprint (the C4
        # tiling fns return one). A list of scalars -> scalar_list (per-band
        # avg/min/max). Distinguish by element type: bytes => tile collection.
        if out and all(isinstance(x, (bytes, bytearray)) for x in out):
            return fingerprint_collection(out)
        return json.dumps(
            {"kind": "scalar_list", "values": [_py(x) for x in out]}, sort_keys=True
        )
    # Plain scalar.
    return json.dumps({"kind": "scalar", "value": _py(out)}, sort_keys=True)
