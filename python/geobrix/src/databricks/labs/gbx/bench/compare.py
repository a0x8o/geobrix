"""Cross-API compare: join heavy vs light shards into speedup + consistency.

Consistency follows the design §11 rules: parse fingerprints (never string-compare),
float-tolerant numeric agreement, dtype excluded (GDAL "Float32" vs numpy "float32"),
nodata_count reported as informational only (neighborhood ops legitimately differ on
the kernel border), and fingerprints are never expected byte-equal across APIs.
"""

from __future__ import annotations

import json
import math
from typing import Tuple

REL_TOL = 1e-3
ABS_TOL = 1e-6
_STATS = ("min", "max", "mean", "std")


def _num(x):
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return float(x)
    return None


def _rel_delta(a, b) -> float:
    if a is None or b is None:
        return 0.0 if (a is None and b is None) else math.inf
    denom = max(abs(a), abs(b))
    return 0.0 if denom == 0 else abs(a - b) / denom


def compare_fingerprints(
    hw_fp: str, lw_fp: str, rel_tol: float = REL_TOL, abs_tol: float = ABS_TOL
) -> Tuple[str, float, int, str]:
    """Return (consistency_class, max_rel_delta, nodata_count_delta, note).

    class: "exact" | "within_tol" | "divergent" | "na".
    nodata_count_delta: sum over bands of (hw - lw) nodata_count (informational).
    """
    if not hw_fp or not lw_fp:
        return ("na", 0.0, 0, "missing fingerprint (spark-path or non-ok)")
    try:
        hw = json.loads(hw_fp)
        lw = json.loads(lw_fp)
    except (ValueError, TypeError):
        return ("divergent", math.inf, 0, "unparseable fingerprint")

    if hw.get("kind") != lw.get("kind"):
        return (
            "divergent",
            math.inf,
            0,
            f"kind mismatch {hw.get('kind')} vs {lw.get('kind')}",
        )
    kind = hw.get("kind")

    deltas = []
    ndc_delta = 0
    exact = True

    if kind == "scalar":
        a, b = _num(hw.get("value")), _num(lw.get("value"))
        deltas.append(_rel_delta(a, b))
        exact = exact and (a == b)
    elif kind == "scalar_list":
        av, bv = hw.get("values") or [], lw.get("values") or []
        if len(av) != len(bv):
            return (
                "divergent",
                math.inf,
                0,
                f"scalar_list length {len(av)} vs {len(bv)}",
            )
        for a, b in zip(av, bv):
            a, b = _num(a), _num(b)
            deltas.append(_rel_delta(a, b))
            exact = exact and (a == b)
    elif kind == "raster":
        hb, lb = hw.get("bands") or [], lw.get("bands") or []
        if len(hb) != len(lb):
            return ("divergent", math.inf, 0, f"band count {len(hb)} vs {len(lb)}")
        for h, l in zip(hb, lb):
            ndc_delta += int(
                (h.get("nodata_count") or 0) - (l.get("nodata_count") or 0)
            )
            for k in _STATS:
                ha, la = _num(h.get(k)), _num(l.get(k))
                deltas.append(_rel_delta(ha, la))
                exact = exact and (ha == la)
    else:
        return ("divergent", math.inf, 0, f"unknown kind {kind}")

    max_delta = max(deltas) if deltas else 0.0
    if exact:
        cls = "exact"
    elif max_delta == math.inf:
        cls = "divergent"
    else:
        cls = "within_tol" if max_delta <= rel_tol else "divergent"
    note = ""
    if ndc_delta != 0 and cls in ("exact", "within_tol"):
        note = f"nodata_count differs by {ndc_delta} (informational; neighborhood-op border)"
    return (cls, (0.0 if max_delta == math.inf else max_delta), ndc_delta, note)
