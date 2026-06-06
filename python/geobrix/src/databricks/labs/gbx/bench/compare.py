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


def _close(a, b, rel_tol: float, abs_tol: float) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(a - b) <= max(abs_tol, rel_tol * max(abs(a), abs(b)))


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
    all_close = True

    if kind == "scalar":
        a, b = _num(hw.get("value")), _num(lw.get("value"))
        exact = exact and (a == b)
        all_close = all_close and _close(a, b, rel_tol, abs_tol)
        d = _rel_delta(a, b)
        if d != math.inf:
            deltas.append(d)
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
            exact = exact and (a == b)
            all_close = all_close and _close(a, b, rel_tol, abs_tol)
            d = _rel_delta(a, b)
            if d != math.inf:
                deltas.append(d)
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
                exact = exact and (ha == la)
                all_close = all_close and _close(ha, la, rel_tol, abs_tol)
                d = _rel_delta(ha, la)
                if d != math.inf:
                    deltas.append(d)
    else:
        return ("divergent", math.inf, 0, f"unknown kind {kind}")

    max_delta = max(deltas) if deltas else 0.0
    if exact:
        cls = "exact"
    elif all_close:
        cls = "within_tol"
    else:
        cls = "divergent"
    note = ""
    if ndc_delta != 0 and cls in ("exact", "within_tol"):
        note = f"nodata_count differs by {ndc_delta} (informational; neighborhood-op border)"
    return (cls, max_delta, ndc_delta, note)
