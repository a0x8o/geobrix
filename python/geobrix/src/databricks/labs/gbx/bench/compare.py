"""Cross-API compare: join heavy vs light shards into speedup + consistency.

Consistency follows the design §11 rules: parse fingerprints (never string-compare),
float-tolerant numeric agreement, dtype excluded (GDAL "Float32" vs numpy "float32"),
nodata_count reported as informational only (neighborhood ops legitimately differ on
the kernel border), and fingerprints are never expected byte-equal across APIs.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

from databricks.labs.gbx.bench.results import ResultRow

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


@dataclass(frozen=True)
class CellCompare:
    fn: str
    mode: str
    tile_px: int
    bands: int
    dtype: str
    srid: int
    nodata_frac: float
    rows: int
    hw_median_ms: float
    lw_median_ms: float
    speedup: float  # hw/lw; >1 => lightweight faster
    consistency: str  # exact | within_tol | divergent | na
    max_rel_delta: float
    nodata_count_delta: int
    note: str


def _key(r: ResultRow):
    return (r.fn, r.mode, r.tile_px, r.bands, r.dtype, r.srid, r.nodata_frac, r.rows)


def compare_cells(hw_rows: List[ResultRow], lw_rows: List[ResultRow]):
    """Return (cells, unmatched). cells: matched CellCompare list. unmatched: (fn, side, key)."""
    hw_by = {_key(r): r for r in hw_rows}
    lw_by = {_key(r): r for r in lw_rows}
    cells: List[CellCompare] = []
    for k in sorted(set(hw_by) & set(lw_by)):
        h, lo = hw_by[k], lw_by[k]
        speedup = (h.median_ms / lo.median_ms) if lo.median_ms > 0 else 0.0
        if h.status == "ok" and lo.status == "ok":
            cls, delta, ndc, note = compare_fingerprints(
                h.output_fingerprint, lo.output_fingerprint
            )
        else:
            cls, delta, ndc, note = (
                "na",
                0.0,
                0,
                f"status hw={h.status} lw={lo.status}",
            )
        cells.append(
            CellCompare(
                fn=h.fn,
                mode=h.mode,
                tile_px=h.tile_px,
                bands=h.bands,
                dtype=h.dtype,
                srid=h.srid,
                nodata_frac=h.nodata_frac,
                rows=h.rows,
                hw_median_ms=h.median_ms,
                lw_median_ms=lo.median_ms,
                speedup=speedup,
                consistency=cls,
                max_rel_delta=delta,
                nodata_count_delta=ndc,
                note=note,
            )
        )
    unmatched = [
        (hw_by[k].fn, "heavyweight", k) for k in sorted(set(hw_by) - set(lw_by))
    ]
    unmatched += [
        (lw_by[k].fn, "lightweight", k) for k in sorted(set(lw_by) - set(hw_by))
    ]
    return cells, unmatched


_CSV_FIELDS = [
    "fn",
    "mode",
    "tile_px",
    "bands",
    "dtype",
    "srid",
    "nodata_frac",
    "rows",
    "hw_median_ms",
    "lw_median_ms",
    "speedup",
    "consistency",
    "max_rel_delta",
    "nodata_count_delta",
    "note",
]


def write_csv(cells: List[CellCompare], path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=_CSV_FIELDS)
        w.writeheader()
        for cl in cells:
            w.writerow({k: getattr(cl, k) for k in _CSV_FIELDS})


def summarize_compare(cells, unmatched, hw_rows, lw_rows) -> str:
    lines = ["# GeoBrix benchmark — heavy vs light comparison", "", "## Insights", ""]
    insights = []
    ok_speed = [cl for cl in cells if cl.lw_median_ms > 0 and cl.hw_median_ms > 0]
    if ok_speed:
        lw_win = max(ok_speed, key=lambda cl: cl.speedup)
        hw_win = min(ok_speed, key=lambda cl: cl.speedup)
        insights.append(
            f"Biggest lightweight win: `{lw_win.fn}` ({lw_win.mode}) — {lw_win.speedup:.1f}x "
            f"(hw {lw_win.hw_median_ms:.1f} ms vs lw {lw_win.lw_median_ms:.1f} ms)."
        )
        if hw_win.speedup > 0:
            insights.append(
                f"Biggest heavyweight win: `{hw_win.fn}` ({hw_win.mode}) — {1.0 / hw_win.speedup:.1f}x "
                f"(hw {hw_win.hw_median_ms:.1f} ms vs lw {hw_win.lw_median_ms:.1f} ms)."
            )
    pc = [cl for cl in cells if cl.consistency != "na"]
    n_exact = sum(1 for cl in pc if cl.consistency == "exact")
    n_tol = sum(1 for cl in pc if cl.consistency == "within_tol")
    div = [cl for cl in pc if cl.consistency == "divergent"]
    if pc:
        msg = (
            f"Consistency ({len(pc)} compared cells): exact {n_exact} - within-tol {n_tol} - "
            f"divergent {len(div)}"
        )
        if div:
            msg += " - divergent: " + ", ".join(sorted({cl.fn for cl in div}))
        insights.append(msg)
    ndc = sorted({cl.fn for cl in cells if cl.nodata_count_delta != 0})
    if ndc:
        insights.append(
            "NoData-count differs (informational, neighborhood-op border) for: "
            + ", ".join(ndc)
            + " - value stats still agree within tolerance; not a divergence."
        )
    if unmatched:
        ufns = sorted({u[0] for u in unmatched})
        insights.append(
            f"{len(unmatched)} unmatched cell(s) across {len(ufns)} fn(s): "
            + ", ".join(ufns[:8])
        )
    lines += [f"- {b}" for b in insights] if insights else ["- (no cells compared)"]
    lines += [""]

    for mode in ("pure-core", "spark-path"):
        mc = [cl for cl in cells if cl.mode == mode]
        if not mc:
            continue
        lines += [
            f"## {mode} (hw vs lw)",
            "",
            "| fn | tile_px | bands | rows | hw_ms | lw_ms | speedup | consistency | note |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for cl in sorted(mc, key=lambda c: c.speedup, reverse=True):
            lines.append(
                f"| {cl.fn} | {cl.tile_px} | {cl.bands} | {cl.rows} | {cl.hw_median_ms:.3f} | "
                f"{cl.lw_median_ms:.3f} | {cl.speedup:.2f} | {cl.consistency} | {cl.note} |"
            )
        lines += [""]
    return "\n".join(lines)


def main(argv=None):
    import argparse

    from databricks.labs.gbx.bench import results as _r

    ap = argparse.ArgumentParser(prog="bench.compare")
    ap.add_argument("--heavyweight", required=True, help="heavyweight.jsonl shard")
    ap.add_argument("--lightweight", required=True, help="lightweight.jsonl shard")
    ap.add_argument(
        "--out-dir", required=True, help="dir for comparison.csv + summary.md"
    )
    a = ap.parse_args(argv)

    hw_rows = _r.read_jsonl(a.heavyweight)
    lw_rows = _r.read_jsonl(a.lightweight)
    cells, unmatched = compare_cells(hw_rows, lw_rows)
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    write_csv(cells, out / "comparison.csv")
    (out / "summary.md").write_text(
        summarize_compare(cells, unmatched, hw_rows, lw_rows)
    )
    print(
        f"compared {len(cells)} cells ({len(unmatched)} unmatched) -> "
        f"{out}/comparison.csv, summary.md"
    )


if __name__ == "__main__":
    main()
