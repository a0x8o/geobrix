"""Benchmark result row schema, JSONL IO, and a single-API markdown summary."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List


@dataclass(frozen=True)
class ResultRow:
    run_id: str
    api: str  # "heavyweight" | "lightweight"
    fn: str
    category: str
    mode: str  # "pure-core" | "spark-path"
    tile_px: int
    bands: int
    dtype: str
    srid: int
    rows: int
    nodata_frac: float
    warmup_iters: int
    measured_iters: int
    median_ms: float
    min_ms: float
    p90_ms: float
    throughput_mpix_s: float
    throughput_rows_s: float
    peak_rss_mb: float
    status: str  # "ok" | "na_by_design" | "error"
    note: str
    env_arch: str
    env_cpu_model: str
    env_cpu_count: int
    env_os: str
    env_gbx_version: str
    env_gdal_version: str
    env_runtime_version: str
    env_where: str  # "docker" | "venv" | "cluster"
    output_fingerprint: str = (
        ""  # JSON summary of the output (pure-core only); "" when not captured
    )


def write_jsonl(rows: List[ResultRow], path) -> None:
    with Path(path).open("w") as fh:
        for row in rows:
            fh.write(json.dumps(asdict(row)) + "\n")


def read_jsonl(path) -> List[ResultRow]:
    out = []
    with Path(path).open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(ResultRow(**json.loads(line)))
    return out


def _derive_insights(rows, ok, errs, na, pure, spark) -> List[str]:
    """Data-derived, human-readable observations. Each bullet is conditional on the data."""
    out: List[str] = []
    if pure:
        s = max(pure, key=lambda r: r.median_ms)
        out.append(
            f"Slowest pure-core op: `{s.fn}` — {s.median_ms:.2f} ms "
            f"at {s.tile_px}²/{s.bands}-band ({s.dtype})."
        )
        tps = [r.throughput_mpix_s for r in pure if r.throughput_mpix_s]
        if tps:
            out.append(
                f"Pure-core throughput spans {min(tps):.1f}–{max(tps):.1f} Mpix/s "
                f"across the size sweep."
            )
        byfn = defaultdict(list)
        for r in pure:
            byfn[r.fn].append(r)
        for fn, rs in byfn.items():
            if len({r.tile_px for r in rs}) >= 2:
                small = min(rs, key=lambda r: r.tile_px)
                large = max(rs, key=lambda r: r.tile_px)
                if small.median_ms > 0 and small.tile_px > 0:
                    t_ratio = large.median_ms / small.median_ms
                    px_ratio = (large.tile_px**2) / (small.tile_px**2)
                    rel = t_ratio / px_ratio if px_ratio else 0
                    shape = (
                        "≈linear in pixels"
                        if 0.5 <= rel <= 2.0
                        else "sub-linear" if t_ratio < px_ratio else "supra-linear"
                    )
                    out.append(
                        f"Scaling: `{fn}` pure-core {small.tile_px}²→{large.tile_px}² is "
                        f"{t_ratio:.1f}× time for {px_ratio:.0f}× pixels ({shape})."
                    )
                break
    if spark and pure:
        spark_fns = {r.fn for r in spark}
        pure_fns = {r.fn for r in pure}
        common = sorted(spark_fns & pure_fns)
        if common:
            fn = common[0]
            sp = min((r for r in spark if r.fn == fn), key=lambda r: r.rows)
            pc = min((r for r in pure if r.fn == fn), key=lambda r: r.tile_px)
            out.append(
                f"Spark-path carries Spark/UDF overhead: `{fn}` {sp.median_ms:.0f} ms for "
                f"{sp.rows} rows (spark-path) vs {pc.median_ms:.3f} ms for one tile (pure-core)."
            )
    if errs:
        efns = sorted({r.fn for r in errs})
        notes = sorted({r.note.split(":")[0] for r in errs if r.note})
        hint = (" — e.g. " + "; ".join(notes[:2])) if notes else ""
        shown = ", ".join(efns[:8]) + (" …" if len(efns) > 8 else "")
        out.append(
            f"⚠ {len(errs)} error row(s) across {len(efns)} fn(s): {shown}{hint}."
        )
    if na:
        out.append(f"{len(na)} row(s) N/A by design (mode/shape not applicable).")
    flagged = []
    for r in ok:
        if r.mode != "pure-core" or not r.output_fingerprint or r.nodata_frac <= 0:
            continue
        try:
            fp = json.loads(r.output_fingerprint)
        except (ValueError, TypeError):
            continue
        if fp.get("kind") == "raster":
            bands = fp.get("bands") or []
            if bands and all(b.get("nodata_count") == 0 for b in bands):
                flagged.append(r.fn)
    if flagged:
        uniq = sorted(set(flagged))
        shown = ", ".join(uniq[:6]) + (" …" if len(uniq) > 6 else "")
        out.append(
            f"🔎 Consistency: {len(flagged)} output(s) on nodata-bearing tiles report "
            f"nodata_count=0 ({shown}) — sentinels likely treated as data. "
            f"Verify NoData semantics before heavy-vs-light comparison."
        )
    return out


def summarize(rows: List[ResultRow]) -> str:
    """Human-friendly markdown summary: insights at the top, then status + tables."""
    if not rows:
        return "# Benchmark summary\n\n(no rows)\n"
    first = rows[0]
    ok = [r for r in rows if r.status == "ok"]
    errs = [r for r in rows if r.status == "error"]
    na = [r for r in rows if r.status == "na_by_design"]
    pure = [r for r in ok if r.mode == "pure-core"]
    spark = [r for r in ok if r.mode == "spark-path"]

    lines = [f"# GeoBrix benchmark summary — {first.api} (run: {first.run_id})", ""]
    lines.append(
        f"_Env: {first.env_arch} · {first.env_os} · gbx {first.env_gbx_version} · "
        f"GDAL {first.env_gdal_version} · where={first.env_where} · "
        f"{len({r.fn for r in rows})} functions · {len(rows)} rows_"
    )
    lines += ["", "## Insights", ""]
    insights = _derive_insights(rows, ok, errs, na, pure, spark)
    lines += [f"- {b}" for b in insights] if insights else ["- (no notable insights)"]
    lines += [
        "",
        "## Status",
        "",
        f"ok {len(ok)} · na_by_design {len(na)} · error {len(errs)}",
        "",
    ]

    slow = sorted(pure, key=lambda r: r.median_ms, reverse=True)[:15]
    if slow:
        lines += ["## Slowest pure-core functions (by median_ms)", ""]
        lines += [
            "| fn | tile_px | bands | dtype | median_ms | mpix/s |",
            "|---|---|---|---|---|---|",
        ]
        for r in slow:
            lines.append(
                f"| {r.fn} | {r.tile_px} | {r.bands} | {r.dtype} | "
                f"{r.median_ms:.3f} | {r.throughput_mpix_s:.1f} |"
            )
        lines.append("")
    if spark:
        lines += ["## Spark-path (median_ms by rows)", ""]
        lines += ["| fn | rows | median_ms | rows/s |", "|---|---|---|---|"]
        for r in sorted(spark, key=lambda r: (r.fn, r.rows)):
            lines.append(
                f"| {r.fn} | {r.rows} | {r.median_ms:.3f} | {r.throughput_rows_s:.2f} |"
            )
        lines.append("")
    if errs:
        lines += [
            "## Errors",
            "",
            "| fn | tile_px | bands | note |",
            "|---|---|---|---|",
        ]
        for r in errs[:30]:
            lines.append(f"| {r.fn} | {r.tile_px} | {r.bands} | {r.note} |")
        lines.append("")
    return "\n".join(lines)


def main(argv=None):
    import argparse

    ap = argparse.ArgumentParser(prog="bench.results")
    ap.add_argument(
        "--in", dest="inp", required=True, help="input <engine>.jsonl shard"
    )
    ap.add_argument(
        "--out",
        default=None,
        help="output .md (default: <shard-without-.jsonl>.summary.md)",
    )
    a = ap.parse_args(argv)
    rows = read_jsonl(a.inp)
    out = a.out or (
        a.inp[:-6] + ".summary.md"
        if a.inp.endswith(".jsonl")
        else a.inp + ".summary.md"
    )
    Path(out).write_text(summarize(rows))
    print(f"summary -> {out}")


if __name__ == "__main__":
    main()
