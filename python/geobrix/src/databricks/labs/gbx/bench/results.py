"""Benchmark result row schema, JSONL IO, and a single-API markdown summary."""

from __future__ import annotations

import json
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


def write_pretty_json(rows: List[ResultRow], path) -> None:
    """Write an indented JSON array (human-readable) alongside the canonical JSONL.

    The output_fingerprint field is stored as a JSON string; expand it into a
    nested object so the pretty view is readable rather than an escaped blob.
    """
    expanded = []
    for row in rows:
        d = asdict(row)
        fp = d.get("output_fingerprint")
        if fp:
            try:
                d["output_fingerprint"] = json.loads(fp)
            except (ValueError, TypeError):
                pass  # leave as the original string if it isn't valid JSON
        expanded.append(d)
    Path(path).write_text(json.dumps(expanded, indent=2))


def read_jsonl(path) -> List[ResultRow]:
    out = []
    with Path(path).open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(ResultRow(**json.loads(line)))
    return out


def summarize(rows: List[ResultRow]) -> str:
    """Single-API markdown summary: slowest functions + status counts."""
    ok = [r for r in rows if r.status == "ok"]
    slowest = sorted(ok, key=lambda r: r.median_ms, reverse=True)[:15]
    lines = ["# Benchmark summary", "", "## Slowest functions (by median_ms)", ""]
    lines.append("| fn | mode | tile_px | bands | rows | median_ms |")
    lines.append("|---|---|---|---|---|---|")
    for r in slowest:
        lines.append(
            f"| {r.fn} | {r.mode} | {r.tile_px} | {r.bands} | {r.rows} | {r.median_ms:.3f} |"
        )
    n_err = sum(1 for r in rows if r.status == "error")
    n_na = sum(1 for r in rows if r.status == "na_by_design")
    lines += [
        "",
        f"Rows: {len(rows)} · ok: {len(ok)} · na_by_design: {n_na} · error: {n_err}",
    ]
    return "\n".join(lines)
