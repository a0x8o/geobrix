#!/usr/bin/env python3
"""Standing vectorization check for the pyrx core.

Scans ``python/geobrix/src/databricks/labs/gbx/pyrx/core/*.py`` for
vectorizable anti-patterns — code that does per-pixel / per-element work in
Python instead of in NumPy/SciPy. This is a perf check that runs INDEPENDENT of
the heavy-vs-light benchmark: it catches functions where both tiers are slow,
or where light beats a slow heavy but still leaves easy headroom on the table.

High-signal patterns (the ones that cost real time on rasters):
  * scipy.ndimage.generic_filter  — a Python callback invoked per pixel
  * np.vectorize / np.frompyfunc / .apply(  — "fake" vectorization (Python loop)
  * a scalar-lib call inside a comprehension over coordinate/pixel arrays
    e.g. ``[h3.latlng_to_cell(...) for lo, la in zip(lon, lat)]``
  * a for-loop whose range is pixel-scale (height/width/shape/size)

Benign patterns are NOT flagged: per-band loops (``range(1, ds.count + 1)``,
count is 1-4), per-zoom / per-stop / per-geometry-part loops — these are tiny
and their inner pixel work is already vectorized.

Allowlist: append ``# vectorscan: ok <reason>`` to a line to exclude it (used
for genuinely unavoidable cases, e.g. h3's lat/lng->cell which has no array
API and is impractical to reimplement, unlike quadbin's tile-Morton encoding).

Exit code: 0 always in report mode; with --strict, 1 if any non-allowlisted
finding remains.
"""

import argparse
import glob
import os
import re
import sys

CORE_GLOB = (
    "python/geobrix/src/databricks/labs/gbx/pyrx/core/*.py"
)

ALLOW_MARK = "# vectorscan: ok"

# (label, compiled regex). Order matters only for the reported label.
PATTERNS = [
    ("generic_filter", re.compile(r"\bgeneric_filter\b")),
    ("np.vectorize", re.compile(r"\b(?:np|numpy)\.vectorize\b")),
    ("frompyfunc", re.compile(r"\bfrompyfunc\b")),
    (".apply(", re.compile(r"\.apply\(")),
    ("comprehension-over-zip", re.compile(r"\bfor\b.*\bin\s+zip\(")),
    ("ndindex", re.compile(r"\bnp\.ndindex\b")),
]

# A for/range loop is pixel-scale (suspect) if its range references one of these
# and is NOT a per-band loop over ds.count.
_RANGE_RE = re.compile(r"\bfor\b.*\bin\s+range\(")
_PIXEL_SCALE_RE = re.compile(r"\b(height|width|shape|\.size|len\()\b")
_PER_BAND_RE = re.compile(r"range\(\s*1?\s*,?\s*\)?.*ds\.count|range\(\s*ds\.count")


def _is_pixel_scale_range(line: str) -> bool:
    if not _RANGE_RE.search(line):
        return False
    if "ds.count" in line:  # per-band loop — benign
        return False
    return bool(_PIXEL_SCALE_RE.search(line))


def scan_file(path: str):
    findings = []
    with open(path, encoding="utf-8") as fh:
        for n, raw in enumerate(fh, start=1):
            line = raw.rstrip("\n")
            if ALLOW_MARK in line:
                continue
            stripped = line.strip()
            if stripped.startswith("#"):  # pure comment line
                continue
            for label, rx in PATTERNS:
                if rx.search(line):
                    findings.append((n, label, stripped))
                    break
            else:
                if _is_pixel_scale_range(line):
                    findings.append((n, "pixel-scale range loop", stripped))
    return findings


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="pyrx vectorization anti-pattern scan")
    ap.add_argument(
        "--root",
        default=os.environ.get("GBX_PROJECT_ROOT", "."),
        help="project root (defaults to CWD)",
    )
    ap.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero if any non-allowlisted finding remains",
    )
    args = ap.parse_args(argv)

    files = sorted(glob.glob(os.path.join(args.root, CORE_GLOB)))
    if not files:
        print(f"vectorscan: no pyrx core files found under {args.root!r}", file=sys.stderr)
        return 2

    total = 0
    for path in files:
        rel = os.path.relpath(path, args.root)
        hits = scan_file(path)
        if not hits:
            continue
        total += len(hits)
        for n, label, code in hits:
            print(f"{rel}:{n}: [{label}] {code}")

    print()
    if total == 0:
        print("vectorscan: clean — no non-allowlisted vectorization anti-patterns.")
        return 0
    print(
        f"vectorscan: {total} candidate(s) flagged across "
        f"{len(files)} core file(s). Review each: vectorize, or annotate with "
        f"'{ALLOW_MARK} <reason>' if genuinely unavoidable."
    )
    return 1 if args.strict else 0


if __name__ == "__main__":
    sys.exit(main())
