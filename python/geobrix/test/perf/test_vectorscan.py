"""Unit tests for the gbx:perf:vectorscan scanner (scripts/perf/vectorscan.py).

Loads the standalone script as a module and exercises the anti-pattern
detection, benign-loop exclusion, allowlist annotation, and the main() exit
codes on synthetic source files.
"""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[4] / "scripts" / "perf" / "vectorscan.py"


def _load():
    spec = importlib.util.spec_from_file_location("vectorscan", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


vs = _load()


def test_script_exists():
    assert _SCRIPT.is_file(), f"scanner missing at {_SCRIPT}"


@pytest.mark.parametrize(
    "line,expected",
    [
        ("        for x in range(width):", True),
        ("    for y in range(height):", True),
        ("    res = arr[: shape[0]]", False),  # no for-loop
        ("    for bi in range(1, ds.count + 1):", False),  # per-band -> benign
        ("    for z in range(lo, hi + 1):", False),  # per-zoom -> benign
        ("    for k in range(k_start, k_end + 1):", False),  # small count -> benign
    ],
)
def test_is_pixel_scale_range(line, expected):
    assert vs._is_pixel_scale_range(line) is expected


def _write(tmp_path, body: str) -> Path:
    # Mirror the layout the scanner globs: <root>/python/.../pyrx/core/<f>.py
    core = tmp_path / "python/geobrix/src/databricks/labs/gbx/pyrx/core"
    core.mkdir(parents=True)
    f = core / "f.py"
    f.write_text(body)
    return f


def test_scan_file_flags_high_signal(tmp_path):
    f = _write(
        tmp_path,
        "import numpy as np\n"
        "def a(x):\n"
        "    return ndimage.generic_filter(x, np.nanmin, size=3)\n"
        "def b(x):\n"
        "    return np.vectorize(f)(x)\n"
        "def c(lon, lat):\n"
        "    return [h3.cell(lo, la) for lo, la in zip(lon, lat)]\n",
    )
    labels = {label for _, label, _ in vs.scan_file(str(f))}
    assert "generic_filter" in labels
    assert "np.vectorize" in labels
    assert "comprehension-over-zip" in labels


def test_scan_file_ignores_benign_and_comments(tmp_path):
    f = _write(
        tmp_path,
        "def a(ds):\n"
        "    for bi in range(1, ds.count + 1):\n"  # per-band benign
        "        pass\n"
        "    # generic_filter mentioned only in a comment\n"
        "    return 0\n",
    )
    assert vs.scan_file(str(f)) == []


def test_scan_file_honors_allowlist(tmp_path):
    f = _write(
        tmp_path,
        "def a(lon, lat):\n"
        "    return [h3.cell(lo, la) for lo, la in zip(lon, lat)]"
        "  # vectorscan: ok (h3 no array API)\n",
    )
    assert vs.scan_file(str(f)) == []


def test_main_exit_codes(tmp_path, capsys):
    _write(
        tmp_path,
        "def a(x):\n    return ndimage.generic_filter(x, f, size=3)\n",
    )
    # report mode -> 0 even with findings
    assert vs.main(["--root", str(tmp_path)]) == 0
    # strict -> 1 when a non-allowlisted finding remains
    assert vs.main(["--root", str(tmp_path), "--strict"]) == 1


def test_main_clean_returns_zero(tmp_path):
    _write(tmp_path, "def a(ds):\n    for bi in range(1, ds.count + 1):\n        pass\n")
    assert vs.main(["--root", str(tmp_path), "--strict"]) == 0
