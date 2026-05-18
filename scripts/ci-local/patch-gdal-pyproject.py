#!/usr/bin/env python3
"""Patch GDAL's pyproject.toml so modern setuptools accepts it.

GDAL 3.11.4's published pyproject.toml declares `[project.license]` with
BOTH `file` and `text` keys (or via separate `license =` lines), which
violates PEP 621 (must be exactly one). Modern setuptools (anything with
vendored validate-pyproject ≥ ~0.15, i.e. setuptools ≥ 64.x) rejects this
with `invalid pyproject.toml config: project.license`.

This script normalizes any `license = ...` line in the `[project]` table
into a single `license = { text = "MIT" }` form so the build proceeds.
Used by Dockerfile.gha-runner at runner-image build time. Doesn't touch
upstream GDAL — only the local sdist extraction.

Run from the gdal-X.Y.Z source directory (cwd contains pyproject.toml).
"""
import re
import sys
from pathlib import Path

PYPROJECT = Path("pyproject.toml")
if not PYPROJECT.exists():
    sys.exit(f"no pyproject.toml in cwd ({Path.cwd()})")

src = PYPROJECT.read_text()

print("--- pyproject.toml BEFORE patch ---", flush=True)
print(src, flush=True)
print("--- end ---", flush=True)

lines = src.split("\n")
out = []
in_project = False
license_replaced = False
for line in lines:
    stripped = line.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        in_project = stripped == "[project]"
    if in_project and re.match(r"\s*license\s*=", line):
        if not license_replaced:
            out.append('license = {text = "MIT"}  # patched by scripts/ci-local/patch-gdal-pyproject.py')
            license_replaced = True
        # subsequent license = lines in [project] are dropped
    else:
        out.append(line)

PYPROJECT.write_text("\n".join(out))

print("--- pyproject.toml AFTER patch ---", flush=True)
print(PYPROJECT.read_text(), flush=True)
print("--- end ---", flush=True)
print(f"license_replaced={license_replaced}", flush=True)
