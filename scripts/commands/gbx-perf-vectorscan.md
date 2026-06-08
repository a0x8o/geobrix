# gbx:perf:vectorscan

Scan the pyrx core (`python/geobrix/src/databricks/labs/gbx/pyrx/core/*.py`) for **vectorizable anti-patterns** — per-pixel / per-element work done in Python instead of NumPy/SciPy.

This is a standing perf check that runs **independent of the heavy-vs-light benchmark**. Heavy comparison misses functions where both tiers are slow, or where light already beats a slow heavy but still leaves easy vectorization headroom. The scan catches them directly.

**Flags (high-signal):** `scipy.ndimage.generic_filter` (per-pixel Python callback), `np.vectorize`/`np.frompyfunc`/`.apply(` (fake vectorization), a scalar-lib call inside a comprehension over coordinate/pixel arrays (e.g. `[h3.latlng_to_cell(...) for lo, la in zip(lon, lat)]`), and pixel-scale `range(...)` loops (range over `height`/`width`/`shape`/`size`).

**Ignored (benign):** per-band loops (`range(1, ds.count + 1)`, count 1-4), per-zoom / per-color-stop / per-geometry-part loops — tiny, with vectorized inner work.

**Allowlist:** append `# vectorscan: ok <reason>` to a line to exclude it. Used for genuinely unavoidable cases (e.g. `_h3_cells` — h3 has no array API and its icosahedral encoding is impractical to reimplement, unlike quadbin's tile-Morton).

**Usage:** `bash scripts/commands/gbx-perf-vectorscan.sh [options]`

**Options:** `--strict` (exit non-zero on any non-allowlisted finding — use as a perf-review/pre-push gate), `--log <path>`, `--help`.

**Examples:**
- `bash scripts/commands/gbx-perf-vectorscan.sh` — report candidates (exit 0).
- `bash scripts/commands/gbx-perf-vectorscan.sh --strict --log vectorscan.log` — gate mode.
