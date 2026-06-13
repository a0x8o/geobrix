# Benchmark results lifecycle — design spec

**Date:** 2026-06-07 · **Branch:** `beta/0.4.0` · **Status:** design approved (forks settled), pre-plan.
**Goal:** Replace the pile of ad-hoc `test-logs/bench/<run-id>/` dirs with a **per-function authoritative store** keyed to the commit/state each function was validated at, a **change-aware** run command that benchmarks only functions affected by current changes, and a **cleanup** command. The scorecard then reflects the *current best-known* result per function with staleness flags.

## 1. Motivation

Today every bench invocation writes a new `<run-id>/` dir; results accumulate, go stale, and there's no single source of truth for "what is `rst_slope`'s current heavy-vs-light result, and is it still valid?". For the heavyweight-deprecation evidence base ([[terrain-crs-scale-gdal-normal]]) we want, per function: the **latest authoritative** comparison + **provenance** (which commit/source-state it was validated at) + **staleness** (did its source change since?). And iterating on one function shouldn't require re-benchmarking all 70+ — only the **affected** functions.

## 2. Authoritative store

`test-logs/bench/authoritative/<fn>.json` — one record per function, **the latest authoritative result**, overwritten when that function is (re)validated. Schema:
```json
{
  "fn": "rst_slope",
  "validated_commit": "<git HEAD sha at validation, or 'dirty:<sha>' if working tree had uncommitted changes>",
  "validated_at": "<ISO timestamp, passed in — scripts can't use Date.now()>",
  "sources_hash": "<sha256 of the concatenated current content of this fn's `sources` files at validation>",
  "corpus": {"tile_px": [256,512], "srids": [4326,32618], "bands": 2, "dtype": "float32", "nodata_frac": 0.0},
  "set": "core",
  "cells": [ {tile_px, srid, mode, consistency, max_rel_delta, nodata_count_delta, speedup, hw_median_ms, lw_median_ms, hw_mpix_s, lw_mpix_s} , ... ],
  "heavy_rows": [ <BenchRow> ... ],
  "light_rows": [ <BenchRow> ... ]
}
```
- **Latest-only:** re-validating a function overwrites its record (no history pile). Git history is the audit trail if needed.
- **Provenance + staleness:** `sources_hash` is the authority signal. A record is **stale** when the current hash of its `sources` ≠ stored `sources_hash` (works for committed *and* uncommitted changes; no git archaeology needed). `validated_commit` is informational.
- The store lives under the gitignored `test-logs/bench/` (not committed) — it's a local working cache, regenerable via the bench. (If we later want it shared/CI-persisted, that's a separate decision; out of scope now.)

## 3. `sources` on `FnSpec` (the change→function map)

Add `sources: tuple[str, ...] = ()` to `FnSpec` — repo-relative paths whose content defines the function's behavior on **both** engines:
- pyrx core module(s): e.g. `python/.../pyrx/core/focal.py`, plus shared `python/.../pyrx/core/_nodata.py` where used.
- heavy expression + shared helpers: e.g. `src/main/scala/.../expressions/RST_Filter.scala`, `.../operations/KernelFilter.scala`, `.../gdal/GDALBlock.scala`.
- (Deliberately **exclude** the bench harness files `spec.py`/`BenchDispatch.scala` — editing the registry shouldn't mark every function stale; harness changes are validated by the bench's own tests.)
- Shared files appear in many functions' `sources` → a change to `_nodata.py`/`GDALBlock.scala`/`PixelCombineRasters.scala` correctly marks **all** dependents affected (matching how those fan out — exactly the case convention-only inference gets wrong).

Each FnSpec must declare its `sources`; a registry-completeness test asserts every registered function has a non-empty `sources` and that the listed paths exist.

## 4. Commands

- **`gbx:bench:changed [--base <ref>] [--set core|full] [--all-affected]`** — the change-aware runner:
  1. Compute changed paths: working-tree changes vs `HEAD` (`git diff --name-only HEAD` + untracked), or vs `--base <ref>` if given.
  2. Resolve **affected functions** = registered fns with any `sources` path in the changed set.
  3. Benchmark ONLY those (gen corpus if absent → heavyweight → lightweight → compare), and **write/overwrite their authoritative records** (stamping `validated_commit`/`sources_hash`/timestamp).
  4. Report: changed paths, affected functions (re)validated, and any **changed path mapped to no function** (unmapped — warn, so a forgotten `sources` entry surfaces).
- **`gbx:bench:seed [--set core|full]`** — one-shot: run the full (or core) set and populate the authoritative store for all those functions (bootstraps the store after the cleanup; also the "rebuild everything" escape hatch).
- **`gbx:bench:clean [--runs | --orphans | --all]`** — prune `test-logs/bench/`:
  - `--runs` (default): delete ad-hoc `<run-id>/` dirs, keep `authoritative/`.
  - `--orphans`: also delete `authoritative/<fn>.json` for functions no longer in the registry.
  - `--all`: wipe everything (including authoritative).
- **`gbx:bench:status [--stale-only]`** — render the scorecard **from the store**: per function, last consistency/speedup/max_rel_delta, `validated_commit`, and **STALE** flag (sources changed since). Plus the aggregate Coverage & parity block (N/107, parity counts, functional-parity-gap, not-yet-covered). This replaces "read the last run's summary.md".

## 5. Pre-push staleness warning (cheap, non-blocking)

A pre-push step (alongside the QC judge, or folded into a `gbx:*` pre-push helper) that — **without running any benchmark** — checks: for each function whose `sources` changed in the push range (or working tree), is its authoritative record missing or stale (`sources_hash` mismatch)? If so, **warn** (list the functions + suggest `gbx:bench:changed`). Never blocks (benchmarking is Docker-slow; the dev decides when to validate). This keeps the store honest without gating velocity.

## 6. Scorecard migration

`compare.py`'s Coverage & parity block (and the per-function table) now reads the **authoritative store** rather than a single run's cells: aggregate over `authoritative/*.json`, mark stale functions, compute coverage = `|store| / 107`, parity/perf counts from the stored cells, functional-parity-gap from `registered_rst()` vs pyrx-implemented, not-yet-covered = registered minus store. `gbx:bench:status` is the entry point; `summarize_compare` stays available for a single ad-hoc run.

## 7. Sequencing

Build this lifecycle infra **before resuming Phase 2 coverage**, so Phase 2's bucket-C functions declare `sources` from the start and land directly in the store. After this: re-seed the store (`gbx:bench:seed --set full`) to capture the current 70 functions' authoritative results, then resume Phase 2 (each new function benchmarked via `gbx:bench:changed` as it's added).

## 8. Testing & validation
- **Unit (venv):** `sources` completeness (every fn has existing-path sources); change→function resolution (given a changed-paths set, the right functions are selected; a shared-file change selects all dependents); store read/write round-trip; staleness detection (hash mismatch → stale); `gbx:bench:status` renders coverage/parity from a synthetic store.
- **Integration:** `gbx:bench:changed` on a one-file edit (e.g. touch `focal.py`) selects exactly `rst_filter`/`rst_convolve`, benchmarks only those, writes 2 records; `gbx:bench:clean --runs` removes ad-hoc dirs but keeps `authoritative/`.
- **Bootstrap:** `gbx:bench:seed --set full` populates the store; `gbx:bench:status` shows 70/107 with no stale.

## 9. Out of scope
- Committing/sharing the store (stays a local gitignored cache).
- Auto-benchmarking on push (warning only).
- Phase 2 coverage (resumes after).
- Historical result retention (latest-only; git is the audit trail).

## 10. Risks
- **`sources` drift:** if a function gains a real dependency not listed in `sources`, change-aware runs miss it. Mitigation: the unmapped-changed-path warning in `gbx:bench:changed` + the completeness test; shared helpers explicitly listed.
- **Scripts can't call `Date.now()`** (workflow constraint) — `validated_at` is stamped by the shell command (bash `date`), passed into the Python writer; the Python/Scala bench code receives it as an arg.
- **Staleness via content hash** assumes `sources` fully capture behavior; the warning is advisory, and `gbx:bench:seed` is always available to rebuild.

---
*Design approved 2026-06-07 (forks: per-function records + provenance; explicit `sources`; opt-in `gbx:bench:changed` + cheap pre-push staleness warning; `gbx:bench:clean` + ran the stale-dir purge). Next: implementation plan (writing-plans), then re-seed + resume Phase 2. Sources: bench `spec.py`/`compare.py`/`runner.py`, `test-logs/bench/` layout.*
