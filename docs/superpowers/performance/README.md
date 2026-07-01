# GeoBrix Performance Engineering Corpus

Developer-facing patterns for distributed performance in the light (Serverless-safe) tier.
One file per pattern; each entry is a problem → symptom → fix → applicability matrix → evidence chain.

**Distinct from `docs/docs/api/performance.mdx`** (user-facing execution shapes + function
classification). This corpus is the internal "how/why we got the gain" engineering record; the
two cross-reference but do not merge.

## Index

| Slug | Pattern | Status |
|---|---|---|
| [serverless-aoi-ingestion-strategy](serverless-aoi-ingestion-strategy.md) | Serverless-first AOI ingestion: distributed read-in-place + bbox predicate pushdown + column-hash repartition, with whole-file download fallback | Pattern/correctness confirmed; cluster-scale speedup not yet measured |
| [notebook-pmtiles-rendering](notebook-pmtiles-rendering.md) | In-notebook PMTiles rendering via base64-embedded in-browser FileSource (no tile server, no remote range requests) + size-guarded static fallback for GitHub-renderable output | Correctness/portability confirmed (offline tests); no distributed compute speedup — driver-side render |
| [pmtiles-vector-merge](pmtiles-vector-merge.md) | (1) Vector tile aggregation must merge per `(z,x,y)`, not first-wins — correctness fix, both tiers, audited unique to `gbx_pmtiles_agg`; (2) GDAL `GetMemFileBuffer` returns NULL for OGR-written `/vsimem/` — use real temp dir + Java file I/O for driver output | Correctness fix; 278 vectorx+pmtiles Scala tests green; no measured speedup |
