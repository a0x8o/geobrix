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
