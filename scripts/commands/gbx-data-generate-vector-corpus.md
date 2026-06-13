# Generate Vector Corpus

Generates N synthetic vector features in a chosen `*_gbx` light-writer format via the light vector writers, for scaled benchmarking. Runs inside the `geobrix-dev` Docker container.

---

## Usage

```bash
bash scripts/commands/gbx-data-generate-vector-corpus.sh [OPTIONS]
```

## Options

- `--format <fmt>` — Light-writer format to use (default: `geojson_gbx`)
- `--features <N>` — Number of synthetic features to generate (default: `1000`)
- `--out <path>` — Output path inside the container (default: `/tmp/vector_corpus.geojson`)
- `--log <path>` — Write output to log file
- `--help` — Show help

## Requires

- Docker container `geobrix-dev` running (`gbx:docker:start`)
- Light vector writers installed (geobrix wheel built/available in container)

## Examples

```bash
bash scripts/commands/gbx-data-generate-vector-corpus.sh
bash scripts/commands/gbx-data-generate-vector-corpus.sh --format geojson_gbx --features 5000 --out /tmp/bench_corpus.geojson
```
