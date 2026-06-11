# gbx:bench:readers

Time the light raster reader (`raster_gbx`) per-file in pure-local mode, or
over a corpus directory via the Spark data source in spark-path mode.

## Usage

```
bash scripts/commands/gbx-bench-readers.sh [OPTIONS]
```

## Options

| Option | Default | Description |
|---|---|---|
| `--corpus <dir>` | *(required)* | Directory containing `*.tif` files to benchmark |
| `--mode <m>` | `pure-local` | `pure-local` \| `spark-path` \| `both` |
| `--run-id <id>` | `local` | Run ID label embedded in result rows |
| `--size-mib <n>` | `16` | Tile size budget in MiB passed to the reader |
| `--warmup <n>` | `1` | Warmup iterations per file/path |
| `--measured <n>` | `3` | Measured iterations per file/path |
| `--out <path>` | *(print only)* | Write results to this JSONL file |
| `--log <path>` | *(none)* | Tee output to `test-logs/<path>` |
| `--help`, `-h` | | Show help and exit |

## Examples

```bash
# Time all .tif files in a corpus dir (pure-local)
bash scripts/commands/gbx-bench-readers.sh \
  --corpus sample-data/Volumes/main/default/bench-corpus \
  --mode pure-local --warmup 1 --measured 3

# Run both modes and write results to JSONL
bash scripts/commands/gbx-bench-readers.sh \
  --corpus /tmp/bench-tifs \
  --mode both --warmup 1 --measured 3 \
  --out test-logs/bench/local/readers.jsonl \
  --log bench-readers.log
```
