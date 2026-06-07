# gbx:bench:gen-data

Generate the seeded benchmark corpus (GeoTIFF tiles + `corpus.json` manifest) in the isolated pyrx venv, then run the validity gate.

**Usage:** `bash scripts/commands/gbx-bench-gen-data.sh [options]`

**Options:** `--out <dir>`, `--tile-px <list>`, `--bands <list>`, `--dtypes <list>`, `--srids <list>`, `--nodata-frac <list>`, `--row-rows <n>`, `--seed <n>`, `--log <path>`, `--help`.

**Examples:**
- `bash scripts/commands/gbx-bench-gen-data.sh --log gen.log`
- `bash scripts/commands/gbx-bench-gen-data.sh --tile-px 256,512 --bands 1,4 --nodata-frac 0.02,0.5 --row-rows 100`
