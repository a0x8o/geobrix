# gbx:venv:sync

Create or refresh the isolated `uv` venv (`.venv-pyrx/`, no system site-packages) for the lightweight pyrx API and benchmarks, installing the `[pyrx,test]` extras from `python/geobrix`.

**Usage:** `bash scripts/commands/gbx-venv-sync.sh [--python 3.12] [--recreate] [--log <path>]`

**Options:**
- `--python <ver>` — Python version for the venv (default 3.12)
- `--recreate` — delete and recreate the venv
- `--log <path>` — tee output to `test-logs/<path>`
- `--help, -h` — show help

**Examples:**
- `bash scripts/commands/gbx-venv-sync.sh`
- `bash scripts/commands/gbx-venv-sync.sh --recreate --log venv-sync.log`
