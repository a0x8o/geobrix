# gbx:test:pyrx

Run the pyrx (lightweight, JAR-free raster API) test suite in the dev container.

## Usage

`bash scripts/commands/gbx-test-pyrx.sh [OPTIONS]`

## Options

- `--path <dir>` — specific test file or directory (default: `python/geobrix/test/pyrx/`)
- `--log <path>` — write output to `test-logs/<path>` (relative) or an absolute path
- `--help`, `-h` — show usage

## Examples

- `bash scripts/commands/gbx-test-pyrx.sh`
- `bash scripts/commands/gbx-test-pyrx.sh --path python/geobrix/test/pyrx/test_functions_spark.py --log pyrx.log`
