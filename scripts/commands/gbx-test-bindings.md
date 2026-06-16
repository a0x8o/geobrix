# gbx:test:bindings

Verify every registered GeoBrix function exists across all language bindings.

Checks that each name in `docs/tests-function-info/registered_functions.txt` (the canonical SQL surface) also appears as a Scala companion (`override def name`), a Python binding (`functions.py`), and a `function-info.json` entry. Exits non-zero if any registered function is missing from a binding (which would surface at runtime as `UNRESOLVED_ROUTINE`). Runs on the host — pure file parsing, no Docker.

## Usage

```bash
bash scripts/commands/gbx-test-bindings.sh [OPTIONS]
```

## Options

- `--log <path>` — write output to a log file (`filename` → `test-logs/filename`; relative → under `test-logs/`; absolute → as-is)
- `--help`, `-h` — show help and exit

## Examples

```bash
bash scripts/commands/gbx-test-bindings.sh
bash scripts/commands/gbx-test-bindings.sh --log binding-parity.log
```
