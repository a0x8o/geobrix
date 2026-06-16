# Stop Docker Container

Stop geobrix-dev container

## Usage

```bash
bash scripts/commands/gbx-docker-stop.sh [OPTIONS]
```

## Options

- `--force` - Force stop (kill immediately)
- `--timeout <seconds>` - Timeout before force stop (default: 10)
- `--help` - Display help message

## Examples

```bash
# Stop container gracefully
bash scripts/commands/gbx-docker-stop.sh

# Force stop immediately
bash scripts/commands/gbx-docker-stop.sh --force

# Stop with custom timeout
bash scripts/commands/gbx-docker-stop.sh --timeout 30
```

## Notes

- Default timeout is 10 seconds
- Force stop uses `docker kill`
- Safe to run even if container not running
- Graceful shutdown allows cleanup operations
