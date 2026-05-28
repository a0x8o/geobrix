# Docker Exec

Execute commands or launch interactive shells in geobrix-dev container

## Usage

```bash
bash scripts/commands/gbx-docker-exec.sh [MODE|COMMAND] [OPTIONS]
```

## Interactive Shell Modes

- `--spark` - Launch Spark shell (spark-shell)
- `--pyspark` - Launch PySpark shell
- `--python` - Launch Python 3 shell
- `--scala` - Launch Scala REPL
- `--bash` - Launch interactive bash shell

## Command Execution

- `<command>` - Execute bash command and exit
- `--command <cmd>` - Execute bash command and exit (explicit)

## Options

- `--interactive` - Run command in interactive mode (keep TTY)
- `--log <path>` - Write output to log file (non-interactive only)
- `--help` - Display help message

## Examples

```bash
# Interactive shells
bash scripts/commands/gbx-docker-exec.sh --spark
bash scripts/commands/gbx-docker-exec.sh --pyspark
bash scripts/commands/gbx-docker-exec.sh --python
bash scripts/commands/gbx-docker-exec.sh --scala
bash scripts/commands/gbx-docker-exec.sh --bash

# Execute commands
bash scripts/commands/gbx-docker-exec.sh "ls -la /root/geobrix"
bash scripts/commands/gbx-docker-exec.sh "mvn -version"
bash scripts/commands/gbx-docker-exec.sh --command "python3 --version"

# Execute with logging
bash scripts/commands/gbx-docker-exec.sh "mvn test" --log maven-test.log
```

## Notes

- Requires geobrix-dev container to be running
- Interactive shells use `-it` flag (TTY)
- Command execution uses standard `docker exec`
- Logging only available for non-interactive commands
