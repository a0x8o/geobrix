# Run GitHub Actions locally with act

Validate `.github/workflows/*.yml` changes before pushing by running them locally via [act](https://github.com/nektos/act). Catches ~80% of CI bugs (typos, missing env, action SHA pin issues, step ordering, composite-action structure errors) without a push-and-iterate loop.

## Usage

```bash
bash .cursor/commands/gbx-ci-act.sh [act-arguments...]
```

## Examples

```bash
# List jobs across all workflows
gbx:ci:act -l

# Run one job from one workflow
gbx:ci:act -W .github/workflows/build_main.yml -j build

# Simulate a push event (runs all workflows triggered on push)
gbx:ci:act push

# Simulate a PR event
gbx:ci:act pull_request

# Pass any other arg through to act
gbx:ci:act --help
```

## First-time setup

```bash
brew install act
```

The first invocation builds `geobrix-ci-runner:local` (~5 min). Subsequent runs reuse the image.

## What's wired

- **Runner image**: `catthehacker/ubuntu:full-24.04` (the standard `act` runner image, closest to GitHub-hosted `ubuntu-24.04`) plus Databricks corp registry proxies pre-baked:
  - pip → `pypi-proxy.dev.databricks.com`
  - Maven → `maven-proxy.dev.databricks.com`
  - npm → `npm-proxy.dev.databricks.com`
- **Platform map**: `ubuntu-latest`, `ubuntu-24.04`, `ubuntu-22.04`, `larger`, `linux-ubuntu-latest` → `geobrix-ci-runner:local`
- **JFrog auth stub**: Real `.github/actions/jfrog-auth/action.yml` is bind-mounted over inside the act container with a no-op stub. Real `.github/` on disk is never modified.

## Coverage

| Catches | Doesn't catch |
|---|---|
| YAML syntax errors | JFrog OIDC token exchange (mocked) |
| Action SHA pin issues | `larger-runners` actually being larger |
| Step ordering | Real GitHub event payloads (head_sha, head_ref) |
| Composite action structure | Real secrets (CODECOV_TOKEN, REPO_ACCESS_TOKEN) |
| pip/Maven/npm install correctness | Org-level runner-group access policy |
| Matrix expansion | Protected env / branch protection gating |
| Conditional `if:` evaluation | |

For the gaps, push to a draft PR and let real CI exercise them.

## Notes

- The runner image is roughly 2 GB; cached locally after first build
- `act` reuses the image across runs; rebuild via `docker rmi geobrix-ci-runner:local && gbx:ci:act -l`
- Full mechanics in `scripts/ci-local/README.md`; runbook context in `.cursor/agents/docker.md`
