# Local GitHub Actions dry-run (`act`)

Validate `.github/workflows/*.yml` changes locally before pushing. Catches ~80% of CI bugs (typos, missing env, action SHA pin issues, step ordering, composite-action structure errors).

## What's here

| File | Purpose |
|---|---|
| `Dockerfile.gha-runner` | Runner image: `catthehacker/ubuntu:act-24.04` (slim ~700 MB) + corp pip/Maven/npm proxy pre-baked. Tagged `geobrix-ci-runner:local`. |
| `pip.conf` | Pip config baked into the runner image (`pypi-proxy.dev.databricks.com`). |
| `maven-settings.xml` | Maven `<mirror>` settings baked into the runner image (`maven-proxy.dev.databricks.com`). |
| `npmrc` | npm registry config baked into the runner image (`npm-proxy.dev.databricks.com`); also covers yarn classic + pnpm. Used by `deploy-docs.yml`'s `npm ci`. |
| `jfrog-auth-stub/action.yml` | No-op composite action. Copied by `run-act.sh` into a sibling workspace mirror (`.cache/act-workspace/`) where it overlays the real one — see "How real `.github/` stays untouched" below. |
| `run-act.sh` | Pre-flight checks + image build + workspace-mirror prep + `act` invocation. |

## Quick start

```bash
# One-time install
brew install act

# Use via the Cursor command (recommended)
gbx:ci:act -l                                          # list jobs
gbx:ci:act -W .github/workflows/build_main.yml -j build  # run one job
gbx:ci:act push                                        # simulate a push event

# Or directly
bash scripts/ci-local/run-act.sh -l
```

First run builds `geobrix-ci-runner:local` (~5 min). Subsequent runs reuse the image.

## How real `.github/` stays untouched

`act` parses workflow + composite-action YAML on the **host filesystem** (before any container starts), so a Docker-only overlay isn't enough. Instead we materialize a mirror at `.cache/act-workspace/` (gitignored):

```
PROJECT_ROOT/                       # real, never modified
├── .github/                        # CI uses this — untouched
└── ...

.cache/act-workspace/               # gitignored mirror (rebuilt on every run)
├── .github/                        # FRESH COPY of real .github/ ...
│   └── actions/
│       └── jfrog-auth/
│           └── action.yml          # ... with the stub overlaid here
├── pom.xml -> ../../pom.xml        # symlink
├── src/    -> ../../src/           # symlink
├── scripts/ -> ../../scripts/      # symlink
├── .git    -> ../../.git           # symlink (so git-aware actions work)
└── ...                             # all other top-level entries symlinked
```

`run-act.sh` `cd`s into the mirror, then invokes `act --bind`, so the bind-mounted workspace is the mirror. The stub composite emits a `::notice::` and exits; pip/Maven still find the corp proxy via the pre-baked config in the runner image.

The mirror is regenerated from scratch on every run — fast (~50 ms) because `.github/` is small (~100 KB) and everything else is symlinked.

## Deliberate local-act variations from CI

The mirror's composite actions are patched in two ways for local-act runs only. Both are act-environment workarounds, NOT changes to CI behavior:

| What | Why | Visible at runtime |
|---|---|---|
| `${{ matrix.X }}` → literal value | act doesn't propagate matrix context into composite-action `run:` blocks (nektos/act#2206-class issue). Pin values come from `gbx:versions:audit`. | `🔧 Rewriting matrix refs in mirror's composite actions...` |

The runner image also pre-installs `gdal[numpy]==3.11.4` into the tool-cache Python so the workflow's `pip install --no-build-isolation gdal[numpy]==X` step becomes a no-op. Reason: GDAL 3.11.4's `pyproject.toml` declares both `[project.license].file` and `[project.license].text`, which modern setuptools rejects (`invalid pyproject.toml config: project.license`). pip's "already satisfied" path skips the pyproject.toml validation entirely. See `Dockerfile.gha-runner`.

Both edits live only in `.cache/act-workspace/.github/actions/`. The real `.github/actions/{scala,python}_build/action.yml` files on disk are unchanged.

If a workflow change is suspected to be the source of a CI failure, check `gbx:versions:audit` and the run-act.sh patch list to make sure the variation isn't masking the real issue.

## What's covered

- Workflow YAML syntax + action SHA pin validity
- Step ordering (e.g. would `setup-python` actually be available before our pip step?)
- Composite action structure (`scala_build`, `python_build`)
- pip + Maven installs (route through corp proxy, same as the dev container)
- Most matrix expansions
- Conditional `if:` evaluation

## What's NOT covered

- **JFrog OIDC**: no real GitHub OIDC issuer locally — the real `jfrog-auth` is bind-mounted out
- **`runs-on: { group: larger-runners, labels: larger }`**: treated as a label alias only — no actual "larger" machine; you get whatever Docker resources you allocate
- **Real GitHub event payloads**: `act` mocks `head_sha`, `head_ref`, etc. with placeholder values
- **Secrets**: `CODECOV_TOKEN`, `REPO_ACCESS_TOKEN` aren't set; workflow falls back to `GITHUB_TOKEN` via the `||` pattern, and codecov-action has `fail_ci_if_error: false` so it warns instead
- **Runner-group access policy**: org-level allowlist isn't simulated; if a workflow is gated by a protected env, that gating isn't replayed locally

## Iteration loop

```bash
# Edit a workflow…
vim .github/workflows/build_main.yml

# …and dry-run it
gbx:ci:act -W .github/workflows/build_main.yml -j build --pull=false

# When clean, push and let real CI take over (which exercises JFrog OIDC + larger runners)
git push
```

## Architecture (Apple Silicon caveat)

Real GitHub-hosted runners are **linux/amd64**. We build the runner image and
run `act` with `--container-architecture linux/amd64` to match. On Apple
Silicon, Docker Desktop emulates amd64 via Rosetta (≈1.5–2× slowdown).

Why not native arm64 on Apple Silicon? Ubuntu's `archive.ubuntu.com` only
ships **amd64** packages — arm64 lives on `ports.ubuntu.com` via a different
path. Native-arm64 runs 404 on every `apt-get install` because the workflow
sources.list points at `archive.ubuntu.com`. Forcing amd64 is the simplest
fix that also keeps us workflow-faithful (real CI is amd64 too).

## Maintenance

- **If catthehacker bumps `ubuntu:full-24.04`**, rebuild: `docker rmi geobrix-ci-runner:local && bash scripts/ci-local/run-act.sh -l`.
- **If proxy URLs change** (per go/pypi-registry-access or go/maven-registry-access), edit `pip.conf` / `maven-settings.xml` and rebuild.
- **If a new composite action also needs OIDC and act-stubbing**, add a sibling stub directory and extend `run-act.sh` with another `--container-options` mount.

## See also

- [act docs](https://nektosact.com/) — full `act` command reference
- `.cursor/agents/docker.md` — "Local GHA dry-runs with `act`" section
- `prompts/security/2026-05-06-jfrog-runner-versions-securitymd.md` — context for why JFrog OIDC exists
