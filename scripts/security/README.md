# scripts/security/

Tooling that implements the Databricks Labs [Repository Lockdown policy](https://docs.google.com/document/d/1J50oKQxG9WhGXWEl5zlbCq5pf9AGh57yDhZh9nxCQC0/edit)
for GeoBrix. Two pinning regimes:

1. **GitHub Actions** — every third-party Action must be pinned to a full
   commit SHA taken from a release published before the
   `2026-03-10T00:00:00Z` cutoff. Tag names are preserved as inline
   comments for human-readable cross-reference; the comment is **not**
   authoritative — reviewers verify the SHA against the referenced
   release.

2. **Maven dependencies** — every dependency, transitive dependency,
   plugin, and plugin dependency must be signed by a PGP key whose
   fingerprint appears in `.maven-keys.list` at the repo root. Strict
   verification is implemented by `pgpverify-maven-plugin` under the
   `verify-pgp` Maven profile (see `pom.xml`). Today the profile is
   opt-in (`-Pverify-pgp`); once the keysmap is fully populated and the
   `Verify Maven dependency PGP signatures` workflow is green, flip the
   profile to `<activeByDefault>true</activeByDefault>` and add the
   workflow to required status checks.

## Scripts

| Script | Requires | Purpose |
|---|---|---|
| `list-external-actions` | `yq` (Mike Farah) | Emit the set of external actions referenced by any workflow or composite action under `.github/`, one per line. |
| `resolve-action-ref` | `gh`, `jq` | For each `action[@ref]`, resolve the most recent pre-cutoff release tag to the commit SHA it points at. Marks already-pinned entries with `✓` and drift with `⚠`. |
| `pin-gh-actions` | `git` | Consume `resolve-action-ref` output, rewrite every `uses:` line under `.github/` to the new SHA form (skipping `databricks*`-owned actions), and stage the result with `git add`. Prints the staged diff for review — **does not commit**. |
| `maven-pgp-bootstrap` | `mvn`, `awk` | Run `pgpverify-maven-plugin` with relaxed settings, capture every PGP fingerprint Maven Central serves for the resolved closure, and emit draft `.maven-keys.list` entries on stdout. The output is a draft — every fingerprint must be cross-checked against the project's published signing key before being committed. |
| `maven-pgp-verify` | `mvn` | Run strict verification (`mvn -Pverify-pgp verify`). Exits non-zero if any artifact is unsigned, weakly signed, or signed by a key not in `.maven-keys.list`. |

## Typical flow — GitHub Actions

```sh
cd "$(git rev-parse --show-toplevel)"

# 1. Preview what would change
./scripts/security/list-external-actions \
  | xargs ./scripts/security/resolve-action-ref

# 2. Apply (stages under .github/)
./scripts/security/list-external-actions \
  | xargs ./scripts/security/resolve-action-ref \
  | ./scripts/security/pin-gh-actions

# 3. Review, then commit
git diff --cached -- .github
git commit -m "Re-pin GitHub Actions to commits from releases prior to 2026-03-10"
```

## Typical flow — Maven PGP keysmap

Run inside the `geobrix-dev` container so Maven hits the `db-maven-proxy`
mirror (the proxy must pass `.asc` files through unmodified — confirm
once before relying on this).

```sh
cd "$(git rev-parse --show-toplevel)"

# 1. Generate a draft keysmap from the current resolved closure.
./scripts/security/maven-pgp-bootstrap > /tmp/draft.list

# 2. Cross-check every fingerprint in /tmp/draft.list against the
#    project's published signing key. Trust-anchor URLs for the direct
#    deps in pom.xml are listed at the top of .maven-keys.list. Do NOT
#    skip this step — the entire trust model rests on it.

# 3. Replace the TODO block in .maven-keys.list with the reviewed
#    entries.

# 4. Confirm strict verification passes.
./scripts/security/maven-pgp-verify

# 5. Commit. Once the workflow is green on master, flip the verify-pgp
#    profile in pom.xml to <activeByDefault>true</activeByDefault> and
#    add the "Verify Maven dependency PGP signatures" check to branch
#    protection's required status checks.
git diff -- pom.xml .maven-keys.list
git commit -m "Populate Maven PGP keysmap from reviewed signatures"
```

## Notes

- **`databricks*` / `databrickslabs*` actions are skipped.** They are
  considered first-party by the policy and do not require pinning; they
  remain on tag references.
- **Mono-repo tag prefixes.** `resolve-action-ref` handles actions under a
  mono-repo path (e.g. `databrickslabs/sandbox/acceptance` → tags like
  `acceptance/v0.4.4`). Review the `⚠` output before applying — the doc
  flags this as a known glitch.
- **`pin-gh-actions` does not switch branches.** Unlike the reference
  implementation at `databrickslabs/blueprint`, this script assumes the
  caller has already checked out the target branch.
- **Comment is informational only.** A reviewer verifying this PR must
  re-run `resolve-action-ref` (or an equivalent `gh api` lookup) to
  confirm every SHA corresponds to the claimed tag.

## Refresh cadence

**GitHub Actions:** the cutoff date is a constant inside
`resolve-action-ref` and `pin-gh-actions`. It will only change when the
policy is updated by the Databricks Labs team, at which point both
scripts should be updated in lockstep.

**Maven keysmap:** re-run `maven-pgp-bootstrap` whenever a Dependabot PR
bumps a Maven dep (or any direct dep is added/removed in `pom.xml`). The
"Verify Maven dependency PGP signatures" workflow runs automatically on
PRs touching `pom.xml` or `.maven-keys.list`, so drift surfaces as a
failing required check.
