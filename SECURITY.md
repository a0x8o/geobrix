# Security Policy

We take the security of GeoBrix seriously and appreciate the efforts of security researchers and users to responsibly disclose any vulnerabilities.

## Supported Versions

GeoBrix is currently **Beta** (0.3.x). Security update releases apply only to the latest released version and are not backported to earlier 0.3.x releases. When a security update is released, it will be called out at the top of the version release notes.

## Reporting a Vulnerability

If you discover a security vulnerability in GeoBrix:

- **DO NOT** open a public GitHub issue.
- Please email us at [labs@databricks.com](mailto:labs@databricks.com) with:
  - A description of the vulnerability
  - Steps to reproduce it
  - Potential impact or affected components
- Alternatively, you can also share this information directly with your Databricks representative.

We will review your report promptly and work with you to verify and resolve the issue. We aim to acknowledge receipt of your report within 48 hours.

## Security Best Practices

- Use the latest released version of GeoBrix.
- Review the GeoBrix [documentation](https://databrickslabs.github.io/geobrix/) for recommended configurations and operational security considerations.
- GeoBrix wraps GDAL/OGR, which can read a wide range of raster and vector formats. When ingesting raster or vector data from untrusted sources, prefer the project's recommended GDAL version pin and apply the GDAL driver allowlist guidance in the docs rather than enabling all drivers by default.

## Repository / CI Configuration Requirements

The CI workflows in `.github/workflows/` reference the GitHub Environment
`runtime` (`environment: runtime`) on every job that performs a checkout
with `REPO_ACCESS_TOKEN`. This environment is the trust boundary that
gates non-exempt secrets behind protection rules. **Repository admins
must create the environment with the following protection rules** for
the gating to be meaningful:

- **Deployment branches**: restrict to `main` only.
- **Required reviewers**: at least one CODEOWNER for `.github/`.
- **Wait timer** (optional): a few minutes is reasonable to allow a
  human to cancel an unexpected workflow before it consumes secrets.

If the environment does not exist when a workflow runs, GitHub
auto-creates it with **no protection rules**, defeating the gating
purpose. The workflow continues to function because the token reference
falls back via `${{ secrets.REPO_ACCESS_TOKEN || secrets.GITHUB_TOKEN }}`,
but the security claim is void until protection rules are configured.

The `REPO_ACCESS_TOKEN` secret itself should be a fine-grained PAT
scoped to `contents:write` on this repository only, stored as an
environment secret (not a repo secret). Several workflows
(`build_main.yml` doc-inventory job, `deploy-docs.yml`) rely on this
to push commits back to PR branches with strict-token policies in
effect; the fallback to `GITHUB_TOKEN` works for read but may fail
write operations depending on the org policy.
