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
