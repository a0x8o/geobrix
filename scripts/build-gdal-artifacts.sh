#!/bin/bash
#
# Developer-local tool that produces the slowly-changing GDAL "platform"
# tarball which lives under resources/static/ (tracked by Git LFS) and is
# reused across many GeoBrix releases. Rebuild + commit only when:
#   - GDAL_PPA_VERSION below changes
#   - A new Ubuntu LTS becomes the DBR base image (noble → 26.04)
#   - A security advisory against one of the bundled libraries
#
# Run inside a fresh Ubuntu 24.04 container so the resolved .debs and
# source-built wheel match the DBR 17.3 LTS (noble) cluster base image:
#
#   docker run --rm -it -v "$PWD":/work -w /work ubuntu:24.04 bash -c '
#       apt-get update && apt-get install -y sudo &&
#       ./scripts/build-gdal-artifacts.sh \
#           --jni scripts/gdal311/libgdalalljni.so \
#           --out /tmp/gdal-artifacts \
#           --platform-only
#   '
#
# Then commit the two outputs into resources/static/:
#   geobrix-gdal-platform-noble.tar.gz          (Git LFS)
#   geobrix-gdal-platform-noble.tar.gz.sha256
#
# Architecture: x86_64 / amd64 only — these two names refer to the same
# instruction set (Intel and AMD CPUs both qualify; `amd64` is just
# Debian's name for `x86_64`). The PPA ships only amd64 binaries; building
# on ARM / aarch64 (Graviton, Ampere, Apple Silicon) would produce a bundle
# the cluster init script refuses to install. The arch check below fails
# fast on the wrong host.
#
# This script is the upstream end of the trust chain: it verifies the
# UbuntuGIS PPA key fingerprint against UBUNTUGIS_FPR, adds the PPA,
# downloads the resolved .deb set, source-builds the GDAL Python wheel
# against those headers, writes a SHA256SUMS manifest inside the bundle,
# then packages everything into a single .tar.gz with a .sha256 sidecar.
# The PR reviewer reruns this script locally and confirms the sha256
# matches the committed file — that PR-review step IS the trust anchor for
# everything downstream (release workflow, cluster init script, runtime).
#
# Legacy mode: --jar (without --platform-only) produces a release-shape
# tarball with the JAR baked in. Kept for one-off debugging; the canonical
# release flow at .github/workflows/package-geobrix-artifacts.yml grafts the
# per-release JAR into the committed platform tarball itself.

set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

# ---- arch guard ----------------------------------------------------------
# Bail before we burn time downloading amd64 .debs onto an ARM build host.
# Intel and AMD CPUs both report `x86_64` here and are equally supported;
# the exclusion is aarch64 (Graviton, Ampere, Apple Silicon).
HOST_ARCH="$(uname -m)"
if [ "$HOST_ARCH" != "x86_64" ]; then
    echo "Unsupported build host architecture: $HOST_ARCH" >&2
    echo "This script produces amd64 / x86_64 artifacts only (Intel or AMD CPUs)." >&2
    echo "Run on a non-ARM builder." >&2
    exit 1
fi

# ---- args ----------------------------------------------------------------
# Two modes:
#   default       : produce a release-shape tarball with the JAR baked in
#                   (named geobrix-gdal-artifacts-v<version>-noble.tar.gz).
#                   Used historically when CI built per-release tarballs.
#   --platform-only : produce the slowly-changing platform bundle without
#                   any JAR, named geobrix-gdal-platform-noble.tar.gz. This
#                   is the file that gets committed under resources/static/
#                   and reused across many releases. The per-release JAR is
#                   grafted in later by package-geobrix-artifacts.yml.
JNI_PATH=""
JAR_PATH=""
OUT_DIR=""
PLATFORM_ONLY="false"
while [ $# -gt 0 ]; do
    case "$1" in
        --jni) JNI_PATH="$2"; shift 2 ;;
        --jar) JAR_PATH="$2"; shift 2 ;;
        --out) OUT_DIR="$2"; shift 2 ;;
        --platform-only) PLATFORM_ONLY="true"; shift ;;
        -h|--help)
            grep '^# ' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

[ -n "$JNI_PATH" ] && [ -f "$JNI_PATH" ] || { echo "--jni PATH (existing file) required" >&2; exit 1; }
[ -n "$OUT_DIR" ] || { echo "--out DIR required" >&2; exit 1; }
if [ "$PLATFORM_ONLY" = "false" ]; then
    [ -n "$JAR_PATH" ] && [ -f "$JAR_PATH" ] || { echo "--jar PATH (existing file) required (or pass --platform-only to skip)" >&2; exit 1; }
fi

# sudo no-op when running as root (typical inside Docker).
SUDO=""
[ "$EUID" -ne 0 ] && SUDO="sudo"

# ---- pins (keep in lockstep with the runtime scripts and CI actions) -----
# Same key fingerprint as geobrix-gdal-init.sh — if the embedded block below
# ever drifts from the runtime script's block, the fingerprint check fails
# closed in both places.
UBUNTUGIS_FPR="2EC86B48E6A9F326623CD22FFF0E7BBEC491C6A1"

# Same GDAL version as geobrix-gdal-init.sh line 100. Bump in lockstep.
GDAL_PPA_VERSION="3.11.4+dfsg-1~noble0"

# Pip toolchain pins — match .github/actions/{scala,python}_build/action.yml
# and the original geobrix-gdal-init.sh lines 109–110.
PIP_VERSION="25.0.1"
SETUPTOOLS_VERSION="80.9.0"  # >= 77.0.0 required to parse PEP 639 SPDX license strings in GDAL 3.11+ sdist (`license = "MIT"`); 74.0.0 fails with `project.license must be valid exactly by one definition`. Keep in lockstep with python/geobrix/requirements-ci.in.
WHEEL_VERSION="0.45.1"
CYTHON_VERSION="3.0.12"
NUMPY_VERSION="2.1.3"

# Non-PPA system packages the original init script pulled (line 101). We
# bundle them too so cluster start can dpkg -i without any apt-get update.
EXTRA_SYSTEM_PKGS=(unixodbc libcurl3-gnutls libsnappy-dev libopenjp2-7)

# ---- output dirs ---------------------------------------------------------
OUT_DIR="$(cd "$(dirname "$OUT_DIR")" && pwd)/$(basename "$OUT_DIR")"
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR/debs" "$OUT_DIR/wheels"

# ---- step 1: add UbuntuGIS PPA with fingerprint-pinned key ---------------
# This is the runtime trust anchor's *upstream* end: anything that flows into
# the artifact bundle from the PPA is signed by a key whose fingerprint we
# match against UBUNTUGIS_FPR here. If a future Launchpad key rotation or a
# tampered key block changes the fingerprint, this fails BEFORE any .deb gets
# downloaded into the bundle.

$SUDO apt-get update -y
$SUDO apt-get install -y ca-certificates gpg lsb-release python3-pip python3-venv

UBUNTUGIS_KEYRING="/etc/apt/keyrings/ubuntugis.gpg"
UBUNTUGIS_LIST="/etc/apt/sources.list.d/ubuntugis-unstable.list"

UBUNTUGIS_KEY_ASC="$(mktemp)"
trap 'rm -f "$UBUNTUGIS_KEY_ASC"' EXIT
cat > "$UBUNTUGIS_KEY_ASC" <<'UBUNTUGIS_KEY_EOF'
-----BEGIN PGP PUBLIC KEY BLOCK-----
Comment: Hostname:
Version: Hockeypuck 2.2

xsFNBGYzcWcBEACZy6Cs/d6xE5dYOX7MY9nMNGALohNGal+lT/gvuU16NYrXV/qs
7NyOLjUmFuEflrbMbOuqW6XaK8FRCkOCMbJAGcxlieLK7e2oV472rw/fMVJYk9du
ebQoYcNfB4Pylb4xpZvG9+zwWWICMZG8JlcV+hLWAC5L9WY/6GycRZMarukPntY5
f9r6KMohMtcpiqjtpIccTKbxLwB/wPRTri2+clSG1PABhIhLzQqQv2qIlsVGjt0r
eP1DjoNin0yrBrsNZysVSEQW4/3KEW4PN4VqhoGwrNPygN0dwCyQ/yn+ulFhwzgI
KTGlDkEEn+ozONMIccWjGxck3SCjCCH2QO3UwX10AifChgFoms5mKuE0MLYRqgWK
wPGly5n5yBOhz8ctXRQ7L0613hJ6GiBkZMqOTIdXY4NT52e6tsXTaJ/Jx4VwFg64
j0qJZ5TE1Z//kSTpEmEELsq0rl3Iz9gxeMqalVhoJXBRKb7MMwJn4p0rjbhp9jWj
4tN26LqwLfCNVPrEomUG7ERG6Rs45CfPOh3bLCm9yd3++bcAGN8ne3F1YABY/kyf
bXtjQ/ihhpFMbqUtcUkEIS8xfbnwdORvH+wmaBbSpaMW1JCJNmM3KsdzY16PsckO
Z7YHAqZacirlNN/dZbsFLow958ssjwgGYquVNhiBckE2vIzObdrcHqsx8QARAQAB
zRtMYXVuY2hwYWQgUFBBIGZvciBVYnVudHVHSVPCwY4EEwEKADgWIQQuyGtI5qnz
JmI80i//Dnu+xJHGoQUCZjNxZwIbAwULCQgHAgYVCgkICwIEFgIDAQIeAQIXgAAK
CRD/Dnu+xJHGoY8RD/9nviKd8w55J7MxUhI3s6ka15BXqKamZ7zmVn+nYNU9QY3V
HK3gh1Z1SytNcS572AZuym1dTGe779zfIchQ6VN8aFwhLTKMyg4FBGP0opYCPEG1
y2wwcSTNeOyiwPBECYae0tXi9btYB3GswO30GaQXTpKAy0LDaHSm4zfUkKfnofAQ
lZdznTXgxUJqSn8fzFMIY4bDEImgRp1TS5sIavKQKpFLNJKP1bnCl1/YSTm67SOx
rH1Q0URKJIRsgfj/L4Rt1SW8EZqFb9tDHfcfGSpdvD7LWe7NMVYHBn9CUsSMbfW8
SwBkUAw/6l0ODeKmUNqSbYTia0GBhX/LwsFrc3cydSlX8NZSKwGztM9F+tOHXaS9
eVap7Ow6dTuaw/fyJIf57PAVSAkmJ41nSAygr4XaleDTJXHE4T0tHWusb3AXdKUR
4bSthlSQKrFnYnLTBKuN5ijQ5TLzFbMjD22JvFpSQeQeGYkjNfmLOcLU1p4pWCM+
z5EgjOJcGPbjFqlEkMraUPONJuzFdAnx6d7OdGY9TWserSuI8+392mXhU+9SiS8T
nrbb0Y/WYJmcqkQRmwe6eCs7G+3UJhulUKWEYm37255aNiHKJl+FZEgZ9Zh5tsN/
RrcIov5r9ncdNv8VP6c6IkOCbH9bOo4jto02TV/WMACEcXCVU7nZCdbCYpHCqA==
=cYNc
-----END PGP PUBLIC KEY BLOCK-----
UBUNTUGIS_KEY_EOF

actual_fpr=$(gpg --show-keys --with-fingerprint --with-colons "$UBUNTUGIS_KEY_ASC" \
    | awk -F: '/^fpr:/ {print $10; exit}')

if [ -z "$actual_fpr" ] || [ "$actual_fpr" != "$UBUNTUGIS_FPR" ]; then
    echo "ubuntugis key fingerprint mismatch: got='${actual_fpr}' expected='${UBUNTUGIS_FPR}'" >&2
    exit 1
fi

$SUDO install -d -m 0755 /etc/apt/keyrings
$SUDO gpg --dearmor --yes -o "$UBUNTUGIS_KEYRING" < "$UBUNTUGIS_KEY_ASC"
$SUDO chmod 0644 "$UBUNTUGIS_KEYRING"

CODENAME="$(lsb_release -sc)"
if [ "$CODENAME" != "noble" ]; then
    echo "WARNING: building on '${CODENAME}', not noble — artifacts may not match DBR 17.3 LTS." >&2
fi

echo "deb [signed-by=${UBUNTUGIS_KEYRING}] https://ppa.launchpadcontent.net/ubuntugis/ubuntugis-unstable/ubuntu ${CODENAME} main" \
    | $SUDO tee "$UBUNTUGIS_LIST" >/dev/null

$SUDO apt-get update -y

# ---- step 2: download the runtime .deb set into the bundle --------------
# IMPORTANT: request libgdal37 (runtime), NOT libgdal-dev (headers). The
# transitive closure of libgdal-dev pulls in build-time helpers (automake,
# libtool, autotools-dev, etc.) that the cluster's dpkg later fails to
# configure (their post-install scripts depend on packages not in DBR's
# base image). Requesting libgdal37 walks only the runtime side of the
# dep graph — libproj25, libgeos-c1t64, proj-data, libhdf5*, libnetcdf*,
# libspatialite8t64, etc. — which is exactly what the cluster needs.
#
# libgdal-dev IS installed below (step 2b), but only into THIS build
# container so we can source-compile the GDAL Python wheel against its
# headers. Its .debs never enter the bundle.
$SUDO apt-get clean
$SUDO apt-get install -y --reinstall --download-only \
    "libgdal37=${GDAL_PPA_VERSION}" \
    "gdal-bin=${GDAL_PPA_VERSION}" \
    "python3-gdal=${GDAL_PPA_VERSION}" \
    "${EXTRA_SYSTEM_PKGS[@]}"

# Collect them. /var/cache/apt/archives/partial/ holds in-flight downloads —
# ignore that. Use cp -L just in case any .deb is a symlink (rare but safe).
shopt -s nullglob
debs=(/var/cache/apt/archives/*.deb)
[ "${#debs[@]}" -gt 0 ] || { echo "no .debs downloaded — check apt-get output above" >&2; exit 1; }
$SUDO cp -L "${debs[@]}" "$OUT_DIR/debs/"
$SUDO chown -R "$(id -u):$(id -g)" "$OUT_DIR/debs"

# Safety-net filter: drop any straggler -dev / autotools / -tools packages
# that may have slipped in. With the step 2 change above (request libgdal37
# instead of libgdal-dev), apt should walk only the runtime side of the
# transitive graph and these globs should match nothing — but if a future
# DBR runtime lib bump or PPA rebuild surprises us, this keeps the bundle
# clean. Each pattern is something we KNOW is build-time-only:
#   *-dev_*.deb    : headers + pkg-config (libfoo-dev)
#   automake / libtool / autotools-dev : configure script helpers
#   libpng-tools   : has exact-version pin on libpng16-16t64 that's
#                    out-of-sync with DBR base
( cd "$OUT_DIR/debs" && \
    rm -f -- *-dev_*.deb \
             automake_*.deb libtool_*.deb autotools-dev_*.deb autoconf_*.deb \
             libpng-tools_*.deb )

# Emit a human-readable manifest of every .deb in the bundle. The PPA's
# libgdal is built against the PPA's libproj / libgeos / proj-data, so those
# come in via transitive deps and MUST be present here — otherwise the
# cluster's older system libproj wins at link time and runtime CRS / geometry
# operations break in subtle ways. The grep-and-fail checks below make the
# presence of those transitive PPA packages an explicit invariant rather
# than an implicit consequence of apt's resolver.
# dpkg-deb -W only inspects the FIRST file in its arglist even when many
# are passed — so loop one file at a time. ~200 .debs × ~10 ms each is
# ~2 s, negligible against the wheel build below.
( cd "$OUT_DIR/debs" && \
    for deb in *.deb; do
        dpkg-deb -W --showformat='${Package} ${Version} ${Architecture}\n' "$deb"
    done | LC_ALL=C sort > "$OUT_DIR/PACKAGES.txt" )

for required in libgdal libproj libgeos proj-data; do
    if ! grep -q "^${required}" "$OUT_DIR/PACKAGES.txt"; then
        echo "ERROR: no ${required}* package in bundle — apt resolver did not pull it as a transitive dep of libgdal-dev. Check PPA contents and rerun." >&2
        exit 1
    fi
done

# Install libgdal-dev + others into THIS build container only — these
# are needed for the GDAL Python wheel source-compile below. By this
# point step 2 has already collected the RUNTIME debs into the bundle,
# so installing libgdal-dev here doesn't add it to the bundle; this is
# strictly a build-time concern.
$SUDO apt-get install -y \
    "libgdal-dev=${GDAL_PPA_VERSION}" \
    "gdal-bin=${GDAL_PPA_VERSION}" \
    "python3-gdal=${GDAL_PPA_VERSION}" \
    "${EXTRA_SYSTEM_PKGS[@]}"

# ---- step 3: build wheels in an isolated venv ----------------------------
# A venv keeps system Python's site-packages out of the resolution graph so
# `pip wheel` produces deterministic outputs against only the pinned set.

VENV="$(mktemp -d)/venv"
python3 -m venv "$VENV"
# shellcheck disable=SC1091
source "$VENV/bin/activate"

# Upgrade pip itself first so the resolver/build-backend code matches the pin.
pip install --no-cache-dir "pip==${PIP_VERSION}"

# Pip toolchain + numpy: pre-built wheels are fine (no native compile needed
# against system libs).
pip wheel --wheel-dir="$OUT_DIR/wheels" --no-cache-dir \
    "pip==${PIP_VERSION}" \
    "setuptools==${SETUPTOOLS_VERSION}" \
    "wheel==${WHEEL_VERSION}" \
    "cython==${CYTHON_VERSION}" \
    "numpy==${NUMPY_VERSION}"

# Install the build toolchain into the venv so the GDAL sdist build below
# can use it via --no-build-isolation. Without that flag, pip's PEP 517
# isolated build environment re-downloads setuptools/wheel/numpy/cython
# into a temp venv per build — which under amd64 emulation on M-series
# Macs costs 30+ minutes of pip-resolver churn. --no-build-isolation tells
# pip "use what I've already installed in this venv instead." We install
# the same pinned versions the scala_build composite action uses.
pip install --no-cache-dir \
    "setuptools==${SETUPTOOLS_VERSION}" \
    "wheel==${WHEEL_VERSION}" \
    "numpy==${NUMPY_VERSION}" \
    "cython==${CYTHON_VERSION}"

# GDAL: --no-binary :all: forces sdist compile against the libgdal-dev we
# installed in step 2 (signed by the fingerprint-pinned PPA). This is the
# operation the original init script did on every cluster start — here it
# runs once per artifact build.
#
# --no-build-isolation: see note above. Required to keep the M-series
#   emulated build under 30 min instead of multi-hour.
# --no-deps: we already pre-installed numpy + cython at the pinned
#   versions above. Without this flag, pip's resolver walks numpy sdist
#   candidates from PyPI to satisfy the [numpy] extra — and the latest
#   numpy sdists use meson-python as their build backend, which is not
#   in our venv. The resolver fails with `Cannot import 'mesonpy'`
#   before the GDAL compile even starts.
export GDAL_CONFIG=/usr/bin/gdal-config
pip wheel --wheel-dir="$OUT_DIR/wheels" --no-cache-dir --no-build-isolation --no-deps --no-binary :all: \
    "GDAL[numpy]==$(gdal-config --version).*"

deactivate

# ---- step 4: native (+ JAR, unless --platform-only) ----------------------
cp "$JNI_PATH" "$OUT_DIR/libgdalalljni.so"
if [ "$PLATFORM_ONLY" = "false" ]; then
    cp "$JAR_PATH" "$OUT_DIR/$(basename "$JAR_PATH")"
fi

# ---- step 5: SHA256SUMS — the inner trust anchor -------------------------
# Sort for deterministic output so the same inputs always produce the same
# SHA256SUMS file (useful for audit/diff and for reproducible builds).
# Relative paths so the runtime can `cd <extracted> && sha256sum -c SHA256SUMS`.
( cd "$OUT_DIR" && find . -type f ! -name SHA256SUMS -print0 \
    | LC_ALL=C sort -z \
    | xargs -0 sha256sum > SHA256SUMS )

# ---- step 6: package as a single release tarball + sidecar --------------
# One tarball at the release-attachment level (GitHub release, internal
# mirror), plus a matching .sha256 sidecar. The cluster init script reads
# the sidecar from the UC Volume at runtime to get the expected hash and
# tarball filename — no hash is hardcoded in the init script, which means
# a security patch or GDAL bump can re-publish (tarball, sidecar) without
# requiring a new init-script release. Trust binds via UC Volume ACLs.

if [ "$PLATFORM_ONLY" = "true" ]; then
    # Platform tarball: name keyed on Ubuntu codename only — version-stable
    # across many GeoBrix releases. Bumped only when GDAL_PPA_VERSION changes.
    TARBALL_NAME="geobrix-gdal-platform-${CODENAME}.tar.gz"
    VERSION=""
else
    # Release-shape tarball: include GeoBrix version parsed from the JAR name.
    VERSION="$(basename "$JAR_PATH" | sed -nE 's/^geobrix-(.+)-jar-with-dependencies\.jar$/\1/p')"
    [ -n "$VERSION" ] || { echo "could not parse geobrix version from JAR name: $(basename "$JAR_PATH")" >&2; exit 1; }
    TARBALL_NAME="geobrix-gdal-artifacts-v${VERSION}-${CODENAME}.tar.gz"
fi

PARENT_DIR="$(dirname "$OUT_DIR")"
BUNDLE_NAME="$(basename "$OUT_DIR")"

# Use deterministic tar flags so the same inputs produce the same bytes:
# --sort=name for stable entry order, --mtime/--owner/--group to strip
# build-time metadata. Makes the outer SHA256 reproducible across rebuilds
# of the same input set, which is what enables the init-script pin model.
( cd "$PARENT_DIR" && tar \
    --sort=name --mtime='UTC 2020-01-01' --owner=0 --group=0 --numeric-owner \
    -czf "$TARBALL_NAME" "$BUNDLE_NAME" )

TARBALL_PATH="$PARENT_DIR/$TARBALL_NAME"
TARBALL_SHA256="$(sha256sum "$TARBALL_PATH" | awk '{print $1}')"
echo "${TARBALL_SHA256}  ${TARBALL_NAME}" > "$TARBALL_PATH.sha256"

# ---- summary -------------------------------------------------------------
echo
echo "==> Artifact bundle ready: $OUT_DIR"
echo "    debs:   $(ls "$OUT_DIR/debs" | wc -l) packages"
echo "    wheels: $(ls "$OUT_DIR/wheels" | wc -l) wheels"
echo "    GDAL:   $(gdal-config --version)"
echo
echo "==> PPA-sourced transitive deps captured in bundle (these are intentionally"
echo "    the UbuntuGIS PPA versions, not the system Ubuntu versions — they pair"
echo "    with the PPA libgdal that was source-linked against them):"
grep -E '^(libgdal|libproj|libgeos|proj-data|libspatialite|libnetcdf|libhdf[45])' "$OUT_DIR/PACKAGES.txt" \
    | sed 's/^/      /'
echo
echo "==> Tarball: $TARBALL_PATH"
echo "    size:   $(du -h "$TARBALL_PATH" | awk '{print $1}')"
echo "    sha256: $TARBALL_SHA256"
echo "    arch:   amd64 / x86_64 (Intel or AMD) — ARM / aarch64 not supported"
echo

if [ "$PLATFORM_ONLY" = "true" ]; then
    echo "==> --platform-only build. Commit these two files to resources/static/"
    echo "    (the .tar.gz is tracked by Git LFS — see .gitattributes):"
    echo "      $TARBALL_NAME"
    echo "      $TARBALL_NAME.sha256"
    echo
    echo "    The per-release JAR is grafted in at release time by"
    echo "    .github/workflows/package-geobrix-artifacts.yml."
else
    echo "==> Release-shape build. Artifacts to attach to the GeoBrix v${VERSION} release:"
    echo "    1. $TARBALL_NAME                    (the bundle)"
    echo "    2. $TARBALL_NAME.sha256             (sidecar — trust anchor)"
    echo "    3. scripts/geobrix-gdal-init.sh                          (init script — versioned with this release)"
    echo
    echo "==> Operator (per cluster):"
    echo "    1. Download all three from the release page."
    echo "    2. Verify the tarball:    sha256sum -c $TARBALL_NAME.sha256"
    echo "    3. Upload tarball + sidecar to UC Volume (NOT the init script):"
    echo "       databricks fs cp $TARBALL_PATH \\"
    echo "           dbfs:/Volumes/geospatial_docs/gdal_artifacts/noble/geobrix/ --overwrite"
    echo "       databricks fs cp $TARBALL_PATH.sha256 \\"
    echo "           dbfs:/Volumes/geospatial_docs/gdal_artifacts/noble/geobrix/ --overwrite"
fi
echo "    4. Point the cluster's init-script setting at the downloaded"
echo "       geobrix-gdal-init.sh (in workspace files or its own volume path)."
