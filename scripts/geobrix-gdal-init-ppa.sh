#!/bin/bash
#
# ============================================================================
# LEGACY PATH — slow cluster start (~15 minutes)
# ============================================================================
# This script adds the UbuntuGIS PPA, downloads + installs GDAL .debs on every
# cluster boot, and SOURCE-COMPILES the GDAL Python bindings against the
# freshly-installed libgdal-dev. Total cold-start cost on a Databricks cluster
# is typically 10–15 minutes (PPA fetch + apt install + ~5–8 min source build
# of GDAL[numpy] under pip --no-binary :all:).
#
# Prefer scripts/geobrix-gdal-init.sh for new clusters: it installs the same
# fingerprint-verified set of artifacts in 30–90 seconds by pre-staging the
# CI-built bundle in a Unity Catalog Volume. Keep this script around only for:
#   - bootstrapping the very first artifact bundle (CI uses this dance to
#     produce what gets staged for the tarball script), or
#   - troubleshooting a cluster that can't read from the staging volume.
#
# Databricks cluster init script. This file is uploaded to a Workspace
# volume and run by the cluster on boot — the ubuntugis PPA signing key is embedded
# inline below. Keep this file self-contained.

set -euo pipefail

sudo add-apt-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-backports main universe multiverse restricted"
sudo add-apt-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-updates main universe multiverse restricted"
sudo add-apt-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-security main multiverse restricted universe"
sudo add-apt-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc) main multiverse restricted universe"

# - add ubuntugis PPA with fingerprint-pinned GPG key.
#   We do NOT call `add-apt-repository ppa:ubuntugis/ubuntugis-unstable`:
#   that helper auto-installs whatever key Launchpad serves (TOFU). Instead
#   the signing key is embedded below and rejected unless its fingerprint
#   matches UBUNTUGIS_FPR — so a tampered cluster image, a swapped key
#   block in this script, or a Launchpad MITM all fail closed before any
#   GDAL package gets pulled through the PPA's signing chain.
#
#   Expected fingerprint sourced from Launchpad's signing_key_fingerprint API:
#     curl https://launchpad.net/api/1.0/~ubuntugis/+archive/ubuntu/ubuntugis-unstable \
#       | jq -r .signing_key_fingerprint
#   Re-verify on key bump and update the embedded block below in lockstep.
UBUNTUGIS_FPR="2EC86B48E6A9F326623CD22FFF0E7BBEC491C6A1"
UBUNTUGIS_KEYRING="/etc/apt/keyrings/ubuntugis.gpg"
UBUNTUGIS_LIST="/etc/apt/sources.list.d/ubuntugis-unstable.list"

sudo apt-get install -y software-properties-common gpg

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

sudo install -d -m 0755 /etc/apt/keyrings
sudo gpg --dearmor --yes -o "$UBUNTUGIS_KEYRING" < "$UBUNTUGIS_KEY_ASC"
sudo chmod 0644 "$UBUNTUGIS_KEYRING"

CODENAME="$(lsb_release -sc)"
{
    echo "deb [signed-by=${UBUNTUGIS_KEYRING}] https://ppa.launchpadcontent.net/ubuntugis/ubuntugis-unstable/ubuntu ${CODENAME} main"
    echo "deb-src [signed-by=${UBUNTUGIS_KEYRING}] https://ppa.launchpadcontent.net/ubuntugis/ubuntugis-unstable/ubuntu ${CODENAME} main"
} | sudo tee "$UBUNTUGIS_LIST" >/dev/null

sudo apt-get update -y

# Update VOL_DIR to point at the Unity Catalog volume where you've staged
# libgdalalljni.so + geobrix-*-jar-with-dependencies.jar before deploying
# this script to a cluster.
VOL_DIR="/Volumes/geospatial_docs/gdal_artifacts/noble/geobrix"
if [ ! -d "$VOL_DIR" ]; then
    echo "VOL_DIR not found: $VOL_DIR" >&2
    echo "Edit this script and set VOL_DIR to the volume containing the GeoBrix native + JAR artifacts before re-running." >&2
    exit 1
fi

# install natives — keep GDAL_PPA_VERSION in sync with CI (.github/actions/*/action.yml).
# https://gdal.org/en/stable/api/python/python_bindings.html
# https://medium.com/@felipempfreelancer/install-gdal-for-python-on-ubuntu-24-04-9ed65dd39cac
GDAL_PPA_VERSION="3.11.4+dfsg-1~noble0"
sudo apt-get -o DPkg::Lock::Timeout=-1 install -y unixodbc libcurl3-gnutls libsnappy-dev libopenjp2-7
sudo apt-get -o DPkg::Lock::Timeout=-1 install -y \
  "libgdal-dev=${GDAL_PPA_VERSION}" \
  "gdal-bin=${GDAL_PPA_VERSION}" \
  "python3-gdal=${GDAL_PPA_VERSION}"

# pip install GDAL (match deps to DBR 17.3 LTS — see release notes for the runtime).
# Bootstrap pins must match .github/actions/{scala,python}_build/action.yml — keep these in sync.
pip install --upgrade pip==25.0.1 setuptools==80.9.0 wheel==0.45.1 cython==3.0.12  # setuptools >= 77.0.0 required for GDAL 3.11+ sdist's PEP 639 SPDX license string
pip install numpy==2.1.3
export GDAL_CONFIG=/usr/bin/gdal-config
# --no-binary :all: forces sdist compile against the apt-installed libgdal
# headers above (signed by the fingerprint-pinned ubuntugis key), rather
# than accepting whatever pre-built wheel PyPI happens to serve.
pip install --no-cache-dir --no-binary :all: --force-reinstall GDAL[numpy]=="$(gdal-config --version).*"

# copy JNI and JAR. Quote VOL_DIR so paths with spaces don't break under
# `set -u`; the glob expands after substitution.
cp "$VOL_DIR/libgdalalljni.so" /usr/lib/libgdalalljni.so
cp "$VOL_DIR"/geobrix-*-jar-with-dependencies.jar /databricks/jars
