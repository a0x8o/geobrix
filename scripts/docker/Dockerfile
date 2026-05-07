# --- GLOBAL ARGUMENTS ---
ARG BUILDPLATFORM=linux/amd64
ARG GDAL_VERSION=3.11.4
ARG PDAL_VERSION=2.8.2
# Pinned commit for PDAL ${PDAL_VERSION} (Labs lockdown policy: tags are mutable).
# To bump PDAL_VERSION, resolve the new tag with:
#   gh api repos/PDAL/PDAL/git/refs/tags/<version> --jq .object.sha
# and update PDAL_SHA in lockstep.
ARG PDAL_SHA=736fa0a66af4bed7105dff5fa152edf26bbb8a3a

# ==========================================
# STAGE 1: OS Base & Java
# e.g. `docker build --target base -t geobrix-base .`
# ==========================================
# Pinned by digest (Labs lockdown policy). To refresh, re-pull ubuntu:24.04 from
# Docker Hub and update the sha256 below. Tag comment retained for readability.
FROM --platform=${BUILDPLATFORM} ubuntu@sha256:c4a8d5503dfb2a3eb8ab5f807da5bc69a85730fb49b5cfca2330194ebcc41c7b AS base
# ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg && mkdir -p /usr/share/keyrings

RUN curl -fsSL https://repos.azul.com/azul-repo.key | gpg --dearmor -o /usr/share/keyrings/azul.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/azul.gpg] https://repos.azul.com/zulu/deb stable main" > /etc/apt/sources.list.d/azul.list

RUN apt-get update && apt-get install -y zulu17-jdk --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Fix Java Pathing
RUN ln -s /usr/lib/jvm/zulu17-ca-amd64 /usr/lib/jvm/zulu-17
ENV JAVA_HOME=/usr/lib/jvm/zulu-17
ENV PATH=$JAVA_HOME/bin:$PATH

# ==========================================
# STAGE 2: System Dependencies
# e.g. `docker build --target system-deps -t geobrix-system-deps .`
# ==========================================
FROM base AS system-deps

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    software-properties-common && \
    add-apt-repository -y universe && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ant autoconf automake bash-completion build-essential cmake g++ git \
    libcurl4-gnutls-dev libgeos-dev libgeotiff-dev libhdf4-alt-dev libhdf5-dev \
    libjsoncpp-dev libnetcdf-dev libopenjp2-7-dev libpoppler-dev libpoppler-private-dev \
    libpq-dev libproj-dev libsnappy-dev libspatialite-dev libsqlite3-dev \
    libtiff-dev libtool libwebp-dev libxml2-dev ninja-build pkg-config \
    proj-bin python3-dev python3-pip python3-venv sqlite3 swig unixodbc wget && \
    rm -rf /var/lib/apt/lists/*

# Global Env Vars
ENV ROOTDIR=/usr/local
ENV LD_LIBRARY_PATH=/usr/local/lib
ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
ENV JUPYTER_RUNTIME_DIR=/tmp/jupyter_runtime
ENV JUPYTER_ALLOW_INSECURE_WRITES=1
ENV CORES=2

WORKDIR $ROOTDIR/

# ==========================================
# STAGE 3: Hadoop Builder
# e.g. `docker build --target hadoop-builder -t geobrix-hadoop-builder .`
# ==========================================
FROM system-deps AS hadoop-builder
ENV HADOOP_VERSION=3.4.0
ENV HADOOP_HOME=/usr/local/hadoop
# Pinned SHA-512 of the Apache Hadoop 3.4.0 source tarball (Labs lockdown policy).
# Cross-checked against https://downloads.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz.sha512
# To bump HADOOP_VERSION, update HADOOP_SHA512 in lockstep.
ENV HADOOP_SHA512=6f653c0109f97430047bd3677c50da7c8a2809d153b231794cf980b3208a6b4beff8ff1a03a01094299d459a3a37a3fe16731629987165d71f328657dbf2f24c

RUN mkdir -p $ROOTDIR/src \
    && wget -q -O /tmp/hadoop.tar.gz https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && echo "${HADOOP_SHA512}  /tmp/hadoop.tar.gz" | sha512sum -c - \
    && tar -xzf /tmp/hadoop.tar.gz -C $ROOTDIR/src/ \
    && rm /tmp/hadoop.tar.gz \
    && mv $ROOTDIR/src/hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    ln -s $HADOOP_HOME/bin/hadoop /usr/local/bin/hadoop && \
    ln -s $HADOOP_HOME/bin/hdfs /usr/local/bin/hdfs && \
    ln -s $HADOOP_HOME/bin/yarn /usr/local/bin/yarn && \
    ln -s $HADOOP_HOME/bin/mapred /usr/local/bin/mapred && \
    echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# ==========================================
# STAGE 4: GDAL Builder (The Slowest Part)
# e.g. `docker build --target gdal-builder -t geobrix-gdal-builder .`
# ==========================================
FROM hadoop-builder AS gdal-builder
ARG GDAL_VERSION
# Pinned SHA-256 of the GDAL 3.11.4 source tarball (Labs lockdown policy).
# Upstream OSGeo only publishes MD5; this SHA-256 was computed locally after
# MD5-verifying the upstream download (md5: 9f4fa4b3be48fb60d5dd76fecb11a5f6).
# To bump GDAL_VERSION, update GDAL_SHA256 in lockstep.
ENV GDAL_SHA256=0fa36ee34d4451db586d2bf78ea0dbfa3b0dfae0516587f8130d21add0ac9dad
RUN wget -q -O /tmp/gdal.tar.gz https://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz \
    && echo "${GDAL_SHA256}  /tmp/gdal.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/gdal.tar.gz -C $ROOTDIR/src/ \
    && rm /tmp/gdal.tar.gz

RUN mkdir -p src/gdal-${GDAL_VERSION}/build && cd src/gdal-${GDAL_VERSION}/build \
    && cmake -G Ninja .. \
        -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DGDAL_USE_JAVA=ON -DJAVA_HOME="${JAVA_HOME}" \
        -DJNI_INCLUDE_DIRS="${JAVA_HOME}/include;${JAVA_HOME}/include/linux" \
        -DBUILD_JAVA_BINDINGS=ON -DBUILD_PYTHON_BINDINGS=OFF \
    && cmake --build . --parallel 8 && cmake --install . && ldconfig

# Redundant Symlinks as requested for project compatibility
RUN ln -s $ROOTDIR/lib/libgdal.so /usr/lib/libgdal.so && \
    ln -s $ROOTDIR/lib/libgdal.so /usr/lib/libgdal.so.30 && \
    ln -s $ROOTDIR/lib/jni/libgdalalljni.so /usr/lib/libgdalalljni.so && \
    mkdir -p /usr/lib/jni && ln -s $ROOTDIR/lib/jni/libgdalalljni.so /usr/lib/jni/libgdalalljni.so.30

# ==========================================
# STAGE 5: PDAL Builder
# e.g. `docker build --target pdal-builder -t geobrix-pdal-builder .`
# ==========================================
FROM gdal-builder AS pdal-builder
ARG PDAL_VERSION
ARG PDAL_SHA
RUN mkdir -p /tmp/pdal && cd /tmp/pdal \
    && git init -q \
    && git remote add origin https://github.com/PDAL/PDAL.git \
    && git fetch --depth 1 -q origin ${PDAL_SHA} \
    && git checkout -q FETCH_HEAD \
    && mkdir -p build && cd build \
    && cmake -G Ninja .. \
        -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DGDAL_CONFIG=/usr/local/bin/gdal-config \
        -DGDAL_INCLUDE_DIR=/usr/local/include \
        -DGDAL_LIBRARY=/usr/local/lib/libgdal.so \
        -DGEOTIFF_INCLUDE_DIR=/usr/include/geotiff \
        -DBUILD_PLUGIN_PYTHON=OFF -DBUILD_PLUGIN_GREYHOUND=OFF \
    && cmake --build . --parallel 2 && cmake --install . && ldconfig && \
    cd / && rm -rf /tmp/pdal

# ==========================================
# STAGE 6: Final Development Environment
# e.g. `docker build --target final -t geobrix-final .`
# ==========================================
FROM pdal-builder AS final

# --- Maven Installation (Pinned Checksum per Labs lockdown policy) ---
# Pinning the SHA-512 in-Dockerfile prevents a compromised archive.apache.org
# from serving both a bad tarball and a matching bad checksum. To bump
# MAVEN_VERSION, update MAVEN_SHA512 in lockstep.
ARG MAVEN_VERSION=3.9.9
ARG MAVEN_SHA512=a555254d6b53d267965a3404ecb14e53c3827c09c3b94b5678835887ab404556bfaf78dcfe03ba76fa2508649dca8531c74bca4d5846513522404d48e8c4ac8b
RUN mkdir -p $ROOTDIR/share/maven \
  && curl -fsSL -o /tmp/apache-maven.tar.gz https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  && echo "${MAVEN_SHA512}  /tmp/apache-maven.tar.gz" | sha512sum -c - \
  && tar -xzf /tmp/apache-maven.tar.gz -C $ROOTDIR/share/maven --strip-components=1 \
  && ln -s $ROOTDIR/share/maven/bin/mvn $ROOTDIR/bin/mvn && rm /tmp/apache-maven.tar.gz

ENV MAVEN_HOME="$ROOTDIR/share/maven"
# Version pins — keep in sync with .github/actions/{scala,python}_build/action.yml,
# scripts/geobrix-gdal-init.sh, and notebooks/tests/requirements.txt.
# Run `gbx:versions:audit` after edits to confirm coverage.
# DBR-aligned values: https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts
ENV SPARK_VERSION=4.0.0
ENV NUMPY_VERSION=2.1.3
ENV PANDAS_VERSION=2.2.3
ENV SETUPTOOLS_VERSION=74.0.0
ENV PIP_VERSION=25.0.1
ENV WHEEL_VERSION=0.45.1
ENV REQUESTS_VERSION=2.32.3
ENV IPYTHON_VERSION=8.30.0
ENV IPYKERNEL_VERSION=6.29.5
ENV IPYWIDGETS_VERSION=7.8.1
ENV JUPYTERLAB_VERSION=4.3.4
ENV JUPYTER_SERVER_VERSION=2.14.1
ENV JUPYTER_CORE_VERSION=5.7.2
ENV JUPYTER_CLIENT_VERSION=8.6.3
ENV TORNADO_VERSION=6.4.2
ENV NBFORMAT_VERSION=5.10.4
ENV NBCONVERT_VERSION=7.16.4
# Not in DBR; pinned to a PyPI stable that pre-dates the 7-day embargo on the
# corp pypi-proxy.dev.databricks.com (per go/pypi-registry-access). build/pytest-cov also match CI.
# 1.5.0 (released 2026-04-30) is currently embargoed; 1.4.4 is the latest available.
ENV BUILD_VERSION=1.4.4
ENV PYTEST_VERSION=8.4.2
ENV PYTEST_COV_VERSION=7.1.0
ENV GEOPANDAS_VERSION=1.1.3
# keplergl 0.3.4+ requires ipywidgets>=8.1.5, which conflicts with DBR's ipywidgets==7.8.1.
# 0.3.3 was never published on PyPI (gap between 0.3.2 stable and 0.3.4 alphas), so 0.3.2
# is the last keplergl compatible with the ipywidgets 7.x line. We prefer DBR alignment
# for ipywidgets over latest keplergl in the dev container.
ENV KEPLERGL_VERSION=0.3.2
# Intentional DBR divergence — DBR has pyzmq 26.2.0 but our Jupyter ZMQ workaround
# requires the older 25.1.1 (see "ZMQ fixes" section below). Re-evaluate on next bump.
ENV PYZMQ_VERSION=25.1.1

# --- Python Environment ---
#
# DATABRICKS-SPECIFIC: route pip through the corp PyPI proxy.
#
# Per go/pypi-registry-access (Remote registry access at Databricks), public
# PyPI (pypi.org) is DNS-blocked from Databricks corp network as a supply-chain
# hardening measure (the proxy filters package versions newer than 7 days).
# Without this configuration, every `pip install` in subsequent layers fails
# with "Connection refused" / cannot resolve pypi.org.
#
# This is the local-build counterpart to CI's JFrog OIDC routing: CI uses
# JFrog `db-pypi` (token-bound), local Docker uses the corp PyPI proxy
# (network-bound). Both are Databricks-managed trust boundaries.
#
# Mirrors the existing Maven proxy pattern (~/.m2/settings.xml → maven-proxy.dev.databricks.com).
# Update if go/pypi-registry-access changes the URL.
RUN mkdir -p /etc/pip /root/.pip && printf '[global]\ntimeout = 60\nindex-url = https://pypi-proxy.dev.databricks.com/simple\n' | tee /etc/pip.conf /root/.pip/pip.conf >/dev/null
ENV PIP_INDEX_URL=https://pypi-proxy.dev.databricks.com/simple

# 1. Upgrade pip using --ignore-installed to bypass the Debian RECORD error
RUN python3 -m pip install --upgrade pip==$PIP_VERSION --ignore-installed --break-system-packages

# 2. Install base tools and NumPy 2.x
RUN pip3 install wheel==$WHEEL_VERSION \
    setuptools==$SETUPTOOLS_VERSION \
    numpy==$NUMPY_VERSION \
    --ignore-installed \
    --break-system-packages

# 3. Build Python GDAL against your source headers
RUN pip3 install gdal==$(gdal-config --version) --break-system-packages

# Build GDAL Python bindings against the source headers built in Stage 4
RUN pip3 install gdal==$(gdal-config --version) --break-system-packages

# Final library installs. All pinned packages match CI / DBR 17.3 LTS or current
# PyPI stable — see ENV block above for sources. Unpinned: black/isort/flake8 (lint
# tools, version drift is low-risk), py4j (tracks pyspark), pystac-client/
# planetary-computer/rasterio/rioxarray/pdal (geospatial dev stack, not in DBR),
# pytest-mock/nest-asyncio/pytest-asyncio (test helpers, not in DBR).
RUN pip3 install black isort flake8 build==$BUILD_VERSION py4j requests==$REQUESTS_VERSION \
        keplergl==$KEPLERGL_VERSION ipython==$IPYTHON_VERSION ipykernel==$IPYKERNEL_VERSION \
        ipywidgets==$IPYWIDGETS_VERSION pyspark==$SPARK_VERSION --break-system-packages \
    && pip3 install pytest==$PYTEST_VERSION pytest-cov==$PYTEST_COV_VERSION pytest-mock nest-asyncio pytest-asyncio --break-system-packages \
    && pip3 install pandas==$PANDAS_VERSION geopandas==$GEOPANDAS_VERSION pystac-client planetary-computer rasterio rioxarray --break-system-packages \
    && pip3 install pdal --break-system-packages

# ZMQ fixes
ENV ZMQ_TRANSPORT=tcp
# Add this to your main apt-get block
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    haveged \
    libzmq3-dev \
    && rm -rf /var/lib/apt/lists/*

# Jupyter specific (final stage only: runtime dir and binding)
# Bind to 0.0.0.0 so kernel ZMQ works reliably in Docker (no security requirement in dev)
ENV JUPYTER_RUNTIME_DIR=/tmp/jupyter_runtime
ENV JUPYTER_IP=0.0.0.0
ENV PYDEVD_DISABLE_FILE_VALIDATION=1
ENV JUPYTER_PLATFORM_DIRS=1
RUN mkdir -p /tmp/jupyter_runtime && chmod 1777 /tmp/jupyter_runtime
RUN python3 -m ipykernel install --name python3 --display-name "Python 3 (Geobrix)" --env JUPYTER_IP 0.0.0.0 --env JUPYTER_RUNTIME_DIR /tmp/jupyter_runtime
RUN pip3 install --upgrade jupyterlab==$JUPYTERLAB_VERSION jupyter_server==$JUPYTER_SERVER_VERSION \
        jupyter_core==$JUPYTER_CORE_VERSION jupyter_client==$JUPYTER_CLIENT_VERSION \
        tornado==$TORNADO_VERSION nbformat==$NBFORMAT_VERSION nbconvert==$NBCONVERT_VERSION \
        testbook --break-system-packages
RUN pip3 install --no-cache-dir --break-system-packages pyzmq==$PYZMQ_VERSION

# Final refresh of the shared library cache
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/lib/x86_64-linux-gnu
RUN ldconfig

WORKDIR /root/geobrix

# Copy the entrypoint script into the container
# this starts haveged (maybe more?)
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

# Make the entrypoint script executable
RUN chmod +x /usr/local/bin/entrypoint.sh

# Set the entrypoint to the custom script
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Set the default command to /bin/bash, passed as an argument to the entrypoint script
CMD ["/bin/bash"]
