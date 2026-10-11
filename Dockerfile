# syntax=docker/dockerfile:1
# Development and CI image. Everything runs in here; nothing is installed on the host.
#  - conda base env pinned to the Python minor version Databricks serverless reports
#  - a headless JRE, because PySpark drives a local Spark JVM for the unit tests
#  - the Databricks CLI, for OAuth login and Asset Bundles
FROM condaforge/miniforge3:26.5.3-0

ARG PYTHON_VERSION=3.12
ARG DATABRICKS_CLI_VERSION=1.19.0
ARG DATABRICKS_CLI_SHA256_AMD64=bddf87df3d72b61384da724b40f18ab483a98af742748260097a7bb8154b070e
ARG DATABRICKS_CLI_SHA256_ARM64=2997ccbda57090ee48bd9282d5cb3a5bef78025db64fd1573fbd7c8726856086
# Match the host user so files written to the bind-mounted repo aren't owned by root.
ARG HOST_UID=1000
ARG HOST_GID=1000

# socat forwards the CLI's OAuth callback port, see scripts/auth-login.sh.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless socat curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

RUN mamba install -y -n base "python=${PYTHON_VERSION}" && mamba clean -afy

RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      amd64) sha="$DATABRICKS_CLI_SHA256_AMD64" ;; \
      arm64) sha="$DATABRICKS_CLI_SHA256_ARM64" ;; \
      *) echo "unsupported arch $arch" >&2; exit 1 ;; \
    esac; \
    url="https://github.com/databricks/cli/releases/download/v${DATABRICKS_CLI_VERSION}/databricks_cli_${DATABRICKS_CLI_VERSION}_linux_${arch}.tar.gz"; \
    curl -fsSL -o /tmp/cli.tar.gz "$url"; \
    echo "${sha}  /tmp/cli.tar.gz" | sha256sum -c -; \
    tar -xzf /tmp/cli.tar.gz -C /usr/local/bin databricks; \
    rm /tmp/cli.tar.gz; \
    databricks --version

# Ubuntu base images ship a user with UID 1000; replace it with ours.
RUN set -eux; \
    existing="$(getent passwd "$HOST_UID" | cut -d: -f1 || true)"; \
    if [ -n "$existing" ]; then userdel -r "$existing" 2>/dev/null || userdel "$existing"; fi; \
    getent group "$HOST_GID" >/dev/null || groupadd -g "$HOST_GID" dev; \
    useradd -m -u "$HOST_UID" -g "$HOST_GID" -s /bin/bash dev; \
    mkdir -p /workspace /home/dev/.databricks; \
    chown -R "$HOST_UID:$HOST_GID" /workspace /home/dev

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_LOCAL_IP=127.0.0.1

# Install dependencies against a stub package. At runtime the repo is bind-mounted
# over /workspace and the editable install picks up the real src/ without a rebuild,
# so the slow layer (pyspark is ~400 MB) only rebuilds when pyproject.toml changes.
WORKDIR /workspace
COPY pyproject.toml ./
RUN mkdir -p src/gh_lakehouse \
 && touch src/gh_lakehouse/__init__.py README.md \
 && pip install --no-cache-dir -e ".[dev]"

USER dev

# Resolve the Delta Lake jars from Maven once, at build time, into the ivy cache,
# so tests start offline and CI doesn't hit Maven on every run.
RUN python - <<'EOF'
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
configure_spark_with_delta_pip(builder).getOrCreate().stop()
EOF

CMD ["bash"]
