# 1. Bring in Node.js from the official Debian-based image
# (Must use 'slim' not 'alpine' so the binaries match your Python base image's glibc)
FROM hbdc-harbor-registry-non-prod.uidai.net.in/base/node:20-slim AS node-src

# 2. Base image
FROM hbdc-harbor-registry-non-prod.uidai.net.in/base/python_base:3.14.6-slim

# Configure Ubuntu mirrors
RUN echo "deb http://10.81.213.11:8081/ubuntu/mirror/archive.ubuntu.com/ubuntu jammy restricted universe main multiverse\n\
deb http://10.81.213.11:8081/ubuntu/mirror/archive.ubuntu.com/ubuntu/ jammy-updates restricted universe main multiverse\n\
deb http://10.81.213.11:8081/ubuntu/mirror/archive.ubuntu.com/ubuntu/ jammy-security restricted universe main multiverse\n\
deb http://10.81.213.11:8081/ubuntu/mirror/archive.ubuntu.com/ubuntu/ jammy-backports restricted universe main multiverse" > /etc/apt/sources.list

ARG DEBIAN_FRONTEND=noninteractive

# ---- Network / Proxy Configuration ----
ENV HTTP_PROXY=http://10.10.206.59:8080 \
    HTTPS_PROXY=http://10.10.206.59:8080 \
    NO_PROXY=10.10.206.59,10.10.204.46,10.10.109.101,10.81.213.11,localhost,127.0.0.1

# ---- Environment ----
ENV PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:${PATH} \
    PIP_INDEX_URL=http://10.10.206.59:8080/repository/pypi-proxy/simple \
    PIP_EXTRA_INDEX_URL=http://10.10.204.46:8080/repository/pypi-proxy/simple \
    PIP_ROOT_USER_ACTION=ignore \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PYTHONIOENCODING=utf-8 \
    PYTHONUTF8=1

WORKDIR /app

# ---- System dependencies ----
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    ca-certificates \
    build-essential \
    libsqlite3-0 \
    ripgrep \
    && rm -rf /var/lib/apt/lists/*

# ---- Verify Python >= 3.12 ----
RUN python3 -c "import sys; exit(0 if sys.version_info >= (3, 12) else 1)" \
    || (echo "ERROR: Agentic Resident CRM requires Python >=3.12, found $(python3 --version 2>&1)" && exit 1)

# ---- Node.js for opencode CLI ----
# Copy Node and npm binaries directly from the official image.
COPY --from=node-src /usr/local/bin/node /usr/local/bin/node
COPY --from=node-src /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s /usr/local/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
 && ln -s /usr/local/lib/node_modules/npx/bin/npx-cli.js /usr/local/bin/npx

# ---- Python dependencies (in an isolated venv) ----
COPY requirements.txt .
RUN python3 -m venv /venv && \
    /venv/bin/pip install --no-cache-dir \
        --trusted-host 10.10.206.59 \
        --trusted-host 10.10.204.46 \
        --upgrade "pip>=23.3" setuptools wheel && \
    /venv/bin/pip install --no-cache-dir \
        --trusted-host 10.10.206.59 \
        --trusted-host 10.10.204.46 \
        "cyclonedx-bom" && \
    /venv/bin/pip install --no-cache-dir \
        --trusted-host 10.10.206.59 \
        --trusted-host 10.10.204.46 \
        -r requirements.txt

ENV PATH="/venv/bin:/usr/local/bin:${PATH}"

# Generate CycloneDX Software Bill of Materials (SBOM)
RUN cyclonedx-py environment --of JSON -o /SCA-bom.json

# ---- opencode CLI (the harness) ----
RUN npm config set registry http://10.10.206.59:8080/repository/npm-proxy/ && \
    npm install -g opencode-ai

# ---- Copy source ----
COPY src /app/src
COPY start.py /app/start.py
COPY local_run.py /app/local_run.py
COPY agent_policy_context.md /app/agent_policy_context.md
COPY opencode.json /app/opencode.json
COPY AGENTS.md /app/AGENTS.md
COPY reason_codes.csv /app/reason_codes.csv
COPY version.json /app/version.json
COPY entrypoint.sh /app/entrypoint.sh

# ---- Runtime directories ----
RUN mkdir -p /app/local_casesheets /app/local_checkpoints /app/docs_cache

# ---- Non-root user (with home dir for opencode config) ----
RUN useradd -m -u 8888 appuser && \
    chmod +x /app/entrypoint.sh && \
    chown -R appuser:appuser /app

USER appuser

# ---- API port ----
EXPOSE 8000

# ---- Unset Proxy for Runtime ----
ENV HTTP_PROXY="" \
    HTTPS_PROXY=""

# ---- Default: start the process supervisor (API + Kafka consumers) ----
ENTRYPOINT ["/app/entrypoint.sh"]
