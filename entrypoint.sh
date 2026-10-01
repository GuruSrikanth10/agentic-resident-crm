#!/bin/bash
# Entrypoint: generate opencode provider config from runtime env vars, then start.
#
# opencode_runner.py's _harness_config() returns {} — it does not set
# OPENCODE_CONFIG_CONTENT. So the provider config (baseURL, API key) must
# live in ~/.config/opencode/config.json. But the LLM endpoint is a runtime
# env var (from the ConfigMap), not a build-time value. This script bridges
# that gap: it reads the env vars and writes the config file before start.py
# runs.
#
# Only runs when some lane uses the harness. The lanes are
# USE_OPENCODE_HARNESS_REJECTION and USE_OPENCODE_HARNESS_DLT, each falling
# back to the older single switch USE_OPENCODE_HARNESS when its own value is
# empty -- exactly what opencode_runner.lane_enabled() does, and
# tests/test_opencode_harness.py runs the block below and compares the two.
# With no lane on, start.py is called directly — no config file, no opencode
# server, no overhead.

set -e

# BEGIN harness-lanes
norm() { printf '%s' "$1" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]'; }
HARNESS_LEGACY="$(norm "${USE_OPENCODE_HARNESS:-}")"
HARNESS_REJECTION="$(norm "${USE_OPENCODE_HARNESS_REJECTION:-}")"
HARNESS_DLT="$(norm "${USE_OPENCODE_HARNESS_DLT:-}")"
[ -n "$HARNESS_REJECTION" ] || HARNESS_REJECTION="$HARNESS_LEGACY"
[ -n "$HARNESS_DLT" ] || HARNESS_DLT="$HARNESS_LEGACY"
# END harness-lanes

if [ "$HARNESS_REJECTION" = "true" ] || [ "$HARNESS_DLT" = "true" ]; then
    CONFIG_DIR="${HOME}/.config/opencode"
    mkdir -p "$CONFIG_DIR"

    # Read the LLM endpoint and key from the same env vars llm_utils.py uses.
    BASE_URL="${LLM_BASE_URL_COMPLEX:-http://localhost:8000/v1}"
    API_KEY="${LLM_API_KEY_COMPLEX:-dummy}"

    # MUST match opencode_runner.DEFAULT_MODEL exactly. The first segment is
    # the provider key: this script writes the provider block under that name,
    # and opencode_runner asks for a model under that name. Two different
    # defaults means the config declares a provider nobody requests, every
    # harness task fails, and every node falls back to the direct LLM -- which
    # looks like the harness doing nothing rather than like a broken config.
    # tests/test_opencode_harness.py asserts the two stay equal.
    MODEL="${OPENCODE_MODEL:-uidai/glm-5.2-fp8}"

    # The model id in OPENCODE_MODEL is "provider/model" — split on "/".
    PROVIDER="${MODEL%%/*}"
    MODEL_NAME="${MODEL#*/}"

    if [ "${PROVIDER}" = "${MODEL}" ]; then
        echo "ERROR: OPENCODE_MODEL must be 'provider/model', got '${MODEL}'." >&2
        echo "       Without a provider segment the generated config declares" >&2
        echo "       a provider the harness never asks for." >&2
        exit 1
    fi

    cat > "${CONFIG_DIR}/config.json" <<EOF
{
  "provider": {
    "${PROVIDER}": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "OpenAI Compatible",
      "options": {
        "baseURL": "${BASE_URL}",
        "apiKey": "${API_KEY}"
      },
      "models": {
        "${MODEL_NAME}": {"name": "${MODEL_NAME}"}
      }
    }
  }
}
EOF

    echo "opencode provider config written to ${CONFIG_DIR}/config.json"
    echo "  provider: ${PROVIDER}"
    echo "  model:    ${MODEL_NAME}"
    echo "  baseURL:  ${BASE_URL}"
fi

exec python3 start.py
