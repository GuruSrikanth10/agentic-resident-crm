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
# Only runs when USE_OPENCODE_HARNESS=true. Otherwise start.py is called
# directly — no config file, no opencode server, no overhead.

set -e

if [ "${USE_OPENCODE_HARNESS}" = "true" ]; then
    CONFIG_DIR="${HOME}/.config/opencode"
    mkdir -p "$CONFIG_DIR"

    # Read the LLM endpoint and key from the same env vars llm_utils.py uses.
    BASE_URL="${LLM_BASE_URL_COMPLEX:-http://localhost:8000/v1}"
    API_KEY="${LLM_API_KEY_COMPLEX:-dummy}"
    MODEL="${OPENCODE_MODEL:-opencode/glm-5.2-fp8}"

    # The model id in OPENCODE_MODEL is "provider/model" — split on "/".
    PROVIDER="${MODEL%%/*}"
    MODEL_NAME="${MODEL#*/}"

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
