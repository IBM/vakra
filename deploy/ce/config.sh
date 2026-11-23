#!/bin/bash
# ============================================================
# Shared config for deploying VAKRA to IBM Code Engine + COS.
#
# Mirrors the team's existing Code Engine deploy conventions so
# everything lands in the SAME account/region/project/registry you already use.
# This is an ADDITIONAL, admin-only path — it never touches the local
# docker-compose flow or any existing CE app / COS bucket.
#
# Override anything via env, e.g.:  COS_BUCKET=my-bucket ./1_create_bucket_and_pds.sh
# ============================================================

# ---- Account / region / project (reused from the routing account) ----------
export REGION="${REGION:-us-east}"
export RESOURCE_GROUP_NAME="${RESOURCE_GROUP_NAME:-routing}"
export CE_PROJECT_NAME="${CE_PROJECT_NAME:-ce-project-routing}"

# ---- Container registry (global icr.io — us-east has no regional endpoint) ---
export REGISTRY_HOST="${REGISTRY_HOST:-icr.io}"
export REGISTRY_NAMESPACE="${REGISTRY_NAMESPACE:-routing_namespace}"
export REGISTRY_SECRET_NAME="${REGISTRY_SECRET_NAME:-icr-secret-1}"
export IMAGE_REPO="${REGISTRY_HOST}/${REGISTRY_NAMESPACE}/vakra-benchmark"
export IMAGE_REF="${IMAGE_REF:-${IMAGE_REPO}:latest}"
export EXPLORER_IMAGE_REPO="${REGISTRY_HOST}/${REGISTRY_NAMESPACE}/vakra-explorer"
export EXPLORER_IMAGE_REF="${EXPLORER_IMAGE_REF:-${EXPLORER_IMAGE_REPO}:latest}"

# ---- Object Storage (new bucket in your existing COS instance) --------------
# COS_CREDS_JSON points at a COS *service-credential* JSON (create it in IBM Cloud with
# "Include HMAC Credential" enabled). It must contain credentials.{apikey,
# resource_instance_id, cos_hmac_keys.{access_key_id,secret_access_key}}.
export COS_CREDS_JSON="${COS_CREDS_JSON:-$HOME/.cos_creds.json}"
# NEW bucket, dedicated to VAKRA. Globally-unique name — change if taken.
export COS_BUCKET="${COS_BUCKET:-vakra-benchmark-data-$REGION}"
export COS_ENDPOINT="${COS_ENDPOINT:-https://s3.$REGION.cloud-object-storage.appdomain.cloud}"
# CE HMAC secret (for the persistent data store) + the data store name.
export COS_ACCESS_SECRET_NAME="${COS_ACCESS_SECRET_NAME:-vakra-cos-access}"
export PDS_NAME="${PDS_NAME:-vakra-store}"

# ---- Code Engine apps: one per capability (all from the same image) ---------
# name  ->  CAPABILITY_ID
export APP_CAP1="${APP_CAP1:-vakra-cap1}"
export APP_CAP2="${APP_CAP2:-vakra-cap2}"
export APP_CAP3="${APP_CAP3:-vakra-cap3}"
export APP_CAP4="${APP_CAP4:-vakra-cap4}"
export APP_EXPLORER="${APP_EXPLORER:-vakra-explorer}"

# Data delivery on CE:  mount = COS pds mount (default, recommended)
#                       sync  = download COS -> ephemeral disk at startup (fallback)
export DATA_SOURCE="${DATA_SOURCE:-mount}"

# Sizing. Capabilities 1-3 are light; capability 4 loads embeddings + ChromaDB.
export CPU="${CPU:-1}"
export MEMORY="${MEMORY:-4G}"
# Per-capability sizing, measured under parallel load (see README "Parallel runs & sizing").
# cap1 uses CPU/MEMORY above. cap2: ~37 calls/s; cap3: ~14-22 calls/s; cap4: ~22 SQL calls/s
# and ~3.5 semantic searches/s (search is CPU-bound).
export CAP2_CPU="${CAP2_CPU:-6}"
export CAP2_MEMORY="${CAP2_MEMORY:-48G}"
export CAP3_CPU="${CAP3_CPU:-4}"
export CAP3_MEMORY="${CAP3_MEMORY:-16G}"
export CAP4_CPU="${CAP4_CPU:-8}"
export CAP4_MEMORY="${CAP4_MEMORY:-32G}"

# Runtime tuning passed to every app (see docker/entrypoint-unified.sh, docker/mcp_http_bridge.py).
export M3_WORKERS="${M3_WORKERS:-4}"                      # uvicorn workers for the M3 SQL API
export BRIDGE_MAX_PERSISTENT="${BRIDGE_MAX_PERSISTENT:-64}" # live per-domain backends (~60 MB each)
export MCP_BACKEND_TIMEOUT="${MCP_BACKEND_TIMEOUT:-30}"     # seconds a tool call waits for the API
export CAP4_MCP_BACKEND_TIMEOUT="${CAP4_MCP_BACKEND_TIMEOUT:-120}"  # queued searches wait, not fail
# Ephemeral storage: tiny for mount mode; must exceed the dataset for sync mode.
if [ "$DATA_SOURCE" = "sync" ]; then
  export EPHEMERAL="${EPHEMERAL:-30G}"
  export CAP4_EPHEMERAL="${CAP4_EPHEMERAL:-40G}"
  # sync needs enough memory headroom to hold the ephemeral tier; bump if needed.
  export MEMORY="${MEMORY_SYNC:-$MEMORY}"
else
  export EPHEMERAL="${EPHEMERAL:-2G}"
  export CAP4_EPHEMERAL="${CAP4_EPHEMERAL:-4G}"
fi

# Keep >=1 instance warm so endpoints never cold-start. MIN_SCALE=0 -> scale-to-zero.
export MIN_SCALE="${MIN_SCALE:-1}"
export MAX_SCALE="${MAX_SCALE:-1}"
export BRIDGE_PORT="${BRIDGE_PORT:-8080}"

# Repo root (two levels up from deploy/ce/).
export APP_ROOT="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/../.." &> /dev/null && pwd )"
export CE_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
export URLS_ENV_FILE="${URLS_ENV_FILE:-$CE_DIR/.ce_urls.env}"

# ---- Pull COS creds (apikey + CRN + HMAC) out of the credential JSON --------
if [[ -f "$COS_CREDS_JSON" ]]; then
  export COS_API_KEY="$(python3 -c "import json;c=json.load(open('$COS_CREDS_JSON'))['credentials'];print(c['apikey'])" 2>/dev/null)"
  export COS_INSTANCE_CRN="$(python3 -c "import json;c=json.load(open('$COS_CREDS_JSON'))['credentials'];print(c['resource_instance_id'])" 2>/dev/null)"
  export COS_HMAC_ACCESS_KEY_ID="$(python3 -c "import json;c=json.load(open('$COS_CREDS_JSON'))['credentials'];print(c['cos_hmac_keys']['access_key_id'])" 2>/dev/null)"
  export COS_HMAC_SECRET_ACCESS_KEY="$(python3 -c "import json;c=json.load(open('$COS_CREDS_JSON'))['credentials'];print(c['cos_hmac_keys']['secret_access_key'])" 2>/dev/null)"
fi

# ---- Helpers ---------------------------------------------------------------
function require_login() {
  if ! ibmcloud target -o json 2>/dev/null | python3 -c "import sys,json;json.load(sys.stdin)" >/dev/null 2>&1; then
    echo "Not logged in. Run:  ibmcloud login --sso  (then re-run)"; exit 1
  fi
}

function ce_target() {
  ibmcloud target -r "$REGION" -g "$RESOURCE_GROUP_NAME" >/dev/null
  ibmcloud ce project select --name "$CE_PROJECT_NAME" >/dev/null
}

# Admin gate: this path can create cloud resources. Require an explicit opt-in.
function admin_guard() {
  if [[ "${VAKRA_CE_ADMIN:-}" != "1" ]]; then
    echo "Refusing to run: this is the admin-only Code Engine path."
    echo "Set VAKRA_CE_ADMIN=1 to proceed, e.g.:  VAKRA_CE_ADMIN=1 $0"
    exit 1
  fi
  if [[ "${1:-}" == "-y" || "${YES:-}" == "1" ]]; then return 0; fi
  echo "About to act on:"
  echo "   region=$REGION  group=$RESOURCE_GROUP_NAME  project=$CE_PROJECT_NAME"
  echo "   registry=$IMAGE_REPO"
  echo "   bucket=$COS_BUCKET  data-source=$DATA_SOURCE"
  read -r -p "Proceed? [y/N] " ans
  [[ "$ans" == "y" || "$ans" == "Y" ]] || { echo "Aborted."; exit 1; }
}
