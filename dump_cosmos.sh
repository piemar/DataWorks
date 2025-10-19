#!/usr/bin/env bash
set -euo pipefail

# Cosmos DB (Mongo API) -> mongodump helper
# Usage examples:
#   ./dump_cosmos.sh dumps/volvo-service-orders   # dump into directory (gzip)
#   ./dump_cosmos.sh --archive cosmos_dump.gz     # dump into single gzip archive
# Options:
#   --archive <file>  Dump to a single gzip archive file instead of a directory
#   --db <name>       Override DB name from env
#   --collection <c>  Dump only this collection

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ -f .env_local ]]; then
  # shellcheck disable=SC1091
  source .env_local
elif [[ -f config.env ]]; then
  # shellcheck disable=SC1091
  source config.env

COSMOS_URI="${COSMOS_DB_CONNECTION_STRING:-}"
COSMOS_DB="${COSMOS_DB_NAME:-volvo-service-orders}"
COSMOS_COLL="${COSMOS_DB_COLLECTION:-}"

if ! command -v mongodump >/dev/null 2>&1; then
  echo "ERROR: mongodump not found. Install MongoDB Database Tools: https://www.mongodb.com/try/download/database-tools" >&2
  exit 1
fi

if [[ -z "$COSMOS_URI" ]]; then
  echo "ERROR: COSMOS_DB_CONNECTION_STRING not set (in config.env)." >&2
  exit 1
fi

OUT_DIR=""
ARCHIVE_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --archive)
      ARCHIVE_FILE="$2"; shift 2;;
    --db)
      COSMOS_DB="$2"; shift 2;;
    --collection)
      COSMOS_COLL="$2"; shift 2;;
    *)
      if [[ -z "$OUT_DIR" ]]; then
        OUT_DIR="$1"; shift
      else
        echo "Unknown arg: $1" >&2; exit 1
      fi;;
  esac
done

timestamp() { date '+%Y-%m-%d %H:%M:%S'; }

echo "[$(timestamp)] Starting mongodump from Cosmos..."

# Build base args
DUMP_ARGS=(
  --uri "$COSMOS_URI"
  --db "$COSMOS_DB"
  --gzip
  --readPreference primary
  --numParallelCollections 4
)

if [[ -n "$COSMOS_COLL" ]]; then
  DUMP_ARGS+=( --collection "$COSMOS_COLL" )
fi

if [[ -n "$ARCHIVE_FILE" ]]; then
  DUMP_ARGS+=( --archive="$ARCHIVE_FILE" )
else
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="dumps/${COSMOS_DB}-$(date +%Y%m%d-%H%M%S)"
  fi
  mkdir -p "$OUT_DIR"
  DUMP_ARGS+=( --out "$OUT_DIR" )
fi

echo "[$(timestamp)] mongodump args: db=$COSMOS_DB coll=${COSMOS_COLL:-ALL} target=${ARCHIVE_FILE:-$OUT_DIR}"
mongodump "${DUMP_ARGS[@]}"

echo "[$(timestamp)] mongodump completed successfully."

