#!/usr/bin/env bash
set -euo pipefail

# Restore mongodump to MongoDB Atlas
# Usage examples:
#   ./restore_to_atlas.sh dumps/volvo-service-orders-20231019-120000
#   ./restore_to_atlas.sh --archive cosmos_dump.gz
# Options:
#   --archive <file>  Restore from single gzip archive
#   --db <name>       Override DB name in Atlas
#   --collection <c>  Restore only this collection
#   --drop            Drop target collection(s) before restore

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ -f .env_local ]]; then
  # shellcheck disable=SC1091
  source .env_local
elif [[ -f config.env ]]; then
  # shellcheck disable=SC1091
  source config.env

ATLAS_URI="${MIG_TARGET_DB_CONNECTION_STRING:-}"
ATLAS_DB="${MIG_TARGET_DB_NAME:-volvo-service-orders}"
ATLAS_COLL="${MIG_TARGET_DB_COLLECTION:-}"

if ! command -v mongorestore >/dev/null 2>&1; then
  echo "ERROR: mongorestore not found. Install MongoDB Database Tools: https://www.mongodb.com/try/download/database-tools" >&2
  exit 1
fi

if [[ -z "$ATLAS_URI" ]]; then
  echo "ERROR: MIG_TARGET_DB_CONNECTION_STRING not set (in .env)." >&2
  exit 1
fi

SRC_DIR=""
ARCHIVE_FILE=""
DROP_FLAG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --archive)
      ARCHIVE_FILE="$2"; shift 2;;
    --db)
      ATLAS_DB="$2"; shift 2;;
    --collection)
      ATLAS_COLL="$2"; shift 2;;
    --drop)
      DROP_FLAG="--drop"; shift;;
    *)
      if [[ -z "$SRC_DIR" ]]; then
        SRC_DIR="$1"; shift
      else
        echo "Unknown arg: $1" >&2; exit 1
      fi;;
  esac
done

timestamp() { date '+%Y-%m-%d %H:%M:%S'; }

echo "[$(timestamp)] Starting mongorestore to Atlas..."

RESTORE_ARGS=(
  --uri "$ATLAS_URI"
  --db "$ATLAS_DB"
  --gzip
  --numInsertionWorkersPerCollection 40
  --maintainInsertionOrder
)

if [[ -n "$ATLAS_COLL" ]]; then
  RESTORE_ARGS+=( --collection "$ATLAS_COLL" )
fi

if [[ -n "$DROP_FLAG" ]]; then
  RESTORE_ARGS+=( $DROP_FLAG )
fi

if [[ -n "$ARCHIVE_FILE" ]]; then
  RESTORE_ARGS+=( --archive="$ARCHIVE_FILE" )
else
  if [[ -z "$SRC_DIR" ]]; then
    echo "ERROR: Provide dump directory or --archive file" >&2
    exit 1
  fi
  RESTORE_ARGS+=( "$SRC_DIR" )
fi

echo "[$(timestamp)] mongorestore args: db=$ATLAS_DB coll=${ATLAS_COLL:-ALL} src=${ARCHIVE_FILE:-$SRC_DIR}"
mongorestore "${RESTORE_ARGS[@]}"

echo "[$(timestamp)] mongorestore completed successfully."





# mongodump --uri="mongodb://user:pwd@poc-new-vida.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@poc-new-vida@" \
#   --archive \
#   --gzip | \
# mongorestore \
#   --uri="mongodb+srv://user:pwd@cluster0.tcrpd.mongodb.net/?retryWrites=false&w=0&appName=Cluster0" \
#   --archive \
#   --gzip \
#   --numInsertionWorkersPerCollection=4 \
#   --drop